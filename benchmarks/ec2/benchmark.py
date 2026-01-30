#!/usr/bin/env python3
"""
Thunderduck EC2 Benchmark Driver

Main entry point for running TPC-H and TPC-DS benchmarks comparing
Thunderduck vs Apache Spark on AWS EC2 instances managed by CDK.

Usage:
    # First deploy infrastructure with CDK
    cd benchmarks/cdk && npx aws-cdk deploy -c instance_type=m8g.16xlarge

    # Get SSH key
    cd benchmarks/ec2
    python -c "from provisioner import CDKProvisioner; CDKProvisioner.get_ssh_key_from_ssm('ThunderduckBenchmark', 'benchmark_key.pem')"

    # Run benchmarks (uses default stack name and key file)
    python benchmark.py --scale-factor 100 --s3-bucket my-bucket --benchmark tpch
"""
import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from provisioner import CDKProvisioner
from setup_s3 import setup_s3_bucket, check_data_exists
from results import generate_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run TPC-H/TPC-DS benchmarks on CDK-managed EC2 instances",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Deploy infrastructure first
    cd benchmarks/cdk
    npx aws-cdk deploy -c instance_type=m8g.16xlarge

    # Get SSH key (saves to benchmark_key.pem by default)
    cd benchmarks/ec2
    python -c "from provisioner import CDKProvisioner; CDKProvisioner.get_ssh_key_from_ssm('ThunderduckBenchmark', 'benchmark_key.pem')"

    # Run benchmarks (uses default stack name and key file)
    python benchmark.py --scale-factor 100 --s3-bucket my-bucket --benchmark tpch

Environment variables:
    BENCHMARK_S3_BUCKET   S3 bucket for test data (required if --s3-bucket not set)
    BENCHMARK_S3_PREFIX   S3 prefix for test data (default: thunderduck-benchmarks)
    AWS_REGION            AWS region (optional, defaults to us-east-1)
        """,
    )

    parser.add_argument(
        "--stack-name",
        default="ThunderduckBenchmark",
        help="CDK stack name (default: ThunderduckBenchmark)",
    )
    parser.add_argument(
        "--key-file",
        default="benchmark_key.pem",
        help="Path to SSH private key file (default: benchmark_key.pem)",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="TPC-H/TPC-DS scale factor (1, 10, 100, 1000, or custom)",
    )
    parser.add_argument(
        "--benchmark",
        choices=["tpch", "tpcds", "all"],
        default="all",
        help="Which benchmark to run (default: all)",
    )
    parser.add_argument(
        "--s3-bucket",
        default=os.environ.get("BENCHMARK_S3_BUCKET"),
        help="S3 bucket for test data",
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("BENCHMARK_S3_PREFIX", "thunderduck-benchmarks"),
        help="S3 prefix for test data (default: thunderduck-benchmarks)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of runs per query (default: 5, first is warmup)",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmark_results",
        help="Directory for output files (default: benchmark_results)",
    )
    parser.add_argument(
        "--thunderduck-jar",
        help="Path to Thunderduck JAR file (default: auto-detect from project)",
    )
    parser.add_argument(
        "--region",
        help="AWS region (default: from environment)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without actually running",
    )

    args = parser.parse_args()

    if not args.s3_bucket:
        parser.error("--s3-bucket or BENCHMARK_S3_BUCKET environment variable required")

    return args


def find_thunderduck_jar() -> Optional[Path]:
    """Find the Thunderduck JAR in the project directory."""
    project_root = Path(__file__).parent.parent.parent
    target_dir = project_root / "connect-server" / "target"

    if not target_dir.exists():
        return None

    jars = list(target_dir.glob("thunderduck-connect-server-*.jar"))
    # Prefer non-original JARs (shaded)
    jars = [j for j in jars if "original" not in j.name]

    if jars:
        return max(jars, key=lambda p: p.stat().st_mtime)
    return None


def run_benchmark_on_instance(
    provisioner: CDKProvisioner,
    engine: str,
    args: argparse.Namespace,
    thunderduck_jar: Optional[Path],
) -> Dict:
    """Run benchmark on a single EC2 instance."""
    print(f"[{engine}] Setting up instance...")

    try:
        # Get SSH client
        ssh = provisioner.get_ssh_client(engine)

        # Upload setup files
        sftp = ssh.open_sftp()

        # Upload remote setup script
        setup_script = Path(__file__).parent / "remote_setup.sh"
        sftp.put(str(setup_script), "/tmp/remote_setup.sh")

        # Upload benchmark runner
        runner_script = Path(__file__).parent / "run_benchmark.py"
        sftp.put(str(runner_script), "/tmp/run_benchmark.py")

        # Upload query files
        queries_dir = Path(__file__).parent.parent / "queries"
        try:
            sftp.mkdir("/tmp/queries")
        except IOError:
            pass  # Directory may already exist
        for query_file in queries_dir.glob("*.py"):
            sftp.put(str(query_file), f"/tmp/queries/{query_file.name}")

        # For Thunderduck, upload JAR
        if engine == "thunderduck" and thunderduck_jar:
            print(f"[{engine}] Uploading Thunderduck JAR...")
            sftp.put(str(thunderduck_jar), "/tmp/thunderduck.jar")

        sftp.close()

        # Run setup script
        print(f"[{engine}] Running setup script...")
        spark_version = engine.replace("spark-", "") if engine.startswith("spark-") else None
        setup_cmd = f"chmod +x /tmp/remote_setup.sh && /tmp/remote_setup.sh {engine}"
        if spark_version:
            setup_cmd += f" {spark_version}"

        stdin, stdout, stderr = ssh.exec_command(setup_cmd, timeout=600)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            error = stderr.read().decode()
            raise RuntimeError(f"Setup failed: {error}")

        # Download data from S3
        print(f"[{engine}] Downloading benchmark data from S3...")
        benchmarks = []
        if args.benchmark in ["tpch", "all"]:
            benchmarks.append("tpch")
        if args.benchmark in ["tpcds", "all"]:
            benchmarks.append("tpcds")

        for benchmark in benchmarks:
            s3_path = f"s3://{args.s3_bucket}/{args.s3_prefix}/{benchmark}/sf{args.scale_factor}"
            local_path = f"/tmp/data/{benchmark}"
            download_cmd = f"mkdir -p {local_path} && aws s3 sync {s3_path} {local_path}"
            stdin, stdout, stderr = ssh.exec_command(download_cmd, timeout=1800)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                error = stderr.read().decode()
                raise RuntimeError(f"Data download failed: {error}")

        # Run benchmark
        print(f"[{engine}] Running benchmark queries...")
        benchmark_cmd = (
            f"python3 /tmp/run_benchmark.py "
            f"--engine {engine} "
            f"--benchmark {args.benchmark} "
            f"--scale-factor {args.scale_factor} "
            f"--runs {args.runs} "
            f"--data-dir /tmp/data"
        )

        stdin, stdout, stderr = ssh.exec_command(benchmark_cmd, timeout=7200)
        output = stdout.read().decode()
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error = stderr.read().decode()
            raise RuntimeError(f"Benchmark failed: {error}")

        # Parse results from output
        results = json.loads(output.split("RESULTS_JSON:")[-1].strip())

        return {"engine": engine, "results": results, "success": True}

    except Exception as e:
        return {"engine": engine, "error": str(e), "success": False}


def main():
    args = parse_args()

    print("=" * 70)
    print("Thunderduck EC2 Benchmark (CDK)")
    print("=" * 70)
    print(f"Stack Name: {args.stack_name}")
    print(f"Scale Factor: {args.scale_factor}")
    print(f"Benchmark: {args.benchmark}")
    print(f"S3 Bucket: {args.s3_bucket}/{args.s3_prefix}")
    print(f"Runs per Query: {args.runs}")
    print("=" * 70)

    # Find Thunderduck JAR
    thunderduck_jar = args.thunderduck_jar
    if not thunderduck_jar:
        thunderduck_jar = find_thunderduck_jar()
        if thunderduck_jar:
            print(f"Found Thunderduck JAR: {thunderduck_jar}")
        else:
            print("Warning: Thunderduck JAR not found. Build with 'mvn package' first.")
            return 1

    # Connect to CDK stack
    print(f"\nConnecting to CDK stack '{args.stack_name}'...")
    try:
        provisioner = CDKProvisioner(
            stack_name=args.stack_name,
            key_file=args.key_file,
            region=args.region,
        )
    except Exception as e:
        print(f"Error: {e}")
        return 1

    engines = provisioner.get_engines()
    print(f"Found {len(engines)} engines: {engines}")

    if args.dry_run:
        print("\n[DRY RUN] Would perform the following:")
        print(f"  1. Check/generate data in S3 at sf{args.scale_factor}")
        print(f"  2. Run benchmarks on {len(engines)} instances")
        print(f"  3. Collect results and generate report")
        return 0

    # Setup S3 bucket
    print("\nSetting up S3 bucket...")
    setup_s3_bucket(args.s3_bucket, args.s3_prefix)

    # Check if data exists
    benchmarks_to_check = []
    if args.benchmark in ["tpch", "all"]:
        benchmarks_to_check.append("tpch")
    if args.benchmark in ["tpcds", "all"]:
        benchmarks_to_check.append("tpcds")

    for benchmark in benchmarks_to_check:
        if not check_data_exists(args.s3_bucket, args.s3_prefix, benchmark, args.scale_factor):
            print(f"\nError: {benchmark.upper()} data not found in S3 at sf{args.scale_factor}.")
            print(f"Generate data first with: python generate_data.py --benchmark {benchmark} --scale-factor {args.scale_factor} --upload-to-s3 {args.s3_bucket}")
            return 1

    # Run benchmarks in parallel
    print(f"\nRunning benchmarks on {len(engines)} instances...")
    results = []

    try:
        with ThreadPoolExecutor(max_workers=len(engines)) as executor:
            futures = {
                executor.submit(
                    run_benchmark_on_instance,
                    provisioner,
                    engine,
                    args,
                    thunderduck_jar,
                ): engine
                for engine in engines
            }

            for future in as_completed(futures):
                engine = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result["success"]:
                        print(f"[{engine}] Benchmark completed successfully")
                    else:
                        print(f"[{engine}] Benchmark failed: {result.get('error')}")
                except Exception as e:
                    print(f"[{engine}] Exception: {e}")
                    results.append({"engine": engine, "error": str(e), "success": False})

        # Generate report
        print("\nGenerating report...")
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = output_dir / f"benchmark_report_{timestamp}.md"
        json_path = output_dir / f"benchmark_results_{timestamp}.json"

        # Create a mock args object with instance_type for the report
        class ReportArgs:
            pass
        report_args = ReportArgs()
        report_args.instance_type = "CDK-managed"
        report_args.scale_factor = args.scale_factor
        report_args.benchmark = args.benchmark
        report_args.runs = args.runs

        generate_report(results, report_args, report_path, json_path)
        print(f"Report saved to: {report_path}")
        print(f"JSON results saved to: {json_path}")

    finally:
        provisioner.close_all()

    return 0


if __name__ == "__main__":
    sys.exit(main())
