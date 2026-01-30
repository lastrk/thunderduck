#!/usr/bin/env python3
"""
Benchmark Runner (Remote)

Executes benchmark queries on an EC2 instance and returns results.
This script runs on the remote EC2 instance, not the local driver.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

# Add queries directory to path
sys.path.insert(0, "/tmp/queries")


@dataclass
class QueryResult:
    """Result of a single query execution."""

    query_name: str
    run_times_ms: List[float]
    avg_time_ms: float
    row_count: int
    success: bool
    error: Optional[str] = None


def start_thunderduck_server() -> bool:
    """Start Thunderduck Connect Server."""
    print("Starting Thunderduck server...")

    # Kill any existing server
    subprocess.run(["pkill", "-f", "thunderduck"], capture_output=True)
    time.sleep(2)

    # Start server
    jar_path = "/opt/thunderduck/thunderduck.jar"
    if not os.path.exists(jar_path):
        print(f"Error: Thunderduck JAR not found at {jar_path}")
        return False

    cmd = [
        "java",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-jar", jar_path,
    ]

    with open("/tmp/thunderduck.log", "w") as log_file:
        subprocess.Popen(cmd, stdout=log_file, stderr=log_file)

    # Wait for server to start
    for i in range(30):
        result = subprocess.run(
            ["netstat", "-tlnp"],
            capture_output=True,
            text=True,
        )
        if ":15002" in result.stdout:
            print("Thunderduck server started on port 15002")
            return True
        time.sleep(1)

    print("Warning: Thunderduck server may not have started")
    return False


def start_spark_server(spark_version: str) -> bool:
    """Start Spark Connect Server."""
    print(f"Starting Spark Connect server (version {spark_version})...")

    # Kill any existing server
    subprocess.run(["pkill", "-f", "spark-connect"], capture_output=True)
    time.sleep(2)

    spark_home = f"/opt/spark-{spark_version}"
    if not os.path.exists(spark_home):
        print(f"Error: Spark not found at {spark_home}")
        return False

    cmd = [
        f"{spark_home}/sbin/start-connect-server.sh",
        f"--packages", f"org.apache.spark:spark-connect_2.13:{spark_version}",
        "--conf", "spark.master=local[*]",
        "--conf", "spark.driver.memory=8g",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.connect.grpc.binding.port=15002",
    ]

    with open("/tmp/spark-connect.log", "w") as log_file:
        subprocess.Popen(cmd, stdout=log_file, stderr=log_file, env={
            **os.environ,
            "SPARK_HOME": spark_home,
        })

    # Wait for server to start
    for i in range(60):
        result = subprocess.run(
            ["netstat", "-tlnp"],
            capture_output=True,
            text=True,
        )
        if ":15002" in result.stdout:
            print("Spark Connect server started on port 15002")
            return True
        time.sleep(1)

    print("Warning: Spark Connect server may not have started")
    return False


def load_tables(spark: SparkSession, data_dir: str, benchmark: str) -> Dict[str, Any]:
    """Load benchmark tables from parquet files.

    Args:
        spark: SparkSession
        data_dir: Directory containing parquet files
        benchmark: Benchmark name (tpch or tpcds)

    Returns:
        Dict mapping table name to DataFrame
    """
    benchmark_dir = Path(data_dir) / benchmark
    tables = {}

    for parquet_file in benchmark_dir.glob("*.parquet"):
        table_name = parquet_file.stem
        print(f"  Loading {table_name}...")
        df = spark.read.parquet(str(parquet_file))
        df.createOrReplaceTempView(table_name)
        tables[table_name] = df

    return tables


def run_query(
    query_fn,
    spark: SparkSession,
    tables: Dict[str, Any],
    runs: int,
) -> QueryResult:
    """Run a single query multiple times and collect timing.

    Args:
        query_fn: Query function to execute
        spark: SparkSession
        tables: Dict of DataFrames
        runs: Number of runs

    Returns:
        QueryResult with timing information
    """
    query_name = query_fn.__name__
    run_times = []
    row_count = 0
    error = None

    try:
        for run in range(runs):
            start_time = time.time()

            # Execute query and collect results
            result = query_fn(spark, tables)
            rows = result.collect()

            elapsed_ms = (time.time() - start_time) * 1000
            run_times.append(elapsed_ms)

            if run == 0:
                row_count = len(rows)

            print(f"    {query_name} run {run + 1}: {elapsed_ms:.1f}ms ({row_count} rows)")

        # Calculate average excluding warmup (first run)
        avg_time = sum(run_times[1:]) / len(run_times[1:]) if len(run_times) > 1 else run_times[0]

        return QueryResult(
            query_name=query_name,
            run_times_ms=run_times,
            avg_time_ms=avg_time,
            row_count=row_count,
            success=True,
        )

    except Exception as e:
        return QueryResult(
            query_name=query_name,
            run_times_ms=run_times,
            avg_time_ms=0,
            row_count=0,
            success=False,
            error=str(e),
        )


def run_benchmark(
    engine: str,
    benchmark: str,
    data_dir: str,
    runs: int,
) -> List[QueryResult]:
    """Run all queries for a benchmark.

    Args:
        engine: Engine name (thunderduck, spark-3.5.3, etc.)
        benchmark: Benchmark name (tpch or tpcds)
        data_dir: Directory containing benchmark data
        runs: Number of runs per query

    Returns:
        List of QueryResults
    """
    # Start appropriate server
    if engine == "thunderduck":
        if not start_thunderduck_server():
            return []
    elif engine.startswith("spark-"):
        spark_version = engine.replace("spark-", "")
        if not start_spark_server(spark_version):
            return []
    else:
        print(f"Unknown engine: {engine}")
        return []

    # Give server time to fully initialize
    time.sleep(5)

    # Create SparkSession connected to server
    print("Connecting to server...")
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Load tables
    print(f"Loading {benchmark.upper()} tables...")
    tables = load_tables(spark, data_dir, benchmark)
    print(f"Loaded {len(tables)} tables")

    # Import queries
    if benchmark == "tpch":
        from tpch_dataframe import TPCH_QUERIES as queries
    elif benchmark == "tpcds":
        from tpcds_dataframe import TPCDS_QUERIES as queries
    else:
        print(f"Unknown benchmark: {benchmark}")
        return []

    # Run queries
    results = []
    print(f"\nRunning {len(queries)} queries ({runs} runs each)...\n")

    for query_name, query_fn in queries.items():
        print(f"  Query: {query_name}")
        result = run_query(query_fn, spark, tables, runs)
        results.append(result)

        if not result.success:
            print(f"    ERROR: {result.error}")

    spark.stop()
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmark queries on EC2 instance",
    )
    parser.add_argument(
        "--engine",
        required=True,
        help="Engine to benchmark (thunderduck, spark-3.5.3, spark-4.0.1)",
    )
    parser.add_argument(
        "--benchmark",
        choices=["tpch", "tpcds", "all"],
        default="all",
        help="Which benchmark to run",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="Scale factor of data",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of runs per query",
    )
    parser.add_argument(
        "--data-dir",
        default="/tmp/data",
        help="Directory containing benchmark data",
    )

    args = parser.parse_args()

    benchmarks = []
    if args.benchmark in ["tpch", "all"]:
        benchmarks.append("tpch")
    if args.benchmark in ["tpcds", "all"]:
        benchmarks.append("tpcds")

    all_results = {
        "engine": args.engine,
        "scale_factor": args.scale_factor,
        "runs": args.runs,
        "benchmarks": {},
    }

    for benchmark in benchmarks:
        print(f"\n{'=' * 60}")
        print(f"Running {benchmark.upper()} benchmark on {args.engine}")
        print(f"{'=' * 60}\n")

        results = run_benchmark(
            args.engine,
            benchmark,
            args.data_dir,
            args.runs,
        )

        all_results["benchmarks"][benchmark] = [asdict(r) for r in results]

    # Output results as JSON (parsed by driver)
    print("\nRESULTS_JSON:" + json.dumps(all_results))

    return 0


if __name__ == "__main__":
    sys.exit(main())
