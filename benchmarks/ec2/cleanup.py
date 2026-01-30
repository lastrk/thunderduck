#!/usr/bin/env python3
"""
EC2 Cleanup Script

Cleanup script to terminate Thunderduck benchmark resources.

Usage:
    # CDK-managed infrastructure (recommended)
    python cleanup.py --destroy-stack ThunderduckBenchmark

    # Legacy instance cleanup (for manually tagged instances)
    python cleanup.py                    # List all benchmark instances
    python cleanup.py --terminate        # Terminate all benchmark instances
    python cleanup.py --run-id abc123    # Terminate instances from specific run
    python cleanup.py --older-than 24h   # Terminate instances older than 24 hours
"""
import argparse
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional

import boto3

TAG_KEY = "thunderduck-benchmark"
TAG_VALUE = "true"


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string like '24h', '2d', '30m' into timedelta."""
    match = re.match(r"^(\d+)([hdm])$", duration_str.lower())
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}. Use format like '24h', '2d', '30m'")

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    elif unit == "m":
        return timedelta(minutes=value)
    else:
        raise ValueError(f"Unknown unit: {unit}")


def find_benchmark_instances(
    run_id: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict]:
    """Find all EC2 instances tagged as benchmark instances.

    Args:
        run_id: Optional run ID to filter by
        region: AWS region (default: from environment)

    Returns:
        List of instance info dicts
    """
    ec2 = boto3.client("ec2", region_name=region)

    filters = [
        {"Name": f"tag:{TAG_KEY}", "Values": [TAG_VALUE]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
    ]

    if run_id:
        filters.append({"Name": "tag:thunderduck-bench-run-id", "Values": [run_id]})

    response = ec2.describe_instances(Filters=filters)
    instances = []

    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
            instances.append({
                "instance_id": instance["InstanceId"],
                "state": instance["State"]["Name"],
                "launch_time": instance["LaunchTime"],
                "instance_type": instance["InstanceType"],
                "name": tags.get("Name", ""),
                "run_id": tags.get("thunderduck-bench-run-id", ""),
                "engine": tags.get("thunderduck-bench-engine", ""),
                "ip": instance.get("PublicIpAddress") or instance.get("PrivateIpAddress", "N/A"),
            })

    return instances


def terminate_instances(
    instance_ids: List[str],
    dry_run: bool = False,
    region: Optional[str] = None,
) -> None:
    """Terminate specified instances.

    Args:
        instance_ids: List of EC2 instance IDs to terminate
        dry_run: If True, only print what would be done
        region: AWS region
    """
    if not instance_ids:
        print("No instances to terminate")
        return

    ec2 = boto3.client("ec2", region_name=region)

    if dry_run:
        print(f"[DRY RUN] Would terminate {len(instance_ids)} instances:")
        for iid in instance_ids:
            print(f"  - {iid}")
        return

    print(f"Terminating {len(instance_ids)} instances...")
    ec2.terminate_instances(InstanceIds=instance_ids)

    # Wait for termination
    waiter = ec2.get_waiter("instance_terminated")
    waiter.wait(InstanceIds=instance_ids)

    print(f"Terminated: {instance_ids}")


def cleanup_old_instances(
    hours: Optional[int] = None,
    duration: Optional[timedelta] = None,
    dry_run: bool = False,
    region: Optional[str] = None,
) -> List[Dict]:
    """Terminate benchmark instances older than specified duration.

    Args:
        hours: Number of hours (deprecated, use duration)
        duration: Time duration
        dry_run: If True, only print what would be done
        region: AWS region

    Returns:
        List of instances that were (or would be) terminated
    """
    if duration is None and hours is not None:
        duration = timedelta(hours=hours)
    elif duration is None:
        duration = timedelta(hours=24)

    cutoff = datetime.now(timezone.utc) - duration
    instances = find_benchmark_instances(region=region)
    old_instances = [i for i in instances if i["launch_time"] < cutoff]

    if old_instances:
        instance_ids = [i["instance_id"] for i in old_instances]
        terminate_instances(instance_ids, dry_run=dry_run, region=region)

    return old_instances


def destroy_cdk_stack(
    stack_name: str,
    region: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Destroy a CDK-managed benchmark stack.

    Args:
        stack_name: Name of the CDK stack to destroy
        region: AWS region
        dry_run: If True, only print what would be done

    Returns:
        Exit code from cdk destroy command
    """
    cdk_dir = Path(__file__).parent.parent / "cdk"

    if not cdk_dir.exists():
        print(f"Error: CDK directory not found at {cdk_dir}")
        return 1

    # Build the cdk destroy command (using npx to avoid global install)
    cmd = ["npx", "aws-cdk", "destroy", stack_name, "--force"]

    if region:
        # Set region via environment for CDK
        env = os.environ.copy()
        env["CDK_DEFAULT_REGION"] = region
    else:
        env = None

    if dry_run:
        print(f"[DRY RUN] Would run: {' '.join(cmd)}")
        print(f"[DRY RUN] Working directory: {cdk_dir}")
        return 0

    print(f"Destroying CDK stack '{stack_name}'...")
    print(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=cdk_dir,
            env=env,
            check=False,
        )
        if result.returncode == 0:
            print(f"Stack '{stack_name}' destroyed successfully")
        else:
            print(f"CDK destroy failed with exit code {result.returncode}")
        return result.returncode
    except FileNotFoundError:
        print("Error: 'npx' command not found. Install Node.js: https://nodejs.org/")
        return 1
    except Exception as e:
        print(f"Error running cdk destroy: {e}")
        return 1


def list_cdk_stacks(region: Optional[str] = None) -> List[str]:
    """List CDK stacks that look like benchmark stacks.

    Args:
        region: AWS region

    Returns:
        List of stack names
    """
    cfn = boto3.client("cloudformation", region_name=region)

    try:
        response = cfn.list_stacks(
            StackStatusFilter=[
                "CREATE_COMPLETE",
                "UPDATE_COMPLETE",
                "ROLLBACK_COMPLETE",
            ]
        )
    except Exception as e:
        print(f"Error listing stacks: {e}")
        return []

    # Filter for likely benchmark stacks
    benchmark_stacks = []
    for stack in response.get("StackSummaries", []):
        name = stack["StackName"]
        # Look for stacks with "benchmark" or "thunderduck" in name
        if "benchmark" in name.lower() or "thunderduck" in name.lower():
            benchmark_stacks.append(name)

    return benchmark_stacks


def print_instances(instances: List[Dict]) -> None:
    """Print instance list in a formatted table."""
    if not instances:
        print("No benchmark instances found")
        return

    print(f"\nFound {len(instances)} benchmark instance(s):\n")
    print(f"{'Instance ID':<20} {'State':<10} {'Type':<12} {'Engine':<15} {'Run ID':<10} {'Age':<15} {'IP':<15}")
    print("-" * 100)

    now = datetime.now(timezone.utc)
    for inst in instances:
        age = now - inst["launch_time"]
        age_str = f"{int(age.total_seconds() // 3600)}h {int((age.total_seconds() % 3600) // 60)}m"

        print(
            f"{inst['instance_id']:<20} "
            f"{inst['state']:<10} "
            f"{inst['instance_type']:<12} "
            f"{inst['engine']:<15} "
            f"{inst['run_id']:<10} "
            f"{age_str:<15} "
            f"{inst['ip']:<15}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Cleanup Thunderduck benchmark EC2 resources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # CDK-managed infrastructure (recommended)
    python cleanup.py --destroy              # Destroy default stack (ThunderduckBenchmark)
    python cleanup.py --destroy-stack MyStack  # Destroy a specific stack
    python cleanup.py --list-stacks          # List benchmark stacks
    python cleanup.py --destroy --dry-run    # Show what would happen

    # Legacy instance cleanup (non-CDK)
    python cleanup.py                    # List all benchmark instances
    python cleanup.py --terminate        # Terminate all benchmark instances
    python cleanup.py --older-than 24h --terminate  # Terminate old instances
        """,
    )

    # CDK-related arguments
    cdk_group = parser.add_argument_group("CDK Stack Management")
    cdk_group.add_argument(
        "--destroy",
        action="store_true",
        help="Destroy the default CDK stack (ThunderduckBenchmark)",
    )
    cdk_group.add_argument(
        "--destroy-stack",
        metavar="STACK_NAME",
        help="Destroy a specific CDK stack by name",
    )
    cdk_group.add_argument(
        "--list-stacks",
        action="store_true",
        help="List CDK stacks that appear to be benchmark stacks",
    )

    # Legacy instance arguments
    instance_group = parser.add_argument_group("Legacy Instance Cleanup")
    instance_group.add_argument(
        "--terminate",
        action="store_true",
        help="Terminate matching instances (default: list only)",
    )
    instance_group.add_argument(
        "--run-id",
        help="Filter by run ID",
    )
    instance_group.add_argument(
        "--older-than",
        help="Filter instances older than duration (e.g., 24h, 2d, 30m)",
    )

    # Common arguments
    parser.add_argument(
        "--region",
        help="AWS region",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually executing",
    )

    args = parser.parse_args()

    # Handle CDK stack destruction
    if args.destroy or args.destroy_stack:
        stack_name = args.destroy_stack or "ThunderduckBenchmark"
        return destroy_cdk_stack(
            stack_name=stack_name,
            region=args.region,
            dry_run=args.dry_run,
        )

    # Handle listing CDK stacks
    if args.list_stacks:
        stacks = list_cdk_stacks(region=args.region)
        if stacks:
            print("Found benchmark-related CDK stacks:")
            for stack in stacks:
                print(f"  - {stack}")
            print(f"\nTo destroy a stack: python cleanup.py --destroy-stack <stack-name>")
        else:
            print("No benchmark-related CDK stacks found")
        return 0

    # Legacy instance cleanup
    # Find instances
    instances = find_benchmark_instances(run_id=args.run_id, region=args.region)

    # Filter by age if specified
    if args.older_than:
        duration = parse_duration(args.older_than)
        cutoff = datetime.now(timezone.utc) - duration
        instances = [i for i in instances if i["launch_time"] < cutoff]
        print(f"Filtering instances older than {args.older_than}")

    # Print instances
    print_instances(instances)

    # Terminate if requested
    if args.terminate and instances:
        print()
        if args.dry_run:
            print("[DRY RUN] Would terminate the above instances")
        else:
            confirm = input("Are you sure you want to terminate these instances? [y/N] ")
            if confirm.lower() == "y":
                instance_ids = [i["instance_id"] for i in instances]
                terminate_instances(instance_ids, region=args.region)
            else:
                print("Aborted")

    return 0


if __name__ == "__main__":
    sys.exit(main())
