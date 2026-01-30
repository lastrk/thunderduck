#!/usr/bin/env python3
"""
S3 Bucket Setup Script

Creates S3 bucket (if needed) and configures lifecycle policy for auto-expiration
of test data after a specified number of days.
"""
import argparse
import sys
from typing import Optional

import boto3
from botocore.exceptions import ClientError


def setup_s3_bucket(
    bucket_name: str,
    prefix: str = "thunderduck-benchmarks",
    expiration_days: int = 30,
    region: Optional[str] = None,
) -> bool:
    """Create S3 bucket and configure lifecycle policy for test data expiration.

    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix for test data (lifecycle policy applies to this prefix)
        expiration_days: Number of days after which objects expire
        region: AWS region (default: from session)

    Returns:
        True if bucket was created or already exists
    """
    session = boto3.session.Session()
    region = region or session.region_name or "us-east-1"
    s3 = boto3.client("s3", region_name=region)

    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            # Bucket doesn't exist, create it
            try:
                create_params = {"Bucket": bucket_name}
                # LocationConstraint is required for regions other than us-east-1
                if region != "us-east-1":
                    create_params["CreateBucketConfiguration"] = {
                        "LocationConstraint": region
                    }
                s3.create_bucket(**create_params)
                print(f"Created bucket '{bucket_name}' in {region}")
            except ClientError as create_error:
                print(f"Failed to create bucket: {create_error}")
                return False
        elif error_code == "403":
            print(f"Access denied to bucket '{bucket_name}'. Check permissions.")
            return False
        else:
            print(f"Error checking bucket: {e}")
            return False

    # Configure lifecycle policy for automatic expiration
    lifecycle_config = {
        "Rules": [
            {
                "ID": "ExpireTestData",
                "Status": "Enabled",
                "Filter": {"Prefix": prefix},
                "Expiration": {"Days": expiration_days},
                "NoncurrentVersionExpiration": {"NoncurrentDays": expiration_days},
            }
        ]
    }

    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config,
        )
        print(
            f"Configured lifecycle policy: objects under '{prefix}/' expire after {expiration_days} days"
        )
    except ClientError as e:
        print(f"Warning: Could not set lifecycle policy: {e}")
        # Don't fail if lifecycle policy can't be set

    return True


def check_data_exists(
    bucket_name: str,
    prefix: str,
    benchmark: str,
    scale_factor: int,
    region: Optional[str] = None,
) -> bool:
    """Check if benchmark data exists in S3.

    Args:
        bucket_name: S3 bucket name
        prefix: S3 prefix
        benchmark: Benchmark name (tpch or tpcds)
        scale_factor: Scale factor
        region: AWS region

    Returns:
        True if data exists
    """
    s3 = boto3.client("s3", region_name=region)

    # Check for a marker file or common table
    data_prefix = f"{prefix}/{benchmark}/sf{scale_factor}/"

    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=data_prefix,
            MaxKeys=1,
        )
        return response.get("KeyCount", 0) > 0
    except ClientError:
        return False


def list_available_data(
    bucket_name: str,
    prefix: str = "thunderduck-benchmarks",
    region: Optional[str] = None,
) -> None:
    """List available benchmark data in S3.

    Args:
        bucket_name: S3 bucket name
        prefix: S3 prefix
        region: AWS region
    """
    s3 = boto3.client("s3", region_name=region)

    try:
        # List all prefixes under the benchmark prefix
        paginator = s3.get_paginator("list_objects_v2")

        found_data = {}
        for benchmark in ["tpch", "tpcds"]:
            found_data[benchmark] = []

            for page in paginator.paginate(
                Bucket=bucket_name,
                Prefix=f"{prefix}/{benchmark}/",
                Delimiter="/",
            ):
                for common_prefix in page.get("CommonPrefixes", []):
                    # Extract scale factor from prefix like "thunderduck-benchmarks/tpch/sf100/"
                    sf_prefix = common_prefix["Prefix"]
                    sf = sf_prefix.split("/")[-2]  # Get "sf100" part
                    if sf.startswith("sf"):
                        found_data[benchmark].append(sf)

        print(f"\nAvailable benchmark data in s3://{bucket_name}/{prefix}/:\n")
        for benchmark, scale_factors in found_data.items():
            if scale_factors:
                print(f"  {benchmark.upper()}: {', '.join(sorted(scale_factors))}")
            else:
                print(f"  {benchmark.upper()}: (none)")

    except ClientError as e:
        print(f"Error listing data: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Setup S3 bucket for Thunderduck benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "bucket_name",
        nargs="?",
        help="S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        default="thunderduck-benchmarks",
        help="S3 prefix for test data (default: thunderduck-benchmarks)",
    )
    parser.add_argument(
        "--expiration-days",
        type=int,
        default=30,
        help="Days after which test data expires (default: 30)",
    )
    parser.add_argument(
        "--region",
        help="AWS region",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available benchmark data",
    )

    args = parser.parse_args()

    if args.list:
        if not args.bucket_name:
            parser.error("bucket_name required with --list")
        list_available_data(args.bucket_name, args.prefix, args.region)
        return 0

    if not args.bucket_name:
        parser.error("bucket_name required")

    success = setup_s3_bucket(
        args.bucket_name,
        args.prefix,
        args.expiration_days,
        args.region,
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
