#!/usr/bin/env python3
"""
Data Generation Tool

Generates TPC-H and TPC-DS data via DuckDB's built-in extensions.
Can upload generated data to S3 for reuse across benchmark runs.
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import duckdb

# TPC-H tables
TPCH_TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

# TPC-DS tables (subset that are commonly used)
TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def generate_tpch(scale_factor: int, output_dir: str) -> List[str]:
    """Generate TPC-H data using DuckDB's tpch extension.

    Args:
        scale_factor: TPC-H scale factor (1 = 1GB, 10 = 10GB, etc.)
        output_dir: Directory to write parquet files

    Returns:
        List of generated file paths
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-H data at SF{scale_factor}...")

    conn = duckdb.connect()
    conn.execute("INSTALL tpch; LOAD tpch;")
    conn.execute(f"CALL dbgen(sf={scale_factor});")

    generated_files = []
    for table in TPCH_TABLES:
        file_path = output_path / f"{table}.parquet"
        print(f"  Exporting {table}...")
        conn.execute(f"COPY {table} TO '{file_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")
        generated_files.append(str(file_path))

    conn.close()
    print(f"TPC-H data generated at {output_dir}")
    return generated_files


def generate_tpcds(scale_factor: int, output_dir: str) -> List[str]:
    """Generate TPC-DS data using DuckDB's tpcds extension.

    Args:
        scale_factor: TPC-DS scale factor (1 = 1GB, 10 = 10GB, etc.)
        output_dir: Directory to write parquet files

    Returns:
        List of generated file paths
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-DS data at SF{scale_factor}...")

    conn = duckdb.connect()
    conn.execute("INSTALL tpcds; LOAD tpcds;")
    conn.execute(f"CALL dsdgen(sf={scale_factor});")

    generated_files = []
    for table in TPCDS_TABLES:
        file_path = output_path / f"{table}.parquet"
        print(f"  Exporting {table}...")
        try:
            conn.execute(f"COPY {table} TO '{file_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")
            generated_files.append(str(file_path))
        except Exception as e:
            print(f"  Warning: Could not export {table}: {e}")

    conn.close()
    print(f"TPC-DS data generated at {output_dir}")
    return generated_files


def sync_to_s3(
    local_dir: str,
    s3_bucket: str,
    s3_prefix: str,
    benchmark: str,
    scale_factor: int,
) -> bool:
    """Upload generated data to S3.

    Args:
        local_dir: Local directory with parquet files
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix (e.g., "thunderduck-benchmarks")
        benchmark: Benchmark name (tpch or tpcds)
        scale_factor: Scale factor

    Returns:
        True if sync succeeded
    """
    s3_path = f"s3://{s3_bucket}/{s3_prefix}/{benchmark}/sf{scale_factor}/"

    print(f"Uploading data to {s3_path}...")

    cmd = [
        "aws", "s3", "sync",
        local_dir,
        s3_path,
        "--only-show-errors",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error uploading to S3: {result.stderr}")
        return False

    print(f"Data uploaded to {s3_path}")
    return True


def download_from_s3(
    s3_bucket: str,
    s3_prefix: str,
    benchmark: str,
    scale_factor: int,
    local_dir: str,
) -> bool:
    """Download data from S3.

    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix
        benchmark: Benchmark name (tpch or tpcds)
        scale_factor: Scale factor
        local_dir: Local directory to download to

    Returns:
        True if download succeeded
    """
    s3_path = f"s3://{s3_bucket}/{s3_prefix}/{benchmark}/sf{scale_factor}/"

    print(f"Downloading data from {s3_path}...")

    os.makedirs(local_dir, exist_ok=True)

    cmd = [
        "aws", "s3", "sync",
        s3_path,
        local_dir,
        "--only-show-errors",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error downloading from S3: {result.stderr}")
        return False

    print(f"Data downloaded to {local_dir}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Generate TPC-H/TPC-DS benchmark data using DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python generate_data.py --benchmark tpch --scale-factor 100 --output-dir /tmp/data/tpch
    python generate_data.py --benchmark tpcds --scale-factor 10 --output-dir /tmp/data/tpcds
    python generate_data.py --benchmark tpch --scale-factor 100 --upload-to-s3 my-bucket
        """,
    )

    parser.add_argument(
        "--benchmark",
        choices=["tpch", "tpcds", "all"],
        default="all",
        help="Which benchmark data to generate (default: all)",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        required=True,
        help="Scale factor (1 = 1GB, 10 = 10GB, etc.)",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/benchmark-data",
        help="Directory to write data (default: /tmp/benchmark-data)",
    )
    parser.add_argument(
        "--upload-to-s3",
        metavar="BUCKET",
        help="Upload generated data to S3 bucket",
    )
    parser.add_argument(
        "--s3-prefix",
        default="thunderduck-benchmarks",
        help="S3 prefix (default: thunderduck-benchmarks)",
    )

    args = parser.parse_args()

    benchmarks = []
    if args.benchmark in ["tpch", "all"]:
        benchmarks.append("tpch")
    if args.benchmark in ["tpcds", "all"]:
        benchmarks.append("tpcds")

    for benchmark in benchmarks:
        output_dir = os.path.join(args.output_dir, benchmark)

        if benchmark == "tpch":
            generate_tpch(args.scale_factor, output_dir)
        else:
            generate_tpcds(args.scale_factor, output_dir)

        if args.upload_to_s3:
            sync_to_s3(
                output_dir,
                args.upload_to_s3,
                args.s3_prefix,
                benchmark,
                args.scale_factor,
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
