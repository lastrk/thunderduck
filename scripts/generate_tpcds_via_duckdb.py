#!/usr/bin/env python3
"""
Generate TPC-DS data using DuckDB's built-in TPC-DS extension
Then export to Parquet for use with Spark

Usage:
    python generate_tpcds_via_duckdb.py --sf 0.01 --output /path/to/output
    python generate_tpcds_via_duckdb.py -s 1  # Uses default output path
"""

import argparse
import duckdb
from pathlib import Path


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate TPC-DS data using DuckDB and export to Parquet"
    )
    parser.add_argument(
        "--sf", "-s",
        type=float,
        required=True,
        help="Scale factor for TPC-DS data generation (e.g., 0.01, 1, 10)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output directory for Parquet files (default: /workspace/data/tpcds_sf<SF>)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Determine output directory
    if args.output:
        output_dir = Path(args.output)
    else:
        # Default output path based on scale factor
        sf_str = str(args.sf).replace(".", "")
        output_dir = Path(f"/workspace/data/tpcds_sf{sf_str}")

    print("="*80)
    print("TPC-DS DATA GENERATION via DuckDB")
    print("="*80)
    print(f"Scale Factor: {args.sf}")
    print(f"Output Directory: {output_dir}")

    # Create DuckDB connection
    conn = duckdb.connect(':memory:')

    # Install and load TPC-DS extension
    print("\n1. Installing TPC-DS extension...")
    conn.execute("INSTALL tpcds;")
    conn.execute("LOAD tpcds;")
    print("   Done - TPC-DS extension loaded")

    # Generate TPC-DS data at specified scale factor
    print(f"\n2. Generating TPC-DS data (SF={args.sf})...")
    print("   This may take a few minutes...")

    conn.execute(f"CALL dsdgen(sf={args.sf});")
    print("   Done - TPC-DS data generated in memory")

    # Get list of tables
    print("\n3. Checking generated tables...")
    # SHOW TABLES returns: (table_name,)
    tables_result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()
    tables = [row[0] for row in tables_result]

    print(f"   Done - Generated {len(tables)} tables:")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"     - {table}: {count:,} rows")

    # Export to Parquet
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n4. Exporting to Parquet ({output_dir})...")
    total_size = 0
    for table in tables:
        output_file = output_dir / f"{table}.parquet"
        conn.execute(f"COPY {table} TO '{output_file}' (FORMAT PARQUET);")
        size_mb = output_file.stat().st_size / (1024 * 1024)
        total_size += size_mb
        print(f"   Done - {table}.parquet ({size_mb:.2f} MB)")

    print(f"\n5. Summary:")
    print(f"   Tables: {len(tables)}")
    print(f"   Total size: {total_size:.2f} MB")
    print(f"   Location: {output_dir}")

    conn.close()

    print("\n" + "="*80)
    print("TPC-DS DATA GENERATION COMPLETE")
    print("="*80)
    print(f"\nData ready for TPC-DS query validation!")

if __name__ == "__main__":
    main()
