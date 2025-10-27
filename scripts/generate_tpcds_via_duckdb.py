#!/usr/bin/env python3
"""
Generate TPC-DS data using DuckDB's built-in TPC-DS extension
Then export to Parquet for use with Spark
"""

import duckdb
from pathlib import Path

def main():
    print("="*80)
    print("TPC-DS DATA GENERATION via DuckDB")
    print("="*80)

    # Create DuckDB connection
    conn = duckdb.connect(':memory:')

    # Install and load TPC-DS extension
    print("\n1. Installing TPC-DS extension...")
    conn.execute("INSTALL tpcds;")
    conn.execute("LOAD tpcds;")
    print("   ✓ TPC-DS extension loaded")

    # Generate TPC-DS data at scale factor 1 (1GB)
    print("\n2. Generating TPC-DS data (SF=1, ~1GB)...")
    print("   This may take a few minutes...")

    conn.execute("CALL dsdgen(sf=1);")
    print("   ✓ TPC-DS data generated in memory")

    # Get list of tables
    print("\n3. Checking generated tables...")
    # SHOW TABLES returns: (table_name,)
    tables_result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall()
    tables = [row[0] for row in tables_result]

    print(f"   ✓ Generated {len(tables)} tables:")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"     - {table}: {count:,} rows")

    # Export to Parquet
    output_dir = Path("/workspace/data/tpcds_sf1")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n4. Exporting to Parquet ({output_dir})...")
    total_size = 0
    for table in tables:
        output_file = output_dir / f"{table}.parquet"
        conn.execute(f"COPY {table} TO '{output_file}' (FORMAT PARQUET);")
        size_mb = output_file.stat().st_size / (1024 * 1024)
        total_size += size_mb
        print(f"   ✓ {table}.parquet ({size_mb:.2f} MB)")

    print(f"\n5. Summary:")
    print(f"   ✓ Tables: {len(tables)}")
    print(f"   ✓ Total size: {total_size:.2f} MB")
    print(f"   ✓ Location: {output_dir}")

    conn.close()

    print("\n" + "="*80)
    print("TPC-DS DATA GENERATION COMPLETE")
    print("="*80)
    print(f"\nData ready for TPC-DS query validation!")

if __name__ == "__main__":
    main()
