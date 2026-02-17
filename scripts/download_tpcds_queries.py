#!/usr/bin/env python3
"""Download all 99 TPC-DS queries from databricks/spark-sql-perf"""

import urllib.request
from pathlib import Path


def main():
    base_url = "https://raw.githubusercontent.com/databricks/spark-sql-perf/master/src/main/resources/tpcds_2_4"
    output_dir = Path("/workspace/benchmarks/tpcds_queries")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading 99 TPC-DS queries...")
    success = 0
    failed = []

    for q in range(1, 100):
        url = f"{base_url}/q{q}.sql"
        output_file = output_dir / f"q{q}.sql"

        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                content = response.read().decode('utf-8')
                output_file.write_text(content)
                success += 1
                if q % 10 == 0:
                    print(f"  ✓ Downloaded Q1-Q{q}")
        except Exception:
            failed.append(q)

    print("\nResults:")
    print(f"  ✓ Successfully downloaded: {success}/99")
    if failed:
        print(f"  ✗ Failed: {len(failed)} queries")
        print(f"    {failed[:10]}{'...' if len(failed) > 10 else ''}")

    # Check total size
    total_size = sum(f.stat().st_size for f in output_dir.glob("q*.sql"))
    print(f"\nTotal size: {total_size / 1024:.1f} KB")
    print(f"Output directory: {output_dir}")

if __name__ == "__main__":
    main()
