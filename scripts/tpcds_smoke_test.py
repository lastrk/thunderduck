#!/usr/bin/env python3
"""
Smoke test all TPC-DS queries to see which work with current Thunderduck implementation
"""

from pyspark.sql import SparkSession
from pathlib import Path
import time

def main():
    print("="*80)
    print("TPC-DS SMOKE TEST - Testing All Queries")
    print("="*80)

    # Create Spark session (local mode for smoke test)
    spark = SparkSession.builder.appName("TPCDSSmokeTest").getOrCreate()

    print(f"\n✓ Spark session created (version {spark.version})")

    # Load all TPC-DS tables
    data_dir = Path("/workspace/data/tpcds_sf1")
    tables = sorted([f.stem for f in data_dir.glob("*.parquet")])

    print(f"\nLoading {len(tables)} TPC-DS tables...")
    for table in tables:
        path = str(data_dir / f"{table}.parquet")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)
        if table in ["store_sales", "catalog_sales", "web_sales", "inventory"]:
            print(f"  ✓ {table}: {df.count():,} rows (large)")
        elif len(tables) < 30:  # Only print all if not too many
            print(f"  ✓ {table}")

    print(f"  ✓ All {len(tables)} tables loaded")

    # Test all queries
    queries_dir = Path("/workspace/benchmarks/tpcds_queries")
    query_files = sorted(queries_dir.glob("q*.sql"))

    print(f"\n{'='*80}")
    print(f"Testing {len(query_files)} TPC-DS queries")
    print(f"{'='*80}\n")

    results = {
        "success": [],
        "failed": {},
        "window_function": [],
        "other_errors": []
    }

    for query_file in query_files:
        query_name = query_file.stem

        try:
            query = query_file.read_text()

            # Quick check for window functions
            has_window = any(keyword in query.upper() for keyword in [
                "ROW_NUMBER()", "RANK()", "DENSE_RANK()", "OVER (", "PARTITION BY"
            ])

            start = time.time()
            result = spark.sql(query)
            row_count = result.count()
            elapsed = time.time() - start

            results["success"].append((query_name, row_count, elapsed))

            if query_name in [1, 10, 20, 30, 40, 50, 60, 70, 80, 90]:
                print(f"✓ {query_name}: {row_count} rows ({elapsed:.2f}s)")

        except Exception as e:
            error_msg = str(e)

            # Categorize error
            if "window" in error_msg.lower() or "over" in error_msg.lower():
                results["window_function"].append(query_name)
                cat = "WINDOW"
            elif "rollup" in error_msg.lower() or "cube" in error_msg.lower():
                results["other_errors"].append((query_name, "ROLLUP/CUBE"))
                cat = "ROLLUP/CUBE"
            else:
                # Store first 100 chars of error
                results["failed"][query_name] = error_msg[:100]
                cat = "OTHER"

            if query_name in ["q1", "q10", "q20", "q30", "q40", "q50"]:
                print(f"✗ {query_name}: {cat} - {error_msg[:60]}")

    # Summary
    print(f"\n{'='*80}")
    print("SMOKE TEST RESULTS")
    print(f"{'='*80}\n")

    total = len(query_files)
    success = len(results["success"])
    window = len(results["window_function"])
    failed = len(results["failed"])

    print(f"Total queries tested: {total}")
    print(f"  ✓ Working: {success} ({success*100//total}%)")
    print(f"  ✗ Need window functions: {window}")
    print(f"  ✗ Other failures: {failed}")

    if success > 0:
        print(f"\nWorking queries (ready for validation):")
        for qname, rows, elapsed in sorted(results["success"])[:20]:
            print(f"  ✓ {qname}: {rows} rows")
        if len(results["success"]) > 20:
            remaining = len(results["success"]) - 20
            print(f"  ... and {remaining} more")

    if window > 0:
        print(f"\nQueries needing window functions:")
        print(f"  {', '.join(sorted(results['window_function'])[:30])}")
        if len(results["window_function"]) > 30:
            remaining_win = len(results['window_function']) - 30
            print(f"  ... and {remaining_win} more")

    if failed > 0:
        print(f"\nOther failures (sample):")
        for qname, error in list(results["failed"].items())[:5]:
            print(f"  ✗ {qname}: {error}")

    print(f"\n{'='*80}")
    print(f"RECOMMENDATION:")
    print(f"{'='*80}")

    if success >= 30:
        print(f"✓ {success} queries work! Ready for Tier 1 validation.")
    elif success >= 20:
        print(f"✓ {success} queries work. Good foundation for validation.")
    else:
        print(f"⚠ Only {success} queries work. May need feature implementation first.")

    if window > 40:
        print(f"⚠ {window} queries need window functions (major feature gap)")

    print(f"\nNext step: Generate Spark reference data for {min(success, 30)} working queries")
    print(f"{'='*80}\n")

    spark.stop()

if __name__ == "__main__":
    main()
