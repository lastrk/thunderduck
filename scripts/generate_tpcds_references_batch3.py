#!/usr/bin/env python3
"""Generate Spark reference data for TPC-DS Batch 3 (Q11-Q15)"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession


BATCH_3_QUERIES = [11, 12, 13, '14a', '14b', 15]  # Note: Q14 has variants

def convert_row_to_json(row):
    """Convert PySpark Row to JSON-serializable dict"""
    result = {}
    for k, v in row.asDict().items():
        if v is None:
            result[k] = None
        elif isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, date):
            result[k] = v.isoformat()
        elif isinstance(v, int):
            result[k] = v
        elif isinstance(v, float):
            result[k] = v
        elif isinstance(v, str):
            result[k] = v
        else:
            result[k] = str(v)
    return result

def main():
    print("="*80)
    print("TPC-DS REFERENCE GENERATION - Batch 3 (Q11-Q15)")
    print("="*80)

    spark = SparkSession.builder.appName("TPCDSRefBatch3").getOrCreate()
    print(f"\n✓ Spark session created (version {spark.version})")

    # Load TPC-DS tables
    data_dir = Path("/workspace/data/tpcds_sf1")
    tables = sorted([f.stem for f in data_dir.glob("*.parquet")])

    print(f"\nLoading {len(tables)} TPC-DS tables...")
    for table in tables:
        path = str(data_dir / f"{table}.parquet")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)
    print(f"  ✓ All {len(tables)} tables loaded")

    # Generate references
    queries_dir = Path("/workspace/benchmarks/tpcds_queries")
    output_dir = Path("/workspace/tests/integration/expected_results")

    # Test Q11-Q15
    test_queries = [11, 12, 13, '14a', '14b', 15]

    print(f"\n{'='*80}")
    print(f"Generating references for {len(test_queries)} queries")
    print(f"{'='*80}")

    results = {}
    for query_id in test_queries:
        try:
            query_file = queries_dir / f"q{query_id}.sql"
            query = query_file.read_text()

            print(f"\nQ{query_id}:")
            result = spark.sql(query)
            rows = result.collect()

            json_rows = [convert_row_to_json(row) for row in rows]

            output = {
                "query_num": str(query_id),
                "row_count": len(rows),
                "columns": result.columns,
                "rows": json_rows
            }

            output_file = output_dir / f"tpcds_q{query_id}_spark_reference.json"
            with open(output_file, 'w') as f:
                json.dump(output, f, indent=2)

            print(f"  ✓ {len(rows)} rows")
            print("  ✓ Saved")
            results[query_id] = {"success": True, "rows": len(rows)}

        except Exception as e:
            print(f"  ✗ FAILED: {str(e)[:100]}")
            results[query_id] = {"success": False, "error": str(e)[:100]}

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    success = sum(1 for r in results.values() if r.get("success"))
    print(f"\n✓ Successful: {success}/{len(test_queries)}")

    if success > 0:
        print("\nReady for validation:")
        for qid in test_queries:
            if results.get(qid, {}).get("success"):
                print(f"  TPC-DS Q{qid}: {results[qid]['rows']} rows")

    print(f"\n{'='*80}\n")
    spark.stop()

if __name__ == "__main__":
    main()
