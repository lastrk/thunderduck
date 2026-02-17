#!/usr/bin/env python3
"""Generate Spark reference data for TPC-DS Batch 4 (Q16-Q20)"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession


BATCH_4_QUERIES = [16, 17, 18, 19, 20]

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
    print("TPC-DS REFERENCE GENERATION - Batch 4 (Q16-Q20)")
    print("="*80)

    spark = SparkSession.builder.appName("TPCDSRefBatch4").getOrCreate()
    print("\n✓ Spark session created")

    # Load TPC-DS tables
    data_dir = Path("/workspace/data/tpcds_sf1")
    tables = sorted([f.stem for f in data_dir.glob("*.parquet")])

    print(f"Loading {len(tables)} TPC-DS tables...")
    for table in tables:
        path = str(data_dir / f"{table}.parquet")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)
    print("  ✓ All loaded")

    # Generate references
    queries_dir = Path("/workspace/benchmarks/tpcds_queries")
    output_dir = Path("/workspace/tests/integration/expected_results")

    print(f"\n{'='*80}")
    print(f"Generating references for {len(BATCH_4_QUERIES)} queries")
    print(f"{'='*80}")

    results = {}
    for query_num in BATCH_4_QUERIES:
        try:
            query_file = queries_dir / f"q{query_num}.sql"
            query = query_file.read_text()

            print(f"\nQ{query_num}:", end=" ")
            result = spark.sql(query)
            rows = result.collect()

            json_rows = [convert_row_to_json(row) for row in rows]

            output = {
                "query_num": query_num,
                "row_count": len(rows),
                "columns": result.columns,
                "rows": json_rows
            }

            output_file = output_dir / f"tpcds_q{query_num}_spark_reference.json"
            with open(output_file, 'w') as f:
                json.dump(output, f, indent=2)

            print(f"{len(rows)} rows ✓")
            results[query_num] = {"success": True, "rows": len(rows)}

        except Exception as e:
            print(f"FAILED - {str(e)[:80]}")
            results[query_num] = {"success": False}

    # Summary
    success = sum(1 for r in results.values() if r.get("success"))
    print(f"\n{'='*80}")
    print(f"✓ Successful: {success}/{len(BATCH_4_QUERIES)}")
    print(f"{'='*80}\n")

    spark.stop()

if __name__ == "__main__":
    main()
