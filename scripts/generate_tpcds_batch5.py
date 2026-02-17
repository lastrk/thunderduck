#!/usr/bin/env python3
"""Generate Spark reference data for TPC-DS Batch 5 (Q21-Q25)"""

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession


BATCH_5_QUERIES = [21, 22, '23a', '23b', '24a', '24b', 25]

def convert_row_to_json(row):
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
    print("TPC-DS Batch 5 (Q21-Q25)")
    print("="*80)

    spark = SparkSession.builder.appName("TPCDSBatch5").getOrCreate()

    data_dir = Path("/workspace/data/tpcds_sf1")
    for table_file in data_dir.glob("*.parquet"):
        df = spark.read.parquet(str(table_file))
        df.createOrReplaceTempView(table_file.stem)

    print("✓ Tables loaded\n")

    queries_dir = Path("/workspace/benchmarks/tpcds_queries")
    output_dir = Path("/workspace/tests/integration/expected_results")

    results = {}
    for qid in BATCH_5_QUERIES:
        try:
            query = (queries_dir / f"q{qid}.sql").read_text()
            print(f"Q{qid}:", end=" ")

            result = spark.sql(query)
            rows = result.collect()
            json_rows = [convert_row_to_json(row) for row in rows]

            output = {
                "query_num": str(qid),
                "row_count": len(rows),
                "columns": result.columns,
                "rows": json_rows
            }

            (output_dir / f"tpcds_q{qid}_spark_reference.json").write_text(json.dumps(output, indent=2))
            print(f"{len(rows)} rows ✓")
            results[qid] = True

        except Exception as e:
            print(f"FAILED - {str(e)[:60]}")
            results[qid] = False

    success = sum(1 for v in results.values() if v)
    print(f"\n✓ {success}/{len(BATCH_5_QUERIES)} successful\n")
    spark.stop()

if __name__ == "__main__":
    main()
