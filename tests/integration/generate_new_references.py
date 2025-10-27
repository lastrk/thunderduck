#!/usr/bin/env python3
"""
Generate Spark reference data for TPC-H queries Q2, Q4, Q7-Q11, Q14-Q17, Q19-Q22
"""

import json
from pathlib import Path
from pyspark.sql import SparkSession
from decimal import Decimal
from datetime import date

# Queries to generate references for
NEW_QUERIES = [2, 4, 7, 8, 9, 11, 14, 15, 16, 17, 19, 20, 21, 22]

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

def generate_reference(spark, query_num):
    """Generate reference data for a single query"""
    queries_dir = Path("/workspace/benchmarks/tpch_queries")
    data_dir = Path("/workspace/data/tpch_sf001")
    output_dir = Path("/workspace/tests/integration/expected_results")

    # Load query
    query_file = queries_dir / f"q{query_num}.sql"
    with open(query_file) as f:
        query = f.read()

    print(f"\n{'='*80}")
    print(f"Generating reference for Q{query_num}")
    print(f"{'='*80}")

    # Execute query
    try:
        result = spark.sql(query)
        rows = result.collect()

        # Convert to JSON
        json_rows = [convert_row_to_json(row) for row in rows]

        # Save reference
        output = {
            "query_num": query_num,
            "row_count": len(rows),
            "columns": result.columns,
            "rows": json_rows
        }

        output_file = output_dir / f"q{query_num}_spark_reference.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"✓ Generated reference: {len(rows)} rows")
        print(f"✓ Columns: {', '.join(result.columns)}")
        print(f"✓ Saved to: {output_file}")

        return True, len(rows)

    except Exception as e:
        print(f"✗ FAILED: {str(e)}")
        return False, 0

def main():
    print("="*80)
    print("SPARK REFERENCE DATA GENERATION - 14 New TPC-H Queries")
    print("="*80)

    # Create Spark session (local mode)
    spark = (SparkSession.builder
             .appName("TPCHReferenceGeneration")
             .config("spark.sql.adaptive.enabled", "false")  # Consistent with Spark Connect
             .getOrCreate())

    print(f"\n✓ Spark session created (version {spark.version})")

    # Load all TPC-H tables
    data_dir = Path("/workspace/data/tpch_sf001")
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

    print(f"\nLoading {len(tables)} TPC-H tables...")
    for table in tables:
        path = str(data_dir / f"{table}.parquet")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)
        print(f"  ✓ {table}: {df.count()} rows")

    # Generate references
    print(f"\n{'='*80}")
    print(f"Generating references for {len(NEW_QUERIES)} queries")
    print(f"{'='*80}")

    results = {}
    for query_num in NEW_QUERIES:
        success, row_count = generate_reference(spark, query_num)
        results[query_num] = {"success": success, "rows": row_count}

    # Summary
    print(f"\n{'='*80}")
    print("GENERATION SUMMARY")
    print(f"{'='*80}")

    successful = sum(1 for r in results.values() if r["success"])
    failed = len(results) - successful

    print(f"\nResults:")
    print(f"  ✓ Successful: {successful}/{len(NEW_QUERIES)}")
    print(f"  ✗ Failed: {failed}/{len(NEW_QUERIES)}")

    if successful > 0:
        print(f"\nSuccessful queries:")
        for qnum, result in results.items():
            if result["success"]:
                print(f"  Q{qnum}: {result['rows']} rows")

    if failed > 0:
        print(f"\nFailed queries:")
        for qnum, result in results.items():
            if not result["success"]:
                print(f"  Q{qnum}: FAILED")

    print(f"\n{'='*80}")
    print(f"Total reference files: {successful + 8}/22 (including 8 existing)")
    print(f"{'='*80}\n")

    spark.stop()

if __name__ == "__main__":
    main()
