"""
Generate Reference Results from Spark Local Mode

This script runs all TPC-H queries on Spark local mode and saves results
to be used as reference for correctness validation.
"""

from pyspark.sql import SparkSession
from pathlib import Path
import json


def generate_reference_results():
    """Generate reference results for all TPC-H queries"""

    print("=" * 80)
    print("Generating Spark Local Mode Reference Results")
    print("=" * 80)

    # Start Spark local mode
    spark = (SparkSession.builder
            .master("local[2]")
            .appName("TPC-H-Reference-Generator")
            .getOrCreate())

    data_dir = Path("/workspace/data/tpch_sf001")
    output_dir = Path("/workspace/tests/integration/expected_results")
    output_dir.mkdir(exist_ok=True)

    # Load all tables as temp views
    print("\nLoading TPC-H tables...")
    for table in ['customer', 'lineitem', 'nation', 'orders',
                  'part', 'partsupp', 'region', 'supplier']:
        df = spark.read.parquet(str(data_dir / f"{table}.parquet"))
        df.createOrReplaceTempView(table)
        print(f"  ✓ {table}")

    # Queries to generate references for
    queries = [1, 3, 5, 6, 10, 12, 13, 18]

    print("\nGenerating reference results...")
    for qnum in queries:
        query_file = Path(f"/workspace/benchmarks/tpch_queries/q{qnum}.sql")

        if not query_file.exists():
            print(f"  ✗ Q{qnum}: Query file not found")
            continue

        try:
            # Load and execute query
            sql = query_file.read_text()
            result = spark.sql(sql)
            rows = result.collect()

            # Convert to serializable format
            reference_data = []
            for row in rows:
                row_dict = row.asDict()
                # Convert types for JSON serialization
                converted = {}
                for k, v in row_dict.items():
                    if v is None:
                        converted[k] = None
                    elif isinstance(v, bool):
                        # Handle bool before int (bool is subclass of int)
                        converted[k] = v
                    elif isinstance(v, int):
                        # Keep integers as integers (don't convert to float!)
                        converted[k] = v
                    elif isinstance(v, float):
                        # Keep floats as floats
                        converted[k] = v
                    elif hasattr(v, '__float__'):  # Decimal only
                        # Only convert Decimal to float, not int
                        converted[k] = float(v)
                    elif hasattr(v, 'isoformat'):  # date, datetime
                        converted[k] = v.isoformat()
                    else:
                        converted[k] = v
                reference_data.append(converted)

            # Save to JSON
            output_file = output_dir / f"q{qnum}_spark_reference.json"
            with open(output_file, 'w') as f:
                json.dump({
                    'query_num': qnum,
                    'row_count': len(rows),
                    'columns': result.columns,
                    'rows': reference_data
                }, f, indent=2)

            print(f"  ✓ Q{qnum}: {len(rows)} rows → {output_file.name}")

        except Exception as e:
            print(f"  ✗ Q{qnum}: {str(e)[:100]}")

    spark.stop()

    print("\n" + "=" * 80)
    print("Reference generation complete")
    print(f"Saved to: {output_dir}")
    print("=" * 80)


if __name__ == "__main__":
    generate_reference_results()
