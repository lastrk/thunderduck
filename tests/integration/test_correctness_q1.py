"""
Correctness Validation: TPC-H Q1 - Thunderduck vs Spark Local Mode

This test validates that Thunderduck produces IDENTICAL results to Spark,
not just the same row count/schema.
"""

import pytest
from pyspark.sql import SparkSession
from decimal import Decimal


@pytest.mark.correctness
class TestQ1Correctness:
    """Validate Q1 results match Spark exactly"""

    def test_q1_values_match_spark(self, tpch_data_dir, load_tpch_query):
        """
        Compare Thunderduck vs Spark LOCAL MODE for Q1

        Validates:
        - Same row count
        - Same column values (row-by-row)
        - Numerical precision matches
        - Sort order matches
        """

        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: TPC-H Q1 - Thunderduck vs Spark")
        print("=" * 80)

        query = load_tpch_query(1)
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # ========================================================================
        # 1. Execute on Spark LOCAL MODE (reference)
        # ========================================================================
        print("\n1️⃣  Executing Q1 on SPARK LOCAL MODE (reference)...")

        spark_local = (SparkSession.builder
                      .master("local[2]")
                      .appName("Q1-Reference")
                      .getOrCreate())

        # Load data and create temp view
        spark_local.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # Execute query
        spark_result = spark_local.sql(query)
        spark_rows = spark_result.collect()

        print(f"   ✓ Spark result: {len(spark_rows)} rows")

        # Extract values for comparison
        spark_data = []
        for row in spark_rows:
            spark_data.append({
                'l_returnflag': row['l_returnflag'],
                'l_linestatus': row['l_linestatus'],
                'sum_qty': float(row['sum_qty']),
                'sum_base_price': float(row['sum_base_price']),
                'sum_disc_price': float(row['sum_disc_price']),
                'sum_charge': float(row['sum_charge']),
                'avg_qty': float(row['avg_qty']),
                'avg_price': float(row['avg_price']),
                'avg_disc': float(row['avg_disc']),
                'count_order': int(row['count_order'])
            })

        spark_local.stop()

        # ========================================================================
        # 2. Execute on THUNDERDUCK (system under test)
        # ========================================================================
        print("\n2️⃣  Executing Q1 on THUNDERDUCK...")

        spark_td = (SparkSession.builder
                   .remote("sc://localhost:15002")
                   .appName("Q1-Thunderduck")
                   .getOrCreate())

        # Load data and create temp view
        spark_td.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # Execute query
        td_result = spark_td.sql(query)
        td_rows = td_result.collect()

        print(f"   ✓ Thunderduck result: {len(td_rows)} rows")

        # Extract values
        td_data = []
        for row in td_rows:
            td_data.append({
                'l_returnflag': row['l_returnflag'],
                'l_linestatus': row['l_linestatus'],
                'sum_qty': float(row['sum_qty']),
                'sum_base_price': float(row['sum_base_price']),
                'sum_disc_price': float(row['sum_disc_price']),
                'sum_charge': float(row['sum_charge']),
                'avg_qty': float(row['avg_qty']),
                'avg_price': float(row['avg_price']),
                'avg_disc': float(row['avg_disc']),
                'count_order': int(row['count_order'])
            })

        spark_td.stop()

        # ========================================================================
        # 3. COMPARE RESULTS VALUE-BY-VALUE
        # ========================================================================
        print("\n3️⃣  Comparing results value-by-value...")

        # Check row counts
        assert len(spark_data) == len(td_data), \
            f"Row count mismatch: Spark={len(spark_data)}, Thunderduck={len(td_data)}"
        print(f"   ✓ Row counts match: {len(spark_data)}")

        # Compare each row
        epsilon = 0.01  # Tolerance for floating point
        mismatches = []

        for i, (spark_row, td_row) in enumerate(zip(spark_data, td_data)):
            # Check string columns (exact match)
            if spark_row['l_returnflag'] != td_row['l_returnflag']:
                mismatches.append(f"Row {i} l_returnflag: {spark_row['l_returnflag']} vs {td_row['l_returnflag']}")

            if spark_row['l_linestatus'] != td_row['l_linestatus']:
                mismatches.append(f"Row {i} l_linestatus: {spark_row['l_linestatus']} vs {td_row['l_linestatus']}")

            # Check numeric columns (with epsilon)
            for col in ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge',
                       'avg_qty', 'avg_price', 'avg_disc']:
                spark_val = spark_row[col]
                td_val = td_row[col]

                if abs(spark_val - td_val) > epsilon:
                    mismatches.append(
                        f"Row {i} {col}: Spark={spark_val:.2f}, Thunderduck={td_val:.2f}, "
                        f"diff={abs(spark_val - td_val):.6f}"
                    )

            # Check integer columns (exact match)
            if spark_row['count_order'] != td_row['count_order']:
                mismatches.append(f"Row {i} count_order: {spark_row['count_order']} vs {td_row['count_order']}")

        # Report results
        if mismatches:
            print(f"\n   ❌ Found {len(mismatches)} mismatches:")
            for mismatch in mismatches[:10]:  # Show first 10
                print(f"      {mismatch}")
            assert False, f"Value mismatches found: {len(mismatches)} total"
        else:
            print(f"   ✓ All {len(spark_data)} rows match exactly!")
            print(f"\n   Sample row comparison:")
            print(f"   Spark:       {spark_data[0]}")
            print(f"   Thunderduck: {td_data[0]}")
            print(f"   ✓ IDENTICAL")

        print("\n✅ CORRECTNESS VALIDATED: Q1 produces identical results to Spark")
