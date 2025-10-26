"""
Simplified Differential Testing: Spark Local vs Thunderduck

Tests queries without requiring temporary views (COMMAND not yet implemented).
Uses direct Parquet file paths in SQL queries.
"""

import pytest
from pyspark.sql import SparkSession
import time


@pytest.mark.differential
class TestSimpleDifferential:
    """Basic differential tests using direct file paths"""

    def test_simple_scan_differential(self, tpch_data_dir):
        """Compare simple scan query between Spark and Thunderduck"""

        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        print("\n" + "=" * 80)
        print("DIFFERENTIAL TEST: Simple Scan")
        print("=" * 80)

        # Test with Spark local
        print("\n1. Executing on Spark LOCAL mode...")
        spark_local = (SparkSession.builder
                      .master("local[2]")
                      .appName("Differential-Local")
                      .getOrCreate())

        start = time.time()
        spark_df = spark_local.read.parquet(lineitem_path)
        spark_count = spark_df.count()
        spark_time = time.time() - start

        # Get first few rows
        spark_rows = spark_df.limit(5).collect()
        spark_local.stop()

        print(f"   ✓ Spark: {spark_count:,} rows in {spark_time:.3f}s")

        # Test with Thunderduck
        print("\n2. Executing on THUNDERDUCK...")
        spark_td = (SparkSession.builder
                   .remote("sc://localhost:15002")
                   .appName("Differential-Thunderduck")
                   .getOrCreate())

        start = time.time()
        td_df = spark_td.read.parquet(lineitem_path)
        td_count = td_df.count()
        td_time = time.time() - start

        # Get first few rows
        td_rows = td_df.limit(5).collect()
        spark_td.stop()

        print(f"   ✓ Thunderduck: {td_count:,} rows in {td_time:.3f}s")

        # Compare
        print(f"\n3. Comparison:")
        print(f"   Spark count: {spark_count:,}")
        print(f"   Thunderduck count: {td_count:,}")
        print(f"   Match: {'✓' if spark_count == td_count else '✗'}")
        print(f"   Speedup: {spark_time/td_time:.2f}x")

        assert spark_count == td_count, f"Count mismatch: {spark_count} vs {td_count}"
        print("\n✓ TEST PASSED")


    def test_filter_aggregate_differential(self, tpch_data_dir):
        """Compare filter + aggregate between Spark and Thunderduck"""

        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        print("\n" + "=" * 80)
        print("DIFFERENTIAL TEST: Filter + Aggregate")
        print("=" * 80)

        # Test with Spark local
        print("\n1. Executing on Spark LOCAL mode...")
        spark_local = (SparkSession.builder
                      .master("local[2]")
                      .appName("Differential-Local")
                      .getOrCreate())

        start = time.time()
        spark_df = spark_local.read.parquet(lineitem_path)
        spark_result = spark_df.filter("l_quantity > 40").agg({"l_quantity": "sum"})
        spark_value = spark_result.collect()[0][0]
        spark_time = time.time() - start
        spark_local.stop()

        print(f"   ✓ Spark: sum = {spark_value:.2f} in {spark_time:.3f}s")

        # Test with Thunderduck
        print("\n2. Executing on THUNDERDUCK...")
        spark_td = (SparkSession.builder
                   .remote("sc://localhost:15002")
                   .appName("Differential-Thunderduck")
                   .getOrCreate())

        start = time.time()
        td_df = spark_td.read.parquet(lineitem_path)
        td_result = td_df.filter("l_quantity > 40").agg({"l_quantity": "sum"})
        td_value = td_result.collect()[0][0]
        td_time = time.time() - start
        spark_td.stop()

        print(f"   ✓ Thunderduck: sum = {td_value:.2f} in {td_time:.3f}s")

        # Compare
        print(f"\n3. Comparison:")
        print(f"   Spark sum: {spark_value:.2f}")
        print(f"   Thunderduck sum: {td_value:.2f}")
        print(f"   Difference: {abs(spark_value - td_value):.6f}")
        print(f"   Match: {'✓' if abs(spark_value - td_value) < 0.01 else '✗'}")
        print(f"   Speedup: {spark_time/td_time:.2f}x")

        assert abs(spark_value - td_value) < 0.01, \
            f"Value mismatch: {spark_value} vs {td_value}"
        print("\n✓ TEST PASSED")


    def test_groupby_differential(self, tpch_data_dir):
        """Compare group by between Spark and Thunderduck"""

        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        print("\n" + "=" * 80)
        print("DIFFERENTIAL TEST: Group By + Aggregate")
        print("=" * 80)

        # Test with Spark local
        print("\n1. Executing on Spark LOCAL mode...")
        spark_local = (SparkSession.builder
                      .master("local[2]")
                      .appName("Differential-Local")
                      .getOrCreate())

        start = time.time()
        spark_df = spark_local.read.parquet(lineitem_path)
        spark_result = spark_df.groupBy("l_returnflag").count().orderBy("l_returnflag")
        spark_rows = spark_result.collect()
        spark_time = time.time() - start
        spark_local.stop()

        print(f"   ✓ Spark: {len(spark_rows)} groups in {spark_time:.3f}s")
        for row in spark_rows[:3]:
            print(f"      {row['l_returnflag']}: {row['count']}")

        # Test with Thunderduck
        print("\n2. Executing on THUNDERDUCK...")
        spark_td = (SparkSession.builder
                   .remote("sc://localhost:15002")
                   .appName("Differential-Thunderduck")
                   .getOrCreate())

        start = time.time()
        td_df = spark_td.read.parquet(lineitem_path)
        td_result = td_df.groupBy("l_returnflag").count().orderBy("l_returnflag")
        td_rows = td_result.collect()
        td_time = time.time() - start
        spark_td.stop()

        print(f"   ✓ Thunderduck: {len(td_rows)} groups in {td_time:.3f}s")
        for row in td_rows[:3]:
            print(f"      {row['l_returnflag']}: {row['count']}")

        # Compare
        print(f"\n3. Comparison:")
        print(f"   Spark groups: {len(spark_rows)}")
        print(f"   Thunderduck groups: {len(td_rows)}")
        print(f"   Match: {'✓' if len(spark_rows) == len(td_rows) else '✗'}")
        print(f"   Speedup: {spark_time/td_time:.2f}x")

        assert len(spark_rows) == len(td_rows), \
            f"Group count mismatch: {len(spark_rows)} vs {len(td_rows)}"

        # Compare each group
        for spark_row, td_row in zip(spark_rows, td_rows):
            assert spark_row['l_returnflag'] == td_row['l_returnflag']
            assert spark_row['count'] == td_row['count']

        print("\n✓ TEST PASSED")
