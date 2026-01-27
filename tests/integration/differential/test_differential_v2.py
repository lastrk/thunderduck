"""
TPC-H Differential Testing V2: Apache Spark Connect vs Thunderduck

This test suite runs TPC-H queries on both:
1. Apache Spark 4.0.1 Connect (reference) - running in Podman container
2. Thunderduck Connect (test) - system under test

Results are compared row-by-row with detailed diff output on mismatch.

Key improvements over V1:
- Uses Podman container for Spark Connect (no manual install needed)
- Detailed row-by-row diff on mismatch
- Better error messages
- Session-scoped fixtures for performance
"""

import pytest
import time
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# TPC-H Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q1_Differential:
    """TPC-H Q1: Pricing Summary Report"""

    def test_q1_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q1"""
        query = load_tpch_query(1)

        # Execute on Spark reference
        print("\n" + "=" * 80)
        print("Executing Q1 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        # Execute on Thunderduck
        print("\nExecuting Q1 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Compare results with detailed diff
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Assert equality with detailed diff on failure
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q1",
            epsilon=1e-6,
            max_diff_rows=5
        )


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q3_Differential:
    """TPC-H Q3: Shipping Priority"""

    def test_q3_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q3"""
        query = load_tpch_query(3)

        # Execute on both systems
        print("\n" + "=" * 80)
        print("Executing Q3 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        print("\nExecuting Q3 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q3",
            epsilon=1e-6
        )


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q6_Differential:
    """TPC-H Q6: Forecasting Revenue Change"""

    def test_q6_differential(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for Q6"""
        query = load_tpch_query(6)

        # Execute on both systems
        print("\n" + "=" * 80)
        print("Executing Q6 on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        print("\nExecuting Q6 on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name="TPC-H Q6",
            epsilon=1e-6
        )


# ============================================================================
# Parameterized Tests for All TPC-H Queries
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
@pytest.mark.parametrize("query_num", [
    2, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
])
class TestTPCH_AllQueries_Differential:
    """Differential tests for all other TPC-H queries"""

    def test_query_differential(
        self,
        query_num,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        load_tpch_query
    ):
        """Compare Spark and Thunderduck results for query N"""
        query = load_tpch_query(query_num)

        # Execute on Spark reference
        print("\n" + "=" * 80)
        print(f"Executing Q{query_num} on Spark Reference...")
        start_ref = time.time()
        reference_result = spark_reference.sql(query)
        ref_time = time.time() - start_ref
        print(f"✓ Spark Reference completed in {ref_time:.3f}s")

        # Execute on Thunderduck
        print(f"\nExecuting Q{query_num} on Thunderduck...")
        start_td = time.time()
        test_result = spark_thunderduck.sql(query)
        td_time = time.time() - start_td
        print(f"✓ Thunderduck completed in {td_time:.3f}s")

        # Performance summary
        print(f"\nPerformance:")
        print(f"  Spark Reference: {ref_time:.3f}s")
        print(f"  Thunderduck:     {td_time:.3f}s")
        if td_time > 0:
            print(f"  Speedup:         {ref_time/td_time:.2f}x")

        # Compare with diff
        assert_dataframes_equal(
            reference_result,
            test_result,
            query_name=f"TPC-H Q{query_num}",
            epsilon=1e-6,
            max_diff_rows=5
        )


# ============================================================================
# Quick Sanity Test
# ============================================================================

@pytest.mark.differential
@pytest.mark.quick
class TestDifferential_Sanity:
    """Quick sanity test to verify differential framework is working"""

    def test_simple_select(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck
    ):
        """Test simple SELECT query"""
        query = "SELECT COUNT(*) as cnt FROM lineitem"

        print("\n" + "=" * 80)
        print("Sanity Test: Simple SELECT COUNT(*)")
        print("=" * 80)

        # Execute on both
        ref_result = spark_reference.sql(query)
        test_result = spark_thunderduck.sql(query)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="Sanity: SELECT COUNT(*)"
        )

        print("✓ Differential framework is working correctly!")


# ============================================================================
# TPC-H DataFrame API Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
@pytest.mark.dataframe
class TestTPCH_DataFrame_Differential:
    """
    TPC-H queries implemented using DataFrame API for differential testing.

    These tests verify that DataFrame operations (filter, groupBy, agg, join, etc.)
    produce identical results on both Spark Reference and Thunderduck.
    """

    def test_q1_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """
        TPC-H Q1 via DataFrame API: Pricing Summary Report
        Tests: filter, groupBy, agg (sum, avg, count), orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-H Q1 DataFrame API: Pricing Summary Report")
        print("=" * 80)

        def build_q1(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return (lineitem
                .filter(F.col("l_shipdate") <= "1998-09-02")
                .groupBy("l_returnflag", "l_linestatus")
                .agg(
                    F.sum("l_quantity").alias("sum_qty"),
                    F.sum("l_extendedprice").alias("sum_base_price"),
                    F.sum(F.col("l_extendedprice") * (F.lit(1) - F.col("l_discount")))
                        .alias("sum_disc_price"),
                    F.sum(F.col("l_extendedprice") *
                          (F.lit(1) - F.col("l_discount")) *
                          (F.lit(1) + F.col("l_tax")))
                        .alias("sum_charge"),
                    F.avg("l_quantity").alias("avg_qty"),
                    F.avg("l_extendedprice").alias("avg_price"),
                    F.avg("l_discount").alias("avg_disc"),
                    F.count("*").alias("count_order")
                )
                .orderBy("l_returnflag", "l_linestatus")
            )

        # Execute on both
        ref_result = build_q1(spark_reference)
        test_result = build_q1(spark_thunderduck)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-H Q1 DataFrame",
            epsilon=1e-6
        )

    def test_q3_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """
        TPC-H Q3 via DataFrame API: Shipping Priority
        Tests: multi-table join, filter, groupBy, agg, orderBy, limit
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-H Q3 DataFrame API: Shipping Priority")
        print("=" * 80)

        def build_q3(spark):
            customer = spark.read.parquet(str(tpch_data_dir / "customer.parquet"))
            orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

            return (lineitem
                .join(orders, lineitem["l_orderkey"] == orders["o_orderkey"])
                .join(customer, orders["o_custkey"] == customer["c_custkey"])
                .filter(
                    (F.col("c_mktsegment") == "BUILDING") &
                    (F.col("o_orderdate") < "1995-03-15") &
                    (F.col("l_shipdate") > "1995-03-15")
                )
                .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
                .agg(
                    F.sum(F.col("l_extendedprice") * (F.lit(1) - F.col("l_discount")))
                        .alias("revenue")
                )
                .orderBy(F.col("revenue").desc(), "o_orderdate")
                .limit(10)
            )

        # Execute on both
        ref_result = build_q3(spark_reference)
        test_result = build_q3(spark_thunderduck)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-H Q3 DataFrame",
            epsilon=1e-6
        )

    def test_q6_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """
        TPC-H Q6 via DataFrame API: Forecasting Revenue Change
        Tests: filter with multiple conditions, agg (sum)
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-H Q6 DataFrame API: Forecasting Revenue Change")
        print("=" * 80)

        def build_q6(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

            return (lineitem
                .filter(
                    (F.col("l_shipdate") >= "1994-01-01") &
                    (F.col("l_shipdate") < "1995-01-01") &
                    (F.col("l_discount") >= 0.05) &
                    (F.col("l_discount") <= 0.07) &
                    (F.col("l_quantity") < 24)
                )
                .agg(
                    F.sum(F.col("l_extendedprice") * F.col("l_discount"))
                        .alias("revenue")
                )
            )

        # Execute on both
        ref_result = build_q6(spark_reference)
        test_result = build_q6(spark_thunderduck)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-H Q6 DataFrame",
            epsilon=1e-6
        )

    def test_q12_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """
        TPC-H Q12 via DataFrame API: Shipping Modes and Order Priority
        Tests: join, filter with IN, conditional aggregation (when/otherwise)
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-H Q12 DataFrame API: Shipping Modes and Order Priority")
        print("=" * 80)

        def build_q12(spark):
            orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

            return (lineitem
                .join(orders, lineitem["l_orderkey"] == orders["o_orderkey"])
                .filter(
                    (F.col("l_shipmode").isin("MAIL", "SHIP")) &
                    (F.col("l_commitdate") < F.col("l_receiptdate")) &
                    (F.col("l_shipdate") < F.col("l_commitdate")) &
                    (F.col("l_receiptdate") >= "1994-01-01") &
                    (F.col("l_receiptdate") < "1995-01-01")
                )
                .groupBy("l_shipmode")
                .agg(
                    F.sum(F.when(
                        F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1
                    ).otherwise(0)).alias("high_line_count"),
                    F.sum(F.when(
                        ~F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1
                    ).otherwise(0)).alias("low_line_count")
                )
                .orderBy("l_shipmode")
            )

        # Execute on both
        ref_result = build_q12(spark_reference)
        test_result = build_q12(spark_thunderduck)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-H Q12 DataFrame",
            epsilon=1e-6
        )

    def test_q5_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """
        TPC-H Q5 via DataFrame API: Local Supplier Volume
        Tests: 6-way join, complex predicates, group by, aggregate
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-H Q5 DataFrame API: Local Supplier Volume")
        print("=" * 80)

        def build_q5(spark):
            region = spark.read.parquet(str(tpch_data_dir / "region.parquet"))
            nation = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            customer = spark.read.parquet(str(tpch_data_dir / "customer.parquet"))
            orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            supplier = spark.read.parquet(str(tpch_data_dir / "supplier.parquet"))

            return (region
                .filter(F.col("r_name") == "ASIA")
                .join(nation, F.col("r_regionkey") == F.col("n_regionkey"))
                .join(customer, F.col("n_nationkey") == F.col("c_nationkey"))
                .join(orders, F.col("c_custkey") == F.col("o_custkey"))
                .filter(
                    (F.col("o_orderdate") >= "1994-01-01") &
                    (F.col("o_orderdate") < "1995-01-01")
                )
                .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                .join(
                    supplier,
                    (F.col("l_suppkey") == F.col("s_suppkey")) &
                    (F.col("s_nationkey") == F.col("n_nationkey"))
                )
                .groupBy("n_name")
                .agg(
                    F.sum(F.col("l_extendedprice") * (F.lit(1) - F.col("l_discount")))
                        .alias("revenue")
                )
                .orderBy(F.col("revenue").desc())
            )

        # Execute on both
        ref_result = build_q5(spark_reference)
        test_result = build_q5(spark_thunderduck)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-H Q5 DataFrame",
            epsilon=1e-6
        )


# ============================================================================
# Basic DataFrame Operations Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.dataframe
class TestBasicOperations_Differential:
    """
    Basic DataFrame operations differential tests.
    Ensures fundamental operations work identically on Spark and Thunderduck.
    """

    def test_simple_filter(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple filter operation"""
        from pyspark.sql import functions as F

        def build_query(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return lineitem.filter(F.col("l_quantity") > 40).select("l_orderkey", "l_quantity")

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_filter")

    def test_simple_select(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple select/project operation"""
        def build_query(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return lineitem.select("l_orderkey", "l_quantity", "l_extendedprice").limit(100)

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_select")

    def test_simple_aggregate(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple aggregation"""
        from pyspark.sql import functions as F

        def build_query(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return lineitem.agg(F.sum("l_quantity").alias("total_quantity"))

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_aggregate", epsilon=1e-6)

    def test_simple_groupby(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple group by operation"""
        from pyspark.sql import functions as F

        def build_query(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return lineitem.groupBy("l_returnflag").agg(F.count("*").alias("count"))

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_groupby")

    def test_simple_orderby(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple order by operation"""
        from pyspark.sql import functions as F

        def build_query(spark):
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return lineitem.orderBy(F.col("l_quantity").desc()).limit(10)

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_orderby")

    def test_simple_join(
        self,
        spark_reference,
        spark_thunderduck,
        tpch_tables_reference,
        tpch_tables_thunderduck,
        tpch_data_dir
    ):
        """Test simple join operation"""
        from pyspark.sql import functions as F

        def build_query(spark):
            orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
            lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return (orders
                .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
                .select("o_orderkey", "l_quantity")
                .limit(100)
            )

        ref_result = build_query(spark_reference)
        test_result = build_query(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, "simple_join")
