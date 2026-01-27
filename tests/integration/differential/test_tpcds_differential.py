"""
TPC-DS Differential Testing: Apache Spark Connect vs Thunderduck

This test suite runs TPC-DS queries on both:
1. Apache Spark 4.0.1 Connect (reference) - running natively
2. Thunderduck Connect (test) - system under test

Results are compared row-by-row with detailed diff output on mismatch.

Coverage:
- 93 out of 99 TPC-DS queries
- Variant queries: Q14a/b, Q23a/b, Q24a/b, Q39a/b
- Total: ~97 unique query patterns

Exclusions:
- Q36: Uses GROUPING() in window PARTITION BY (DuckDB limitation)
- Q72: Causes Spark OOM in differential testing environment
"""

import pytest
import time
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# TPC-DS Query Lists
# ============================================================================

# Standard queries (1-99, excluding those with variants and Q36)
STANDARD_QUERIES = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 15, 16, 17, 18, 19, 20,
    21, 22, 25, 26, 27, 28, 29, 30,
    31, 32, 33, 34, 35, 37, 38,  # Q36 excluded (DuckDB limitation)
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
    61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
    71, 73, 74, 75, 76, 77, 78, 79, 80,  # Q72 excluded (Spark OOM)
    81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
    91, 92, 93, 94, 95, 96, 97, 98, 99
]

# Queries with a/b variants (instead of just the number)
VARIANT_QUERIES = ['14a', '14b', '23a', '23b', '24a', '24b', '39a', '39b']

# All query identifiers for parameterized testing
ALL_QUERIES = STANDARD_QUERIES + VARIANT_QUERIES


def run_differential_query(
    spark_reference,
    spark_thunderduck,
    query: str,
    query_name: str,
    epsilon: float = 1e-6
):
    """
    Execute a query on both Spark Reference and Thunderduck, compare results.

    Args:
        spark_reference: Spark session connected to Apache Spark
        spark_thunderduck: Spark session connected to Thunderduck
        query: SQL query string
        query_name: Name for display purposes
        epsilon: Tolerance for floating-point comparison
    """
    # Execute on Spark reference
    print("\n" + "=" * 80)
    print(f"Executing {query_name} on Spark Reference...")
    start_ref = time.time()
    reference_result = spark_reference.sql(query)
    ref_time = time.time() - start_ref
    print(f"✓ Spark Reference completed in {ref_time:.3f}s")

    # Execute on Thunderduck
    print(f"\nExecuting {query_name} on Thunderduck...")
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

    # Compare with detailed diff
    assert_dataframes_equal(
        reference_result,
        test_result,
        query_name=query_name,
        epsilon=epsilon,
        max_diff_rows=5
    )


# ============================================================================
# TPC-DS Differential Tests - Parameterized
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.parametrize("query_id", ALL_QUERIES)
class TestTPCDS_Differential:
    """
    Differential tests for all TPC-DS queries.

    Runs each query on both Apache Spark 4.0.1 and Thunderduck,
    comparing results row-by-row.
    """

    def test_query_differential(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """Compare Spark and Thunderduck results for TPC-DS query"""
        query = load_tpcds_query(query_id)

        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}",
            epsilon=1e-6
        )


# ============================================================================
# Quick Sanity Test
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.quick
class TestTPCDS_Sanity:
    """Quick sanity test to verify TPC-DS differential framework is working"""

    def test_simple_count(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck
    ):
        """Test simple SELECT COUNT(*) on a TPC-DS table"""
        query = "SELECT COUNT(*) as cnt FROM store_sales"

        print("\n" + "=" * 80)
        print("TPC-DS Sanity Test: Simple SELECT COUNT(*)")
        print("=" * 80)

        # Execute on both
        ref_result = spark_reference.sql(query)
        test_result = spark_thunderduck.sql(query)

        # Compare
        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Sanity: SELECT COUNT(*)"
        )

        print("✓ TPC-DS differential framework is working correctly!")


# ============================================================================
# Batch Tests for Faster Feedback
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch1:
    """TPC-DS Q1-Q10 differential tests (batch for faster feedback)"""

    @pytest.mark.parametrize("query_id", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    def test_batch1(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 1-10"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch2:
    """TPC-DS Q11-Q20 differential tests"""

    @pytest.mark.parametrize("query_id", [11, 12, 13, '14a', '14b', 15, 16, 17, 18, 19, 20])
    def test_batch2(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 11-20 (with Q14 variants)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch3:
    """TPC-DS Q21-Q30 differential tests"""

    @pytest.mark.parametrize("query_id", [21, 22, '23a', '23b', '24a', '24b', 25, 26, 27, 28, 29, 30])
    def test_batch3(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 21-30 (with Q23, Q24 variants)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch4:
    """TPC-DS Q31-Q40 differential tests (Q36 excluded)"""

    @pytest.mark.parametrize("query_id", [31, 32, 33, 34, 35, 37, 38, '39a', '39b', 40])
    def test_batch4(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 31-40 (Q36 excluded, Q39 variants included)"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch5:
    """TPC-DS Q41-Q50 differential tests"""

    @pytest.mark.parametrize("query_id", [41, 42, 43, 44, 45, 46, 47, 48, 49, 50])
    def test_batch5(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 41-50"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch6:
    """TPC-DS Q51-Q60 differential tests"""

    @pytest.mark.parametrize("query_id", [51, 52, 53, 54, 55, 56, 57, 58, 59, 60])
    def test_batch6(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 51-60"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch7:
    """TPC-DS Q61-Q70 differential tests"""

    @pytest.mark.parametrize("query_id", [61, 62, 63, 64, 65, 66, 67, 68, 69, 70])
    def test_batch7(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 61-70"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch8:
    """TPC-DS Q71-Q80 differential tests"""

    @pytest.mark.parametrize("query_id", [71, 73, 74, 75, 76, 77, 78, 79, 80])  # Q72 excluded
    def test_batch8(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 71-80"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch9:
    """TPC-DS Q81-Q90 differential tests"""

    @pytest.mark.parametrize("query_id", [81, 82, 83, 84, 85, 86, 87, 88, 89, 90])
    def test_batch9(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 81-90"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


@pytest.mark.differential
@pytest.mark.tpcds
class TestTPCDS_Batch10:
    """TPC-DS Q91-Q99 differential tests"""

    @pytest.mark.parametrize("query_id", [91, 92, 93, 94, 95, 96, 97, 98, 99])
    def test_batch10(
        self,
        query_id,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        load_tpcds_query
    ):
        """TPC-DS queries 91-99"""
        query = load_tpcds_query(query_id)
        run_differential_query(
            spark_reference,
            spark_thunderduck,
            query,
            query_name=f"TPC-DS Q{query_id}"
        )


# ============================================================================
# TPC-DS DataFrame API Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpcds
@pytest.mark.dataframe
class TestTPCDS_DataFrame_Differential:
    """
    TPC-DS queries implemented using DataFrame API for differential testing.

    These tests verify that DataFrame operations produce identical results
    on both Spark Reference and Thunderduck. Only queries compatible with
    pure DataFrame API (no CTEs, ROLLUP, etc.) are included.
    """

    def test_q3_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q3 via DataFrame API: Item brand sales analysis
        Tests: multi-table join, filter, groupBy, agg, orderBy, limit
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q3 DataFrame API: Item Brand Sales")
        print("=" * 80)

        def build_q3(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manufact_id") == 128) &
                    (F.col("d_moy") == 11)
                )
                .groupBy("d_year", "i_brand", "i_brand_id")
                .agg(F.sum("ss_ext_sales_price").alias("sum_agg"))
                .select(
                    F.col("d_year"),
                    F.col("i_brand_id").alias("brand_id"),
                    F.col("i_brand").alias("brand"),
                    F.col("sum_agg")
                )
                .orderBy("d_year", F.col("sum_agg").desc(), "brand_id")
                .limit(100)
            )

        ref_result = build_q3(spark_reference)
        test_result = build_q3(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q3 DataFrame",
            epsilon=1e-6
        )

    def test_q7_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q7 via DataFrame API: Promotional item analysis
        Tests: 5-table join, filter with OR, groupBy, avg aggregations
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q7 DataFrame API: Promotional Item Analysis")
        print("=" * 80)

        def build_q7(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            promotion = spark.read.parquet(str(tpcds_data_dir / "promotion.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .join(customer_demographics, store_sales["ss_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(promotion, store_sales["ss_promo_sk"] == promotion["p_promo_sk"])
                .filter(
                    (F.col("cd_gender") == "M") &
                    (F.col("cd_marital_status") == "S") &
                    (F.col("cd_education_status") == "College") &
                    ((F.col("p_channel_email") == "N") | (F.col("p_channel_event") == "N")) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("i_item_id")
                .agg(
                    F.avg("ss_quantity").alias("agg1"),
                    F.avg("ss_list_price").alias("agg2"),
                    F.avg("ss_coupon_amt").alias("agg3"),
                    F.avg("ss_sales_price").alias("agg4")
                )
                .orderBy("i_item_id")
                .limit(100)
            )

        ref_result = build_q7(spark_reference)
        test_result = build_q7(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q7 DataFrame",
            epsilon=1e-6
        )

    def test_q42_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q42 via DataFrame API: Monthly sales by category
        Tests: join, filter, groupBy with multiple columns, sum, orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q42 DataFrame API: Monthly Sales by Category")
        print("=" * 80)

        def build_q42(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manager_id") == 1) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("d_year", "i_category_id", "i_category")
                .agg(F.sum("ss_ext_sales_price").alias("sum_agg"))
                .orderBy(
                    F.col("sum_agg").desc(),
                    "d_year",
                    "i_category_id",
                    "i_category"
                )
                .limit(100)
            )

        ref_result = build_q42(spark_reference)
        test_result = build_q42(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q42 DataFrame",
            epsilon=1e-6
        )

    def test_q52_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q52 via DataFrame API: Brand revenue analysis
        Tests: 3-table join, filter, groupBy, sum, orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q52 DataFrame API: Brand Revenue Analysis")
        print("=" * 80)

        def build_q52(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manager_id") == 1) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("d_year", "i_brand", "i_brand_id")
                .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
                .orderBy("d_year", F.col("ext_price").desc(), "i_brand_id")
                .limit(100)
            )

        ref_result = build_q52(spark_reference)
        test_result = build_q52(spark_thunderduck)

        assert_dataframes_equal(
            ref_result,
            test_result,
            query_name="TPC-DS Q52 DataFrame",
            epsilon=1e-6
        )

    def test_q12_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q12 via DataFrame API: Web sales by item class
        Tests: join, filter with isin, groupBy, window function (partitionBy)
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        print("\n" + "=" * 80)
        print("TPC-DS Q12 DataFrame API: Web Sales by Item Class")
        print("=" * 80)

        def build_q12(spark):
            web_sales = spark.read.parquet(str(tpcds_data_dir / "web_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (web_sales
                .join(item, web_sales["ws_item_sk"] == item["i_item_sk"])
                .join(date_dim, web_sales["ws_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    (F.col("d_date") >= "1999-02-01") &
                    (F.col("d_date") <= "1999-03-03") &
                    F.col("i_category").isin("Sports", "Books", "Home")
                )
                .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
                .agg(F.sum("ws_ext_sales_price").alias("itemrevenue"))
                .withColumn("revenueratio",
                    F.col("itemrevenue") * 100 / F.sum("itemrevenue").over(Window.partitionBy("i_class")))
                .orderBy("i_category", "i_class", "i_item_id", "i_item_desc", F.col("revenueratio").desc())
                .limit(100)
            )

        ref_result = build_q12(spark_reference)
        test_result = build_q12(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q12 DataFrame", epsilon=1e-6)

    def test_q19_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q19 via DataFrame API: Store and catalog brand comparison
        Tests: 6-table join, filter with string functions (substr)
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q19 DataFrame API: Brand Comparison")
        print("=" * 80)

        def build_q19(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            customer = spark.read.parquet(str(tpcds_data_dir / "customer.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .join(customer, store_sales["ss_customer_sk"] == customer["c_customer_sk"])
                .join(customer_address, customer["c_current_addr_sk"] == customer_address["ca_address_sk"])
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .filter(
                    (F.col("i_manager_id") == 8) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 1998) &
                    (F.substring("s_zip", 1, 2) != F.substring("ca_zip", 1, 2))
                )
                .groupBy("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
                .orderBy(F.col("ext_price").desc(), "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .limit(100)
            )

        ref_result = build_q19(spark_reference)
        test_result = build_q19(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q19 DataFrame", epsilon=1e-6)

    def test_q20_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q20 via DataFrame API: Catalog sales by item category
        Tests: join, filter with isin, groupBy, window function
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        print("\n" + "=" * 80)
        print("TPC-DS Q20 DataFrame API: Catalog Sales by Category")
        print("=" * 80)

        def build_q20(spark):
            catalog_sales = spark.read.parquet(str(tpcds_data_dir / "catalog_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (catalog_sales
                .join(item, catalog_sales["cs_item_sk"] == item["i_item_sk"])
                .join(date_dim, catalog_sales["cs_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    F.col("i_category").isin("Sports", "Books", "Home") &
                    (F.col("d_date") >= "1999-02-01") &
                    (F.col("d_date") <= "1999-03-03")
                )
                .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
                .agg(F.sum("cs_ext_sales_price").alias("itemrevenue"))
                .withColumn("revenueratio",
                    F.col("itemrevenue") * 100 / F.sum("itemrevenue").over(Window.partitionBy("i_class")))
                .orderBy("i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio")
                .limit(100)
            )

        ref_result = build_q20(spark_reference)
        test_result = build_q20(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q20 DataFrame", epsilon=1e-6)

    def test_q26_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q26 via DataFrame API: Catalog sales promotional analysis
        Tests: 5-table join, filter with OR, groupBy, avg aggregations
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q26 DataFrame API: Promotional Analysis")
        print("=" * 80)

        def build_q26(spark):
            catalog_sales = spark.read.parquet(str(tpcds_data_dir / "catalog_sales.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            promotion = spark.read.parquet(str(tpcds_data_dir / "promotion.parquet"))

            return (catalog_sales
                .join(date_dim, catalog_sales["cs_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, catalog_sales["cs_item_sk"] == item["i_item_sk"])
                .join(customer_demographics, catalog_sales["cs_bill_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(promotion, catalog_sales["cs_promo_sk"] == promotion["p_promo_sk"])
                .filter(
                    (F.col("cd_gender") == "M") &
                    (F.col("cd_marital_status") == "S") &
                    (F.col("cd_education_status") == "College") &
                    ((F.col("p_channel_email") == "N") | (F.col("p_channel_event") == "N")) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("i_item_id")
                .agg(
                    F.avg("cs_quantity").alias("agg1"),
                    F.avg("cs_list_price").alias("agg2"),
                    F.avg("cs_coupon_amt").alias("agg3"),
                    F.avg("cs_sales_price").alias("agg4")
                )
                .orderBy("i_item_id")
                .limit(100)
            )

        ref_result = build_q26(spark_reference)
        test_result = build_q26(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q26 DataFrame", epsilon=1e-6)

    def test_q43_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q43 via DataFrame API: Store sales by day of week
        Tests: join, filter, groupBy, conditional aggregation (when)
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q43 DataFrame API: Sales by Day of Week")
        print("=" * 80)

        def build_q43(spark):
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .filter((F.col("s_gmt_offset") == -5) & (F.col("d_year") == 2000))
                .groupBy("s_store_name", "s_store_id")
                .agg(
                    F.sum(F.when(F.col("d_day_name") == "Sunday", F.col("ss_sales_price"))).alias("sun_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Monday", F.col("ss_sales_price"))).alias("mon_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Tuesday", F.col("ss_sales_price"))).alias("tue_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Wednesday", F.col("ss_sales_price"))).alias("wed_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Thursday", F.col("ss_sales_price"))).alias("thu_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Friday", F.col("ss_sales_price"))).alias("fri_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Saturday", F.col("ss_sales_price"))).alias("sat_sales")
                )
                .orderBy("s_store_name", "s_store_id", "sun_sales", "mon_sales", "tue_sales",
                         "wed_sales", "thu_sales", "fri_sales", "sat_sales")
                .limit(100)
            )

        ref_result = build_q43(spark_reference)
        test_result = build_q43(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q43 DataFrame", epsilon=1e-6)

    def test_q55_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q55 via DataFrame API: Brand manager analysis
        Tests: 3-table join, filter, groupBy, sum, orderBy desc
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q55 DataFrame API: Brand Manager Analysis")
        print("=" * 80)

        def build_q55(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .filter(
                    (F.col("i_manager_id") == 28) &
                    (F.col("d_moy") == 11) &
                    (F.col("d_year") == 1999)
                )
                .groupBy("i_brand_id", "i_brand")
                .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
                .orderBy(F.col("ext_price").desc(), "i_brand_id")
                .limit(100)
            )

        ref_result = build_q55(spark_reference)
        test_result = build_q55(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q55 DataFrame", epsilon=1e-6)

    def test_q96_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q96 via DataFrame API: Store sales time series count
        Tests: 4-table join, filter with multiple conditions, count aggregation
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q96 DataFrame API: Time Series Count")
        print("=" * 80)

        def build_q96(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            household_demographics = spark.read.parquet(str(tpcds_data_dir / "household_demographics.parquet"))
            time_dim = spark.read.parquet(str(tpcds_data_dir / "time_dim.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))

            return (store_sales
                .join(household_demographics, store_sales["ss_hdemo_sk"] == household_demographics["hd_demo_sk"])
                .join(time_dim, store_sales["ss_sold_time_sk"] == time_dim["t_time_sk"])
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .filter(
                    (F.col("t_hour") == 8) &
                    (F.col("t_minute") >= 30) &
                    (F.col("hd_dep_count") == 5) &
                    (F.col("s_store_name") == "ese")
                )
                .agg(F.count("*").alias("count_star"))
                .orderBy("count_star")
                .limit(100)
            )

        ref_result = build_q96(spark_reference)
        test_result = build_q96(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q96 DataFrame", epsilon=1e-6)

    def test_q98_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q98 via DataFrame API: Store sales by item category with window
        Tests: 3-table join, filter with between, groupBy, window function
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        print("\n" + "=" * 80)
        print("TPC-DS Q98 DataFrame API: Category Window Analysis")
        print("=" * 80)

        def build_q98(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (store_sales
                .join(item, store_sales["ss_item_sk"] == item["i_item_sk"])
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(F.col("d_date").between("1999-02-22", "1999-03-24"))
                .groupBy("i_category", "i_class", "i_item_id", "i_item_desc", "i_current_price")
                .agg(F.sum("ss_ext_sales_price").alias("itemrevenue"))
                .withColumn("revenueratio",
                    F.col("itemrevenue") * 100 / F.sum("itemrevenue").over(Window.partitionBy("i_class")))
                .select("i_category", "i_class", "i_item_id", "i_item_desc", "i_current_price",
                        "itemrevenue", "revenueratio")
                .orderBy("i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio")
                .limit(100)
            )

        ref_result = build_q98(spark_reference)
        test_result = build_q98(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q98 DataFrame", epsilon=1e-6)

    def test_q15_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q15 via DataFrame API: Catalog sales by zip codes
        Tests: 4-table join, filter with substr/isin, groupBy, sum
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q15 DataFrame API: Catalog Sales by Zip")
        print("=" * 80)

        def build_q15(spark):
            catalog_sales = spark.read.parquet(str(tpcds_data_dir / "catalog_sales.parquet"))
            customer = spark.read.parquet(str(tpcds_data_dir / "customer.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (catalog_sales
                .join(customer, catalog_sales["cs_bill_customer_sk"] == customer["c_customer_sk"])
                .join(customer_address, customer["c_current_addr_sk"] == customer_address["ca_address_sk"])
                .join(date_dim, catalog_sales["cs_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    (F.substring("ca_zip", 1, 5).isin(
                        "85669", "86197", "88274", "83405", "86475", "85392", "85460", "80348", "81792"
                    )) |
                    F.col("ca_state").isin("CA", "WA", "GA") |
                    (F.col("cs_sales_price") > 500)
                )
                .filter((F.col("d_qoy") == 2) & (F.col("d_year") == 2001))
                .groupBy("ca_zip")
                .agg(F.sum("cs_sales_price").alias("sum_sales"))
                .orderBy("ca_zip")
                .limit(100)
            )

        ref_result = build_q15(spark_reference)
        test_result = build_q15(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q15 DataFrame", epsilon=1e-6)

    def test_q41_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q41 via DataFrame API: Popular product items
        Tests: filter with complex conditions, distinct, orderBy
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q41 DataFrame API: Popular Products")
        print("=" * 80)

        def build_q41(spark):
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))

            return (item
                .filter(
                    ((F.col("i_manufact_id").between(738, 778)) &
                     ((F.col("i_manager_id").between(50, 90)) |
                      (F.col("i_manager_id").between(100, 140)))) |
                    ((F.col("i_manufact_id").between(788, 828)) &
                     (F.col("i_manager_id").between(75, 115)))
                )
                .select("i_product_name")
                .distinct()
                .orderBy("i_product_name")
                .limit(100)
            )

        ref_result = build_q41(spark_reference)
        test_result = build_q41(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q41 DataFrame", epsilon=1e-6)

    def test_q45_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q45 via DataFrame API: Web sales by zip codes
        Tests: 5-table join, filter with isin, groupBy, sum
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q45 DataFrame API: Web Sales by Zip")
        print("=" * 80)

        def build_q45(spark):
            web_sales = spark.read.parquet(str(tpcds_data_dir / "web_sales.parquet"))
            customer = spark.read.parquet(str(tpcds_data_dir / "customer.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (web_sales
                .join(customer, web_sales["ws_bill_customer_sk"] == customer["c_customer_sk"])
                .join(customer_address, customer["c_current_addr_sk"] == customer_address["ca_address_sk"])
                .join(item, web_sales["ws_item_sk"] == item["i_item_sk"])
                .join(date_dim, web_sales["ws_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    F.col("ca_zip").isin("85669", "86197", "88274", "83405",
                                         "86475", "85392", "85460", "80348", "81792") &
                    (F.col("d_qoy") == 2) &
                    (F.col("d_year") == 2000)
                )
                .groupBy("ca_zip", "ca_city")
                .agg(F.sum("ws_sales_price").alias("sum_sales"))
                .orderBy("ca_zip", "ca_city")
                .limit(100)
            )

        ref_result = build_q45(spark_reference)
        test_result = build_q45(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q45 DataFrame", epsilon=1e-6)

    def test_q62_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q62 via DataFrame API: Web site shipping analysis
        Tests: 5-table join, filter, conditional aggregation by day
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q62 DataFrame API: Shipping Analysis")
        print("=" * 80)

        def build_q62(spark):
            web_sales = spark.read.parquet(str(tpcds_data_dir / "web_sales.parquet"))
            warehouse = spark.read.parquet(str(tpcds_data_dir / "warehouse.parquet"))
            ship_mode = spark.read.parquet(str(tpcds_data_dir / "ship_mode.parquet"))
            web_site = spark.read.parquet(str(tpcds_data_dir / "web_site.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (web_sales
                .join(warehouse, web_sales["ws_warehouse_sk"] == warehouse["w_warehouse_sk"])
                .join(ship_mode, web_sales["ws_ship_mode_sk"] == ship_mode["sm_ship_mode_sk"])
                .join(web_site, web_sales["ws_web_site_sk"] == web_site["web_site_sk"])
                .join(date_dim, web_sales["ws_ship_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    F.col("d_month_seq").between(1200, 1211) &
                    F.col("sm_carrier").isin("DHL", "BARIAN")
                )
                .groupBy(
                    F.substring("w_warehouse_name", 1, 20).alias("warehouse_name_substr"),
                    "sm_type",
                    "web_name"
                )
                .agg(
                    F.sum(F.when(F.col("d_day_name") == "Sunday", F.col("ws_ext_sales_price")).otherwise(0)).alias("sun_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Monday", F.col("ws_ext_sales_price")).otherwise(0)).alias("mon_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Tuesday", F.col("ws_ext_sales_price")).otherwise(0)).alias("tue_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Wednesday", F.col("ws_ext_sales_price")).otherwise(0)).alias("wed_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Thursday", F.col("ws_ext_sales_price")).otherwise(0)).alias("thu_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Friday", F.col("ws_ext_sales_price")).otherwise(0)).alias("fri_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Saturday", F.col("ws_ext_sales_price")).otherwise(0)).alias("sat_sales")
                )
                .orderBy("warehouse_name_substr", "sm_type", "web_name")
                .limit(100)
            )

        ref_result = build_q62(spark_reference)
        test_result = build_q62(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q62 DataFrame", epsilon=1e-6)

    def test_q84_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q84 via DataFrame API: Customer income and location
        Tests: 6-table join, filter, concat_ws, distinct
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q84 DataFrame API: Customer Income Analysis")
        print("=" * 80)

        def build_q84(spark):
            customer = spark.read.parquet(str(tpcds_data_dir / "customer.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            household_demographics = spark.read.parquet(str(tpcds_data_dir / "household_demographics.parquet"))
            income_band = spark.read.parquet(str(tpcds_data_dir / "income_band.parquet"))
            store_returns = spark.read.parquet(str(tpcds_data_dir / "store_returns.parquet"))

            return (customer
                .join(customer_address, customer["c_current_addr_sk"] == customer_address["ca_address_sk"])
                .join(customer_demographics, customer["c_current_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(household_demographics, customer["c_current_hdemo_sk"] == household_demographics["hd_demo_sk"])
                .join(income_band, household_demographics["hd_income_band_sk"] == income_band["ib_income_band_sk"])
                .join(store_returns, customer["c_customer_sk"] == store_returns["sr_customer_sk"])
                .filter(
                    (F.col("ca_city") == "Edgewood") &
                    (F.col("ib_lower_bound") >= 38128) &
                    (F.col("ib_upper_bound") <= 88128)
                )
                .select(
                    F.col("c_customer_id"),
                    F.concat_ws(" ", F.col("c_last_name"), F.col("c_first_name")).alias("customer_name")
                )
                .distinct()
                .orderBy("c_customer_id")
                .limit(100)
            )

        ref_result = build_q84(spark_reference)
        test_result = build_q84(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q84 DataFrame", epsilon=1e-6)

    def test_q99_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q99 via DataFrame API: Catalog shipping analysis
        Tests: 5-table join, filter, conditional aggregation
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q99 DataFrame API: Catalog Shipping")
        print("=" * 80)

        def build_q99(spark):
            catalog_sales = spark.read.parquet(str(tpcds_data_dir / "catalog_sales.parquet"))
            warehouse = spark.read.parquet(str(tpcds_data_dir / "warehouse.parquet"))
            ship_mode = spark.read.parquet(str(tpcds_data_dir / "ship_mode.parquet"))
            call_center = spark.read.parquet(str(tpcds_data_dir / "call_center.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (catalog_sales
                .join(warehouse, catalog_sales["cs_warehouse_sk"] == warehouse["w_warehouse_sk"])
                .join(ship_mode, catalog_sales["cs_ship_mode_sk"] == ship_mode["sm_ship_mode_sk"])
                .join(call_center, catalog_sales["cs_call_center_sk"] == call_center["cc_call_center_sk"])
                .join(date_dim, catalog_sales["cs_ship_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    F.col("d_month_seq").between(1200, 1211) &
                    F.col("sm_carrier").isin("DHL", "BARIAN")
                )
                .groupBy(
                    F.substring("w_warehouse_name", 1, 20).alias("warehouse_substr"),
                    "sm_type",
                    "cc_name"
                )
                .agg(
                    F.sum(F.when(F.col("d_day_name") == "Sunday", F.col("cs_sales_price")).otherwise(0)).alias("sun_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Monday", F.col("cs_sales_price")).otherwise(0)).alias("mon_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Tuesday", F.col("cs_sales_price")).otherwise(0)).alias("tue_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Wednesday", F.col("cs_sales_price")).otherwise(0)).alias("wed_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Thursday", F.col("cs_sales_price")).otherwise(0)).alias("thu_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Friday", F.col("cs_sales_price")).otherwise(0)).alias("fri_sales"),
                    F.sum(F.when(F.col("d_day_name") == "Saturday", F.col("cs_sales_price")).otherwise(0)).alias("sat_sales")
                )
                .orderBy("warehouse_substr", "sm_type", "cc_name")
                .limit(100)
            )

        ref_result = build_q99(spark_reference)
        test_result = build_q99(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q99 DataFrame", epsilon=1e-6)

    def test_q82_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q82 via DataFrame API: Item inventory and price comparison
        Tests: 4-table join with left join, filter with between/isin
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q82 DataFrame API: Inventory Price Comparison")
        print("=" * 80)

        def build_q82(spark):
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            inventory = spark.read.parquet(str(tpcds_data_dir / "inventory.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))

            return (item
                .join(inventory, item["i_item_sk"] == inventory["inv_item_sk"])
                .join(date_dim, inventory["inv_date_sk"] == date_dim["d_date_sk"])
                .join(store_sales, item["i_item_sk"] == store_sales["ss_item_sk"], "left")
                .filter(
                    F.col("i_current_price").between(62, 92) &
                    F.col("d_date").between("2000-05-25", "2000-07-24") &
                    F.col("i_manufact_id").isin(129, 270, 821, 423)
                )
                .groupBy("i_item_id", "i_item_desc", "i_current_price")
                .agg(F.sum("inv_quantity_on_hand").alias("inv_total"))
                .orderBy("i_item_id")
                .limit(100)
            )

        ref_result = build_q82(spark_reference)
        test_result = build_q82(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q82 DataFrame", epsilon=1e-6)

    def test_q91_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q91 via DataFrame API: Catalog returns by call center
        Tests: 7-table join, complex filter with OR/isin, count aggregation
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q91 DataFrame API: Call Center Returns")
        print("=" * 80)

        def build_q91(spark):
            call_center = spark.read.parquet(str(tpcds_data_dir / "call_center.parquet"))
            catalog_returns = spark.read.parquet(str(tpcds_data_dir / "catalog_returns.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            customer = spark.read.parquet(str(tpcds_data_dir / "customer.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            household_demographics = spark.read.parquet(str(tpcds_data_dir / "household_demographics.parquet"))

            return (catalog_returns
                .join(date_dim, catalog_returns["cr_returned_date_sk"] == date_dim["d_date_sk"])
                .join(customer, catalog_returns["cr_returning_customer_sk"] == customer["c_customer_sk"])
                .join(customer_demographics, customer["c_current_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(household_demographics, customer["c_current_hdemo_sk"] == household_demographics["hd_demo_sk"])
                .join(customer_address, customer["c_current_addr_sk"] == customer_address["ca_address_sk"])
                .join(call_center, catalog_returns["cr_call_center_sk"] == call_center["cc_call_center_sk"])
                .filter(
                    (F.col("d_year") == 1998) &
                    (F.col("d_moy") == 11) &
                    ((F.col("cd_marital_status") == "M") | (F.col("cd_marital_status") == "S")) &
                    (F.col("cd_education_status") == "Unknown") &
                    (F.col("hd_buy_potential").isin("Unknown", "1001-5000") &
                     (F.col("ca_gmt_offset") == -7))
                )
                .groupBy("cc_call_center_id", "cc_name", "cc_manager", "cd_marital_status", "cd_education_status")
                .agg(F.count("cr_returning_customer_sk").alias("returns_count"))
                .orderBy(F.col("returns_count").desc())
                .limit(100)
            )

        ref_result = build_q91(spark_reference)
        test_result = build_q91(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q91 DataFrame", epsilon=1e-6)

    def test_q37_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q37 via DataFrame API: Item and inventory analysis
        Tests: 4-table join, filter with between/isin, groupBy, left join
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q37 DataFrame API: Item Inventory Analysis")
        print("=" * 80)

        def build_q37(spark):
            item = spark.read.parquet(str(tpcds_data_dir / "item.parquet"))
            inventory = spark.read.parquet(str(tpcds_data_dir / "inventory.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))
            catalog_sales = spark.read.parquet(str(tpcds_data_dir / "catalog_sales.parquet"))

            return (item
                .join(inventory, item["i_item_sk"] == inventory["inv_item_sk"])
                .join(date_dim, inventory["inv_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    F.col("i_current_price").between(68, 98) &
                    F.col("d_date").between("2000-02-01", "2000-04-01") &
                    F.col("i_manufact_id").isin(677, 940, 694, 808)
                )
                .join(catalog_sales, item["i_item_sk"] == catalog_sales["cs_item_sk"], "left")
                .groupBy("i_item_id", "i_item_desc", "i_current_price")
                .agg(F.sum("inv_quantity_on_hand").alias("inventory_total"))
                .orderBy("i_item_id")
                .limit(100)
            )

        ref_result = build_q37(spark_reference)
        test_result = build_q37(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q37 DataFrame", epsilon=1e-6)

    def test_q50_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q50 via DataFrame API: Store returns analysis
        Tests: 4-table join, groupBy with many columns, aggregations
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q50 DataFrame API: Store Returns Analysis")
        print("=" * 80)

        def build_q50(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            store_returns = spark.read.parquet(str(tpcds_data_dir / "store_returns.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (store_returns
                .join(date_dim, store_returns["sr_returned_date_sk"] == date_dim["d_date_sk"])
                .join(store_sales,
                      (store_returns["sr_item_sk"] == store_sales["ss_item_sk"]) &
                      (store_returns["sr_ticket_number"] == store_sales["ss_ticket_number"]))
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .filter(F.col("d_year") == 2000)
                .groupBy(
                    "s_store_name", "s_company_id", "s_street_number", "s_street_name",
                    "s_street_type", "s_suite_number", "s_city", "s_county", "s_state", "s_zip"
                )
                .agg(
                    F.sum("sr_returned_date_sk").alias("returns_count"),
                    F.sum("sr_return_amt").alias("total_returns")
                )
                .orderBy("s_store_name")
                .limit(20)
            )

        ref_result = build_q50(spark_reference)
        test_result = build_q50(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q50 DataFrame", epsilon=1e-6)

    def test_q48_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q48 via DataFrame API: Store sales by customer demographics
        Tests: 5-table join, complex OR filters, aggregation
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q48 DataFrame API: Customer Demographics")
        print("=" * 80)

        def build_q48(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (store_sales
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .join(customer_demographics, store_sales["ss_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(customer_address, store_sales["ss_addr_sk"] == customer_address["ca_address_sk"])
                .filter(
                    ((F.col("cd_marital_status") == "M") &
                     (F.col("cd_education_status") == "4 yr Degree") &
                     F.col("ss_sales_price").between(100.00, 150.00)) |
                    ((F.col("cd_marital_status") == "D") &
                     (F.col("cd_education_status") == "2 yr Degree") &
                     F.col("ss_sales_price").between(50.00, 100.00)) |
                    ((F.col("cd_marital_status") == "S") &
                     (F.col("cd_education_status") == "College") &
                     F.col("ss_sales_price").between(150.00, 200.00))
                )
                .filter(
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("CO", "OH", "TX") &
                     F.col("ss_net_profit").between(0, 2000)) |
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("OR", "MN", "KY") &
                     F.col("ss_net_profit").between(150, 3000)) |
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("VA", "CA", "MS") &
                     F.col("ss_net_profit").between(50, 25000))
                )
                .filter(F.col("d_year") == 2000)
                .agg(F.sum("ss_quantity").alias("quantity"))
            )

        ref_result = build_q48(spark_reference)
        test_result = build_q48(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q48 DataFrame", epsilon=1e-6)

    def test_q13_dataframe(
        self,
        spark_reference,
        spark_thunderduck,
        tpcds_tables_reference,
        tpcds_tables_thunderduck,
        tpcds_data_dir
    ):
        """
        TPC-DS Q13 via DataFrame API: Store sales averages
        Tests: 6-table join, very complex OR filters, avg aggregations
        """
        from pyspark.sql import functions as F

        print("\n" + "=" * 80)
        print("TPC-DS Q13 DataFrame API: Store Sales Averages")
        print("=" * 80)

        def build_q13(spark):
            store_sales = spark.read.parquet(str(tpcds_data_dir / "store_sales.parquet"))
            store = spark.read.parquet(str(tpcds_data_dir / "store.parquet"))
            customer_demographics = spark.read.parquet(str(tpcds_data_dir / "customer_demographics.parquet"))
            household_demographics = spark.read.parquet(str(tpcds_data_dir / "household_demographics.parquet"))
            customer_address = spark.read.parquet(str(tpcds_data_dir / "customer_address.parquet"))
            date_dim = spark.read.parquet(str(tpcds_data_dir / "date_dim.parquet"))

            return (store_sales
                .join(store, store_sales["ss_store_sk"] == store["s_store_sk"])
                .join(customer_demographics, store_sales["ss_cdemo_sk"] == customer_demographics["cd_demo_sk"])
                .join(household_demographics, store_sales["ss_hdemo_sk"] == household_demographics["hd_demo_sk"])
                .join(customer_address, store_sales["ss_addr_sk"] == customer_address["ca_address_sk"])
                .join(date_dim, store_sales["ss_sold_date_sk"] == date_dim["d_date_sk"])
                .filter(
                    ((F.col("cd_marital_status") == "D") &
                     (F.col("cd_education_status") == "Advanced Degree") &
                     F.col("ss_sales_price").between(100.00, 150.00) &
                     (F.col("hd_dep_count") == 3)) |
                    ((F.col("cd_marital_status") == "S") &
                     (F.col("cd_education_status") == "College") &
                     F.col("ss_sales_price").between(50.00, 100.00) &
                     (F.col("hd_dep_count") == 1)) |
                    ((F.col("cd_marital_status") == "W") &
                     (F.col("cd_education_status") == "2 yr Degree") &
                     F.col("ss_sales_price").between(150.00, 200.00) &
                     (F.col("hd_dep_count") == 1))
                )
                .filter(
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("TX", "OH", "TX") &
                     F.col("ss_net_profit").between(100, 200)) |
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("OR", "NM", "KY") &
                     F.col("ss_net_profit").between(150, 300)) |
                    ((F.col("ca_country") == "United States") &
                     F.col("ca_state").isin("VA", "TX", "MS") &
                     F.col("ss_net_profit").between(50, 250))
                )
                .agg(
                    F.avg("ss_quantity").alias("avg_quantity"),
                    F.avg("ss_ext_sales_price").alias("avg_sales_price"),
                    F.avg("ss_ext_discount_amt").alias("avg_discount"),
                    F.count("ss_quantity").alias("count_quantity")
                )
            )

        ref_result = build_q13(spark_reference)
        test_result = build_q13(spark_thunderduck)

        assert_dataframes_equal(ref_result, test_result, query_name="TPC-DS Q13 DataFrame", epsilon=1e-6)
