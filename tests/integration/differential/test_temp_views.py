"""
Temporary View Differential Tests

Tests createOrReplaceTempView functionality comparing Thunderduck against Apache Spark 4.0.1.
"""

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


class TestTempViewBasics:
    """Basic temporary view operations - differential tests"""

    def test_create_temp_view(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test creating a simple temporary view and querying it"""
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Load data and create temp views on both
        ref_df = spark_reference.read.parquet(lineitem_path)
        ref_df.createOrReplaceTempView("lineitem")

        td_df = spark_thunderduck.read.parquet(lineitem_path)
        td_df.createOrReplaceTempView("lineitem")

        # Query the views
        ref_result = spark_reference.sql("SELECT COUNT(*) as cnt FROM lineitem")
        td_result = spark_thunderduck.sql("SELECT COUNT(*) as cnt FROM lineitem")

        assert_dataframes_equal(ref_result, td_result, "create_temp_view_count")

    def test_create_temp_view_with_filter(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test creating temp view from filtered DataFrame"""
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Load and filter data on both
        ref_df = spark_reference.read.parquet(lineitem_path).filter("l_quantity > 40")
        ref_df.createOrReplaceTempView("high_quantity")

        td_df = spark_thunderduck.read.parquet(lineitem_path).filter("l_quantity > 40")
        td_df.createOrReplaceTempView("high_quantity")

        # Query the filtered views
        ref_result = spark_reference.sql("SELECT COUNT(*) as cnt FROM high_quantity")
        td_result = spark_thunderduck.sql("SELECT COUNT(*) as cnt FROM high_quantity")

        assert_dataframes_equal(ref_result, td_result, "temp_view_with_filter_count")

    def test_multiple_temp_views(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test multiple temporary views in same session"""
        orders_path = str(tpch_data_dir / "orders.parquet")
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Create views on Spark Reference
        spark_reference.read.parquet(orders_path).createOrReplaceTempView("orders")
        spark_reference.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # Create views on Thunderduck
        spark_thunderduck.read.parquet(orders_path).createOrReplaceTempView("orders")
        spark_thunderduck.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # Query joining both views
        join_query = """
            SELECT COUNT(*) as cnt
            FROM orders o
            JOIN lineitem l ON o.o_orderkey = l.l_orderkey
        """
        ref_result = spark_reference.sql(join_query)
        td_result = spark_thunderduck.sql(join_query)

        assert_dataframes_equal(ref_result, td_result, "multiple_temp_views_join")

    def test_replace_temp_view(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test replacing an existing temporary view"""
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Create initial views on both
        ref_df1 = spark_reference.read.parquet(lineitem_path)
        ref_df1.createOrReplaceTempView("test_view")

        td_df1 = spark_thunderduck.read.parquet(lineitem_path)
        td_df1.createOrReplaceTempView("test_view")

        # Replace with filtered version on both
        ref_df2 = ref_df1.filter("l_quantity > 45")
        ref_df2.createOrReplaceTempView("test_view")

        td_df2 = td_df1.filter("l_quantity > 45")
        td_df2.createOrReplaceTempView("test_view")

        # Query replaced views
        ref_result = spark_reference.sql("SELECT COUNT(*) as cnt FROM test_view")
        td_result = spark_thunderduck.sql("SELECT COUNT(*) as cnt FROM test_view")

        assert_dataframes_equal(ref_result, td_result, "replace_temp_view_count")


class TestTPCHWithTempViews:
    """TPC-H queries using temporary views - differential tests"""

    def test_tpch_q1_with_temp_view(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test TPC-H Q1 using temporary view"""
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Create temp views on both
        spark_reference.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")
        spark_thunderduck.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # TPC-H Q1 query
        q1 = """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                AVG(l_quantity) as avg_qty,
                AVG(l_extendedprice) as avg_price,
                AVG(l_discount) as avg_disc,
                COUNT(*) as count_order
            FROM lineitem
            WHERE l_shipdate <= DATE '1998-09-02'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """

        ref_result = spark_reference.sql(q1)
        td_result = spark_thunderduck.sql(q1)

        assert_dataframes_equal(ref_result, td_result, "tpch_q1_with_temp_view")

    def test_tpch_q3_with_temp_views(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test TPC-H Q3 using multiple temporary views"""
        # Create temp views on Spark Reference
        spark_reference.read.parquet(str(tpch_data_dir / "customer.parquet")).createOrReplaceTempView("customer")
        spark_reference.read.parquet(str(tpch_data_dir / "orders.parquet")).createOrReplaceTempView("orders")
        spark_reference.read.parquet(str(tpch_data_dir / "lineitem.parquet")).createOrReplaceTempView("lineitem")

        # Create temp views on Thunderduck
        spark_thunderduck.read.parquet(str(tpch_data_dir / "customer.parquet")).createOrReplaceTempView("customer")
        spark_thunderduck.read.parquet(str(tpch_data_dir / "orders.parquet")).createOrReplaceTempView("orders")
        spark_thunderduck.read.parquet(str(tpch_data_dir / "lineitem.parquet")).createOrReplaceTempView("lineitem")

        # TPC-H Q3 query
        q3 = """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            FROM customer
            JOIN orders ON c_custkey = o_custkey
            JOIN lineitem ON l_orderkey = o_orderkey
            WHERE c_mktsegment = 'BUILDING'
                AND o_orderdate < DATE '1995-03-15'
                AND l_shipdate > DATE '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
        """

        ref_result = spark_reference.sql(q3)
        td_result = spark_thunderduck.sql(q3)

        assert_dataframes_equal(ref_result, td_result, "tpch_q3_with_temp_views")

    def test_tpch_q6_with_temp_view(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test TPC-H Q6 using temporary view"""
        lineitem_path = str(tpch_data_dir / "lineitem.parquet")

        # Create temp views on both
        spark_reference.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")
        spark_thunderduck.read.parquet(lineitem_path).createOrReplaceTempView("lineitem")

        # TPC-H Q6 query
        q6 = """
            SELECT
                SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '1994-01-01'
                AND l_shipdate < DATE '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        """

        ref_result = spark_reference.sql(q6)
        td_result = spark_thunderduck.sql(q6)

        assert_dataframes_equal(ref_result, td_result, "tpch_q6_with_temp_view")
