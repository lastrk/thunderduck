"""
TPC-H benchmark differential tests (DataFrame API only).
Verifies Thunderduck matches Spark 4.1.1 behavior exactly for TPC-H queries.

Thunderduck only supports the DataFrame API path (not SparkSQL via spark.sql()),
so all tests use DataFrame operations exclusively.

TPC-H data is expected at tests/integration/tpch_sf001/ and is loaded via
the tpch_tables_reference and tpch_tables_thunderduck fixtures from conftest.py.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
@pytest.mark.tpch
@pytest.mark.usefixtures("tpch_tables_reference", "tpch_tables_thunderduck")
class TestTPCHDifferential:
    """Differential tests for TPC-H queries (DataFrame API).

    Each test builds identical DataFrame pipelines on both spark_reference
    and spark_thunderduck, then compares results with assert_dataframes_equal.

    TPC-H tables are loaded as temp views by the tpch_tables_reference and
    tpch_tables_thunderduck fixtures (from conftest.py).
    """

    # =========================================================================
    # Query 1: Pricing Summary Report
    # =========================================================================

    def test_q01_dataframe(self, spark_reference, spark_thunderduck):
        """Q1: Pricing Summary Report (DataFrame API) - exact parity check."""
        def build_q01(session):
            return session.table("lineitem") \
                .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01").cast("date"), 90)) \
                .groupBy("l_returnflag", "l_linestatus") \
                .agg(
                    F.sum("l_quantity").alias("sum_qty"),
                    F.sum("l_extendedprice").alias("sum_base_price"),
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax"))).alias("sum_charge"),
                    F.avg("l_quantity").alias("avg_qty"),
                    F.avg("l_extendedprice").alias("avg_price"),
                    F.avg("l_discount").alias("avg_disc"),
                    F.count("*").alias("count_order")
                ) \
                .orderBy("l_returnflag", "l_linestatus")

        assert_dataframes_equal(
            build_q01(spark_reference), build_q01(spark_thunderduck),
            query_name="TPC-H Q1 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 2: Minimum Cost Supplier
    # =========================================================================

    def test_q02_dataframe(self, spark_reference, spark_thunderduck):
        """Q2: Minimum Cost Supplier (DataFrame API) - exact parity check."""
        window_spec = Window.partitionBy("p_partkey")

        def build_q02(session):
            return session.table("part") \
                .filter((F.col("p_size") == 15) & F.col("p_type").like("%BRASS")) \
                .join(session.table("partsupp"), F.col("p_partkey") == F.col("ps_partkey")) \
                .join(session.table("supplier"), F.col("s_suppkey") == F.col("ps_suppkey")) \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .join(session.table("region"), F.col("n_regionkey") == F.col("r_regionkey")) \
                .filter(F.col("r_name") == "EUROPE") \
                .withColumn("min_supplycost", F.min("ps_supplycost").over(window_spec)) \
                .filter(F.col("ps_supplycost") == F.col("min_supplycost")) \
                .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment") \
                .orderBy(F.col("s_acctbal").desc(), "n_name", "s_name", "p_partkey") \
                .limit(100)

        assert_dataframes_equal(
            build_q02(spark_reference), build_q02(spark_thunderduck),
            query_name="TPC-H Q2 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 3: Shipping Priority
    # =========================================================================

    def test_q03_dataframe(self, spark_reference, spark_thunderduck):
        """Q3: Shipping Priority (DataFrame API) - exact parity check."""
        def build_q03(session):
            return session.table("customer") \
                .filter(F.col("c_mktsegment") == "BUILDING") \
                .join(session.table("orders"), F.col("c_custkey") == F.col("o_custkey")) \
                .filter(F.col("o_orderdate") < F.lit("1995-03-15")) \
                .join(session.table("lineitem"), F.col("o_orderkey") == F.col("l_orderkey")) \
                .filter(F.col("l_shipdate") > F.lit("1995-03-15")) \
                .groupBy("l_orderkey", "o_orderdate", "o_shippriority") \
                .agg(
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
                ) \
                .orderBy(F.col("revenue").desc(), "o_orderdate") \
                .limit(10)

        assert_dataframes_equal(
            build_q03(spark_reference), build_q03(spark_thunderduck),
            query_name="TPC-H Q3 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 4: Order Priority Checking
    # =========================================================================

    def test_q04_dataframe(self, spark_reference, spark_thunderduck):
        """Q4: Order Priority Checking (DataFrame API) - exact parity check.
        Uses semi-join to implement EXISTS subquery."""
        def build_q04(session):
            late_lineitems = session.table("lineitem") \
                .filter(F.col("l_commitdate") < F.col("l_receiptdate")) \
                .select("l_orderkey").distinct()

            return session.table("orders") \
                .filter(
                    (F.col("o_orderdate") >= F.lit("1993-07-01")) &
                    (F.col("o_orderdate") < F.add_months(F.lit("1993-07-01").cast("date"), 3))
                ) \
                .join(late_lineitems, F.col("o_orderkey") == F.col("l_orderkey"), "left_semi") \
                .groupBy("o_orderpriority") \
                .agg(F.count("*").alias("order_count")) \
                .orderBy("o_orderpriority")

        assert_dataframes_equal(
            build_q04(spark_reference), build_q04(spark_thunderduck),
            query_name="TPC-H Q4 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 5: Local Supplier Volume
    # =========================================================================

    def test_q05_dataframe(self, spark_reference, spark_thunderduck):
        """Q5: Local Supplier Volume (DataFrame API) - exact parity check."""
        def build_q05(session):
            return session.table("customer") \
                .join(session.table("orders"), F.col("c_custkey") == F.col("o_custkey")) \
                .filter(
                    (F.col("o_orderdate") >= F.lit("1994-01-01")) &
                    (F.col("o_orderdate") < F.add_months(F.lit("1994-01-01").cast("date"), 12))
                ) \
                .join(session.table("lineitem"), F.col("l_orderkey") == F.col("o_orderkey")) \
                .join(session.table("supplier"), F.col("l_suppkey") == F.col("s_suppkey")) \
                .filter(F.col("c_nationkey") == F.col("s_nationkey")) \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .join(session.table("region"), F.col("n_regionkey") == F.col("r_regionkey")) \
                .filter(F.col("r_name") == "ASIA") \
                .groupBy("n_name") \
                .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")) \
                .orderBy(F.col("revenue").desc())

        assert_dataframes_equal(
            build_q05(spark_reference), build_q05(spark_thunderduck),
            query_name="TPC-H Q5 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 6: Forecasting Revenue Change
    # =========================================================================

    def test_q06_dataframe(self, spark_reference, spark_thunderduck):
        """Q6: Forecasting Revenue Change (DataFrame API) - exact parity check."""
        def build_q06(session):
            return session.table("lineitem") \
                .filter(
                    (F.col("l_shipdate") >= F.lit("1994-01-01")) &
                    (F.col("l_shipdate") < F.add_months(F.lit("1994-01-01").cast("date"), 12)) &
                    (F.col("l_discount").between(0.05, 0.07)) &
                    (F.col("l_quantity") < 24)
                ) \
                .agg(
                    F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")
                )

        assert_dataframes_equal(
            build_q06(spark_reference), build_q06(spark_thunderduck),
            query_name="TPC-H Q6 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 7: Volume Shipping
    # =========================================================================

    def test_q07_dataframe(self, spark_reference, spark_thunderduck):
        """Q7: Volume Shipping (DataFrame API) - exact parity check.
        Multi-join with nation-pair filter (FRANCE/GERMANY), year extraction."""
        def build_q07(session):
            n1 = session.table("nation").alias("n1")
            n2 = session.table("nation").alias("n2")

            shipping = session.table("supplier") \
                .join(session.table("lineitem"), F.col("s_suppkey") == F.col("l_suppkey")) \
                .join(session.table("orders"), F.col("l_orderkey") == F.col("o_orderkey")) \
                .join(session.table("customer"), F.col("o_custkey") == F.col("c_custkey")) \
                .join(n1, F.col("s_nationkey") == F.col("n1.n_nationkey")) \
                .join(n2, F.col("c_nationkey") == F.col("n2.n_nationkey")) \
                .filter(
                    (
                        (F.col("n1.n_name") == "FRANCE") & (F.col("n2.n_name") == "GERMANY")
                    ) | (
                        (F.col("n1.n_name") == "GERMANY") & (F.col("n2.n_name") == "FRANCE")
                    )
                ) \
                .filter(
                    (F.col("l_shipdate") >= F.lit("1995-01-01")) &
                    (F.col("l_shipdate") <= F.lit("1996-12-31"))
                ) \
                .select(
                    F.col("n1.n_name").alias("supp_nation"),
                    F.col("n2.n_name").alias("cust_nation"),
                    F.year("l_shipdate").alias("l_year"),
                    (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume")
                )

            return shipping \
                .groupBy("supp_nation", "cust_nation", "l_year") \
                .agg(F.sum("volume").alias("revenue")) \
                .orderBy("supp_nation", "cust_nation", "l_year")

        assert_dataframes_equal(
            build_q07(spark_reference), build_q07(spark_thunderduck),
            query_name="TPC-H Q7 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 8: National Market Share
    # =========================================================================

    def test_q08_dataframe(self, spark_reference, spark_thunderduck):
        """Q8: National Market Share (DataFrame API) - exact parity check.
        8-table join, CASE WHEN for nation-specific revenue, year extraction."""
        def build_q08(session):
            n1 = session.table("nation").alias("n1")
            n2 = session.table("nation").alias("n2")

            all_nations = session.table("part") \
                .filter(F.col("p_type") == "ECONOMY ANODIZED STEEL") \
                .join(session.table("lineitem"), F.col("p_partkey") == F.col("l_partkey")) \
                .join(session.table("supplier"), F.col("l_suppkey") == F.col("s_suppkey")) \
                .join(session.table("orders"), F.col("l_orderkey") == F.col("o_orderkey")) \
                .filter(
                    (F.col("o_orderdate") >= F.lit("1995-01-01")) &
                    (F.col("o_orderdate") <= F.lit("1996-12-31"))
                ) \
                .join(session.table("customer"), F.col("o_custkey") == F.col("c_custkey")) \
                .join(n1, F.col("c_nationkey") == F.col("n1.n_nationkey")) \
                .join(session.table("region"), F.col("n1.n_regionkey") == F.col("r_regionkey")) \
                .filter(F.col("r_name") == "AMERICA") \
                .join(n2, F.col("s_nationkey") == F.col("n2.n_nationkey")) \
                .select(
                    F.year("o_orderdate").alias("o_year"),
                    (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume"),
                    F.col("n2.n_name").alias("nation")
                )

            return all_nations \
                .groupBy("o_year") \
                .agg(
                    (F.sum(F.when(F.col("nation") == "BRAZIL", F.col("volume")).otherwise(0)) /
                     F.sum("volume")).alias("mkt_share")
                ) \
                .orderBy("o_year")

        assert_dataframes_equal(
            build_q08(spark_reference), build_q08(spark_thunderduck),
            query_name="TPC-H Q8 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 9: Product Type Profit Measure
    # =========================================================================

    def test_q09_dataframe(self, spark_reference, spark_thunderduck):
        """Q9: Product Type Profit Measure (DataFrame API) - exact parity check.
        6-table join, profit arithmetic, LIKE filter, year extraction."""
        def build_q09(session):
            profit = session.table("part") \
                .filter(F.col("p_name").like("%green%")) \
                .join(session.table("lineitem"), F.col("p_partkey") == F.col("l_partkey")) \
                .join(session.table("supplier"), F.col("l_suppkey") == F.col("s_suppkey")) \
                .join(session.table("partsupp"),
                      (F.col("l_suppkey") == F.col("ps_suppkey")) &
                      (F.col("l_partkey") == F.col("ps_partkey"))) \
                .join(session.table("orders"), F.col("l_orderkey") == F.col("o_orderkey")) \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .select(
                    F.col("n_name").alias("nation"),
                    F.year("o_orderdate").alias("o_year"),
                    (F.col("l_extendedprice") * (1 - F.col("l_discount")) -
                     F.col("ps_supplycost") * F.col("l_quantity")).alias("amount")
                )

            return profit \
                .groupBy("nation", "o_year") \
                .agg(F.sum("amount").alias("sum_profit")) \
                .orderBy("nation", F.col("o_year").desc())

        assert_dataframes_equal(
            build_q09(spark_reference), build_q09(spark_thunderduck),
            query_name="TPC-H Q9 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 10: Returned Item Reporting
    # =========================================================================

    def test_q10_dataframe(self, spark_reference, spark_thunderduck):
        """Q10: Returned Item Reporting (DataFrame API) - exact parity check.
        4-table join, date filter, groupBy with limit."""
        def build_q10(session):
            return session.table("customer") \
                .join(session.table("orders"), F.col("c_custkey") == F.col("o_custkey")) \
                .filter(
                    (F.col("o_orderdate") >= F.lit("1993-10-01")) &
                    (F.col("o_orderdate") < F.add_months(F.lit("1993-10-01").cast("date"), 3))
                ) \
                .join(session.table("lineitem"), F.col("l_orderkey") == F.col("o_orderkey")) \
                .filter(F.col("l_returnflag") == "R") \
                .join(session.table("nation"), F.col("c_nationkey") == F.col("n_nationkey")) \
                .groupBy(
                    "c_custkey", "c_name", "c_acctbal", "c_phone",
                    "n_name", "c_address", "c_comment"
                ) \
                .agg(
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
                ) \
                .select(
                    "c_custkey", "c_name", "revenue", "c_acctbal",
                    "n_name", "c_address", "c_phone", "c_comment"
                ) \
                .orderBy(F.col("revenue").desc()) \
                .limit(20)

        assert_dataframes_equal(
            build_q10(spark_reference), build_q10(spark_thunderduck),
            query_name="TPC-H Q10 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 11: Important Stock Identification
    # =========================================================================

    def test_q11_dataframe(self, spark_reference, spark_thunderduck):
        """Q11: Important Stock Identification (DataFrame API) - exact parity check.
        Join + groupBy + HAVING via crossJoin with threshold subquery."""
        def build_q11(session):
            # Base: partsupp joined with supplier and nation (Germany)
            base = session.table("partsupp") \
                .join(session.table("supplier"), F.col("ps_suppkey") == F.col("s_suppkey")) \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .filter(F.col("n_name") == "GERMANY")

            # Threshold: SUM(ps_supplycost * ps_availqty) * 0.0001
            threshold = base \
                .agg(
                    (F.sum(F.col("ps_supplycost") * F.col("ps_availqty")) * 0.0001).alias("threshold_value")
                )

            # Group by part, filter by threshold using crossJoin
            return base \
                .groupBy("ps_partkey") \
                .agg(
                    F.sum(F.col("ps_supplycost") * F.col("ps_availqty")).alias("value")
                ) \
                .crossJoin(threshold) \
                .filter(F.col("value") > F.col("threshold_value")) \
                .select("ps_partkey", "value") \
                .orderBy(F.col("value").desc())

        assert_dataframes_equal(
            build_q11(spark_reference), build_q11(spark_thunderduck),
            query_name="TPC-H Q11 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 12: Shipping Modes and Order Priority
    # =========================================================================

    def test_q12_dataframe(self, spark_reference, spark_thunderduck):
        """Q12: Shipping Modes and Order Priority (DataFrame API) - exact parity check.
        Join with CASE SUM for high/low priority counts by ship mode."""
        def build_q12(session):
            return session.table("orders") \
                .join(session.table("lineitem"), F.col("o_orderkey") == F.col("l_orderkey")) \
                .filter(
                    F.col("l_shipmode").isin("MAIL", "SHIP") &
                    (F.col("l_commitdate") < F.col("l_receiptdate")) &
                    (F.col("l_shipdate") < F.col("l_commitdate")) &
                    (F.col("l_receiptdate") >= F.lit("1994-01-01")) &
                    (F.col("l_receiptdate") < F.add_months(F.lit("1994-01-01").cast("date"), 12))
                ) \
                .groupBy("l_shipmode") \
                .agg(
                    F.sum(F.when(
                        F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1
                    ).otherwise(0)).alias("high_line_count"),
                    F.sum(F.when(
                        ~F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1
                    ).otherwise(0)).alias("low_line_count")
                ) \
                .orderBy("l_shipmode")

        assert_dataframes_equal(
            build_q12(spark_reference), build_q12(spark_thunderduck),
            query_name="TPC-H Q12 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 13: Customer Distribution
    # =========================================================================

    def test_q13_dataframe(self, spark_reference, spark_thunderduck):
        """Q13: Customer Distribution (DataFrame API) - exact parity check.
        LEFT OUTER JOIN, count, nested groupBy (distribution of order counts)."""
        def build_q13(session):
            # Count orders per customer (excluding special complaints)
            c_orders = session.table("customer") \
                .join(
                    session.table("orders").filter(~F.col("o_comment").like("%special%requests%")),
                    F.col("c_custkey") == F.col("o_custkey"),
                    "left_outer"
                ) \
                .groupBy("c_custkey") \
                .agg(F.count("o_orderkey").alias("c_count"))

            # Distribution: how many customers have each order count
            return c_orders \
                .groupBy("c_count") \
                .agg(F.count("*").alias("custdist")) \
                .orderBy(F.col("custdist").desc(), F.col("c_count").desc())

        assert_dataframes_equal(
            build_q13(spark_reference), build_q13(spark_thunderduck),
            query_name="TPC-H Q13 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 14: Promotion Effect
    # =========================================================================

    def test_q14_dataframe(self, spark_reference, spark_thunderduck):
        """Q14: Promotion Effect (DataFrame API) - exact parity check.
        Join with CASE for promo revenue percentage."""
        def build_q14(session):
            return session.table("lineitem") \
                .join(session.table("part"), F.col("l_partkey") == F.col("p_partkey")) \
                .filter(
                    (F.col("l_shipdate") >= F.lit("1995-09-01")) &
                    (F.col("l_shipdate") < F.add_months(F.lit("1995-09-01").cast("date"), 1))
                ) \
                .agg(
                    (F.lit(100.00) *
                     F.sum(F.when(
                         F.col("p_type").like("PROMO%"),
                         F.col("l_extendedprice") * (1 - F.col("l_discount"))
                     ).otherwise(0)) /
                     F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")))
                    ).alias("promo_revenue")
                )

        assert_dataframes_equal(
            build_q14(spark_reference), build_q14(spark_thunderduck),
            query_name="TPC-H Q14 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 15: Top Supplier
    # =========================================================================

    def test_q15_dataframe(self, spark_reference, spark_thunderduck):
        """Q15: Top Supplier (DataFrame API) - exact parity check.
        CTE (DataFrame reuse) for revenue view, join with max subquery."""
        def build_q15(session):
            # Revenue "view" (CTE equivalent)
            revenue = session.table("lineitem") \
                .filter(
                    (F.col("l_shipdate") >= F.lit("1996-01-01")) &
                    (F.col("l_shipdate") < F.add_months(F.lit("1996-01-01").cast("date"), 3))
                ) \
                .groupBy(F.col("l_suppkey").alias("supplier_no")) \
                .agg(
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("total_revenue")
                )

            # Max revenue
            max_revenue = revenue.agg(F.max("total_revenue").alias("max_revenue"))

            return session.table("supplier") \
                .join(revenue, F.col("s_suppkey") == F.col("supplier_no")) \
                .crossJoin(max_revenue) \
                .filter(F.col("total_revenue") == F.col("max_revenue")) \
                .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue") \
                .orderBy("s_suppkey")

        assert_dataframes_equal(
            build_q15(spark_reference), build_q15(spark_thunderduck),
            query_name="TPC-H Q15 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 16: Parts/Supplier Relationship
    # =========================================================================

    def test_q16_dataframe(self, spark_reference, spark_thunderduck):
        """Q16: Parts/Supplier Relationship (DataFrame API) - exact parity check.
        Anti-join for NOT IN (complaint suppliers), countDistinct, LIKE filters."""
        def build_q16(session):
            # Suppliers with complaints (to exclude)
            complaint_suppliers = session.table("supplier") \
                .filter(F.col("s_comment").like("%Customer%Complaints%")) \
                .select("s_suppkey")

            return session.table("partsupp") \
                .join(
                    session.table("part"),
                    F.col("ps_partkey") == F.col("p_partkey")
                ) \
                .filter(
                    (F.col("p_brand") != "Brand#45") &
                    (~F.col("p_type").like("MEDIUM POLISHED%")) &
                    F.col("p_size").isin(49, 14, 23, 45, 19, 3, 36, 9)
                ) \
                .join(complaint_suppliers, F.col("ps_suppkey") == F.col("s_suppkey"), "left_anti") \
                .groupBy("p_brand", "p_type", "p_size") \
                .agg(F.countDistinct("ps_suppkey").alias("supplier_cnt")) \
                .orderBy(F.col("supplier_cnt").desc(), "p_brand", "p_type", "p_size")

        assert_dataframes_equal(
            build_q16(spark_reference), build_q16(spark_thunderduck),
            query_name="TPC-H Q16 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 17: Small-Quantity-Order Revenue
    # =========================================================================

    def test_q17_dataframe(self, spark_reference, spark_thunderduck):
        """Q17: Small-Quantity-Order Revenue (DataFrame API) - exact parity check.
        Correlated subquery via join with pre-aggregated AVG per part."""
        def build_q17(session):
            # Pre-compute average quantity per part
            # Rename l_partkey to avoid ambiguity when joining back to lineitem
            avg_qty = session.table("lineitem") \
                .groupBy("l_partkey") \
                .agg((F.avg("l_quantity") * 0.2).alias("avg_qty_threshold")) \
                .withColumnRenamed("l_partkey", "avg_l_partkey")

            return session.table("part") \
                .filter(
                    (F.col("p_brand") == "Brand#23") &
                    (F.col("p_container") == "MED BOX")
                ) \
                .join(session.table("lineitem"), F.col("p_partkey") == F.col("l_partkey")) \
                .join(avg_qty, F.col("l_partkey") == F.col("avg_l_partkey")) \
                .filter(F.col("l_quantity") < F.col("avg_qty_threshold")) \
                .agg(
                    (F.sum("l_extendedprice") / 7.0).alias("avg_yearly")
                )

        assert_dataframes_equal(
            build_q17(spark_reference), build_q17(spark_thunderduck),
            query_name="TPC-H Q17 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 18: Large Volume Customer
    # =========================================================================

    def test_q18_dataframe(self, spark_reference, spark_thunderduck):
        """Q18: Large Volume Customer (DataFrame API) - exact parity check.
        Semi-join for IN with HAVING, multi-join, groupBy, limit."""
        def build_q18(session):
            # Subquery: orderkeys where total quantity > 300
            large_orders = session.table("lineitem") \
                .groupBy("l_orderkey") \
                .agg(F.sum("l_quantity").alias("total_qty")) \
                .filter(F.col("total_qty") > 300) \
                .select("l_orderkey")

            return session.table("customer") \
                .join(session.table("orders"), F.col("c_custkey") == F.col("o_custkey")) \
                .join(large_orders, F.col("o_orderkey") == large_orders["l_orderkey"], "left_semi") \
                .join(session.table("lineitem"), F.col("o_orderkey") == F.col("l_orderkey")) \
                .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice") \
                .agg(F.sum("l_quantity").alias("total_qty")) \
                .orderBy(F.col("o_totalprice").desc(), "o_orderdate") \
                .limit(100)

        assert_dataframes_equal(
            build_q18(spark_reference), build_q18(spark_thunderduck),
            query_name="TPC-H Q18 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 19: Discounted Revenue
    # =========================================================================

    def test_q19_dataframe(self, spark_reference, spark_thunderduck):
        """Q19: Discounted Revenue (DataFrame API) - exact parity check.
        Join with complex OR conditions across 3 brand/container/size segments."""
        def build_q19(session):
            joined = session.table("lineitem") \
                .join(session.table("part"), F.col("l_partkey") == F.col("p_partkey"))

            # Three segments with brand/container/size/quantity filters
            seg1 = (
                (F.col("p_brand") == "Brand#12") &
                F.col("p_container").isin("SM CASE", "SM BOX", "SM PACK", "SM PKG") &
                (F.col("l_quantity") >= 1) & (F.col("l_quantity") <= 11) &
                (F.col("p_size").between(1, 5))
            )
            seg2 = (
                (F.col("p_brand") == "Brand#23") &
                F.col("p_container").isin("MED BAG", "MED BOX", "MED PKG", "MED PACK") &
                (F.col("l_quantity") >= 10) & (F.col("l_quantity") <= 20) &
                (F.col("p_size").between(1, 10))
            )
            seg3 = (
                (F.col("p_brand") == "Brand#34") &
                F.col("p_container").isin("LG CASE", "LG BOX", "LG PACK", "LG PKG") &
                (F.col("l_quantity") >= 20) & (F.col("l_quantity") <= 30) &
                (F.col("p_size").between(1, 15))
            )

            return joined \
                .filter(
                    (seg1 | seg2 | seg3) &
                    (F.col("l_shipmode").isin("AIR", "AIR REG")) &
                    (F.col("l_shipinstruct") == "DELIVER IN PERSON")
                ) \
                .agg(
                    F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
                )

        assert_dataframes_equal(
            build_q19(spark_reference), build_q19(spark_thunderduck),
            query_name="TPC-H Q19 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )

    # =========================================================================
    # Query 20: Potential Part Promotion
    # =========================================================================

    def test_q20_dataframe(self, spark_reference, spark_thunderduck):
        """Q20: Potential Part Promotion (DataFrame API) - exact parity check.
        Chained semi-joins for nested subqueries."""
        def build_q20(session):
            # Inner subquery: parts starting with 'forest'
            forest_parts = session.table("part") \
                .filter(F.col("p_name").like("forest%")) \
                .select("p_partkey")

            # Middle subquery: lineitem aggregation for threshold
            li_agg = session.table("lineitem") \
                .filter(
                    (F.col("l_shipdate") >= F.lit("1994-01-01")) &
                    (F.col("l_shipdate") < F.add_months(F.lit("1994-01-01").cast("date"), 12))
                ) \
                .groupBy("l_partkey", "l_suppkey") \
                .agg((F.sum("l_quantity") * 0.5).alias("qty_threshold"))

            # Partsupp filtered by forest parts and quantity threshold
            qualifying_ps = session.table("partsupp") \
                .join(forest_parts, F.col("ps_partkey") == F.col("p_partkey"), "left_semi") \
                .join(li_agg,
                      (F.col("ps_suppkey") == F.col("l_suppkey")) &
                      (F.col("ps_partkey") == F.col("l_partkey"))) \
                .filter(F.col("ps_availqty") > F.col("qty_threshold")) \
                .select("ps_suppkey")

            return session.table("supplier") \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .filter(F.col("n_name") == "CANADA") \
                .join(qualifying_ps, F.col("s_suppkey") == F.col("ps_suppkey"), "left_semi") \
                .select("s_name", "s_address") \
                .orderBy("s_name")

        assert_dataframes_equal(
            build_q20(spark_reference), build_q20(spark_thunderduck),
            query_name="TPC-H Q20 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 21: Suppliers Who Kept Orders Waiting
    # =========================================================================

    def test_q21_dataframe(self, spark_reference, spark_thunderduck):
        """Q21: Suppliers Who Kept Orders Waiting (DataFrame API) - exact parity check.
        Semi-join (EXISTS) + anti-join (NOT EXISTS), self-join on lineitem."""
        def build_q21(session):
            lineitem = session.table("lineitem")

            # L1: the main lineitem (late delivery)
            l1 = lineitem.alias("l1")

            # EXISTS: another supplier for same order (l2.l_suppkey != l1.l_suppkey)
            l2_exists = lineitem.select(
                F.col("l_orderkey").alias("l2_orderkey"),
                F.col("l_suppkey").alias("l2_suppkey")
            ).distinct()

            # NOT EXISTS: no other supplier that also delivered late
            l3_late = lineitem \
                .filter(F.col("l_receiptdate") > F.col("l_commitdate")) \
                .select(
                    F.col("l_orderkey").alias("l3_orderkey"),
                    F.col("l_suppkey").alias("l3_suppkey")
                ).distinct()

            base = session.table("supplier") \
                .join(l1, F.col("s_suppkey") == F.col("l1.l_suppkey")) \
                .join(session.table("orders"), F.col("l1.l_orderkey") == F.col("o_orderkey")) \
                .filter(F.col("o_orderstatus") == "F") \
                .filter(F.col("l1.l_receiptdate") > F.col("l1.l_commitdate")) \
                .join(session.table("nation"), F.col("s_nationkey") == F.col("n_nationkey")) \
                .filter(F.col("n_name") == "SAUDI ARABIA")

            # EXISTS: at least one other supplier on same order
            base_with_exists = base \
                .join(l2_exists,
                      (F.col("l1.l_orderkey") == F.col("l2_orderkey")) &
                      (F.col("l1.l_suppkey") != F.col("l2_suppkey")),
                      "left_semi")

            # NOT EXISTS: no other supplier with late delivery on same order
            base_final = base_with_exists \
                .join(l3_late,
                      (F.col("l1.l_orderkey") == F.col("l3_orderkey")) &
                      (F.col("l1.l_suppkey") != F.col("l3_suppkey")),
                      "left_anti")

            return base_final \
                .groupBy("s_name") \
                .agg(F.count("*").alias("numwait")) \
                .orderBy(F.col("numwait").desc(), "s_name") \
                .limit(100)

        assert_dataframes_equal(
            build_q21(spark_reference), build_q21(spark_thunderduck),
            query_name="TPC-H Q21 DataFrame",
            ignore_nullable=True
        )

    # =========================================================================
    # Query 22: Global Sales Opportunity
    # =========================================================================

    def test_q22_dataframe(self, spark_reference, spark_thunderduck):
        """Q22: Global Sales Opportunity (DataFrame API) - exact parity check.
        Substring for country code, anti-join for NOT EXISTS, avg subquery."""
        def build_q22(session):
            country_codes = ["13", "31", "23", "29", "30", "18", "17"]

            # Average account balance for customers with positive balance in target countries
            avg_bal = session.table("customer") \
                .filter(
                    (F.col("c_acctbal") > 0.00) &
                    F.substring("c_phone", 1, 2).isin(*country_codes)
                ) \
                .agg(F.avg("c_acctbal").alias("avg_acctbal"))

            # Customers with no orders (anti-join)
            custsale = session.table("customer") \
                .filter(F.substring("c_phone", 1, 2).isin(*country_codes)) \
                .join(session.table("orders"), F.col("c_custkey") == F.col("o_custkey"), "left_anti") \
                .crossJoin(avg_bal) \
                .filter(F.col("c_acctbal") > F.col("avg_acctbal")) \
                .select(
                    F.substring("c_phone", 1, 2).alias("cntrycode"),
                    "c_acctbal"
                )

            return custsale \
                .groupBy("cntrycode") \
                .agg(
                    F.count("*").alias("numcust"),
                    F.sum("c_acctbal").alias("totacctbal")
                ) \
                .orderBy("cntrycode")

        assert_dataframes_equal(
            build_q22(spark_reference), build_q22(spark_thunderduck),
            query_name="TPC-H Q22 DataFrame",
            ignore_nullable=True,
            epsilon=0.01
        )
