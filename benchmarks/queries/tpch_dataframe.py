#!/usr/bin/env python3
"""
TPC-H Benchmark Queries - DataFrame API Implementation

All 22 TPC-H queries implemented using PySpark DataFrame API.
These queries are designed to work with both Thunderduck and Spark.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Any, Callable, Dict


def q1(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q1: Pricing Summary Report"""
    return (
        tables["lineitem"]
        .filter(F.col("l_shipdate") <= "1998-09-02")
        .groupBy("l_returnflag", "l_linestatus")
        .agg(
            F.sum("l_quantity").alias("sum_qty"),
            F.sum("l_extendedprice").alias("sum_base_price"),
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax"))).alias("sum_charge"),
            F.avg("l_quantity").alias("avg_qty"),
            F.avg("l_extendedprice").alias("avg_price"),
            F.avg("l_discount").alias("avg_disc"),
            F.count("*").alias("count_order"),
        )
        .orderBy("l_returnflag", "l_linestatus")
    )


def q2(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q2: Minimum Cost Supplier"""
    europe = tables["region"].filter(F.col("r_name") == "EUROPE")

    nation_region = tables["nation"].join(
        europe, tables["nation"]["n_regionkey"] == europe["r_regionkey"]
    )

    supplier_nation = tables["supplier"].join(
        nation_region, tables["supplier"]["s_nationkey"] == nation_region["n_nationkey"]
    )

    partsupp_supplier = tables["partsupp"].join(
        supplier_nation, tables["partsupp"]["ps_suppkey"] == supplier_nation["s_suppkey"]
    )

    # Subquery: minimum supply cost per part
    min_cost = (
        partsupp_supplier
        .groupBy("ps_partkey")
        .agg(F.min("ps_supplycost").alias("min_supplycost"))
    )

    brass_parts = tables["part"].filter(
        (F.col("p_size") == 15) & F.col("p_type").endswith("BRASS")
    )

    result = (
        brass_parts
        .join(partsupp_supplier, brass_parts["p_partkey"] == partsupp_supplier["ps_partkey"])
        .join(min_cost, (partsupp_supplier["ps_partkey"] == min_cost["ps_partkey"]) &
                        (partsupp_supplier["ps_supplycost"] == min_cost["min_supplycost"]))
        .select(
            "s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr",
            "s_address", "s_phone", "s_comment"
        )
        .orderBy(F.desc("s_acctbal"), "n_name", "s_name", "p_partkey")
        .limit(100)
    )

    return result


def q3(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q3: Shipping Priority"""
    return (
        tables["customer"]
        .filter(F.col("c_mktsegment") == "BUILDING")
        .join(tables["orders"], tables["customer"]["c_custkey"] == tables["orders"]["o_custkey"])
        .filter(F.col("o_orderdate") < "1995-03-15")
        .join(tables["lineitem"], tables["orders"]["o_orderkey"] == tables["lineitem"]["l_orderkey"])
        .filter(F.col("l_shipdate") > "1995-03-15")
        .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
        .agg(
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
        )
        .orderBy(F.desc("revenue"), "o_orderdate")
        .limit(10)
    )


def q4(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q4: Order Priority Checking"""
    exists_lineitem = (
        tables["lineitem"]
        .filter(F.col("l_commitdate") < F.col("l_receiptdate"))
        .select("l_orderkey")
        .distinct()
    )

    return (
        tables["orders"]
        .filter(
            (F.col("o_orderdate") >= "1993-07-01") &
            (F.col("o_orderdate") < "1993-10-01")
        )
        .join(exists_lineitem, tables["orders"]["o_orderkey"] == exists_lineitem["l_orderkey"])
        .groupBy("o_orderpriority")
        .agg(F.count("*").alias("order_count"))
        .orderBy("o_orderpriority")
    )


def q5(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q5: Local Supplier Volume"""
    asia = tables["region"].filter(F.col("r_name") == "ASIA")

    return (
        tables["customer"]
        .join(tables["orders"], tables["customer"]["c_custkey"] == tables["orders"]["o_custkey"])
        .filter(
            (F.col("o_orderdate") >= "1994-01-01") &
            (F.col("o_orderdate") < "1995-01-01")
        )
        .join(tables["lineitem"], tables["orders"]["o_orderkey"] == tables["lineitem"]["l_orderkey"])
        .join(tables["supplier"],
              (tables["lineitem"]["l_suppkey"] == tables["supplier"]["s_suppkey"]) &
              (tables["customer"]["c_nationkey"] == tables["supplier"]["s_nationkey"]))
        .join(tables["nation"], tables["supplier"]["s_nationkey"] == tables["nation"]["n_nationkey"])
        .join(asia, tables["nation"]["n_regionkey"] == asia["r_regionkey"])
        .groupBy("n_name")
        .agg(
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
        )
        .orderBy(F.desc("revenue"))
    )


def q6(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q6: Forecasting Revenue Change"""
    return (
        tables["lineitem"]
        .filter(
            (F.col("l_shipdate") >= "1994-01-01") &
            (F.col("l_shipdate") < "1995-01-01") &
            (F.col("l_discount") >= 0.05) &
            (F.col("l_discount") <= 0.07) &
            (F.col("l_quantity") < 24)
        )
        .agg(
            F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")
        )
    )


def q7(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q7: Volume Shipping"""
    nations = tables["nation"].filter(
        F.col("n_name").isin("FRANCE", "GERMANY")
    )

    n1 = nations.alias("n1")
    n2 = nations.alias("n2")

    return (
        tables["supplier"]
        .join(n1, tables["supplier"]["s_nationkey"] == F.col("n1.n_nationkey"))
        .join(tables["lineitem"], tables["supplier"]["s_suppkey"] == tables["lineitem"]["l_suppkey"])
        .filter(
            (F.col("l_shipdate") >= "1995-01-01") &
            (F.col("l_shipdate") <= "1996-12-31")
        )
        .join(tables["orders"], tables["lineitem"]["l_orderkey"] == tables["orders"]["o_orderkey"])
        .join(tables["customer"], tables["orders"]["o_custkey"] == tables["customer"]["c_custkey"])
        .join(n2, tables["customer"]["c_nationkey"] == F.col("n2.n_nationkey"))
        .filter(
            ((F.col("n1.n_name") == "FRANCE") & (F.col("n2.n_name") == "GERMANY")) |
            ((F.col("n1.n_name") == "GERMANY") & (F.col("n2.n_name") == "FRANCE"))
        )
        .withColumn("l_year", F.year("l_shipdate"))
        .withColumn("volume", F.col("l_extendedprice") * (1 - F.col("l_discount")))
        .groupBy(F.col("n1.n_name").alias("supp_nation"),
                 F.col("n2.n_name").alias("cust_nation"),
                 "l_year")
        .agg(F.sum("volume").alias("revenue"))
        .orderBy("supp_nation", "cust_nation", "l_year")
    )


def q8(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q8: National Market Share"""
    america = tables["region"].filter(F.col("r_name") == "AMERICA")

    return (
        tables["part"]
        .filter(F.col("p_type") == "ECONOMY ANODIZED STEEL")
        .join(tables["lineitem"], tables["part"]["p_partkey"] == tables["lineitem"]["l_partkey"])
        .join(tables["supplier"], tables["lineitem"]["l_suppkey"] == tables["supplier"]["s_suppkey"])
        .join(tables["orders"], tables["lineitem"]["l_orderkey"] == tables["orders"]["o_orderkey"])
        .filter(
            (F.col("o_orderdate") >= "1995-01-01") &
            (F.col("o_orderdate") <= "1996-12-31")
        )
        .join(tables["customer"], tables["orders"]["o_custkey"] == tables["customer"]["c_custkey"])
        .join(tables["nation"].alias("n1"), tables["customer"]["c_nationkey"] == F.col("n1.n_nationkey"))
        .join(america, F.col("n1.n_regionkey") == america["r_regionkey"])
        .join(tables["nation"].alias("n2"), tables["supplier"]["s_nationkey"] == F.col("n2.n_nationkey"))
        .withColumn("o_year", F.year("o_orderdate"))
        .withColumn("volume", F.col("l_extendedprice") * (1 - F.col("l_discount")))
        .groupBy("o_year")
        .agg(
            (F.sum(F.when(F.col("n2.n_name") == "BRAZIL", F.col("volume")).otherwise(0)) /
             F.sum("volume")).alias("mkt_share")
        )
        .orderBy("o_year")
    )


def q9(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q9: Product Type Profit Measure"""
    return (
        tables["part"]
        .filter(F.col("p_name").contains("green"))
        .join(tables["lineitem"], tables["part"]["p_partkey"] == tables["lineitem"]["l_partkey"])
        .join(tables["supplier"], tables["lineitem"]["l_suppkey"] == tables["supplier"]["s_suppkey"])
        .join(tables["partsupp"],
              (tables["lineitem"]["l_suppkey"] == tables["partsupp"]["ps_suppkey"]) &
              (tables["lineitem"]["l_partkey"] == tables["partsupp"]["ps_partkey"]))
        .join(tables["orders"], tables["lineitem"]["l_orderkey"] == tables["orders"]["o_orderkey"])
        .join(tables["nation"], tables["supplier"]["s_nationkey"] == tables["nation"]["n_nationkey"])
        .withColumn("o_year", F.year("o_orderdate"))
        .withColumn("amount",
                    F.col("l_extendedprice") * (1 - F.col("l_discount")) -
                    F.col("ps_supplycost") * F.col("l_quantity"))
        .groupBy("n_name", "o_year")
        .agg(F.sum("amount").alias("sum_profit"))
        .orderBy("n_name", F.desc("o_year"))
    )


def q10(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q10: Returned Item Reporting"""
    return (
        tables["customer"]
        .join(tables["orders"], tables["customer"]["c_custkey"] == tables["orders"]["o_custkey"])
        .filter(
            (F.col("o_orderdate") >= "1993-10-01") &
            (F.col("o_orderdate") < "1994-01-01")
        )
        .join(tables["lineitem"], tables["orders"]["o_orderkey"] == tables["lineitem"]["l_orderkey"])
        .filter(F.col("l_returnflag") == "R")
        .join(tables["nation"], tables["customer"]["c_nationkey"] == tables["nation"]["n_nationkey"])
        .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
        .agg(
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
        )
        .orderBy(F.desc("revenue"))
        .limit(20)
    )


def q11(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q11: Important Stock Identification"""
    germany = tables["nation"].filter(F.col("n_name") == "GERMANY")

    ps_supp = (
        tables["partsupp"]
        .join(tables["supplier"], tables["partsupp"]["ps_suppkey"] == tables["supplier"]["s_suppkey"])
        .join(germany, tables["supplier"]["s_nationkey"] == germany["n_nationkey"])
    )

    threshold = (
        ps_supp
        .agg(F.sum(F.col("ps_supplycost") * F.col("ps_availqty")).alias("total"))
        .collect()[0]["total"] * 0.0001
    )

    return (
        ps_supp
        .groupBy("ps_partkey")
        .agg(F.sum(F.col("ps_supplycost") * F.col("ps_availqty")).alias("value"))
        .filter(F.col("value") > threshold)
        .orderBy(F.desc("value"))
    )


def q12(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q12: Shipping Modes and Order Priority"""
    return (
        tables["orders"]
        .join(tables["lineitem"], tables["orders"]["o_orderkey"] == tables["lineitem"]["l_orderkey"])
        .filter(
            F.col("l_shipmode").isin("MAIL", "SHIP") &
            (F.col("l_commitdate") < F.col("l_receiptdate")) &
            (F.col("l_shipdate") < F.col("l_commitdate")) &
            (F.col("l_receiptdate") >= "1994-01-01") &
            (F.col("l_receiptdate") < "1995-01-01")
        )
        .groupBy("l_shipmode")
        .agg(
            F.sum(F.when(F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0))
                .alias("high_line_count"),
            F.sum(F.when(~F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0))
                .alias("low_line_count"),
        )
        .orderBy("l_shipmode")
    )


def q13(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q13: Customer Distribution"""
    customer_orders = (
        tables["customer"]
        .join(
            tables["orders"].filter(~F.col("o_comment").rlike(".*special.*requests.*")),
            tables["customer"]["c_custkey"] == tables["orders"]["o_custkey"],
            "left"
        )
        .groupBy("c_custkey")
        .agg(F.count("o_orderkey").alias("c_count"))
    )

    return (
        customer_orders
        .groupBy("c_count")
        .agg(F.count("*").alias("custdist"))
        .orderBy(F.desc("custdist"), F.desc("c_count"))
    )


def q14(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q14: Promotion Effect"""
    return (
        tables["lineitem"]
        .filter(
            (F.col("l_shipdate") >= "1995-09-01") &
            (F.col("l_shipdate") < "1995-10-01")
        )
        .join(tables["part"], tables["lineitem"]["l_partkey"] == tables["part"]["p_partkey"])
        .agg(
            (100.0 * F.sum(
                F.when(F.col("p_type").startswith("PROMO"),
                       F.col("l_extendedprice") * (1 - F.col("l_discount")))
                .otherwise(0)
            ) / F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))))
            .alias("promo_revenue")
        )
    )


def q15(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q15: Top Supplier"""
    revenue = (
        tables["lineitem"]
        .filter(
            (F.col("l_shipdate") >= "1996-01-01") &
            (F.col("l_shipdate") < "1996-04-01")
        )
        .groupBy("l_suppkey")
        .agg(
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("total_revenue")
        )
    )

    max_revenue = revenue.agg(F.max("total_revenue")).collect()[0][0]

    return (
        tables["supplier"]
        .join(revenue, tables["supplier"]["s_suppkey"] == revenue["l_suppkey"])
        .filter(F.col("total_revenue") == max_revenue)
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .orderBy("s_suppkey")
    )


def q16(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q16: Parts/Supplier Relationship"""
    complaint_suppliers = (
        tables["supplier"]
        .filter(F.col("s_comment").rlike(".*Customer.*Complaints.*"))
        .select("s_suppkey")
    )

    return (
        tables["partsupp"]
        .join(
            tables["part"],
            tables["partsupp"]["ps_partkey"] == tables["part"]["p_partkey"]
        )
        .filter(
            (F.col("p_brand") != "Brand#45") &
            (~F.col("p_type").startswith("MEDIUM POLISHED")) &
            F.col("p_size").isin(49, 14, 23, 45, 19, 3, 36, 9)
        )
        .join(complaint_suppliers, tables["partsupp"]["ps_suppkey"] == complaint_suppliers["s_suppkey"], "left_anti")
        .groupBy("p_brand", "p_type", "p_size")
        .agg(F.countDistinct("ps_suppkey").alias("supplier_cnt"))
        .orderBy(F.desc("supplier_cnt"), "p_brand", "p_type", "p_size")
    )


def q17(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q17: Small-Quantity-Order Revenue"""
    avg_qty = (
        tables["lineitem"]
        .groupBy("l_partkey")
        .agg((0.2 * F.avg("l_quantity")).alias("avg_qty"))
    )

    return (
        tables["part"]
        .filter((F.col("p_brand") == "Brand#23") & (F.col("p_container") == "MED BOX"))
        .join(tables["lineitem"], tables["part"]["p_partkey"] == tables["lineitem"]["l_partkey"])
        .join(avg_qty, tables["lineitem"]["l_partkey"] == avg_qty["l_partkey"])
        .filter(F.col("l_quantity") < F.col("avg_qty"))
        .agg((F.sum("l_extendedprice") / 7.0).alias("avg_yearly"))
    )


def q18(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q18: Large Volume Customer"""
    large_orders = (
        tables["lineitem"]
        .groupBy("l_orderkey")
        .agg(F.sum("l_quantity").alias("sum_qty"))
        .filter(F.col("sum_qty") > 300)
    )

    return (
        tables["customer"]
        .join(tables["orders"], tables["customer"]["c_custkey"] == tables["orders"]["o_custkey"])
        .join(large_orders, tables["orders"]["o_orderkey"] == large_orders["l_orderkey"])
        .join(tables["lineitem"], tables["orders"]["o_orderkey"] == tables["lineitem"]["l_orderkey"])
        .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
        .agg(F.sum("l_quantity").alias("sum_qty"))
        .orderBy(F.desc("o_totalprice"), "o_orderdate")
        .limit(100)
    )


def q19(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q19: Discounted Revenue"""
    return (
        tables["lineitem"]
        .join(tables["part"], tables["lineitem"]["l_partkey"] == tables["part"]["p_partkey"])
        .filter(
            (
                (F.col("p_brand") == "Brand#12") &
                F.col("p_container").isin("SM CASE", "SM BOX", "SM PACK", "SM PKG") &
                (F.col("l_quantity") >= 1) & (F.col("l_quantity") <= 11) &
                (F.col("p_size") >= 1) & (F.col("p_size") <= 5)
            ) |
            (
                (F.col("p_brand") == "Brand#23") &
                F.col("p_container").isin("MED BAG", "MED BOX", "MED PKG", "MED PACK") &
                (F.col("l_quantity") >= 10) & (F.col("l_quantity") <= 20) &
                (F.col("p_size") >= 1) & (F.col("p_size") <= 10)
            ) |
            (
                (F.col("p_brand") == "Brand#34") &
                F.col("p_container").isin("LG CASE", "LG BOX", "LG PACK", "LG PKG") &
                (F.col("l_quantity") >= 20) & (F.col("l_quantity") <= 30) &
                (F.col("p_size") >= 1) & (F.col("p_size") <= 15)
            )
        )
        .filter(F.col("l_shipmode").isin("AIR", "AIR REG"))
        .filter(F.col("l_shipinstruct") == "DELIVER IN PERSON")
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
    )


def q20(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q20: Potential Part Promotion"""
    forest_parts = tables["part"].filter(F.col("p_name").startswith("forest"))

    lineitem_sum = (
        tables["lineitem"]
        .filter(
            (F.col("l_shipdate") >= "1994-01-01") &
            (F.col("l_shipdate") < "1995-01-01")
        )
        .groupBy("l_partkey", "l_suppkey")
        .agg((0.5 * F.sum("l_quantity")).alias("sum_qty"))
    )

    canada = tables["nation"].filter(F.col("n_name") == "CANADA")

    ps_filtered = (
        tables["partsupp"]
        .join(forest_parts, tables["partsupp"]["ps_partkey"] == forest_parts["p_partkey"])
        .join(lineitem_sum,
              (tables["partsupp"]["ps_partkey"] == lineitem_sum["l_partkey"]) &
              (tables["partsupp"]["ps_suppkey"] == lineitem_sum["l_suppkey"]))
        .filter(F.col("ps_availqty") > F.col("sum_qty"))
        .select("ps_suppkey")
        .distinct()
    )

    return (
        tables["supplier"]
        .join(canada, tables["supplier"]["s_nationkey"] == canada["n_nationkey"])
        .join(ps_filtered, tables["supplier"]["s_suppkey"] == ps_filtered["ps_suppkey"])
        .select("s_name", "s_address")
        .orderBy("s_name")
    )


def q21(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q21: Suppliers Who Kept Orders Waiting"""
    saudi = tables["nation"].filter(F.col("n_name") == "SAUDI ARABIA")

    l1 = tables["lineitem"].alias("l1")
    l2 = tables["lineitem"].alias("l2")
    l3 = tables["lineitem"].alias("l3")

    # Orders with at least one late lineitem
    late_orders = (
        tables["orders"]
        .filter(F.col("o_orderstatus") == "F")
        .join(l1.filter(F.col("l_receiptdate") > F.col("l_commitdate")),
              tables["orders"]["o_orderkey"] == F.col("l1.l_orderkey"))
    )

    # Suppliers from Saudi Arabia
    saudi_suppliers = (
        tables["supplier"]
        .join(saudi, tables["supplier"]["s_nationkey"] == saudi["n_nationkey"])
    )

    # Find suppliers with late orders where:
    # - There exists another supplier for the same order
    # - No other supplier delivered late
    result = (
        saudi_suppliers
        .join(late_orders, saudi_suppliers["s_suppkey"] == F.col("l1.l_suppkey"))
        .join(l2.filter(F.col("l2.l_suppkey") != F.col("l1.l_suppkey")),
              F.col("l1.l_orderkey") == F.col("l2.l_orderkey"))
        .join(
            l3.filter(
                (F.col("l3.l_suppkey") != F.col("l1.l_suppkey")) &
                (F.col("l3.l_receiptdate") > F.col("l3.l_commitdate"))
            ),
            F.col("l1.l_orderkey") == F.col("l3.l_orderkey"),
            "left_anti"
        )
        .groupBy("s_name")
        .agg(F.count("*").alias("numwait"))
        .orderBy(F.desc("numwait"), "s_name")
        .limit(100)
    )

    return result


def q22(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-H Q22: Global Sales Opportunity"""
    country_codes = ["13", "31", "23", "29", "30", "18", "17"]

    customers_with_code = (
        tables["customer"]
        .withColumn("cntrycode", F.substring("c_phone", 1, 2))
        .filter(F.col("cntrycode").isin(country_codes))
    )

    avg_balance = (
        customers_with_code
        .filter(F.col("c_acctbal") > 0)
        .agg(F.avg("c_acctbal").alias("avg_acctbal"))
        .collect()[0]["avg_acctbal"]
    )

    has_orders = (
        tables["orders"]
        .select("o_custkey")
        .distinct()
    )

    return (
        customers_with_code
        .filter(F.col("c_acctbal") > avg_balance)
        .join(has_orders, customers_with_code["c_custkey"] == has_orders["o_custkey"], "left_anti")
        .groupBy("cntrycode")
        .agg(
            F.count("*").alias("numcust"),
            F.sum("c_acctbal").alias("totacctbal")
        )
        .orderBy("cntrycode")
    )


# Export all queries
TPCH_QUERIES: Dict[str, Callable] = {
    "q1": q1,
    "q2": q2,
    "q3": q3,
    "q4": q4,
    "q5": q5,
    "q6": q6,
    "q7": q7,
    "q8": q8,
    "q9": q9,
    "q10": q10,
    "q11": q11,
    "q12": q12,
    "q13": q13,
    "q14": q14,
    "q15": q15,
    "q16": q16,
    "q17": q17,
    "q18": q18,
    "q19": q19,
    "q20": q20,
    "q21": q21,
    "q22": q22,
}
