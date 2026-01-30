#!/usr/bin/env python3
"""
TPC-DS Benchmark Queries - DataFrame API Implementation

A subset of TPC-DS queries implemented using PySpark DataFrame API.
These are the queries that translate well to DataFrame operations.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Any, Callable, Dict


def q3(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q3: Report the total extended sales price per item brand of specific manufacturer."""
    return (
        tables["date_dim"]
        .filter(F.col("d_moy") == 11)
        .join(tables["store_sales"],
              tables["date_dim"]["d_date_sk"] == tables["store_sales"]["ss_sold_date_sk"])
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_manufact_id") == 128)
        .groupBy("d_year", "i_brand", "i_brand_id")
        .agg(F.sum("ss_ext_sales_price").alias("sum_agg"))
        .orderBy("d_year", "sum_agg", "i_brand_id")
        .limit(100)
    )


def q7(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q7: Compute average quantity, price, discount, and profit."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter(F.col("d_year") == 2000)
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .join(tables["customer_demographics"],
              tables["store_sales"]["ss_cdemo_sk"] == tables["customer_demographics"]["cd_demo_sk"])
        .join(tables["promotion"],
              tables["store_sales"]["ss_promo_sk"] == tables["promotion"]["p_promo_sk"])
        .filter(
            (F.col("cd_gender") == "M") &
            (F.col("cd_marital_status") == "S") &
            (F.col("cd_education_status") == "College") &
            ((F.col("p_channel_email") == "N") | (F.col("p_channel_event") == "N"))
        )
        .groupBy("i_item_id")
        .agg(
            F.avg("ss_quantity").alias("agg1"),
            F.avg("ss_list_price").alias("agg2"),
            F.avg("ss_coupon_amt").alias("agg3"),
            F.avg("ss_sales_price").alias("agg4"),
        )
        .orderBy("i_item_id")
        .limit(100)
    )


def q12(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q12: Compute the revenue ratio across item classes."""
    return (
        tables["web_sales"]
        .join(tables["date_dim"],
              tables["web_sales"]["ws_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter(
            (F.col("d_date") >= "2001-01-12") &
            (F.col("d_date") <= F.date_add(F.lit("2001-01-12"), 90))
        )
        .join(tables["item"],
              tables["web_sales"]["ws_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_category").isin("Sports", "Books", "Home"))
        .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
        .agg(F.sum("ws_ext_sales_price").alias("itemrevenue"))
        .withColumn(
            "revenueratio",
            F.col("itemrevenue") * 100.0 /
            F.sum("itemrevenue").over(Window.partitionBy("i_class"))
        )
        .orderBy("i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio")
        .limit(100)
    )


def q19(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q19: Analyze returned items."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 1999) & (F.col("d_moy") == 11))
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .join(tables["customer"],
              tables["store_sales"]["ss_customer_sk"] == tables["customer"]["c_customer_sk"])
        .join(tables["customer_address"],
              tables["customer"]["c_current_addr_sk"] == tables["customer_address"]["ca_address_sk"])
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(
            F.col("i_manager_id") == 8
        )
        .filter(
            F.substring("ca_zip", 1, 5) != F.substring("s_zip", 1, 5)
        )
        .groupBy("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
        .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
        .orderBy(F.desc("ext_price"), "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
        .limit(100)
    )


def q27(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q27: Compute average quantity, average list price, and average sales price."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter(F.col("d_year") == 1998)
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_state") == "TN")
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .join(tables["customer_demographics"],
              tables["store_sales"]["ss_cdemo_sk"] == tables["customer_demographics"]["cd_demo_sk"])
        .filter(
            (F.col("cd_gender") == "M") &
            (F.col("cd_marital_status") == "S") &
            (F.col("cd_education_status") == "College")
        )
        .groupBy("i_item_id", "s_state")
        .agg(
            F.avg("ss_quantity").alias("agg1"),
            F.avg("ss_list_price").alias("agg2"),
            F.avg("ss_coupon_amt").alias("agg3"),
            F.avg("ss_sales_price").alias("agg4"),
        )
        .orderBy("i_item_id", "s_state")
        .limit(100)
    )


def q34(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q34: Display customers with specific buying behavior."""
    ss_filtered = (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter(
            (F.col("d_year") == 1999) &
            (F.col("d_dom") >= 1) & (F.col("d_dom") <= 3)
        )
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_county") == "Williamson County")
        .join(tables["household_demographics"],
              tables["store_sales"]["ss_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter(
            ((F.col("hd_buy_potential") == ">10000") | (F.col("hd_buy_potential") == "Unknown")) &
            (F.col("hd_vehicle_count") > 0) &
            (F.when(F.col("hd_vehicle_count") > 0,
                    F.col("hd_dep_count") / F.col("hd_vehicle_count"))
             .otherwise(0) > 1.2)
        )
        .groupBy("ss_ticket_number", "ss_customer_sk")
        .agg(F.count("*").alias("cnt"))
        .filter((F.col("cnt") >= 15) & (F.col("cnt") <= 20))
    )

    return (
        ss_filtered
        .join(tables["customer"],
              ss_filtered["ss_customer_sk"] == tables["customer"]["c_customer_sk"])
        .select(
            "c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag",
            "ss_ticket_number", "cnt"
        )
        .orderBy("c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag", "ss_ticket_number")
    )


def q42(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q42: For a given year and month, compute sum of sales by item category."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 2000) & (F.col("d_moy") == 12))
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_manager_id") == 1)
        .groupBy("d_year", "i_category_id", "i_category")
        .agg(F.sum("ss_ext_sales_price").alias("sum_sales"))
        .orderBy("sum_sales", "d_year", "i_category_id", "i_category")
        .limit(100)
    )


def q52(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q52: Compute extended price for item brand."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 2000) & (F.col("d_moy") == 12))
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_manager_id") == 1)
        .groupBy("d_year", "i_brand_id", "i_brand")
        .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
        .orderBy("d_year", "ext_price", "i_brand_id")
        .limit(100)
    )


def q55(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q55: Compute revenue for items by manager."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 2001) & (F.col("d_moy") == 11))
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_manager_id") == 28)
        .groupBy("i_brand_id", "i_brand")
        .agg(F.sum("ss_ext_sales_price").alias("ext_price"))
        .orderBy("ext_price", "i_brand_id")
        .limit(100)
    )


def q68(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q68: Compute store sales based on customer demographics."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 1999) & (F.col("d_moy").isin(4, 5, 6)))
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_city").isin("Midway", "Fairview"))
        .join(tables["customer"],
              tables["store_sales"]["ss_customer_sk"] == tables["customer"]["c_customer_sk"])
        .join(tables["customer_address"],
              tables["customer"]["c_current_addr_sk"] == tables["customer_address"]["ca_address_sk"])
        .join(tables["household_demographics"],
              tables["store_sales"]["ss_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter(
            ((F.col("hd_dep_count") == 4) | (F.col("hd_vehicle_count") == 3)) &
            (F.col("ca_city") != F.col("s_city"))
        )
        .groupBy(
            "ss_ticket_number", "ss_customer_sk", "ss_addr_sk",
            "ca_city", "c_first_name", "c_last_name"
        )
        .agg(
            F.sum("ss_coupon_amt").alias("bought_city_amt"),
            F.sum("ss_ext_sales_price").alias("bought_city_sales"),
        )
        .orderBy("c_last_name", "c_first_name", "ca_city", "bought_city_amt", "ss_ticket_number")
        .limit(100)
    )


def q73(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q73: Count customers by purchase behavior."""
    ss_dj = (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year").isin(1999, 2000, 2001)) & (F.col("d_dom") >= 1) & (F.col("d_dom") <= 2))
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_county").isin(
            "Williamson County", "Williamson County", "Williamson County", "Williamson County"
        ))
        .join(tables["household_demographics"],
              tables["store_sales"]["ss_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter((F.col("hd_buy_potential").isin("501-1000", "Unknown")) & (F.col("hd_vehicle_count") > 0))
        .filter(
            (F.col("hd_dep_count") / F.col("hd_vehicle_count")) > 1
        )
        .groupBy("ss_ticket_number", "ss_customer_sk")
        .agg(F.count("*").alias("cnt"))
        .filter((F.col("cnt") >= 1) & (F.col("cnt") <= 5))
    )

    return (
        ss_dj
        .join(tables["customer"],
              ss_dj["ss_customer_sk"] == tables["customer"]["c_customer_sk"])
        .select("c_last_name", "c_first_name", "c_salutation", "c_preferred_cust_flag", "ss_ticket_number", "cnt")
        .orderBy("cnt", F.desc("c_last_name"))
    )


def q79(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q79: Compute customer profitability."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 2000) & (F.col("d_dow").isin(6, 0)))
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_number_employees") >= 200)
        .filter(F.col("s_number_employees") <= 295)
        .join(tables["household_demographics"],
              tables["store_sales"]["ss_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter((F.col("hd_dep_count") == 6) | (F.col("hd_vehicle_count") > 2))
        .join(tables["customer"],
              tables["store_sales"]["ss_customer_sk"] == tables["customer"]["c_customer_sk"])
        .groupBy("c_last_name", "c_first_name", "ss_ticket_number")
        .agg(
            F.sum("ss_coupon_amt").alias("amt"),
            F.sum("ss_net_profit").alias("profit"),
        )
        .orderBy("c_last_name", "c_first_name", "ss_ticket_number")
        .limit(100)
    )


def q82(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q82: Report inventory quantities for items."""
    return (
        tables["item"]
        .filter((F.col("i_current_price") >= 30) & (F.col("i_current_price") <= 60))
        .filter(F.col("i_manufact_id").isin(129, 270, 821, 423))
        .join(tables["inventory"],
              tables["item"]["i_item_sk"] == tables["inventory"]["inv_item_sk"])
        .filter((F.col("inv_quantity_on_hand") >= 100) & (F.col("inv_quantity_on_hand") <= 500))
        .join(tables["date_dim"],
              tables["inventory"]["inv_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_date") >= "2001-05-01") & (F.col("d_date") <= "2001-07-31"))
        .join(tables["store_sales"],
              tables["item"]["i_item_sk"] == tables["store_sales"]["ss_item_sk"])
        .select("i_item_id", "i_item_desc", "i_current_price")
        .distinct()
        .orderBy("i_item_id")
        .limit(100)
    )


def q84(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q84: Report customer income band."""
    return (
        tables["store_returns"]
        .join(tables["customer_demographics"],
              tables["store_returns"]["sr_cdemo_sk"] == tables["customer_demographics"]["cd_demo_sk"])
        .filter(
            (F.col("cd_marital_status") == "M") &
            (F.col("cd_education_status") == "Unknown")
        )
        .join(tables["customer"],
              tables["store_returns"]["sr_customer_sk"] == tables["customer"]["c_customer_sk"])
        .join(tables["customer_address"],
              tables["customer"]["c_current_addr_sk"] == tables["customer_address"]["ca_address_sk"])
        .filter(F.col("ca_city") == "Edgewood")
        .join(tables["household_demographics"],
              tables["customer"]["c_current_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .join(tables["income_band"],
              tables["household_demographics"]["hd_income_band_sk"] == tables["income_band"]["ib_income_band_sk"])
        .filter((F.col("ib_lower_bound") >= 38128) & (F.col("ib_upper_bound") <= 88128))
        .select("c_customer_id")
        .distinct()
        .orderBy("c_customer_id")
        .limit(100)
    )


def q91(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q91: Compute call center returns by call center manager."""
    return (
        tables["catalog_returns"]
        .join(tables["date_dim"],
              tables["catalog_returns"]["cr_returned_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 2000) & (F.col("d_moy") == 12))
        .join(tables["call_center"],
              tables["catalog_returns"]["cr_call_center_sk"] == tables["call_center"]["cc_call_center_sk"])
        .join(tables["customer"],
              tables["catalog_returns"]["cr_returning_customer_sk"] == tables["customer"]["c_customer_sk"])
        .join(tables["customer_address"],
              tables["customer"]["c_current_addr_sk"] == tables["customer_address"]["ca_address_sk"])
        .filter(F.col("ca_gmt_offset") == -7)
        .join(tables["customer_demographics"],
              tables["catalog_returns"]["cr_returning_cdemo_sk"] == tables["customer_demographics"]["cd_demo_sk"])
        .filter(
            ((F.col("cd_marital_status") == "M") & (F.col("cd_education_status") == "Unknown")) |
            ((F.col("cd_marital_status") == "W") & (F.col("cd_education_status") == "Advanced Degree"))
        )
        .join(tables["household_demographics"],
              tables["catalog_returns"]["cr_returning_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter(F.col("hd_buy_potential").like("Unknown%"))
        .groupBy("cc_call_center_id", "cc_name", "cc_manager")
        .agg(F.sum("cr_net_loss").alias("net_loss"))
        .orderBy("net_loss")
    )


def q96(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q96: Count store sales by time and household demographics."""
    return (
        tables["store_sales"]
        .join(tables["time_dim"],
              tables["store_sales"]["ss_sold_time_sk"] == tables["time_dim"]["t_time_sk"])
        .filter((F.col("t_hour") == 20) & (F.col("t_minute") >= 30))
        .join(tables["store"],
              tables["store_sales"]["ss_store_sk"] == tables["store"]["s_store_sk"])
        .filter(F.col("s_store_name") == "ese")
        .join(tables["household_demographics"],
              tables["store_sales"]["ss_hdemo_sk"] == tables["household_demographics"]["hd_demo_sk"])
        .filter(F.col("hd_dep_count") == 7)
        .agg(F.count("*").alias("count"))
    )


def q98(spark: SparkSession, tables: Dict[str, DataFrame]) -> DataFrame:
    """TPC-DS Q98: Compute store sales by item."""
    return (
        tables["store_sales"]
        .join(tables["date_dim"],
              tables["store_sales"]["ss_sold_date_sk"] == tables["date_dim"]["d_date_sk"])
        .filter((F.col("d_year") == 1999) & (F.col("d_moy") == 5))
        .join(tables["item"],
              tables["store_sales"]["ss_item_sk"] == tables["item"]["i_item_sk"])
        .filter(F.col("i_category").isin("Books", "Electronics", "Sports"))
        .groupBy("i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price")
        .agg(F.sum("ss_ext_sales_price").alias("itemrevenue"))
        .withColumn(
            "revenueratio",
            F.col("itemrevenue") * 100.0 /
            F.sum("itemrevenue").over(Window.partitionBy("i_class"))
        )
        .orderBy("i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio")
        .limit(100)
    )


# Export all queries
TPCDS_QUERIES: Dict[str, Callable] = {
    "q3": q3,
    "q7": q7,
    "q12": q12,
    "q19": q19,
    "q27": q27,
    "q34": q34,
    "q42": q42,
    "q52": q52,
    "q55": q55,
    "q68": q68,
    "q73": q73,
    "q79": q79,
    "q82": q82,
    "q84": q84,
    "q91": q91,
    "q96": q96,
    "q98": q98,
}
