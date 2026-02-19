import marimo

__generated_with = "0.19.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Thunderduck Playground

    Compare **Thunderduck** vs **Apache Spark** performance side-by-side using TPC-H benchmark data.

    This notebook provides:
    - Pre-configured PySpark clients for both engines
    - TPC-H sample data (8 tables, SF0.01 - ~3MB by default, but can be changed to generate more realistic size sample data)
    - Example DataFrame API transformations
    - Performance comparison utilities

    ---

    ## How it works

    Two SparkSession clients are configured:
    - `spark_td` → **Thunderduck** (DuckDB-powered, single-node optimized)
    - `spark_ref` → **Apache Spark** (reference implementation)

    All examples use the **PySpark DataFrame API** (filter, select, groupBy, join, etc.).
    Run any cell to execute transformations on both engines and compare results.
    """)
    return


@app.cell
def _():
    import os
    from pathlib import Path

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    from util import (
        compare_results,
        find_best_data_dir,
        format_comparison,
        generate_tpch_data,
        load_tables,
        warmup_tables,
    )

    # Connection URLs from environment (set by launch.sh)
    THUNDERDUCK_URL = os.environ.get("THUNDERDUCK_URL", "sc://localhost:15002")
    SPARK_URL = os.environ.get("SPARK_URL", "sc://localhost:15003")

    # Data directories - prefer generated data if available
    PLAYGROUND_DIR = Path(__file__).parent if "__file__" in dir() else Path("playground")
    GENERATED_DATA_DIR = PLAYGROUND_DIR / "data"
    FALLBACK_DATA_DIR = Path(os.environ.get("TPCH_DATA_DIR", "tests/integration/tpch_sf001"))
    return (
        F,
        FALLBACK_DATA_DIR,
        GENERATED_DATA_DIR,
        SPARK_URL,
        SparkSession,
        THUNDERDUCK_URL,
        Window,
        compare_results,
        find_best_data_dir,
        format_comparison,
        load_tables,
        warmup_tables,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Generate TPC-H Data

    The default TPC-H data (SF0.01) is too small to show Thunderduck's performance advantages.
    Run the cell below to generate larger datasets.

    **Scale Factor Reference:**
    - SF 0.01 = ~3 MB (default, too small)
    - SF 0.1 = ~100 MB
    - SF 1.0 = ~1 GB (recommended for testing)
    - SF 10.0 = ~10 GB (good for benchmarks)
    - SF 20.0 = ~20 GB (uses disk-backed temp DB)

    *Note: Large scale factors use a temporary disk-backed database to avoid OOM.*
    """)
    return


@app.cell
def _():
    # Uncomment to generate TPC-H data at SF=1 (~1GB)
    # This only needs to be run once - data is persisted to playground/data/
    #
    # generate_tpch_data(GENERATED_DATA_DIR, scale_factor=20.0)
    pass
    return


@app.cell
def _(SPARK_URL, SparkSession, THUNDERDUCK_URL):
    # Initialize Thunderduck client
    spark_td = (
        SparkSession.builder
        .appName("Thunderduck-Playground")
        .remote(THUNDERDUCK_URL)
        .getOrCreate()
    )

    # Initialize Spark reference client
    spark_ref = (
        SparkSession.builder
        .appName("Spark-Reference-Playground")
        .remote(SPARK_URL)
        .getOrCreate()
    )

    print(f"Thunderduck: {THUNDERDUCK_URL}")
    print(f"Spark:       {SPARK_URL}")
    return spark_ref, spark_td


@app.cell
def _(
    FALLBACK_DATA_DIR,
    GENERATED_DATA_DIR,
    find_best_data_dir,
    load_tables,
    spark_ref,
    spark_td,
    warmup_tables,
):
    data_dir, scale_factor = find_best_data_dir(GENERATED_DATA_DIR, FALLBACK_DATA_DIR)

    if data_dir is None:
        print("ERROR: No TPC-H data found!")
        print(f"  - Generated data expected at: {GENERATED_DATA_DIR}")
        print(f"  - Fallback data expected at: {FALLBACK_DATA_DIR}")
        raise FileNotFoundError("TPC-H data not found")

    # Load tables into both sessions
    load_tables(spark_td, data_dir)
    load_tables(spark_ref, data_dir)
    print(f"Loaded TPC-H tables (SF={scale_factor}) from {data_dir}")

    # Warm up caches so neither engine pays cold-read penalty
    print("Warming up Thunderduck cache...")
    warmup_tables(spark_td)
    print("Warming up Spark cache...")
    warmup_tables(spark_ref)
    print("Cache warmup complete.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Example 1: Full Scan + Heavy Aggregation (TPC-H Q1)

    Scan the entire `lineitem` table and compute 8 aggregate functions grouped by return flag
    and line status. This is the canonical OLAP benchmark -- pure vectorized scan and
    aggregation speed with no joins, no limits, and no early termination.

    At SF=20 this processes ~120 million rows.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    def pricing_summary(spark):
        lineitem = spark.table("lineitem")
        return (
            lineitem
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

    comp1 = compare_results(
        pricing_summary(spark_td),
        pricing_summary(spark_ref),
        "Pricing Summary (TPC-H Q1)"
    )

    mo.vstack([
        mo.md(format_comparison(comp1)),
        mo.ui.table(comp1['result'], label="Results")
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Example 2: Vectorized Filter + Aggregation (TPC-H Q6)

    Full scan of `lineitem` with multiple filter predicates and a single SUM.
    Tests how fast the engine can evaluate compound filter expressions across
    every row and aggregate the survivors. No joins, no grouping -- just raw
    scan-filter-aggregate throughput.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    def forecasting_revenue(spark):
        lineitem = spark.table("lineitem")
        return (
            lineitem
            .filter(
                (F.col("l_shipdate") >= "1994-01-01")
                & (F.col("l_shipdate") < "1995-01-01")
                & (F.col("l_discount").between(0.05, 0.07))
                & (F.col("l_quantity") < 24)
            )
            .agg(
                F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")
            )
        )

    comp2 = compare_results(
        forecasting_revenue(spark_td),
        forecasting_revenue(spark_ref),
        "Forecasting Revenue Change (TPC-H Q6)"
    )

    mo.vstack([
        mo.md(format_comparison(comp2)),
        mo.ui.table(comp2['result'], label="Results")
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Example 3: Large-Table Hash Join

    Join `lineitem` with `partsupp` -- both are large tables (120M x 16M rows at SF=20).
    Neither table is small enough for broadcast, so both engines must use a hash or
    sort-merge join. DuckDB's single-process hash join avoids Spark's shuffle overhead.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    def large_join_profit(spark):
        lineitem = spark.table("lineitem")
        partsupp = spark.table("partsupp")
        return (
            lineitem
            .join(
                partsupp,
                (lineitem.l_partkey == partsupp.ps_partkey)
                & (lineitem.l_suppkey == partsupp.ps_suppkey),
            )
            .groupBy("l_returnflag")
            .agg(
                F.sum(
                    F.col("l_extendedprice") - F.col("ps_supplycost") * F.col("l_quantity")
                ).alias("profit"),
                F.count("*").alias("row_count"),
            )
            .orderBy("l_returnflag")
        )

    comp3 = compare_results(
        large_join_profit(spark_td),
        large_join_profit(spark_ref),
        "Lineitem-Partsupp Join Profit"
    )

    mo.vstack([
        mo.md(format_comparison(comp3)),
        mo.ui.table(comp3['result'], label="Results")
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ---

    ## Example 4: Multi-Level Aggregation Rollup

    Two-stage aggregation: first compute per-customer spending totals (join + GROUP BY
    over millions of rows), then roll up to nation-level statistics. Tests nested
    aggregation pipelines where Spark must shuffle twice.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    def multi_level_rollup(spark):
        customer = spark.table("customer")
        orders = spark.table("orders")
        nation = spark.table("nation")

        per_customer = (
            customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy("n_name", "c_custkey")
            .agg(
                F.sum("o_totalprice").alias("spending"),
                F.count("*").alias("order_count"),
            )
        )

        return (
            per_customer
            .groupBy("n_name")
            .agg(
                F.count("*").alias("num_customers"),
                F.sum("spending").alias("total_revenue"),
                F.avg("spending").alias("avg_spending"),
                F.max("spending").alias("max_spending"),
                F.avg("order_count").alias("avg_orders"),
            )
            .orderBy(F.desc("total_revenue"))
        )

    comp4 = compare_results(
        multi_level_rollup(spark_td),
        multi_level_rollup(spark_ref),
        "Nation-Level Customer Rollup"
    )

    mo.vstack([
        mo.md(format_comparison(comp4)),
        mo.ui.table(comp4['result'], label="Results")
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Example 5: Window Functions at Scale

    Compute running revenue totals per customer using a window function over the full
    `orders` table, then find each nation's top 3 customers. The window must sort and
    scan all rows -- no shortcut via LIMIT.
    """)
    return


@app.cell
def _(F, Window, compare_results, format_comparison, mo, spark_ref, spark_td):
    def window_top_customers(spark):
        customer = spark.table("customer")
        orders = spark.table("orders")
        nation = spark.table("nation")

        # Window over full orders table: cumulative spending per customer
        customer_spending = (
            customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy("n_name", "c_custkey", "c_name")
            .agg(F.sum("o_totalprice").alias("total_spending"))
        )

        # Rank all customers within each nation (full sort, no limit pushdown)
        window = Window.partitionBy("n_name").orderBy(F.desc("total_spending"))

        ranked = (
            customer_spending
            .withColumn("nation_rank", F.rank().over(window))
            .withColumn(
                "pct_of_nation",
                F.col("total_spending") / F.sum("total_spending").over(
                    Window.partitionBy("n_name")
                ) * 100,
            )
        )

        return (
            ranked
            .filter(F.col("nation_rank") <= 3)
            .select("n_name", "c_name", "total_spending", "nation_rank", "pct_of_nation")
            .orderBy("n_name", "nation_rank")
        )

    comp5 = compare_results(
        window_top_customers(spark_td),
        window_top_customers(spark_ref),
        "Top 3 Customers per Nation (Window)"
    )

    mo.vstack([
        mo.md(format_comparison(comp5)),
        mo.ui.table(comp5['result'], label="Results")
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Try Your Own Transformations

    Edit the DataFrame code below to run your own experiments.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    # Your custom DataFrame transformation
    # Modify this function to try different operations!
    def custom_transform(spark):
        lineitem = spark.table("lineitem")
        return (
            lineitem
            .agg(
                F.count("*").alias("total_lineitems"),
                F.sum("l_quantity").alias("total_quantity"),
                F.avg("l_extendedprice").alias("avg_price")
            )
        )

    custom_comp = compare_results(
        custom_transform(spark_td),
        custom_transform(spark_ref),
        "Custom Transformation"
    )

    mo.vstack([
        mo.md(format_comparison(custom_comp)),
        mo.ui.table(custom_comp['result'], label="Results")
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## Available Tables

    The following TPC-H tables are available via `spark.table("name")`:

    | Table | Description | Key Columns |
    |-------|-------------|-------------|
    | `lineitem` | Order line items | l_orderkey, l_partkey, l_quantity, l_extendedprice |
    | `orders` | Customer orders | o_orderkey, o_custkey, o_totalprice, o_orderdate |
    | `customer` | Customer info | c_custkey, c_name, c_nationkey |
    | `part` | Parts catalog | p_partkey, p_name, p_retailprice |
    | `supplier` | Suppliers | s_suppkey, s_name, s_nationkey |
    | `partsupp` | Part-supplier | ps_partkey, ps_suppkey, ps_supplycost |
    | `nation` | Nations | n_nationkey, n_name, n_regionkey |
    | `region` | Regions | r_regionkey, r_name |

    ### DataFrame API Quick Reference

    ```python
    # Get a table
    df = spark.table("lineitem")

    # Filter rows
    df.filter(F.col("l_quantity") > 10)

    # Select columns
    df.select("l_orderkey", "l_quantity")

    # Add computed column
    df.withColumn("net_price", F.col("l_extendedprice") * (1 - F.col("l_discount")))

    # Aggregate
    df.groupBy("l_returnflag").agg(F.sum("l_quantity").alias("total_qty"))

    # Join tables
    orders.join(lineitem, orders.o_orderkey == lineitem.l_orderkey)

    # Window functions
    from pyspark.sql.window import Window
    window = Window.partitionBy("col1").orderBy("col2")
    df.withColumn("rank", F.rank().over(window))
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
