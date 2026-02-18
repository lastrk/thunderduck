import marimo

__generated_with = "0.19.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Thunderduck Playground

    Compare **Thunderduck** vs **Apache Spark** performance side-by-side using TPC-H benchmark data.

    This notebook provides:
    - Pre-configured PySpark clients for both engines
    - TPC-H sample data (SF0.01 - ~3MB, 8 tables)
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
    import subprocess
    import time
    from pathlib import Path

    import duckdb
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

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
        duckdb,
        time,
    )


@app.cell
def _(GENERATED_DATA_DIR, duckdb, mo):
    import tempfile
    import shutil

    def generate_tpch_data(scale_factor: float = 1.0):
        """
        Generate TPC-H data using DuckDB's built-in tpch extension.

        Uses a temporary disk-backed database to handle large scale factors
        without running out of memory.

        Args:
            scale_factor: TPC-H scale factor (1.0 = ~1GB, 0.1 = ~100MB, 10 = ~10GB)
        """
        output_dir = GENERATED_DATA_DIR / f"tpch_sf{scale_factor}"
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Generating TPC-H data (SF={scale_factor})...")
        print(f"Output directory: {output_dir}")

        # Use a temporary file-based database to handle large datasets
        # DuckDB will spill to disk instead of running out of memory
        temp_dir = tempfile.mkdtemp(prefix="tpch_gen_")
        temp_db = f"{temp_dir}/tpch.duckdb"

        try:
            conn = duckdb.connect(temp_db)

            # Configure for large data generation
            conn.execute("SET memory_limit='4GB';")  # Limit memory, spill to disk
            conn.execute("SET threads=4;")  # Parallel generation

            # Install and load TPC-H extension
            print("Installing TPC-H extension...")
            conn.execute("INSTALL tpch;")
            conn.execute("LOAD tpch;")

            # Generate data
            print(f"Generating data (this may take a while for SF={scale_factor})...")
            conn.execute(f"CALL dbgen(sf={scale_factor});")

            # Export each table to parquet
            tables = ['lineitem', 'orders', 'customer', 'part',
                      'supplier', 'partsupp', 'nation', 'region']

            total_size = 0
            for table in tables:
                output_file = output_dir / f"{table}.parquet"
                print(f"  Exporting {table}...")
                conn.execute(f"COPY {table} TO '{output_file}' (FORMAT PARQUET);")
                size_mb = output_file.stat().st_size / (1024 * 1024)
                total_size += size_mb
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"    {table}: {count:,} rows ({size_mb:.1f} MB)")

            conn.close()

            print(f"\nTotal size: {total_size:.1f} MB")
            print(f"Data ready at: {output_dir}")
            return output_dir

        finally:
            # Clean up temporary database
            shutil.rmtree(temp_dir, ignore_errors=True)

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
    # generate_tpch_data(scale_factor=20.0)
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
def _(FALLBACK_DATA_DIR, GENERATED_DATA_DIR, spark_ref, spark_td):
    # Load TPC-H tables into both sessions
    TPCH_TABLES = ['lineitem', 'orders', 'customer', 'part',
                   'supplier', 'partsupp', 'nation', 'region']

    # Check for generated data (prefer larger scale factors)
    def find_best_data_dir():
        """Find the best available TPC-H data directory."""
        # Check for generated data at various scale factors
        # for sf in [20.0, 10.0, 1.0, 0.1]:
        for sf in [20.0]:
            sf_dir = GENERATED_DATA_DIR / f"tpch_sf{sf}"
            if sf_dir.exists() and (sf_dir / "lineitem.parquet").exists():
                return sf_dir, sf
        # Fall back to default small dataset
        if FALLBACK_DATA_DIR.exists():
            return FALLBACK_DATA_DIR, 0.01
        return None, None

    data_dir, scale_factor = find_best_data_dir()

    if data_dir is None:
        print("ERROR: No TPC-H data found!")
        print(f"  - Generated data expected at: {GENERATED_DATA_DIR}")
        print(f"  - Fallback data expected at: {FALLBACK_DATA_DIR}")
        raise FileNotFoundError("TPC-H data not found")

    def load_tables(spark, data_dir, tables):
        """Load parquet tables and register as temp views."""
        for table in tables:
            path = str(data_dir / f"{table}.parquet")
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(table)

    # Load into both sessions
    load_tables(spark_td, data_dir, TPCH_TABLES)
    load_tables(spark_ref, data_dir, TPCH_TABLES)

    print(f"Loaded {len(TPCH_TABLES)} TPC-H tables (SF={scale_factor})")
    print(f"Data source: {data_dir}")
    return


@app.cell
def _(time):
    def compare_results(df_td, df_ref, title="Comparison"):
        """
        Execute query on both engines, compare results and timing.

        Args:
            df_td: DataFrame from Thunderduck session
            df_ref: DataFrame from Spark reference session
            title: Title for the comparison

        Returns:
            Dictionary with timing and result comparison
        """
        # Execute Thunderduck
        start_td = time.time()
        result_td = df_td.toPandas()
        time_td = time.time() - start_td

        # Execute Spark
        start_ref = time.time()
        result_ref = df_ref.toPandas()
        time_ref = time.time() - start_ref

        # Compare results
        rows_match = len(result_td) == len(result_ref)

        # Sort both for comparison (order may differ)
        if rows_match and len(result_td) > 0:
            cols = list(result_td.columns)
            result_td_sorted = result_td.sort_values(by=cols).reset_index(drop=True)
            result_ref_sorted = result_ref.sort_values(by=cols).reset_index(drop=True)
            try:
                values_match = result_td_sorted.equals(result_ref_sorted)
            except Exception:
                values_match = False
        else:
            values_match = rows_match

        # Calculate speedup
        speedup = time_ref / time_td if time_td > 0 else float('inf')

        return {
            'title': title,
            'thunderduck_time_ms': time_td * 1000,
            'spark_time_ms': time_ref * 1000,
            'speedup': speedup,
            'rows_td': len(result_td),
            'rows_ref': len(result_ref),
            'match': rows_match and values_match,
            'result': result_td
        }


    def format_comparison(comp):
        """Format comparison result as a summary string."""
        match_symbol = "✓" if comp['match'] else "✗"
        return f"""
    ### {comp['title']}

    | Metric | Thunderduck | Spark |
    |--------|-------------|-------|
    | **Time** | {comp['thunderduck_time_ms']:.1f}ms | {comp['spark_time_ms']:.1f}ms |
    | **Speedup** | **{comp['speedup']:.1f}x** | 1.0x |
    | **Rows** | {comp['rows_td']} | {comp['rows_ref']} |
    | **Match** | {match_symbol} | - |
    """

    return compare_results, format_comparison


@app.cell
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


@app.cell
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


@app.cell
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


@app.cell
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
