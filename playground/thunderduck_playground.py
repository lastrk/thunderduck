import marimo

__generated_with = "0.18.4"
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

    ## Example 1: Basic Aggregation

    Count orders by status - a simple GROUP BY query.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    # Orders by status - simple groupBy aggregation
    def orders_by_status(spark):
        return (
            spark.table("orders")
            .groupBy("o_orderstatus")
            .agg(F.count("*").alias("order_count"))
            .orderBy("o_orderstatus")
        )

    comp1 = compare_results(
        orders_by_status(spark_td),
        orders_by_status(spark_ref),
        "Orders by Status"
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

    ## Example 2: Filter and Select

    Find high-value line items with discount applied.
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    # High-value line items with net price calculation
    def high_value_items(spark):
        lineitem = spark.table("lineitem")
        return (
            lineitem
            .select(
                F.col("l_orderkey"),
                F.col("l_partkey"),
                F.col("l_quantity"),
                F.col("l_extendedprice"),
                F.col("l_discount"),
                (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("net_price")
            )
            .filter(F.col("l_extendedprice") > 50000)
            .orderBy(F.desc("net_price"))
            .limit(20)
        )

    comp2 = compare_results(
        high_value_items(spark_td),
        high_value_items(spark_ref),
        "High-Value Line Items"
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

    ## Example 3: DataFrame API

    Same revenue-by-nation query using pure DataFrame operations (no SQL).
    """)
    return


@app.cell
def _(F, compare_results, format_comparison, mo, spark_ref, spark_td):
    def revenue_by_nation_df(spark):
        """Revenue by nation using DataFrame API."""
        customer = spark.table("customer")
        orders = spark.table("orders")
        lineitem = spark.table("lineitem")
        nation = spark.table("nation")

        return (
            customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy(nation.n_name.alias("nation"))
            .agg(F.sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)).alias("revenue"))
            .orderBy(F.desc("revenue"))
            .limit(10)
        )

    comp3 = compare_results(
        revenue_by_nation_df(spark_td),
        revenue_by_nation_df(spark_ref),
        "Revenue by Nation (DataFrame API)"
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

    ## Example 4: Window Functions

    Rank customers by total spending within each nation.
    """)
    return


@app.cell
def _(F, Window, compare_results, format_comparison, mo, spark_ref, spark_td):
    def top_customers_per_nation(spark):
        """Top 3 customers by spending in each nation."""
        customer = spark.table("customer")
        orders = spark.table("orders")
        nation = spark.table("nation")

        # Calculate customer spending
        customer_spending = (
            customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy("n_name", "c_custkey", "c_name")
            .agg(F.sum("o_totalprice").alias("total_spending"))
        )

        # Rank within nation
        window = Window.partitionBy("n_name").orderBy(F.desc("total_spending"))

        return (
            customer_spending
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= 3)
            .select("n_name", "c_name", "total_spending", "rank")
            .orderBy("n_name", "rank")
        )

    comp4 = compare_results(
        top_customers_per_nation(spark_td),
        top_customers_per_nation(spark_ref),
        "Top 3 Customers per Nation"
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

    ## Example 5: TPC-H Query 1 (Pricing Summary)

    **KNOWN ISSUE**: This example is currently disabled due to a Thunderduck compatibility gap.

    The query uses `F.date_sub(F.lit("1998-12-01"), 90)` which Spark handles by auto-casting
    the string to a DATE. Thunderduck/DuckDB requires explicit casting and throws:
    ```
    Binder Error: Could not choose a best candidate function for "-(STRING_LITERAL, INTERVAL)"
    ```

    This needs to be fixed in Thunderduck to match Spark's behavior.
    See: `docs/SPARK_CONNECT_GAP_ANALYSIS.md`
    """)
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
