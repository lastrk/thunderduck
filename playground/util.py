"""Playground utilities: data generation, table loading, cache warmup, and comparison."""

import shutil
import tempfile
import time
from pathlib import Path

import duckdb

TPCH_TABLES = [
    "lineitem",
    "orders",
    "customer",
    "part",
    "supplier",
    "partsupp",
    "nation",
    "region",
]

TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def generate_tpch_data(base_dir: Path, scale_factor: float = 1.0) -> Path:
    """Generate TPC-H parquet data using DuckDB's tpch extension.

    Uses a temporary disk-backed database to handle large scale factors
    without running out of memory.

    Args:
        base_dir: Parent directory (a tpch_sf{scale_factor} subdirectory is created).
        scale_factor: TPC-H scale factor (1.0 ~ 1 GB, 10.0 ~ 10 GB, etc.).

    Returns:
        Path to the generated data directory.
    """
    output_dir = base_dir / f"tpch_sf{scale_factor}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-H data (SF={scale_factor})...")
    print(f"Output directory: {output_dir}")

    temp_dir = tempfile.mkdtemp(prefix="tpch_gen_")
    temp_db = f"{temp_dir}/tpch.duckdb"

    try:
        conn = duckdb.connect(temp_db)
        conn.execute("SET memory_limit='4GB';")
        conn.execute("SET threads=4;")

        print("Installing TPC-H extension...")
        conn.execute("INSTALL tpch;")
        conn.execute("LOAD tpch;")

        print(f"Generating data (this may take a while for SF={scale_factor})...")
        conn.execute(f"CALL dbgen(sf={scale_factor});")

        total_size = 0
        for table in TPCH_TABLES:
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
        shutil.rmtree(temp_dir, ignore_errors=True)


def generate_tpcds_data(base_dir: Path, scale_factor: float = 20.0) -> Path:
    """Generate TPC-DS parquet data using DuckDB's tpcds extension.

    Uses a temporary disk-backed database to handle large scale factors
    without running out of memory.

    Args:
        base_dir: Parent directory (a tpcds_sf{scale_factor} subdirectory is created).
        scale_factor: TPC-DS scale factor (1.0 ~ 1 GB, 20.0 ~ 20 GB, etc.).

    Returns:
        Path to the generated data directory.
    """
    output_dir = base_dir / f"tpcds_sf{scale_factor}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-DS data (SF={scale_factor})...")
    print(f"Output directory: {output_dir}")

    temp_dir = tempfile.mkdtemp(prefix="tpcds_gen_")
    temp_db = f"{temp_dir}/tpcds.duckdb"

    try:
        conn = duckdb.connect(temp_db)
        conn.execute("SET memory_limit='4GB';")
        conn.execute("SET threads=4;")

        print("Installing TPC-DS extension...")
        conn.execute("INSTALL tpcds;")
        conn.execute("LOAD tpcds;")

        print(f"Generating data (this may take a while for SF={scale_factor})...")
        conn.execute(f"CALL dsdgen(sf={scale_factor});")

        total_size = 0
        for table in TPCDS_TABLES:
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
        shutil.rmtree(temp_dir, ignore_errors=True)


def find_best_tpcds_data_dir(generated_data_dir: Path):
    """Find the best available TPC-DS data directory.

    Checks generated data at scale factors 20, 10, 1 (largest first).

    Returns:
        Tuple of (data_dir, scale_factor) or (None, None) if nothing found.
    """
    for sf in [20.0, 10.0, 1.0]:
        sf_dir = generated_data_dir / f"tpcds_sf{sf}"
        if sf_dir.exists() and (sf_dir / "store_sales.parquet").exists():
            return sf_dir, sf
    return None, None


def find_best_data_dir(generated_data_dir: Path, fallback_data_dir: Path):
    """Find the best available TPC-H data directory.

    Checks generated data at scale factors 20, 10, 1, and 0.1 (largest first),
    then falls back to the default small dataset.

    Returns:
        Tuple of (data_dir, scale_factor) or (None, None) if nothing found.
    """
    for sf in [20.0, 10.0, 1.0, 0.1]:
        sf_dir = generated_data_dir / f"tpch_sf{sf}"
        if sf_dir.exists() and (sf_dir / "lineitem.parquet").exists():
            return sf_dir, sf
    if fallback_data_dir.exists():
        return fallback_data_dir, 0.01
    return None, None


def load_tables(spark, data_dir, tables=None):
    """Load parquet tables and register as temp views.

    Args:
        spark: SparkSession (Thunderduck or Spark).
        data_dir: Path containing {table}.parquet files.
        tables: List of table names (defaults to TPCH_TABLES).
    """
    if tables is None:
        tables = TPCH_TABLES
    for table in tables:
        path = str(Path(data_dir) / f"{table}.parquet")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(table)


def warmup_tables(spark, tables=None):
    """Force a full scan of every table to prime the OS page cache.

    Calls .count() on each table, which reads all parquet metadata and at
    least one column per row group.

    Args:
        spark: SparkSession to warm up.
        tables: List of table names (defaults to TPCH_TABLES).
    """
    if tables is None:
        tables = TPCH_TABLES
    for table in tables:
        spark.table(table).count()


def load_tables_duckdb(conn, data_dir, tables=None):
    """Register parquet files as DuckDB views.

    Args:
        conn: DuckDB connection.
        data_dir: Path containing {table}.parquet files.
        tables: List of table names (defaults to TPCH_TABLES).
    """
    if tables is None:
        tables = TPCH_TABLES
    for table in tables:
        path = str(Path(data_dir) / f"{table}.parquet")
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
        )


def warmup_tables_duckdb(conn, tables=None):
    """Force a full scan of every DuckDB table to prime the OS page cache.

    Args:
        conn: DuckDB connection.
        tables: List of table names (defaults to TPCH_TABLES).
    """
    if tables is None:
        tables = TPCH_TABLES
    for table in tables:
        conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()


def compare_results(df_td, df_ref, title="Comparison"):
    """Execute a query on both engines, compare results and timing.

    Args:
        df_td: DataFrame from Thunderduck session.
        df_ref: DataFrame from Spark reference session.
        title: Title for the comparison.

    Returns:
        Dictionary with timing and result comparison.
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

    speedup = time_ref / time_td if time_td > 0 else float("inf")

    return {
        "title": title,
        "thunderduck_time_ms": time_td * 1000,
        "spark_time_ms": time_ref * 1000,
        "speedup": speedup,
        "rows_td": len(result_td),
        "rows_ref": len(result_ref),
        "match": rows_match and values_match,
        "result": result_td,
    }


def format_comparison(comp):
    """Format a comparison result as a markdown summary table."""
    match_symbol = "\u2713" if comp["match"] else "\u2717"
    return f"""
### {comp['title']}

| Metric | Thunderduck | Spark |
|--------|-------------|-------|
| **Time** | {comp['thunderduck_time_ms']:.1f}ms | {comp['spark_time_ms']:.1f}ms |
| **Speedup** | **{comp['speedup']:.1f}x** | 1.0x |
| **Rows** | {comp['rows_td']} | {comp['rows_ref']} |
| **Match** | {match_symbol} | - |
"""
