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
    # TPC-DS Compute Benchmark: DuckDB vs Thunderduck vs Apache Spark

    Benchmarks four compute-intensive TPC-DS queries that stress hash tables,
    sort operators, and morsel-driven parallelism — exactly where DuckDB's
    vectorized engine should outperform Spark:

    - **Q47** — Monthly sales trends with `AVG() OVER` windows + CTE self-joins (lag/lead)
    - **Q51** — Cumulative `SUM() OVER (ROWS UNBOUNDED PRECEDING)` + `FULL OUTER JOIN`
    - **Q67** — `GROUP BY ROLLUP` over 8 columns + `RANK() OVER` (massive hash table)
    - **Q78** — Three CTEs (web/catalog/store) with multi-fact joins + channel ratios

    Three engines compared:

    - **DuckDB** — vanilla DuckDB (in-process, native SQL)
    - **Thunderduck** — DuckDB-backed Spark Connect server (PySpark API)
    - **Apache Spark** — reference Spark Connect server (PySpark API)

    All three engines run the **same SQL** (standard SQL, no dialect differences).
    An OS page-cache warmup pass runs first so no engine pays a cold-read penalty.

    Each query runs **N iterations**; the reported time is the **median**.

    ---
    """)
    return


@app.cell
def _(mo):
    iterations_slider = mo.ui.slider(
        start=1, stop=20, value=5, step=1,
        label="Iterations per query",
    )
    sf_dropdown = mo.ui.dropdown(
        options={"SF=1 (~1 GB)": 1.0, "SF=10 (~10 GB)": 10.0, "SF=20 (~20 GB)": 20.0},
        value="SF=20 (~20 GB)",
        label="Scale factor",
    )
    mo.md(f"""
    ### Configuration

    {sf_dropdown} {iterations_slider}

    **Tip:** These queries are compute-heavy (5-30s at SF=20). Use 3-5 iterations
    for stable medians without waiting too long. Lower SF for faster iteration.
    """)
    return iterations_slider, sf_dropdown


@app.cell
def _():
    import os
    import time
    from pathlib import Path

    import duckdb
    from pyspark.sql import SparkSession

    from util import (
        TPCDS_TABLES,
        generate_tpcds_data,
        load_tables,
        load_tables_duckdb,
        warmup_tables,
        warmup_tables_duckdb,
    )

    THUNDERDUCK_URL = os.environ.get("THUNDERDUCK_URL", "sc://localhost:15002")
    SPARK_URL = os.environ.get("SPARK_URL", "sc://localhost:15003")

    PLAYGROUND_DIR = Path(__file__).parent if "__file__" in dir() else Path("playground")
    GENERATED_DATA_DIR = PLAYGROUND_DIR / "data"
    PROJECT_ROOT = PLAYGROUND_DIR.parent
    QUERIES_DIR = PROJECT_ROOT / "tests" / "integration" / "sql" / "tpcds_queries"
    return (
        GENERATED_DATA_DIR,
        Path,
        QUERIES_DIR,
        SPARK_URL,
        SparkSession,
        THUNDERDUCK_URL,
        duckdb,
        generate_tpcds_data,
        load_tables,
        load_tables_duckdb,
        os,
        time,
        warmup_tables,
        warmup_tables_duckdb,
    )


@app.cell
def _(SPARK_URL, SparkSession, THUNDERDUCK_URL):
    spark_td = (
        SparkSession.builder
        .appName("Thunderduck-Benchmark")
        .remote(THUNDERDUCK_URL)
        .getOrCreate()
    )
    spark_ref = (
        SparkSession.builder
        .appName("Spark-Benchmark")
        .remote(SPARK_URL)
        .getOrCreate()
    )
    print(f"Thunderduck: {THUNDERDUCK_URL}")
    print(f"Spark:       {SPARK_URL}")
    return spark_ref, spark_td


@app.cell
def _(
    GENERATED_DATA_DIR,
    TPCDS_TABLES,
    duckdb,
    generate_tpcds_data,
    load_tables,
    load_tables_duckdb,
    os,
    sf_dropdown,
    spark_ref,
    spark_td,
    warmup_tables,
    warmup_tables_duckdb,
):
    # Use selected scale factor; generate data if not already present
    scale_factor = sf_dropdown.value
    data_dir = GENERATED_DATA_DIR / f"tpcds_sf{scale_factor}"
    if not data_dir.exists() or not (data_dir / "store_sales.parquet").exists():
        print(f"TPC-DS SF={scale_factor} data not found. Generating (this may take several minutes)...")
        data_dir = generate_tpcds_data(GENERATED_DATA_DIR, scale_factor=scale_factor)

    # DuckDB in-process connection — no resource caps, let it use all available hardware
    duck_conn = duckdb.connect()

    load_tables(spark_td, data_dir, TPCDS_TABLES)
    load_tables(spark_ref, data_dir, TPCDS_TABLES)
    load_tables_duckdb(duck_conn, data_dir, TPCDS_TABLES)
    print(f"Loaded TPC-DS tables (SF={scale_factor}) from {data_dir}")

    print("Warming up DuckDB cache...")
    warmup_tables_duckdb(duck_conn, TPCDS_TABLES)
    print("Warming up Thunderduck cache...")
    warmup_tables(spark_td, TPCDS_TABLES)
    print("Warming up Spark cache...")
    warmup_tables(spark_ref, TPCDS_TABLES)
    print("Cache warmup complete.")
    return duck_conn, scale_factor


@app.cell
def _(QUERIES_DIR):
    # ---------------------------------------------------------------------------
    # Load TPC-DS queries from files.
    # Standard SQL — same query works on all three engines.
    # ---------------------------------------------------------------------------

    QUERY_NUMS = [47, 51, 67, 78]
    QUERIES = {}
    for _qnum in QUERY_NUMS:
        _path = QUERIES_DIR / f"q{_qnum}.sql"
        QUERIES[_qnum] = _path.read_text()

    return QUERIES, QUERY_NUMS


@app.cell
def _(
    QUERIES,
    QUERY_NUMS,
    duck_conn,
    iterations_slider,
    mo,
    spark_ref,
    spark_td,
    time,
):
    import statistics

    N = iterations_slider.value

    # ---------------------------------------------------------------------------
    # Benchmark runner: execute each query N times on all three engines,
    # report median elapsed time.
    # ---------------------------------------------------------------------------

    def _run_spark(spark, sql):
        start = time.time()
        result = spark.sql(sql).toPandas()
        elapsed = time.time() - start
        return result, elapsed

    def _run_duckdb(conn, sql):
        start = time.time()
        result = conn.execute(sql).fetchdf()
        elapsed = time.time() - start
        return result, elapsed

    def _bench(fn, *args):
        """Run fn N times, return (row_count, median_seconds, status) or failure."""
        timings = []
        row_count = None
        for _ in range(N):
            try:
                result, elapsed = fn(*args)
                timings.append(elapsed)
                row_count = len(result)
            except Exception as e:
                return None, None, str(e)[:80]
        return row_count, statistics.median(timings), "OK"

    results = []
    for qnum in QUERY_NUMS:
        label = f"Q{qnum}"
        sql = QUERIES[qnum]
        print(f"Running {label} ({N}x)...", end=" ", flush=True)

        # --- DuckDB ---
        db_rows, db_t, db_st = _bench(_run_duckdb, duck_conn, sql)

        # --- Thunderduck ---
        td_rows, td_t, td_st = _bench(_run_spark, spark_td, sql)

        # --- Spark ---
        sp_rows, sp_t, sp_st = _bench(_run_spark, spark_ref, sql)

        results.append({
            "query": label,
            "db_ms": round(db_t * 1000, 1) if db_t is not None else None,
            "td_ms": round(td_t * 1000, 1) if td_t is not None else None,
            "spark_ms": round(sp_t * 1000, 1) if sp_t is not None else None,
            "db_rows": db_rows,
            "td_rows": td_rows,
            "spark_rows": sp_rows,
            "db_status": db_st,
            "td_status": td_st,
            "spark_status": sp_st,
        })

        parts = []
        parts.append(f"DB={'FAIL' if db_t is None else f'{db_t*1000:.0f}ms'}")
        parts.append(f"TD={'FAIL' if td_t is None else f'{td_t*1000:.0f}ms'}")
        parts.append(f"Spark={'FAIL' if sp_t is None else f'{sp_t*1000:.0f}ms'}")
        if td_t and sp_t and td_t > 0:
            parts.append(f"TD/Spark={sp_t/td_t:.1f}x")
        if db_t and sp_t and db_t > 0:
            parts.append(f"DB/Spark={sp_t/db_t:.1f}x")
        print("  ".join(parts))

    # ---- Build markdown results table ----
    header = (
        "| Query | DuckDB | Thunderduck | Spark | TD vs Spark | DB vs Spark | TD Overhead |\n"
        "|-------|--------|-------------|-------|-------------|-------------|-------------|\n"
    )
    rows = []
    td_fail = []
    for r in results:
        db = f"{r['db_ms']}ms" if r['db_ms'] is not None else "FAIL"
        td = f"{r['td_ms']}ms" if r['td_ms'] is not None else "FAIL"
        sp = f"{r['spark_ms']}ms" if r['spark_ms'] is not None else "FAIL"

        # TD vs Spark speedup
        if r['td_ms'] is not None and r['spark_ms'] is not None and r['td_ms'] > 0:
            td_sp = f"**{r['spark_ms'] / r['td_ms']:.1f}x**"
        else:
            td_sp = "N/A"

        # DB vs Spark speedup
        if r['db_ms'] is not None and r['spark_ms'] is not None and r['db_ms'] > 0:
            db_sp = f"**{r['spark_ms'] / r['db_ms']:.1f}x**"
        else:
            db_sp = "N/A"

        # TD overhead vs raw DuckDB
        if r['td_ms'] is not None and r['db_ms'] is not None and r['db_ms'] > 0:
            overhead = f"{r['td_ms'] / r['db_ms']:.1f}x"
        else:
            overhead = "N/A"

        if r['td_status'] != "OK":
            td_fail.append(r['query'])

        rows.append(f"| {r['query']} | {db} | {td} | {sp} | {td_sp} | {db_sp} | {overhead} |")

    # ---- Summary stats (queries where all three succeeded) ----
    ok = [r for r in results if r['db_ms'] is not None and r['td_ms'] is not None and r['spark_ms'] is not None]
    nq = len(ok)
    if nq > 0:
        tot_db = sum(r['db_ms'] for r in ok)
        tot_td = sum(r['td_ms'] for r in ok)
        tot_sp = sum(r['spark_ms'] for r in ok)
        geo_td_sp = 1.0
        geo_db_sp = 1.0
        geo_overhead = 1.0
        for r in ok:
            geo_td_sp *= r['spark_ms'] / r['td_ms'] if r['td_ms'] > 0 else 1.0
            geo_db_sp *= r['spark_ms'] / r['db_ms'] if r['db_ms'] > 0 else 1.0
            geo_overhead *= r['td_ms'] / r['db_ms'] if r['db_ms'] > 0 else 1.0
        geo_td_sp = geo_td_sp ** (1.0 / nq)
        geo_db_sp = geo_db_sp ** (1.0 / nq)
        geo_overhead = geo_overhead ** (1.0 / nq)
        total_td_sp = f"{tot_sp / tot_td:.2f}" if tot_td > 0 else "N/A"
        total_db_sp = f"{tot_sp / tot_db:.2f}" if tot_db > 0 else "N/A"
        total_overhead = f"{tot_td / tot_db:.2f}" if tot_db > 0 else "N/A"
    else:
        tot_db = tot_td = tot_sp = 0
        geo_td_sp = geo_db_sp = geo_overhead = 0
        total_td_sp = total_db_sp = total_overhead = "N/A"

    passed = sum(1 for r in results if r['td_status'] == "OK" and r['spark_status'] == "OK" and r['db_status'] == "OK")
    rows_text = "\n".join(rows)

    summary_md = f"""
## Benchmark Results ({N} iterations, median time)

{passed}/{len(results)} queries completed on all three engines.

**TD vs Spark** = how much faster Thunderduck is than Spark (higher is better).
**DB vs Spark** = how much faster vanilla DuckDB is than Spark (higher is better).
**TD Overhead** = Thunderduck time / DuckDB time — the cost of the Spark Connect layer (lower is better).

{header}{rows_text}
| **Total** | **{tot_db:.0f}ms** | **{tot_td:.0f}ms** | **{tot_sp:.0f}ms** | **{total_td_sp}x** | **{total_db_sp}x** | **{total_overhead}x** |
| **Geo Mean** | | | | **{geo_td_sp:.2f}x** | **{geo_db_sp:.2f}x** | **{geo_overhead:.2f}x** |
"""

    if td_fail:
        fail_list = ", ".join(td_fail)
        summary_md += f"\n**Failed on Thunderduck:** {fail_list}\n"

    mo.md(summary_md)
    return


if __name__ == "__main__":
    app.run()
