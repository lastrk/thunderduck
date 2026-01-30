# Feature Specification: Interactive Comparison Notebook

**Status**: Draft
**Created**: 2025-12-23
**Goal**: Lower the barrier for experimentation with Thunderduck by providing an interactive notebook experience

---

## Executive Summary

Create a single-command experience to launch an interactive Marimo notebook with pre-configured PySpark clients for both Spark and Thunderduck, using existing TPC-H/TPC-DS benchmark data. Users can immediately run DataFrame operations side-by-side and compare performance.

---

## Problem Statement

Currently, trying out Thunderduck requires:
1. Building the project from source
2. Understanding server startup scripts
3. Configuring PySpark clients manually
4. Finding or creating test data
5. Writing comparison code

This multi-step process discourages experimentation and makes it hard to quickly validate whether Thunderduck works for a given use case.

---

## Proposed Solution

### One-Command Experience

```bash
./scripts/launch-playground.sh
```

This single command will:
1. Build Thunderduck if needed (or skip if already built)
2. Start the Thunderduck server (port 15002)
3. Start the Spark reference server (port 15003)
4. Wait for both servers to be healthy
5. Launch a Marimo notebook with pre-configured environment
6. Open browser to the notebook URL
7. Clean up servers on exit (Ctrl+C)

### Why Marimo?

| Feature | Marimo | Jupyter | Benefit |
|---------|--------|---------|---------|
| **File format** | Pure Python (.py) | JSON (.ipynb) | Git-friendly, code review friendly |
| **Reactivity** | Automatic cell re-execution | Manual | No hidden state bugs |
| **Reproducibility** | Guaranteed | Fragile | Same results every time |
| **Deployment** | Script or web app | Requires server | Easy sharing |
| **Dependencies** | Auto-install on import | Manual | Lower friction |

---

## Technical Design

### Directory Structure

```
scripts/
├── launch-playground.sh          # Main entry point
└── playground/
    ├── requirements.txt          # Python deps (marimo, pyspark, pandas)
    └── setup_playground.py       # Environment setup utilities

playground/                       # Marimo notebook files
├── thunderduck_playground.py     # Main comparison notebook
├── tpch_examples.py              # TPC-H focused examples
└── tpcds_examples.py             # TPC-DS focused examples
```

### Component 1: Launch Script (`scripts/launch-playground.sh`)

```bash
#!/usr/bin/env bash
# Launch Thunderduck interactive playground
#
# Usage: ./scripts/launch-playground.sh [--no-build] [--spark-only] [--thunderduck-only]

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
THUNDERDUCK_PORT=15002
SPARK_PORT=15003

# 1. Check/install Python dependencies
ensure_python_deps() {
    pip install -q marimo pyspark==4.0.1 pandas pyarrow
}

# 2. Build Thunderduck (if needed)
build_if_needed() {
    JAR="$PROJECT_ROOT/connect-server/target/thunderduck-connect-server-*.jar"
    if ! ls $JAR 1>/dev/null 2>&1 || [ "$1" = "--rebuild" ]; then
        echo "Building Thunderduck..."
        mvn -q package -DskipTests -pl connect-server -am
    fi
}

# 3. Start servers
start_servers() {
    # Start Thunderduck
    "$PROJECT_ROOT/tests/scripts/start-server.sh" &
    TD_PID=$!

    # Start Spark reference
    "$PROJECT_ROOT/tests/scripts/start-spark-4.0.1-reference.sh" &
    SPARK_PID=$!

    # Wait for health
    wait_for_port $THUNDERDUCK_PORT 60
    wait_for_port $SPARK_PORT 60
}

# 4. Launch Marimo
launch_notebook() {
    export THUNDERDUCK_URL="sc://localhost:$THUNDERDUCK_PORT"
    export SPARK_URL="sc://localhost:$SPARK_PORT"
    export TPCH_DATA_DIR="$PROJECT_ROOT/tests/integration/tpch_sf001"
    export TPCDS_DATA_DIR="$PROJECT_ROOT/data/tpcds_sf1"

    marimo edit "$PROJECT_ROOT/playground/thunderduck_playground.py"
}

# 5. Cleanup on exit
cleanup() {
    echo "Shutting down servers..."
    pkill -9 -f thunderduck-connect-server || true
    pkill -9 -f "SparkConnectServer.*$SPARK_PORT" || true
}
trap cleanup EXIT

# Main
ensure_python_deps
build_if_needed "$@"
start_servers
launch_notebook
```

### Component 2: Main Notebook (`playground/thunderduck_playground.py`)

Marimo notebooks are pure Python files with special cell markers:

```python
import marimo

__generated_with = "0.10.0"
app = marimo.App(width="full")


@app.cell
def __():
    """
    # Thunderduck Playground

    Compare Spark vs Thunderduck performance side-by-side using TPC-H benchmark data.

    This notebook provides:
    - Pre-configured PySpark clients for both engines
    - TPC-H sample data (SF0.01 - ~3MB, 8 tables)
    - Example DataFrame transformations
    - Performance comparison utilities
    """
    import marimo as mo
    return mo,


@app.cell
def __(mo):
    mo.md("""
    ## Setup

    Two SparkSession clients are pre-configured:
    - `spark_td` → Thunderduck (port 15002)
    - `spark_ref` → Apache Spark (port 15003)
    """)
    return


@app.cell
def __():
    """Initialize PySpark clients"""
    import os
    from pyspark.sql import SparkSession

    # Thunderduck client
    spark_td = (SparkSession.builder
        .appName("Thunderduck-Playground")
        .remote(os.environ.get("THUNDERDUCK_URL", "sc://localhost:15002"))
        .getOrCreate())

    # Spark reference client
    spark_ref = (SparkSession.builder
        .appName("Spark-Reference-Playground")
        .remote(os.environ.get("SPARK_URL", "sc://localhost:15003"))
        .getOrCreate())

    print(f"✓ Thunderduck connected: {spark_td.conf.get('spark.remote')}")
    print(f"✓ Spark connected: {spark_ref.conf.get('spark.remote')}")
    return spark_td, spark_ref


@app.cell
def __(spark_td, spark_ref):
    """Load TPC-H tables into both sessions"""
    import os
    from pathlib import Path

    tpch_dir = Path(os.environ.get("TPCH_DATA_DIR", "tests/integration/tpch_sf001"))

    # Table names
    TPCH_TABLES = ['lineitem', 'orders', 'customer', 'part',
                   'supplier', 'partsupp', 'nation', 'region']

    # Load into Thunderduck
    for table in TPCH_TABLES:
        df = spark_td.read.parquet(str(tpch_dir / f"{table}.parquet"))
        df.createOrReplaceTempView(table)

    # Load into Spark reference
    for table in TPCH_TABLES:
        df = spark_ref.read.parquet(str(tpch_dir / f"{table}.parquet"))
        df.createOrReplaceTempView(table)

    print(f"✓ Loaded {len(TPCH_TABLES)} TPC-H tables into both sessions")
    return TPCH_TABLES, tpch_dir


@app.cell
def __(mo):
    mo.md("""
    ## Example 1: Basic Aggregation

    Count orders by status - a simple GROUP BY query.
    """)
    return


@app.cell
def __(spark_td, spark_ref, compare_results):
    """Orders by status"""
    query = """
    SELECT o_orderstatus, COUNT(*) as order_count
    FROM orders
    GROUP BY o_orderstatus
    ORDER BY o_orderstatus
    """

    result = compare_results(
        spark_td.sql(query),
        spark_ref.sql(query),
        "Orders by Status"
    )
    result
    return


@app.cell
def __(mo):
    mo.md("""
    ## Example 2: Join with Aggregation

    Revenue by nation - joins customer, orders, lineitem, and nation tables.
    """)
    return


@app.cell
def __(spark_td, spark_ref, compare_results):
    """Revenue by nation"""
    query = """
    SELECT
        n.n_name as nation,
        SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    JOIN nation n ON c.c_nationkey = n.n_nationkey
    GROUP BY n.n_name
    ORDER BY revenue DESC
    LIMIT 10
    """

    result = compare_results(
        spark_td.sql(query),
        spark_ref.sql(query),
        "Top 10 Nations by Revenue"
    )
    result
    return


@app.cell
def __(mo):
    mo.md("""
    ## Example 3: DataFrame API

    Same query using pure DataFrame operations (no SQL).
    """)
    return


@app.cell
def __(spark_td, spark_ref, compare_results):
    """DataFrame API example"""
    from pyspark.sql import functions as F

    def revenue_by_nation(spark):
        customer = spark.table("customer")
        orders = spark.table("orders")
        lineitem = spark.table("lineitem")
        nation = spark.table("nation")

        return (customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy(nation.n_name.alias("nation"))
            .agg(F.sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)).alias("revenue"))
            .orderBy(F.desc("revenue"))
            .limit(10))

    result = compare_results(
        revenue_by_nation(spark_td),
        revenue_by_nation(spark_ref),
        "Revenue by Nation (DataFrame API)"
    )
    result
    return


@app.cell
def __(mo):
    mo.md("""
    ## Example 4: Window Functions

    Rank customers by total spending within each nation.
    """)
    return


@app.cell
def __(spark_td, spark_ref, compare_results):
    """Window function example"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    def top_customers_per_nation(spark):
        customer = spark.table("customer")
        orders = spark.table("orders")
        nation = spark.table("nation")

        customer_spending = (customer
            .join(orders, customer.c_custkey == orders.o_custkey)
            .join(nation, customer.c_nationkey == nation.n_nationkey)
            .groupBy("n_name", "c_custkey", "c_name")
            .agg(F.sum("o_totalprice").alias("total_spending")))

        window = Window.partitionBy("n_name").orderBy(F.desc("total_spending"))

        return (customer_spending
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= 3)
            .select("n_name", "c_name", "total_spending", "rank")
            .orderBy("n_name", "rank"))

    result = compare_results(
        top_customers_per_nation(spark_td),
        top_customers_per_nation(spark_ref),
        "Top 3 Customers per Nation"
    )
    result
    return


@app.cell
def __():
    """Comparison utility function"""
    import time
    import pandas as pd
    import marimo as mo

    def compare_results(df_td, df_ref, title="Comparison"):
        """
        Execute query on both engines, compare results and timing.
        Returns a formatted comparison view.
        """
        # Execute Thunderduck
        start_td = time.time()
        result_td = df_td.toPandas()
        time_td = time.time() - start_td

        # Execute Spark
        start_ref = time.time()
        result_ref = df_ref.toPandas()
        time_ref = time.time() - start_ref

        # Compare
        match = result_td.equals(result_ref)
        speedup = time_ref / time_td if time_td > 0 else float('inf')

        # Format output
        summary = f"""
### {title}

| Metric | Thunderduck | Spark |
|--------|-------------|-------|
| **Time** | {time_td*1000:.1f}ms | {time_ref*1000:.1f}ms |
| **Speedup** | {speedup:.1f}x | 1.0x |
| **Rows** | {len(result_td)} | {len(result_ref)} |
| **Match** | {'✓' if match else '✗'} | - |
"""

        return mo.vstack([
            mo.md(summary),
            mo.ui.table(result_td, label="Results")
        ])

    return compare_results,


@app.cell
def __(mo):
    mo.md("""
    ---

    ## Try Your Own Queries

    Edit the cell below to run your own DataFrame operations or SQL queries.
    """)
    return


@app.cell
def __(spark_td, spark_ref, compare_results):
    """Your custom query here"""

    # Example: Modify this query
    my_query = """
    SELECT COUNT(*) as total_lineitems FROM lineitem
    """

    compare_results(
        spark_td.sql(my_query),
        spark_ref.sql(my_query),
        "Custom Query"
    )
    return


if __name__ == "__main__":
    app.run()
```

### Component 3: Environment Setup Utilities

```python
# scripts/playground/setup_playground.py
"""
Utilities for setting up the playground environment.
"""

import subprocess
import time
import socket
from pathlib import Path


def wait_for_port(port: int, timeout: int = 60) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', port))
                return True
        except ConnectionRefusedError:
            time.sleep(0.5)
    return False


def check_server_health(url: str) -> bool:
    """Check if a Spark Connect server is responding."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote(url).getOrCreate()
        spark.sql("SELECT 1").collect()
        spark.stop()
        return True
    except Exception:
        return False


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / "pom.xml").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find project root")
```

---

## Data Available

### TPC-H (Small - for quick experiments)

| Table | Rows | Size | Description |
|-------|------|------|-------------|
| lineitem | ~60K | 1.8 MB | Order line items |
| orders | ~15K | 528 KB | Customer orders |
| customer | ~1.5K | 125 KB | Customer information |
| part | ~2K | 69 KB | Parts catalog |
| supplier | ~100 | 11 KB | Supplier information |
| partsupp | ~8K | 421 KB | Part-supplier relationships |
| nation | 25 | 2.3 KB | Nations |
| region | 5 | 1.1 KB | Regions |

**Location**: `/workspace/tests/integration/tpch_sf001/`

### TPC-DS (Large - for realistic benchmarks)

- 24 tables, 371 MB total
- Scale Factor 1 (SF1)
- Includes both dimension and fact tables
- **Location**: `/workspace/data/tpcds_sf1/`

---

## Example Snippets to Include

### Category 1: Basic Operations

```python
# Filter and aggregate
orders_df.filter(col("o_orderstatus") == "F") \
    .groupBy("o_orderpriority") \
    .count() \
    .orderBy("count", ascending=False)

# Select with expressions
lineitem_df.select(
    "l_orderkey",
    (col("l_extendedprice") * (1 - col("l_discount"))).alias("net_price")
)
```

### Category 2: Joins

```python
# Multi-table join
customer_df.join(orders_df, "c_custkey") \
    .join(nation_df, customer_df.c_nationkey == nation_df.n_nationkey) \
    .groupBy("n_name") \
    .agg(sum("o_totalprice").alias("total_revenue"))
```

### Category 3: Window Functions

```python
# Running totals
window = Window.partitionBy("n_nationkey").orderBy("o_orderdate")
orders_df.withColumn("running_total", sum("o_totalprice").over(window))

# Ranking
window = Window.partitionBy("n_name").orderBy(desc("revenue"))
df.withColumn("rank", rank().over(window))
```

### Category 4: Complex Aggregations

```python
# Pivot
orders_df.groupBy("o_orderyear") \
    .pivot("o_orderstatus") \
    .agg(count("*"))

# Cube/Rollup
orders_df.cube("o_orderstatus", "o_orderpriority") \
    .agg(sum("o_totalprice"), count("*"))
```

---

## User Experience Flow

```
User runs: ./scripts/launch-playground.sh

┌──────────────────────────────────────────────────────────────┐
│ Thunderduck Playground                                        │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Building Thunderduck... ✓                                    │
│ Starting Thunderduck server (port 15002)... ✓                │
│ Starting Spark reference server (port 15003)... ✓            │
│                                                              │
│ Launching Marimo notebook...                                 │
│ Open in browser: http://localhost:2718                       │
│                                                              │
│ Press Ctrl+C to stop all servers and exit                    │
│                                                              │
└──────────────────────────────────────────────────────────────┘

Browser opens → User sees pre-configured notebook
               → Runs example cells
               → Sees side-by-side comparison
               → Modifies queries to experiment
               → Ctrl+C to exit
               → All servers cleaned up automatically
```

---

## Success Criteria

1. **One command**: `./scripts/launch-playground.sh` works on fresh clone (after `mvn package`)
2. **Zero configuration**: No manual setup required
3. **Immediate feedback**: First comparison result in <30 seconds from launch
4. **Educational**: Examples demonstrate Thunderduck's value proposition
5. **Interactive**: Users can modify and re-run cells easily
6. **Clean exit**: All processes cleaned up on Ctrl+C

---

## Implementation Tasks

### Phase 1: Core Infrastructure
- [ ] Create `scripts/launch-playground.sh` with server orchestration
- [ ] Create `scripts/playground/requirements.txt` with dependencies
- [ ] Create `scripts/playground/setup_playground.py` with utilities

### Phase 2: Main Notebook
- [ ] Create `playground/thunderduck_playground.py` with Marimo notebook
- [ ] Implement `compare_results()` utility function
- [ ] Add TPC-H data loading cells
- [ ] Add 4-5 example query cells

### Phase 3: Additional Notebooks
- [ ] Create `playground/tpch_examples.py` with TPC-H focused examples
- [ ] Create `playground/tpcds_examples.py` with TPC-DS focused examples (optional)

### Phase 4: Documentation & Polish
- [ ] Add README to playground directory
- [ ] Update main README with playground instructions
- [ ] Test on fresh clone
- [ ] Add error handling for common issues (port conflicts, missing data)

---

## Dependencies

### Python
- `marimo>=0.10.0`
- `pyspark==4.0.1`
- `pandas`
- `pyarrow`

### System
- Java 17+ (for Spark/Thunderduck servers)
- Python 3.9+
- Maven (for building Thunderduck)

---

## Open Questions

1. **TPC-DS inclusion**: Should the main notebook include TPC-DS examples, or keep it focused on TPC-H for faster startup?
   - **Recommendation**: TPC-H only in main notebook, separate TPC-DS notebook for larger-scale testing

2. **Server pre-check**: Should we check if servers are already running and reuse them?
   - **Recommendation**: Yes, skip startup if healthy servers detected

3. **Docker option**: Should we provide a Docker-based alternative for environments without Java?
   - **Recommendation**: Future enhancement, not MVP

4. **Result persistence**: Should comparison results be saved for later analysis?
   - **Recommendation**: Future enhancement, Marimo's reactive model handles immediate comparisons

---

## Appendix: Marimo Installation

```bash
# Basic installation
pip install marimo

# With SQL and AI features
pip install "marimo[recommended]"

# Verify installation
marimo --version

# Tutorial (optional)
marimo tutorial intro
```
