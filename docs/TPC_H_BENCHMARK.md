# TPC-H Benchmark Guide

thunderduck includes a comprehensive TPC-H benchmark framework for performance testing and SQL introspection.

## Data Generation

TPC-H benchmark data must be generated at specific scale factors before running queries. The directory structure follows the pattern: `data/tpch_sf[scale_factor]`

### Scale Factor Guidelines

| Scale Factor | Size | Use Case | Directory Name |
|--------------|------|----------|----------------|
| 0.01 | ~10MB | Quick testing, development | `data/tpch_sf001` |
| 1 | ~1GB | CI/CD, integration tests | `data/tpch_sf1` |
| 10 | ~10GB | Performance benchmarks | `data/tpch_sf10` |
| 100 | ~100GB | Stress testing | `data/tpch_sf100` |

### Method 1: Using DuckDB TPC-H Extension (Recommended)

DuckDB has a built-in TPC-H extension that generates data directly:

```bash
# Install DuckDB if not already installed
wget https://github.com/duckdb/duckdb/releases/download/v1.4.3/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip

# Generate TPC-H data at scale factor 0.01 (10MB)
mkdir -p data/tpch_sf001

./duckdb << 'EOF'
INSTALL tpch;
LOAD tpch;

-- Generate all tables at SF 0.01
CALL dbgen(sf=0.01);

-- Export all 8 required tables to Parquet format
COPY (SELECT * FROM customer) TO 'data/tpch_sf001/customer.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM lineitem) TO 'data/tpch_sf001/lineitem.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM nation) TO 'data/tpch_sf001/nation.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM orders) TO 'data/tpch_sf001/orders.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM part) TO 'data/tpch_sf001/part.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM partsupp) TO 'data/tpch_sf001/partsupp.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM region) TO 'data/tpch_sf001/region.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM supplier) TO 'data/tpch_sf001/supplier.parquet' (FORMAT PARQUET);
EOF
```

**Generate at different scale factors:**

```bash
# SF 1 (1GB) - CI/CD benchmarks
mkdir -p data/tpch_sf1
./duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=1); \
  COPY customer TO 'data/tpch_sf1/customer.parquet' (FORMAT PARQUET); \
  COPY lineitem TO 'data/tpch_sf1/lineitem.parquet' (FORMAT PARQUET); \
  COPY nation TO 'data/tpch_sf1/nation.parquet' (FORMAT PARQUET); \
  COPY orders TO 'data/tpch_sf1/orders.parquet' (FORMAT PARQUET); \
  COPY part TO 'data/tpch_sf1/part.parquet' (FORMAT PARQUET); \
  COPY partsupp TO 'data/tpch_sf1/partsupp.parquet' (FORMAT PARQUET); \
  COPY region TO 'data/tpch_sf1/region.parquet' (FORMAT PARQUET); \
  COPY supplier TO 'data/tpch_sf1/supplier.parquet' (FORMAT PARQUET);"

# SF 10 (10GB) - Performance testing
mkdir -p data/tpch_sf10
./duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=10); \
  COPY customer TO 'data/tpch_sf10/customer.parquet' (FORMAT PARQUET); \
  COPY lineitem TO 'data/tpch_sf10/lineitem.parquet' (FORMAT PARQUET); \
  COPY nation TO 'data/tpch_sf10/nation.parquet' (FORMAT PARQUET); \
  COPY orders TO 'data/tpch_sf10/orders.parquet' (FORMAT PARQUET); \
  COPY part TO 'data/tpch_sf10/part.parquet' (FORMAT PARQUET); \
  COPY partsupp TO 'data/tpch_sf10/partsupp.parquet' (FORMAT PARQUET); \
  COPY region TO 'data/tpch_sf10/region.parquet' (FORMAT PARQUET); \
  COPY supplier TO 'data/tpch_sf10/supplier.parquet' (FORMAT PARQUET);"
```

### Method 2: Using tpchgen-rs (20x Faster for Large Datasets)

tpchgen-rs is a Rust-based TPC-H data generator significantly faster than classic dbgen:

```bash
# Install Rust and tpchgen-rs
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo install tpchgen-cli

# Create data directory
mkdir -p data

# Generate data at different scale factors
tpchgen-cli -s 0.01 --format=parquet --output=data/tpch_sf001  # 10MB (development)
tpchgen-cli -s 1 --format=parquet --output=data/tpch_sf1       # 1GB (CI)
tpchgen-cli -s 10 --format=parquet --output=data/tpch_sf10     # 10GB (nightly)
tpchgen-cli -s 100 --format=parquet --output=data/tpch_sf100   # 100GB (stress)
```

### Verify Data Generation

```bash
# Check that all 8 tables are generated
ls -lh data/tpch_sf001/

# Expected output:
# customer.parquet    (~12K for SF 0.01)
# lineitem.parquet    (~48K for SF 0.01)
# nation.parquet      (~1.2K for SF 0.01)
# orders.parquet      (~24K for SF 0.01)
# part.parquet        (~16K for SF 0.01)
# partsupp.parquet    (~32K for SF 0.01)
# region.parquet      (~800 bytes for SF 0.01)
# supplier.parquet    (~4.0K for SF 0.01)

# Verify Parquet files are valid
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 1 --mode explain --data ./data/tpch_sf001
```

### Required Directory Structure

```
thunderduck/
└── data/
    ├── tpch_sf001/          # Scale Factor 0.01 (10MB)
    │   ├── customer.parquet
    │   ├── lineitem.parquet
    │   ├── nation.parquet
    │   ├── orders.parquet
    │   ├── part.parquet
    │   ├── partsupp.parquet
    │   ├── region.parquet
    │   └── supplier.parquet
    ├── tpch_sf1/            # Scale Factor 1 (1GB)
    │   └── [same 8 tables]
    ├── tpch_sf10/           # Scale Factor 10 (10GB)
    │   └── [same 8 tables]
    └── tpch_sf100/          # Scale Factor 100 (100GB)
        └── [same 8 tables]
```

**Important Notes:**
- All 8 tables must be present for queries to execute
- Files must be in Parquet format with `.parquet` extension
- Directory naming: `tpch_sf<scale>` where scale is zero-padded for < 1 (e.g., `sf001` for 0.01)
- The scale factor is automatically inferred from the directory name

## Running Queries

### Command-Line Interface

The `TPCHCommandLine` tool provides a simple CLI for executing TPC-H queries with different modes:

```bash
# Build the benchmarks JAR first
mvn clean package -pl benchmarks

# Run single query with EXPLAIN
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 1 \
  --mode explain \
  --data ./data/tpch_sf001

# Run with EXPLAIN ANALYZE (includes execution statistics)
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 6 \
  --mode analyze \
  --data ./data/tpch_sf001

# Execute query and show results
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 3 \
  --mode execute \
  --data ./data/tpch_sf001
```

## Command-Line Examples

### 1. Query 1: Pricing Summary Report (Scan + Aggregation)

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 1 \
  --mode explain \
  --data ./data/tpch_sf001
```

**Expected Output:**
```
============================================================
TPC-H Query 1
Mode: EXPLAIN
Data Path: ./data/tpch_sf001
Scale Factor: 0.01
============================================================

============================================================
GENERATED SQL
============================================================

SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  ...
FROM read_parquet('./data/tpch_sf001/lineitem.parquet')
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus

============================================================
DUCKDB EXPLAIN
============================================================

┌─────────────────────────────┐
│         QUERY PLAN          │
└─────────────────────────────┘
...

============================================================
Execution time: 15 ms
============================================================
```

### 2. Query 6: Forecasting Revenue Change (Selective Scan)

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 6 \
  --mode analyze \
  --data ./data/tpch_sf001
```

This shows execution statistics including:
- Row counts at each operator
- Execution time per operator
- Memory usage

### 3. Query 3: Shipping Priority (Multi-table Join)

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 3 \
  --mode execute \
  --data ./data/tpch_sf001
```

Shows actual query results in tabular format.

### 4. Run All Queries

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query all \
  --mode execute \
  --data ./data/tpch_sf001
```

Currently executes queries 1, 3, and 6 (more queries coming soon).

## Programmatic API

You can also use the `TPCHClient` Java API in your code:

```java
import com.thunderduck.tpch.TPCHClient;
import org.apache.arrow.vector.VectorSchemaRoot;

// Create client
TPCHClient client = new TPCHClient("./data/tpch_sf001", 0.01);

// Execute query
VectorSchemaRoot result = client.executeQuery(1);
System.out.println("Rows: " + result.getRowCount());
result.close();

// Get EXPLAIN output
String plan = client.explainQuery(1);
System.out.println(plan);

// Get EXPLAIN ANALYZE output
String stats = client.explainAnalyzeQuery(1);
System.out.println(stats);

// Clean up
client.close();
```

## Performance Results

Based on TPC-H benchmark at scale factor 10 (10GB):

| Query | thunderduck | Spark 4.0.x | Speedup |
|-------|-------------|-------------|---------|
| Q1 (Scan + Agg) | 0.55s | 3.0s | 5.5x |
| Q6 (Selective Scan) | 0.12s | 1.0s | 8.3x |
| Q3 (Join + Agg) | 1.4s | 8.0s | 5.7x |

---

**See Also:**
- [Main README](../README.md)
- [Differential Testing Architecture](architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)
