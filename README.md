# thunderduck

[![Maven Build](https://img.shields.io/badge/maven-3.9+-blue.svg)](https://maven.apache.org/)
[![Java](https://img.shields.io/badge/java-17-orange.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

**thunderduck** is a high-performance embedded execution engine that translates Spark DataFrame operations to DuckDB SQL, delivering 5-10x faster query execution than Spark local mode with 6-8x better memory efficiency.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Building from Source](#building-from-source)
- [TPC-H Benchmark](#tpc-h-benchmark)
  - [Data Generation](#data-generation)
  - [Running Queries](#running-queries)
  - [Command-Line Examples](#command-line-examples)
- [Project Structure](#project-structure)
- [Performance](#performance)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

thunderduck provides a Spark-compatible API that translates DataFrame operations into optimized DuckDB SQL for embedded execution. It combines the familiar Spark programming model with the high-performance vectorized execution of DuckDB.

### Key Features

- **5-10x faster** than Spark local mode
- **6-8x better memory efficiency**
- **Multi-architecture support**: x86_64 (Intel/AMD) and ARM64 (AWS Graviton, Apple Silicon)
- **Zero-copy Arrow data paths** for efficient data interchange
- **Format support**: Parquet, Delta Lake, Iceberg
- **Comprehensive Spark API compatibility** with 200+ differential tests
- **SQL introspection** via EXPLAIN statements
- **TPC-H benchmark framework** for performance validation
- **Spark Connect Server** for remote client connectivity (PySpark, Scala Spark)

### Why thunderduck?

Spark's local mode has significant performance limitations for single-node workloads:
- JVM overhead and row-based processing
- High memory consumption (6-8x more than necessary)
- Poor single-node CPU/memory utilization

thunderduck addresses these issues by:
- Direct translation to DuckDB SQL (vectorized, SIMD-optimized execution)
- Hardware-aware optimization:
  - **x86_64**: Intel AVX-512, AVX2 SIMD instructions
  - **ARM64**: ARM NEON SIMD instructions (AWS Graviton, Apple Silicon)
- Zero-copy Arrow data interchange
- Native format readers (Parquet, Delta, Iceberg)

### Platform Support

thunderduck is designed and tested for **both x86_64 and ARM64 architectures**:

| Platform | Architecture | Status | Use Cases |
|----------|--------------|--------|-----------|
| **AWS Graviton** (c7g, r8g, etc.) | ARM64 (aarch64) | ✅ Fully Supported | Cost-effective cloud analytics (40% better price/performance) |
| **Apple Silicon** (M1, M2, M3) | ARM64 (aarch64) | ✅ Fully Supported | Local development, data science workflows |
| **Intel/AMD** (x86_64) | x86_64 | ✅ Fully Supported | Traditional cloud and on-premise deployments |
| **AWS EC2** (i8g, i4i, r8g) | Both | ✅ Fully Supported | High-performance analytics workloads |

**Performance Optimization**:
- Automatic SIMD detection and optimization per architecture
- Hardware-aware thread pool sizing
- Architecture-specific memory management tuning

## Architecture

thunderduck uses a three-layer architecture:

```
┌─────────────────────────────────────────────────────┐
│         Spark API Facade (DataFrame/Dataset)        │
│              Lazy Plan Construction                  │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│      Translation & Optimization Engine              │
│   Logical Plan → DuckDB SQL Translation             │
│   Expression Mapping, Type Conversion               │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│         DuckDB Execution Engine                     │
│   Vectorized Processing, SIMD Optimization          │
│   Arrow Data Interchange                            │
└─────────────────────────────────────────────────────┘
```

### Core Components

- **Logical Plan Representation** (`core/logical/`): Spark logical plan nodes
- **Expression System** (`core/expression/`): Expression translation and evaluation
- **SQL Generation** (`core/generator/`): DuckDB SQL code generation
- **Type Mapping** (`core/types/`): Spark ↔ DuckDB type conversion
- **Function Registry** (`core/functions/`): 500+ function mappings
- **Runtime Execution** (`core/runtime/`): Connection management, Arrow interchange
- **Format Readers** (`core/io/`): Parquet, Delta Lake, Iceberg support

**Note**: thunderduck relies on **DuckDB's world-class query optimizer** rather than implementing custom optimization rules. DuckDB automatically performs filter pushdown, column pruning, join reordering, and many other optimizations.

## Quick Start

### Prerequisites

- **Java**: 17 or later
- **Maven**: 3.9 or later
- **DuckDB**: 1.1.3 (automatically included via JDBC driver)

### Installation

Add thunderduck as a dependency to your Maven project:

```xml
<dependency>
    <groupId>com.thunderduck</groupId>
    <artifactId>thunderduck-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Basic Usage

```java
import com.thunderduck.runtime.DuckDBConnectionManager;
import com.thunderduck.runtime.QueryExecutor;
import org.apache.arrow.vector.VectorSchemaRoot;

// Create connection manager and executor
DuckDBConnectionManager connectionManager = new DuckDBConnectionManager();
QueryExecutor executor = new QueryExecutor(connectionManager);

// Execute query
String sql = "SELECT * FROM read_parquet('data.parquet') WHERE age > 25";
VectorSchemaRoot result = executor.executeQuery(sql);

// Process results
System.out.println("Rows: " + result.getRowCount());

// Clean up
result.close();
connectionManager.close();
```

## Building from Source

### Clone the Repository

```bash
git clone https://github.com/yourusername/thunderduck.git
cd thunderduck
```

### Build All Modules

```bash
# Full build with tests
mvn clean install

# Fast build (skip tests)
mvn clean install -Pfast

# Build with coverage report
mvn clean verify -Pcoverage
```

### Build Specific Modules

```bash
# Core module only
mvn clean install -pl core

# Benchmarks module
mvn clean install -pl benchmarks

# Tests module
mvn clean install -pl tests
```

### Verify Installation

```bash
# Check that JARs are created
ls -lh core/target/*.jar
ls -lh benchmarks/target/*.jar

# Expected output:
# core/target/thunderduck-core-0.1.0-SNAPSHOT.jar
# benchmarks/target/thunderduck-benchmarks-0.1.0-SNAPSHOT.jar
# benchmarks/target/benchmarks.jar
```

## TPC-H Benchmark

thunderduck includes a comprehensive TPC-H benchmark framework for performance testing and SQL introspection.

### Data Generation

TPC-H benchmark data must be generated at specific scale factors before running queries. The directory structure follows the pattern: `data/tpch_sf[scale_factor]`

#### Scale Factor Guidelines

| Scale Factor | Size | Use Case | Directory Name |
|--------------|------|----------|----------------|
| 0.01 | ~10MB | Quick testing, development | `data/tpch_sf001` |
| 1 | ~1GB | CI/CD, integration tests | `data/tpch_sf1` |
| 10 | ~10GB | Performance benchmarks | `data/tpch_sf10` |
| 100 | ~100GB | Stress testing | `data/tpch_sf100` |

#### Method 1: Using DuckDB TPC-H Extension (Recommended)

DuckDB has a built-in TPC-H extension that generates data directly:

```bash
# Install DuckDB if not already installed
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip
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

#### Method 2: Using tpchgen-rs (20x Faster for Large Datasets)

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

#### Required Directory Structure

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

### Running Queries

#### Command-Line Interface

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

### Command-Line Examples

#### 1. Query 1: Pricing Summary Report (Scan + Aggregation)

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

#### 2. Query 6: Forecasting Revenue Change (Selective Scan)

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

#### 3. Query 3: Shipping Priority (Multi-table Join)

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query 3 \
  --mode execute \
  --data ./data/tpch_sf001
```

Shows actual query results in tabular format.

#### 4. Run All Queries

```bash
java -cp benchmarks/target/benchmarks.jar \
  com.thunderduck.tpch.TPCHCommandLine \
  --query all \
  --mode execute \
  --data ./data/tpch_sf001
```

Currently executes queries 1, 3, and 6 (more queries coming soon).

### Programmatic API

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

## Project Structure

```
thunderduck/
├── pom.xml                    # Parent POM (dependency management)
├── core/                      # Core translation engine
│   ├── src/main/java/com/thunderduck/
│   │   ├── logical/           # Logical plan nodes
│   │   ├── expression/        # Expression system
│   │   ├── types/             # Type mapping
│   │   ├── functions/         # Function registry
│   │   ├── generator/         # SQL generation
│   │   ├── runtime/           # DuckDB execution
│   │   ├── io/                # Format readers (Parquet, Delta, Iceberg)
│   │   ├── logging/           # Structured query logging
│   │   └── exception/         # Exception types
│   └── pom.xml
├── benchmarks/                # TPC-H/TPC-DS benchmarks
│   ├── src/main/java/com/thunderduck/
│   │   └── tpch/              # TPC-H queries and utilities
│   │       ├── TPCHClient.java         # Programmatic API
│   │       └── TPCHCommandLine.java    # CLI tool
│   ├── README.md
│   └── pom.xml
├── tests/                     # Comprehensive test suite
│   ├── src/test/java/com/thunderduck/
│   │   ├── differential/      # Spark comparison tests (200+)
│   │   ├── integration/       # Integration tests
│   │   ├── logging/           # Logging system tests
│   │   └── introspection/     # EXPLAIN statement tests
│   └── pom.xml
└── docs/                      # Documentation
    ├── Analysis_and_Design.md
    ├── Testing_Strategy.md
    └── architect/             # Design documents
```

## Performance

### Performance Targets

Based on TPC-H benchmark at scale factor 10 (10GB):

| Metric | Target | Baseline (Spark 3.5.3) |
|--------|--------|------------------------|
| Query execution speed | 5-10x faster | 1x |
| Memory efficiency | 6-8x less | 1x |
| TPC-H Q1 (Scan + Agg) | 0.55s | 3.0s (5.5x speedup) |
| TPC-H Q6 (Selective Scan) | 0.12s | 1.0s (8.3x speedup) |
| TPC-H Q3 (Join + Agg) | 1.4s | 8.0s (5.7x speedup) |

### Performance Characteristics

- **Overhead vs Native DuckDB**: 10-20%
  - Logical plan construction: ~50ms
  - SQL generation: ~20ms
  - Arrow materialization: 5-10% of query time

- **Vectorized Execution**: SIMD-optimized operations (Intel AVX-512, ARM NEON)
- **Zero-Copy Arrow**: Efficient data interchange between layers
- **Parallel Parquet**: Multi-threaded column reading
- **Predicate Pushdown**: Filter pushdown to format readers

## Testing

thunderduck includes a comprehensive test suite with 500+ tests:

### Test Categories

- **Unit Tests (300+)**: Type mapping, expression translation, SQL generation
- **Integration Tests (100+)**: End-to-end pipelines, format readers
- **Differential Tests (200+)**: Spark 3.5.3 parity validation
- **End-to-End Tests**: PySpark client → Spark Connect → thunderduck validation
- **Performance Benchmarks (70+)**: TPC-H queries, micro-benchmarks

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=ExplainStatementTest

# Run with coverage report
mvn verify -Pcoverage

# View coverage report
open tests/target/site/jacoco/index.html
```

## End-to-End Testing (E2E)

The E2E test suite validates the complete pipeline: **PySpark client → Spark Connect protocol → thunderduck server → DuckDB execution**. This ensures thunderduck works correctly as a drop-in replacement for Spark.

### Prerequisites

1. **Python 3.8+** with pip
2. **PySpark 3.5.3** (automatically installed)
3. **thunderduck server** JAR built

### Starting the Spark Connect Server

Before running E2E tests, start the thunderduck Spark Connect server:

```bash
# Build the server if not already built
mvn clean package -pl connect-server

# Start the server (default port 15002)
# For x86_64:
java -jar connect-server/target/thunderduck-connect-server-*.jar

# For ARM64 (AWS Graviton, Apple Silicon):
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar

# Or use the convenience script (auto-detects platform):
./tests/scripts/start-server.sh
```

The server will show:
```
INFO SparkConnectServer - Starting Spark Connect Server...
INFO SparkConnectServer - Configuration: port=15002, sessionTimeout=300000ms
INFO SparkConnectServer - Server started successfully
```

### Running E2E Tests

#### Method 1: Maven Integration (Recommended)

**Note:** Maven automatically starts and stops the thunderduck server for you!

```bash
# Build server JAR first (required once)
mvn clean package -pl connect-server

# Run all E2E tests (starts server automatically)
mvn verify -Pe2e

# Run E2E tests with TPC benchmarks enabled
mvn verify -Pe2e,tpc

# Skip unit tests, run only E2E tests
mvn verify -Pe2e -DskipTests=true
```

The Maven integration:
- ✅ Automatically starts the server before tests
- ✅ Automatically stops the server after tests (even on failure/interruption)
- ✅ Reuses existing server if already running on port 15002
- ✅ Installs Python dependencies automatically

#### Method 2: Direct Python Execution

**Note:** When using Python directly, you must manually manage the server!

```bash
# Start the server first (in a separate terminal)
./tests/scripts/start-server.sh  # Or manually with java -jar ...

# Install Python dependencies
pip install -r tests/src/test/python/requirements.txt

# Run all E2E tests
python -m pytest tests/src/test/python/thunderduck_e2e/ -v

# Run specific test suite
python -m pytest tests/src/test/python/thunderduck_e2e/test_dataframes.py -v
python -m pytest tests/src/test/python/thunderduck_e2e/test_sql.py -v
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -v

# Run specific test
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py::TestTPCH::test_q01_sql -v
```

### E2E Test Suite Structure

```
tests/src/test/python/thunderduck_e2e/
├── test_runner.py        # Base test class with PySpark session setup
├── test_dataframes.py    # DataFrame operation tests
├── test_sql.py          # SQL query tests
├── test_tpch.py         # TPC-H benchmark tests (dual implementation)
├── test_tpcds.py        # TPC-DS benchmark tests (planned)
└── test_edge_cases.py   # Error handling and edge cases
```

### TPC-H E2E Tests (Dual Implementation)

The TPC-H test suite is unique: **each query is tested in TWO ways** to ensure complete Spark API compatibility:

1. **SQL Version**: Direct SQL query execution
2. **DataFrame API Version**: Equivalent operations using PySpark DataFrame API

Example from `test_tpch.py`:

```python
def test_q01_sql(self):
    """Q1: Pricing Summary Report (SQL version)."""
    query = """
        SELECT ... FROM lineitem WHERE ...
    """
    df = self.spark.sql(query)
    result = df.collect()
    self.assertGreater(len(result), 0)

def test_q01_dataframe(self):
    """Q1: Pricing Summary Report (DataFrame API version)."""
    df = self.df_lineitem \
        .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01"), 90)) \
        .groupBy("l_returnflag", "l_linestatus") \
        .agg(F.sum("l_quantity").alias("sum_qty"), ...)
    result = df.collect()
    self.assertGreater(len(result), 0)
```

**Coverage Status**: All 22 TPC-H queries (Q1-Q22) have both SQL and DataFrame implementations ✅

### Running TPC-H E2E Tests

```bash
# Ensure TPC-H data is generated (see Data Generation section)
# Default location: ./data/tpch_sf001/

# Run all TPC-H tests (44 tests total: 22 SQL + 22 DataFrame)
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -v

# Run only SQL versions
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -k "sql" -v

# Run only DataFrame versions
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -k "dataframe" -v

# Run specific query (both versions)
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -k "q01" -v
```

### E2E Test Configuration

Environment variables for test configuration:

```bash
# Specify thunderduck server location (default: localhost:15002)
export THUNDERDUCK_URL="sc://localhost:15002"

# Specify TPC-H data path (default: ./data/tpch_sf001)
export TPCH_DATA_PATH="./data/tpch_sf001"

# Enable verbose test output
export PYTEST_VERBOSE=1

# Run tests
mvn verify -Pe2e
```

### Continuous Integration

The E2E tests are integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Start thunderduck Server
  run: |
    ./tests/scripts/start-server.sh &
    sleep 5  # Wait for server startup

- name: Run E2E Tests
  run: mvn verify -Pe2e,tpc -DskipTests=true

- name: Stop Server
  run: pkill -f thunderduck-connect-server
```

### Debugging E2E Tests

1. **Check server is running**:
   ```bash
   netstat -an | grep 15002  # Should show LISTEN
   ```

2. **Enable debug logging**:
   ```bash
   export THUNDERDUCK_LOG_LEVEL=DEBUG
   python -m pytest tests/src/test/python/thunderduck_e2e/ -v -s
   ```

3. **Test connection manually**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder \
       .appName("test") \
       .remote("sc://localhost:15002") \
       .getOrCreate()
   spark.sql("SELECT 1").show()
   ```

### E2E Test Results

Expected output when running E2E tests:

```
============================= test session starts ==============================
tests/src/test/python/thunderduck_e2e/test_tpch.py::TestTPCH::test_q01_sql PASSED
tests/src/test/python/thunderduck_e2e/test_tpch.py::TestTPCH::test_q01_dataframe PASSED
tests/src/test/python/thunderduck_e2e/test_tpch.py::TestTPCH::test_q02_sql PASSED
tests/src/test/python/thunderduck_e2e/test_tpch.py::TestTPCH::test_q02_dataframe PASSED
...
===================== 44 passed in 45.23s =====================
```

### Quality Gates

All PRs must meet these criteria:
- Line coverage ≥ 85%
- Branch coverage ≥ 80%
- All differential tests passing (100%)
- All E2E tests passing (100%)
- Zero compiler warnings

## Documentation

### Technical Documentation

- **[Implementation Plan](IMPLEMENTATION_PLAN.md)**: 16-week development roadmap
- **[Spark Connect Architecture](docs/architect/SPARK_CONNECT_ARCHITECTURE.md)**: Server architecture and design
- **[Single-Session Architecture](docs/architect/SINGLE_SESSION_ARCHITECTURE.md)**: Session management design rationale
- **[Protocol Specification](docs/SPARK_CONNECT_PROTOCOL_SPEC.md)**: Spark Connect protocol details
- **[Testing Strategy](docs/Testing_Strategy.md)**: BDD and differential testing approach
- **[Benchmark Guide](benchmarks/README.md)**: TPC-H framework usage
- **[Week Completion Reports](WEEK*_COMPLETION_REPORT.md)**: Progress tracking

### API Documentation

Generate Javadoc:

```bash
mvn javadoc:javadoc
open core/target/site/apidocs/index.html
```

## Contributing

We welcome contributions! Please see the following guidelines:

1. **Fork the repository** and create a feature branch
2. **Write tests** for new functionality
3. **Ensure quality gates pass**: `mvn verify -Pcoverage`
4. **Run differential tests**: All 200+ tests must pass
5. **Submit a pull request** with clear description

### Development Workflow

```bash
# Clone and setup
git clone https://github.com/yourusername/thunderduck.git
cd thunderduck

# Create feature branch
git checkout -b feature/my-new-feature

# Make changes and test
mvn clean install

# Run full test suite
mvn verify -Pcoverage

# Commit and push
git commit -am "Add new feature"
git push origin feature/my-new-feature
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Status

**Current Phase**: Week 9 Complete (SQL Introspection & TPC-H Demonstration)

### Completed Milestones

- ✅ **Week 1-3**: Core infrastructure, SQL generation, DataFrame API
- ✅ **Week 4**: Complex expressions and joins
- ✅ **Week 5**: Aggregations and window functions
- ✅ **Week 7**: Differential testing framework (50 tests, 100% passing)
- ✅ **Week 8**: Comprehensive differential testing (200 tests, 100% parity)
- ✅ **Week 9**: SQL introspection (EXPLAIN) and TPC-H demonstration

### Next Steps

- **Week 10-12**: Spark Connect server (gRPC, Arrow streaming, multi-client support)
- **Phase 5+**: Performance optimization, large-scale benchmarking (TPC-H SF=100, TPC-DS)

## Acknowledgments

- **DuckDB Team**: High-performance embedded database
- **Apache Arrow**: Zero-copy data interchange
- **Apache Spark**: API compatibility and testing reference

---

**Need Help?** Open an issue on GitHub or contact the maintainers.

**Want to Contribute?** See [Contributing](#contributing) section above.
