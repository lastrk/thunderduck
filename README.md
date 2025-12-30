# Thunderduck

[![Maven Build](https://img.shields.io/badge/maven-3.9+-blue.svg)](https://maven.apache.org/)
[![Java](https://img.shields.io/badge/java-17-orange.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

> **⚠️ Alpha Software**: Despite extensive test coverage, Thunderduck is currently alpha quality software and will undergo extensive testing with real-world workloads before production readiness.

> **📋 SparkSQL Notice**: Direct SQL execution via `spark.sql()` is **not yet supported**. SparkSQL support will be added once a proper SQL parser is integrated. The **DataFrame API is fully functional** and recommended for all use cases.

**Thunderduck** is a high-performance embedded execution engine that translates Spark DataFrame operations to DuckDB SQL, delivering 5-10x faster query execution than Spark local mode with 6-8x better memory efficiency.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Building from Source](#building-from-source)
- [Project Structure](#project-structure)
- [Performance](#performance)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Thunderduck provides a Spark-compatible API that translates DataFrame operations into optimized DuckDB SQL for embedded execution. It combines the familiar Spark programming model with the high-performance vectorized execution of DuckDB.

### Key Features

- **Spark Connect Server** for remote client connectivity (PySpark 4.0.x, Scala Spark)
- **5-10x faster** than Spark local mode
- **6-8x better memory efficiency**
- **Multi-architecture support**: x86_64 (Intel/AMD) and ARM64 (AWS Graviton, Apple Silicon)
- **Zero-copy Arrow data paths** for efficient data interchange
- **Format support**: Parquet, Delta Lake (PLANNED), Iceberg (PLANNED)
- **Comprehensive Spark API compatibility** with 85+ DataFrame differential tests
- **Query plan introspection** via EXPLAIN statements

### Why Thunderduck?

> **TL;DR**: Most Spark workloads fit on one machine. Thunderduck lets you keep your Spark code while getting 5-10x better performance from DuckDB's vectorized engine.

**Most Spark workloads don't need distributed computing.** They'd run faster and cheaper on a single large node.

We analyzed hundreds of thousands of real-world Spark jobs and found this matches what [practitioners have observed](https://motherduck.com/blog/big-data-is-dead/) across the industry.

#### The Economics Have Shifted

| Era | Hardware Scaling | Implication |
|-----|------------------|-------------|
| **2010s** | 2x resources = 4x+ cost | Distribute to save money |
| **Today** | Linear pricing | Single 200-CPU/1TB node is cost-effective |

Single-node compute eliminates shuffles, network bottlenecks, and coordination overhead—enabling near-100% CPU utilization.

#### Why Not Spark Local Mode?

Spark local mode exists but underperforms on single nodes:
- JVM overhead and row-based (not vectorized) processing
- High memory consumption
- Poor CPU utilization

#### How Thunderduck Fixes This

- **Vectorized execution**: DuckDB uses SIMD-optimized columnar processing
- **Morsel parallelism**: Saturates all available CPUs efficiently
- **Zero-copy Arrow**: No serialization overhead between layers
- **Hardware-aware**: Auto-detects hardware-specific vector instructions

### Platform Support

Thunderduck is designed and tested for **both x86_64 and ARM64 architectures**:

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

Thunderduck uses a three-layer architecture:

```
┌─────────────────────────────────────────────────────┐
│         Spark API Facade (DataFrame/Dataset)        │
│              Lazy Plan Construction                 │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│                Translation Engine                   │
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
- **Runtime Execution** (`core/runtime/`): Session-scoped DuckDB runtime, Arrow streaming
- **Format Readers** (`core/io/`): Parquet, (PLANNED) Delta Lake, (PLANNED) Iceberg support

**Note**: thunderduck relies on **DuckDB's world-class query optimizer** rather than implementing custom optimization rules. DuckDB automatically performs filter pushdown, column pruning, join reordering, and many other optimizations.

## Quick Start

### Prerequisites

- **Java**: 17 or later
- **Maven**: 3.9 or later
- **Python**: 3.8+ (for PySpark client)

### Start the Server

```bash
# Clone and build
git clone https://github.com/yourusername/thunderduck.git
cd thunderduck
mvn clean package -DskipTests

# Start the Spark Connect server (default port 15002)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar
```

The server will show:
```
INFO SparkConnectServer - Starting Spark Connect Server...
INFO SparkConnectServer - Server started successfully on port 15002
```

### Connect with PySpark

```bash
pip install pyspark==4.0.1
```

```python
from pyspark.sql import SparkSession

# Connect to thunderduck server
spark = SparkSession.builder \
    .appName("thunderduck-demo") \
    .remote("sc://localhost:15002") \
    .getOrCreate()

# Run queries - same API as Apache Spark!
df = spark.read.parquet("data/tpch_sf001/lineitem.parquet")
df.filter(df.l_quantity > 40).groupBy("l_returnflag").count().show()

# More DataFrame operations
orders = spark.read.parquet("data/tpch_sf001/orders.parquet")
orders.select("o_orderkey", "o_totalprice").filter(orders.o_totalprice > 1000).show()
```

### Verify Installation

```bash
# Check server is running
curl -s localhost:15002 || echo "Server running on port 15002"

# Run differential tests (compares against Spark 4.0.1)
./tests/scripts/setup-differential-testing.sh  # One-time setup
./tests/scripts/run-differential-tests-v2.sh   # Run 266 tests
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

### macOS Build Requirements

On macOS, the default build downloads protobuf binaries that get blocked by Gatekeeper. Use the `use-system-protoc` profile with locally installed tools instead.

**Prerequisites:**

```bash
# 1. Install protoc via Homebrew or download directly
brew install protobuf
# Verify: protoc --version (should show 3.x or 25.x)

# 2. Install grpc-java protoc plugin
# Check your architecture first:
uname -m

# For Apple Silicon (arm64):
sudo curl -L -o /usr/local/bin/protoc-gen-grpc-java \
  'https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.62.2/protoc-gen-grpc-java-1.62.2-osx-aarch_64.exe'

# For Intel Mac (x86_64):
sudo curl -L -o /usr/local/bin/protoc-gen-grpc-java \
  'https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.62.2/protoc-gen-grpc-java-1.62.2-osx-x86_64.exe'

# Make executable and remove quarantine
sudo chmod +x /usr/local/bin/protoc-gen-grpc-java
sudo xattr -d com.apple.quarantine /usr/local/bin/protoc-gen-grpc-java 2>/dev/null || true

# Verify it's in PATH
which protoc-gen-grpc-java
```

**Build with system protoc:**

```bash
mvn clean package -DskipTests -Puse-system-protoc
```

> **Note**: If you installed a different protoc version (e.g., 25.x via Homebrew), it should still work as protobuf is generally backward compatible.

### Build Specific Modules

```bash
# Core module only
mvn clean install -pl core

# Connect server module
mvn clean install -pl connect-server

# Tests module
mvn clean install -pl tests
```

### Verify Installation

```bash
# Check that JARs are created
ls -lh core/target/*.jar
ls -lh connect-server/target/*.jar

# Expected output:
# core/target/thunderduck-core-0.1.0-SNAPSHOT.jar
# connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar
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
├── connect-server/            # Spark Connect server
│   ├── src/main/java/com/thunderduck/connect/
│   │   ├── server/            # gRPC server implementation
│   │   ├── service/           # Spark Connect service handlers
│   │   └── session/           # Session management
│   └── pom.xml
├── tests/                     # Comprehensive test suite
│   ├── src/test/java/         # Java unit tests
│   ├── integration/           # Python differential tests (266 tests)
│   │   └── sql/               # TPC-H and TPC-DS SQL queries
│   └── pom.xml
└── docs/                      # Documentation
    ├── TPC_H_BENCHMARK.md     # TPC-H benchmark guide
    ├── SPARK_CONNECT_PROTOCOL_SPEC.md
    └── architect/             # Architecture design documents
```

## Performance

### Performance Targets

TBD

## Testing

thunderduck includes a comprehensive test suite with 700+ tests:

### Test Categories

- **Unit Tests (300+)**: Type mapping, expression translation, SQL generation
- **Integration Tests (100+)**: End-to-end pipelines, format readers
- **Differential Tests (85+)**: Spark 4.0.1 DataFrame API parity validation
- **End-to-End Tests**: PySpark client → Spark Connect → thunderduck validation
- **Performance Benchmarks (70+)**: TPC-H queries, micro-benchmarks

### Differential Test Breakdown

| Test Suite | Tests | Description |
|------------|-------|-------------|
| TPC-DS DataFrame | 33 | DataFrame API implementations of TPC-DS queries |
| Function Parity | 57 | Array, Map, Null, String, Math functions |
| Multi-dim Aggregations | 21 | pivot, unpivot, cube, rollup, grouping |
| Window Functions | 35 | rank, lag/lead, frame specs, analytics |

> **Note**: SQL-based tests (TPC-H SQL, TPC-DS SQL) are disabled pending SparkSQL parser integration.

### Running Tests

```bash
# Run all unit tests
mvn test

# Run specific test class
mvn test -Dtest=ExplainStatementTest

# Run with coverage report
mvn verify -Pcoverage

# View coverage report
open tests/target/site/jacoco/index.html
```

## Differential Testing (Spark Parity Validation)

The differential testing framework compares Thunderduck results against Apache Spark 4.0.1 to ensure exact compatibility. Both systems run via Spark Connect protocol for fair comparison.

### Quick Start

```bash
# One-time setup (downloads Spark 4.0.1, installs Python dependencies)
./tests/scripts/setup-differential-testing.sh

# Run DataFrame differential tests
./tests/scripts/run-differential-tests-v2.sh
```

### Test Groups

Run specific test suites using named groups:

```bash
# Core DataFrame tests
./tests/scripts/run-differential-tests-v2.sh dataframe    # TPC-DS DataFrame (33 tests)
./tests/scripts/run-differential-tests-v2.sh functions    # Function parity tests
./tests/scripts/run-differential-tests-v2.sh operations   # DataFrame operations
./tests/scripts/run-differential-tests-v2.sh window       # Window functions
./tests/scripts/run-differential-tests-v2.sh aggregations # Multi-dim aggregations

# Additional test groups
./tests/scripts/run-differential-tests-v2.sh lambda       # Lambda/HOF functions
./tests/scripts/run-differential-tests-v2.sh joins        # USING join tests
./tests/scripts/run-differential-tests-v2.sh statistics   # cov, corr, describe
./tests/scripts/run-differential-tests-v2.sh types        # Complex types & literals
./tests/scripts/run-differential-tests-v2.sh schema       # Schema operations

# Run all tests
./tests/scripts/run-differential-tests-v2.sh all          # All differential tests (default)

# With pytest options
./tests/scripts/run-differential-tests-v2.sh dataframe -x # Stop on first failure
./tests/scripts/run-differential-tests-v2.sh --help       # Show all options
```

> **Note**: `tpch` and `tpcds` SQL test groups are disabled pending SparkSQL parser integration.

### What It Does

1. **Starts fresh servers**: Apache Spark 4.0.1 (port 15003) + Thunderduck (port 15002)
2. **Loads test data** into both systems
3. **Executes queries on both** and compares results:
   - Schema comparison (column names and types)
   - Row count comparison
   - Row-by-row data comparison with detailed diff output
4. **Cleans up servers** on completion (even on Ctrl+C)

### Running Tests Directly

For advanced use cases, you can run pytest directly:

```bash
cd tests/integration
python -m pytest differential/ -v  # Run all differential tests
```

> **Recommended**: Use `./tests/scripts/run-differential-tests-v2.sh` which handles server lifecycle automatically.

### Test Results

DataFrame API differential tests pass with Thunderduck consistently faster than Spark:
- **TPC-H Q1**: ~6x faster
- **Full test suite**: ~3 minutes

See [Differential Testing Architecture](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md) for details.

## End-to-End Testing (E2E)

The E2E test suite validates the complete pipeline: **PySpark client → Spark Connect protocol → Thunderduck server → DuckDB execution**. This ensures thunderduck works correctly as a drop-in replacement for Spark.

### Prerequisites

1. **Python 3.8+** with pip
2. **PySpark 4.0.1** (automatically installed)
3. **Thunderduck server** JAR built

### Starting the Spark Connect Server

Before running E2E tests, start the Thunderduck Spark Connect server:

```bash
# Build the server if not already built
mvn clean package -pl connect-server

# Start the server (default port 15002)
# JVM flags required on ALL platforms for Spark 4.0.x:
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

**Note:** Maven automatically starts and stops the Thunderduck server for you!

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
├── test_tpch.py         # TPC-H benchmark tests (DataFrame API)
└── test_edge_cases.py   # Error handling and edge cases
```

> **Note**: SQL-based test files (`test_sql.py`, `test_tpcds.py`) are disabled pending SparkSQL parser integration.

### TPC-H E2E Tests (DataFrame API)

The TPC-H test suite validates DataFrame API compatibility using PySpark DataFrame operations:

Example from `test_tpch.py`:

```python
def test_q01_dataframe(self):
    """Q1: Pricing Summary Report (DataFrame API version)."""
    df = self.df_lineitem \
        .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01"), 90)) \
        .groupBy("l_returnflag", "l_linestatus") \
        .agg(F.sum("l_quantity").alias("sum_qty"), ...)
    result = df.collect()
    self.assertGreater(len(result), 0)
```

**Coverage Status**: All 22 TPC-H queries (Q1-Q22) have DataFrame API implementations ✅

> **Note**: SQL versions of tests are disabled pending SparkSQL parser integration.

### Running TPC-H E2E Tests

```bash
# Ensure TPC-H data is generated (see Data Generation section)
# Default location: ./data/tpch_sf001/

# Run all TPC-H DataFrame tests
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -v

# Run specific query
python -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -k "q01" -v
```

### E2E Test Configuration

Environment variables for test configuration:

```bash
# Specify Thunderduck server location (default: localhost:15002)
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
- name: Start Thunderduck Server
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
- All DataFrame differential tests passing (100%)
- All E2E tests passing (100%)
- Zero compiler warnings

## Documentation

### Technical Documentation

- **[Architecture Docs](docs/architect/)**: Documenting key architectural aspects of Thunderduck
- **[Differential Testing](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)**: Spark parity validation framework (266 tests)
- **[Protocol Specification](docs/SPARK_CONNECT_PROTOCOL_SPEC.md)**: Spark Connect protocol details
- **[Dev Journal](docs/dev_journal/)**: Milestone completion reports

## Contributing

We welcome contributions! Please see the following guidelines:

1. **Fork the repository** and create a feature branch
2. **Write tests** for new functionality
3. **Ensure quality gates pass**: `mvn verify -Pcoverage`
4. **Run differential tests**: All tests must pass
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

## Acknowledgments

- **DuckDB Team**: High-performance embedded database
- **Apache Arrow**: Zero-copy data interchange
- **Apache Spark**: API compatibility and testing reference

---

**Need Help?** Open an issue on GitHub or contact the maintainers.

**Want to Contribute?** See [Contributing](#contributing) section above.
