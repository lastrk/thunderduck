# Thunderduck

[![Maven Build](https://img.shields.io/badge/maven-3.9+-blue.svg)](https://maven.apache.org/)
[![Java](https://img.shields.io/badge/java-21-orange.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

> **Alpha Software**: Despite extensive test coverage, Thunderduck is currently alpha quality software and will undergo extensive testing with real-world workloads before production readiness.

**Thunderduck** is an embedded execution engine that translates Spark operations to DuckDB SQL, providing fast single-node query execution as a drop-in replacement for Apache Spark.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Building from Source](#building-from-source)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Overview

Thunderduck provides a Spark-compatible API that translates DataFrame operations into optimized DuckDB SQL for embedded execution. It combines the familiar Spark programming model with the high-performance vectorized execution of DuckDB.

### Key Features

- **Spark Connect Server** for remote client connectivity (PySpark 4.1.x, Scala Spark)
- **Faster than Spark local mode** via DuckDB's vectorized engine
- **Multi-architecture support**: x86_64 (Intel/AMD) and ARM64 (AWS Graviton, Apple Silicon)
- **Arrow-native data interchange** with DuckDB's vectorized engine
- **Format support**: Parquet, Delta Lake (PLANNED), Iceberg (PLANNED)
- **746 differential tests** against Spark 4.1.1 -- TPC-H (100%), TPC-DS (99%), functions, joins, window, aggregations
- **Two compatibility modes**: Relaxed (vanilla DuckDB, best-effort type matching) and Strict (DuckDB extension, exact Spark type parity)
- **Query plan introspection** via EXPLAIN statements

### Why Thunderduck?

Most Spark workloads [don't need distributed computing](https://motherduck.com/blog/big-data-is-dead/) — they'd run faster and cheaper on a single node. Thunderduck lets you keep your Spark code while replacing the execution engine with DuckDB's vectorized, SIMD-optimized columnar processing. Zero-copy Arrow interchange eliminates serialization overhead between layers.

### Platform Support

Thunderduck supports **x86_64** (Intel/AMD) and **ARM64** (AWS Graviton, Apple Silicon) architectures. DuckDB automatically applies SIMD optimizations per architecture.

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
│   Arrow-Native Data Interchange                     │
└─────────────────────────────────────────────────────┘
```

### Core Components

- **Logical Plan Representation** (`core/logical/`): Spark logical plan nodes
- **Expression System** (`core/expression/`): Expression translation and evaluation
- **SQL Generation** (`core/generator/`): DuckDB SQL code generation
- **Type Mapping** (`core/types/`): Spark ↔ DuckDB type conversion
- **Function Registry** (`core/functions/`): 170+ function mappings
- **Spark Compatibility Mode** (`core/runtime/`): Strict/relaxed/auto modes with optional DuckDB extension
- **Runtime Execution** (`core/runtime/`): Session-scoped DuckDB runtime, Arrow streaming
- **Format Readers** (`core/io/`): Parquet, (PLANNED) Delta Lake, (PLANNED) Iceberg support

**Note**: thunderduck relies on **DuckDB's world-class query optimizer** rather than implementing custom optimization rules. DuckDB automatically performs filter pushdown, column pruning, join reordering, and many other optimizations.

## Quick Start

### Prerequisites

- **Java**: 21 or later
- **Maven**: 3.9 or later
- **Python**: 3.8+ (for PySpark client)

### Start the Server

```bash
# Clone and build
git clone https://github.com/lastrk/thunderduck.git
cd thunderduck
mvn clean package -DskipTests

# Start the Spark Connect server (default port 15002)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar

# Or start in strict mode (requires DuckDB extension, exact Spark type parity)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar --strict

# Or start in relaxed mode (vanilla DuckDB, no extension needed)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar --relaxed
```

The server will show:
```
INFO SparkConnectServer - Starting Spark Connect Server...
INFO SparkConnectServer - Server started successfully on port 15002
```

### Connect with PySpark

```bash
pip install pyspark==4.1.1
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

# Run differential tests (compares against Spark 4.1.1)
./tests/scripts/setup-differential-testing.sh  # One-time setup (creates .venv)
./tests/scripts/run-differential-tests-v2.sh   # Run tests (auto-detects .venv)
```

## Building from Source

### Clone the Repository

```bash
git clone https://github.com/lastrk/thunderduck.git
cd thunderduck

# Initialize submodules (required for DuckDB extension)
git submodule update --init --recursive
```

### Build All Modules

```bash
# Fast build (skip tests)
mvn clean package -DskipTests

# Build with Spark compatibility extension (strict mode support)
mvn clean package -DskipTests -Pbuild-extension

# Full build with tests
mvn clean install

# Build with coverage report
mvn clean verify -Pcoverage
```

### macOS Build Requirements

On macOS, protobuf binaries get blocked by Gatekeeper. Install `protoc` and `protoc-gen-grpc-java` locally, then build with the `use-system-protoc` profile:

```bash
brew install protobuf
# Install protoc-gen-grpc-java for your architecture (see docs)
mvn clean package -DskipTests -Puse-system-protoc
```

### Build Specific Modules

```bash
# Core module only
mvn clean install -pl core

# Connect server module
mvn clean install -pl connect-server

# Tests module
mvn clean install -pl tests
```

### Spark Compatibility Extension (Optional)

Thunderduck includes an optional DuckDB C extension (`thunderduck-duckdb-extension/`) that implements Spark-precise numerical semantics. Without it, Thunderduck uses vanilla DuckDB functions which produce correct values but may differ in output types. With the extension loaded, operations like decimal division use exact Spark rounding and type rules.

**When you need it**: Workloads that depend on exact decimal precision, `ROUND_HALF_UP` rounding, or strict Spark type compatibility.

**When you don't**: General analytics where approximate numeric equivalence is acceptable.

#### Building the Extension

Prerequisites: CMake 3.5+, C++17 compiler, Ninja (recommended)

**Using Maven (Recommended):**

```bash
# Build everything including extension, bundled automatically
mvn clean package -DskipTests -Pbuild-extension
```

The Maven `build-extension` profile detects the current platform, builds the extension via CMake/Ninja, copies it to `core/src/main/resources/extensions/<platform>/`, and includes it in the JAR. First build compiles DuckDB core (~2-5 min), subsequent builds are incremental (seconds).

**Manual build (for extension development):**

```bash
# Build for current platform
cd thunderduck-duckdb-extension
GEN=ninja make release

# Output: build/release/extension/thdck_spark_funcs/thdck_spark_funcs.duckdb_extension

# Copy to resources and rebuild JAR
PLATFORM=$(duckdb -c "PRAGMA platform" 2>/dev/null || echo "linux_amd64")
mkdir -p core/src/main/resources/extensions/$PLATFORM
cp thunderduck-duckdb-extension/build/release/extension/thdck_spark_funcs/thdck_spark_funcs.duckdb_extension \
   core/src/main/resources/extensions/$PLATFORM/
mvn clean package -DskipTests
```

When the server starts, it auto-detects and loads bundled extensions. If no extension is found for the current platform, the server starts normally using vanilla DuckDB functions.

> See [Spark Compatibility Extension Architecture](docs/architect/SPARK_COMPAT_EXTENSION.md) for full details on the two-mode compatibility design.

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
├── thunderduck-duckdb-extension/                # Optional DuckDB C extension (Spark-precise semantics)
│   ├── src/                   # Extension source (C++)
│   ├── test/sql/              # Extension SQL tests
│   ├── docs/                  # Integration guide
│   ├── CMakeLists.txt         # CMake build config
│   └── Makefile               # Build shortcuts
├── tests/                     # Comprehensive test suite
│   ├── src/test/java/         # Java unit tests
│   ├── integration/           # Python differential tests
│   │   ├── differential/      # Differential test suites (37 test files)
│   │   └── sql/               # TPC-H and TPC-DS SQL queries
│   └── pom.xml
└── docs/                      # Documentation
    ├── SPARK_CONNECT_GAP_ANALYSIS.md    # Comprehensive coverage analysis
    ├── SUPPORTED_OPERATIONS.md          # Quick operation reference
    └── architect/                       # Architecture design documents
```

## Testing

Thunderduck includes a comprehensive test suite with differential testing against Apache Spark 4.1.1:

### Test Categories

- **Unit Tests**: Type mapping, expression translation, SQL generation (Java/Maven)
- **Differential Tests**: Compare Thunderduck results against Spark 4.1.1 via Spark Connect protocol (Python/pytest)

### Differential Test Coverage (746 tests across 37 files)

| Test Suite | Tests | Description |
|------------|-------|-------------|
| TPC-H SQL + DataFrame | 51 | All 22 queries via both SQL and DataFrame API |
| TPC-DS SQL + DataFrame | 132 | TPC-DS queries via both paths |
| Functions | 57 | Array, Map, Null, String, Math functions |
| Aggregations | 21 | pivot, unpivot, cube, rollup, grouping |
| Window Functions | 35 | rank, lag/lead, frame specs, analytics |
| Joins | 26 | Inner, left, right, full, semi, anti, cross, USING |
| Set Operations | 14 | union, intersect, except with edge cases |
| Date/Time | 18 | Extraction, arithmetic, formatting |
| Type Casting | 14 | Explicit CAST operations |
| Statistics | 16 | cov, corr, describe, summary, crosstab |
| And more | ~362 | Conditional, sorting, distinct, schema, lambda, etc. |

See [Spark Connect Gap Analysis](docs/SPARK_CONNECT_GAP_ANALYSIS.md) for the full coverage inventory.

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

The differential testing framework compares Thunderduck results against Apache Spark 4.1.1 to ensure exact compatibility. Both systems run via Spark Connect protocol for fair comparison.

### Quick Start

```bash
# One-time setup (downloads Spark 4.1.1, creates .venv, installs Python deps)
./tests/scripts/setup-differential-testing.sh

# Run differential tests (auto-detects .venv, no manual activation needed)
./tests/scripts/run-differential-tests-v2.sh

# Or run pytest directly (activate venv first)
source .venv/bin/activate
cd tests/integration
python3 -m pytest differential/ -v
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

### Compatibility Modes in Tests

Tests support both compatibility modes:

```bash
# Run in strict mode (requires extension build: mvn clean package -DskipTests -Pbuild-extension)
THUNDERDUCK_COMPAT_MODE=strict ./tests/scripts/run-differential-tests-v2.sh

# Run in relaxed mode (vanilla DuckDB, no extension needed)
THUNDERDUCK_COMPAT_MODE=relaxed ./tests/scripts/run-differential-tests-v2.sh

# Auto mode (detect extension at runtime, default)
./tests/scripts/run-differential-tests-v2.sh
```

### Advanced: Direct pytest

For development and debugging, you can run pytest directly (activate venv first):

```bash
source .venv/bin/activate
cd tests/integration
THUNDERDUCK_COMPAT_MODE=strict python3 -m pytest differential/test_differential_v2.py -v --tb=long
```

### What It Does

1. **Starts fresh servers**: Apache Spark 4.1.1 + Thunderduck (auto-allocated ports)
2. **Loads test data** into both systems
3. **Executes queries on both** and compares results:
   - Schema comparison (column names and types)
   - Row count comparison
   - Row-by-row data comparison with detailed diff output
4. **Cleans up servers** on completion (even on Ctrl+C)

See [Differential Testing Architecture](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md) for details.

## Documentation

### Technical Documentation

- **[Architecture Docs](docs/architect/)**: Key architectural aspects of Thunderduck
- **[Spark Compat Extension](docs/architect/SPARK_COMPAT_EXTENSION.md)**: Two-mode compatibility design (strict/relaxed)
- **[Differential Testing](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)**: Spark parity validation framework
- **[Gap Analysis](docs/SPARK_CONNECT_GAP_ANALYSIS.md)**: Comprehensive Spark Connect protocol coverage analysis
- **[Supported Operations](docs/SUPPORTED_OPERATIONS.md)**: Quick reference of supported Spark operations
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
git clone https://github.com/lastrk/thunderduck.git
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
