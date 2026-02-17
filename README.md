# Thunderduck

[![Maven Build](https://img.shields.io/badge/maven-3.9+-blue.svg)](https://maven.apache.org/)
[![Java](https://img.shields.io/badge/java-21-orange.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

> **Alpha Software**: Despite extensive test coverage, Thunderduck is currently alpha quality software and will undergo extensive testing with real-world workloads before production readiness.

**Thunderduck** is an embedded execution engine that translates Spark operations to DuckDB SQL, providing fast single-node query execution as a drop-in replacement for Apache Spark.

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

## Quick Start

### Prerequisites

- **Java** 21+, **Python** 3.11+, **Maven** 3.9+ (required)
- **C++ compiler** + **CMake** (optional — enables strict Spark type parity)

### Launch the Playground

```bash
git clone https://github.com/lastrk/thunderduck.git
cd thunderduck
./playground/launch.sh
```

This single script initializes submodules, builds Thunderduck (with the Spark compatibility extension if a C++ toolchain is detected), starts both servers, and opens a [Marimo](https://marimo.io/) notebook for interactive exploration.

## Architecture

Thunderduck uses a three-layer architecture:

```
┌─────────────────────────────────────────────────────────┐
│         Spark API Facade (DataFrame/Dataset)            │
│              Lazy Plan Construction                     │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│                Translation Engine                       │
│   Logical Plan → DuckDB SQL Translation                 │
│   Expression Mapping, Type Conversion                   │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│         DuckDB Execution Engine                         │
│   Vectorized Processing, SIMD Optimization              │
│   Arrow-Native Data Interchange                         │
└─────────────────────────────────────────────────────────┘
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

**Note**: Thunderduck relies on **DuckDB's world-class query optimizer** rather than implementing custom optimization rules. DuckDB automatically performs filter pushdown, column pruning, join reordering, and many other optimizations.

## Building from Source

See [Prerequisites](#prerequisites) above.

### Clone the Repository

```bash
git clone https://github.com/lastrk/thunderduck.git
cd thunderduck
git submodule update --init --recursive   # required for DuckDB extension
```

### macOS Build Requirements

On macOS, protobuf binaries get blocked by Gatekeeper. Install `protoc` locally, then build with the `use-system-protoc` profile:

```bash
brew install protobuf
mvn clean package -DskipTests -Puse-system-protoc
```

### Build (Java only, relaxed mode)

```bash
mvn clean package -DskipTests
```

### Build with DuckDB Extension (strict mode)

```bash
mvn clean package -DskipTests -Pbuild-extension
```

The extension implements Spark-precise numerical semantics (exact decimal division, `ROUND_HALF_UP` rounding). Without it, Thunderduck uses vanilla DuckDB functions which produce correct values but may differ in output types.

### Start the Server

```bash
# Default (auto-detect extension)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar

# Strict mode (requires extension build)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar --strict

# Relaxed mode (vanilla DuckDB, no extension needed)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar --relaxed
```

## Testing

Assumes the project is already built (see [Building from Source](#building-from-source)).

### Unit Tests

```bash
mvn install -pl core -DskipTests    # install core to local repo first
mvn test -pl tests                   # run test module
```

### Differential Tests — Relaxed Mode

```bash
./tests/scripts/run-differential-tests-v2.sh tpch
```

### Differential Tests — Strict Mode

```bash
THUNDERDUCK_COMPAT_MODE=strict ./tests/scripts/run-differential-tests-v2.sh tpch
```

Requires extension build (`mvn clean package -DskipTests -Pbuild-extension`).

The run script handles virtualenv setup, server lifecycle, and cleanup automatically. Run `./tests/scripts/run-differential-tests-v2.sh --help` for all test groups and options.

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

## Documentation

- **[Architecture Docs](docs/architect/)**: Key architectural aspects of Thunderduck
- **[Spark Compat Extension](docs/architect/SPARK_COMPAT_EXTENSION.md)**: Two-mode compatibility design (strict/relaxed)
- **[Differential Testing](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)**: Spark parity validation framework
- **[Gap Analysis](docs/SPARK_CONNECT_GAP_ANALYSIS.md)**: Comprehensive Spark Connect protocol coverage analysis
- **[Supported Operations](docs/SUPPORTED_OPERATIONS.md)**: Quick reference of supported Spark operations

## Contributing

1. **Fork the repository** and create a feature branch
2. **Write tests** for new functionality
3. **Ensure quality gates pass**: `mvn verify -Pcoverage`
4. **Run differential tests**: All tests must pass
5. **Submit a pull request** with clear description

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **DuckDB Team**: High-performance embedded database
- **Apache Arrow**: Zero-copy data interchange
- **Apache Spark**: API compatibility and testing reference
