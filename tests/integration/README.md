# Thunderduck Integration Tests

Pytest-based integration tests for Thunderduck Spark Connect server using PySpark 4.0.1 client.

## Overview

This test suite validates the Spark Connect server implementation by running queries against both Thunderduck and Apache Spark 4.0.1, comparing results for correctness.

## Directory Structure

```
tests/integration/
├── conftest.py                         # Pytest fixtures
├── README.md                           # This file
│
├── differential/                       # All tests (vs Apache Spark 4.0.1)
│   ├── test_differential_v2.py         # TPC-H SQL + DataFrame (34 tests)
│   ├── test_tpcds_differential.py      # TPC-DS SQL + DataFrame (124 tests)
│   ├── test_tpcds_dataframe_differential.py  # TPC-DS DataFrame API (33 tests)
│   ├── test_dataframe_functions.py     # Function parity (57 tests)
│   ├── test_multidim_aggregations.py   # pivot, cube, rollup (21 tests)
│   ├── test_window_functions.py        # Window functions (35 tests)
│   ├── test_dataframe_ops_differential.py  # DataFrame operations (25 tests)
│   ├── test_lambda_differential.py     # Lambda/HOF functions (18 tests)
│   ├── test_using_joins_differential.py    # USING join syntax (11 tests)
│   ├── test_statistics_differential.py # cov, corr, describe, summary (16 tests)
│   ├── test_complex_types_differential.py  # struct, array, map access (15 tests)
│   ├── test_type_literals_differential.py  # Type literals, intervals (32 tests)
│   ├── test_to_schema_differential.py  # df.to(schema) support (12 tests)
│   ├── test_catalog_operations.py      # Catalog operations differential (14 tests)
│   ├── test_empty_dataframe.py         # Empty DataFrame differential (16 tests)
│   ├── test_simple_sql.py              # Basic SQL differential (3 tests)
│   └── test_temp_views.py              # Temporary view differential (7 tests)
│
├── tpch_sf001/                         # TPC-H SF0.01 data (parquet)
├── tpcds_dataframe/                    # TPC-DS DataFrame API implementations
├── sql/                                # SQL query files
└── utils/                              # Test utilities
```

## Quick Start

### Run Differential Tests (Recommended)

Compare Thunderduck against Apache Spark 4.0.1:

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run all differential tests
./tests/scripts/run-differential-tests-v2.sh

# Run specific test groups
./tests/scripts/run-differential-tests-v2.sh tpch         # TPC-H (34 tests)
./tests/scripts/run-differential-tests-v2.sh tpcds        # TPC-DS (126 tests)
./tests/scripts/run-differential-tests-v2.sh functions    # Functions (57 tests)
./tests/scripts/run-differential-tests-v2.sh aggregations # Aggregations (21 tests)
./tests/scripts/run-differential-tests-v2.sh window       # Window (35 tests)
./tests/scripts/run-differential-tests-v2.sh operations   # DataFrame ops (25 tests)
./tests/scripts/run-differential-tests-v2.sh lambda       # Lambda/HOF (18 tests)
./tests/scripts/run-differential-tests-v2.sh joins        # USING joins (11 tests)
./tests/scripts/run-differential-tests-v2.sh statistics   # Statistics (16 tests)
./tests/scripts/run-differential-tests-v2.sh types        # Complex types (47 tests)
./tests/scripts/run-differential-tests-v2.sh schema       # ToSchema (12 tests)
./tests/scripts/run-differential-tests-v2.sh dataframe    # TPC-DS DataFrame API (34 tests)
```

### Run All Tests

All tests are now in the `differential/` directory:

```bash
cd tests/integration

# Run all tests
python3 -m pytest differential/ -v

# Run specific test file
python3 -m pytest differential/test_catalog_operations.py -v
```

## Test Categories

### Differential Tests (`differential/`)

These tests run the same query on both Thunderduck and Apache Spark 4.0.1, comparing results row-by-row.

| Suite | Tests | Description |
|-------|-------|-------------|
| TPC-H | 34 | Q1-Q22 SQL + DataFrame API |
| TPC-DS SQL | 124 | 100 SQL + 24 DataFrame (Q36, Q72 excluded) |
| TPC-DS DataFrame | 33 | DataFrame API queries (Q72 excluded) |
| Functions | 57 | Array, Map, String, Math |
| Aggregations | 21 | pivot, unpivot, cube, rollup |
| Window | 35 | rank, lag/lead, frames |
| Operations | 25 | drop, withColumn, union, sample |
| Lambda | 18 | transform, filter, aggregate |
| Joins | 11 | USING join syntax |
| Statistics | 16 | cov, corr, describe, summary |
| Complex Types | 15 | struct.field, arr[i], map[key] |
| Type Literals | 32 | timestamps, intervals, arrays, maps |
| ToSchema | 12 | df.to(schema) column reorder/cast |
| Catalog | 14 | functionExists, tableExists, temp views |
| Empty DataFrame | 16 | Empty DataFrame handling |
| SQL Basics | 3 | Basic SQL connectivity |
| Temp Views | 7 | Temporary view management |
| **Total** | **~475** | |

**Note**: Q36 excluded (DuckDB GROUPING limitation), Q72 excluded (Spark OOM).

## Fixtures

Key fixtures in `conftest.py`:

### Session Management (class-scoped)

- `spark_thunderduck` / `spark` - Thunderduck session (port 15002)
- `spark_reference` - Apache Spark session (port 15003)
- `orchestrator` - Session-scoped orchestrator with timing and health monitoring

### Isolated Sessions (function-scoped)

- `spark_reference_isolated` - Function-scoped Spark session (per-test isolation)
- `thunderduck_isolated` - Function-scoped Thunderduck session (per-test isolation)
- `fresh_spark_server` - Restarts Spark server before test (slow, use sparingly)
- `fresh_thunderduck_server` - Restarts Thunderduck server before test (slow, use sparingly)

### Data Fixtures

- `tpch_data_dir` - Path to TPC-H parquet files
- `tpch_tables_reference` / `tpch_tables_thunderduck` - TPC-H tables loaded in sessions
- `tpcds_tables_reference` / `tpcds_tables_thunderduck` - TPC-DS tables loaded in sessions

## Configuration

All configuration is via environment variables. Defaults are tuned for quick failure detection.

### Timeout Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CONNECT_TIMEOUT` | 10s | PySpark session creation timeout |
| `QUERY_PLAN_TIMEOUT` | 5s | Query plan building timeout (should be <1s normally) |
| `COLLECT_TIMEOUT` | 10s | Result collection/materialization timeout |
| `HEALTH_CHECK_TIMEOUT` | 2s | Server health check ping timeout |
| `SERVER_STARTUP_TIMEOUT` | 60s | Server startup timeout |

### Port Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_PORT` | 15003 | Spark Reference server port |
| `THUNDERDUCK_PORT` | 15002 | Thunderduck server port |

### Memory Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_MEMORY` | 4g | Spark driver memory |
| `THUNDERDUCK_MEMORY` | 2g | Thunderduck JVM heap |

### Behavior Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR` | false | Continue on hard errors (for CI/CD) |

### Examples

```bash
# Default configuration (local development)
./tests/scripts/run-differential-tests-v2.sh

# Longer timeout for slow queries
COLLECT_TIMEOUT=30 ./tests/scripts/run-differential-tests-v2.sh tpcds

# CI/CD mode - continue on hard errors
THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true ./tests/scripts/run-differential-tests-v2.sh

# Custom ports
SPARK_PORT=15010 THUNDERDUCK_PORT=15011 ./tests/scripts/run-differential-tests-v2.sh
```

## Error Types

The V2 infrastructure distinguishes between:

### Hard Errors (halt test suite)
- `ServerCrashError` - Server process died
- `QueryTimeoutError` - Query exceeded timeout (deadlock detection)
- `ServerConnectionError` - Failed to connect to server
- `HealthCheckError` - Server health check failed

### Soft Errors (continue testing)
- `ResultMismatchError` - Results differ between Spark and Thunderduck

Set `THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true` to continue on hard errors (useful for CI/CD to collect all failures).

## Timing Measurements

The V2 infrastructure measures and reports:

- **Connect Times** - Time to create PySpark session
- **Query Plan Times** - Time to build query plan (usually negligible)
- **Collect Times** - Time to materialize and transfer results

A timing summary is printed at the end of each test run:

```
======================================================================
TIMING SUMMARY
======================================================================

Connect Times (seconds):
  Spark Reference:  n= 150  mean=0.245  min=0.089  max=1.234
  Thunderduck:      n= 150  mean=0.156  min=0.045  max=0.567

Query Plan Times (seconds) - time to build query plan:
  Spark Reference:  n= 600  mean=0.002  min=0.001  max=0.015
  Thunderduck:      n= 600  mean=0.001  min=0.000  max=0.008

Collect Times (seconds) - time to materialize + transfer:
  Spark Reference:  n= 600  mean=0.823  min=0.045  max=5.234
  Thunderduck:      n= 600  mean=0.156  min=0.012  max=2.345

Total Times (seconds):
  Spark Reference:  532.456
  Thunderduck:      98.234
======================================================================
```

## Troubleshooting

### Server Won't Start

```bash
pkill -9 -f thunderduck-connect-server
mvn clean package -DskipTests
```

### Check Server Logs

```bash
tail -f /tmp/thunderduck-server.log
```

---

**Last Updated**: 2025-12-20
