# Differential Testing Architecture

**Last Updated:** 2025-12-16
**Status:** Production Ready
**Spark Version:** 4.0.1

## Overview

The differential testing framework compares Thunderduck against Apache Spark 4.0.1 to ensure exact compatibility. Both systems run via Spark Connect protocol for fair comparison.

**Test Coverage (266 tests - ALL PASSING):**

| Test Category | Tests | Status |
|--------------|-------|--------|
| TPC-H SQL (Differential) | 23 | ✅ ALL PASS |
| TPC-H DataFrame API (Differential) | 4 | ✅ ALL PASS |
| TPC-DS SQL (Differential) | 102 | ✅ ALL PASS |
| TPC-DS DataFrame API (Differential) | 24 | ✅ ALL PASS |
| DataFrame Function Parity (Differential) | 57 | ✅ ALL PASS |
| Multi-dimensional Aggregations (Differential) | 21 | ✅ ALL PASS |
| Window Functions (Differential) | 35 | ✅ ALL PASS |
| **Total Differential Tests** | **266** | ✅ **ALL PASS** |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Test Suite (Pytest)                        │
│        test_differential_v2.py / test_tpcds_differential.py   │
│        test_dataframe_functions.py / test_multidim_agg.py     │
│                   test_window_functions.py                    │
└───────────────────┬────────────────┬─────────────────────────┘
                    │                │
        ┌───────────▼──────┐  ┌──────▼──────────┐
        │  Spark Reference │  │  Thunderduck    │
        │     Fixture      │  │     Fixture     │
        └───────────┬──────┘  └──────┬──────────┘
                    │                │
     PySpark        │                │        PySpark
     Connect        │                │        Connect
     Client         │                │        Client
                    │                │
        ┌───────────▼──────┐  ┌──────▼──────────┐
        │  Spark Connect   │  │  Thunderduck    │
        │  4.0.1 Server    │  │  Connect Server │
        │  (Native)        │  │  (Java)         │
        │  :15003          │  │  :15002         │
        └──────────────────┘  └──────────────────┘
                │                      │
        ┌───────▼──────┐      ┌───────▼──────┐
        │ Apache Spark │      │   DuckDB     │
        │ Local Engine │      │   Runtime    │
        └──────────────┘      └──────────────┘
```

## Components

### 1. Server Management (`DualServerManager`)

Orchestrates both servers with automatic lifecycle management:

```python
class DualServerManager:
    def __init__(self, thunderduck_port=15002, spark_reference_port=15003):
        ...

    def start_both(self, timeout=120) -> Tuple[bool, bool]:
        """Start both servers, returns (spark_ok, thunderduck_ok)"""

    def stop_both(self):
        """Stop both servers gracefully"""
```

**Location:** `tests/integration/utils/dual_server_manager.py`

### 2. DataFrame Diff Utility (`dataframe_diff.py`)

Detailed row-by-row comparison with:
- Schema validation (names, types, nullability)
- Numeric tolerance (configurable epsilon)
- Detailed diff output showing exact differences

**Location:** `tests/integration/utils/dataframe_diff.py`

### 3. Test Fixtures (`conftest.py`)

Session-scoped fixtures for performance:

```python
@pytest.fixture(scope="session")
def dual_server_manager():
    """Starts both servers, kills on teardown"""

@pytest.fixture(scope="session")
def spark_reference(dual_server_manager):
    """PySpark session connected to Spark 4.0.1"""

@pytest.fixture(scope="session")
def spark_thunderduck(dual_server_manager):
    """PySpark session connected to Thunderduck"""
```

Includes signal handlers for proper cleanup on Ctrl+C.

### 4. Setup Script (`setup-differential-testing.sh`)

One-time setup that:
1. Checks Java/Python prerequisites
2. Downloads and installs Apache Spark 4.0.1
3. Creates Python virtual environment
4. Installs all dependencies
5. Builds Thunderduck

### 5. Run Script (`run-differential-tests-v2.sh`)

Test runner that:
1. Activates virtual environment
2. Kills existing server processes
3. Runs pytest
4. Cleans up on exit (trap handlers)

## Key Design Decisions

### 1. Spark Connect on Both Sides

**Decision:** Both Thunderduck and Spark reference run as Spark Connect servers.

**Rationale:** Ensures both systems are tested via the same protocol, making comparisons fair and reducing protocol-level differences.

### 2. Native Spark Installation (vs Containers)

**Decision:** Use native Spark installation instead of Docker/Podman containers.

**Rationale:**
- Simpler setup and debugging
- Works in all environments (including containers)
- Faster startup time
- Easier to customize configuration

### 3. Session-Scoped Fixtures

**Decision:** Start servers once per test session, not per test.

**Rationale:** Starting servers for each test would add 10+ seconds of overhead per test. Session-scoped fixtures make the full TPC-H suite run in ~20 seconds.

### 4. Port Separation

**Decision:** Thunderduck on 15002, Spark on 15003.

**Rationale:** Allows both servers to run simultaneously without conflicts. Tests can connect to both and compare results.

### 5. Isolated Python Environment

**Decision:** Use a dedicated virtual environment in `tests/integration/.venv/`.

**Rationale:**
- Avoids conflicts with system Python packages
- Ensures reproducible dependency versions
- Isolates PySpark 4.0.1 from other projects

## File Structure

```
tests/
├── scripts/
│   ├── setup-differential-testing.sh     # One-time setup
│   ├── run-differential-tests-v2.sh      # Run tests with named groups
│   ├── start-spark-4.0.1-reference.sh    # Start Spark
│   └── stop-spark-4.0.1-reference.sh     # Stop Spark
│
├── integration/
│   ├── .venv/                            # Python virtual environment
│   ├── .env                              # Environment config
│   ├── conftest.py                       # Pytest fixtures
│   │
│   │── # Differential Test Suites (266 tests total)
│   ├── test_differential_v2.py           # TPC-H SQL + DataFrame tests (27 tests)
│   ├── test_tpcds_differential.py        # TPC-DS SQL + DataFrame tests (126 tests)
│   ├── test_dataframe_functions.py       # Function parity tests (57 tests)
│   ├── test_multidim_aggregations.py     # pivot, unpivot, cube, rollup (21 tests)
│   ├── test_window_functions.py          # Window function tests (35 tests)
│   │
│   └── utils/
│       ├── dual_server_manager.py        # Server orchestration
│       ├── dataframe_diff.py             # Row-by-row comparison utility
│       ├── server_manager.py             # Single server manager
│       └── result_validator.py           # Result validation utilities

benchmarks/
├── tpch_queries/                         # TPC-H SQL queries (Q1-Q22)
└── tpcds_queries/                        # TPC-DS SQL queries (Q1-Q99 + variants)
```

## Usage

### Quick Start

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run ALL differential tests (266 tests)
./tests/scripts/run-differential-tests-v2.sh
```

### Running by Test Group

The test runner supports named test groups for targeted testing:

```bash
# Run specific test group
./tests/scripts/run-differential-tests-v2.sh tpch         # TPC-H tests (27 tests)
./tests/scripts/run-differential-tests-v2.sh tpcds        # TPC-DS tests (126 tests)
./tests/scripts/run-differential-tests-v2.sh functions    # Function parity (57 tests)
./tests/scripts/run-differential-tests-v2.sh aggregations # Multi-dim aggregations (21 tests)
./tests/scripts/run-differential-tests-v2.sh window       # Window functions (35 tests)
./tests/scripts/run-differential-tests-v2.sh all          # All tests (default)

# With additional pytest args
./tests/scripts/run-differential-tests-v2.sh window -x    # Stop on first failure
./tests/scripts/run-differential-tests-v2.sh all --tb=long # Verbose traceback

# Show help
./tests/scripts/run-differential-tests-v2.sh --help
```

### Running with Pytest Directly

```bash
# Activate venv first
source tests/integration/.venv/bin/activate
cd tests/integration

# Run all TPC-H tests (~20 seconds)
python -m pytest test_differential_v2.py -v

# Run all TPC-DS tests (~5 minutes)
python -m pytest test_tpcds_differential.py -k "Batch" -v

# Run function parity tests
python -m pytest test_dataframe_functions.py -v

# Run aggregation tests
python -m pytest test_multidim_aggregations.py -v

# Run window function tests
python -m pytest test_window_functions.py -v

# Run specific TPC-H query
python -m pytest test_differential_v2.py::TestTPCH_Q1_Differential -v -s

# Run by marker
python -m pytest -m "differential" -v  # All differential tests
python -m pytest -m "window" -v        # Window function tests
python -m pytest -m "functions" -v     # Function parity tests
```

## Test Output

### Successful Test
```
================================================================================
Comparing: TPC-H Q1
================================================================================
✓ Schemas match
  Reference rows: 4
  Test rows:      4
✓ Row counts match
✓ All 4 rows match

Performance:
  Spark Reference: 2.156s
  Thunderduck:     0.234s
  Speedup:         9.21x

================================================================================
✓ TPC-H Q1 PASSED
================================================================================
```

### Failed Test with Diff
```
================================================================================
DIFF: 1 mismatched rows
================================================================================

--- Row 0 ---
Reference row:
  l_returnflag: N
  sum_charge: 10385578.38

Test row:
  l_returnflag: N
❌ sum_charge: 10385578.37

Differences:
  sum_charge:
    Reference: 10385578.38 (float)
    Test:      10385578.37 (float)
    Diff:      0.0100000000
```

## Server Lifecycle

1. **Before tests start:** `kill_all_servers()` ensures clean slate
2. **Fixture setup:** Starts both servers via `DualServerManager`
3. **Tests run:** Use `spark_reference` and `spark_thunderduck` fixtures
4. **Fixture teardown:** `stop_both()` stops servers gracefully
5. **Signal handlers:** Ctrl+C triggers cleanup via registered signal handlers
6. **Fallback:** `atexit` handler ensures cleanup even on crashes

## Prerequisites

### Required Software
- Python 3.8+ with venv module
- Java 17+
- Maven (for building Thunderduck)
- curl (for downloading Spark)

### Required Data
- TPC-H SF0.01 data in `data/tpch_sf001/`
- TPC-DS SF1 data in `data/tpcds_sf1/`

## Troubleshooting

### Port already in use
```bash
pkill -9 -f "org.apache.spark.sql.connect.service.SparkConnectServer"
pkill -9 -f thunderduck-connect-server
```

### Virtual environment not found
```bash
./tests/scripts/setup-differential-testing.sh
```

### Import errors
```bash
source tests/integration/.venv/bin/activate
python -c "import pyspark; print(pyspark.__version__)"
```

## Detailed Test Coverage

### TPC-H Tests (27 tests)

- **SQL Tests**: Q1-Q22 (23 queries)
- **DataFrame API Tests**: Q1, Q3, Q6, Q12 (4 queries)
- Sanity test

### TPC-DS Tests (126 tests)

- **SQL Tests**: Q1-Q99 (102 queries, Q36 excluded)
- **Variant queries**: Q14a/b, Q23a/b, Q24a/b, Q39a/b
- **DataFrame API Tests**: 24 queries (Q3, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q41, Q42, Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q82, Q84, Q91, Q96, Q98, Q99)
- **Q36 Excluded**: Uses `GROUPING()` in window `PARTITION BY`, unsupported by DuckDB

### DataFrame Function Parity Tests (57 tests)

| Test Class | Tests | Functions Covered |
|------------|-------|-------------------|
| TestArrayFunctions | 17 | `array_contains`, `array_size`, `sort_array`, `array_distinct`, `array_union`, `array_intersect`, `array_except`, `arrays_overlap`, `array_position`, `element_at`, `explode`, `explode_outer`, `flatten`, `reverse`, `slice`, `array_join` |
| TestMapFunctions | 7 | `map_keys`, `map_values`, `map_entries`, `size`, `element_at`, `map_from_arrays`, `explode` on maps |
| TestNullFunctions | 8 | `coalesce`, `isnull`, `isnotnull`, `ifnull`, `nvl`, `nvl2`, `nullif`, `nanvl` |
| TestStringFunctions | 14 | `concat`, `concat_ws`, `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`, `substring`, `instr`, `locate`, `lpad`, `rpad`, `repeat`, `reverse`, `split`, `replace`, `initcap` |
| TestMathFunctions | 11 | `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `mod`, `pmod`, `greatest`, `least`, `log`, `exp`, `sign` |

### Multi-dimensional Aggregation Tests (21 tests)

| Test Class | Tests | Operations Covered |
|------------|-------|--------------------|
| TestPivotFunctions | 6 | `pivot` with sum, avg, max/min, multiple aggregations, explicit values, multiple groupBy |
| TestUnpivotFunctions | 3 | `unpivot`, `melt`, subset columns |
| TestCubeFunctions | 4 | `cube` with single/two columns, `grouping()`, `grouping_id()` |
| TestRollupFunctions | 5 | `rollup` with single/two/three columns, `grouping()`, with filter |
| TestAdvancedAggregations | 3 | cube vs rollup comparison, pivot then aggregate, multiple aggregations |

### Window Function Tests (35 tests)

| Test Class | Tests | Functions Covered |
|------------|-------|-------------------|
| TestRankingFunctions | 7 | `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist`, multiple ranking |
| TestAnalyticFunctions | 10 | `lag`, `lead` (with offset/default), `first`, `last`, `nth_value`, combined lag/lead |
| TestFrameSpecifications | 7 | `ROWS BETWEEN`, `RANGE BETWEEN` with various boundaries (unbounded, fixed, current) |
| TestAggregateWindowFunctions | 6 | `sum`, `avg`, `min`, `max`, `count`, `stddev` over windows, ratio to partition |
| TestAdvancedWindowFunctions | 5 | multiple windows, window with filter, running difference, cumulative metrics, global window |

### DataFrame API Operations Tested

| Operation | Coverage |
|-----------|----------|
| `filter()` | All 28 DataFrame tests |
| `groupBy().agg()` | Q1, Q3, Q6, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q42, Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q82, Q84, Q91, Q96, Q98, Q99 |
| `join()` (multi-table) | Q3, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q43, Q45, Q48, Q50, Q62, Q82, Q84, Q91, Q96, Q99 |
| `orderBy()` / `limit()` | Most queries |
| `F.sum()`, `F.avg()`, `F.count()` | Q1, Q3, Q6, Q7, Q12, Q13, Q20, Q26, Q43, Q52, Q55, Q62, Q91, Q98, Q99 |
| `F.when().otherwise()` | Q12, Q43, Q62, Q99 (conditional aggregation) |
| `F.col().isin()` | Q7, Q12, Q15, Q20, Q26, Q37, Q41, Q45, Q82, Q91 |
| Window functions | Q12, Q20, Q98 (partition by, revenue ratio) |
| `F.substring()` | Q19, Q62, Q99 |
| `F.concat_ws()` | Q84 |
| `distinct()` | Q41, Q84 |
| Left joins | Q37, Q82 |

## Future Work

1. ~~Add TPC-DS differential tests~~ ✅ DONE (126 tests)
2. ~~Add DataFrame function parity tests~~ ✅ DONE (57 tests)
3. ~~Add multi-dimensional aggregation tests~~ ✅ DONE (21 tests)
4. ~~Add window function tests~~ ✅ DONE (35 tests)
5. Add complex data types tests (nested structs, advanced array/map) - TODO
6. Add performance benchmarking and reporting - TODO
7. Integrate into CI/CD pipeline - TODO
8. Create HTML test report generation - TODO

## Known Limitations

### TPC-DS Q36 (Excluded)

- Uses `GROUPING()` in window `PARTITION BY` clause
- DuckDB does not support this pattern
- A rewritten version exists (`q36_rewritten.sql`) using UNION ALL

### TPC-DS Queries Incompatible with DataFrame API

65 TPC-DS queries require SQL-specific features:
- CTEs (WITH clauses): 26 queries
- Subqueries in FROM clause: 36 queries
- ROLLUP/GROUPING SETS: 10 queries
- EXISTS/NOT EXISTS: 5 queries
- INTERSECT/EXCEPT: 3 queries

---

**See Also:**
- [Session Management Architecture](SESSION_MANAGEMENT_ARCHITECTURE.md)
- [Arrow Streaming Architecture](ARROW_STREAMING_ARCHITECTURE.md)
