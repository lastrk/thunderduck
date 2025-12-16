# Thunderduck Integration Tests

Pytest-based integration tests for Thunderduck Spark Connect server using PySpark 4.0.1 client.

## Overview

This test suite validates the Spark Connect server implementation by executing TPC-H and TPC-DS queries, DataFrame operations, and function parity tests through a real PySpark Spark Connect client. Tests compare Thunderduck results against Apache Spark 4.0.1 reference.

**Differential Test Coverage (266 tests - ALL PASSING):**

| Test Suite | Tests | Description |
|------------|-------|-------------|
| TPC-H SQL | 23 | Q1-Q22 + sanity test |
| TPC-H DataFrame | 4 | Q1, Q3, Q6, Q12 via DataFrame API |
| TPC-DS SQL | 102 | Q1-Q99 (Q36 excluded) + variants |
| TPC-DS DataFrame | 24 | 24 queries via DataFrame API |
| Function Parity | 57 | Array, Map, Null, String, Math functions |
| Multi-dim Aggregations | 21 | pivot, unpivot, cube, rollup |
| Window Functions | 35 | rank, lag/lead, frame specs |
| **Total** | **266** | **All passing against Spark 4.0.1** |

## Quick Start (Differential Tests)

The recommended way to run integration tests is via the differential testing framework, which compares Thunderduck against Apache Spark 4.0.1:

```bash
# One-time setup (installs Spark 4.0.1, creates venv with all dependencies)
./tests/scripts/setup-differential-testing.sh

# Run ALL differential tests (266 tests)
./tests/scripts/run-differential-tests-v2.sh

# Run specific test group
./tests/scripts/run-differential-tests-v2.sh tpch         # TPC-H tests (27 tests)
./tests/scripts/run-differential-tests-v2.sh tpcds        # TPC-DS tests (126 tests)
./tests/scripts/run-differential-tests-v2.sh functions    # Function parity (57 tests)
./tests/scripts/run-differential-tests-v2.sh aggregations # Multi-dim aggregations (21 tests)
./tests/scripts/run-differential-tests-v2.sh window       # Window functions (35 tests)

# Show help
./tests/scripts/run-differential-tests-v2.sh --help
```

See [Differential Testing Architecture](../../docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md) for details.

## Directory Structure

```
tests/integration/
├── README.md                       # This file
├── conftest.py                     # pytest configuration and fixtures
├── .venv/                          # Python virtual environment (created by setup script)
├── .env                            # Environment configuration
│
│── # Differential Test Suites (266 tests total)
├── test_differential_v2.py         # TPC-H SQL + DataFrame tests (27 tests)
├── test_tpcds_differential.py      # TPC-DS SQL + DataFrame tests (126 tests)
├── test_dataframe_functions.py     # Function parity tests (57 tests)
├── test_multidim_aggregations.py   # pivot, unpivot, cube, rollup (21 tests)
├── test_window_functions.py        # Window function tests (35 tests)
│
│── # Legacy/Utility Tests
├── test_simple_sql.py              # Basic SQL connectivity tests
├── test_tpch_queries.py            # TPC-H query tests (standalone)
├── test_dataframe_operations.py    # DataFrame operation tests
├── test_temp_views.py              # Temp view functionality tests
│
└── utils/                          # Test utilities
    ├── __init__.py
    ├── server_manager.py           # Server lifecycle management
    ├── dual_server_manager.py      # Manages both Spark + Thunderduck servers
    ├── dataframe_diff.py           # Detailed DataFrame comparison
    └── result_validator.py         # Result validation utilities
```

## Setup Options

### Option 1: Differential Testing (Recommended)

Uses a dedicated virtual environment with all dependencies:

```bash
# One-time setup
./tests/scripts/setup-differential-testing.sh

# Run tests
./tests/scripts/run-differential-tests-v2.sh
```

### Option 2: Manual Setup

```bash
# Build server
mvn clean package -DskipTests

# Install dependencies (in a venv is recommended)
pip install pytest pyspark==4.0.1 pandas pyarrow grpcio grpcio-status

# Run TPC-H differential tests (~20 seconds)
cd tests/integration
python -m pytest test_differential_v2.py -v

# Run TPC-DS differential tests (~5 minutes)
python -m pytest test_tpcds_differential.py -k "Batch" -v
```

### Running Tests

**Run all tests**:
```bash
pytest tests/integration/ -v
```

**Run specific test file**:
```bash
pytest tests/integration/test_simple_sql.py -v
```

**Run specific test class**:
```bash
pytest tests/integration/test_tpch_queries.py::TestTPCHQuery1 -v
```

**Run specific test**:
```bash
pytest tests/integration/test_tpch_queries.py::TestTPCHQuery1::test_q1_sql -v
```

**Run with output**:
```bash
pytest tests/integration/ -v -s
```

**Run tests by marker**:
```bash
# Run only TPC-H tests
pytest tests/integration/ -v -m tpch

# Run only DataFrame API tests
pytest tests/integration/ -v -m dataframe

# Run only SQL tests
pytest tests/integration/ -v -m sql
```

**Run with timeout**:
```bash
# Set custom timeout for slow queries
pytest tests/integration/ -v --timeout=120
```

## Test Markers

The test suite uses custom pytest markers to categorize tests:

- `@pytest.mark.differential` - Differential tests (Spark vs Thunderduck)
- `@pytest.mark.tpch` - TPC-H benchmark tests
- `@pytest.mark.tpcds` - TPC-DS benchmark tests
- `@pytest.mark.dataframe` - Tests using DataFrame API
- `@pytest.mark.sql` - Tests using SQL
- `@pytest.mark.functions` - DataFrame function parity tests
- `@pytest.mark.aggregations` - Multi-dimensional aggregation tests
- `@pytest.mark.window` - Window function tests
- `@pytest.mark.slow` - Tests that take >10 seconds
- `@pytest.mark.quick` - Quick sanity tests

Markers are automatically assigned based on test names.

## Test Structure

### 1. TPC-H Query Tests

**File**: `test_tpch_queries.py`

Each TPC-H query has multiple test methods:

```python
class TestTPCHQuery1:
    def test_q1_sql(self, spark, load_tpch_query, validator):
        """Execute Q1 via SQL"""
        sql = load_tpch_query(1)  # Loads benchmarks/tpch_queries/q1.sql
        result = spark.sql(sql)
        # Validate results...

    def test_q1_dataframe_api(self, spark, tpch_data_dir, validator):
        """Execute Q1 via DataFrame API"""
        df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        result = df.filter(...).groupBy(...).agg(...).orderBy(...)
        # Validate results...

    def test_q1_sql_vs_dataframe(self, spark, load_tpch_query, tpch_data_dir):
        """Compare SQL and DataFrame API results"""
        # Execute both approaches and compare
```

**Current Coverage** (266 differential tests - all passing):
- ✅ TPC-H: 27 tests (Q1-Q22 SQL + 4 DataFrame API)
- ✅ TPC-DS: 126 tests (102 SQL + 24 DataFrame API, Q36 excluded)
- ✅ Function Parity: 57 tests (array, map, null, string, math functions)
- ✅ Multi-dim Aggregations: 21 tests (pivot, unpivot, cube, rollup)
- ✅ Window Functions: 35 tests (ranking, analytic, frame specs)

### 2. Basic DataFrame Operations

**File**: `test_tpch_queries.py` (class `TestBasicDataFrameOperations`)

Tests fundamental DataFrame operations:
- Reading Parquet files
- Filtering rows
- Selecting columns
- Aggregating data
- Grouping data
- Sorting results
- Joining tables

### 3. Simple SQL Tests

**File**: `test_simple_sql.py`

Basic connectivity and SQL execution tests:
- Simple SELECT statements
- Multiple columns
- VALUES clauses

## Fixtures

### Session-Scoped Fixtures

These fixtures are created once per test session:

- `server_manager` - Manages Spark Connect server lifecycle
- `spark_session` - PySpark Spark Connect session
- `workspace_dir` - Path to workspace root
- `tpch_data_dir` - Path to TPC-H data directory
- `tpch_queries_dir` - Path to TPC-H SQL queries

### Function-Scoped Fixtures

These fixtures are created for each test:

- `spark` - Alias for `spark_session`
- `validator` - ResultValidator instance
- `load_tpch_query` - Function to load TPC-H SQL queries
- `lineitem_df` - DataFrame for lineitem table
- `orders_df` - DataFrame for orders table
- `customer_df` - DataFrame for customer table
- (and 5 more table fixtures)

### Example Usage

```python
def test_my_query(spark, lineitem_df, validator):
    # Use lineitem_df directly
    result = lineitem_df.filter(col("l_quantity") > 40).count()

    # Or read manually
    result2 = spark.read.parquet("/workspace/data/tpch_sf001/lineitem.parquet")

    # Validate results
    validator.validate_row_count(result, expected_count=100)
```

## Server Management

### ServerManager Class

The `ServerManager` class handles server lifecycle:

```python
from utils.server_manager import ServerManager

# Standalone usage
manager = ServerManager(host="localhost", port=15002)

try:
    if manager.start(timeout=60):
        print("Server started successfully")
        # Run queries...
finally:
    manager.stop()

# Context manager usage
with ServerManager() as manager:
    # Server starts automatically
    # Run queries...
    pass  # Server stops automatically
```

**Features**:
- Automatic server startup/shutdown
- Port availability checking
- Graceful shutdown with timeout
- Process group management
- Log file creation
- Error handling and reporting

### Server Logs

Server logs are automatically created:
- `tests/integration/logs/server_stdout.log` - Server output
- `tests/integration/logs/server_stderr.log` - Server errors

Check logs when tests fail:
```bash
tail -f tests/integration/logs/server_stderr.log
```

## Result Validation

### ResultValidator Class

The `ResultValidator` class provides utilities for validating query results:

```python
from utils.result_validator import ResultValidator

validator = ResultValidator(epsilon=1e-6)

# Validate row count
validator.validate_row_count(df, expected_count=4)

# Validate schema
validator.validate_schema(df, ["col1", "col2", "col3"])

# Validate column values
validator.validate_column_values(df, "status", ["A", "F", "O"])

# Validate aggregates
validator.validate_aggregate_result(df, {
    "sum_qty": 1000.0,
    "avg_price": 25.50,
    "count": 42
})

# Compare DataFrames
validator.validate_dataframe_equals(actual_df, expected_df, check_order=True)

# Debug comparison
validator.print_comparison(actual_df, expected_df)
```

**Features**:
- Flexible validation methods
- Floating-point comparison with epsilon
- Schema validation
- Aggregate validation
- DataFrame comparison
- Debug printing

## Writing New Tests

### Template for TPC-H Query

```python
class TestTPCHQueryN:
    """TPC-H QN: Query Description

    Tests: operations tested
    Complexity: Simple/Moderate/Complex
    """

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_qN_sql(self, spark, load_tpch_query, validator):
        """Test TPC-H QN via SQL"""
        sql = load_tpch_query(N)
        result = spark.sql(sql)
        rows = result.collect()

        # Validate results
        validator.validate_row_count(result, expected_count=...)
        validator.validate_schema(result, [...])

        # Custom validations
        assert len(rows) > 0

        print(f"\n✓ TPC-H QN (SQL) passed: {len(rows)} rows returned")

    @pytest.mark.tpch
    @pytest.mark.dataframe
    @pytest.mark.timeout(60)
    def test_qN_dataframe_api(self, spark, tpch_data_dir, validator):
        """Test TPC-H QN via DataFrame API"""
        # Load tables
        table1 = spark.read.parquet(str(tpch_data_dir / "table1.parquet"))

        # Build query
        result = (table1
            .filter(...)
            .groupBy(...)
            .agg(...)
            .orderBy(...)
        )

        # Validate
        rows = result.collect()
        validator.validate_row_count(result, expected_count=...)

        print(f"\n✓ TPC-H QN (DataFrame API) passed: {len(rows)} rows")
```

### Template for Basic Operation

```python
def test_operation_name(spark, tpch_data_dir):
    """Test description"""
    df = spark.read.parquet(str(tpch_data_dir / "table.parquet"))

    result = df.operation(...)

    # Assertions
    assert condition, "Error message"

    print("\n✓ Operation succeeded")
```

## Troubleshooting

### Server Won't Start

1. **Check if port is in use**:
   ```bash
   lsof -i :15002
   # or
   ss -lptn 'sport = :15002'
   ```

2. **Kill existing server**:
   ```bash
   kill $(lsof -ti:15002)
   ```

3. **Check server logs**:
   ```bash
   tail -50 tests/integration/logs/server_stderr.log
   ```

4. **Rebuild server**:
   ```bash
   mvn clean package -DskipTests -pl connect-server -am
   ```

### Tests Timeout

1. **Increase timeout**:
   ```python
   @pytest.mark.timeout(120)  # 2 minutes
   ```

2. **Check server performance**:
   - Review server logs for slow queries
   - Check if DuckDB is struggling with data size

### Data Not Found

1. **Verify data exists**:
   ```bash
   ls -la /workspace/data/tpch_sf001/
   ```

2. **Generate data if missing**:
   - See main project README for data generation instructions

### Test Failures

1. **Run single test with verbose output**:
   ```bash
   pytest tests/integration/test_tpch_queries.py::TestTPCHQuery1::test_q1_sql -v -s
   ```

2. **Use validator's print_comparison**:
   ```python
   validator.print_comparison(actual_df, expected_df)
   ```

3. **Collect and inspect results**:
   ```python
   rows = result.collect()
   for row in rows:
       print(row)
   ```

## Performance Benchmarking

To track query performance, capture timing:

```python
import time

def test_q1_performance(spark, load_tpch_query):
    sql = load_tpch_query(1)

    start = time.time()
    result = spark.sql(sql)
    rows = result.collect()  # Force execution
    duration = time.time() - start

    print(f"\nQ1 execution time: {duration:.3f}s")
    assert duration < 5.0, f"Q1 too slow: {duration:.3f}s"
```

## CI/CD Integration

To run tests in CI:

```yaml
# .github/workflows/integration-tests.yml
- name: Run integration tests
  run: |
    mvn clean package -DskipTests -pl connect-server -am
    pytest tests/integration/ -v --junit-xml=integration-test-results.xml
```

## Future Enhancements

- [x] Add TPC-DS differential tests (126 tests, all passing)
- [x] Add DataFrame function parity tests (57 tests)
- [x] Add multi-dimensional aggregation tests (21 tests)
- [x] Add window function tests (35 tests)
- [x] Add test runner with named test groups
- [ ] Add complex data types tests (nested structs, advanced array/map)
- [ ] Add performance regression detection
- [ ] Create HTML test report generation
- [ ] Integrate into CI/CD pipeline

## Resources

- [TPC-H Specification](http://www.tpc.org/tpch/)
- [Spark Connect Protocol](https://github.com/apache/spark/tree/master/connector/connect)
- [PySpark API Reference](https://spark.apache.org/docs/4.0.1/api/python/)
- [pytest Documentation](https://docs.pytest.org/)
- [Differential Testing Architecture](../../docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)

---

**Last Updated**: 2025-12-16
**PySpark Version**: 4.0.1
