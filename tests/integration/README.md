# Thunderduck Integration Tests

Pytest-based integration tests for Thunderduck Spark Connect server using real PySpark client.

## Overview

This test suite validates the Spark Connect server implementation by executing TPC-H queries and DataFrame operations through a real PySpark Spark Connect client. Tests cover both SQL and DataFrame API approaches.

## Directory Structure

```
tests/integration/
├── README.md                   # This file
├── conftest.py                 # pytest configuration and fixtures
├── test_tpch_queries.py        # TPC-H integration tests
├── test_simple_sql.py          # Basic SQL connectivity tests
├── utils/                      # Test utilities
│   ├── __init__.py
│   ├── server_manager.py       # Server lifecycle management
│   └── result_validator.py     # Result validation utilities
├── logs/                       # Server logs (auto-created)
└── expected_results/           # Expected query results
```

## Quick Start

### Prerequisites

1. **Build the server**:
   ```bash
   mvn clean package -DskipTests -pl connect-server -am
   ```

2. **Install Python dependencies**:
   ```bash
   pip install pytest pytest-timeout pyspark==3.5.3 pandas
   ```

3. **Generate TPC-H data** (if not already done):
   ```bash
   # Data should be in /workspace/data/tpch_sf001/
   # See main README for data generation instructions
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

- `@pytest.mark.tpch` - TPC-H benchmark tests
- `@pytest.mark.dataframe` - Tests using DataFrame API
- `@pytest.mark.sql` - Tests using SQL
- `@pytest.mark.slow` - Tests that take >10 seconds
- `@pytest.mark.timeout(N)` - Tests with custom timeout

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

**Current Coverage**:
- ✅ Q1: Pricing Summary Report (scan, filter, aggregate, sort)
- ✅ Q3: Shipping Priority (3-way joins, filtering, aggregation, limit)
- ✅ Q6: Forecasting Revenue Change (complex filters, aggregate)

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

- [ ] Add performance regression detection
- [ ] Implement expected result caching
- [ ] Add more TPC-H queries (Q2, Q4, Q5, Q7-Q22)
- [ ] Create HTML test report generation
- [ ] Add memory profiling
- [ ] Implement query plan validation
- [ ] Add distributed testing support

## Resources

- [TPC-H Specification](http://www.tpc.org/tpch/)
- [Spark Connect Protocol](https://github.com/apache/spark/tree/master/connector/connect)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [pytest Documentation](https://docs.pytest.org/)

---

**Last Updated**: 2025-10-25
**Version**: 1.0
