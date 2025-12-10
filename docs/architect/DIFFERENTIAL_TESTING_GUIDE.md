# Differential Testing Guide

**Version:** 1.0
**Date:** 2025-12-09
**Purpose:** Comprehensive guide for differential and end-to-end testing of Thunderduck against Apache Spark

---

## Table of Contents

1. [Overview](#1-overview)
2. [Prerequisites](#2-prerequisites)
3. [Quick Start](#3-quick-start)
4. [Test Architecture](#4-test-architecture)
5. [Running Tests](#5-running-tests)
6. [Test Suites](#6-test-suites)
7. [Configuration](#7-configuration)
8. [Analyzing Results](#8-analyzing-results)
9. [Troubleshooting](#9-troubleshooting)
10. [CI/CD Integration](#10-cicd-integration)
11. [Writing New Tests](#11-writing-new-tests)

---

## 1. Overview

Differential testing validates that Thunderduck produces **identical results** to Apache Spark for the same queries. This is crucial for ensuring compatibility and correctness as a drop-in Spark replacement.

### Why Differential Testing?

- **Correctness Validation**: Ensures Thunderduck matches Spark's behavior exactly
- **Type Compatibility**: Validates that return types match (not just values)
- **Regression Detection**: Catches breaking changes early
- **API Coverage**: Tests both SQL and DataFrame APIs

### Testing Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Spark Connect** | Test against real Spark via Spark Connect protocol | Production validation |
| **Embedded Spark** | Use embedded Spark session within JVM | CI/CD pipelines |
| **Comparative** | Run both servers and compare results | Side-by-side analysis |

---

## 2. Prerequisites

### 2.1 Install Apache Spark 3.5.3

```bash
# Download Spark
wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.12.tgz

# Extract
tar -xzf spark-3.5.3-bin-hadoop3-scala2.12.tgz

# Move to /opt (or your preferred location)
sudo mv spark-3.5.3-bin-hadoop3-scala2.12 /opt/spark

# Set environment variables
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

### 2.2 Install PySpark

```bash
pip install pyspark==3.5.3
```

### 2.3 Install Python Test Dependencies

```bash
pip install pandas>=2.0.0 numpy>=1.24.0 pytest>=7.0.0 pytest-timeout>=2.1.0
```

### 2.4 Build Thunderduck

```bash
mvn clean package -DskipTests
```

---

## 3. Quick Start

### Step 1: Start Spark Connect Server

```bash
./tests/scripts/start-spark-connect.sh
```

This starts Spark in local mode with Spark Connect enabled on port 15002.

### Step 2: Run Differential Tests

```bash
# Run all differential tests
./tests/scripts/run-differential-tests.sh

# Run specific test suite
./tests/scripts/run-differential-tests.sh EndToEndQueryTest

# Run with verbose output
VERBOSE=true ./tests/scripts/run-differential-tests.sh
```

### Step 3: Stop Spark Connect Server

```bash
./tests/scripts/stop-spark-connect.sh
```

---

## 4. Test Architecture

### 4.1 End-to-End Pipeline

```
┌─────────────────┐
│  PySpark Client │  (Python test scripts)
└────────┬────────┘
         │ Spark Connect Protocol (gRPC)
         ↓
┌─────────────────┐
│  Thunderduck    │  (:50051)
│  SparkConnect   │
│     Server      │
└────────┬────────┘
         │ Translation & Execution
         ↓
┌─────────────────┐
│     DuckDB      │
└─────────────────┘
```

### 4.2 Test Directory Structure

```
tests/
├── src/
│   ├── main/
│   │   └── resources/
│   │       └── test-data/              # Shared test data
│   └── test/
│       ├── java/
│       │   └── com/thunderduck/
│       │       ├── differential/       # Differential test suites
│       │       │   ├── SparkConnectDifferentialTest.java
│       │       │   ├── TPCHSparkConnectTest.java
│       │       │   └── SparkDataFrameDifferentialTest.java
│       │       └── e2e/
│       │           ├── EndToEndTestRunner.java
│       │           └── SparkConnectE2ETest.java
│       └── python/
│           └── thunderduck_e2e/
│               ├── __init__.py
│               ├── test_runner.py      # Main test orchestrator
│               ├── spark_client.py     # PySpark client utilities
│               ├── validation.py       # Result validation
│               ├── test_dataframes.py  # DataFrame operation tests
│               ├── test_sql.py         # SQL functionality tests
│               ├── test_tpch.py        # TPC-H benchmark tests
│               └── test_edge_cases.py  # Edge cases & error handling
```

---

## 5. Running Tests

### 5.1 Against Spark Connect Server (Recommended)

```bash
# Start Spark Connect server
./tests/scripts/start-spark-connect.sh

# Run all differential tests
mvn test -pl tests \
  -Dtest="*Differential*" \
  -Dspark.connect.url=sc://localhost:15002

# Run specific test suite
mvn test -pl tests \
  -Dtest=SparkDataFrameDifferentialTest \
  -Dspark.connect.url=sc://localhost:15002

# Run with verbose output
mvn test -pl tests \
  -Dtest="*Differential*" \
  -Dspark.connect.url=sc://localhost:15002 \
  -Ddifferential.verbose=true

# Stop server when done
./tests/scripts/stop-spark-connect.sh
```

### 5.2 Against Embedded Spark (Local Mode)

```bash
# Run differential tests with embedded Spark
mvn test -pl tests \
  -Dtest="*Differential*" \
  -Dspark.master=local[*]

# With specific memory settings
mvn test -pl tests \
  -Dtest="*Differential*" \
  -Dspark.master=local[*] \
  -Dspark.driver.memory=4g \
  -Dspark.executor.memory=4g
```

### 5.3 Comparative Testing (Both Servers)

```bash
# Terminal 1: Start Spark Connect
./tests/scripts/start-spark-connect.sh

# Terminal 2: Start Thunderduck server
./tests/scripts/start-server.sh

# Terminal 3: Run comparative tests
mvn test -pl tests \
  -Dtest=ComparativeEndToEndTest \
  -Dspark.url=sc://localhost:15002 \
  -Dthunderduck.url=sc://localhost:50051
```

### 5.4 Python Tests Directly

```bash
# Run all Python E2E tests
python3 -m pytest tests/src/test/python/thunderduck_e2e/ -v

# Run specific test file
python3 -m pytest tests/src/test/python/thunderduck_e2e/test_tpch.py -v

# Run with coverage
python3 -m pytest tests/src/test/python/thunderduck_e2e/ --cov=thunderduck_e2e
```

---

## 6. Test Suites

### 6.1 Available Test Suites

| Test Suite | Description | Command |
|------------|-------------|---------|
| All Tests | Run complete test suite | `./tests/scripts/run-differential-tests.sh` |
| DataFrame Tests | Test DataFrame operations | `./tests/scripts/run-differential-tests.sh '*DataFrame*'` |
| SQL Tests | Test SQL query compatibility | `./tests/scripts/run-differential-tests.sh '*SQL*'` |
| Type Tests | Test type system compatibility | `./tests/scripts/run-differential-tests.sh '*Type*'` |
| Window Function Tests | Test window functions | `./tests/scripts/run-differential-tests.sh '*Window*'` |
| Aggregate Tests | Test aggregation functions | `./tests/scripts/run-differential-tests.sh '*Aggregate*'` |
| End-to-End Tests | Full integration tests | `./tests/scripts/run-differential-tests.sh 'EndToEnd*'` |
| TPC-H Tests | TPC-H benchmark validation | `./tests/scripts/run-differential-tests.sh 'TPCH*'` |
| TPC-DS Tests | TPC-DS benchmark validation | `./tests/scripts/run-differential-tests.sh 'TPCDS*'` |

### 6.2 Core Differential Tests

Located in `tests/src/test/java/com/thunderduck/differential/`:

1. **SparkDataFrameDifferentialTest**
   - Tests DataFrame operations (select, filter, group by, join)
   - Validates schema compatibility
   - Compares row-by-row results

2. **SparkSQLDifferentialTest**
   - Tests SQL query execution
   - Validates complex queries with aggregations
   - Tests window functions

3. **SparkTypeDifferentialTest**
   - Tests type compatibility
   - Validates numeric precision
   - Tests null handling

### 6.3 TPC-H Tests

```bash
# Run TPC-H differential tests
mvn test -pl tests \
  -Dtest=TPCHDifferentialTest \
  -Dspark.connect.url=sc://localhost:15002 \
  -Dtpch.scale=0.01
```

### 6.4 TPC-DS Tests

```bash
# Run TPC-DS differential tests
mvn test -pl tests \
  -Dtest=TPCDSDifferentialTest \
  -Dspark.connect.url=sc://localhost:15002 \
  -Dtpcds.scale=1
```

### 6.5 Dual API Testing Requirement

**Important**: Each TPC-H and TPC-DS query is tested in TWO ways:

1. **SQL Query**: Tests SQL translation path
2. **DataFrame API**: Tests DataFrame API translation path

This ensures complete coverage of both Spark APIs through the Spark Connect protocol.

```python
# Example: TPC-H Q1 tested both ways

def test_tpch_q1_sql(self):
    """TPC-H Query 1: SQL version."""
    df = self.spark.sql("""
        SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty
        FROM lineitem
        WHERE l_shipdate <= '1998-09-01'
        GROUP BY l_returnflag, l_linestatus
    """)
    # Validate results

def test_tpch_q1_dataframe(self):
    """TPC-H Query 1: DataFrame API version."""
    df = self.df_lineitem \
        .filter(F.col("l_shipdate") <= "1998-09-01") \
        .groupBy("l_returnflag", "l_linestatus") \
        .agg(F.sum("l_quantity").alias("sum_qty"))
    # Validate results match SQL version
```

---

## 7. Configuration

### 7.1 System Properties

| Property | Description | Default |
|----------|-------------|---------|
| `spark.connect.url` | Spark Connect server URL | `sc://localhost:15002` |
| `spark.master` | Spark master URL for embedded mode | `local[*]` |
| `differential.verbose` | Enable verbose output | `false` |
| `differential.save.divergences` | Save divergences to file | `true` |
| `differential.max.rows` | Max rows to compare | `10000` |
| `differential.epsilon` | Float comparison tolerance | `1e-10` |

### 7.2 Environment Variables

```bash
# Spark configuration
export SPARK_HOME=/opt/spark
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g

# Test configuration
export SPARK_CONNECT_URL=sc://localhost:15002
export DIFFERENTIAL_TEST_DATA=/path/to/test/data
export VERBOSE=true
export MAX_COMPARE_ROWS=10000

# TPC data paths
export TPCH_DATA_PATH=./data/tpch_sf001
export TPCDS_DATA_PATH=./data/tpcds_sf1
```

### 7.3 Maven Configuration

```bash
# Float comparison tolerance
-Ddifferential.epsilon=1e-10

# Save divergences to file
-Ddifferential.save.divergences=true

# Test timeout in seconds
-Dmaven.surefire.timeout=600

# Spark SQL timeout
-Dspark.sql.broadcastTimeout=600
```

---

## 8. Analyzing Results

### 8.1 Success Output

```
✓ Query passed: SELECT * FROM employees
✓ Query passed: SELECT COUNT(*) FROM employees GROUP BY department
===================================================================
✓ All tests passed!
===================================================================
Tests run: 25, Failures: 0, Errors: 0, Skipped: 0
```

### 8.2 Failure Output

```
✗ Query failed: SELECT AVG(salary) FROM employees
  Row count mismatch: Expected 1, Got 0

Divergence Report: target/differential-reports/test_avg_divergence.json
===================================================================
✗ Some tests failed
===================================================================
Tests run: 25, Failures: 3, Errors: 0, Skipped: 0
```

### 8.3 Divergence Reports

Failed tests generate detailed reports in `target/differential-reports/`:

```
target/differential-reports/
├── SparkDataFrameDifferentialTest/
│   ├── test_groupby_divergence.json
│   ├── test_groupby_expected.csv
│   └── test_groupby_actual.csv
└── summary.html
```

### 8.4 View Divergences

```bash
# Generate HTML report
mvn differential:report

# Open in browser
open target/differential-reports/summary.html
```

### 8.5 Common Divergence Types

| Type | Description | Common Cause |
|------|-------------|--------------|
| **Schema Mismatch** | Column names or types differ | Type mapping issues |
| **Row Count Mismatch** | Different number of rows | Query logic error |
| **Value Mismatch** | Different values in cells | Function implementation |
| **Order Mismatch** | Rows in different order | Non-deterministic queries |
| **Precision Mismatch** | Float/double precision differences | Rounding differences |

---

## 9. Troubleshooting

### 9.1 Spark Connect Server Won't Start

```bash
# Check if port is in use
lsof -i :15002

# Check Spark logs
tail -f /tmp/spark-connect-logs/*.log

# Verify Spark installation
$SPARK_HOME/bin/spark-submit --version
```

### 9.2 Connection Refused

```bash
# Verify server is running
ps aux | grep SparkConnectServer

# Test with spark-shell
$SPARK_HOME/bin/spark-shell --remote sc://localhost:15002
```

### 9.3 Out of Memory

```bash
# Increase memory for Maven
export MAVEN_OPTS="-Xmx8g -XX:MaxPermSize=2g"

# Increase memory for Spark
export SPARK_DRIVER_MEMORY=8g
./stop-spark-connect.sh
./start-spark-connect.sh
```

### 9.4 Test Timeout

```bash
# Increase timeout
mvn test -pl tests \
  -Dtest="*Differential*" \
  -Dmaven.surefire.timeout=600 \
  -Dspark.sql.broadcastTimeout=600
```

### 9.5 Manual Testing with PySpark

```python
from pyspark.sql import SparkSession

# Connect to Spark
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Run query
df = spark.sql("SELECT * FROM range(10)")
df.show()
```

---

## 10. CI/CD Integration

### 10.1 GitHub Actions Example

```yaml
name: Differential Tests

on: [push, pull_request]

jobs:
  differential-test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up JDK 11
      uses: actions/setup-java@v2
      with:
        java-version: '11'

    - name: Install Spark
      run: |
        wget -q https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3-scala2.12.tgz
        tar -xzf spark-3.5.3-bin-hadoop3-scala2.12.tgz
        echo "SPARK_HOME=$PWD/spark-3.5.3-bin-hadoop3-scala2.12" >> $GITHUB_ENV

    - name: Start Spark Connect
      run: ./start-spark-connect.sh

    - name: Run Differential Tests
      run: |
        mvn test -pl tests \
          -Dtest="*Differential*" \
          -Dspark.connect.url=sc://localhost:15002

    - name: Upload Reports
      if: failure()
      uses: actions/upload-artifact@v2
      with:
        name: differential-reports
        path: target/differential-reports/
```

### 10.2 Simple CI Pipeline

```yaml
- name: Run Differential Tests
  run: |
    ./tests/scripts/start-spark-connect.sh
    ./tests/scripts/run-differential-tests.sh
    ./tests/scripts/stop-spark-connect.sh
```

---

## 11. Writing New Tests

### 11.1 Python Test Base Class

```python
from thunderduck_e2e.test_runner import ThunderduckE2ETestBase
import pandas as pd

class ThunderduckE2ETestBase(unittest.TestCase):
    """Base class for all end-to-end tests."""

    @classmethod
    def setUpClass(cls):
        """Start Thunderduck server and create PySpark session."""
        cls.server_process = cls._start_thunderduck_server()
        cls.spark = SparkSession.builder \
            .appName("thunderduck-e2e-tests") \
            .remote("sc://localhost:50051") \
            .getOrCreate()
        cls._setup_test_data()

    @classmethod
    def tearDownClass(cls):
        """Stop PySpark session and Thunderduck server."""
        cls.spark.stop()
        cls._stop_thunderduck_server(cls.server_process)

    def assert_dataframes_equal(self, df1, df2, check_order=True):
        """Assert two DataFrames are equal."""
        pdf1 = df1.toPandas()
        pdf2 = df2.toPandas()

        if not check_order:
            pdf1 = pdf1.sort_values(by=list(pdf1.columns)).reset_index(drop=True)
            pdf2 = pdf2.sort_values(by=list(pdf2.columns)).reset_index(drop=True)

        pd.testing.assert_frame_equal(pdf1, pdf2, check_dtype=False)
```

### 11.2 Example DataFrame Test

```python
class TestDataFrameOperations(ThunderduckE2ETestBase):
    """Test DataFrame operations through Spark Connect."""

    def test_select_columns(self):
        """Test column selection."""
        df = self.spark.table("employees").select("name", "salary")
        result = df.collect()
        self.assertEqual(len(result), 4)
        self.assertEqual(len(result[0]), 2)

    def test_filter_operations(self):
        """Test filtering."""
        df = self.spark.table("employees").filter("salary > 60000")
        result = df.collect()
        self.assertEqual(len(result), 3)

    def test_groupby_aggregation(self):
        """Test group by with aggregation."""
        df = self.spark.table("employees") \
            .groupBy("department") \
            .agg({"salary": "avg", "*": "count"})
        result = df.collect()
        self.assertGreater(len(result), 0)
```

### 11.3 Example SQL Test

```python
class TestSQLFunctionality(ThunderduckE2ETestBase):
    """Test SQL execution through Spark Connect."""

    def test_complex_query(self):
        """Test complex SQL with CTEs."""
        df = self.spark.sql("""
            WITH dept_stats AS (
                SELECT department, AVG(salary) as avg_salary
                FROM employees
                GROUP BY department
            )
            SELECT e.name, e.salary, d.avg_salary
            FROM employees e
            JOIN dept_stats d ON e.department = d.department
        """)
        result = df.collect()
        self.assertEqual(len(result), 4)
```

---

## Success Criteria

A complete differential testing implementation should achieve:

- All TPC-H queries (22) execute correctly via PySpark in **both SQL and DataFrame forms**
- All TPC-DS queries (103) execute correctly via PySpark in **both SQL and DataFrame forms**
- SQL and DataFrame versions produce **identical results** for each query
- DataFrame operations match Spark behavior **exactly** (types, not just values)
- Error handling and edge cases are properly managed
- Tests run automatically with `mvn verify`
- Test results are comparable with existing Java DataFrame implementations

---

## References

- [Spark Connect Overview](https://spark.apache.org/docs/3.5.3/spark-connect-overview.html)
- [PySpark Documentation](https://spark.apache.org/docs/3.5.3/api/python/)
- [TPC-H Specification](http://www.tpc.org/tpch/)
- [TPC-DS Specification](http://www.tpc.org/tpcds/)

---

**Document Version:** 1.0
**Last Updated:** 2025-12-09
**Consolidated From:** DIFFERENTIAL_TESTING_GUIDE.md, DIFFERENTIAL_TESTING_README.md, END_TO_END_TEST_PLAN.md
