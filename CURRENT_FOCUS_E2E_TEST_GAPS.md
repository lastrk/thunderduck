# E2E Testing Gap Analysis for Thunderduck

**Version:** 2.0
**Date:** 2025-12-16
**Purpose:** Catalog E2E test coverage and gaps for Thunderduck Spark Connect

---

## Executive Summary

The E2E test suite has comprehensive coverage with **153 differential tests** comparing Thunderduck against Apache Spark 4.0.1. **ALL TESTS PASS**.

| Test Category | Tests | Status |
|--------------|-------|--------|
| TPC-H SQL (Differential) | 23 | ✅ ALL PASS |
| TPC-H DataFrame API (Differential) | 4 | ✅ ALL PASS |
| TPC-DS SQL (Differential) | 102 | ✅ ALL PASS |
| TPC-DS DataFrame API (Differential) | 24 | ✅ ALL PASS |
| **Total Differential Tests** | **153** | ✅ **ALL PASS** |

**Spark Version:** 4.0.1
**Test Framework:** Differential testing (Spark 4.0.1 Reference vs Thunderduck)

---

## Differential Testing Framework (M39)

### Overview

The differential testing framework runs queries on both Apache Spark 4.0.1 and Thunderduck, comparing results row-by-row to ensure exact compatibility.

```
┌──────────────────────────────────────────────────────────────┐
│                    Test Suite (Pytest)                        │
│        test_differential_v2.py / test_tpcds_differential.py   │
└───────────────────┬────────────────┬─────────────────────────┘
                    │                │
        ┌───────────▼──────┐  ┌──────▼──────────┐
        │  Spark Reference │  │  Thunderduck    │
        │  4.0.1 (Native)  │  │  Connect Server │
        │  Port: 15003     │  │  Port: 15002    │
        └──────────────────┘  └──────────────────┘
```

### Test Coverage

**TPC-H (23 SQL + 4 DataFrame = 27 tests):**
- Q1-Q22 SQL queries
- Q1, Q3, Q6, Q12 DataFrame API implementations
- Sanity test

**TPC-DS (102 SQL + 24 DataFrame = 126 tests):**
- Q1-Q99 SQL queries (Q36 excluded - DuckDB limitation)
- Variant queries: Q14a/b, Q23a/b, Q24a/b, Q39a/b
- 24 DataFrame API implementations (Q3, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q41, Q42, Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q82, Q84, Q91, Q96, Q98, Q99)

### DataFrame API Operations Tested

**TPC-DS DataFrame Tests (24 queries):**
Q3, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q41, Q42, Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q82, Q84, Q91, Q96, Q98, Q99

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

---

## How to Run Tests

### Quick Start (Recommended)

```bash
# One-time setup (downloads Spark 4.0.1, creates venv)
./tests/scripts/setup-differential-testing.sh

# Run all differential tests
./tests/scripts/run-differential-tests-v2.sh
```

### Run Specific Test Suites

```bash
# Activate venv
source tests/integration/.venv/bin/activate
cd tests/integration

# TPC-H SQL tests (~20 seconds)
python -m pytest test_differential_v2.py -v

# TPC-H DataFrame tests
python -m pytest test_differential_v2.py::TestTPCH_DataFrame_Differential -v

# TPC-DS SQL tests (~5 minutes)
python -m pytest test_tpcds_differential.py -k "Batch" -v

# TPC-DS DataFrame tests
python -m pytest test_tpcds_differential.py::TestTPCDS_DataFrame_Differential -v

# All DataFrame tests
python -m pytest -k "dataframe" -v
```

---

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

## Test Infrastructure

### File Structure
```
tests/
├── scripts/
│   ├── setup-differential-testing.sh     # One-time setup
│   ├── run-differential-tests-v2.sh      # Run all tests
│   ├── start-spark-4.0.1-reference.sh    # Start Spark reference
│   └── stop-spark-4.0.1-reference.sh     # Stop Spark reference
│
├── integration/
│   ├── .venv/                            # Python virtual environment
│   ├── conftest.py                       # Pytest fixtures
│   ├── test_differential_v2.py           # TPC-H differential tests
│   ├── test_tpcds_differential.py        # TPC-DS differential tests
│   └── utils/
│       ├── dual_server_manager.py        # Server orchestration
│       └── dataframe_diff.py             # Row-by-row comparison
```

### Key Fixtures
- `dual_server_manager` - Starts both Spark 4.0.1 and Thunderduck servers
- `spark_reference` - PySpark session to Spark 4.0.1 (port 15003)
- `spark_thunderduck` - PySpark session to Thunderduck (port 15002)
- `tpch_tables_reference` / `tpch_tables_thunderduck` - TPC-H temp views
- `tpcds_tables_reference` / `tpcds_tables_thunderduck` - TPC-DS temp views

---

## Historical Issues (All Fixed)

| Issue | Milestone | Status |
|-------|-----------|--------|
| Temp view isolation | M29 | ✅ FIXED |
| TPC-H data corruption | M29 | ✅ FIXED |
| Spark 4.0.1 schema inference | M31 | ✅ FIXED |
| Window function ORDER BY | M38 | ✅ FIXED |
| NA functions schema inference | M37 | ✅ FIXED |
| withColumn replace existing | M37 | ✅ FIXED |

---

## Remaining Gaps / Future Work

### Infrastructure
| Priority | Item | Status |
|----------|------|--------|
| P1 | CI/CD integration for differential tests | TODO |
| P3 | HTML test report generation | TODO |
| P3 | Performance benchmarking reports | TODO |

### Additional Test Suites (from Research)

Based on research of public Spark test suites, these areas would improve coverage:

| Priority | Test Suite | Operations | Status |
|----------|------------|------------|--------|
| P1 | Complex Data Types | `explode`, `collect_list`, `array_contains`, `flatten`, nested structs, maps | TODO |
| P2 | Window Functions | `rank`, `dense_rank`, `row_number`, `lag`, `lead`, `first`, `last`, frame specs | TODO |
| P2 | Multi-dimensional Aggregations | `pivot`, `unpivot`, `cube`, `rollup` | TODO |
| P3 | Apache Spark DataFrameFunctionsSuite | Official function parity tests | TODO |

**Details:**

1. **Complex Data Types** (P1)
   - Arrays: `explode`, `explode_outer`, `collect_list`, `collect_set`, `array_contains`, `flatten`
   - Structs: Nested field access with dot notation, `withField`, `dropFields`
   - Maps: `map_from_entries`, `explode` on maps, key-value operations
   - Source: [Spark Complex Types](https://docs.databricks.com/en/optimizations/complex-types.html)

2. **Window Functions** (P2)
   - Ranking: `rank()`, `dense_rank()`, `row_number()`, `ntile()`
   - Analytic: `lag()`, `lead()`, `first()`, `last()`
   - Frame specs: `ROWS BETWEEN`, `RANGE BETWEEN`
   - Source: [Spark Window Functions](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)

3. **Multi-dimensional Aggregations** (P2)
   - `pivot()` - Rotate rows to columns with aggregation
   - `unpivot()` - Rotate columns to rows (Spark 3.4+)
   - `cube()` - All combinations of grouping columns
   - `rollup()` - Hierarchical aggregations with subtotals

4. **DataFrameFunctionsSuite** (P3)
   - Source: `github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/DataFrameFunctionsSuite.scala`
   - Comprehensive function parity tests from Apache Spark

**Resources:**
- [spark-fast-tests](https://github.com/mrpowers-io/spark-fast-tests) - Community testing library
- [PySpark Testing Utils](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html) - Built-in Spark 3.5+
- [spark-sql-perf](https://github.com/databricks/spark-sql-perf) - Databricks performance benchmarks

---

## References

- [Differential Testing Architecture](/workspace/docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)
- [Session Management Architecture](/workspace/docs/architect/SESSION_MANAGEMENT_ARCHITECTURE.md)
- [M39 Dev Journal](/workspace/docs/dev_journal/M39_DIFFERENTIAL_TESTING_V2.md)

---

**Document Version:** 2.2
**Last Updated:** 2025-12-16 (153 differential tests, organized future test suite TODOs)
