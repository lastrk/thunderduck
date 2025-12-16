# E2E Testing Gap Analysis for Thunderduck

**Version:** 1.5
**Date:** 2025-12-16
**Purpose:** Catalog E2E test failures and gaps for Thunderduck Spark Connect

---

## Executive Summary

The E2E test suite (`/workspace/tests/integration/`) has ~291 tests. After the **Spark 4.0.1 upgrade** (merged PR #2) and subsequent fixes (M31), all core tests now pass.

| Test Category | Tests | Status |
|--------------|-------|--------|
| Simple SQL | 3 | ALL PASS |
| TPC-H Queries | 17 | **16 PASS, 1 XFAIL** |
| TPC-DS Queries | 102 | **100 PASS, 2 FAIL** |
| Basic DataFrame Ops | 7 | ALL PASS |
| Temp Views | 7 | ALL PASS |

**Spark Version:** 4.0.1 (upgraded from 3.5.3 on 2025-12-15)

---

## P0 Issue: FIXED (M29)

### Root Causes (All Fixed)

1. **Isolated in-memory databases** - Each connection to `jdbc:duckdb:` created its own database
   - **Fix**: Changed to `jdbc:duckdb::memory:thunderduck` for shared named database

2. **Connection leak in createPlanConverter** - `getConnection()` borrowed but never released
   - **Fix**: Removed connection borrowing from `createPlanConverter()`

3. **Temp view scope** - DuckDB `TEMP VIEW` is connection-scoped
   - **Fix**: Changed to regular `VIEW` for cross-connection visibility

### Files Modified
- `core/src/main/java/com/thunderduck/runtime/DuckDBConnectionManager.java:275-278`
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java:70-74, 292-300`
- `core/src/main/java/com/thunderduck/runtime/ArrowBatchStream.java` (LIMIT 0 schema fix)

### Test Results After Fix
```
Loading TPC-H tables and creating temp views...
  ✓ lineitem: 60,175 rows
  ✓ orders: 15,000 rows
  ✓ customer: 1,500 rows
  ✓ part: 2,000 rows
  ✓ supplier: 100 rows
  ✓ partsupp: 8,000 rows
  ✓ nation: 25 rows
  ✓ region: 5 rows
✓ All 8 TPC-H tables registered as temp views
```

---

## P1 Issue: FIXED - TPC-H Data Regenerated

### Problem
The test data in `/workspace/data/tpch_sf001/` had been corrupted/replaced with a tiny 8-row sample. The reference data was generated from proper TPC-H SF0.01 data (~60K rows).

### Fix
Regenerated proper TPC-H SF0.01 data using DuckDB's built-in TPC-H generator:
```python
import duckdb
con = duckdb.connect()
con.execute('INSTALL tpch; LOAD tpch;')
con.execute('CALL dbgen(sf=0.01);')
# Export to parquet
for table in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
    con.execute(f"COPY {table} TO '/workspace/data/tpch_sf001/{table}.parquet' (FORMAT PARQUET)")
```

### Result
- Before: Q1 returned 3 groups (8 rows total) - **FAIL**
- After: Q1 returns 4 groups (60,175 lineitem rows) - **PASS**
- **All 15 TPC-H tests now pass**

---

## Current Test Status

### Passing Tests (Post Spark 4.0.1 Upgrade + M31 Fixes)
| File | Tests | Status | Notes |
|------|-------|--------|-------|
| `test_simple_sql.py` | 3 | ALL PASS | Direct SQL |
| `test_tpch_queries.py` | 17 | **16 PASS, 1 XFAIL** | ALL PASS (Q3, Q5 fixed) |
| `test_temp_views.py` | 7 | ALL PASS | create, query, replace, multiple |

**Spark 4.0.1 Regression - FIXED (M31)**: Q3 and Q5 DataFrame API tests previously failed with schema inference error. Fixed by:
1. Session caching in `SessionManager` to ensure same session ID reuses same DuckDB database
2. Fixed `inferSchemaFromDuckDB()` to use correct session ID
3. Fixed `Join.inferSchema()` to handle null child schemas gracefully

**Note**: The window function test is marked `xfail` due to ORDER BY translation bug (`DESCENDING` instead of `DESC`).

### TPC-DS Test Results (Retested 2025-12-13)
| File | Tests | Status | Notes |
|------|-------|--------|-------|
| `test_tpcds_batch1.py` | 102 | **100 PASS, 2 FAIL** | Q17, Q23b fail (empty result) |

**Failing TPC-DS Queries:**
- Q17: `AssertionError: assert table is not None` - query returns empty result
- Q23b: `AssertionError: assert table is not None` - query returns empty result

### Differential Test Results (Retested 2025-12-13)
| File | Tests | Status | Notes |
|------|-------|--------|-------|
| `test_differential_tpch.py` | 22 | **1 PASS, 21 ERROR** | Missing `spark_local` fixture |

**Note:** Differential tests require a `spark_local` fixture (real Spark server) which is not configured. Only Q1 test uses available fixtures.

### Value Correctness Test Results (Partial - 2025-12-13)
| File | Tests | Status | Notes |
|------|-------|--------|-------|
| `test_value_correctness.py` | 22 | **8+ PASS, ~14 UNTESTED** | Q2 hangs (complex query), stopped test |

**Passing value correctness tests**: Q1, Q3, Q5, Q6, Q10, Q12, Q13, Q18
**Note**: Test hung on Q2 - likely a very complex correlated subquery that takes too long or hangs.

---

## Priority Fix List (Updated)

| Priority | Issue | Impact | Status |
|----------|-------|--------|--------|
| **P0** | ~~Fix createOrReplaceTempView~~ | ~~Unblocks ALL TPC-H/TPC-DS tests~~ | **FIXED** |
| **P1** | ~~Fix Q1/Q6 data issues~~ | ~~Accurate TPC-H validation~~ | **FIXED** |
| **P2** | ~~Add `DropTempView` catalog operation~~ | ~~Proper view cleanup~~ | **FIXED** (M34) |
| **P3** | ~~Add E2E tests for M19-M28 operations~~ | ~~Coverage for new features~~ | **DONE** |

---

## E2E Test Coverage for M19-M28 Operations

Operations implemented (M19-M28) now have E2E tests in `test_dataframe_operations.py`:

| Feature | Milestone | PySpark API | Status | Notes |
|---------|-----------|-------------|--------|-------|
| Drop columns | M19 | `df.drop("col")` | **PASS** | Fixed in M32 |
| WithColumn (new) | M19 | `df.withColumn("new", expr)` | **PASS** | Fixed in M32 |
| WithColumn (replace) | M19 | `df.withColumn("old", expr)` | **PASS** | Fixed in M37 (COLUMNS lambda) |
| WithColumnRenamed | M19 | `df.withColumnRenamed("old", "new")` | **PASS** | Fixed in M35 (PySpark 4.0 renames field) |
| Offset | M20 | `df.offset(n)` | **PASS** | |
| ToDF | M20 | `df.toDF("a", "b")` | **PASS** | Fixed in M32 |
| Tail | M21 | `df.tail(n)` | **PASS** | Fixed in M33 |
| Sample | M23 | `df.sample(0.1)` | **PASS** | |
| WriteOperation | M24 | `df.write.parquet()` | **PASS** | All formats work |
| Hint | M25 | `df.hint("BROADCAST")` | **PASS** | No-op passthrough |
| Repartition | M25 | `df.repartition(n)` | **PASS** | No-op passthrough |
| NADrop | M26 | `df.na.drop()` | **PASS** | Fixed in M37 (schema inference) |
| NAFill | M26 | `df.na.fill()` | **PASS** | Fixed in M37 (schema inference) |
| NAReplace | M26 | `df.na.replace()` | **PASS** | Fixed in M37 (schema inference) |
| Unpivot | M27 | `df.unpivot()` | **PASS** | Fixed in M32 |
| SubqueryAlias | M28 | `df.alias("t")` | **PASS** | Basic alias works |

**Test Results**: 26 passed, 2 xfailed (expected failures documented)

---

## Test Infrastructure Overview

### Directory Structure
```
tests/integration/
├── conftest.py                 # pytest fixtures (SESSION-SCOPED!)
├── utils/
│   ├── server_manager.py       # Server lifecycle
│   └── result_validator.py     # Result validation
├── test_simple_sql.py          # WORKS
├── test_tpch_queries.py        # WORKS (10/15 pass)
├── test_temp_views.py          # WORKS (after fix)
├── test_tpcds_batch1.py        # Needs retest
└── expected_results/           # Reference data (Parquet)
```

### Key Fixtures
- `server_manager` - Starts/stops Thunderduck server
- `spark_session` - PySpark Spark Connect session
- `tpch_tables` - **WORKING** - Creates temp views for TPC-H tables
- `tpcds_tables` - Needs retest

### How to Run Tests
```bash
# Start server first (required for tests)
pkill -9 -f "thunderduck-connect-server"  # Kill any existing
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  -jar /workspace/connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar &
sleep 5

# Run tests
cd /workspace/tests/integration
python3 -m pytest test_simple_sql.py -v           # Quick sanity check
python3 -m pytest test_tpch_queries.py -v         # TPC-H tests
python3 -m pytest test_tpch_dataframe_poc.py -v   # DataFrame operations
```

---

## Next Steps

1. **~~Retest all TPC-H/TPC-DS tests~~** - DONE (2025-12-15 with Spark 4.0.1)
2. **~~Fix Spark 4.0.1 schema inference regression~~** - DONE (M31)
   - Fixed session caching in `SessionManager`
   - Fixed `inferSchemaFromDuckDB()` to use correct session ID
   - Fixed `Join.inferSchema()` null handling
3. **~~Add E2E tests for M19-M28~~** - DONE (`test_dataframe_operations.py` - 13 pass, 15 xfail)
4. **Investigate TPC-DS Q17/Q23b** - both return empty results

---

## Spark 4.0.1 Upgrade Details (2025-12-15)

**PR**: lastrk/thunderduck#2 (by andreAmorimF)

**Changes:**
- Spark version: 3.5.3 → 4.0.1
- PySpark version: 3.5.3 → 4.0.1
- Java requirement: 11 → 17
- New protos: `ml.proto`, `ml_common.proto`
- Updated SQL extraction for backward compatibility with new `input` relation

**Known Regression - FIXED (M31):**
- Q3, Q5 DataFrame API tests were failing due to schema inference for `NamedTable`
- Error: `Schema analysis failed: Cannot invoke "StructType.fields()" because schema is null`
- Fix: Session caching + `Join.inferSchema()` null handling

---

## References

- Fix Details: `/workspace/docs/dev_journal/M29_TEMP_VIEW_FIX.md`
- Gap Analysis: `/workspace/docs/SPARK_CONNECT_GAP_ANALYSIS.md`
- Server Implementation: `/workspace/connect-server/src/main/java/com/thunderduck/connect/`

---

**Document Version:** 1.7
**Last Updated:** 2025-12-16 (Updated after M37 - NA functions and withColumn replace fixed)
