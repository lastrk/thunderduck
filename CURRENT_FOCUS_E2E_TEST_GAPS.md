# E2E Testing Gap Analysis for Thunderduck

**Version:** 1.1
**Date:** 2025-12-13
**Purpose:** Catalog E2E test failures and gaps for Thunderduck Spark Connect

---

## Executive Summary

The E2E test suite (`/workspace/tests/integration/`) has ~291 tests. After the **P0 fix for `createOrReplaceTempView`** (commit `6653a3d`), most infrastructure issues are resolved.

| Test Category | Tests | Status |
|--------------|-------|--------|
| Simple SQL | 3 | PASS |
| TPC-H Queries | 15 | 10 PASS / 5 data issues |
| TPC-H DataFrame | 7 | 2 PASS (window functions) |
| Basic DataFrame Ops | 7 | ALL PASS |
| Temp Views | 6 | ALL PASS (manual test) |

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
  ✓ lineitem: 8 rows
  ✓ orders: 8 rows
  ✓ customer: 5 rows
  ✓ part: 2,000 rows
  ✓ supplier: 100 rows
  ✓ partsupp: 8,000 rows
  ✓ nation: 25 rows
  ✓ region: 5 rows
✓ All 8 TPC-H tables registered as temp views
```

---

## Current Test Status

### Passing Tests
| File | Tests | Status | Notes |
|------|-------|--------|-------|
| `test_simple_sql.py` | 3 | PASS | Direct SQL |
| `test_tpch_queries.py` | 15 | 10 PASS | Basic DataFrame ops ALL pass |
| `test_tpch_dataframe_poc.py` | 7 | 2 PASS | Window functions pass |
| Manual temp view test | 6 | ALL PASS | create, query, replace, multiple |

### Remaining Failures (Data/Query Issues - NOT Infrastructure)
| Test | Issue | Root Cause |
|------|-------|------------|
| Q1 pricing_summary | 3 groups instead of 4 | Small test dataset (SF0.01) |
| Q6 revenue_forecast | NULL result | Data filtering produces no matches |
| Some complex expressions | INVALID_ARGUMENT | Expression conversion gaps |

---

## Priority Fix List (Updated)

| Priority | Issue | Impact | Status |
|----------|-------|--------|--------|
| **P0** | ~~Fix createOrReplaceTempView~~ | ~~Unblocks ALL TPC-H/TPC-DS tests~~ | **FIXED** |
| **P1** | Fix Q1/Q6 data issues | Accurate TPC-H validation | Open |
| **P2** | Add `DropTempView` catalog operation | Proper view cleanup | Open |
| **P3** | Add E2E tests for M19-M28 operations | Coverage for new features | Open |

---

## Missing E2E Test Coverage

Operations implemented (M19-M28) but NOT covered by E2E tests:

| Feature | Milestone | PySpark API | E2E Test? |
|---------|-----------|-------------|-----------|
| Drop columns | M19 | `df.drop("col")` | NO |
| WithColumn | M19 | `df.withColumn("new", expr)` | NO |
| WithColumnRenamed | M19 | `df.withColumnRenamed("old", "new")` | NO |
| Offset | M20 | `df.offset(n)` | NO |
| ToDF | M20 | `df.toDF("a", "b")` | NO |
| Tail | M21 | `df.tail(n)` | NO |
| ShowString | M22 | `df.show()` | NO |
| Sample | M23 | `df.sample(0.1)` | NO |
| WriteOperation | M24 | `df.write.parquet()` | NO |
| Hint | M25 | `df.hint("BROADCAST")` | NO |
| Repartition | M25 | `df.repartition(n)` | NO |
| NADrop | M26 | `df.na.drop()` | NO |
| NAFill | M26 | `df.na.fill()` | NO |
| NAReplace | M26 | `df.na.replace()` | NO |
| Unpivot | M27 | `df.unpivot()` | NO |
| SubqueryAlias | M28 | `df.alias("t")` | NO |

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

1. **Retest all TPC-H/TPC-DS tests** with rebuilt server to confirm fix propagation
2. **Investigate Q1/Q6 failures** - likely test data or expression issues, not infrastructure
3. **Add E2E tests for M19-M28** - create new test file for recent features
4. **Update conftest.py comment** - line 167-170 comment is now stale

---

## References

- Fix Details: `/workspace/docs/dev_journal/M29_TEMP_VIEW_FIX.md`
- Gap Analysis: `/workspace/docs/SPARK_CONNECT_GAP_ANALYSIS.md`
- Server Implementation: `/workspace/connect-server/src/main/java/com/thunderduck/connect/`

---

**Document Version:** 1.1
**Last Updated:** 2025-12-13 (Updated after P0 fix)
