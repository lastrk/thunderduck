# M51: Test Infrastructure Overhaul and Session Fix

**Date**: 2025-12-20
**Status**: Complete

## Summary

Major overhaul of the E2E differential test infrastructure, including consolidation of tests under the `differential/` directory, implementation of a robust test orchestrator, and fixing a critical session reuse bug that caused differential tests to fail silently.

## Changes Made

### 1. Test Directory Consolidation

Moved standalone test files into the `differential/` directory where all tests now compare Thunderduck against Apache Spark 4.0.1:

| Old Location | New Location |
|-------------|--------------|
| `test_catalog_operations.py` | `differential/test_catalog_operations.py` |
| `test_empty_dataframe.py` | `differential/test_empty_dataframe.py` |
| `test_simple_sql.py` | `differential/test_simple_sql.py` |
| `test_temp_views.py` | `differential/test_temp_views.py` |

### 2. Test Orchestrator Implementation

Created `tests/integration/utils/test_orchestrator.py` - a robust test orchestrator that provides:

- **TimingCollector**: Measures connect, query plan, and collect times for both servers
- **ServerSupervisor**: Manages server processes with health monitoring, log capture, and graceful shutdown
- **TestOrchestrator**: Central coordinator with configurable timeouts and diagnostic collection

Key features:
- Low timeouts by default to detect deadlocks/blocking fast
- Timing measurements for performance analysis
- Diagnostic collection on hard errors
- Server health checks

### 3. Critical Bug Fix: SparkSession Reuse

**Problem**: `SparkSession.builder.remote(url).getOrCreate()` was returning the **same session object** for both Spark Reference and Thunderduck connections. This meant:
- Both `spark_reference` and `spark_thunderduck` fixtures pointed to the same server
- Differential tests were silently comparing Spark to itself, not Thunderduck

**Evidence**: Test output showed both fixtures at the same memory address:
```
spark_reference = <pyspark.sql.connect.session.SparkSession object at 0xffff71035120>
spark_thunderduck = <pyspark.sql.connect.session.SparkSession object at 0xffff71035120>
```

**Fix**: Changed `getOrCreate()` to `create()` in `test_orchestrator.py:558`:
```python
# Before (buggy)
result[0] = SparkSession.builder.remote(remote_url).getOrCreate()

# After (fixed)
result[0] = SparkSession.builder.remote(remote_url).create()
```

This ensures each fixture gets a truly separate PySpark session connected to its designated server.

### 4. Test Fixture Updates

Updated `conftest.py` to use the new orchestrator infrastructure:

- `dual_server_manager` - Session-scoped fixture that starts both servers
- `orchestrator` - Session-scoped orchestrator with timing and health monitoring
- `spark_reference` / `spark_thunderduck` - Class-scoped sessions via orchestrator
- `spark_reference_isolated` / `thunderduck_isolated` - Function-scoped for per-test isolation

### 5. DataFrame Comparison Utility Enhancement

Enhanced `utils/dataframe_diff.py` with more robust comparison:
- Schema comparison with type and nullable checks
- Row-by-row value comparison with configurable tolerance
- Clear diagnostic output on mismatches

### 6. Catalog Operations Test Reduction

Reduced catalog operations tests from 54 to 14:
- Kept only tests that can meaningfully compare between Spark and Thunderduck
- Deleted DuckDB-specific tests (table creation, schema management) that cannot be differentially tested

## Files Modified

| File | Changes |
|------|---------|
| `tests/integration/conftest.py` | Integrated orchestrator, added new fixtures |
| `tests/integration/utils/test_orchestrator.py` | New - orchestrator implementation |
| `tests/integration/utils/exceptions.py` | New - custom exception types |
| `tests/integration/utils/dataframe_diff.py` | Enhanced comparison utilities |
| `tests/integration/differential/test_catalog_operations.py` | New - consolidated catalog tests |
| `tests/integration/differential/test_empty_dataframe.py` | New - consolidated empty DF tests |
| `tests/integration/differential/test_simple_sql.py` | New - consolidated SQL tests |
| `tests/integration/differential/test_temp_views.py` | New - consolidated temp view tests |
| `tests/integration/README.md` | Updated documentation |
| `tests/scripts/run-differential-tests-v2.sh` | Updated test groups |
| `tests/scripts/setup-differential-testing.sh` | Improved setup script |

## Test Counts

| Test Group | Count | Status |
|------------|-------|--------|
| Catalog Operations | 14 | All passing |
| Empty DataFrame | 16 | All passing |
| Simple SQL | 3 | All passing |
| Temp Views | 7 | 2 pass, 5 fail (type mismatches) |

Note: Temp view test failures are due to pre-existing Thunderduck type parity issues (Decimal precision, nullable flags), not infrastructure problems.

## Verification

```bash
# Run catalog tests (all pass)
python3 -m pytest differential/test_catalog_operations.py -v

# Verify session fix
python3 -m pytest differential/test_catalog_operations.py::TestDropTempView::test_drop_existing_temp_view -v
# Result: PASSED (was FAILED before fix)
```

## Key Learnings

1. **PySpark session management**: `getOrCreate()` reuses existing sessions by default, which can cause subtle bugs in test infrastructure where multiple connections are needed.

2. **Test infrastructure validation**: Always verify that differential test fixtures actually connect to different servers - session reuse can cause silent test failures.

3. **Centralized session creation**: Having a single point for session creation (TestOrchestrator) makes it easier to apply fixes consistently.

## Related Issues

- Fixes silent test failure where `test_drop_existing_temp_view` was incorrectly failing
- Enables proper differential testing between Thunderduck and Spark Reference
