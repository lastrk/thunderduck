# M86: Spark 4.1.1 Upgrade and macOS Timezone Fix

**Date**: 2026-02-17

## Summary

Upgraded all project references from Spark 4.0.1 to 4.1.1 (POM already had 4.1.1, but scripts/docs/tests still referenced 4.0.1). Fixed a macOS-specific timezone bug where 6 datetime differential tests failed due to DuckDB using UTC for TIMESTAMP literals while Spark operates in the session timezone.

## Test Results

| Mode | Passed | Failed | Skipped | Time |
|------|--------|--------|---------|------|
| Relaxed | 671 | 0 | 162 | 57.83s |
| Strict | 673 | 0 | 160 | 58.12s |

## Changes

### 1. Spark 4.0.1 → 4.1.1 Reference Update (~48 files)

Updated all remaining 4.0.1 references across:
- **Scripts**: Renamed `start-spark-4.0.1-reference.sh` → `start-spark-4.1.1-reference.sh` and `stop-spark-4.0.1-reference.sh` → `stop-spark-4.1.1-reference.sh`
- **Source code**: `connect-server/pom.xml`, `SparkConnectServiceImpl.java`, `DDLStatementTest.java`
- **Test infrastructure**: `conftest.py`, `dual_server_manager.py`, `test_orchestrator.py`
- **30 differential test docstrings**
- **Docs**: `TPC_H_BENCHMARK.md`, `DIFFERENTIAL_TESTING_ARCHITECTURE.md`, `CATALOG_OPERATIONS.md`, `SQL_PARSER_RESEARCH.md`
- **Benchmarks**: `remote_setup.sh`, `run_benchmark.py`, `README.md`, `cdk/app.py`
- **Playground**: `launch.sh`

Excluded: `docs/dev_journal/` (historical), `benchmarks/cdk/cdk.out/` (generated), DuckDB ICU extension (Unicode 4.0.1, not Spark).

### 2. macOS Timezone Fix (SQLGenerator.java + DuckDBRuntime.java)

**Root cause**: PySpark converts `datetime` objects to UTC before sending via Arrow (`TimeStampMicroTZVector`). DuckDB stored these as plain `TIMESTAMP` (timezone-unaware), so functions like `hour()`, `dayofweek()`, and `date_trunc()` operated on UTC values. On Linux (UTC timezone) this matched Spark. On macOS (e.g., CST/UTC-6), Spark returns local-time results while DuckDB returned UTC-based results.

**Fix**: Two changes working together:

1. **`SQLGenerator.formatSQLValue()`**: Changed Instant formatting from `TIMESTAMP '...'` to `TIMESTAMPTZ '... UTC'`, preserving UTC semantics while enabling timezone-aware function behavior.

2. **`DuckDBRuntime.configureConnection()`**: Added `SET TimeZone='<system timezone>'` so DuckDB's TIMESTAMPTZ functions respect the local timezone, matching Spark's session timezone behavior.

This ensures:
- Extraction functions (`hour`, `dayofweek`) return local-time values (matching Spark)
- Epoch functions (`unix_timestamp`) correctly return UTC seconds
- Formatting functions (`date_format`) produce local-time strings
- Truncation functions (`date_trunc`) truncate in local time
