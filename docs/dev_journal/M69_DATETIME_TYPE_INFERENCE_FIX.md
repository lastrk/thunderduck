# M69: Fix Datetime Extraction Function Type Inference

**Date:** 2025-12-22
**Status:** Complete

## Summary

Fixed 10 failing datetime differential tests caused by type mismatches between Spark and DuckDB for date extraction functions, datediff, and unix_timestamp.

## Problems Identified

### 1. Date Extraction Functions (8 tests)
- **Issue**: DuckDB's `YEAR()`, `MONTH()`, `DAY()`, etc. return `BIGINT`, but Spark returns `INTEGER`
- **Tests affected**: test_year, test_month, test_day, test_hour, test_minute, test_second, test_dayofweek, test_dayofyear_quarter_weekofyear

### 2. DATEDIFF Function (1 test)
- **Issue**: DuckDB's `datediff('day', A, B)` returns `B - A` (second minus first), but the translation assumed `A - B`
- **Result**: Sign inversion (Spark: 5, Thunderduck: -5)

### 3. UNIX_TIMESTAMP Function (1 test)
- **Issue**: DuckDB's `epoch()` returns `DOUBLE`, but Spark returns `LONG`

## Solutions

### Fix 1: Extended `mergeSchemas()` for Integer/Long Types

**File:** `SparkConnectServiceImpl.java`

Extended the M68 type merging strategy to handle additional type mismatches:

```java
// Type merging strategy:
// 1. DecimalType: prefer logical plan's type (computed using Spark rules)
// 2. IntegerType vs LongType: prefer logical (date extraction functions return INT in Spark)
// 3. LongType vs DoubleType: prefer logical (unix_timestamp returns LONG in Spark)
// 4. Other types: use DuckDB's type (handles aggregates, etc.)
if (logicalType instanceof IntegerType && duckType instanceof LongType) {
    finalType = logicalType;
} else if (logicalType instanceof LongType && duckType instanceof DoubleType) {
    finalType = logicalType;
}
```

### Fix 2: Corrected DATEDIFF Argument Order

**File:** `FunctionRegistry.java`

DuckDB's `datediff('day', A, B)` returns `B - A`, not `A - B`:

```java
// Spark datediff(end, start) returns (end - start) days
// DuckDB datediff('day', A, B) returns (B - A) days (second minus first!)
// So we swap args: datediff('day', start, end) = end - start
CUSTOM_TRANSLATORS.put("datediff", args ->
    "datediff('day', " + args[1] + ", " + args[0] + ")");
```

Also fixed `months_between` for consistency.

### Fix 3: Cast UNIX_TIMESTAMP to BIGINT

**File:** `FunctionRegistry.java`

```java
// DuckDB: epoch(timestamp) returns DOUBLE, but Spark returns LONG
// Cast to BIGINT to match Spark's return type
return "CAST(epoch(" + args[0] + ") AS BIGINT)";
```

## Files Modified

1. `/workspace/connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`
   - `mergeSchemas()`: Added IntegerType and LongType preference rules

2. `/workspace/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`
   - `datediff`: Swapped argument order to match DuckDB semantics
   - `months_between`: Swapped argument order for consistency
   - `unix_timestamp`: Added CAST to BIGINT

## Test Results

- **Datetime tests**: 18 passed, 2 skipped
  - Skipped: `months_between` (complex fractional calculation), `next_day` (no DuckDB equivalent)
- **DataFrame ops tests**: 25 passed (no regressions)

## Key Insights

1. **DuckDB datediff semantics**: `datediff('day', A, B)` returns `B - A`, which is the opposite of what one might assume
2. **Type merging strategy**: The `mergeSchemas()` pattern of preferring logical plan types for specific type pairs is effective for handling Spark/DuckDB type differences
3. **Always verify assumptions**: Testing DuckDB semantics directly (`SELECT datediff(...)`) was crucial for debugging

## Lessons Learned

- When translating functions between SQL dialects, always verify the exact semantics with direct testing
- DuckDB's date functions may have subtle differences from other SQL dialects
- The type merging strategy in `mergeSchemas()` is a powerful pattern for handling type inference mismatches
