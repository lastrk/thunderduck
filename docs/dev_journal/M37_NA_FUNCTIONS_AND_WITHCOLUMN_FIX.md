# M37: NA Functions and WithColumn Replace Fix

**Date:** 2025-12-16
**Focus:** Enable NA functions with schema inference, fix withColumn replace operation

---

## Summary

This milestone addresses two related issues:
1. NA functions (`na.drop()`, `na.fill()`, `na.replace()`) failing due to missing schema inference
2. `withColumn()` column replacement producing duplicate columns instead of replacing

---

## Problem 1: NA Functions Failing

### Symptoms
```
NADrop with empty cols requires schema inference, but no connection available
```

Tests affected:
- `test_na_drop_any` - XFAIL
- `test_na_fill_value` - XFAIL
- `test_na_replace` - XFAIL

### Root Cause
`SparkConnectServiceImpl.createPlanConverter()` was creating `PlanConverter` without passing a database connection, which meant `SchemaInferrer` was null:

```java
// OLD CODE - no connection passed
private PlanConverter createPlanConverter() {
    return new PlanConverter();  // SchemaInferrer is null!
}
```

When NA functions need to infer column types (e.g., `na.drop("any")` without specifying columns), they require schema inference which needs a database connection.

### Fix
Added a session-aware `createPlanConverter(Session)` method that passes the DuckDB connection:

```java
/**
 * Creates a PlanConverter with schema inference capability using the session's DuckDB connection.
 * This enables NA functions (dropna, fillna, replace) that need to infer column types.
 *
 * @param session the session providing the DuckDB connection
 * @return a PlanConverter instance with schema inference capability
 */
private PlanConverter createPlanConverter(Session session) {
    return new PlanConverter(session.getRuntime().getConnection());
}
```

Updated 6 call sites to use the session-aware method:
- `ShowString` input relation conversion (line 141)
- Main plan execution (line 208)
- Temp view creation (line 282)
- Schema analysis (line 474)
- Write operation (line 1557)
- SaveAsTable operation (line 1640)

---

## Problem 2: WithColumn Replace Producing Duplicates

### Symptoms
`df.withColumn("existing_col", expr)` was adding a new column instead of replacing the existing one, resulting in duplicate column names.

### Root Cause
The original `WithColumns` implementation used `SELECT *, expr AS col` which always adds columns:

```sql
-- OLD: Always adds new column, even if col exists
SELECT *, (n_nationkey + 100) AS n_nationkey FROM ...
-- Result: n_nationkey, ..., n_nationkey (duplicate!)
```

### Fix
Changed to use DuckDB's `COLUMNS()` lambda syntax to exclude columns being replaced:

```sql
-- NEW: Filter out existing columns, then add new values
SELECT COLUMNS(c -> c NOT IN ('n_nationkey')), (n_nationkey + 100) AS n_nationkey FROM ...
-- Result: ..., n_nationkey (replaced, no duplicate)
```

Key code change in `RelationConverter.convertWithColumns()`:

```java
// Build the COLUMNS filter to exclude columns being replaced
StringBuilder excludeFilter = new StringBuilder();
excludeFilter.append("COLUMNS(c -> c NOT IN (");
for (int i = 0; i < colNames.size(); i++) {
    if (i > 0) excludeFilter.append(", ");
    excludeFilter.append("'").append(colNames.get(i).replace("'", "''")).append("'");
}
excludeFilter.append("))");

// Build: SELECT COLUMNS(c -> c NOT IN ('col1', 'col2')), expr1 AS col1, expr2 AS col2 FROM (...)
String sql = String.format("SELECT %s, %s FROM (%s) AS _withcol_subquery",
    excludeFilter.toString(), newColExprs.toString(), inputSql);
```

This approach:
- Works for both add (new column, no match in filter) and replace (existing column filtered out)
- Uses DuckDB-native lambda syntax for efficiency
- Properly handles column name escaping

---

## Files Modified

1. **`connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`**
   - Added `createPlanConverter(Session session)` method
   - Updated 6 call sites to use session-aware method

2. **`connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java`**
   - Rewrote `convertWithColumns()` to use `COLUMNS()` lambda syntax
   - Properly handles column replacement vs addition

3. **`tests/integration/test_dataframe_operations.py`**
   - Removed `@pytest.mark.xfail` from `test_na_drop_any`
   - Removed `@pytest.mark.xfail` from `test_na_fill_value`
   - Removed `@pytest.mark.xfail` from `test_na_replace`
   - Removed `@pytest.mark.xfail` from `test_with_column_replace`
   - Removed `@pytest.mark.xfail` from `test_complex_chain`
   - Updated xfail reason for `test_na_drop_all` (separate union issue)

---

## Test Results

### Before
```
test_na_drop_any XFAIL
test_na_fill_value XFAIL
test_na_replace XFAIL
test_with_column_replace XFAIL
test_complex_chain XFAIL
```

### After
```
test_na_drop_any PASSED
test_na_drop_all XFAIL (separate union issue)
test_na_drop_subset PASSED
test_na_fill_value PASSED
test_na_replace PASSED
test_with_column_replace PASSED
test_complex_chain PASSED
```

**Summary**: 5 previously failing tests now pass. 1 remaining xfail is due to a separate `union()` operation issue with `createDataFrame`.

---

## Lessons Learned

1. **Connection lifecycle matters**: NA functions that need to infer schema require database connections. Passing connections through session objects is cleaner than borrowing from pools.

2. **DuckDB lambda syntax is powerful**: `COLUMNS(c -> c NOT IN (...))` provides a clean way to filter columns dynamically without knowing the full schema ahead of time.

3. **Test xfail markers should be specific**: Updated the `test_na_drop_all` xfail reason to clarify it's a union issue, not an NA function issue.

---

## Related Issues

- The `na.fill(0)` on mixed-type columns still has issues due to DuckDB type checking (can't fill string column with integer). This is a type compatibility issue, not a schema inference issue.
- The `test_na_drop_all` test uses `union()` with `createDataFrame` which has a separate issue unrelated to NA functions.
