# Known Issues

This document tracks known issues discovered during testing that need to be addressed in future work.

**Last Updated**: 2025-12-19

---

## 1. COUNT_DISTINCT Function Not Supported

**Test**: `test_dataframes.py::TestDataFrameOperations::test_distinct_operations`

**Error**:
```
SQL error: Catalog Error: Scalar Function with name count_distinct does not exist!
Did you mean "count_if"?

LINE 1: SELECT COUNT_DISTINCT(department) AS "dept_count" FROM ...
```

**Root Cause**: DuckDB doesn't have a `COUNT_DISTINCT()` function. The standard SQL approach is `COUNT(DISTINCT column)`.

**Affected Code**: Likely in `ExpressionConverter` or aggregate function handling.

**Fix**: Transform `countDistinct(col)` to `COUNT(DISTINCT col)` during SQL generation.

**Priority**: Medium - Common Spark operation

---

## 2. Empty DataFrame Analyze Issue

**Test**: `test_dataframes.py::TestLocalRelationOperations::test_count_on_empty_dataframe`

**Error**:
```
pyspark.errors.exceptions.connect.SparkConnectException: No analyze result found!
```

**Root Cause**: When `createDataFrame([], schema)` is called with an empty list, the analyze request doesn't return a proper result.

**Affected Code**: `SparkConnectServiceImpl.analyzePlan()` or schema inference for empty dataframes.

**Fix**: Handle empty dataframes in analyze requests, returning proper schema without data.

**Priority**: Medium - Edge case but important for testing

---

## 3. Natural Join Without Explicit Condition

**Test**: `test_dataframes.py::TestLocalRelationOperations::test_join_local_dataframes`

**Error**:
```
Plan deserialization failed: condition is required for non-CROSS joins
```

**Root Cause**: When PySpark sends a join without an explicit condition (natural join or using columns), the `RelationConverter` requires a condition for non-CROSS joins.

**Affected Code**: `RelationConverter.convertJoin()`

**Context**: The test does:
```python
joined = df1.join(df2, "id")  # Join using column name
```

This is a "using" join where the condition is implicit (join on columns with same name).

**Fix**: Handle `Join.UsingJoin` case in RelationConverter - generate equality condition from the using columns.

**Priority**: High - Common Spark pattern

---

## 4. TPC-H Tests Require Data Setup

**Tests**: All `test_tpch.py::TestTPCH::*` tests (44 tests)

**Error**:
```
SQL error: Catalog Error: Table with name lineitem does not exist!
```

**Root Cause**: TPC-H tests require the TPC-H benchmark tables to be pre-loaded into DuckDB.

**Status**: Not a bug - requires test environment setup.

**Fix Options**:
1. Add TPC-H data generation as part of test setup
2. Mark TPC-H tests as requiring external data
3. Use DuckDB's built-in TPC-H extension to generate data

**Priority**: Low - Test infrastructure, not a bug

---

## Summary Table

| Issue | Test | Priority | Complexity |
|-------|------|----------|------------|
| COUNT_DISTINCT | test_distinct_operations | Medium | Low |
| Empty DataFrame | test_count_on_empty_dataframe | Medium | Medium |
| Natural/Using Join | test_join_local_dataframes | High | Medium |
| TPC-H Data | test_tpch.py | Low | Test setup |

---

## Related Files

- `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java` - Aggregate functions
- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` - Join handling
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java` - Analyze requests
