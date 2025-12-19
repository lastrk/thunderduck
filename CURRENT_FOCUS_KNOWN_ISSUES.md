# Known Issues

This document tracks known issues discovered during testing that need to be addressed in future work.

**Last Updated**: 2025-12-19

---

## 1. ~~COUNT_DISTINCT Function Not Supported~~ RESOLVED

**Status**: Fixed in commit (pending)

**Solution**: Added custom translators in `FunctionRegistry.java` for `count_distinct`, `sum_distinct`, and `avg_distinct` that generate proper SQL syntax with DISTINCT inside parentheses.

**Before**: `COUNT_DISTINCT(department)` - Error
**After**: `COUNT(DISTINCT department)` - Works

---

## 2. ~~Empty DataFrame Analyze Issue~~ RESOLVED

**Status**: Fixed in commit (pending)

**Solution**: Implemented `SchemaParser` utility class to parse Spark's struct format schema strings (e.g., `struct<id:int,name:string>`) into `StructType` objects. Updated `LocalDataRelation.inferSchema()` to use SchemaParser when Arrow data is empty but schema string is provided.

**Files Changed**:
- `core/src/main/java/com/thunderduck/types/SchemaParser.java` - New file
- `core/src/main/java/com/thunderduck/logical/LocalDataRelation.java` - Updated inferSchema()
- `tests/src/test/java/com/thunderduck/types/SchemaParserTest.java` - 45 unit tests
- `tests/integration/test_empty_dataframe.py` - 15 E2E tests (13 pass, 2 skipped for known join issue)

**Before**: `spark.createDataFrame([], schema)` - "No analyze result found!"
**After**: Empty DataFrame created successfully with proper schema

---

## 3. ~~Natural Join Without Explicit Condition~~ RESOLVED

**Status**: Fixed in commit (pending)

**Solution**: Updated `RelationConverter.convertJoin()` to handle the `using_columns` field from the Spark Connect protobuf. When a USING join is received (e.g., `df1.join(df2, "id")`), the converter now builds equality conditions from the using column names.

**Implementation**:
- Added `buildUsingCondition()` helper method in `RelationConverter.java`
- For single column: builds `left.col = right.col`
- For multiple columns: builds `left.col1 = right.col1 AND left.col2 = right.col2`
- Uses plan_id from child relations for proper column qualification

**Files Changed**:
- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` - Added USING handling
- `tests/src/test/java/com/thunderduck/connect/converter/RelationConverterUsingJoinTest.java` - 7 unit tests
- `tests/integration/test_using_joins.py` - 11 E2E tests
- `tests/integration/test_empty_dataframe.py` - Unskipped 2 join tests

**Before**: `df1.join(df2, "id")` - "condition is required for non-CROSS joins"
**After**: Join works correctly, equivalent to `df1.join(df2, df1["id"] == df2["id"])`

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

| Issue | Test | Priority | Status |
|-------|------|----------|--------|
| ~~COUNT_DISTINCT~~ | test_distinct_operations | Medium | **RESOLVED** |
| ~~Empty DataFrame~~ | test_count_on_empty_dataframe | Medium | **RESOLVED** |
| ~~Natural/Using Join~~ | test_join_local_dataframes | High | **RESOLVED** |
| TPC-H Data | test_tpch.py | Low | Test setup |

---

## Related Files

- `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java` - Aggregate functions
- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` - Join handling
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java` - Analyze requests
- `core/src/main/java/com/thunderduck/types/SchemaParser.java` - Schema string parsing
- `core/src/main/java/com/thunderduck/logical/LocalDataRelation.java` - Empty DataFrame handling
