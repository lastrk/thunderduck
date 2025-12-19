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

| Issue | Test | Priority | Status |
|-------|------|----------|--------|
| ~~COUNT_DISTINCT~~ | test_distinct_operations | Medium | **RESOLVED** |
| ~~Empty DataFrame~~ | test_count_on_empty_dataframe | Medium | **RESOLVED** |
| Natural/Using Join | test_join_local_dataframes | High | Open |
| TPC-H Data | test_tpch.py | Low | Test setup |

---

## Related Files

- `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java` - Aggregate functions
- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` - Join handling
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java` - Analyze requests
- `core/src/main/java/com/thunderduck/types/SchemaParser.java` - Schema string parsing
- `core/src/main/java/com/thunderduck/logical/LocalDataRelation.java` - Empty DataFrame handling
