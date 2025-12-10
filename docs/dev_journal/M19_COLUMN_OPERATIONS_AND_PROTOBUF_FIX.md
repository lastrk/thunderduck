# M19 Completion Report: Column Operations and Protobuf Build Fix

## Date: December 10, 2025

## Overview
This milestone implemented three key DataFrame column operations (`drop`, `withColumnRenamed`, `withColumn`) and fixed a critical protobuf build issue that blocked the project.

## Achievements

### 1. Drop Relation Implementation (100% Complete)
- **Feature**: `df.drop('col1', 'col2', ...)` - removes columns from DataFrame
- **SQL Generation**: Uses DuckDB's `EXCLUDE` clause: `SELECT * EXCLUDE (col1, col2) FROM ...`
- **Edge Case**: Non-existent columns throw error (Spark silently ignores - documented difference)

### 2. WithColumnsRenamed Implementation (100% Complete)
- **Feature**: `df.withColumnRenamed('old', 'new')` - renames a column
- **SQL Generation**: Uses DuckDB's `EXCLUDE` + aliasing:
  ```sql
  SELECT * EXCLUDE (old_col), old_col AS new_col FROM ...
  ```
- **Note**: Column order may change (renamed column moves to end)
- **Edge Case**: Non-existent columns throw error (Spark silently ignores - documented difference)

### 3. WithColumns Implementation (100% Complete)
- **Feature**: `df.withColumn('name', expr)` - adds or replaces a column
- **SQL Generation**: Uses DuckDB's `REPLACE` clause for existing columns:
  ```sql
  SELECT *, expr AS new_col FROM ... -- for new columns
  SELECT * REPLACE (expr AS existing_col) FROM ... -- for replacing existing
  ```

### 4. Protobuf Build Fix (100% Complete)
- **Problem**: xolstice protobuf-maven-plugin is archived and broken
  - Plugin reports "Compiling 8 proto files" but generates nothing
  - Plan.java/PlanOrBuilder.java missing from output
  - Caused NoClassDefFoundError at runtime

- **Attempted Solution**: ascopes/protobuf-maven-plugin
  - Requires Maven 3.9.6+ (we have 3.6.3)
  - Configuration syntax incompatible

- **Final Solution**: Custom protoc invocation via maven-antrun-plugin
  1. maven-dependency-plugin downloads protoc and grpc-java plugin
  2. Unzip well-known proto types from protobuf-java JAR
  3. maven-antrun-plugin executes protoc directly
  4. build-helper-maven-plugin adds generated sources to build

## Key Files Modified

| File | Change |
|------|--------|
| `connect-server/pom.xml` | Replaced xolstice plugin with antrun-based protoc execution |
| `connect-server/.../converter/RelationConverter.java` | Added `convertDrop()`, `convertWithColumnsRenamed()`, `convertWithColumns()` |
| `tests/.../test_dataframes.py` | Added `TestColumnOperations` class with 13 E2E tests |
| `docs/architect/PROTOBUF_AND_ARROW_CONFIGURATION.md` | Updated with new build approach |

## Test Results

### E2E Tests (TestColumnOperations)
```
13 passed, 0 failed
```

| Test | Description |
|------|-------------|
| `test_drop_single_column` | Drop single column |
| `test_drop_multiple_columns` | Drop multiple columns |
| `test_drop_nonexistent_column` | Error on non-existent column |
| `test_drop_with_range` | Drop on range DataFrame |
| `test_with_column_renamed_single` | Rename single column |
| `test_with_column_renamed_multiple` | Rename multiple columns |
| `test_with_column_renamed_nonexistent` | Error on non-existent column |
| `test_with_column_add_new` | Add new column |
| `test_with_column_replace_existing` | Replace existing column |
| `test_with_column_literal` | Add constant column |
| `test_with_column_multiple` | Multiple withColumn calls |
| `test_with_column_on_range` | withColumn on range |
| `test_combined_operations` | drop + rename + withColumn |

## Architecture Decision: DuckDB EXCLUDE/REPLACE Syntax

### DuckDB-Specific SQL Features Used
- `EXCLUDE` clause: `SELECT * EXCLUDE (col1, col2) FROM ...`
- `REPLACE` clause: `SELECT * REPLACE (expr AS col) FROM ...`

These DuckDB extensions provide clean SQL generation for column operations without needing to enumerate all columns explicitly.

### Known Behavioral Differences from Spark
1. **Non-existent columns**: DuckDB throws error, Spark silently ignores
2. **Column order**: Renamed/added columns may appear at different positions

## Lessons Learned

1. **xolstice protobuf-maven-plugin is dead**: Archived project with unfixed bugs
2. **ascopes plugin requires Maven 3.9.6+**: Not compatible with Maven 3.6.3
3. **Direct protoc execution works reliably**: Using maven-antrun-plugin + manual proto file extraction
4. **DuckDB's EXCLUDE/REPLACE are powerful**: Enable clean SQL generation for column operations

## Gap Analysis Update

### Relations - Before vs After
| Relation | Before | After |
|----------|--------|-------|
| Drop | Not implemented | Implemented |
| WithColumnsRenamed | Not implemented | Implemented |
| WithColumns | Not implemented | Implemented |
| Offset | Not implemented | Not implemented |
| Tail | Not implemented | Not implemented |

## Next Steps

1. **Implement `Offset`** - Pagination support
2. **Implement `Tail`** - Last N rows
3. **Investigate column order preservation** - For withColumnRenamed

## Conclusion

M19 successfully delivers three essential DataFrame column operations and fixes a critical build blocker. The protobuf build is now stable using direct protoc invocation, bypassing the broken xolstice plugin. All 13 E2E tests pass, demonstrating correct functionality of drop, withColumnRenamed, and withColumn operations.

## Metrics Summary
- **E2E Tests**: 13/13 passed
- **Protobuf Build**: Fixed (antrun-based solution)
- **New Relations**: 3 (Drop, WithColumnsRenamed, WithColumns)
