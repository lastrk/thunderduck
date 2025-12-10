# Spark Connect 3.5.3 Gap Analysis for Thunderduck

**Version:** 1.0
**Date:** 2025-12-09
**Purpose:** Comprehensive analysis of Spark Connect operator support in Thunderduck

---

## Executive Summary

This document provides a detailed gap analysis between Spark Connect 3.5.3's protocol specification and Thunderduck's current implementation. The analysis covers:
- **Relations** (logical plan operators)
- **Expressions** (value computation)
- **Commands** (side-effecting operations)
- **Catalog operations**

### Overall Coverage

| Category | Total Operators | Implemented | Coverage |
|----------|----------------|-------------|----------|
| Relations | 40 | 13 | **32.5%** |
| Expressions | 16 | 9 | **56.25%** |
| Commands | 10 | 0 | **0%** |
| Catalog | 26 | 0 | **0%** |

---

## 1. Relations (Logical Plan Operators)

Relations are the core building blocks of Spark Connect query plans. They represent data transformations and sources.

### 1.1 Implemented Relations

| Relation | Proto Field | Implementation Status | Notes |
|----------|-------------|----------------------|-------|
| **Read** | `read` | ✅ Implemented | Parquet data source, named tables |
| **Project** | `project` | ✅ Implemented | Column selection and computation |
| **Filter** | `filter` | ✅ Implemented | WHERE clause predicates |
| **Aggregate** | `aggregate` | ✅ Implemented | GROUP BY + aggregations |
| **Sort** | `sort` | ✅ Implemented | ORDER BY with null ordering |
| **Limit** | `limit` | ✅ Implemented | LIMIT n |
| **Join** | `join` | ✅ Implemented | All join types (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, CROSS) |
| **SetOperation** | `set_op` | ✅ Implemented | UNION, INTERSECT, EXCEPT |
| **SQL** | `sql` | ✅ Implemented | Direct SQL queries |
| **LocalRelation** | `local_relation` | ✅ Implemented | Arrow IPC data (recent addition) |
| **Deduplicate** | `deduplicate` | ✅ Implemented | DISTINCT operations |
| **ShowString** | `show_string` | ✅ Implemented | Passthrough to input relation |
| **SubqueryAlias** | `subquery_alias` | ⚠️ Partial | Need explicit handling |

### 1.2 Not Implemented Relations

#### High Priority (Common DataFrame Operations)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **Offset** | `offset` | 🔴 HIGH | `df.offset(n)` - pagination |
| **Tail** | `tail` | 🔴 HIGH | `df.tail(n)` - last n rows |
| **Drop** | `drop` | 🔴 HIGH | `df.drop("col")` - drop columns |
| **WithColumns** | `with_columns` | 🔴 HIGH | `df.withColumn("name", expr)` |
| **WithColumnsRenamed** | `with_columns_renamed` | 🔴 HIGH | `df.withColumnRenamed("old", "new")` |
| **ToDF** | `to_df` | 🔴 HIGH | `df.toDF("a", "b", "c")` |
| **Sample** | `sample` | 🔴 HIGH | `df.sample(0.1)` - random sampling |
| **Range** | `range` | 🔴 HIGH | `spark.range(0, 100)` |

#### Medium Priority (Advanced Operations)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **Hint** | `hint` | 🟡 MEDIUM | Query hints (BROADCAST, MERGE, etc.) |
| **Repartition** | `repartition` | 🟡 MEDIUM | `df.repartition(n)` |
| **RepartitionByExpression** | `repartition_by_expression` | 🟡 MEDIUM | `df.repartition(col("x"))` |
| **Unpivot** | `unpivot` | 🟡 MEDIUM | Wide-to-long transformation |
| **ToSchema** | `to_schema` | 🟡 MEDIUM | Schema enforcement |

#### Lower Priority (NA Functions / Statistics)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **NAFill** | `fill_na` | 🟡 MEDIUM | `df.na.fill()` |
| **NADrop** | `drop_na` | 🟡 MEDIUM | `df.na.drop()` |
| **NAReplace** | `replace` | 🟡 MEDIUM | `df.na.replace()` |
| **StatSummary** | `summary` | 🟢 LOW | `df.summary()` |
| **StatDescribe** | `describe` | 🟢 LOW | `df.describe()` |
| **StatCrosstab** | `crosstab` | 🟢 LOW | `df.stat.crosstab()` |
| **StatCov** | `cov` | 🟢 LOW | `df.stat.cov()` |
| **StatCorr** | `corr` | 🟢 LOW | `df.stat.corr()` |
| **StatApproxQuantile** | `approx_quantile` | 🟢 LOW | `df.stat.approxQuantile()` |
| **StatFreqItems** | `freq_items` | 🟢 LOW | `df.stat.freqItems()` |
| **StatSampleBy** | `sample_by` | 🟢 LOW | `df.stat.sampleBy()` |

#### Streaming / UDF (Future)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **Parse** | `parse` | 🟢 LOW | CSV/JSON parsing |
| **MapPartitions** | `map_partitions` | 🔵 FUTURE | Python/Scala UDFs |
| **GroupMap** | `group_map` | 🔵 FUTURE | `applyInPandas` |
| **CoGroupMap** | `co_group_map` | 🔵 FUTURE | `cogroup().applyInPandas` |
| **WithWatermark** | `with_watermark` | 🔵 FUTURE | Streaming watermarks |
| **ApplyInPandasWithState** | `apply_in_pandas_with_state` | 🔵 FUTURE | Stateful streaming |
| **CollectMetrics** | `collect_metrics` | 🔵 FUTURE | Metrics collection |
| **CommonInlineUserDefinedTableFunction** | `common_inline_user_defined_table_function` | 🔵 FUTURE | Python UDTFs |

#### Cache / Catalog (Requires State Management)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **CachedLocalRelation** | `cached_local_relation` | 🟡 MEDIUM | Cached local data |
| **CachedRemoteRelation** | `cached_remote_relation` | 🟡 MEDIUM | Server-side caching |
| **Catalog** | `catalog` | 🟡 MEDIUM | Catalog operations |

---

## 2. Expressions

Expressions compute values and are used in projections, filters, aggregations, etc.

### 2.1 Implemented Expressions

| Expression | Proto Field | Implementation Status | Notes |
|------------|-------------|----------------------|-------|
| **Literal** | `literal` | ✅ Implemented | All primitive types, dates, timestamps, decimals |
| **UnresolvedAttribute** | `unresolved_attribute` | ✅ Implemented | Column references |
| **UnresolvedFunction** | `unresolved_function` | ✅ Implemented | Function calls with argument mapping |
| **Alias** | `alias` | ✅ Implemented | AS expressions |
| **Cast** | `cast` | ✅ Implemented | Type casting |
| **UnresolvedStar** | `unresolved_star` | ✅ Implemented | SELECT * |
| **ExpressionString** | `expression_string` | ✅ Implemented | Raw SQL expressions |
| **Window** | `window` | ✅ Implemented | Window functions with frame specs |
| **SortOrder** | `sort_order` | ✅ Implemented | Sort ordering (handled in RelationConverter) |

### 2.2 Not Implemented Expressions

| Expression | Proto Field | Priority | Use Case |
|------------|-------------|----------|----------|
| **UnresolvedRegex** | `unresolved_regex` | 🟡 MEDIUM | `SELECT \`col_*\`` regex patterns |
| **LambdaFunction** | `lambda_function` | 🟡 MEDIUM | `transform(arr, x -> x + 1)` |
| **UnresolvedNamedLambdaVariable** | `unresolved_named_lambda_variable` | 🟡 MEDIUM | Lambda variables |
| **UnresolvedExtractValue** | `unresolved_extract_value` | 🟡 MEDIUM | `col["key"]`, `col.field` |
| **UpdateFields** | `update_fields` | 🟢 LOW | Struct field manipulation |
| **CallFunction** | `call_function` | 🟢 LOW | Alternative function call syntax |
| **CommonInlineUserDefinedFunction** | `common_inline_user_defined_function` | 🔵 FUTURE | Python/Scala UDFs |

### 2.3 Literal Type Support

| Literal Type | Proto Field | Status | Notes |
|--------------|-------------|--------|-------|
| Null | `null` | ✅ | |
| Binary | `binary` | ✅ | |
| Boolean | `boolean` | ✅ | |
| Byte | `byte` | ✅ | |
| Short | `short` | ✅ | |
| Integer | `integer` | ✅ | |
| Long | `long` | ✅ | |
| Float | `float` | ✅ | |
| Double | `double` | ✅ | |
| Decimal | `decimal` | ✅ | |
| String | `string` | ✅ | |
| Date | `date` | ✅ | Days since epoch |
| Timestamp | `timestamp` | ✅ | Microseconds since epoch |
| TimestampNtz | `timestamp_ntz` | ❌ | Needs implementation |
| CalendarInterval | `calendar_interval` | ❌ | Needs implementation |
| YearMonthInterval | `year_month_interval` | ❌ | Needs implementation |
| DayTimeInterval | `day_time_interval` | ❌ | Needs implementation |
| Array | `array` | ❌ | Complex type literal |
| Map | `map` | ❌ | Complex type literal |
| Struct | `struct` | ❌ | Complex type literal |

---

## 3. Commands

Commands are operations that don't return result data directly but perform side effects.

### 3.1 Implementation Status

| Command | Proto Field | Status | Priority | Use Case |
|---------|-------------|--------|----------|----------|
| **WriteOperation** | `write_operation` | ❌ Not Implemented | 🔴 HIGH | `df.write.parquet()` |
| **CreateDataFrameViewCommand** | `create_dataframe_view` | ❌ Not Implemented | 🔴 HIGH | `df.createOrReplaceTempView()` |
| **SqlCommand** | `sql_command` | ❌ Not Implemented | 🔴 HIGH | `spark.sql()` with side effects |
| **WriteOperationV2** | `write_operation_v2` | ❌ Not Implemented | 🟡 MEDIUM | Table writes |
| **RegisterFunction** | `register_function` | ❌ Not Implemented | 🔵 FUTURE | UDF registration |
| **RegisterTableFunction** | `register_table_function` | ❌ Not Implemented | 🔵 FUTURE | UDTF registration |
| **WriteStreamOperationStart** | `write_stream_operation_start` | ❌ Not Implemented | 🔵 FUTURE | Streaming |
| **StreamingQueryCommand** | `streaming_query_command` | ❌ Not Implemented | 🔵 FUTURE | Streaming |
| **StreamingQueryManagerCommand** | `streaming_query_manager_command` | ❌ Not Implemented | 🔵 FUTURE | Streaming |
| **GetResourcesCommand** | `get_resources_command` | ❌ Not Implemented | 🟢 LOW | Resource info |

---

## 4. Catalog Operations

Catalog operations allow interaction with Spark's metadata catalog.

### 4.1 Implementation Status

All catalog operations are **NOT IMPLEMENTED**:

| Operation | Proto Message | Priority | Use Case |
|-----------|---------------|----------|----------|
| **CurrentDatabase** | `current_database` | 🟡 MEDIUM | `spark.catalog.currentDatabase` |
| **SetCurrentDatabase** | `set_current_database` | 🟡 MEDIUM | `spark.catalog.setCurrentDatabase` |
| **ListDatabases** | `list_databases` | 🟡 MEDIUM | `spark.catalog.listDatabases` |
| **ListTables** | `list_tables` | 🟡 MEDIUM | `spark.catalog.listTables` |
| **ListFunctions** | `list_functions` | 🟢 LOW | `spark.catalog.listFunctions` |
| **ListColumns** | `list_columns` | 🟡 MEDIUM | `spark.catalog.listColumns` |
| **GetDatabase** | `get_database` | 🟢 LOW | `spark.catalog.getDatabase` |
| **GetTable** | `get_table` | 🟢 LOW | `spark.catalog.getTable` |
| **GetFunction** | `get_function` | 🟢 LOW | `spark.catalog.getFunction` |
| **DatabaseExists** | `database_exists` | 🟡 MEDIUM | `spark.catalog.databaseExists` |
| **TableExists** | `table_exists` | 🟡 MEDIUM | `spark.catalog.tableExists` |
| **FunctionExists** | `function_exists` | 🟢 LOW | `spark.catalog.functionExists` |
| **CreateExternalTable** | `create_external_table` | 🟡 MEDIUM | `spark.catalog.createExternalTable` |
| **CreateTable** | `create_table` | 🟡 MEDIUM | `spark.catalog.createTable` |
| **DropTempView** | `drop_temp_view` | 🔴 HIGH | `spark.catalog.dropTempView` |
| **DropGlobalTempView** | `drop_global_temp_view` | 🟡 MEDIUM | `spark.catalog.dropGlobalTempView` |
| **RecoverPartitions** | `recover_partitions` | 🟢 LOW | `spark.catalog.recoverPartitions` |
| **IsCached** | `is_cached` | 🟢 LOW | `spark.catalog.isCached` |
| **CacheTable** | `cache_table` | 🟢 LOW | `spark.catalog.cacheTable` |
| **UncacheTable** | `uncache_table` | 🟢 LOW | `spark.catalog.uncacheTable` |
| **ClearCache** | `clear_cache` | 🟢 LOW | `spark.catalog.clearCache` |
| **RefreshTable** | `refresh_table` | 🟢 LOW | `spark.catalog.refreshTable` |
| **RefreshByPath** | `refresh_by_path` | 🟢 LOW | `spark.catalog.refreshByPath` |
| **CurrentCatalog** | `current_catalog` | 🟢 LOW | `spark.catalog.currentCatalog` |
| **SetCurrentCatalog** | `set_current_catalog` | 🟢 LOW | `spark.catalog.setCurrentCatalog` |
| **ListCatalogs** | `list_catalogs` | 🟢 LOW | `spark.catalog.listCatalogs` |

---

## 5. Function Support

Thunderduck implements function name mapping between Spark and DuckDB.

### 5.1 Explicitly Mapped Functions

These functions have explicit mappings in `ExpressionConverter.mapFunctionName()`:

| Spark Function | DuckDB Function | Category |
|----------------|-----------------|----------|
| `ENDSWITH` | `ENDS_WITH` | String |
| `STARTSWITH` | `STARTS_WITH` | String |
| `CONTAINS` | `CONTAINS` | String |
| `SUBSTRING` | `SUBSTR` | String |
| `RLIKE` | `REGEXP_MATCHES` | String |
| `YEAR/MONTH/DAY` | Same | Date/Time |
| `DAYOFMONTH` | `DAY` | Date/Time |
| `DAYOFWEEK/DAYOFYEAR` | Same | Date/Time |
| `HOUR/MINUTE/SECOND` | Same | Date/Time |
| `DATE_ADD/DATE_SUB` | Same | Date/Time |
| `DATEDIFF` | `DATE_DIFF` | Date/Time |
| `RAND` | `RANDOM` | Math |
| `POW` | `POWER` | Math |
| `LOG` | `LN` | Math |
| `LOG10/LOG2` | Same | Math |
| `STDDEV` | `STDDEV_SAMP` | Aggregate |
| `STDDEV_POP/STDDEV_SAMP` | Same | Aggregate |
| `VAR_POP/VAR_SAMP` | Same | Aggregate |
| `VARIANCE` | `VAR_SAMP` | Aggregate |
| `COLLECT_LIST/COLLECT_SET` | `LIST` | Aggregate |

### 5.2 Window Functions

Supported window functions:
- `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `NTILE`, `CUME_DIST`
- `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`

### 5.3 Binary/Unary Operators

Fully supported:
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Logical: `AND`, `OR`, `NOT`, `&&`, `||`, `!`
- Null checks: `ISNULL`, `ISNOTNULL`

### 5.4 Special Expression Handling

| Feature | Status | Notes |
|---------|--------|-------|
| `ISIN` / `IN` | ✅ | Converted to SQL IN clause |
| `WHEN` / `CASE_WHEN` | ✅ | Converted to CASE statement |
| `OTHERWISE` | ✅ | ELSE clause handling |

---

## 6. Implementation Recommendations

### Phase 1: Critical Gaps (High Priority)

These are commonly used operations that users will expect to work:

1. **Offset** - Required for pagination
2. **Drop** - Drop columns from DataFrame
3. **WithColumns** - Add/replace columns
4. **WithColumnsRenamed** - Rename columns
5. **ToDF** - Rename all columns
6. **Sample** - Random sampling
7. **Range** - Generate sequences
8. **CreateDataFrameViewCommand** - Temp view creation
9. **WriteOperation** - Write to files/tables

**Estimated effort:** 2-3 weeks

### Phase 2: DataFrame NA/Stat Functions (Medium Priority)

1. **NAFill**, **NADrop**, **NAReplace** - Null handling
2. **Hint** - Query optimization hints
3. **Repartition**, **RepartitionByExpression** - Partitioning
4. **Unpivot** - Data reshaping
5. **SubqueryAlias** - Proper alias handling

**Estimated effort:** 1-2 weeks

### Phase 3: Complex Types & Expressions (Medium Priority)

1. **UnresolvedExtractValue** - Struct/Array/Map access
2. **LambdaFunction** - Array transform operations
3. **Complex literal types** (Array, Map, Struct)
4. **Interval types** (CalendarInterval, etc.)

**Estimated effort:** 2-3 weeks

### Phase 4: Catalog Operations (Medium Priority)

1. **DropTempView** - Critical for view management
2. **TableExists**, **DatabaseExists** - Existence checks
3. **ListTables**, **ListDatabases** - Metadata queries
4. **CreateTable**, **CreateExternalTable** - Table creation

**Estimated effort:** 2-3 weeks

### Phase 5: Statistical Functions (Lower Priority)

1. **StatDescribe**, **StatSummary** - Basic statistics
2. **StatCorr**, **StatCov** - Correlation/covariance
3. **StatCrosstab**, **StatFreqItems** - Frequency analysis

**Estimated effort:** 1-2 weeks

### Phase 6: Streaming & UDFs (Future)

1. Streaming operations
2. Python UDF support
3. Scala UDF support

**Estimated effort:** 4+ weeks

---

## 7. Key Observations

### What Triggered This Analysis

The recent contribution (commit `6da4199`) adding `LocalRelation` support demonstrates that external contributors are finding gaps when trying to use Thunderduck. The `LOCAL_RELATION` operator is used by Spark for:
- Returning pre-computed results (like `count()`)
- Creating DataFrames from Python lists
- Cached local data

### TPC-H/TPC-DS Coverage

The current implementation successfully handles TPC-H and TPC-DS queries because these benchmarks primarily use:
- Read (data sources)
- Project (column selection)
- Filter (WHERE clauses)
- Aggregate (GROUP BY)
- Sort (ORDER BY)
- Limit
- Join (various types)

These are all implemented. However, production workloads often include:
- `df.withColumn()` - NOT implemented
- `df.drop()` - NOT implemented
- `df.sample()` - NOT implemented
- `df.na.fill()` - NOT implemented

### Compatibility Concerns

1. **Error Messages**: When an unsupported operator is encountered, Thunderduck throws `PlanConversionException`. Users should receive clear error messages indicating which operator is not supported.

2. **Graceful Degradation**: Consider implementing stub handlers that return helpful error messages rather than generic exceptions.

3. **Version Compatibility**: This analysis is based on Spark Connect 3.5.3. Future Spark versions may add new operators.

---

## 8. Source Files

### Protocol Definitions

| File | Contents |
|------|----------|
| `connect-server/src/main/proto/spark/connect/relations.proto` | 40 relation types |
| `connect-server/src/main/proto/spark/connect/expressions.proto` | 16 expression types |
| `connect-server/src/main/proto/spark/connect/commands.proto` | 10 command types |
| `connect-server/src/main/proto/spark/connect/catalog.proto` | 26 catalog operations |

### Implementation Files

| File | Contents |
|------|----------|
| `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java` | Relation handling |
| `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java` | Expression handling |
| `connect-server/src/main/java/com/thunderduck/connect/converter/PlanConverter.java` | Plan coordination |

---

## Appendix A: Quick Reference - What Works

```python
# These operations work:
df = spark.read.parquet("data.parquet")      # Read
df.select("col1", "col2")                     # Project
df.filter(df.col > 10)                        # Filter
df.groupBy("col").agg(sum("val"))            # Aggregate
df.orderBy("col")                             # Sort
df.limit(100)                                 # Limit
df.join(df2, "key")                           # Join
df.union(df2)                                 # SetOperation
df.distinct()                                 # Deduplicate
spark.sql("SELECT * FROM ...")                # SQL
spark.createDataFrame([(1,2),(3,4)])          # LocalRelation
```

## Appendix B: Quick Reference - What Doesn't Work

```python
# These operations do NOT work (will throw PlanConversionException):
df.drop("col")                                # Drop
df.withColumn("new", expr)                    # WithColumns
df.withColumnRenamed("old", "new")            # WithColumnsRenamed
df.toDF("a", "b", "c")                        # ToDF
df.sample(0.1)                                # Sample
df.na.fill(0)                                 # NAFill
df.na.drop()                                  # NADrop
df.createOrReplaceTempView("view")            # CreateDataFrameViewCommand
df.write.parquet("output")                    # WriteOperation
spark.range(0, 100)                           # Range
df.hint("BROADCAST")                          # Hint
df.repartition(10)                            # Repartition
df.unpivot(...)                               # Unpivot
df.stat.describe()                            # StatDescribe
spark.catalog.listTables()                    # Catalog operations
```

---

**Document Version:** 1.0
**Last Updated:** 2025-12-09
**Author:** Analysis generated from Spark Connect 3.5.3 protobuf definitions
