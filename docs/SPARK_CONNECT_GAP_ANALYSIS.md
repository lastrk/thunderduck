# Spark Connect 3.5.3 Gap Analysis for Thunderduck

**Version:** 2.0
**Date:** 2025-12-12
**Purpose:** Comprehensive analysis of Spark Connect operator support in Thunderduck

---

## Executive Summary

This document provides a detailed gap analysis between Spark Connect 3.5.3's protocol specification and Thunderduck's current implementation. The analysis covers:
- **Relations** (logical plan operators)
- **Expressions** (value computation)
- **Commands** (side-effecting operations)
- **Catalog operations**

### Overall Coverage

| Category | Total Operators | Implemented | Partial | Coverage |
|----------|----------------|-------------|---------|----------|
| Relations | 40 | 28 | 0 | **70%** |
| Expressions | 16 | 9 | 0 | **56.25%** |
| Commands | 10 | 2 | 1 | **25-30%** |
| Catalog | 26 | 0 | 0 | **0%** |

*Partial implementations*: WriteOperation (local paths only, S3/cloud needs httpfs extension)

---

## 1. Relations (Logical Plan Operators)

Relations are the core building blocks of Spark Connect query plans. They represent data transformations and sources.

**Note on Actions vs Transformations**: Most Relations are **transformations** (lazy, return DataFrame). However, some Relations are **action-like** and trigger immediate execution:
- **Tail** - Must scan all data to find last N rows (M21)
- **ShowString** - Executes and returns formatted ASCII table string (M22)

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
| **ShowString** | `show_string` | ✅ Implemented | `df.show()` - formats as ASCII table (M22) |
| **Range** | `range` | ✅ Implemented | `spark.range(start, end, step)` |
| **Drop** | `drop` | ✅ Implemented | `df.drop("col")` - uses DuckDB EXCLUDE (M19) |
| **WithColumns** | `with_columns` | ✅ Implemented | `df.withColumn("name", expr)` - uses REPLACE/append (M19) |
| **WithColumnsRenamed** | `with_columns_renamed` | ✅ Implemented | `df.withColumnRenamed("old", "new")` - uses EXCLUDE+alias (M19) |
| **Offset** | `offset` | ✅ Implemented | `df.offset(n)` - uses existing Limit class (M20) |
| **ToDF** | `to_df` | ✅ Implemented | `df.toDF("a", "b", "c")` - uses positional aliasing (M20) |
| **SubqueryAlias** | `subquery_alias` | ✅ Implemented | `df.alias("t")` - via subquery wrapping (M28). Qualifier field not yet supported. |
| **Tail** | `tail` | ✅ Implemented | `df.tail(n)` - ACTION, O(N) memory via TailBatchCollector (M21) |
| **Sample** | `sample` | ✅ Implemented | `df.sample(fraction, seed)` - Bernoulli sampling via DuckDB USING SAMPLE (M23) |
| **Hint** | `hint` | ✅ Implemented | `df.hint("BROADCAST")` - no-op pass-through (M25). DuckDB optimizer handles automatically. |
| **Repartition** | `repartition` | ✅ Implemented | `df.repartition(n)` - no-op in single-node DuckDB (M25) |
| **RepartitionByExpression** | `repartition_by_expression` | ✅ Implemented | `df.repartition(col("x"))` - no-op in single-node DuckDB (M25) |
| **NADrop** | `drop_na` | ✅ Implemented | `df.na.drop()` - via WHERE IS NOT NULL (M26). Schema inference for empty cols. |
| **NAFill** | `fill_na` | ✅ Implemented | `df.na.fill(value)` - via COALESCE (M26). Schema inference for empty cols. |
| **NAReplace** | `replace` | ✅ Implemented | `df.na.replace(old, new)` - via CASE WHEN (M26). Schema inference for empty cols. |
| **Unpivot** | `unpivot` | ✅ Implemented | `df.unpivot()` - via DuckDB native UNPIVOT (M27). Schema inference for values=None. |

### 1.2 Not Implemented Relations

#### Medium Priority (Advanced Operations)

| Relation | Proto Field | Priority | Use Case |
|----------|-------------|----------|----------|
| **ToSchema** | `to_schema` | 🟡 MEDIUM | Schema enforcement |

#### Lower Priority (Statistics - Return DataFrames)

| Relation | Proto Field | Priority | Returns | Use Case |
|----------|-------------|----------|---------|----------|
| **StatSummary** | `summary` | 🟢 LOW | DataFrame | `df.summary()` - stats as rows |
| **StatDescribe** | `describe` | 🟢 LOW | DataFrame | `df.describe()` - count/mean/std/min/max |
| **StatCrosstab** | `crosstab` | 🟢 LOW | DataFrame | `df.stat.crosstab()` - contingency table |
| **StatFreqItems** | `freq_items` | 🟢 LOW | DataFrame | `df.stat.freqItems()` - frequent items |
| **StatSampleBy** | `sample_by` | 🟢 LOW | DataFrame | `df.stat.sampleBy()` - stratified sample |

#### Lower Priority (Statistics - Return Scalars)

| Relation | Proto Field | Priority | Returns | Use Case |
|----------|-------------|----------|---------|----------|
| **StatCov** | `cov` | 🟢 LOW | Double | `df.stat.cov()` - covariance |
| **StatCorr** | `corr` | 🟢 LOW | Double | `df.stat.corr()` - correlation |
| **StatApproxQuantile** | `approx_quantile` | 🟢 LOW | Array[Double] | `df.stat.approxQuantile()` |

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
| **WriteOperation** | `write_operation` | ⚠️ Partial | - | `df.write.parquet()`, `.csv()`, `.json()` - local paths only (M24). S3/cloud not yet supported. |
| **CreateDataFrameViewCommand** | `create_dataframe_view` | ✅ Implemented | - | `df.createOrReplaceTempView()` |
| **SqlCommand** | `sql_command` | ✅ Implemented | - | `spark.sql()` (DDL + queries) |
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

1. ~~**Range** - Generate sequences~~ ✅ Implemented (2025-12-10)
2. ~~**CreateDataFrameViewCommand** - Temp view creation~~ ✅ Implemented
3. ~~**SqlCommand** - DDL support~~ ✅ Implemented (2025-12-10)
4. ~~**Drop** - Drop columns from DataFrame~~ ✅ Implemented (M19, 2025-12-10)
5. ~~**WithColumns** - Add/replace columns~~ ✅ Implemented (M19, 2025-12-10)
6. ~~**WithColumnsRenamed** - Rename columns~~ ✅ Implemented (M19, 2025-12-10)
7. ~~**Offset** - Required for pagination~~ ✅ Implemented (M20, 2025-12-10)
8. ~~**ToDF** - Rename all columns~~ ✅ Implemented (M20, 2025-12-10)
9. ~~**Sample** - Random sampling~~ ✅ Implemented (M23, 2025-12-12)
10. ~~**WriteOperation** - Write to files/tables~~ ✅ Implemented (M24, 2025-12-12)

**Phase 1 Complete!**

### Phase 2: DataFrame Stat Functions (Medium Priority)

1. ~~**NAFill**, **NADrop**, **NAReplace** - Null handling~~ ✅ Implemented (M26, 2025-12-12)
2. ~~**Hint** - Query optimization hints~~ ✅ Implemented (M25)
3. ~~**Repartition**, **RepartitionByExpression** - Partitioning~~ ✅ Implemented (M25)
4. ~~**Unpivot** - Data reshaping~~ ✅ Implemented (M27, 2025-12-12)
5. ~~**SubqueryAlias** - Proper alias handling~~ ✅ Implemented (M28, 2025-12-12)

**Phase 2 COMPLETE!**

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
- `df.withColumn()` - ✅ Implemented (M19)
- `df.drop()` - ✅ Implemented (M19)
- `df.sample()` - ✅ Implemented (M23)
- `df.write.parquet()` - ✅ Implemented (M24)
- `df.na.fill()` - ✅ Implemented (M26)
- `df.na.drop()` - ✅ Implemented (M26)
- `df.na.replace()` - ✅ Implemented (M26)

### Compatibility Concerns

1. **Error Messages**: When an unsupported operator is encountered, Thunderduck throws `PlanConversionException`. Users should receive clear error messages indicating which operator is not supported.

2. **Graceful Degradation**: Consider implementing stub handlers that return helpful error messages rather than generic exceptions.

3. **Version Compatibility**: This analysis is based on Spark Connect 3.5.3. Future Spark versions may add new operators.

---

## 8. Intentional Incompatibilities

Features intentionally not supported due to Spark/DuckDB architectural differences:

| Feature | Reason |
|---------|--------|
| `df.sample(withReplacement=True)` | DuckDB has no Poisson sampling; throws `PlanConversionException` |
| `df.hint("BROADCAST")` etc. | Accepted but ignored (no-op); DuckDB optimizer handles join strategies |
| `df.repartition(n)` / `df.repartition(col)` | Accepted but ignored (no-op); meaningless in single-node DuckDB |
| `df.alias("t")` with qualifier | Qualifier field not yet supported (multi-catalog scenarios); ignored |
| Streaming operations | DuckDB is not a streaming engine |
| Python/Scala UDFs | Requires JVM/Python interop not available in DuckDB |
| Bucketing | No DuckDB equivalent |

---

## 9. Source Files

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
# TRANSFORMATIONS (lazy, return DataFrame, chainable):
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
spark.range(0, 100)                           # Range
df.drop("col")                                # Drop (M19)
df.withColumn("new", expr)                    # WithColumns (M19)
df.withColumnRenamed("old", "new")            # WithColumnsRenamed (M19)
df.offset(n)                                  # Offset (M20)
df.toDF("a", "b", "c")                        # ToDF (M20)
df.sample(0.1, seed=42)                       # Sample (M23)
df.hint("BROADCAST")                          # Hint (M25) - no-op, DuckDB optimizes automatically
df.repartition(10)                            # Repartition (M25) - no-op in single-node DuckDB
df.repartition(col("x"))                      # RepartitionByExpression (M25) - no-op
df.na.drop()                                  # NADrop (M26) - drop rows with nulls
df.na.drop(subset=["col1", "col2"])           # NADrop (M26) - specific columns
df.na.fill(0)                                 # NAFill (M26) - fill nulls with value
df.na.fill({"col1": 0, "col2": "default"})    # NAFill (M26) - per-column fills
df.na.replace("old", "new")                   # NAReplace (M26) - replace values
df.unpivot(["id"], ["val1", "val2"], "var", "value")  # Unpivot (M27) - wide to long format
df.unpivot(["id"], None, "var", "value")      # Unpivot (M27) - auto-infer value columns
df.alias("t")                                 # SubqueryAlias (M28) - DataFrame aliasing
df.alias("a").join(df.alias("b"), ...)        # SubqueryAlias (M28) - self-joins

# ACTIONS (trigger execution, return values to driver):
df.tail(n)                                    # Tail (M21) - returns List[Row], O(N) memory
df.show()                                     # ShowString (M22) - formats as ASCII table
df.collect()                                  # Collect - returns all rows

# COMMANDS (side effects, no result data):
spark.sql("CREATE TEMP VIEW ...")             # DDL via SqlCommand
df.createOrReplaceTempView("view")            # CreateDataFrameViewCommand
df.write.parquet("/local/path")               # WriteOperation (M24) - local paths only
df.write.csv("/local/path")                   # WriteOperation (M24) - local paths only
df.write.json("/local/path")                  # WriteOperation (M24) - local paths only
```

## Appendix B: Quick Reference - What Doesn't Work

```python
# INTENTIONALLY NOT SUPPORTED (see Section 8):
df.sample(withReplacement=True, fraction=0.5) # Poisson sampling not available in DuckDB

# COMMANDS partially implemented:
df.write.parquet("s3://bucket/path")          # S3 writes need httpfs extension
df.write.csv("s3://bucket/path")              # S3 writes need httpfs extension

# TRANSFORMATIONS not yet implemented (return DataFrames):
df.describe()                                 # StatDescribe - returns DataFrame!
df.summary()                                  # StatSummary - returns DataFrame!

# CATALOG not yet implemented:
spark.catalog.listTables()                    # Catalog operations
```

## Appendix C: Actions vs Transformations

**Understanding the Distinction**:

| Type | Behavior | Returns | Chainable? |
|------|----------|---------|------------|
| **Transformation** | Lazy, builds plan | DataFrame | Yes |
| **Action** | Eager, executes | List/Value | No (terminal) |
| **Command** | Side effect | None/Result | N/A |

**Key Action-Like Relations** (trigger execution):
- `tail(n)` - Must scan all data to find last N rows, returns `List[Row]`
- `show()` / ShowString - Executes and formats output, returns String
- `collect()` - Returns all rows to driver
- `count()` - Returns single Long value

**Important**: Most "stat" operations (`describe()`, `summary()`, `crosstab()`) return **DataFrames**, not scalar values! They are transformations, not actions.

**Protocol vs Semantics**: In Spark Connect protocol, both transformations AND actions are represented as Relations. The distinction is semantic (lazy vs eager), not protocol-based.

---

**Document Version:** 2.2
**Last Updated:** 2025-12-12
**Author:** Analysis generated from Spark Connect 3.5.3 protobuf definitions
**M19 Update:** Added Drop, WithColumns, WithColumnsRenamed implementations
**M20 Update:** Added Offset, ToDF implementations
**M21 Update:** Added Tail implementation (memory-efficient O(N) via TailBatchCollector)
**M22 Update:** ShowString confirmed fully implemented (was incorrectly marked partial)
**v1.5 Update:** Clarified actions vs transformations; added semantic classification
**v1.6 Update:** Corrected ShowString to fully implemented (19 relations, 1 partial)
**v1.7 Update:** Added Sample (M23) - Bernoulli sampling via USING SAMPLE
**v1.8 Update:** Added WriteOperation (M24) - df.write.parquet/csv/json support
**v1.9 Update:** Added Hint, Repartition, RepartitionByExpression (M25) - no-op pass-throughs for distributed ops
**v2.0 Update:** Added NADrop, NAFill, NAReplace (M26) - df.na.drop/fill/replace via schema inference + SQL generation
**v2.1 Update:** Added Unpivot (M27) - df.unpivot() via DuckDB native UNPIVOT syntax with schema inference for values=None
**v2.2 Update:** Added SubqueryAlias (M28) - df.alias() via subquery wrapping. Phase 2 complete! 28/40 relations (70%)
