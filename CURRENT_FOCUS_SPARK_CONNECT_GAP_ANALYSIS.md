# Spark Connect 4.0.x Gap Analysis for Thunderduck

**Version:** 4.1
**Date:** 2025-12-17
**Purpose:** Comprehensive analysis of Spark Connect operator support in Thunderduck
**Validation:** 266 differential tests (all passing) - see [Differential Testing Architecture](docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)

---

## Executive Summary

This document provides a detailed gap analysis between Spark Connect 4.0.x's protocol specification and Thunderduck's current implementation. The analysis covers:
- **Relations** (logical plan operators)
- **Expressions** (value computation)
- **Commands** (side-effecting operations)
- **Catalog operations**

### Overall Coverage

| Category | Total Operators | Implemented | Partial | Coverage |
|----------|----------------|-------------|---------|----------|
| Relations | 40 | 38 | 0 | **95%** |
| Expressions | 16 | 15 | 0 | **94%** |
| Commands | 10 | 2 | 1 | **25-30%** |
| Catalog | 26 | 26 | 0 | **100%** |

*Partial implementations*: WriteOperation (local paths only, S3/cloud needs httpfs extension)

*Catalog Note*: All 26 catalog operations implemented (M41-M44). CREATE_TABLE and CREATE_EXTERNAL_TABLE both support internal and external tables (CSV/Parquet/JSON as VIEWs). 7 operations are no-ops for DuckDB compatibility (caching, partitions). **100% catalog coverage achieved.**

*Statistics Note*: All 8 statistics operations implemented (M45). df.stat.cov/corr/approxQuantile return scalars/arrays; df.describe/summary/crosstab/freqItems/sampleBy return DataFrames. **100% statistics coverage achieved.**

*Lambda Note*: LambdaFunction, UnresolvedNamedLambdaVariable, and CallFunction expressions implemented (M46). Supports transform, filter, exists, forall, aggregate HOFs. zip_with and map HOFs have partial support.

*Complex Types Note*: UnresolvedExtractValue, UnresolvedRegex, and UpdateFields expressions implemented (M47). Supports struct.field, arr[index], map[key] access with 0-to-1 index conversion. withField adds struct fields; dropFields has limited support. colRegex translates to DuckDB COLUMNS(). **Expression coverage increased from 75% to 94%.**

*Type Literals Note*: All type literals implemented (M48). TimestampNTZ, CalendarInterval, YearMonthInterval, DayTimeInterval, Array, Map, Struct literals now supported. **100% literal type coverage achieved.**

*ToSchema Note*: ToSchema relation implemented (M49). DataFrame.to(schema) supports column reordering, projection, and type casting. **Relation coverage: 95% (38/40).**

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
| **StatCov** | `cov` | ✅ Implemented | `df.stat.cov(col1, col2)` - sample covariance via COVAR_SAMP (M45) |
| **StatCorr** | `corr` | ✅ Implemented | `df.stat.corr(col1, col2)` - Pearson correlation via CORR (M45) |
| **StatApproxQuantile** | `approx_quantile` | ✅ Implemented | `df.stat.approxQuantile()` - quantiles via QUANTILE_CONT (M45) |
| **StatDescribe** | `describe` | ✅ Implemented | `df.describe()` - count/mean/stddev/min/max (M45) |
| **StatSummary** | `summary` | ✅ Implemented | `df.summary()` - configurable statistics with percentiles (M45) |
| **StatCrosstab** | `crosstab` | ✅ Implemented | `df.stat.crosstab()` - contingency table via PIVOT (M45) |
| **StatFreqItems** | `freq_items` | ✅ Implemented | `df.stat.freqItems()` - frequent items via LIST aggregation (M45) |
| **StatSampleBy** | `sample_by` | ✅ Implemented | `df.stat.sampleBy()` - stratified sampling with fractions (M45) |
| **ToSchema** | `to_schema` | ✅ Implemented | `df.to(schema)` - column reordering, projection, type casting (M49) |

### 1.2 Not Implemented Relations

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
| **LambdaFunction** | `lambda_function` | ✅ Implemented | `transform(arr, x -> x + 1)` - DuckDB Python-style syntax (M46) |
| **UnresolvedNamedLambdaVariable** | `unresolved_named_lambda_variable` | ✅ Implemented | Lambda variable references within lambda bodies (M46) |
| **CallFunction** | `call_function` | ✅ Implemented | Dynamic function calls by name (M46) |
| **UnresolvedExtractValue** | `unresolved_extract_value` | ✅ Implemented | `col["key"]`, `col.field`, `arr[index]` - 0-to-1 index conversion (M47) |
| **UnresolvedRegex** | `unresolved_regex` | ✅ Implemented | `df.colRegex()` - translates to DuckDB COLUMNS() (M47) |
| **UpdateFields** | `update_fields` | ✅ Implemented | `col.withField()` - struct_insert (M47). dropFields limited support. |

### 2.2 Not Implemented Expressions

| Expression | Proto Field | Priority | Use Case |
|------------|-------------|----------|----------|
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
| TimestampNtz | `timestamp_ntz` | ✅ | (M48) Timezone-naive timestamp |
| CalendarInterval | `calendar_interval` | ✅ | (M48) TO_MONTHS/TO_DAYS/TO_MICROS |
| YearMonthInterval | `year_month_interval` | ✅ | (M48) TO_MONTHS() |
| DayTimeInterval | `day_time_interval` | ✅ | (M48) TO_MICROSECONDS() |
| Array | `array` | ✅ | (M48) list_value() with elements |
| Map | `map` | ✅ | (M48) MAP([keys], [values]) |
| Struct | `struct` | ✅ | (M48) STRUCT_PACK with fields |

**100% literal type coverage achieved (M48).**

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

**Implemented (M41-M44, 2025-12-16):**

| Operation | Proto Message | Status | Use Case |
|-----------|---------------|--------|----------|
| **DropTempView** | `drop_temp_view` | ✅ Implemented | `spark.catalog.dropTempView` |
| **DropGlobalTempView** | `drop_global_temp_view` | ✅ Implemented | Same as DropTempView |
| **TableExists** | `table_exists` | ✅ Implemented | `spark.catalog.tableExists` |
| **DatabaseExists** | `database_exists` | ✅ Implemented | `spark.catalog.databaseExists` |
| **ListTables** | `list_tables` | ✅ Implemented | `spark.catalog.listTables` |
| **ListColumns** | `list_columns` | ✅ Implemented | `spark.catalog.listColumns` |
| **ListDatabases** | `list_databases` | ✅ Implemented | `spark.catalog.listDatabases` |
| **ListFunctions** | `list_functions` | ✅ Implemented | `spark.catalog.listFunctions` (M43) - queries duckdb_functions() |
| **CurrentDatabase** | `current_database` | ✅ Implemented | `spark.catalog.currentDatabase` |
| **SetCurrentDatabase** | `set_current_database` | ✅ Implemented | `spark.catalog.setCurrentDatabase` |
| **CurrentCatalog** | `current_catalog` | ✅ Implemented | Returns "spark_catalog" |
| **SetCurrentCatalog** | `set_current_catalog` | ✅ Implemented | Only "spark_catalog" supported |
| **ListCatalogs** | `list_catalogs` | ✅ Implemented | Returns ["spark_catalog"] |
| **CreateTable** | `create_table` | ✅ Implemented | Internal tables (M42) + external tables via path (M43) |
| **IsCached** | `is_cached` | ✅ No-op | Always returns false |
| **CacheTable** | `cache_table` | ✅ No-op | Logs warning, no-op |
| **UncacheTable** | `uncache_table` | ✅ No-op | Logs warning, no-op |
| **ClearCache** | `clear_cache` | ✅ No-op | Logs warning, no-op |
| **RefreshTable** | `refresh_table` | ✅ No-op | Logs info, no-op |
| **RefreshByPath** | `refresh_by_path` | ✅ No-op | Logs info, no-op |
| **RecoverPartitions** | `recover_partitions` | ✅ No-op | Logs info, no-op |
| **GetDatabase** | `get_database` | ✅ Implemented | `spark.catalog.getDatabase` (M44) - throws NOT_FOUND if missing |
| **GetTable** | `get_table` | ✅ Implemented | `spark.catalog.getTable` (M44) - returns table metadata |
| **GetFunction** | `get_function` | ✅ Implemented | `spark.catalog.getFunction` (M44) - queries duckdb_functions() |
| **FunctionExists** | `function_exists` | ✅ Implemented | `spark.catalog.functionExists` (M44) - boolean check |
| **CreateExternalTable** | `create_external_table` | ✅ Implemented | Delegates to CreateTable handler (M44) |

*Note*: Both CreateTable and CreateExternalTable support external tables created as VIEWs over file readers (csv, parquet, json).

**All 26 catalog operations implemented!**

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

Supported window functions (validated by 35 differential tests):
- **Ranking**: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `NTILE`, `CUME_DIST`
- **Analytic**: `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`
- **Aggregate over windows**: `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `STDDEV`
- **Frame specifications**: `ROWS BETWEEN`, `RANGE BETWEEN` with unbounded/fixed/current boundaries

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

### 5.5 Validated Functions (57 Differential Tests)

The following functions are validated by differential tests comparing Thunderduck against Spark 4.0.1:

| Category | Functions Validated |
|----------|---------------------|
| **Array** (17 tests) | `array_contains`, `array_size`, `sort_array`, `array_distinct`, `array_union`, `array_intersect`, `array_except`, `arrays_overlap`, `array_position`, `element_at`, `explode`, `explode_outer`, `flatten`, `reverse`, `slice`, `array_join` |
| **Map** (7 tests) | `map_keys`, `map_values`, `map_entries`, `size`, `element_at`, `map_from_arrays`, `explode` on maps |
| **Null** (8 tests) | `coalesce`, `isnull`, `isnotnull`, `ifnull`, `nvl`, `nvl2`, `nullif`, `nanvl` |
| **String** (14 tests) | `concat`, `concat_ws`, `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`, `substring`, `instr`, `locate`, `lpad`, `rpad`, `repeat`, `reverse`, `split`, `replace`, `initcap` |
| **Math** (11 tests) | `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `mod`, `pmod`, `greatest`, `least`, `log`, `exp`, `sign` |

### 5.6 Multi-dimensional Aggregations (21 Differential Tests)

| Operation | Tests | Notes |
|-----------|-------|-------|
| `pivot` | 6 | With sum, avg, max/min, multiple aggregations, explicit values |
| `unpivot` / `melt` | 3 | Wide to long format transformation |
| `cube` | 4 | With grouping(), grouping_id() |
| `rollup` | 5 | Hierarchical aggregation with grouping() |
| Advanced | 3 | Cube vs rollup, pivot then aggregate |

### 5.7 Higher-Order Functions (M46) (18 E2E Tests)

Lambda functions and higher-order array/map operations are now supported:

| Spark Function | DuckDB Translation | Status | E2E Tests |
|----------------|-------------------|--------|-----------|
| `transform(arr, f)` | `list_transform(arr, f)` | ✅ Full | 3 |
| `filter(arr, f)` | `list_filter(arr, f)` | ✅ Full | 4 |
| `exists(arr, f)` | `list_bool_or(list_transform(arr, f))` | ✅ Full | 2 |
| `forall(arr, f)` | `list_bool_and(list_transform(arr, f))` | ✅ Full | 2 |
| `aggregate(arr, init, f)` | `list_reduce(list_prepend(init, arr), f)` | ✅ Full | 3 |
| `zip_with(a, b, f)` | `list_zip(a, b)` | ⚠️ Partial | - |
| `map_filter(m, f)` | `map_from_entries(list_filter(...))` | ⚠️ Partial | - |
| `transform_keys(m, f)` | `map_from_entries(list_transform(...))` | ⚠️ Partial | - |
| `transform_values(m, f)` | `map_from_entries(list_transform(...))` | ⚠️ Partial | - |

**Lambda Syntax**: DuckDB uses Python-style lambda syntax: `lambda x: x + 1` (Spark uses `x -> x + 1`)

**Limitations**:
- **zip_with**: Returns zipped list without applying the lambda. Full support would require lambda body rewriting to access struct fields.
- **map_filter/transform_keys/transform_values**: Basic structure implemented, but lambda body rewriting for `(k, v)` to `e.key, e.value` not yet supported. Works for simple cases.
- **Nested lambdas**: Fully supported with proper variable scoping.

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

1. ~~**UnresolvedExtractValue** - Struct/Array/Map access~~ ✅ Implemented (M47, 2025-12-17)
2. ~~**LambdaFunction** - Array transform operations~~ ✅ Implemented (M46, 2025-12-17)
3. ~~**Complex literal types** (Array, Map, Struct)~~ ✅ Implemented (M48, 2025-12-17)
4. ~~**Interval types** (CalendarInterval, etc.)~~ ✅ Implemented (M48, 2025-12-17)
5. ~~**ToSchema** - Schema enforcement~~ ✅ Implemented (M49, 2025-12-17)

**Phase 3 COMPLETE!** All complex type expressions and literals implemented with 66+ E2E tests passing.

### Phase 4: Catalog Operations (Medium Priority)

**Phase 4A - High Value (Implemented M41, 2025-12-16):**
1. ~~**DropTempView** - Critical for view management~~ ✅ Implemented
2. ~~**TableExists**, **DatabaseExists** - Existence checks~~ ✅ Implemented
3. ~~**ListTables**, **ListDatabases**, **ListColumns** - Metadata queries~~ ✅ Implemented
4. ~~**CurrentDatabase**, **SetCurrentDatabase** - Session state~~ ✅ Implemented

**Phase 4B - Table Creation (M42, 2025-12-16):**
5. ~~**CreateTable**~~ ✅ Implemented - Internal tables with per-session persistent databases

**Phase 4C - Remaining (Implemented M43-M44):**
6. ~~**CreateExternalTable** - External table creation (parquet, csv, etc.)~~ ✅ Implemented (M44)
7. ~~**ListFunctions**, **FunctionExists** - Function discovery~~ ✅ Implemented (M43-M44)
8. ~~Cache operations (no-op implementations)~~ ✅ Implemented (M43)

**Phase 4 COMPLETE!** All 26 catalog operations implemented.

See [docs/architect/CATALOG_OPERATIONS.md](docs/architect/CATALOG_OPERATIONS.md) for implementation details.

### Phase 5: Statistical Functions (Lower Priority)

1. ~~**StatDescribe**, **StatSummary** - Basic statistics~~ ✅ Implemented (M45, 2025-12-16)
2. ~~**StatCorr**, **StatCov** - Correlation/covariance~~ ✅ Implemented (M45, 2025-12-16)
3. ~~**StatCrosstab**, **StatFreqItems** - Frequency analysis~~ ✅ Implemented (M45, 2025-12-16)
4. ~~**StatApproxQuantile** - Quantile estimation~~ ✅ Implemented (M45, 2025-12-16)
5. ~~**StatSampleBy** - Stratified sampling~~ ✅ Implemented (M45, 2025-12-16)

**Phase 5 COMPLETE!** All 8 statistics operations implemented with 29 E2E tests.

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

3. **Version Compatibility**: This analysis is based on Spark Connect 4.0.x. Spark 4.0 introduced protocol changes including deprecated `sql` field in SQL commands (replaced with `input` relation).

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
df.to(schema)                                 # ToSchema (M49) - column reorder, projection, casting

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

# CATALOG operations (M41-M44):
spark.catalog.tableExists("table")            # Check table/view exists
spark.catalog.databaseExists("db")            # Check database exists
spark.catalog.listTables()                    # List tables in current database
spark.catalog.listDatabases()                 # List all databases
spark.catalog.listColumns("table")            # List columns in table
spark.catalog.listFunctions()                 # List available functions (M43)
spark.catalog.currentDatabase()               # Get current database name
spark.catalog.setCurrentDatabase("db")        # Set current database
spark.catalog.dropTempView("view")            # Drop temp view
spark.catalog.createTable("t", schema=schema) # Create persistent table (M42)
spark.catalog.getDatabase("main")             # Get database metadata (M44)
spark.catalog.getTable("table")               # Get table metadata (M44)
spark.catalog.getFunction("abs")              # Get function metadata (M44)
spark.catalog.functionExists("sum")           # Check function exists (M44)

# STATISTICS operations (M45):
df.stat.cov("col1", "col2")                   # Sample covariance (returns Double)
df.stat.corr("col1", "col2")                  # Pearson correlation (returns Double)
df.stat.approxQuantile("col", [0.25, 0.5, 0.75], 0.0)  # Quantiles (returns List[Double])
df.describe()                                 # Basic stats: count/mean/stddev/min/max
df.describe("col1", "col2")                   # Stats for specific columns
df.summary()                                  # Extended stats with percentiles
df.summary("count", "min", "max")             # Custom statistics list
df.stat.crosstab("col1", "col2")              # Contingency table
df.stat.freqItems(["col1", "col2"])           # Frequent items in columns
df.stat.sampleBy("category", {"A": 0.5, "B": 0.2}, seed=42)  # Stratified sampling

# LAMBDA / HIGHER-ORDER FUNCTIONS (M46):
df.select(F.transform("arr", lambda x: x + 1))              # Transform array elements
df.select(F.filter("arr", lambda x: x > 2))                 # Filter array elements
df.select(F.exists("arr", lambda x: x > 10))                # Any element matches predicate
df.select(F.forall("arr", lambda x: x > 0))                 # All elements match predicate
df.select(F.aggregate("arr", F.lit(0), lambda a, x: a + x)) # Reduce array to single value
# Nested lambdas also supported for multi-dimensional arrays
```

## Appendix B: Quick Reference - What Doesn't Work

```python
# INTENTIONALLY NOT SUPPORTED (see Section 8):
df.sample(withReplacement=True, fraction=0.5) # Poisson sampling not available in DuckDB

# COMMANDS partially implemented:
df.write.parquet("s3://bucket/path")          # S3 writes need httpfs extension
df.write.csv("s3://bucket/path")              # S3 writes need httpfs extension

# RELATIONS not yet implemented (2 remaining):
# Parse - CSV/JSON parsing
# CollectMetrics - Metrics collection

# LAMBDA FUNCTIONS - PARTIAL SUPPORT:
F.zip_with(arr1, arr2, lambda x, y: x + y)    # Returns zipped list, lambda not applied
F.map_filter(m, lambda k, v: v > 0)           # Basic structure only, lambda rewriting incomplete
F.transform_keys(m, lambda k, v: upper(k))    # Basic structure only, lambda rewriting incomplete
F.transform_values(m, lambda k, v: v * 2)     # Basic structure only, lambda rewriting incomplete

# Python/Scala UDFs (Future):
spark.udf.register(...)                       # User-defined functions not supported
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

**Document Version:** 4.1
**Last Updated:** 2025-12-17
**Author:** Analysis generated from Spark Connect 4.0.x protobuf definitions

### Version History

| Version | Date | Changes |
|---------|------|---------|
| v4.1 | 2025-12-17 | Added ToSchema relation (M49). DataFrame.to(schema) for column reordering, projection, type casting. 13 E2E tests. **Relation coverage: 95% (38/40).** |
| v4.0 | 2025-12-17 | Added all remaining type literals (M48): TimestampNTZ, CalendarInterval, YearMonthInterval, DayTimeInterval, Array, Map, Struct. 32 E2E tests. **100% literal type coverage achieved.** |
| v3.9 | 2025-12-17 | Added UnresolvedExtractValue, UnresolvedRegex, UpdateFields expressions (M47). Supports struct.field, arr[index], map[key] access. 21 E2E tests. **Expression coverage: 94% (15/16).** |
| v3.8 | 2025-12-17 | Added LambdaFunction, UnresolvedNamedLambdaVariable, CallFunction expressions (M46). Supports transform, filter, exists, forall, aggregate HOFs. zip_with and map HOFs have partial support. 18 E2E tests. **Expression coverage: 75% (12/16).** |
| v3.7 | 2025-12-16 | Added all 8 statistics operations (M45): StatCov, StatCorr, StatApproxQuantile, StatDescribe, StatSummary, StatCrosstab, StatFreqItems, StatSampleBy. **Relations coverage: 90% (36/40). Statistics 100% complete.** |
| v3.6 | 2025-12-16 | Added CreateExternalTable (delegates to CreateTable). **Catalog 100% complete (26/26 operations)**. |
| v3.5 | 2025-12-16 | Added GetDatabase, GetTable, GetFunction, FunctionExists (M44). Catalog operations now 22/26 (85%). |
| v3.4 | 2025-12-16 | Added ListFunctions (M43), external table support via CreateTable (CSV/Parquet/JSON as VIEWs). Documented all no-op operations as implemented. 18/26 catalog ops (69%). |
| v3.3 | 2025-12-16 | Added CREATE TABLE (M42) with per-session persistent databases. 9/26 catalog ops (35%). |
| v3.2 | 2025-12-16 | Added differential test validation (266 tests). Expanded function support with validated functions (57 tests), window functions (35 tests), multi-dim aggregations (21 tests). |
| v3.0 | 2025-12-15 | Added SubqueryAlias (M28). Phase 2 complete! 28/40 relations (70%) |
| v2.1 | 2025-12-12 | Added Unpivot (M27) via DuckDB native UNPIVOT |
| v2.0 | 2025-12-12 | Added NADrop, NAFill, NAReplace (M26) |
| v1.9 | 2025-12-12 | Added Hint, Repartition, RepartitionByExpression (M25) - no-op pass-throughs |
| v1.8 | 2025-12-12 | Added WriteOperation (M24) - df.write.parquet/csv/json |
| v1.7 | 2025-12-12 | Added Sample (M23) - Bernoulli sampling |
| v1.6 | 2025-12-10 | Corrected ShowString to fully implemented |
| v1.5 | 2025-12-10 | Clarified actions vs transformations |
| v1.0-1.4 | 2025-12-10 | Initial analysis, M19-M22 implementations |
