# Catalog Operations Architecture

**Version:** 1.0
**Date:** 2025-12-16
**Status:** Implementation in Progress

---

## Overview

This document describes how Thunderduck implements Spark Connect catalog operations by delegating to DuckDB's `information_schema` and system tables.

### Design Principles

1. **Delegate to DuckDB** - Use `information_schema` queries wherever possible
2. **Semantic Mapping** - Bridge Spark's catalog model to DuckDB's schema model
3. **No-op for Unsupported** - Cache/refresh operations return success without action
4. **Session Isolation** - Each Spark Connect session has isolated DuckDB database

---

## Namespace Model Mapping

### Spark vs DuckDB Hierarchy

```
Spark:    Catalog  >  Database  >  Table
DuckDB:   Database >  Schema    >  Table
```

### Mapping Strategy

| Spark Concept | DuckDB Equivalent | Notes |
|---------------|-------------------|-------|
| Catalog | Hardcoded `"default"` | DuckDB has no catalog level |
| Database | Schema | `information_schema.schemata` |
| Table | Table/View | `information_schema.tables` |
| Temp View | Non-temp View | Session-scoped via naming |
| Global Temp View | Not supported | Maps to regular temp view |

---

## Operation Categories

### Category 1: Direct SQL Delegation

These operations map directly to DuckDB information_schema queries:

| Operation | DuckDB Query | Response Type |
|-----------|--------------|---------------|
| `ListTables` | `SELECT FROM information_schema.tables` | Arrow Table |
| `ListColumns` | `SELECT FROM information_schema.columns` | Arrow Table |
| `ListDatabases` | `SELECT FROM information_schema.schemata` | Arrow Table |
| `TableExists` | `SELECT EXISTS(... FROM information_schema.tables)` | Boolean |
| `DatabaseExists` | `SELECT EXISTS(... FROM information_schema.schemata)` | Boolean |
| `GetTable` | `SELECT FROM information_schema.tables WHERE name = ?` | Arrow Table |
| `GetDatabase` | `SELECT FROM information_schema.schemata WHERE name = ?` | Arrow Table |
| `ListFunctions` | `SELECT FROM duckdb_functions()` | Arrow Table |

### Category 2: Session State

These operations manage session-level state:

| Operation | Implementation | Notes |
|-----------|----------------|-------|
| `CurrentDatabase` | Return session's current schema | Default: `"main"` |
| `SetCurrentDatabase` | `SET search_path TO schema` | Validates schema exists |
| `CurrentCatalog` | Return `"default"` | Hardcoded |
| `SetCurrentCatalog` | Validate and store in session | Only `"default"` supported |

### Category 3: View Management

| Operation | Implementation | Notes |
|-----------|----------------|-------|
| `DropTempView` | `DROP VIEW IF EXISTS name` + remove from session registry | Returns true if existed |
| `DropGlobalTempView` | Same as DropTempView | No global concept in DuckDB |
| `CreateDataFrameView` | Already implemented | Uses non-temp views |

### Category 4: No-op Operations

These operations are accepted but perform no action (DuckDB doesn't support these concepts):

| Operation | Behavior | Return Value |
|-----------|----------|--------------|
| `CacheTable` | Log warning, return success | void |
| `UncacheTable` | Log warning, return success | void |
| `ClearCache` | Log warning, return success | void |
| `IsCached` | Always return false | false |
| `RefreshTable` | No-op | void |
| `RefreshByPath` | No-op | void |
| `RecoverPartitions` | No-op | void |

### Category 5: Table Creation

| Operation | Implementation | Notes |
|-----------|----------------|-------|
| `CreateTable` | Convert schema to `CREATE TABLE` DDL | Future work |
| `CreateExternalTable` | Create table with file path | Future work |

---

## SQL Query Mappings

### ListTables

```sql
SELECT
    table_name as name,
    'default' as catalog,
    ARRAY[table_schema] as namespace,
    '' as description,
    CASE
        WHEN table_type = 'VIEW' THEN 'VIEW'
        WHEN table_type = 'BASE TABLE' THEN 'MANAGED'
        ELSE table_type
    END as tableType,
    CASE
        WHEN table_name LIKE 'temp_%' THEN true
        ELSE false
    END as isTemporary
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
  AND (? IS NULL OR table_name LIKE ?)  -- pattern filter
  AND (? IS NULL OR table_schema = ?)    -- database filter
ORDER BY table_schema, table_name
```

### ListColumns

```sql
SELECT
    column_name as name,
    '' as description,
    data_type as dataType,
    CASE WHEN is_nullable = 'YES' THEN true ELSE false END as nullable,
    false as isPartition,
    false as isBucket
FROM information_schema.columns
WHERE table_name = ?
  AND (? IS NULL OR table_schema = ?)
ORDER BY ordinal_position
```

### ListDatabases

```sql
SELECT
    schema_name as name,
    'default' as catalog,
    '' as description,
    '' as locationUri
FROM information_schema.schemata
WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
  AND (? IS NULL OR schema_name LIKE ?)  -- pattern filter
ORDER BY schema_name
```

### TableExists

```sql
SELECT EXISTS(
    SELECT 1 FROM information_schema.tables
    WHERE table_name = ?
      AND table_schema NOT IN ('information_schema', 'pg_catalog')
      AND (? IS NULL OR table_schema = ?)
)
```

### DatabaseExists

```sql
SELECT EXISTS(
    SELECT 1 FROM information_schema.schemata
    WHERE schema_name = ?
      AND schema_name NOT IN ('information_schema', 'pg_catalog')
)
```

### ListFunctions

```sql
SELECT
    function_name as name,
    'default' as catalog,
    ARRAY[''] as namespace,
    description,
    '' as className,
    false as isTemporary
FROM duckdb_functions()
WHERE (? IS NULL OR function_name LIKE ?)  -- pattern filter
ORDER BY function_name
```

---

## Response Schema Definitions

### ListTables Response

| Column | Type | Description |
|--------|------|-------------|
| name | STRING | Table/view name |
| catalog | STRING | Always "default" |
| namespace | ARRAY<STRING> | Schema path |
| description | STRING | Empty (DuckDB has no descriptions) |
| tableType | STRING | "MANAGED", "VIEW", "EXTERNAL" |
| isTemporary | BOOLEAN | True for temp views |

### ListColumns Response

| Column | Type | Description |
|--------|------|-------------|
| name | STRING | Column name |
| description | STRING | Empty |
| dataType | STRING | SQL type name |
| nullable | BOOLEAN | Is nullable |
| isPartition | BOOLEAN | Always false |
| isBucket | BOOLEAN | Always false |

### ListDatabases Response

| Column | Type | Description |
|--------|------|-------------|
| name | STRING | Schema name |
| catalog | STRING | Always "default" |
| description | STRING | Empty |
| locationUri | STRING | Empty |

---

## Temp View Management

### Current Implementation

Thunderduck uses **non-temporary** views in DuckDB because:

1. DuckDB temp views are connection-scoped
2. Different requests may use different connections from pool
3. Non-temp views are visible across all connections to same database

### Naming Convention

```
User view name: "my_view"
DuckDB view name: "my_view" (stored in session registry)
```

### Session Registry

```java
// Session.java
Map<String, LogicalPlan> tempViews = new ConcurrentHashMap<>();

// Check if view is "temporary" (session-managed)
boolean isTemporary = session.getTempViews().containsKey(viewName);
```

### Cleanup

On session close:
1. Iterate through `tempViews` registry
2. Execute `DROP VIEW IF EXISTS` for each
3. Clear registry

---

## Implementation Location

| Component | File | Purpose |
|-----------|------|---------|
| Catalog Handler | `SparkConnectServiceImpl.java` | Main switch statement |
| Result Streaming | `StreamingResultHandler.java` | Arrow response formatting |
| Session State | `Session.java` | View registry, current database |
| Query Execution | `QueryExecutor.java` | Execute information_schema queries |

---

## Error Handling

| Scenario | Error Code | Message |
|----------|------------|---------|
| Table not found | NOT_FOUND | "Table 'name' not found" |
| Database not found | NOT_FOUND | "Database 'name' not found" |
| Invalid pattern | INVALID_ARGUMENT | "Invalid pattern: ..." |
| Permission denied | PERMISSION_DENIED | DuckDB error passthrough |

---

## Testing Strategy

### Unit Tests

Test SQL generation for each operation:
- Correct column selection
- Pattern escaping
- Schema filtering

### Integration Tests (Differential)

Compare against Spark 4.0.1:
```python
# test_catalog_operations.py
def test_list_tables(spark_session, thunderduck_session):
    # Create test tables
    spark_session.sql("CREATE TABLE test1 (id INT)")
    thunderduck_session.sql("CREATE TABLE test1 (id INT)")

    # Compare catalog results
    spark_tables = spark_session.catalog.listTables()
    td_tables = thunderduck_session.catalog.listTables()

    assert_dataframes_equal(spark_tables, td_tables)
```

---

## Implementation Phases

### Phase 4A: High Priority (Current)

1. DropTempView - polish existing
2. TableExists - simple EXISTS query
3. ListTables - information_schema.tables
4. ListColumns - information_schema.columns
5. ListDatabases - information_schema.schemata
6. DatabaseExists - simple EXISTS query

### Phase 4B: Medium Priority

7. CurrentDatabase / SetCurrentDatabase
8. GetTable / GetDatabase
9. ListFunctions / FunctionExists
10. CurrentCatalog / SetCurrentCatalog

### Phase 4C: No-ops

11. Cache operations (no-op)
12. Refresh operations (no-op)

### Phase 4D: Future

13. CreateTable / CreateExternalTable

---

## References

- [Spark Catalog API](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.html)
- [DuckDB Information Schema](https://duckdb.org/docs/sql/information_schema)
- [Spark Connect Protocol - catalog.proto](../../connect-server/src/main/proto/spark/connect/catalog.proto)
- [Gap Analysis](../../CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md)

---

**Last Updated:** 2025-12-16
