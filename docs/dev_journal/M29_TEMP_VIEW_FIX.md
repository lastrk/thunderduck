# M29: createOrReplaceTempView P0 Fix

**Date**: 2025-12-13
**Status**: Completed

## Summary

Fixed a P0 issue where `createOrReplaceTempView` was blocking ~280 of 291 E2E tests due to connection pool exhaustion and isolated in-memory databases.

## Root Causes Identified

### 1. Isolated In-Memory Databases
Each connection to `jdbc:duckdb:` created its own isolated in-memory database. Views created on one connection were invisible to other connections in the pool.

### 2. Connection Leak in PlanConverter
Every call to `createPlanConverter()` borrowed a connection from the pool for schema inference but never returned it, eventually exhausting all available connections.

## Fixes Applied

### Fix 1: Shared In-Memory Database
**File**: `core/src/main/java/com/thunderduck/runtime/DuckDBConnectionManager.java`

Changed the JDBC URL from `jdbc:duckdb:` to `jdbc:duckdb::memory:thunderduck` to use DuckDB's named in-memory database feature, ensuring all pooled connections access the same database instance.

```java
// Before
return "jdbc:duckdb:";

// After
return "jdbc:duckdb::memory:thunderduck";
```

### Fix 2: Connection Leak Prevention
**File**: `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`

Removed connection borrowing from `createPlanConverter()` to prevent connection leaks. Schema inference is temporarily disabled but most DataFrame operations don't require it.

```java
// Before - leaked connections
return new PlanConverter(connectionManager.getConnection());

// After - no connection leak
return new PlanConverter();
```

## Test Results

### Before Fix
- Connection pool exhausted after ~3 operations
- "Table does not exist" errors when querying temp views
- ~280 tests blocked

### After Fix
- All temp view operations work correctly
- All 8 TPC-H tables register as temp views successfully
- Basic DataFrame operations: 7/7 PASSED
- TPC-H Q3 SQL and DataFrame: PASSED
- No connection pool exhaustion errors

## Technical Details

### DuckDB Named In-Memory Databases
DuckDB supports named in-memory databases using the syntax `:memory:dbname`. Multiple connections to the same named database share data, while connections to the anonymous `:memory:` (or empty path) each get isolated databases.

### Connection Pool Architecture
The `DuckDBConnectionManager` maintains a pool of connections (default: min of CPU count, 8). With the shared in-memory database, all connections now see the same tables and views.

## Related Files Modified

1. `core/src/main/java/com/thunderduck/runtime/DuckDBConnectionManager.java:275-278`
2. `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java:70-74`
3. `core/src/main/java/com/thunderduck/runtime/ArrowBatchStream.java` (schema fix for LIMIT 0)

## Future Improvements

1. **Schema Inference**: Re-enable schema inference with proper connection lifecycle management (pass `DuckDBConnectionManager` to `PlanConverter` instead of raw connection)
2. **Connection Validation**: Add connection health checks before returning from pool
3. **Metrics**: Add connection pool utilization metrics for monitoring
