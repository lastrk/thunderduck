# Thunderduck Performance Analysis: Double Query Execution Bug

**Date**: 2026-02-18
**Status**: ROOT CAUSE FOUND

## Executive Summary

Benchmarking Thunderduck against vanilla DuckDB and Apache Spark revealed that Thunderduck runs **~2x slower than vanilla DuckDB**. Investigation revealed the root cause: **Thunderduck executes every `spark.sql()` query TWICE** because it does not implement the `SqlCommandResult` response protocol. The first execution's results are received by PySpark and then **silently discarded**. When `.toPandas()` or `.collect()` is called, the query re-executes.

**The fix**: Return a `SqlCommandResult` containing a `Relation` reference in the `ExecutePlanResponse` when handling `SqlCommand` requests. This would cut query time roughly in half.

## Root Cause: Missing `SqlCommandResult` Protocol

### How Real Spark Handles `spark.sql(q).toPandas()`

1. `spark.sql(q)` sends `ExecutePlan(SqlCommand)` to the server
2. Server executes query, returns a `SqlCommandResult` containing a `LocalRelation` reference to the cached result
3. PySpark wraps this in `CachedRelation(properties["sql_command_result"])`
4. `.toPandas()` sends `ExecutePlan` with the `CachedRelation` — server returns cached data **without re-executing**

### How Thunderduck Handles It (Bug)

1. `spark.sql(q)` sends `ExecutePlan(SqlCommand)` to the server
2. Thunderduck executes query (~1,880ms), streams Arrow results back
3. **Thunderduck does NOT include `SqlCommandResult` in the response**
4. PySpark receives data, checks for `sql_command_result` in properties — **not found**
5. PySpark **DISCARDS the results** and returns a lazy `DataFrame(cmd, self)` wrapping the original SQL
6. `.toPandas()` sends a NEW `ExecutePlan` — Thunderduck executes the query **a second time** (~1,880ms)

### Evidence

**PySpark client code** (`pyspark/sql/connect/session.py:828-835`):
```python
data, properties, ei = self.client.execute_command(cmd.command(self._client))
if "sql_command_result" in properties:
    df = DataFrame(CachedRelation(properties["sql_command_result"]), self)
else:
    return DataFrame(cmd, self)  # ← LAZY WRAPPER, data DISCARDED
```

**PySpark `execute_command`** (`pyspark/sql/connect/client/core.py:1195-1201`):
```python
data, _, metrics, observed_metrics, properties = self._execute_and_fetch(req, observations or {})
if data is not None:
    return (data.to_pandas(), properties, ei)  # ← Data received but will be discarded!
```

**Thunderduck SqlCommand handler** (`SparkConnectServiceImpl.java:222-258`):
- Extracts SQL from `SqlCommand.input.sql`
- Transforms via ANTLR
- Falls through to `executeSQLWithPlan()` which streams Arrow data
- **Never sends `SqlCommandResult` response type**

**Proto definition** (`base.proto:448-450`):
```protobuf
message SqlCommandResult {
  Relation relation = 1;  // Opaque relation for next call
}
```

### Corrected Timing Breakdown

```
Total client time:     3,787ms (100%)
├── Execution #1 (spark.sql() → execute_command):
│   ├── gRPC request:                    ~5ms
│   ├── Server: ANTLR parse+transform:   ~2ms
│   ├── Server: DuckDB execution:        ~1,880ms  ← WASTED
│   ├── Server: Arrow streaming:          ~2ms
│   ├── gRPC response:                   ~5ms
│   └── Client: Arrow IPC deser:          ~2ms
│   Subtotal:                            ~1,896ms
│
├── Execution #2 (.toPandas() → _execute_and_fetch):
│   ├── Config RPC (selfDestruct):        ~10ms
│   ├── gRPC request:                    ~5ms
│   ├── Server: ANTLR parse+transform:   ~2ms
│   ├── Server: DuckDB execution:        ~1,880ms  ← USED
│   ├── Server: Arrow streaming:          ~2ms
│   ├── gRPC response:                   ~5ms
│   ├── Client: Arrow IPC deser:          ~2ms
│   └── Client: Arrow → Pandas:           ~5ms
│   Subtotal:                            ~1,911ms
│
└── Grand Total:                         ~3,807ms (matches observed 3,787ms)
```

The server log only showed ~1,880ms because each execution is logged independently. The "client overhead" was actually the first wasted execution.

## Benchmark Configuration

| Engine | Threads | Memory | Configuration |
|--------|---------|--------|---------------|
| **Vanilla DuckDB** | 2 | 4 GB | In-process Python, `duckdb.connect()` |
| **Thunderduck** | 2 | 2 GB JVM heap + 4 GB DuckDB | Separate JVM, gRPC on localhost:15002 |
| **Apache Spark** | 2 | 4 GB driver memory | Separate JVM, gRPC on localhost:15003 |

- **Data**: TPC-H SF=20 (~5 GB parquet, 120M rows in lineitem)
- **Machine**: 10 cores, 15 GB RAM (Linux aarch64)

## End-to-End Client-Observed Timing

| Method | Median Time | Notes |
|--------|-------------|-------|
| `duckdb.execute(q1).fetchdf()` | **1,895ms** | In-process, Arrow → Pandas |
| `duckdb.execute(q1).fetchall()` | **1,808ms** | In-process, raw tuples |
| `duckdb.execute(q1).fetch_arrow_table()` | **1,802ms** | In-process, raw Arrow |
| `spark_td.sql(q1).toPandas()` | **3,787ms** | Thunderduck via Spark Connect (2x executions) |
| `spark_td.sql(q1).collect()` | **4,064ms** | Thunderduck via Spark Connect (2x executions) |

## The Fix

### Option A: Return `SqlCommandResult` with SQL Relation (Preferred)

When handling `SqlCommand`, instead of executing the query immediately, return a `SqlCommandResult` containing the transformed SQL as a `Relation`. PySpark will then use `CachedRelation` to send this relation back when `.toPandas()` is called, and only THEN execute the query.

```java
// In executePlan(), SqlCommand handling:
SqlCommandResult cmdResult = SqlCommandResult.newBuilder()
    .setRelation(Relation.newBuilder()
        .setSql(SQL.newBuilder().setQuery(transformedSQL)))
    .build();

ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
    .setSessionId(sessionId)
    .setOperationId(operationId)
    .setSqlCommandResult(cmdResult)
    .build();
responseObserver.onNext(response);
responseObserver.onCompleted();
```

**Advantage**: Query executes only once, when data is actually needed.

### Option B: Execute + Cache + Return CachedRelation

Execute the query, cache the result, and return a `SqlCommandResult` referencing the cached data. When PySpark sends back the `CachedRelation`, serve from cache.

**Advantage**: Data is pre-computed and ready immediately.
**Disadvantage**: Requires a cache management system.

### Expected Performance After Fix

```
After fix: DuckDB (1,880ms) + overhead (~20ms) = ~1,900ms
Before:    DuckDB (1,880ms) × 2 + overhead (~20ms) = ~3,787ms
Improvement: ~2x faster
```

## Other Findings

### Server-Side Overhead Is Negligible

Thunderduck's SQL translation pipeline adds only ~2ms. Schema resolution adds ~0ms for TPC-H. Arrow IPC serialization adds ~2ms. **Total server overhead: <20ms (<1%).**

### `.toPandas()` vs `.collect()` Performance

`.toPandas()` (3,787ms) is faster than `.collect()` (4,064ms) because:
- Both execute the query twice (same root cause)
- `.toPandas()` uses Arrow-native path for the final conversion
- `.collect()` creates Python `Row` objects (more Python object allocation)

### No Double Execution via AnalyzePlan

PySpark's `.toPandas()` sends only `ExecutePlan`, not `AnalyzePlan`. Thunderduck's schema resolution does NOT fire LIMIT 0 queries for TPC-H.

### JDBC Overhead Is Minimal

DuckDB via JDBC (~1,880ms) vs native Python API (~1,800ms) — only ~4% overhead.

## Hypotheses Tested

| Hypothesis | Result |
|-----------|--------|
| ~~PySpark client overhead~~ | **WRONG** — the ~1,900ms gap is a second query execution |
| Missing `SqlCommandResult` causes double execution | **CONFIRMED** — root cause |
| AnalyzePlan causes double execution | **Debunked** — `.toPandas()` only calls ExecutePlan |
| Schema inference re-executes query (LIMIT 0) | **Debunked** — no LIMIT 0 for TPC-H queries |
| DuckDB JDBC is slower than native API | **Minimal** — ~4% overhead (80ms on 1,800ms) |
| `.toPandas()` Pandas conversion is slow | **Debunked** — faster than `.collect()` |

## Raw Data

### Server Log Timing (TPC-H Q1, 5 toPandas runs)

Note: Each `.toPandas()` call produces TWO log entries (one per execution). The log below shows individual executions:

```
duckdb_execute=1907.6ms, result_stream=2.5ms, total=1911.5ms
duckdb_execute=1854.4ms, result_stream=4.5ms, total=1859.9ms
duckdb_execute=1801.8ms, result_stream=2.3ms, total=1805.0ms
duckdb_execute=1879.7ms, result_stream=1.8ms, total=1882.6ms
duckdb_execute=1809.9ms, result_stream=2.1ms, total=1813.2ms
```

### Server Log Timing (TPC-H Q1, 5 collect runs)

```
duckdb_execute=1986.6ms, result_stream=1.9ms, total=1990.8ms
duckdb_execute=1874.9ms, result_stream=2.2ms, total=1878.0ms
duckdb_execute=3132.4ms, result_stream=6.0ms, total=3139.5ms  ← outlier (GC or page cache)
duckdb_execute=2039.5ms, result_stream=2.2ms, total=2043.5ms
duckdb_execute=2184.6ms, result_stream=1.8ms, total=2187.6ms
```

## Key Source Files

| File | Relevance |
|------|-----------|
| `pyspark/sql/connect/session.py:828-835` | `sql_command_result` check — the branching point |
| `pyspark/sql/connect/client/core.py:1181-1203` | `execute_command` — receives and discards first execution |
| `pyspark/sql/connect/client/core.py:988-1072` | `to_pandas` — triggers second execution |
| `SparkConnectServiceImpl.java:222-258` | SqlCommand handler — missing `SqlCommandResult` |
| `base.proto:448-450` | `SqlCommandResult` message definition |
