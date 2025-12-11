# Arrow Streaming Architecture Design

**Version**: 1.0
**Date**: 2025-12-11
**Status**: Draft

## Executive Summary

This document describes an optimal target architecture for Thunderduck that enables **zero-copy Arrow pass-through** from DuckDB to Spark Connect clients. The current architecture materializes entire result sets in Java heap memory before conversion to Arrow format. The target architecture streams Arrow batches directly from DuckDB's native Arrow export interface to gRPC responses, minimizing memory usage and improving latency.

---

## 1. Current Architecture Analysis

### 1.1 Data Flow (Current)

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   DuckDB    │───>│  JDBC ResultSet │───>│ List<List<Object>│───>│ VectorSchemaRoot│
│  (executes) │    │   (row-by-row)  │    │ (full materialize│    │  (Arrow format) │
└─────────────┘    └─────────────────┘    └──────────────────┘    └─────────────────┘
                                                                          │
                                                                          ▼
┌─────────────┐    ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PySpark   │<───│  gRPC Response  │<───│  ArrowStreamWriter│<───│ Serialize to IPC│
│   Client    │    │  (ArrowBatch)   │    │  (schema+data)   │    │   byte array    │
└─────────────┘    └─────────────────┘    └──────────────────┘    └─────────────────┘
```

### 1.2 Current Implementation Details

**File**: `core/src/main/java/com/thunderduck/runtime/ArrowInterchange.java:55-97`

```java
// Current approach - FULL MATERIALIZATION
public static VectorSchemaRoot fromResultSet(ResultSet rs) throws SQLException {
    // Build schema from metadata...

    // PROBLEM: Materialize ALL rows into Java heap
    List<List<Object>> rows = new ArrayList<>();
    while (rs.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            row.add(rs.getObject(i));
        }
        rows.add(row);
    }

    // Then convert to Arrow format
    root.setRowCount(rows.size());
    for (int col = 0; col < columnCount; col++) {
        FieldVector vector = root.getVector(col);
        for (int row = 0; row < rows.size(); row++) {
            setVectorValue(vector, row, rows.get(row).get(col));
        }
    }
    return root;
}
```

**File**: `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java:1052-1113`

```java
// Current streaming - single batch, already materialized
private void streamArrowResults(VectorSchemaRoot root, ...) {
    // Serialize entire VectorSchemaRoot to bytes
    ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, ...);
    writer.start();
    writer.writeBatch();  // Single batch with ALL data
    writer.end();

    // Send as single gRPC response
    responseObserver.onNext(response);
    responseObserver.onCompleted();
}
```

### 1.3 Current Architecture Problems

| Issue | Impact | Details |
|-------|--------|---------|
| **Double Materialization** | 2x memory usage | Data materialized in `List<List<Object>>`, then copied to Arrow vectors |
| **No Streaming** | High latency | Client waits for entire result before receiving first row |
| **Java Object Overhead** | Memory bloat | Each cell wrapped in Java object (boxing overhead) |
| **Row-by-Row JDBC** | CPU overhead | rs.next() + getObject() per cell, not vectorized |
| **Large GC Pressure** | Latency spikes | Large temporary allocations cause GC pauses |

### 1.4 Memory Profile (Example: 1M rows × 10 columns)

| Stage | Memory Usage |
|-------|--------------|
| DuckDB internal | ~80MB (columnar, compressed) |
| JDBC ResultSet buffer | ~100MB |
| `List<List<Object>>` | ~400MB (object overhead) |
| Arrow VectorSchemaRoot | ~80MB (columnar) |
| Arrow IPC bytes | ~85MB |
| **Total Peak** | **~665MB** |

---

## 2. Target Architecture

### 2.1 Zero-Copy Arrow Pass-Through

```
┌─────────────┐    ┌─────────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DuckDB    │───>│ DuckDBResultSet     │───>│   ArrowReader   │───>│ VectorSchemaRoot│
│  (executes) │    │ .arrowExportStream()│    │ .loadNextBatch()│    │  (batch N)      │
└─────────────┘    └─────────────────────┘    └─────────────────┘    └────────┬────────┘
                                                                              │ ZERO COPY
                                                                              ▼
┌─────────────┐    ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PySpark   │<───│  gRPC Response  │<───│ ArrowStreamWriter│<───│ Serialize batch │
│   Client    │    │  (ArrowBatch N) │    │  (batch only)    │    │   to IPC bytes  │
└─────────────┘    └─────────────────┘    └──────────────────┘    └─────────────────┘
        ▲                                                                      │
        │              STREAMING - Multiple batches sent progressively         │
        └──────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Design Principles

1. **Direct Arrow Export**: Use `DuckDBResultSet.arrowExportStream()` to get native Arrow batches
2. **Batch Streaming**: Stream each batch to client as soon as it's ready
3. **Memory Bounded**: Only one batch (configurable size) in memory at a time
4. **Schema Once**: Send schema in first batch, subsequent batches are data-only
5. **Back-pressure Aware**: Respect gRPC flow control

### 2.3 Target Memory Profile (Same 1M rows × 10 columns)

| Stage | Memory Usage |
|-------|--------------|
| DuckDB internal | ~80MB (columnar, compressed) |
| Arrow batch (8192 rows) | ~0.7MB per batch |
| Arrow IPC buffer | ~0.8MB |
| **Peak Memory** | **~82MB** (vs 665MB current) |

**Memory Reduction: ~87%**

---

## 3. Component Design

### 3.1 New Components

#### 3.1.1 `ArrowStreamingExecutor`

Replaces direct use of `QueryExecutor` + `ArrowInterchange` for streaming results.

```java
package com.thunderduck.runtime;

/**
 * Executes queries and streams results as Arrow batches.
 * Uses DuckDB's native Arrow export for zero-copy streaming.
 */
public class ArrowStreamingExecutor implements AutoCloseable {

    private final DuckDBConnectionManager connectionManager;
    private final RootAllocator allocator;
    private final int batchSize;  // Default: 8192 rows

    /**
     * Execute query and return streaming Arrow reader.
     * Caller must close the reader when done.
     */
    public ArrowBatchStream executeQueryStreaming(String sql) throws SQLException {
        PooledConnection pooled = connectionManager.borrowConnection();
        Statement stmt = pooled.get().createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        // Cast to DuckDB-specific ResultSet for Arrow export
        DuckDBResultSet duckRS = rs.unwrap(DuckDBResultSet.class);
        ArrowReader reader = (ArrowReader) duckRS.arrowExportStream(allocator, batchSize);

        return new ArrowBatchStream(reader, pooled, stmt, rs);
    }
}
```

#### 3.1.2 `ArrowBatchStream`

Wraps the ArrowReader and manages resource lifecycle.

```java
package com.thunderduck.runtime;

/**
 * Streaming Arrow batch iterator with resource management.
 */
public class ArrowBatchStream implements AutoCloseable, Iterator<VectorSchemaRoot> {

    private final ArrowReader reader;
    private final PooledConnection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private boolean hasNext = true;
    private long totalRows = 0;

    @Override
    public boolean hasNext() {
        if (!hasNext) return false;
        try {
            hasNext = reader.loadNextBatch();
            return hasNext;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Arrow batch", e);
        }
    }

    @Override
    public VectorSchemaRoot next() {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        totalRows += batch.getRowCount();
        return batch;  // Caller must NOT close - owned by reader
    }

    public Schema getSchema() {
        return reader.getVectorSchemaRoot().getSchema();
    }

    @Override
    public void close() {
        // Close in reverse order
        try { reader.close(); } catch (Exception e) { /* log */ }
        try { resultSet.close(); } catch (Exception e) { /* log */ }
        try { statement.close(); } catch (Exception e) { /* log */ }
        connection.close();  // Returns to pool
    }
}
```

#### 3.1.3 `StreamingResultHandler`

Handles streaming results to gRPC client with batching.

```java
package com.thunderduck.connect.service;

/**
 * Streams Arrow batches to gRPC response observer.
 */
public class StreamingResultHandler {

    private final StreamObserver<ExecutePlanResponse> responseObserver;
    private final String sessionId;
    private final String operationId;
    private int batchIndex = 0;

    /**
     * Stream all batches from ArrowBatchStream to client.
     */
    public void streamResults(ArrowBatchStream stream) throws IOException {
        try (stream) {
            while (stream.hasNext()) {
                VectorSchemaRoot batch = stream.next();
                streamBatch(batch);
                batchIndex++;
            }

            // Send completion marker
            sendResultComplete();
        }
    }

    private void streamBatch(VectorSchemaRoot batch) throws IOException {
        // Serialize batch to Arrow IPC format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(batch, null,
                Channels.newChannel(out))) {
            if (batchIndex == 0) {
                writer.start();  // Schema only in first batch
            }
            writer.writeBatch();
        }

        // Build gRPC response
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setResponseId(UUID.randomUUID().toString())
            .setArrowBatch(ArrowBatch.newBuilder()
                .setRowCount(batch.getRowCount())
                .setData(ByteString.copyFrom(out.toByteArray()))
                .build())
            .build();

        responseObserver.onNext(response);
    }

    private void sendResultComplete() {
        ExecutePlanResponse complete = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setResponseId(UUID.randomUUID().toString())
            .setResultComplete(ResultComplete.newBuilder().build())
            .build();

        responseObserver.onNext(complete);
        responseObserver.onCompleted();
    }
}
```

### 3.2 Modified Components

#### 3.2.1 `SparkConnectServiceImpl.executeSQL()`

```java
// BEFORE (current)
private void executeSQL(String sql, String sessionId,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
    QueryExecutor executor = new QueryExecutor(connectionManager);
    VectorSchemaRoot results = executor.executeQuery(sql);  // FULL MATERIALIZATION
    streamArrowResults(results, sessionId, operationId, responseObserver);
    results.close();
}

// AFTER (streaming)
private void executeSQL(String sql, String sessionId,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
    ArrowStreamingExecutor executor = new ArrowStreamingExecutor(connectionManager);
    try (ArrowBatchStream stream = executor.executeQueryStreaming(sql)) {
        StreamingResultHandler handler = new StreamingResultHandler(
            responseObserver, sessionId, operationId);
        handler.streamResults(stream);  // STREAMING - batch by batch
    }
}
```

#### 3.2.2 `DuckDBConnectionManager.createConnection()`

Add streaming configuration to connection setup:

```java
private DuckDBConnection createConnection() throws SQLException {
    Properties props = new Properties();
    // Enable JDBC streaming mode for large results
    props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");

    Connection conn = DriverManager.getConnection(jdbcUrl, props);
    DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);

    // Existing configuration...
    try (Statement stmt = duckConn.createStatement()) {
        stmt.execute(String.format("SET memory_limit='%s'", ...));
        // ...
    }

    return duckConn;
}
```

---

## 4. Interface Design

### 4.1 New Interfaces

```java
/**
 * Represents a stream of Arrow batches with metadata.
 */
public interface ArrowBatchIterator extends Iterator<VectorSchemaRoot>, AutoCloseable {

    /** Get the schema (available before first batch) */
    Schema getSchema();

    /** Total rows streamed so far */
    long getTotalRowCount();

    /** Number of batches streamed so far */
    int getBatchCount();
}

/**
 * Executes queries with streaming results.
 */
public interface StreamingQueryExecutor {

    /** Execute query and return streaming iterator */
    ArrowBatchIterator executeStreaming(String sql) throws SQLException;

    /** Execute query with custom batch size */
    ArrowBatchIterator executeStreaming(String sql, int batchSize) throws SQLException;
}
```

### 4.2 Configuration

```java
public class StreamingConfig {

    /** Default batch size in rows (aligned with DuckDB row group size) */
    public static final int DEFAULT_BATCH_SIZE = 8192;

    /** Maximum batch size (prevents excessive memory per batch) */
    public static final int MAX_BATCH_SIZE = 65536;

    /** Minimum batch size (prevents too many small batches) */
    public static final int MIN_BATCH_SIZE = 1024;

    /** Buffer size for streaming (from DuckDB config) */
    public static final String STREAMING_BUFFER_SIZE = "976.5KiB";
}
```

---

## 5. Data Flow Comparison

### 5.1 Query: SELECT * FROM large_table (1M rows)

**Current Flow**:
```
Time ─────────────────────────────────────────────────────────────────>
│
│ DuckDB    [████████████████████]  execute + materialize JDBC
│ Java      [                    ████████████████]  List<List<Object>>
│ Arrow     [                                    ████████]  VectorSchemaRoot
│ IPC       [                                            ███]  serialize
│ gRPC      [                                               █]  single response
│ Client    [                                                █]  receive all
│
└── Latency: 5000ms total, 5000ms to first row
```

**Target Flow (Streaming)**:
```
Time ─────────────────────────────────────────────────────────────────>
│
│ DuckDB    [██]  batch 1
│ Arrow     [  █]
│ gRPC      [   █]  ◄── First batch to client!
│ Client    [    █]
│
│ DuckDB    [    ██]  batch 2
│ Arrow     [      █]
│ gRPC      [       █]
│ Client    [        █]
│
│           ... (122 batches for 1M rows @ 8192 rows/batch)
│
│ DuckDB    [                                               ██]  batch N
│ gRPC      [                                                  █]  complete
│
└── Latency: 5000ms total, 50ms to first row
```

### 5.2 Memory Comparison

| Phase | Current | Streaming | Reduction |
|-------|---------|-----------|-----------|
| Peak during query | 665 MB | 82 MB | 87% |
| Per-batch overhead | N/A | 0.8 MB | - |
| GC pressure | HIGH | LOW | - |

---

## 6. Error Handling

### 6.1 Partial Result Handling

If an error occurs mid-stream:

```java
public void streamResults(ArrowBatchStream stream) {
    try (stream) {
        while (stream.hasNext()) {
            VectorSchemaRoot batch = stream.next();
            streamBatch(batch);
        }
        sendResultComplete();
    } catch (Exception e) {
        // Send error to client - they received partial results
        responseObserver.onError(Status.INTERNAL
            .withDescription("Query failed after streaming " + batchIndex +
                           " batches: " + e.getMessage())
            .asRuntimeException());
    }
}
```

### 6.2 Client Cancellation

Handle gRPC context cancellation:

```java
public void streamResults(ArrowBatchStream stream) {
    Context grpcContext = Context.current();

    try (stream) {
        while (stream.hasNext()) {
            // Check for cancellation between batches
            if (grpcContext.isCancelled()) {
                logger.info("Query cancelled by client after {} batches", batchIndex);
                return;
            }

            VectorSchemaRoot batch = stream.next();
            streamBatch(batch);
        }
        sendResultComplete();
    }
}
```

---

## 7. Compatibility Considerations

### 7.1 Spark Connect Protocol Compatibility

The Spark Connect protocol expects Arrow IPC format in the `ArrowBatch.data` field. Our streaming approach is fully compatible:

- Each batch is a valid Arrow IPC message
- Schema is embedded in the stream (written once with `writer.start()`)
- PySpark client handles multi-batch responses natively

### 7.2 DuckDB Version Requirements

- Requires DuckDB JDBC 0.9.0+ for `arrowExportStream()` support
- Current Thunderduck uses DuckDB 1.1.3 ✓

### 7.3 Backward Compatibility

The refactored code will be **wire-compatible** with existing PySpark clients:
- Same protobuf messages (`ExecutePlanResponse.ArrowBatch`)
- Same Arrow IPC format
- Only difference: multiple smaller batches vs single large batch

---

## 8. Performance Targets

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Time to first row (1M rows) | 5000ms | 50ms | 100x |
| Peak memory (1M rows) | 665MB | 82MB | 8x |
| Throughput (rows/sec) | 200K | 500K | 2.5x |
| GC pause frequency | High | Low | - |

---

## 9. Testing Strategy

### 9.1 Unit Tests

- `ArrowStreamingExecutorTest`: Test batch iteration, schema extraction
- `StreamingResultHandlerTest`: Test gRPC response building, error handling

### 9.2 Integration Tests

- **Large result streaming**: 10M row query, verify batch-by-batch delivery
- **Memory bounds**: Query with `memory_limit=100MB`, verify no OOM
- **Cancellation**: Cancel mid-stream, verify cleanup

### 9.3 Differential Tests

- Compare streaming vs materialized results for correctness
- Verify row counts, data types, values match exactly

---

## 10. Dependencies and Alternative Approaches

### 10.1 Required Dependency: `arrow-c-data`

The streaming architecture requires the Apache Arrow C Data Interface module:

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-c-data</artifactId>
    <version>${arrow.version}</version>
</dependency>
```

**Why it's required:**

DuckDB's `arrowExportStream()` method uses the [Arrow C Data Interface](https://arrow.apache.org/docs/java/cdata.html) for zero-copy data transfer. Internally, it:

1. Creates an `ArrowArrayStream` C struct pointer in native memory
2. Uses `org.apache.arrow.c.ArrowArrayStream.wrap(pointer)` to wrap the native pointer
3. Uses `org.apache.arrow.c.Data.importArrayStream()` to convert to a Java `ArrowReader`

These classes (`ArrowArrayStream`, `Data`) are in the `arrow-c-data` module. DuckDB uses reflection to load them at runtime, so if they're missing:

```
java.lang.ClassNotFoundException: org.apache.arrow.c.ArrowArrayStream
```

**Benefits of the C Data Interface:**

| Aspect | Benefit |
|--------|---------|
| Zero-copy | Data stays in DuckDB's native memory, no Java heap copy |
| No serialization | Unlike IPC, no serialization overhead within process |
| Memory efficiency | Only batch-sized chunks in Java at a time |
| Type safety | Full Arrow schema metadata preserved |

### 10.2 Alternative Approaches Considered

We evaluated several alternative approaches before selecting the current design:

#### Alternative 1: Pure JDBC Row Iteration (Current Implementation)

**Approach:** Use standard JDBC `ResultSet.next()` + `getObject()` to iterate rows, manually build Arrow vectors.

```java
// Current approach
List<List<Object>> rows = new ArrayList<>();
while (rs.next()) {
    List<Object> row = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
        row.add(rs.getObject(i));
    }
    rows.add(row);
}
// Then copy to Arrow vectors...
```

| Pros | Cons |
|------|------|
| No additional dependencies | O(rows × columns) object allocations |
| Simple implementation | Double materialization (Java objects → Arrow) |
| | High GC pressure |
| | No streaming capability |

**Verdict:** ❌ Rejected - defeats the purpose of the refactoring.

#### Alternative 2: Arrow IPC Extension (SQL-based)

**Approach:** Use DuckDB's `arrow` community extension to export IPC bytes directly via SQL.

```sql
COPY (SELECT * FROM table) TO '/dev/stdout' (FORMAT 'arrow');
```

| Pros | Cons |
|------|------|
| Direct IPC bytes output | Extension not available in JDBC |
| No Java-side serialization | Requires file/pipe I/O, not ResultSet |
| | Community extension, not core JDBC |

**Verdict:** ❌ Rejected - not available through JDBC driver.

#### Alternative 3: ADBC (Arrow Database Connectivity)

**Approach:** Use ADBC instead of JDBC for native Arrow result streaming.

| Pros | Cons |
|------|------|
| Designed for Arrow-native access | **No Java driver for DuckDB** |
| True zero-copy to IPC | Would require C/JNI bindings |
| Industry standard | Complete rewrite of data layer |

**DuckDB ADBC language support:**
- ✅ C/C++, Python, Go, R, Ruby
- ❌ **Java not supported**

**Verdict:** ❌ Rejected - Java ADBC driver for DuckDB does not exist.

#### Alternative 4: Direct IPC Export from DuckDB (Hypothetical)

**Approach:** If DuckDB JDBC had `arrowExportIPC()` returning `byte[]` directly.

| Pros | Cons |
|------|------|
| Skip Java-side IPC serialization | **Does not exist** |
| Minimal Java heap usage | Would require DuckDB JDBC changes |

**Verdict:** ❌ Not available - would require changes to DuckDB's JDBC driver.

### 10.3 Chosen Approach: C Data Interface + Java IPC Serialization

**Data flow:**
```
DuckDB native → arrowExportStream() → ArrowReader → VectorSchemaRoot → ArrowStreamWriter → IPC bytes → gRPC
               ─────────────────────────────────────────────────────    ───────────────────────────────────
               Zero-copy (via arrow-c-data)                              Java-side serialization (unavoidable)
```

**Why this is optimal:**

1. **Eliminates JDBC row iteration** - the expensive O(rows × columns) copy
2. **Zero-copy from DuckDB** - data stays in native memory until serialization
3. **Batch streaming** - only 8K rows in Java heap at a time
4. **IPC serialization is unavoidable** - Spark Connect protocol requires IPC bytes over gRPC

**Trade-off accepted:**

We still serialize to IPC bytes in Java (using `ArrowStreamWriter`). This is unavoidable because:
- Spark Connect protocol transmits `ExecutePlanResponse.ArrowBatch.data` as IPC bytes
- gRPC requires serialized bytes, not memory pointers
- Even with ADBC, we'd need IPC serialization for network transport

The key win is **eliminating the double materialization** (JDBC objects → Arrow vectors), not eliminating serialization entirely.

### 10.4 Dependency Summary

| Module | Purpose | Required |
|--------|---------|----------|
| `arrow-vector` | Arrow vector types, VectorSchemaRoot | Yes (existing) |
| `arrow-memory-netty` | Off-heap memory allocation | Yes (existing) |
| `arrow-c-data` | C Data Interface for DuckDB export | **Yes (new)** |

---

## 11. Appendix: DuckDB Arrow Export API

### 11.1 Method Signature

```java
// From org.duckdb.DuckDBResultSet
public ArrowReader arrowExportStream(BufferAllocator allocator, long batchSize)
```

### 11.2 Usage Pattern

```java
DuckDBResultSet duckRS = resultSet.unwrap(DuckDBResultSet.class);
try (ArrowReader reader = (ArrowReader) duckRS.arrowExportStream(allocator, 8192)) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        // Process batch - DO NOT close, owned by reader
        System.out.println("Batch rows: " + batch.getRowCount());
    }
}
```

### 11.3 Key Behaviors

1. `loadNextBatch()` returns `false` when no more data
2. `getVectorSchemaRoot()` returns the **same object** with updated data
3. Schema available after first `loadNextBatch()` call
4. Batch size is a hint - actual batches may be smaller

---

*Document End*
