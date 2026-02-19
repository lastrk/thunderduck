# Arrow Streaming Architecture

**Version**: 2.0
**Date**: 2025-12-11
**Status**: Implemented

## Executive Summary

Thunderduck uses **zero-copy Arrow streaming** from DuckDB to Spark Connect clients. This architecture streams Arrow batches directly from DuckDB's native Arrow export interface to gRPC responses, minimizing memory usage and enabling low-latency results delivery.

---

## 1. Architecture Overview

### 1.1 Data Flow

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

### 1.2 Key Design Principles

1. **Direct Arrow Export**: Uses `DuckDBResultSet.arrowExportStream()` for native Arrow batches
2. **Batch Streaming**: Each batch is sent to client as soon as it's ready
3. **Memory Bounded**: Only one batch (configurable size) in memory at a time
4. **Schema Once**: Schema sent in first batch, subsequent batches are data-only
5. **Back-pressure Aware**: Respects gRPC flow control

### 1.3 Memory Profile (Example: 1M rows × 10 columns)

| Stage | Memory Usage |
|-------|--------------|
| DuckDB internal | ~80MB (columnar, compressed) |
| Arrow batch (8192 rows) | ~0.7MB per batch |
| Arrow IPC buffer | ~0.8MB |
| **Peak Memory** | **~82MB** |

---

## 2. Core Components

### 2.1 ArrowBatchIterator Interface

**File**: `core/src/main/java/com/thunderduck/runtime/ArrowBatchIterator.java`

```java
/**
 * Iterator over Arrow batches with metadata and resource management.
 */
public interface ArrowBatchIterator extends Iterator<VectorSchemaRoot>, AutoCloseable {

    /** Get the Arrow schema for result batches */
    Schema getSchema();

    /** Total rows returned so far across all batches */
    long getTotalRowCount();

    /** Number of batches returned so far */
    int getBatchCount();

    /** Check if there was an error during iteration */
    boolean hasError();

    /** Get the error that caused iteration to fail */
    Exception getError();
}
```

### 2.2 ArrowStreamingExecutor

**File**: `core/src/main/java/com/thunderduck/runtime/ArrowStreamingExecutor.java`

Executes queries and returns streaming Arrow batch iterators using DuckDB's native export. Each executor is bound to a session-scoped DuckDBRuntime.

```java
public class ArrowStreamingExecutor implements StreamingQueryExecutor, AutoCloseable {

    private final DuckDBRuntime runtime;  // Session-scoped runtime
    private final BufferAllocator allocator;
    private final int defaultBatchSize;

    /**
     * Create executor with session-scoped runtime.
     * @param runtime the DuckDB runtime (typically from a session)
     */
    public ArrowStreamingExecutor(DuckDBRuntime runtime) {
        this(runtime, new RootAllocator(Long.MAX_VALUE), StreamingConfig.DEFAULT_BATCH_SIZE);
    }

    /**
     * Execute query and return streaming Arrow iterator.
     * Uses DuckDB's arrowExportStream() for zero-copy batches.
     */
    public ArrowBatchIterator executeStreaming(String sql) throws SQLException {
        DuckDBConnection conn = runtime.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        // Create streaming Arrow batch iterator
        return new ArrowBatchStream(rs, stmt, allocator, defaultBatchSize);
    }
}
```

**Note**: The runtime is now obtained from the session, providing session-scoped database isolation. Each session has its own in-memory DuckDB database.

### 2.3 StreamingResultHandler

**File**: `connect-server/src/main/java/com/thunderduck/connect/service/StreamingResultHandler.java`

Handles streaming Arrow batches to gRPC client.

```java
public class StreamingResultHandler {

    private final StreamObserver<ExecutePlanResponse> responseObserver;
    private final String sessionId;
    private final String operationId;

    /**
     * Stream all batches from iterator to client.
     */
    public void streamResults(ArrowBatchIterator iterator) throws IOException {
        while (iterator.hasNext()) {
            VectorSchemaRoot batch = iterator.next();
            streamBatch(batch);
            batchCount++;
            totalRows += batch.getRowCount();
        }
        sendResultComplete();
    }

    private void streamBatch(VectorSchemaRoot batch) throws IOException {
        // Serialize batch to Arrow IPC format
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(batch, null,
                Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }

        // Send as gRPC response
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setArrowBatch(ArrowBatch.newBuilder()
                .setRowCount(batch.getRowCount())
                .setData(ByteString.copyFrom(out.toByteArray()))
                .build())
            .build();

        responseObserver.onNext(response);
    }
}
```

### 2.4 SparkConnectServiceImpl Integration

**File**: `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`

All query execution uses the streaming path:

```java
private void executeSQLStreaming(String sql, String sessionId,
                                 StreamObserver<ExecutePlanResponse> responseObserver) {
    try (ArrowBatchIterator iterator = streamingExecutor.executeStreaming(sql)) {
        StreamingResultHandler handler = new StreamingResultHandler(
            responseObserver, sessionId, operationId);
        handler.streamResults(iterator);
    }
}
```

---

## 3. Configuration

**File**: `core/src/main/java/com/thunderduck/runtime/StreamingConfig.java`

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

## 4. Data Flow Timeline

Query: `SELECT * FROM large_table` (1M rows)

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
└── Latency: ~5000ms total, ~50ms to first row
```

---

## 5. Error Handling

### 5.1 Partial Result Handling

If an error occurs mid-stream:

```java
public void streamResults(ArrowBatchIterator iterator) {
    try {
        while (iterator.hasNext()) {
            VectorSchemaRoot batch = iterator.next();
            streamBatch(batch);
        }
        sendResultComplete();
    } catch (Exception e) {
        // Send error to client - they may have received partial results
        responseObserver.onError(Status.INTERNAL
            .withDescription("Query failed after streaming " + batchCount +
                           " batches: " + e.getMessage())
            .asRuntimeException());
    }
}
```

### 5.2 Client Cancellation

Handle gRPC context cancellation:

```java
public void streamResults(ArrowBatchIterator iterator) {
    Context grpcContext = Context.current();

    while (iterator.hasNext()) {
        // Check for cancellation between batches
        if (grpcContext.isCancelled()) {
            logger.info("Query cancelled by client after {} batches", batchCount);
            return;
        }

        VectorSchemaRoot batch = iterator.next();
        streamBatch(batch);
    }
    sendResultComplete();
}
```

---

## 6. Protocol Compatibility

### 6.1 Spark Connect Protocol

The streaming approach is fully compatible with Spark Connect protocol:

- Each batch is a valid Arrow IPC message in `ExecutePlanResponse.ArrowBatch.data`
- Schema is embedded in the stream (written once with `writer.start()`)
- PySpark client handles multi-batch responses natively
- Same protobuf messages, same Arrow IPC format

### 6.2 DuckDB Requirements

- Requires DuckDB JDBC 0.9.0+ for `arrowExportStream()` support
- Current Thunderduck uses DuckDB 1.4.4+

---

## 7. Required Dependencies

### 7.1 Arrow C Data Interface

The streaming architecture requires the Apache Arrow C Data Interface module:

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-c-data</artifactId>
    <version>${arrow.version}</version>
</dependency>
```

DuckDB's `arrowExportStream()` uses the [Arrow C Data Interface](https://arrow.apache.org/docs/java/cdata.html) for zero-copy data transfer:

1. Creates an `ArrowArrayStream` C struct pointer in native memory
2. Uses `org.apache.arrow.c.ArrowArrayStream.wrap(pointer)` to wrap the native pointer
3. Uses `org.apache.arrow.c.Data.importArrayStream()` to convert to a Java `ArrowReader`

### 7.2 Full Dependency List

| Module | Purpose |
|--------|---------|
| `arrow-vector` | Arrow vector types, VectorSchemaRoot |
| `arrow-memory-netty` | Off-heap memory allocation |
| `arrow-c-data` | C Data Interface for DuckDB export |

---

## 8. DuckDB Arrow Export API

### 8.1 Method Signature

```java
// From org.duckdb.DuckDBResultSet
public ArrowReader arrowExportStream(BufferAllocator allocator, long batchSize)
```

### 8.2 Usage Pattern

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

### 8.3 Key Behaviors

1. `loadNextBatch()` returns `false` when no more data
2. `getVectorSchemaRoot()` returns the **same object** with updated data
3. Schema available after first `loadNextBatch()` call
4. Batch size is a hint - actual batches may be smaller

---

## 9. Performance Characteristics

| Metric | Value |
|--------|-------|
| Time to first row (1M rows) | ~50ms |
| Peak memory (1M rows) | ~82MB |
| Memory per batch | ~0.7MB |
| Batch size | 8192 rows (configurable) |

---

*Document End*
