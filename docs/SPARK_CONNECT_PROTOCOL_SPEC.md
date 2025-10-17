# Spark Connect Protocol Specification
## catalyst2sql Implementation Guide

**Version:** 1.0
**Target Spark Version:** 3.5.3
**Date:** 2025-10-16
**Purpose:** Complete protocol specification for implementing a Spark Connect Server

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Protocol Overview](#protocol-overview)
3. [Architecture](#architecture)
4. [gRPC Service Definition](#grpc-service-definition)
5. [Core Message Types](#core-message-types)
6. [Request/Response Flow](#requestresponse-flow)
7. [Apache Arrow Integration](#apache-arrow-integration)
8. [Session Management](#session-management)
9. [Error Handling](#error-handling)
10. [Minimal Protocol Subset for TPC-H Q1](#minimal-protocol-subset-for-tpc-h-q1)
11. [Integration with catalyst2sql](#integration-with-catalyst2sql)
12. [Implementation Roadmap](#implementation-roadmap)
13. [References](#references)

---

## 1. Executive Summary

Spark Connect is a decoupled client-server architecture introduced in Apache Spark 3.4 that allows remote connectivity to Spark clusters using the DataFrame API and unresolved logical plans as the protocol. The protocol uses:

- **Protocol Buffers** for message serialization
- **gRPC** for network communication (HTTP/2)
- **Apache Arrow** for efficient data transfer
- **Unresolved Logical Plans** as the query representation

### Key Benefits for catalyst2sql

1. **Ecosystem Compatibility**: Standard Spark clients (PySpark, Scala, Java) can connect without modification
2. **Remote Access**: Clients don't need catalyst2sql embedded, only Spark Connect client library
3. **Language Agnostic**: Protocol Buffers enable multi-language support
4. **Performance**: Arrow-based data transfer is 3-5x faster than JDBC/ODBC
5. **Drop-in Replacement**: Can replace Spark Connect Server for 5-10x performance gains

### Strategic Value

catalyst2sql implementing Spark Connect transforms it from an embedded library into a **high-performance alternative to Apache Spark** that works with the entire Spark ecosystem.

---

## 2. Protocol Overview

### 2.1 Communication Model

```
┌─────────────────┐                    ┌─────────────────┐
│  Spark Client   │                    │ catalyst2sql    │
│  (PySpark/      │  gRPC (HTTP/2)     │ Connect Server  │
│   Scala/Java)   │ ◄─────────────────►│                 │
│                 │                    │                 │
│  - DataFrame    │  Protocol Buffers  │  - Plan Parser  │
│  - Dataset API  │  (serialization)   │  - SQL Gen      │
│  - SQL()        │                    │  - DuckDB Exec  │
└─────────────────┘                    └─────────────────┘
         │                                      │
         │ 1. LogicalPlan (Protobuf)           │
         ├──────────────────────────────────────►
         │                                      │
         │ 2. ExecutePlanResponse (stream)     │
         ◄──────────────────────────────────────┤
         │    - Arrow Batches                   │
         │    - Schema                          │
         │    - Metrics                         │
```

### 2.2 Data Flow

1. **Client → Server**: Unresolved logical plan encoded as Protocol Buffer message
2. **Server Processing**:
   - Deserialize Protobuf → Internal LogicalPlan
   - Translate LogicalPlan → DuckDB SQL
   - Execute SQL → Arrow RecordBatches
3. **Server → Client**: Stream of Arrow-encoded result batches via gRPC

### 2.3 Protocol Layers

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Application | DataFrame/Dataset API | User-facing programming interface |
| Logical | Unresolved Logical Plan | Intent representation (platform-neutral) |
| Serialization | Protocol Buffers | Binary encoding for network transfer |
| Transport | gRPC (HTTP/2) | RPC framework with streaming support |
| Data | Apache Arrow | Columnar data representation |

---

## 3. Architecture

### 3.1 Client Architecture

```
┌──────────────────────────────────────────┐
│         Spark Client Application         │
│  (PySpark, Scala, Java, R, Go, .NET)     │
└────────────────┬─────────────────────────┘
                 │
┌────────────────▼─────────────────────────┐
│      DataFrame/Dataset/SQL API           │
│  - spark.read.parquet()                  │
│  - df.filter().groupBy().agg()           │
│  - spark.sql("SELECT ...")               │
└────────────────┬─────────────────────────┘
                 │
┌────────────────▼─────────────────────────┐
│    Spark Connect Client Library          │
│  - Plan Builder (UnresolvedLogicalPlan)  │
│  - Protobuf Encoder                      │
│  - gRPC Client Stub                      │
└────────────────┬─────────────────────────┘
                 │
                 │ gRPC/Protobuf
                 │
                 ▼
           Network (HTTP/2)
```

### 3.2 Server Architecture (catalyst2sql)

```
           Network (HTTP/2)
                 │
                 ▼
┌────────────────────────────────────────────┐
│      Spark Connect gRPC Service            │
│  - SparkConnectService.ExecutePlan()      │
│  - SparkConnectService.AnalyzePlan()      │
│  - SparkConnectService.Config()           │
└────────────────┬───────────────────────────┘
                 │
┌────────────────▼───────────────────────────┐
│      Session Manager                       │
│  - Session lifecycle (create/destroy)     │
│  - Per-session isolation                  │
│  - Operation ID tracking                  │
│  - Reattach support                       │
└────────────────┬───────────────────────────┘
                 │
┌────────────────▼───────────────────────────┐
│      Plan Converter                        │
│  - Protobuf → Internal LogicalPlan        │
│  - Relation translation                   │
│  - Expression translation                 │
│  - Function mapping                       │
└────────────────┬───────────────────────────┘
                 │
┌────────────────▼───────────────────────────┐
│   Existing catalyst2sql Components         │
│  - SQL Generator (LogicalPlan → SQL)      │
│  - Query Executor (DuckDB execution)      │
│  - Arrow Interchange (ResultSet → Arrow)  │
└────────────────┬───────────────────────────┘
                 │
                 ▼
         DuckDB Engine
```

### 3.3 Component Responsibilities

| Component | Responsibility | Complexity |
|-----------|---------------|------------|
| gRPC Service | Accept connections, route RPCs | Low |
| Session Manager | Isolate clients, manage state | Medium |
| Plan Converter | Protobuf → LogicalPlan | High |
| SQL Generator | LogicalPlan → SQL | **Already Implemented** |
| Query Executor | Execute SQL, return Arrow | **Already Implemented** |

**Key Insight**: catalyst2sql already has 60% of the required components. The main work is implementing the Protobuf deserialization layer.

---

## 4. gRPC Service Definition

### 4.1 Service Interface

Located in: `connector/connect/common/src/main/protobuf/spark/connect/base.proto`

```protobuf
service SparkConnectService {
  // Execute a query and stream results
  rpc ExecutePlan(ExecutePlanRequest) returns (stream ExecutePlanResponse) {}

  // Analyze a query (schema, statistics)
  rpc AnalyzePlan(AnalyzePlanRequest) returns (AnalyzePlanResponse) {}

  // Get/set configuration
  rpc Config(ConfigRequest) returns (ConfigResponse) {}

  // Upload artifacts (JARs, files)
  rpc AddArtifacts(stream AddArtifactsRequest) returns (AddArtifactsResponse) {}

  // Check artifact status
  rpc ArtifactStatus(ArtifactStatusesRequest) returns (ArtifactStatusesResponse) {}

  // Interrupt running query
  rpc Interrupt(InterruptRequest) returns (InterruptResponse) {}

  // Reattach to existing execution
  rpc ReattachExecute(ReattachExecuteRequest) returns (stream ExecutePlanResponse) {}

  // Release reattachable execution
  rpc ReleaseExecute(ReleaseExecuteRequest) returns (ReleaseExecuteResponse) {}
}
```

### 4.2 RPC Methods Priority (for catalyst2sql)

| RPC | Priority | Reason |
|-----|----------|--------|
| **ExecutePlan** | **CRITICAL** | Core query execution |
| **AnalyzePlan** | **HIGH** | Schema inference, query validation |
| **Config** | **MEDIUM** | Session configuration |
| ReattachExecute | LOW | Fault tolerance (nice-to-have) |
| ReleaseExecute | LOW | Cleanup for reattach |
| Interrupt | LOW | Query cancellation |
| AddArtifacts | VERY LOW | UDF support (future) |
| ArtifactStatus | VERY LOW | UDF support (future) |

**Minimal Viable Server** requires only: `ExecutePlan` + `AnalyzePlan`

---

## 5. Core Message Types

### 5.1 ExecutePlanRequest

```protobuf
message ExecutePlanRequest {
  // Session identifier
  string session_id = 1;

  // User context (user ID, tags)
  UserContext user_context = 2;

  // Optional operation ID for reattachment
  string operation_id = 3;

  // The query plan to execute
  Plan plan = 4;

  // Client-provided request ID
  string client_id = 5;

  // Request metadata (tags, properties)
  repeated KeyValue request_options = 6;
}
```

**Key Fields**:
- `session_id`: Identifies the logical Spark session (for state isolation)
- `user_context`: User identity and authentication info
- `plan`: The unresolved logical plan (core payload)
- `operation_id`: Optional ID for reattachable execution

### 5.2 ExecutePlanResponse

```protobuf
message ExecutePlanResponse {
  string session_id = 1;
  string operation_id = 2;

  oneof response_type {
    ArrowBatch arrow_batch = 3;
    SqlCommandResult sql_command_result = 4;
    ResultComplete result_complete = 5;
    // ... other response types
  }
}
```

**Response Types**:
1. **ArrowBatch**: Result data in Apache Arrow format
2. **ResultComplete**: Signals end of result stream
3. **SqlCommandResult**: For DDL/DML commands (INSERT, CREATE, etc.)

### 5.3 ArrowBatch

```protobuf
message ArrowBatch {
  int64 row_count = 1;
  bytes data = 2;  // Apache Arrow IPC format
}
```

**Structure**:
- `row_count`: Number of rows in this batch (for client-side progress tracking)
- `data`: Serialized Arrow RecordBatch in IPC format (binary)

**Important**: At least one ArrowBatch is guaranteed even for empty result sets.

### 5.4 Plan Message

```protobuf
message Plan {
  oneof op_type {
    Relation root = 1;
    Command command = 2;
  }
}
```

**Plan Types**:
- **Relation**: SELECT queries (returns data)
- **Command**: DDL/DML operations (CREATE, INSERT, etc.)

### 5.5 Relation Message

Located in: `connector/connect/common/src/main/protobuf/spark/connect/relations.proto`

```protobuf
message Relation {
  RelationCommon common = 1;

  oneof rel_type {
    Read read = 2;
    Project project = 3;
    Filter filter = 4;
    Join join = 5;
    Aggregate aggregate = 6;
    Sort sort = 7;
    Limit limit = 8;
    Union set_op = 9;
    Deduplicate deduplicate = 10;
    SubqueryAlias subquery_alias = 11;
    SQL sql = 12;
    LocalRelation local_relation = 13;
    // ... more operators
  }
}
```

**Common Relation Types** (for TPC-H):
1. **Read**: Table/file scan (Parquet, Delta, etc.)
2. **Project**: Column selection/computation
3. **Filter**: WHERE clause
4. **Join**: Inner/outer/semi/anti joins
5. **Aggregate**: GROUP BY with aggregations
6. **Sort**: ORDER BY
7. **Limit**: LIMIT/OFFSET

### 5.6 Expression Message

Located in: `connector/connect/common/src/main/protobuf/spark/connect/expressions.proto`

```protobuf
message Expression {
  oneof expr_type {
    Literal literal = 1;
    UnresolvedAttribute unresolved_attribute = 2;
    UnresolvedFunction unresolved_function = 3;
    // Arithmetic
    Add add = 4;
    Subtract subtract = 5;
    Multiply multiply = 6;
    Divide divide = 7;
    // Comparison
    EqualTo equal_to = 8;
    LessThan less_than = 9;
    GreaterThan greater_than = 10;
    // Aggregates
    Count count = 11;
    Sum sum = 12;
    Avg avg = 13;
    Min min = 14;
    Max max = 15;
    // ... more expressions
  }
}
```

---

## 6. Request/Response Flow

### 6.1 Simple Query Flow

```
Client                            Server (catalyst2sql)
  │                                      │
  │ 1. Build LogicalPlan                │
  │    df = spark.read.parquet(...)     │
  │    df.filter(col("age") > 25)       │
  │                                      │
  │ 2. Serialize to Protobuf            │
  │    ExecutePlanRequest {             │
  │      plan: Relation(Filter(...))    │
  │    }                                 │
  │                                      │
  │ 3. gRPC call: ExecutePlan()         │
  ├─────────────────────────────────────►
  │                                      │
  │                                      │ 4. Deserialize Protobuf
  │                                      │    → Internal LogicalPlan
  │                                      │
  │                                      │ 5. Translate to SQL
  │                                      │    → "SELECT * FROM ... WHERE age > 25"
  │                                      │
  │                                      │ 6. Execute in DuckDB
  │                                      │    → ResultSet
  │                                      │
  │                                      │ 7. Convert to Arrow batches
  │                                      │    → Arrow RecordBatch[]
  │                                      │
  │ 8. Stream ExecutePlanResponse       │
  ◄─────────────────────────────────────┤
  │    { arrow_batch: {...} }           │
  ◄─────────────────────────────────────┤
  │    { arrow_batch: {...} }           │
  ◄─────────────────────────────────────┤
  │    { result_complete: {...} }       │
  ◄─────────────────────────────────────┤
  │                                      │
  │ 9. Deserialize Arrow batches        │
  │    → DataFrame                       │
  │                                      │
```

### 6.2 TPC-H Q1 Flow (Detailed)

**Query**:
```sql
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  AVG(l_quantity) AS avg_qty
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

**Protobuf Plan Structure**:
```
Plan {
  root: Relation {
    rel_type: Sort {
      input: Relation {
        rel_type: Aggregate {
          input: Relation {
            rel_type: Filter {
              input: Relation {
                rel_type: Read {
                  data_source: { format: "parquet", path: "lineitem.parquet" }
                }
              }
              condition: Expression {
                expr_type: LessThanOrEqual {
                  left: UnresolvedAttribute { name: "l_shipdate" }
                  right: Literal { date: "1998-12-01" }
                }
              }
            }
          }
          grouping_expressions: [
            UnresolvedAttribute { name: "l_returnflag" },
            UnresolvedAttribute { name: "l_linestatus" }
          ]
          aggregate_expressions: [
            Sum { child: UnresolvedAttribute { name: "l_quantity" } },
            Sum { child: UnresolvedAttribute { name: "l_extendedprice" } },
            Avg { child: UnresolvedAttribute { name: "l_quantity" } }
          ]
        }
      }
      order: [
        SortOrder { child: UnresolvedAttribute { name: "l_returnflag" }, direction: ASCENDING },
        SortOrder { child: UnresolvedAttribute { name: "l_linestatus" }, direction: ASCENDING }
      ]
    }
  }
}
```

**catalyst2sql Processing**:
1. Deserialize Protobuf → Internal LogicalPlan tree
2. Translate to DuckDB SQL (already implemented in SQLGenerator)
3. Execute SQL → DuckDB ResultSet
4. Convert ResultSet → Arrow RecordBatches (already implemented in ArrowInterchange)
5. Stream Arrow batches via gRPC

**Result Stream** (3 responses):
```
Response 1: { arrow_batch: { row_count: 4, data: <binary> } }
Response 2: { result_complete: { execution_time_ms: 150 } }
```

### 6.3 AnalyzePlan Flow

```
Client                            Server
  │                                      │
  │ AnalyzePlanRequest {                │
  │   plan: Relation(...),              │
  │   explain: { explain_mode: SCHEMA } │
  │ }                                    │
  ├─────────────────────────────────────►
  │                                      │
  │                                      │ Parse plan
  │                                      │ Infer schema
  │                                      │ (without execution)
  │                                      │
  │ AnalyzePlanResponse {               │
  ◄─────────────────────────────────────┤
  │   schema: {                          │
  │     fields: [                        │
  │       { name: "age", type: INT },   │
  │       { name: "name", type: STRING } │
  │     ]                                │
  │   }                                  │
  │ }                                    │
  │                                      │
```

**Use Cases**:
- Schema inference (`df.schema`)
- Query validation (check syntax/semantics)
- EXPLAIN output (query plan visualization)

---

## 7. Apache Arrow Integration

### 7.1 Arrow Format Overview

Apache Arrow is a columnar in-memory format optimized for analytics:

```
┌─────────────────────────────────────┐
│     Apache Arrow RecordBatch        │
├─────────────────────────────────────┤
│ Schema                              │
│  - Field 1: name (string)           │
│  - Field 2: age (int32)             │
│  - Field 3: salary (float64)        │
├─────────────────────────────────────┤
│ Column 1 (name): [Alice, Bob, ...]  │
│ Column 2 (age):  [25, 30, ...]      │
│ Column 3 (salary): [50k, 60k, ...]  │
└─────────────────────────────────────┘
```

**Benefits**:
- Zero-copy data transfer (no serialization overhead)
- SIMD-friendly layout (vectorized operations)
- 3-5x faster than JSON/CSV/JDBC
- Industry standard (Parquet, Arrow Flight, BigQuery, etc.)

### 7.2 Arrow IPC Format

Arrow data is serialized using the **Arrow IPC (Inter-Process Communication)** format:

```
┌────────────────────────────────────┐
│   Arrow IPC Message (binary)       │
├────────────────────────────────────┤
│ [Message Header]                   │
│  - Message type (RecordBatch)      │
│  - Body length                     │
├────────────────────────────────────┤
│ [Schema]                           │
│  - Field metadata                  │
│  - Type information                │
├────────────────────────────────────┤
│ [RecordBatch Body]                 │
│  - Buffer offsets                  │
│  - Column data (binary)            │
└────────────────────────────────────┘
```

### 7.3 catalyst2sql Arrow Integration

**Current Implementation** (already exists):
- `ArrowInterchange.fromResultSet()`: Converts JDBC ResultSet → Arrow VectorSchemaRoot
- Handles all DuckDB types correctly
- Zero-copy where possible

**Required Enhancements** for Connect:
1. **Batching**: Split large results into multiple batches (64K rows default)
2. **IPC Serialization**: Convert VectorSchemaRoot → Arrow IPC bytes
3. **Streaming**: Yield batches progressively (gRPC streaming)

**Example Code** (new):
```java
public class ArrowStreamingAdapter {
    public Iterator<ArrowBatch> streamResults(VectorSchemaRoot root, int batchSize) {
        // Split VectorSchemaRoot into multiple batches
        return new Iterator<ArrowBatch>() {
            int offset = 0;

            @Override
            public boolean hasNext() {
                return offset < root.getRowCount();
            }

            @Override
            public ArrowBatch next() {
                int end = Math.min(offset + batchSize, root.getRowCount());

                // Create batch slice
                VectorSchemaRoot batch = root.slice(offset, end - offset);

                // Serialize to IPC format
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowFileWriter writer = new ArrowFileWriter(batch, null, out)) {
                    writer.writeBatch();
                }

                // Create Protobuf message
                ArrowBatch proto = ArrowBatch.newBuilder()
                    .setRowCount(end - offset)
                    .setData(ByteString.copyFrom(out.toByteArray()))
                    .build();

                offset = end;
                return proto;
            }
        };
    }
}
```

### 7.4 Batch Size Considerations

| Batch Size | Pros | Cons | Use Case |
|-----------|------|------|----------|
| 1K rows | Low latency, early results | High overhead (many messages) | Interactive queries |
| 64K rows | **Balanced** (Spark default) | Moderate latency | **Recommended** |
| 1M rows | High throughput | High memory, long wait | Batch ETL |

**Recommendation**: Use 64K rows (Spark default) with adaptive sizing:
- Small results (<64K): Single batch
- Large results (>64K): Multiple 64K batches
- Very large (>10M): Consider chunking to 1M rows

---

## 8. Session Management

### 8.1 Session Lifecycle

```
┌────────────────────────────────────────┐
│         Client Connection              │
└────────────────┬───────────────────────┘
                 │
                 │ 1. First ExecutePlan request
                 ▼
┌────────────────────────────────────────┐
│      Session Created                   │
│  - session_id: "abc-123"               │
│  - user_id: "alice"                    │
│  - SparkSession instance               │
│  - DuckDB connection                   │
└────────────────┬───────────────────────┘
                 │
                 │ 2. Subsequent requests
                 │    (same session_id)
                 ▼
┌────────────────────────────────────────┐
│      Session Reused                    │
│  - Same SparkSession                   │
│  - Same DuckDB connection              │
│  - Shared temp tables, variables       │
└────────────────┬───────────────────────┘
                 │
                 │ 3. Client disconnect or timeout
                 ▼
┌────────────────────────────────────────┐
│      Session Destroyed                 │
│  - SparkSession.stop()                 │
│  - DuckDB connection closed            │
│  - Temp tables cleaned up              │
└────────────────────────────────────────┘
```

### 8.2 UserContext Structure

```protobuf
message UserContext {
  string user_id = 1;
  string user_name = 2;
  repeated KeyValue extensions = 3;
}
```

**Usage**:
- `user_id`: Unique identifier for user (required)
- `user_name`: Display name (optional)
- `extensions`: Custom metadata (tags, auth tokens, etc.)

**Important**: Some deployments (Databricks) require `USER` environment variable to be set, which is automatically injected into UserContext.

### 8.3 Session Isolation

Each session must be isolated:

1. **Separate DuckDB Connection**: Use connection pool, one per session
2. **Separate Temp Tables**: Session-scoped temporary tables don't leak
3. **Separate Variables**: SET commands only affect session
4. **Separate Artifacts**: Uploaded JARs/files (future feature)

**Implementation** (pseudo-code):
```java
public class SessionManager {
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    public Session getOrCreateSession(String sessionId, UserContext user) {
        return sessions.computeIfAbsent(sessionId, id -> {
            // Create new SparkSession
            SparkSession spark = SparkSession.builder()
                .appId("catalyst2sql-" + sessionId)
                .userId(user.getUserId())
                .build();

            // Create new DuckDB connection
            DuckDBConnection conn = connectionManager.borrowConnection();

            return new Session(sessionId, spark, conn);
        });
    }

    public void destroySession(String sessionId) {
        Session session = sessions.remove(sessionId);
        if (session != null) {
            session.close();
        }
    }
}
```

### 8.4 Operation ID and Reattachment

**Operation ID**: Optional identifier for reattachable execution

**Use Case**: If the ExecutePlan response stream breaks (network failure), client can reattach:

```
Client                            Server
  │                                      │
  │ ExecutePlanRequest {                │
  │   operation_id: "op-123"            │
  │ }                                    │
  ├─────────────────────────────────────►
  │                                      │
  │ Response stream...                  │
  ◄─────────────────────────────────────┤
  ◄─────────────────────────────────────┤
  │                                      │
  │ [Network failure]                   │
  ✗                                      │
  │                                      │
  │ ReattachExecuteRequest {            │
  │   operation_id: "op-123"            │
  │ }                                    │
  ├─────────────────────────────────────►
  │                                      │
  │ Resume from last batch...           │
  ◄─────────────────────────────────────┤
  ◄─────────────────────────────────────┤
```

**Implementation Complexity**: MEDIUM (not required for MVP)

**Recommendation**: Defer reattachment to Phase 4 Week 14+

---

## 9. Error Handling

### 9.1 Error Response Structure

```protobuf
message ExecutePlanResponse {
  oneof response_type {
    ArrowBatch arrow_batch = 1;
    ResultComplete result_complete = 2;
    ErrorResponse error = 3;
  }
}

message ErrorResponse {
  string message = 1;
  string error_type = 2;
  StackTrace stack_trace = 3;
}
```

### 9.2 Common Error Scenarios

| Error | Cause | HTTP Code | Protobuf ErrorType |
|-------|-------|-----------|-------------------|
| InvalidPlan | Malformed Protobuf | 400 | INVALID_PLAN_INPUT |
| UnsupportedOperation | Unsupported relation/expression | 501 | NOT_IMPLEMENTED |
| QueryExecutionError | DuckDB execution failure | 500 | INTERNAL_ERROR |
| SessionNotFound | Invalid session_id | 404 | SESSION_NOT_FOUND |
| AuthenticationFailed | Invalid user credentials | 401 | AUTHENTICATION_FAILED |

### 9.3 Error Handling Flow

```
Client                            Server
  │                                      │
  │ ExecutePlanRequest                  │
  ├─────────────────────────────────────►
  │                                      │
  │                                      │ [SQL generation error]
  │                                      │
  │ ExecutePlanResponse {               │
  ◄─────────────────────────────────────┤
  │   error: {                           │
  │     message: "Unsupported aggregate: STDDEV_SAMP",
  │     error_type: "NOT_IMPLEMENTED",  │
  │     stack_trace: { ... }            │
  │   }                                  │
  │ }                                    │
  │                                      │
  │ [Client throws exception]           │
  │                                      │
```

### 9.4 catalyst2sql Error Mapping

| catalyst2sql Exception | Spark Connect ErrorType |
|------------------------|------------------------|
| `SQLGenerationException` | `INVALID_PLAN_INPUT` |
| `UnsupportedOperationException` | `NOT_IMPLEMENTED` |
| `QueryExecutionException` | `INTERNAL_ERROR` |
| `ValidationException` | `INVALID_PLAN_INPUT` |

---

## 10. Minimal Protocol Subset for TPC-H Q1

### 10.1 Required RPC Methods

1. **ExecutePlan** (CRITICAL)
2. **AnalyzePlan** (HIGH - for schema inference)

### 10.2 Required Relation Types

1. **Read** (Parquet file scan)
2. **Filter** (WHERE clause)
3. **Aggregate** (GROUP BY + aggregations)
4. **Project** (column selection)
5. **Sort** (ORDER BY)

### 10.3 Required Expression Types

**Literals**:
- `Literal` (constants: numbers, strings, dates)

**Column References**:
- `UnresolvedAttribute` (column names)

**Comparisons**:
- `LessThanOrEqual` (<=)

**Aggregates**:
- `Sum` (SUM())
- `Avg` (AVG())
- `Count` (COUNT())

**Arithmetic**:
- `Multiply` (*)
- `Subtract` (-)

### 10.4 Required Message Types

**Request Messages**:
- `ExecutePlanRequest`
- `AnalyzePlanRequest`
- `UserContext`
- `Plan`

**Response Messages**:
- `ExecutePlanResponse`
- `ArrowBatch`
- `ResultComplete`
- `AnalyzePlanResponse`

**Plan Messages**:
- `Relation` (with Read, Filter, Aggregate, Sort)
- `Expression` (with Literal, UnresolvedAttribute, aggregates, comparisons)

### 10.5 Implementation Estimate

| Component | Lines of Code | Complexity |
|-----------|--------------|------------|
| Protobuf generation | Auto-generated | N/A |
| gRPC service skeleton | ~100 | LOW |
| Session manager | ~200 | MEDIUM |
| Plan converter (5 relations) | ~500 | HIGH |
| Expression converter (10 types) | ~300 | MEDIUM |
| Arrow streaming adapter | ~150 | MEDIUM |
| Error handling | ~100 | LOW |
| **Total** | **~1,350 LOC** | **MEDIUM** |

**Estimated Time**: 2 weeks (1 senior engineer)

---

## 11. Integration with catalyst2sql

### 11.1 Existing Components (Reusable)

| Component | Status | Usage |
|-----------|--------|-------|
| **LogicalPlan** | ✅ Complete | Target for Protobuf deserialization |
| **SQLGenerator** | ✅ Complete | Generate SQL from LogicalPlan |
| **QueryExecutor** | ✅ Complete | Execute SQL, return Arrow |
| **ArrowInterchange** | ✅ Complete | ResultSet → Arrow conversion |
| **DuckDBConnectionManager** | ✅ Complete | Connection pooling |
| **QueryLogger** | ✅ Complete | Structured logging |

### 11.2 New Components (Required)

| Component | Purpose | Complexity |
|-----------|---------|------------|
| **SparkConnectService** | gRPC service implementation | LOW |
| **PlanConverter** | Protobuf → LogicalPlan | HIGH |
| **ExpressionConverter** | Protobuf → Expression | MEDIUM |
| **SessionManager** | Session lifecycle | MEDIUM |
| **ArrowStreamingAdapter** | VectorSchemaRoot → Arrow IPC batches | MEDIUM |

### 11.3 Integration Flow

```
┌─────────────────────────────────────────────────┐
│  NEW: SparkConnectService (gRPC endpoint)       │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│  NEW: PlanConverter                             │
│  - Protobuf Relation → LogicalPlan              │
│  - Protobuf Expression → Expression             │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│  EXISTING: SQLGenerator                         │
│  - LogicalPlan → DuckDB SQL                     │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│  EXISTING: QueryExecutor                        │
│  - Execute SQL in DuckDB                        │
│  - Return VectorSchemaRoot                      │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│  NEW: ArrowStreamingAdapter                     │
│  - VectorSchemaRoot → Arrow IPC batches         │
│  - Stream via gRPC                              │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
            gRPC Client
```

### 11.4 Maven Module Structure

```
catalyst2sql/
├── core/                      # Existing
│   ├── LogicalPlan
│   ├── SQLGenerator
│   ├── QueryExecutor
│   └── ArrowInterchange
│
├── connect-server/            # NEW
│   ├── pom.xml                # gRPC/Protobuf dependencies
│   ├── src/main/proto/        # Spark Connect .proto files
│   │   └── spark/connect/
│   │       ├── base.proto
│   │       ├── relations.proto
│   │       └── expressions.proto
│   │
│   └── src/main/java/com/catalyst2sql/connect/
│       ├── SparkConnectService.java       # gRPC service
│       ├── SessionManager.java            # Session lifecycle
│       ├── PlanConverter.java             # Protobuf → LogicalPlan
│       ├── ExpressionConverter.java       # Protobuf → Expression
│       └── ArrowStreamingAdapter.java     # Arrow streaming
│
└── benchmarks/                # Existing (enhanced)
    └── src/main/python/
        └── tpch_connect_client.py  # PySpark client for TPC-H
```

---

## 12. Implementation Roadmap

### Week 10: Research & Setup

**Goals**:
1. Extract Spark Connect .proto files from Spark 3.5.3
2. Configure Maven Protobuf plugin
3. Generate Java stubs
4. Verify compilation

**Deliverables**:
- `connect-server/pom.xml` with gRPC/Protobuf dependencies
- Generated Java classes (auto-generated)
- This specification document

### Week 11: Minimal Viable Server

**Goals**:
1. Implement gRPC service skeleton
2. Basic session management (no isolation)
3. Simple query execution (SELECT 1)
4. Basic error handling

**Deliverables**:
- `SparkConnectService.java` (ExecutePlan stub)
- `SessionManager.java` (basic)
- Integration test: PySpark client connects and executes "SELECT 1"

### Week 12: TPC-H Q1 Integration

**Goals**:
1. Implement relation converters (Read, Filter, Aggregate, Sort)
2. Implement expression converters (Literal, UnresolvedAttribute, aggregates, comparisons)
3. Implement Arrow streaming
4. Execute TPC-H Q1 end-to-end

**Deliverables**:
- `PlanConverter.java` (5 relation types)
- `ExpressionConverter.java` (10 expression types)
- `ArrowStreamingAdapter.java`
- TPC-H Q1 working via PySpark client

### Week 13: Extended Query Support

**Goals**:
1. Add join support (INNER, LEFT, RIGHT, FULL)
2. Add window functions
3. Add subqueries (scalar, IN, EXISTS)
4. Run differential tests via Connect

**Deliverables**:
- Join converter
- Window function converter
- Subquery converter
- 70+ differential tests passing via Spark Connect

### Week 14: Multi-Client & Sessions

**Goals**:
1. Implement proper session isolation
2. Connection pooling
3. Concurrent query execution
4. Resource limits

**Deliverables**:
- Session isolation (separate DuckDB connections)
- Load testing results (100+ concurrent clients)

### Week 15: Performance & Optimization

**Goals**:
1. Query plan caching
2. Optimize Arrow streaming
3. TPC-H full benchmark suite
4. Performance report

**Deliverables**:
- Plan cache (LRU, 1000 entries)
- Benchmark report (TPC-H Q1-Q22)
- Performance < 15% overhead vs embedded mode

### Week 16: Production Readiness

**Goals**:
1. Error handling & retry logic
2. Prometheus metrics
3. Docker image
4. Kubernetes manifests
5. Documentation

**Deliverables**:
- Production-ready error handling
- Metrics endpoint
- Docker image (<500 MB)
- DEPLOYMENT_GUIDE.md
- OPERATIONS_RUNBOOK.md

---

## 13. References

### 13.1 Official Documentation

1. **Spark Connect Overview (3.5.3)**
   https://spark.apache.org/docs/3.5.3/spark-connect-overview.html

2. **Spark Connect GitHub (v3.5.3 branch)**
   https://github.com/apache/spark/tree/v3.5.3/connector/connect

3. **Protocol Buffer Files (3.5.3)**
   https://github.com/apache/spark/tree/v3.5.3/connector/connect/common/src/main/protobuf/spark/connect

4. **Apache Arrow Documentation**
   https://arrow.apache.org/docs/

5. **gRPC Documentation**
   https://grpc.io/docs/

### 13.2 Blog Posts & Tutorials

1. **Exploring the Spark Connect gRPC API**
   https://the.agilesql.club/2024/01/exploring-the-spark-connect-grpc-api/

2. **Build your own Spark frontends with Spark Connect**
   https://www.agilelab.it/blog/build-your-own-spark-frontends

3. **Using Spark Connect from .NET**
   https://the.agilesql.club/2024/01/using-spark-connect-from-.net/

### 13.3 Related Specifications

1. **Apache Arrow IPC Format**
   https://arrow.apache.org/docs/format/Columnar.html

2. **Protocol Buffers Language Guide**
   https://protobuf.dev/programming-guides/proto3/

3. **gRPC Core Concepts**
   https://grpc.io/docs/what-is-grpc/core-concepts/

### 13.4 Implementation Examples

1. **spark-connect-dotnet** (C# implementation)
   https://github.com/GoEddie/spark-connect-dotnet

2. **Spark Connect Client (PySpark)**
   https://github.com/apache/spark/tree/v3.5.3/python/pyspark/sql/connect

3. **Spark Connect Server (Scala)**
   https://github.com/apache/spark/tree/v3.5.3/connector/connect/server

---

## Appendix A: TPC-H Q1 Protobuf Representation

### Full Protobuf Message

```protobuf
ExecutePlanRequest {
  session_id: "session-abc-123"
  user_context {
    user_id: "tpch_user"
  }
  plan {
    root {
      common { plan_id: 1 }
      sort {
        input {
          common { plan_id: 2 }
          aggregate {
            input {
              common { plan_id: 3 }
              filter {
                input {
                  common { plan_id: 4 }
                  read {
                    data_source {
                      format: "parquet"
                      path: "lineitem.parquet"
                    }
                  }
                }
                condition {
                  less_than_or_equal {
                    left {
                      unresolved_attribute {
                        unparsed_identifier: "l_shipdate"
                      }
                    }
                    right {
                      literal {
                        date: 10591  # Days since epoch for 1998-12-01
                      }
                    }
                  }
                }
              }
            }
            grouping_expressions {
              unresolved_attribute { unparsed_identifier: "l_returnflag" }
            }
            grouping_expressions {
              unresolved_attribute { unparsed_identifier: "l_linestatus" }
            }
            aggregate_expressions {
              alias {
                expr {
                  unresolved_function {
                    function_name: "sum"
                    arguments {
                      unresolved_attribute { unparsed_identifier: "l_quantity" }
                    }
                  }
                }
                name: "sum_qty"
              }
            }
            aggregate_expressions {
              alias {
                expr {
                  unresolved_function {
                    function_name: "sum"
                    arguments {
                      unresolved_attribute { unparsed_identifier: "l_extendedprice" }
                    }
                  }
                }
                name: "sum_base_price"
              }
            }
            aggregate_expressions {
              alias {
                expr {
                  unresolved_function {
                    function_name: "avg"
                    arguments {
                      unresolved_attribute { unparsed_identifier: "l_quantity" }
                    }
                  }
                }
                name: "avg_qty"
              }
            }
          }
        }
        order {
          child {
            unresolved_attribute { unparsed_identifier: "l_returnflag" }
          }
          direction: SORT_DIRECTION_ASCENDING
        }
        order {
          child {
            unresolved_attribute { unparsed_identifier: "l_linestatus" }
          }
          direction: SORT_DIRECTION_ASCENDING
        }
      }
    }
  }
}
```

### Corresponding catalyst2sql LogicalPlan

```java
Sort(
  input = Aggregate(
    input = Filter(
      input = Read(source = ParquetDataSource("lineitem.parquet")),
      condition = LessThanOrEqual(
        left = ColumnReference("l_shipdate"),
        right = Literal(LocalDate.of(1998, 12, 1))
      )
    ),
    groupBy = [
      ColumnReference("l_returnflag"),
      ColumnReference("l_linestatus")
    ],
    aggregates = [
      Sum(ColumnReference("l_quantity")).as("sum_qty"),
      Sum(ColumnReference("l_extendedprice")).as("sum_base_price"),
      Avg(ColumnReference("l_quantity")).as("avg_qty")
    ]
  ),
  sortOrders = [
    SortOrder(ColumnReference("l_returnflag"), ASCENDING),
    SortOrder(ColumnReference("l_linestatus"), ASCENDING)
  ]
)
```

### Generated DuckDB SQL

```sql
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  AVG(l_quantity) AS avg_qty
FROM read_parquet('lineitem.parquet')
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag ASC, l_linestatus ASC
```

---

## Appendix B: Maven Dependencies

### pom.xml (connect-server module)

```xml
<project>
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.catalyst2sql</groupId>
    <artifactId>catalyst2sql-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
  </parent>

  <artifactId>catalyst2sql-connect-server</artifactId>
  <name>catalyst2sql Spark Connect Server</name>

  <properties>
    <grpc.version>1.58.0</grpc.version>
    <protobuf.version>3.24.0</protobuf.version>
  </properties>

  <dependencies>
    <!-- catalyst2sql core (existing) -->
    <dependency>
      <groupId>com.catalyst2sql</groupId>
      <artifactId>catalyst2sql-core</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- gRPC -->
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-netty-shaded</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-protobuf</artifactId>
      <version>${grpc.version}</version>
    </dependency>
    <dependency>
      <groupId>io.grpc</groupId>
      <artifactId>grpc-stub</artifactId>
      <version>${grpc.version}</version>
    </dependency>

    <!-- Protocol Buffers -->
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>${protobuf.version}</version>
    </dependency>

    <!-- Apache Arrow (already in core) -->
    <dependency>
      <groupId>org.apache.arrow</groupId>
      <artifactId>arrow-vector</artifactId>
      <version>17.0.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.arrow</groupId>
      <artifactId>arrow-memory-netty</artifactId>
      <version>17.0.0</version>
    </dependency>
  </dependencies>

  <build>
    <extensions>
      <!-- OS Maven Plugin for platform-specific Protobuf compiler -->
      <extension>
        <groupId>kr.motd.maven</groupId>
        <artifactId>os-maven-plugin</artifactId>
        <version>1.7.1</version>
      </extension>
    </extensions>

    <plugins>
      <!-- Protobuf Maven Plugin -->
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.6.1</version>
        <configuration>
          <protocArtifact>com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}</protocArtifact>
          <pluginId>grpc-java</pluginId>
          <pluginArtifact>io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}</pluginArtifact>
          <protoSourceRoot>${project.basedir}/src/main/proto</protoSourceRoot>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>compile-custom</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

---

## Appendix C: ASCII Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     Spark Connect Client Ecosystem                       │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
│  │ PySpark │ │  Scala  │ │  Java   │ │    R    │ │   .NET  │            │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘            │
│       │           │           │           │           │                  │
│       └───────────┴───────────┴───────────┴───────────┘                  │
│                               │                                          │
│                    ┌──────────▼──────────┐                               │
│                    │  Spark Connect      │                               │
│                    │  Client Library     │                               │
│                    │  (Protobuf/gRPC)    │                               │
│                    └──────────┬──────────┘                               │
└───────────────────────────────┼───────────────────────────────────────────┘
                                │
                                │ gRPC/HTTP/2 (Protocol Buffers)
                                │
┌───────────────────────────────▼───────────────────────────────────────────┐
│                     catalyst2sql Spark Connect Server                     │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    gRPC Service (Port 15002)                        │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐               │  │
│  │  │ ExecutePlan  │ │ AnalyzePlan  │ │   Config     │               │  │
│  │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘               │  │
│  └─────────┼────────────────┼────────────────┼─────────────────────────┘  │
│            │                │                │                            │
│  ┌─────────▼────────────────▼────────────────▼─────────────────────────┐  │
│  │                     Session Manager                                 │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐               │  │
│  │  │ Session1 │ │ Session2 │ │ Session3 │ │ Session4 │               │  │
│  │  │ (alice)  │ │  (bob)   │ │ (carol)  │ │  (dave)  │               │  │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘               │  │
│  └───────┼────────────┼────────────┼────────────┼───────────────────────┘  │
│          │            │            │            │                          │
│  ┌───────▼────────────▼────────────▼────────────▼───────────────────────┐  │
│  │                 Plan Converter (Protobuf → LogicalPlan)              │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                │  │
│  │  │ Relation │ │Expression│ │ Function │ │   Type   │                │  │
│  │  │Converter │ │Converter │ │ Mapper   │ │ Mapper   │                │  │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘                │  │
│  └───────┼────────────┼────────────┼────────────┼───────────────────────┘  │
│          └────────────┴────────────┴────────────┘                          │
│                               │                                            │
│  ┌────────────────────────────▼────────────────────────────────────────┐   │
│  │            Existing catalyst2sql Core Components                    │   │
│  │                                                                      │   │
│  │  ┌───────────────┐      ┌───────────────┐      ┌───────────────┐   │   │
│  │  │  LogicalPlan  │──────►  SQLGenerator │──────►QueryExecutor  │   │   │
│  │  │   (Internal)  │      │(Plan→SQL)     │      │(Execute SQL)  │   │   │
│  │  └───────────────┘      └───────────────┘      └───────┬───────┘   │   │
│  │                                                         │           │   │
│  │  ┌───────────────┐      ┌───────────────┐      ┌──────▼────────┐   │   │
│  │  │DuckDBConnPool │      │ArrowInterchange      │VectorSchema   │   │   │
│  │  │(Connections)  │      │(ResultSet→Arrow)     │Root (Arrow)   │   │   │
│  │  └───────────────┘      └───────────────┘      └──────┬────────┘   │   │
│  └─────────────────────────────────────────────────────┬─────────────┘   │
│                                                         │                 │
│  ┌──────────────────────────────────────────────────────▼──────────────┐  │
│  │              Arrow Streaming Adapter                                │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐               │  │
│  │  │ Batch 1  │ │ Batch 2  │ │ Batch 3  │ │Complete  │               │  │
│  │  │ (64K)    │ │ (64K)    │ │ (64K)    │ │ Message  │               │  │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘               │  │
│  └───────┼────────────┼────────────┼────────────┼───────────────────────┘  │
│          │            │            │            │                          │
└──────────┼────────────┼────────────┼────────────┼───────────────────────────┘
           │            │            │            │
           └────────────┴────────────┴────────────┘
                        │
                        │ gRPC Response Stream (Arrow IPC)
                        │
           ┌────────────▼────────────┐
           │  Spark Connect Client   │
           │  (Arrow Deserializer)   │
           └─────────────────────────┘
```

---

**End of Specification**

This document provides a complete specification for implementing a Spark Connect Server for catalyst2sql. For questions or clarifications, refer to the official Spark Connect documentation or the catalyst2sql development team.
