# Spark Connect Protocol Quick Reference
## Cheat Sheet for catalyst2sql Developers

**Version:** 1.0
**Date:** 2025-10-16
**Purpose:** Fast lookup reference for Spark Connect implementation

---

## Protocol Stack (Top to Bottom)

```
User Application (PySpark, Scala, Java, R, .NET)
        ↓
DataFrame/Dataset API (spark.read, df.filter, etc.)
        ↓
Unresolved Logical Plan (intent, not execution)
        ↓
Protocol Buffers (binary serialization)
        ↓
gRPC (HTTP/2 transport)
        ↓
Apache Arrow (columnar data format)
```

---

## gRPC Service Methods

| Method | Request | Response | Priority | Purpose |
|--------|---------|----------|----------|---------|
| **ExecutePlan** | ExecutePlanRequest | stream ExecutePlanResponse | **CRITICAL** | Execute queries |
| **AnalyzePlan** | AnalyzePlanRequest | AnalyzePlanResponse | **HIGH** | Schema inference |
| **Config** | ConfigRequest | ConfigResponse | MEDIUM | Configuration |
| ReattachExecute | ReattachExecuteRequest | stream ExecutePlanResponse | LOW | Fault tolerance |
| ReleaseExecute | ReleaseExecuteRequest | ReleaseExecuteResponse | LOW | Cleanup |
| Interrupt | InterruptRequest | InterruptResponse | LOW | Cancellation |
| AddArtifacts | stream AddArtifactsRequest | AddArtifactsResponse | VERY LOW | UDF support |
| ArtifactStatus | ArtifactStatusesRequest | ArtifactStatusesResponse | VERY LOW | UDF status |

**MVP Requirements**: ExecutePlan + AnalyzePlan only

---

## Core Message Types

### ExecutePlanRequest
```protobuf
message ExecutePlanRequest {
  string session_id = 1;          // Session identifier
  UserContext user_context = 2;   // User info
  string operation_id = 3;        // Optional reattach ID
  Plan plan = 4;                  // CORE: The query plan
  string client_id = 5;           // Client-provided ID
  repeated KeyValue request_options = 6;
}
```

### ExecutePlanResponse (Stream)
```protobuf
message ExecutePlanResponse {
  string session_id = 1;
  string operation_id = 2;

  oneof response_type {
    ArrowBatch arrow_batch = 3;           // Result data
    SqlCommandResult sql_command_result = 4;
    ResultComplete result_complete = 5;   // End marker
    ErrorResponse error = 6;              // Error
  }
}
```

### ArrowBatch
```protobuf
message ArrowBatch {
  int64 row_count = 1;   // Number of rows
  bytes data = 2;        // Arrow IPC format (binary)
}
```

### Plan
```protobuf
message Plan {
  oneof op_type {
    Relation root = 1;   // SELECT queries
    Command command = 2; // DDL/DML (CREATE, INSERT)
  }
}
```

---

## Relation Types (for TPC-H)

| Relation | Spark SQL | Purpose | Priority |
|----------|-----------|---------|----------|
| **Read** | FROM table / read.parquet() | Table/file scan | **CRITICAL** |
| **Filter** | WHERE condition | Row filtering | **CRITICAL** |
| **Project** | SELECT col1, col2 | Column selection | **CRITICAL** |
| **Aggregate** | GROUP BY ... agg() | Aggregations | **CRITICAL** |
| **Sort** | ORDER BY col | Sorting | **CRITICAL** |
| **Join** | JOIN ... ON | Table joins | **HIGH** |
| **Limit** | LIMIT n | Result limiting | HIGH |
| **Union** | UNION / UNION ALL | Set operations | MEDIUM |
| SubqueryAlias | AS alias | Query aliasing | MEDIUM |
| SQL | spark.sql("...") | Raw SQL | LOW |

**TPC-H Q1 Requires**: Read, Filter, Project, Aggregate, Sort

---

## Expression Types (for TPC-H)

### Literals
```protobuf
message Literal {
  oneof literal_type {
    int32 integer = 1;
    int64 long = 2;
    double double = 3;
    string string = 4;
    int32 date = 5;        // Days since epoch
    int64 timestamp = 6;   // Microseconds since epoch
    bool boolean = 7;
    bytes binary = 8;
    Decimal decimal = 9;
  }
}
```

### Column References
```protobuf
message UnresolvedAttribute {
  string unparsed_identifier = 1;   // "col_name" or "table.col_name"
}
```

### Aggregate Functions
```protobuf
message Sum { Expression child = 1; }
message Avg { Expression child = 1; }
message Count { Expression child = 1; }
message Min { Expression child = 1; }
message Max { Expression child = 1; }
```

### Comparisons
```protobuf
message EqualTo { Expression left = 1; Expression right = 2; }
message LessThan { Expression left = 1; Expression right = 2; }
message LessThanOrEqual { Expression left = 1; Expression right = 2; }
message GreaterThan { Expression left = 1; Expression right = 2; }
message GreaterThanOrEqual { Expression left = 1; Expression right = 2; }
```

### Arithmetic
```protobuf
message Add { Expression left = 1; Expression right = 2; }
message Subtract { Expression left = 1; Expression right = 2; }
message Multiply { Expression left = 1; Expression right = 2; }
message Divide { Expression left = 1; Expression right = 2; }
```

---

## TPC-H Q1 Protobuf Structure

```
ExecutePlanRequest {
  session_id: "..."
  user_context { user_id: "..." }
  plan {
    root: Relation {
      Sort {                              // ORDER BY
        input: Relation {
          Aggregate {                     // GROUP BY + aggregations
            input: Relation {
              Filter {                    // WHERE
                input: Relation {
                  Read {                  // FROM
                    data_source {
                      format: "parquet"
                      path: "lineitem.parquet"
                    }
                  }
                }
                condition: LessThanOrEqual { ... }
              }
            }
            grouping_expressions: [ ... ]
            aggregate_expressions: [ Sum(...), Avg(...) ]
          }
        }
        order: [ SortOrder(...) ]
      }
    }
  }
}
```

---

## catalyst2sql Integration Points

### Protobuf → LogicalPlan Mapping

| Protobuf | catalyst2sql LogicalPlan |
|----------|-------------------------|
| proto.Relation.Read | TableScan(source) |
| proto.Relation.Filter | Filter(input, condition) |
| proto.Relation.Project | Project(input, expressions) |
| proto.Relation.Aggregate | Aggregate(input, groupBy, aggregates) |
| proto.Relation.Sort | Sort(input, sortOrders) |
| proto.Relation.Join | Join(left, right, condition, joinType) |
| proto.Relation.Limit | Limit(input, limit) |

### Expression Mapping

| Protobuf | catalyst2sql Expression |
|----------|------------------------|
| proto.Literal | Literal(value, type) |
| proto.UnresolvedAttribute | ColumnReference(name) |
| proto.Sum | FunctionCall("SUM", args) |
| proto.Avg | FunctionCall("AVG", args) |
| proto.EqualTo | BinaryExpression("=", left, right) |
| proto.Add | BinaryExpression("+", left, right) |

---

## Request/Response Flow (Simplified)

```
1. Client: ExecutePlanRequest(plan)
           ↓
2. Server: Deserialize Protobuf → LogicalPlan
           ↓
3. Server: Translate LogicalPlan → SQL
           ↓
4. Server: Execute SQL in DuckDB → ResultSet
           ↓
5. Server: Convert ResultSet → Arrow VectorSchemaRoot
           ↓
6. Server: Split VectorSchemaRoot → Arrow batches (64K rows each)
           ↓
7. Server: Stream ExecutePlanResponse(ArrowBatch) × N
           ↓
8. Server: Send ExecutePlanResponse(ResultComplete)
           ↓
9. Client: Deserialize Arrow batches → DataFrame
```

---

## Session Management

### Session Lifecycle
```
Client connects
    ↓
First ExecutePlanRequest with session_id
    ↓
Server creates Session(session_id, user_id, DuckDB connection)
    ↓
Subsequent requests reuse same session
    ↓
Client disconnects or timeout
    ↓
Server destroys session (close DuckDB connection)
```

### Session Isolation
- Each session has its own DuckDB connection
- Temp tables are session-scoped
- SET commands affect only the session
- No data leakage between sessions

---

## Error Handling

### Error Response
```protobuf
message ErrorResponse {
  string message = 1;           // User-friendly error message
  string error_type = 2;        // Error category
  StackTrace stack_trace = 3;   // Debugging info
}
```

### Error Type Mapping
| catalyst2sql Exception | ErrorType |
|------------------------|-----------|
| SQLGenerationException | INVALID_PLAN_INPUT |
| UnsupportedOperationException | NOT_IMPLEMENTED |
| QueryExecutionException | INTERNAL_ERROR |
| ValidationException | INVALID_PLAN_INPUT |

---

## Arrow Integration

### Arrow IPC Format
```
Arrow RecordBatch (in-memory)
    ↓
Arrow IPC Serialization (binary)
    ↓
bytes in ArrowBatch.data
    ↓
gRPC transmission
    ↓
Client deserialization → Arrow RecordBatch
```

### Batch Sizing
| Batch Size | Use Case | Pros | Cons |
|-----------|----------|------|------|
| 1K rows | Interactive | Low latency | High overhead |
| **64K rows** | **General** | **Balanced** | **Recommended** |
| 1M rows | ETL | High throughput | High memory |

**Default**: 64K rows (Spark default)

---

## Maven Dependencies (Quick Copy)

```xml
<dependencies>
  <!-- gRPC -->
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>1.58.0</version>
  </dependency>
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.58.0</version>
  </dependency>
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>1.58.0</version>
  </dependency>

  <!-- Protocol Buffers -->
  <dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.24.0</version>
  </dependency>

  <!-- Apache Arrow (already in core) -->
  <dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>17.0.0</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <groupId>org.xolstice.maven.plugins</groupId>
      <artifactId>protobuf-maven-plugin</artifactId>
      <version>0.6.1</version>
      <configuration>
        <protocArtifact>com.google.protobuf:protoc:3.24.0:exe:${os.detected.classifier}</protocArtifact>
        <pluginId>grpc-java</pluginId>
        <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.58.0:exe:${os.detected.classifier}</pluginArtifact>
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
```

---

## Protocol File Locations (Spark 3.5.3)

### GitHub URLs
```
base.proto:
https://github.com/apache/spark/blob/v3.5.3/connector/connect/common/src/main/protobuf/spark/connect/base.proto

relations.proto:
https://github.com/apache/spark/blob/v3.5.3/connector/connect/common/src/main/protobuf/spark/connect/relations.proto

expressions.proto:
https://github.com/apache/spark/blob/v3.5.3/connector/connect/common/src/main/protobuf/spark/connect/expressions.proto

types.proto:
https://github.com/apache/spark/blob/v3.5.3/connector/connect/common/src/main/protobuf/spark/connect/types.proto
```

### Local Path (after extraction)
```
catalyst2sql/connect-server/src/main/proto/spark/connect/
├── base.proto
├── relations.proto
├── expressions.proto
├── types.proto
├── commands.proto
├── catalog.proto
└── common.proto
```

---

## PlanConverter Implementation Pattern

```java
public class PlanConverter {
    public LogicalPlan convert(Relation proto) {
        switch (proto.getRelTypeCase()) {
            case READ:
                return convertRead(proto.getRead());
            case FILTER:
                return convertFilter(proto.getFilter());
            case AGGREGATE:
                return convertAggregate(proto.getAggregate());
            case SORT:
                return convertSort(proto.getSort());
            case PROJECT:
                return convertProject(proto.getProject());
            default:
                throw new UnsupportedOperationException(
                    "Unsupported relation: " + proto.getRelTypeCase());
        }
    }

    private Filter convertFilter(Relation.Filter proto) {
        LogicalPlan input = convert(proto.getInput());
        Expression condition = exprConverter.convert(proto.getCondition());
        return new Filter(input, condition);
    }
}
```

---

## Performance Targets

| Metric | Target | Baseline |
|--------|--------|----------|
| TPC-H Q1 (SF=0.01) | < 5s | Spark: 15s |
| TPC-H Q1 (SF=10) | < 10s | Spark: 3min |
| Server overhead | < 15% | vs embedded mode |
| Concurrent clients | 100+ | Spark Connect: ~50 |
| Memory efficiency | 6-8x less | vs Spark |

---

## Testing Checklist

### Week 11 (MVP)
- [ ] Server starts and binds to port 15002
- [ ] PySpark client can connect
- [ ] Execute "SELECT 1 AS col" successfully
- [ ] Results returned in Arrow format
- [ ] Session created per connection

### Week 12 (TPC-H Q1)
- [ ] Read relation converter implemented
- [ ] Filter relation converter implemented
- [ ] Aggregate relation converter implemented
- [ ] Sort relation converter implemented
- [ ] TPC-H Q1 executes successfully
- [ ] Results match expected output
- [ ] Query completes in < 5s (SF=0.01)

### Week 13 (Extended)
- [ ] Join relations implemented
- [ ] Window functions implemented
- [ ] Subqueries implemented
- [ ] 70+ differential tests passing

---

## Common Pitfalls

### 1. Protobuf Field Numbers
**Problem**: Field numbers are immutable in Protocol Buffers
**Solution**: Never change field numbers, only add new fields

### 2. Arrow Batch Size
**Problem**: Single batch for 10M row result causes OOM
**Solution**: Stream batches (64K rows default)

### 3. Session Leaks
**Problem**: Sessions not cleaned up, memory/connection leaks
**Solution**: Implement timeout + explicit cleanup

### 4. Type Mapping
**Problem**: Spark types don't map 1:1 to DuckDB types
**Solution**: Use existing TypeMapper, add explicit conversions

### 5. UnresolvedAttribute Resolution
**Problem**: Column names may be qualified (table.col) or unqualified (col)
**Solution**: Handle both cases, use QueryValidator for validation

---

## Useful Commands

### Generate Protobuf Stubs
```bash
mvn clean compile -pl connect-server
```

### Run Server
```bash
mvn exec:java -pl connect-server -Dexec.mainClass="com.catalyst2sql.connect.SparkConnectServer"
```

### Test with PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("TestClient") \
    .getOrCreate()

df = spark.sql("SELECT 1 AS col")
df.show()
```

### Check gRPC Service
```bash
grpcurl -plaintext localhost:15002 list
# Expected: spark.connect.SparkConnectService
```

---

## References

**Full Specification**: `/workspaces/catalyst2sql/docs/SPARK_CONNECT_PROTOCOL_SPEC.md`
**Implementation Plan**: `/workspaces/catalyst2sql/IMPLEMENTATION_PLAN.md` (Phase 4)

**Official Docs**: https://spark.apache.org/docs/3.5.3/spark-connect-overview.html
**Proto Files**: https://github.com/apache/spark/tree/v3.5.3/connector/connect/common/src/main/protobuf

---

**End of Quick Reference**

Keep this document open while implementing the Spark Connect Server for quick lookups!
