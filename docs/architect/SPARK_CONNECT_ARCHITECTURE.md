# Spark Connect Server Architecture for catalyst2sql

**Document Version**: 1.0
**Date**: 2025-10-16
**Status**: Design Specification
**Author**: Systems Architect

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [System Components](#system-components)
4. [Data Flow](#data-flow)
5. [Module Structure](#module-structure)
6. [Component Designs](#component-designs)
7. [Session Management](#session-management)
8. [Error Handling](#error-handling)
9. [Performance Considerations](#performance-considerations)
10. [Security Architecture](#security-architecture)
11. [Migration Strategy](#migration-strategy)
12. [Deployment Architecture](#deployment-architecture)

---

## 1. Executive Summary

### 1.1 Strategic Vision

catalyst2sql is evolving from an **embedded-only** DuckDB translation layer to a **single-user Spark Connect Server** implementation. This architectural shift enables:

- **Drop-in Spark replacement**: Standard Spark clients (PySpark, Scala Spark) can connect without modification
- **Remote execution**: Clients connect over gRPC instead of embedding the engine
- **Single-user execution**: One active client session at a time, aligned with DuckDB's single-user nature
- **Performance advantage**: 5-10x faster than Spark Connect while maintaining full compatibility
- **Maximal simplicity**: Not a multi-user cluster, just a fast single-user execution engine

### 1.2 Current State (Weeks 1-9)

**Existing Architecture** (✅ Implemented):
```
┌─────────────────────────────────────────────────┐
│          Embedded Mode Architecture             │
├─────────────────────────────────────────────────┤
│  Application Code (Test/Benchmark)              │
│         ↓                                        │
│  catalyst2sql Core Engine                       │
│  ├─ LogicalPlan (14+ node types)               │
│  ├─ Expression System (500+ functions)         │
│  ├─ SQL Generator (DuckDB SQL)                 │
│  ├─ Query Optimizer (7+ rules)                 │
│  └─ Query Executor                             │
│         ↓                                        │
│  DuckDB Runtime                                 │
│  └─ Arrow Interchange                          │
└─────────────────────────────────────────────────┘
```

**Key Assets**:
- 14+ logical plan node types (Select, Filter, Join, Aggregate, Window, etc.)
- 500+ function mappings (Spark → DuckDB)
- Query optimizer with 7+ optimization rules
- Arrow-based data interchange (zero-copy)
- 200+ differential tests (100% Spark parity)
- TPC-H demonstration (Q1, Q3, Q6)

### 1.3 Target Architecture (Weeks 10-16)

**Spark Connect Server Architecture** (🎯 To Implement):
```
┌──────────────────────────────────────────────────────────────────┐
│                    Spark Connect Clients                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   PySpark    │  │  Scala Spark │  │   Java App   │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
└─────────┼──────────────────┼──────────────────┼──────────────────┘
          │                  │                  │
          │        Spark Connect Protocol (gRPC)│
          └──────────────────┴──────────────────┘
                             ↓
┌──────────────────────────────────────────────────────────────────┐
│              Spark Connect Server (catalyst2sql)                 │
├──────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────────┐ │
│  │         SparkConnectService (gRPC Layer)                   │ │
│  │  ├─ ExecutePlan()                                          │ │
│  │  ├─ AnalyzePlan()                                          │ │
│  │  ├─ CreateSession()                                        │ │
│  │  └─ GetArrowBatch()                                        │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       ↓                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │      Translation Layer (connect-server module)             │ │
│  │  ├─ PlanTranslator: Spark Connect Plan → LogicalPlan      │ │
│  │  ├─ ExpressionTranslator: Spark Expr → catalyst2sql Expr  │ │
│  │  ├─ TypeMapper: Spark Types → DuckDB Types                │ │
│  │  └─ ResultSerializer: Arrow → gRPC Response                │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       ↓                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │      Session Management (connect-server module)            │ │
│  │  ├─ SessionManager: Single-session state machine (IDLE/ACTIVE) │ │
│  │  ├─ Session: Minimal session wrapper                       │ │
│  │  ├─ Singleton DuckDB Connection (no pooling)               │ │
│  │  └─ Session Timeout: Auto-cleanup after inactivity (300s)  │ │
│  │  Note: See docs/architect/SINGLE_SESSION_ARCHITECTURE.md   │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       ↓                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │          Core Translation Engine (core module)             │ │
│  │  ├─ LogicalPlan (14+ node types)                          │ │
│  │  ├─ Expression System (500+ functions)                     │ │
│  │  ├─ SQL Generator (DuckDB SQL)                             │ │
│  │  ├─ Query Optimizer (7+ rules)                             │ │
│  │  └─ QueryExecutor                                          │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       ↓                                          │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │           DuckDB Runtime (core module)                     │ │
│  │  ├─ Singleton DuckDB Connection (no pooling)               │ │
│  │  ├─ ArrowInterchange (zero-copy data exchange)             │ │
│  │  ├─ HardwareProfile (SIMD, thread tuning)                  │ │
│  │  └─ QueryLogger (structured logging)                       │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Architecture Overview

### 2.1 Design Principles

1. **Reuse Core Engine**: Preserve existing translation engine (14+ plan types, 500+ functions)
2. **Thin Protocol Layer**: gRPC service is a thin translation layer, not a reimplementation
3. **Single-Session Model**: One active session at a time, aligned with DuckDB's single-user architecture
4. **Zero-Copy Data**: Arrow interchange minimizes serialization overhead
5. **Backward Compatible**: Existing embedded mode continues to work

### 2.2 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     CLIENT LAYER                                │
│  Standard Spark clients: PySpark 3.5+, Scala Spark 3.5+        │
└────────────────────┬────────────────────────────────────────────┘
                     │ Spark Connect Protocol (Protobuf/gRPC)
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│                  PROTOCOL LAYER (NEW)                           │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ SparkConnectService (gRPC)                                │ │
│  │  - Request deserialization (Protobuf → Java objects)      │ │
│  │  - Response serialization (Arrow → Protobuf)              │ │
│  │  - Session lifecycle (create, configure, close)           │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│              TRANSLATION LAYER (NEW)                            │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ PlanTranslator                                            │ │
│  │  - Spark Connect Plan → catalyst2sql LogicalPlan         │ │
│  │  - Preserves plan semantics and optimizations            │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ ExpressionTranslator                                      │ │
│  │  - Spark expressions → catalyst2sql expressions          │ │
│  │  - Function name mapping (500+ functions)                │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│               SESSION LAYER (NEW)                               │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ SessionManager                                            │ │
│  │  - Create/destroy sessions                                │ │
│  │  - Session → Singleton DuckDB connection                  │ │
│  │  - Session timeout and cleanup                            │ │
│  │  - Connection rejection when busy (RESOURCE_EXHAUSTED)    │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Singleton DuckDB Connection (No Pooling)                  │ │
│  │  - Single connection created at server startup            │ │
│  │  - Shared by the single active session                    │ │
│  │  - All server resources available to active session       │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│                 EXECUTION LAYER (EXISTING)                      │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ Core Translation Engine                                   │ │
│  │  - LogicalPlan (14+ types)                               │ │
│  │  - Expression System (500+ functions)                     │ │
│  │  - SQL Generator                                          │ │
│  │  - Query Optimizer (7+ rules)                             │ │
│  └───────────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ QueryExecutor                                             │ │
│  │  - Execute SQL against DuckDB                             │ │
│  │  - Convert ResultSet to Arrow                             │ │
│  │  - Error handling and logging                             │ │
│  └───────────────────────────────────────────────────────────┘ │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│                  STORAGE LAYER (EXISTING)                       │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ DuckDB Engine                                             │ │
│  │  - In-memory or persistent database                       │ │
│  │  - Parquet/CSV readers                                    │ │
│  │  - SIMD-optimized execution                               │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Reuse Spark's Protobuf definitions** | Ensures 100% protocol compatibility, no custom protocol |
| **Thin translation layer** | Minimizes overhead, leverages existing core engine |
| **Single-session model** | Aligned with DuckDB's single-user nature, maximal simplicity |
| **Singleton DuckDB connection** | One connection shared by the single session, no pooling overhead |
| **Session timeout** | Inactive sessions timeout after configurable period (default: 300s) |
| **Connection rejection** | New connections rejected while session active ("server busy" error) |
| **Arrow streaming** | Zero-copy data transfer, optimal for large results |
| **No query compilation cache initially** | Simplicity first, add caching in Week 15 if needed |

---

## 3. System Components

### 3.1 Component Hierarchy

```
catalyst2sql-parent/
├── core/                          # EXISTING: Core translation engine
│   ├── logical/                   # LogicalPlan, Filter, Join, Aggregate, etc.
│   ├── expression/                # Expression, FunctionCall, etc.
│   ├── types/                     # Type system (Spark → DuckDB)
│   ├── functions/                 # Function registry (500+ mappings)
│   ├── optimizer/                 # Query optimization rules
│   ├── runtime/                   # QueryExecutor, DuckDB connections
│   └── sql/                       # SQL generation
│
├── connect-server/                # NEW: Spark Connect Server
│   ├── protocol/                  # gRPC service implementation
│   │   ├── SparkConnectServiceImpl.java
│   │   ├── RequestHandler.java
│   │   └── ResponseSerializer.java
│   ├── translation/               # Spark Connect → catalyst2sql
│   │   ├── PlanTranslator.java
│   │   ├── ExpressionTranslator.java
│   │   ├── TypeMapper.java
│   │   └── SchemaConverter.java
│   ├── session/                   # Session management
│   │   ├── SessionManager.java
│   │   ├── Session.java
│   │   ├── SessionStore.java
│   │   └── ConnectionPool.java
│   ├── server/                    # Server runtime
│   │   ├── SparkConnectServer.java
│   │   ├── ServerConfig.java
│   │   └── ServerBootstrap.java
│   └── streaming/                 # Result streaming
│       ├── ArrowStreamWriter.java
│       ├── BatchProcessor.java
│       └── StreamingContext.java
│
├── tests/                         # EXISTING: Test suite
│   ├── unit/                      # Unit tests
│   ├── integration/               # Integration tests
│   └── differential/              # Spark comparison tests
│
└── benchmarks/                    # EXISTING: TPC-H benchmarks
    ├── tpch/                      # TPC-H queries
    └── micro/                     # Micro-benchmarks
```

### 3.2 Dependency Graph

```
┌──────────────────┐
│ connect-server   │  (NEW: Spark Connect Server)
└────────┬─────────┘
         │ depends on
         ↓
┌──────────────────┐
│      core        │  (EXISTING: Translation engine)
└────────┬─────────┘
         │ depends on
         ↓
┌──────────────────┐
│  DuckDB + Arrow  │  (External dependencies)
└──────────────────┘
```

**Key Insight**: `connect-server` module depends on `core`, but `core` remains independent. This preserves embedded mode functionality.

---

## 4. Data Flow

### 4.1 Query Execution Flow

```
┌────────────┐
│ PySpark    │  1. Client submits query
│ Client     │     spark.sql("SELECT * FROM users WHERE age > 25")
└─────┬──────┘
      │ gRPC ExecutePlan(request)
      │ Protobuf: Plan, sessionId, config
      ↓
┌──────────────────────────────────────────────────┐
│ SparkConnectService.executePlan()                │
│  1. Parse Protobuf request                       │
│  2. Extract Plan and sessionId                   │
│  3. Route to RequestHandler                      │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ SessionManager.getSession(sessionId)             │
│  1. Check server state (IDLE/ACTIVE)             │
│  2. Verify session ID matches current session    │
│  3. Return Session object                        │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ PlanTranslator.translate(sparkPlan)              │
│  1. Walk Spark Connect Plan tree                │
│  2. Convert to catalyst2sql LogicalPlan          │
│  3. Apply type conversions                       │
│  Output: LogicalPlan (e.g., Filter → Project)    │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ QueryOptimizer.optimize(logicalPlan)             │
│  1. Apply optimization rules (7+ rules)          │
│  2. Predicate pushdown, column pruning, etc.     │
│  Output: Optimized LogicalPlan                   │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ SQLGenerator.generate(logicalPlan)               │
│  1. Walk optimized plan tree                     │
│  2. Generate DuckDB SQL                          │
│  Output: "SELECT * FROM users WHERE age > 25"    │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ QueryExecutor.executeQuery(sql, connection)      │
│  1. Execute SQL on session's DuckDB connection   │
│  2. Get JDBC ResultSet                           │
│  3. Convert to Arrow VectorSchemaRoot            │
│  Output: VectorSchemaRoot (columnar data)        │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ ArrowStreamWriter.streamBatches(arrowData)       │
│  1. Split Arrow data into batches (64K rows)     │
│  2. Serialize each batch to Protobuf             │
│  3. Stream via gRPC (server-side streaming)      │
│  Output: Stream of ArrowBatch messages           │
└─────┬────────────────────────────────────────────┘
      │ gRPC response stream
      ↓
┌────────────┐
│ PySpark    │  4. Client receives Arrow batches
│ Client     │     Materializes as DataFrame
└────────────┘
```

### 4.2 Session Creation Flow

```
┌────────────┐
│ PySpark    │  1. spark = SparkSession.builder
│ Client     │         .remote("sc://localhost:15002")
└─────┬──────┘         .getOrCreate()
      │ gRPC AnalyzePlan(CreateSession)
      ↓
┌──────────────────────────────────────────────────┐
│ SparkConnectService.analyzePlan()                │
│  1. Detect CreateSession request                 │
│  2. Generate unique sessionId (UUID)             │
│  3. Route to SessionManager                      │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ SessionManager.getOrCreateSession(sessionId)     │
│  1. Check serverState == IDLE                    │
│  2. If IDLE:                                     │
│     - Create new Session object                  │
│     - Set serverState = ACTIVE                   │
│     - Use singleton DuckDB connection            │
│     - Initialize session state                   │
│     - Start timeout timer (5 min)                │
│  3. If ACTIVE:                                   │
│     - Reject with RESOURCE_EXHAUSTED error       │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌────────────┐
│ PySpark    │  2. Session created (or error if server busy)
│ Client     │     sessionId stored in client context
└────────────┘
```

### 4.3 Error Handling Flow

```
┌────────────┐
│ PySpark    │  1. spark.sql("SELECT * FROM non_existent_table")
│ Client     │
└─────┬──────┘
      │ gRPC ExecutePlan(request)
      ↓
┌──────────────────────────────────────────────────┐
│ SparkConnectService.executePlan()                │
│  try {                                           │
│    // ... normal execution flow                  │
│  } catch (Exception e) {                         │
│    → ErrorHandler.handle(e)                      │
│  }                                               │
└─────┬────────────────────────────────────────────┘
      │
      ↓
┌──────────────────────────────────────────────────┐
│ ErrorHandler.handle(exception)                   │
│  1. Classify error type:                         │
│     - SQLException → TableNotFoundError          │
│     - QueryExecutionException → ExecutionError   │
│     - IllegalArgumentException → InvalidArgument │
│  2. Map to gRPC status code                      │
│  3. Extract user-friendly message                │
│  4. Log error with context (queryId, sessionId)  │
│  5. Create gRPC error response                   │
└─────┬────────────────────────────────────────────┘
      │ gRPC error response
      ↓
┌────────────┐
│ PySpark    │  2. Client receives exception
│ Client     │     raise AnalysisException("Table not found: non_existent_table")
└────────────┘
```

---

## 5. Module Structure

### 5.1 Maven Multi-Module Layout

```xml
<!-- catalyst2sql-parent/pom.xml -->
<project>
  <groupId>com.catalyst2sql</groupId>
  <artifactId>catalyst2sql-parent</artifactId>
  <version>0.2.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <modules>
    <module>core</module>
    <module>connect-server</module>  <!-- NEW MODULE -->
    <module>tests</module>
    <module>benchmarks</module>
  </modules>

  <properties>
    <spark.version>3.5.3</spark.version>
    <grpc.version>1.60.0</grpc.version>
    <protobuf.version>3.25.1</protobuf.version>
    <duckdb.version>1.1.3</duckdb.version>
    <arrow.version>17.0.0</arrow.version>
  </properties>
</project>
```

### 5.2 connect-server Module POM

```xml
<!-- connect-server/pom.xml -->
<project>
  <parent>
    <groupId>com.catalyst2sql</groupId>
    <artifactId>catalyst2sql-parent</artifactId>
    <version>0.2.0-SNAPSHOT</version>
  </parent>

  <artifactId>catalyst2sql-connect-server</artifactId>
  <packaging>jar</packaging>

  <dependencies>
    <!-- Internal: Core translation engine -->
    <dependency>
      <groupId>com.catalyst2sql</groupId>
      <artifactId>catalyst2sql-core</artifactId>
      <version>${project.version}</version>
    </dependency>

    <!-- Spark Connect Protocol (provided) -->
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-connect_2.13</artifactId>
      <version>${spark.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- gRPC Server -->
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

    <!-- Protobuf -->
    <dependency>
      <groupId>com.google.protobuf</groupId>
      <artifactId>protobuf-java</artifactId>
      <version>${protobuf.version}</version>
    </dependency>

    <!-- Arrow (for result streaming) -->
    <dependency>
      <groupId>org.apache.arrow</groupId>
      <artifactId>arrow-vector</artifactId>
      <version>${arrow.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.arrow</groupId>
      <artifactId>flight-core</artifactId>
      <version>${arrow.version}</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <!-- Protobuf code generation -->
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.6.1</version>
        <configuration>
          <protocArtifact>
            com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}
          </protocArtifact>
          <pluginId>grpc-java</pluginId>
          <pluginArtifact>
            io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}
          </pluginArtifact>
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

      <!-- Fat JAR for standalone server -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals>
              <goal>shade</goal>
            </goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>com.catalyst2sql.server.SparkConnectServer</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

### 5.3 Protobuf Setup Strategy

**Option 1: Copy Spark's .proto files** (Recommended)
- Extract from `spark-connect_2.13-3.5.3.jar`
- Place in `connect-server/src/main/proto/`
- Generate Java stubs with `protoc`
- Advantages: Full control, no Spark dependency at build time

**Option 2: Depend on spark-connect artifact**
- Use Spark's precompiled Protobuf classes
- Advantages: Always in sync with Spark
- Disadvantages: Heavy dependency, potential classpath conflicts

**Recommendation**: Option 1 for cleaner build, Option 2 for faster initial development.

---

## 6. Component Designs

### 6.1 SparkConnectService (gRPC Interface)

**File**: `connect-server/src/main/java/com/catalyst2sql/protocol/SparkConnectServiceImpl.java`

```java
package com.catalyst2sql.protocol;

import org.apache.spark.connect.proto.*;
import io.grpc.stub.StreamObserver;
import com.catalyst2sql.session.SessionManager;
import com.catalyst2sql.translation.PlanTranslator;

/**
 * gRPC service implementation for Spark Connect protocol.
 *
 * Handles client requests and delegates to appropriate handlers.
 */
public class SparkConnectServiceImpl
    extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {

    private final SessionManager sessionManager;
    private final PlanTranslator planTranslator;
    private final RequestHandler requestHandler;

    public SparkConnectServiceImpl(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        this.planTranslator = new PlanTranslator();
        this.requestHandler = new RequestHandler(sessionManager, planTranslator);
    }

    /**
     * Execute a logical plan and stream results.
     *
     * @param request ExecutePlanRequest containing plan and session info
     * @param responseObserver Streaming response observer for Arrow batches
     */
    @Override
    public void executePlan(
        ExecutePlanRequest request,
        StreamObserver<ExecutePlanResponse> responseObserver) {

        try {
            // 1. Extract session ID
            String sessionId = request.getSessionId();

            // 2. Get or create session
            Session session = sessionManager.getOrCreateSession(sessionId);

            // 3. Translate plan
            LogicalPlan plan = planTranslator.translate(request.getPlan());

            // 4. Execute query via existing QueryExecutor
            VectorSchemaRoot result = session.executeQuery(plan);

            // 5. Stream results as Arrow batches
            streamResults(result, responseObserver);

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                ErrorHandler.toGrpcStatus(e).asException()
            );
        }
    }

    /**
     * Analyze a plan (used for session creation, schema inference).
     *
     * @param request AnalyzePlanRequest
     * @param responseObserver Response observer
     */
    @Override
    public void analyzePlan(
        AnalyzePlanRequest request,
        StreamObserver<AnalyzePlanResponse> responseObserver) {

        try {
            String sessionId = request.getSessionId();

            // Handle different analysis types
            if (request.hasSchema()) {
                // Schema inference request
                handleSchemaRequest(sessionId, request, responseObserver);
            } else if (request.hasExplain()) {
                // EXPLAIN request
                handleExplainRequest(sessionId, request, responseObserver);
            } else {
                // Session creation or other analysis
                handleGenericAnalysis(sessionId, request, responseObserver);
            }

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                ErrorHandler.toGrpcStatus(e).asException()
            );
        }
    }

    /**
     * Stream Arrow result batches to client.
     */
    private void streamResults(
        VectorSchemaRoot result,
        StreamObserver<ExecutePlanResponse> responseObserver) {

        // Implementation: Batch results into 64K row chunks
        // Convert each batch to Arrow IPC format
        // Send via gRPC streaming
        // See: ArrowStreamWriter component
    }
}
```

**Key Methods**:
- `executePlan()`: Main query execution entry point
- `analyzePlan()`: Session management, schema inference, EXPLAIN
- `streamResults()`: Arrow batch streaming to client

---

### 6.2 PlanTranslator (Spark Connect → LogicalPlan)

**File**: `connect-server/src/main/java/com/catalyst2sql/translation/PlanTranslator.java`

```java
package com.catalyst2sql.translation;

import org.apache.spark.connect.proto.Relation;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.expression.*;

/**
 * Translates Spark Connect logical plans to catalyst2sql LogicalPlan.
 *
 * Handles plan node types:
 * - Read (table scan, Parquet files)
 * - Project (column selection)
 * - Filter (WHERE conditions)
 * - Join (all types)
 * - Aggregate (GROUP BY)
 * - Sort (ORDER BY)
 * - Limit
 * - Union
 * - With (CTEs)
 * - SubqueryAlias
 */
public class PlanTranslator {

    private final ExpressionTranslator exprTranslator;
    private final TypeMapper typeMapper;

    public PlanTranslator() {
        this.exprTranslator = new ExpressionTranslator();
        this.typeMapper = new TypeMapper();
    }

    /**
     * Translate Spark Connect Relation to LogicalPlan.
     *
     * @param relation Spark Connect relation (from Protobuf)
     * @return catalyst2sql LogicalPlan
     */
    public LogicalPlan translate(Relation relation) {
        switch (relation.getRelTypeCase()) {
            case READ:
                return translateRead(relation.getRead());

            case PROJECT:
                return translateProject(relation.getProject());

            case FILTER:
                return translateFilter(relation.getFilter());

            case JOIN:
                return translateJoin(relation.getJoin());

            case AGGREGATE:
                return translateAggregate(relation.getAggregate());

            case SORT:
                return translateSort(relation.getSort());

            case LIMIT:
                return translateLimit(relation.getLimit());

            case UNION:
                return translateUnion(relation.getUnion());

            default:
                throw new UnsupportedOperationException(
                    "Unsupported relation type: " + relation.getRelTypeCase()
                );
        }
    }

    /**
     * Translate Read relation (table scan).
     */
    private LogicalPlan translateRead(Relation.Read read) {
        if (read.hasNamedTable()) {
            // Table scan: SELECT * FROM table_name
            String tableName = read.getNamedTable().getUnparsedIdentifier();
            return new TableScan(tableName);

        } else if (read.hasDataSource()) {
            // Data source read: spark.read.parquet("file.parquet")
            String format = read.getDataSource().getFormat();
            String path = read.getDataSource().getOptionsMap().get("path");

            if ("parquet".equalsIgnoreCase(format)) {
                return new TableScan(
                    String.format("read_parquet('%s')", path)
                );
            } else {
                throw new UnsupportedOperationException(
                    "Unsupported format: " + format
                );
            }
        } else {
            throw new IllegalArgumentException("Invalid Read relation");
        }
    }

    /**
     * Translate Project relation (SELECT columns).
     */
    private LogicalPlan translateProject(Relation.Project project) {
        // Translate child (input relation)
        LogicalPlan child = translate(project.getInput());

        // Translate projection expressions
        List<Expression> projections = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression expr : project.getExpressionsList()) {
            projections.add(exprTranslator.translate(expr));
        }

        return new Project(child, projections);
    }

    /**
     * Translate Filter relation (WHERE condition).
     */
    private LogicalPlan translateFilter(Relation.Filter filter) {
        LogicalPlan child = translate(filter.getInput());
        Expression condition = exprTranslator.translate(filter.getCondition());

        return new Filter(child, condition);
    }

    /**
     * Translate Join relation.
     */
    private LogicalPlan translateJoin(Relation.Join join) {
        LogicalPlan left = translate(join.getLeft());
        LogicalPlan right = translate(join.getRight());

        // Translate join type
        JoinType joinType = translateJoinType(join.getJoinType());

        // Translate join condition (if present)
        Expression condition = join.hasJoinCondition()
            ? exprTranslator.translate(join.getJoinCondition())
            : null;

        return new Join(left, right, joinType, condition);
    }

    /**
     * Translate Aggregate relation (GROUP BY).
     */
    private LogicalPlan translateAggregate(Relation.Aggregate aggregate) {
        LogicalPlan child = translate(aggregate.getInput());

        // Grouping expressions
        List<Expression> groupingExprs = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression expr : aggregate.getGroupingExpressionsList()) {
            groupingExprs.add(exprTranslator.translate(expr));
        }

        // Aggregate functions
        List<Expression> aggregateExprs = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression expr : aggregate.getAggregateExpressionsList()) {
            aggregateExprs.add(exprTranslator.translate(expr));
        }

        return new Aggregate(child, groupingExprs, aggregateExprs);
    }

    // ... other translate methods
}
```

**Key Responsibilities**:
- Walk Spark Connect plan tree recursively
- Map Spark plan nodes to catalyst2sql LogicalPlan nodes
- Preserve plan semantics (no optimization at this layer)
- Handle unsupported operations gracefully

---

### 6.3 SessionManager (Single-Session Management)

**File**: `connect-server/src/main/java/com/catalyst2sql/session/SessionManager.java`

**Note**: This is an illustrative design for the single-session architecture described in Section 7. See `docs/architect/SINGLE_SESSION_ARCHITECTURE.md` for the authoritative single-session design.

```java
package com.catalyst2sql.session;

import com.catalyst2sql.runtime.DuckDBConnectionManager;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages single-session server state machine.
 *
 * Features:
 * - IDLE/ACTIVE state machine (one session at a time)
 * - Session timeout (5 minutes default)
 * - Connection rejection when busy
 * - Singleton DuckDB connection
 *
 * See docs/architect/SINGLE_SESSION_ARCHITECTURE.md for design rationale.
 */
public class SessionManager {

    private enum ServerState { IDLE, ACTIVE }

    private volatile ServerState serverState;
    private volatile Session currentSession;
    private final DuckDBConnection singletonConnection;
    private final ScheduledExecutorService timeoutChecker;
    private final long sessionTimeoutMs;
    private volatile long lastActivityTime;

    public SessionManager(DuckDBConnection singletonConnection) {
        this(singletonConnection, TimeUnit.SECONDS.toMillis(300)); // 5 min default
    }

    public SessionManager(
        DuckDBConnection singletonConnection,
        long sessionTimeoutMs) {

        this.singletonConnection = singletonConnection;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.serverState = ServerState.IDLE;
        this.currentSession = null;
        this.lastActivityTime = 0;

        // Start background timeout checker
        this.timeoutChecker = Executors.newSingleThreadScheduledExecutor();
        this.timeoutChecker.scheduleAtFixedRate(
            this::checkTimeout,
            30, 30, TimeUnit.SECONDS
        );
    }

    /**
     * Get or create session (single-session model).
     *
     * @param sessionId Session identifier (UUID)
     * @return Session object
     * @throws IllegalStateException if server is busy with another session
     */
    public synchronized Session getOrCreateSession(String sessionId) {
        // Check if server is IDLE or session already exists
        if (serverState == ServerState.IDLE) {
            // Create new session (IDLE → ACTIVE transition)
            try {
                Session session = new Session(
                    sessionId,
                    singletonConnection,
                    sessionTimeoutMs
                );
                session.initialize();

                this.currentSession = session;
                this.serverState = ServerState.ACTIVE;
                this.lastActivityTime = System.currentTimeMillis();

                return session;

            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to create session: " + sessionId, e
                );
            }

        } else if (currentSession != null &&
                   currentSession.getSessionId().equals(sessionId)) {
            // Existing session requesting access
            updateActivity();
            return currentSession;

        } else {
            // Server busy with another session
            throw new IllegalStateException(
                "Server busy - active session in progress. " +
                "Please try again later or wait for session timeout."
            );
        }
    }

    /**
     * Get existing session (throws if not found or server idle).
     */
    public synchronized Session getSession(String sessionId) {
        if (serverState == ServerState.IDLE) {
            throw new IllegalArgumentException(
                "Session not found: " + sessionId
            );
        }

        if (currentSession == null ||
            !currentSession.getSessionId().equals(sessionId)) {
            throw new IllegalArgumentException(
                "Session not found: " + sessionId
            );
        }

        updateActivity();
        return currentSession;
    }

    /**
     * Close session and return to IDLE state.
     */
    public synchronized void closeSession(String sessionId) {
        if (currentSession != null &&
            currentSession.getSessionId().equals(sessionId)) {

            try {
                // Clean up session resources
                currentSession.cleanup();

            } catch (Exception e) {
                System.err.println(
                    "Error cleaning up session " + sessionId + ": " +
                    e.getMessage()
                );
            } finally {
                // ACTIVE → IDLE transition
                this.currentSession = null;
                this.serverState = ServerState.IDLE;
                this.lastActivityTime = 0;
            }
        }
    }

    /**
     * Check for session timeout (background task).
     */
    private void checkTimeout() {
        if (serverState == ServerState.ACTIVE && currentSession != null) {
            long now = System.currentTimeMillis();
            long inactiveDuration = now - lastActivityTime;

            if (inactiveDuration > sessionTimeoutMs) {
                System.out.println(
                    "Session timeout detected: " +
                    currentSession.getSessionId() +
                    " (inactive for " + (inactiveDuration / 1000) + "s)"
                );
                closeSession(currentSession.getSessionId());
            }
        }
    }

    /**
     * Update last activity time.
     */
    private void updateActivity() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    /**
     * Shutdown session manager.
     */
    public synchronized void shutdown() {
        timeoutChecker.shutdown();

        if (currentSession != null) {
            closeSession(currentSession.getSessionId());
        }
    }

    /**
     * Get server state (for monitoring).
     */
    public ServerState getServerState() {
        return serverState;
    }

    /**
     * Check if server is busy.
     */
    public boolean isBusy() {
        return serverState == ServerState.ACTIVE;
    }
}
```

**Key Features**:
- **Single-session state machine**: IDLE ↔ ACTIVE transitions
- **Singleton connection**: Reuses single DuckDB connection for all queries
- **Session timeout**: Automatic cleanup after inactivity (default: 5 minutes)
- **Connection rejection**: Rejects new connections when server is ACTIVE
- **Thread-safe**: Synchronized methods for state transitions

---

### 6.4 Session (Per-Client State)

**File**: `connect-server/src/main/java/com/catalyst2sql/session/Session.java`

```java
package com.catalyst2sql.session;

import com.catalyst2sql.runtime.QueryExecutor;
import com.catalyst2sql.logical.LogicalPlan;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.duckdb.DuckDBConnection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a client session with isolated state.
 *
 * Each session has:
 * - Dedicated DuckDB connection
 * - Session configuration (Spark SQL config)
 * - Temporary tables/views
 * - Session timeout tracking
 */
public class Session {

    private final String sessionId;
    private final DuckDBConnection connection;
    private final QueryExecutor executor;
    private final long timeoutMs;

    // Session state
    private final ConcurrentHashMap<String, String> config;
    private final ConcurrentHashMap<String, String> tempViews;
    private volatile long lastAccessTimeMs;

    public Session(
        String sessionId,
        DuckDBConnection connection,
        long timeoutMs) {

        this.sessionId = sessionId;
        this.connection = connection;
        this.executor = new QueryExecutor(
            // Wrap connection in single-connection manager
            new SingleConnectionManager(connection)
        );
        this.timeoutMs = timeoutMs;
        this.config = new ConcurrentHashMap<>();
        this.tempViews = new ConcurrentHashMap<>();
        this.lastAccessTimeMs = System.currentTimeMillis();
    }

    /**
     * Initialize session with default configuration.
     */
    public void initialize() {
        try {
            // Set Spark SQL compatibility defaults
            config.put("spark.sql.ansi.enabled", "false");
            config.put("spark.sql.caseSensitive", "false");

            // Apply configuration to DuckDB connection
            applyConfiguration();

        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to initialize session: " + sessionId, e
            );
        }
    }

    /**
     * Execute a logical plan query.
     *
     * @param plan LogicalPlan to execute
     * @return Arrow result data
     */
    public VectorSchemaRoot executeQuery(LogicalPlan plan) {
        touch(); // Update last access time

        try {
            // Generate SQL from logical plan
            String sql = plan.toSQL(new SQLGenerator());

            // Execute via QueryExecutor
            return executor.executeQuery(sql);

        } catch (Exception e) {
            throw new RuntimeException(
                "Query execution failed in session: " + sessionId, e
            );
        }
    }

    /**
     * Set session configuration.
     */
    public void setConfig(String key, String value) {
        config.put(key, value);
        applyConfiguration();
    }

    /**
     * Create temporary view.
     */
    public void createTempView(String name, String sql) {
        try {
            // Create view in DuckDB
            String viewSQL = String.format(
                "CREATE OR REPLACE TEMP VIEW %s AS %s", name, sql
            );
            executor.executeUpdate(viewSQL);

            // Track in session state
            tempViews.put(name, sql);

        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to create temp view: " + name, e
            );
        }
    }

    /**
     * Check if session is expired.
     */
    public boolean isExpired(long currentTimeMs) {
        return (currentTimeMs - lastAccessTimeMs) > timeoutMs;
    }

    /**
     * Update last access time.
     */
    public void touch() {
        this.lastAccessTimeMs = System.currentTimeMillis();
    }

    /**
     * Clean up session resources.
     */
    public void cleanup() {
        try {
            // Drop all temporary views
            tempViews.keySet().forEach(name -> {
                try {
                    executor.executeUpdate("DROP VIEW IF EXISTS " + name);
                } catch (Exception ignored) {}
            });

            // Clear state
            config.clear();
            tempViews.clear();

        } catch (Exception e) {
            System.err.println(
                "Error cleaning up session: " + sessionId + " - " + e.getMessage()
            );
        }
    }

    /**
     * Apply session configuration to DuckDB connection.
     */
    private void applyConfiguration() {
        // Implementation: Convert Spark config to DuckDB settings
        // Example: spark.sql.ansi.enabled → DuckDB mode
    }

    // Getters
    public String getSessionId() { return sessionId; }
    public DuckDBConnection getConnection() { return connection; }
}
```

**Key Responsibilities**:
- Wrap DuckDB connection with session-specific state
- Execute queries via existing QueryExecutor
- Manage temp views and configuration
- Track session activity for timeout

---

### 6.5 ArrowStreamWriter (Result Streaming)

**File**: `connect-server/src/main/java/com/catalyst2sql/streaming/ArrowStreamWriter.java`

```java
package com.catalyst2sql.streaming;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;

/**
 * Streams Arrow result batches to gRPC client.
 *
 * Features:
 * - Batch results into optimal sizes (64K rows default)
 * - Serialize to Arrow IPC format
 * - Stream via gRPC server-side streaming
 * - Handle backpressure
 */
public class ArrowStreamWriter {

    private static final int DEFAULT_BATCH_SIZE = 65536; // 64K rows

    /**
     * Stream Arrow data to client in batches.
     *
     * @param result Full query result (Arrow VectorSchemaRoot)
     * @param responseObserver gRPC streaming observer
     */
    public static void streamResults(
        VectorSchemaRoot result,
        StreamObserver<ExecutePlanResponse> responseObserver) {

        try {
            int rowCount = result.getRowCount();
            int batchSize = DEFAULT_BATCH_SIZE;

            // Send schema first (Spark Connect requirement)
            sendSchema(result, responseObserver);

            // Stream data in batches
            for (int offset = 0; offset < rowCount; offset += batchSize) {
                int limit = Math.min(batchSize, rowCount - offset);

                // Create batch view (slice of full result)
                VectorSchemaRoot batch = createBatch(result, offset, limit);

                // Serialize batch to Arrow IPC format
                byte[] arrowData = serializeBatch(batch);

                // Send via gRPC
                ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                    .setArrowBatch(
                        ExecutePlanResponse.ArrowBatch.newBuilder()
                            .setData(com.google.protobuf.ByteString.copyFrom(arrowData))
                            .setRowCount(limit)
                            .build()
                    )
                    .build();

                responseObserver.onNext(response);

                // Clean up batch
                batch.close();
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to stream results", e);
        }
    }

    /**
     * Send schema to client (first message).
     */
    private static void sendSchema(
        VectorSchemaRoot result,
        StreamObserver<ExecutePlanResponse> responseObserver) {

        // Serialize Arrow schema
        byte[] schemaBytes = serializeSchema(result.getSchema());

        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSchema(
                ExecutePlanResponse.Schema.newBuilder()
                    .setArrowSchema(
                        com.google.protobuf.ByteString.copyFrom(schemaBytes)
                    )
                    .build()
            )
            .build();

        responseObserver.onNext(response);
    }

    /**
     * Create batch (slice of full result).
     */
    private static VectorSchemaRoot createBatch(
        VectorSchemaRoot full,
        int offset,
        int limit) {

        // Implementation: Copy vectors from full result
        // Use Arrow's VectorUnloader for efficient slicing
        // See: org.apache.arrow.vector.VectorUnloader

        // Placeholder:
        return full; // TODO: Implement slicing
    }

    /**
     * Serialize batch to Arrow IPC format.
     */
    private static byte[] serializeBatch(VectorSchemaRoot batch) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                batch, null, out)) {

                writer.writeBatch();
            }
            return out.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize batch", e);
        }
    }

    /**
     * Serialize schema to Arrow IPC format.
     */
    private static byte[] serializeSchema(org.apache.arrow.vector.types.pojo.Schema schema) {
        // Implementation: Serialize Arrow schema
        // See: org.apache.arrow.vector.ipc.message.MessageSerializer
        return new byte[0]; // Placeholder
    }
}
```

**Key Features**:
- **Batching**: Split large results into 64K row batches
- **Arrow IPC**: Standard Arrow streaming format
- **Schema first**: Send schema before data (Spark Connect protocol)
- **Efficient serialization**: Use Arrow's built-in serializers

---

## 7. Session Management

### 7.1 Single-Session Architecture

**Core Principle**: catalyst2sql implements a **single-session model** aligned with DuckDB's single-user nature.

**State Machine**:
```
┌─────────────────────────────────────────────────────────┐
│              Server State Machine                        │
└─────────────────────────────────────────────────────────┘

States:
  - IDLE: No active session, accepting connections
  - ACTIVE: Has active session, rejecting new connections

Transitions:
  - IDLE → ACTIVE: On client connect (first request)
  - ACTIVE → IDLE: On disconnect or timeout

┌──────┐                              ┌────────┐
│      │──── Client Connect ────────>│        │
│ IDLE │                              │ ACTIVE │
│      │<─── Disconnect/Timeout ─────│        │
└──────┘                              └────────┘
```

### 7.2 Session Lifecycle

```
┌────────────────────────────────────────────────────────────┐
│                    Session Lifecycle                       │
└────────────────────────────────────────────────────────────┘

1. CREATE SESSION (IDLE → ACTIVE)
   ├─ Client: spark.builder.remote("sc://host:port").getOrCreate()
   ├─ Server: Check serverState == IDLE
   ├─ If IDLE:
   │  ├─ Generate sessionId (UUID)
   │  ├─ Create Session object
   │  ├─ Initialize lastActivityTime
   │  ├─ Set serverState = ACTIVE
   │  └─ Response: sessionId returned to client
   └─ If ACTIVE:
      └─ Reject: gRPC RESOURCE_EXHAUSTED "Server busy - session in progress"

2. ACTIVE SESSION
   ├─ Client executes queries with sessionId
   ├─ Update lastActivityTime on each request
   ├─ Reuse singleton DuckDB connection
   └─ State: Temp views, config persisted in Session

3. SESSION TIMEOUT (ACTIVE → IDLE)
   ├─ Background thread: Check lastActivityTime every 30 seconds
   ├─ Expired: (currentTime - lastActivityTime) > sessionTimeout
   ├─ Cleanup: Drop temp views, clear configuration
   ├─ Set serverState = IDLE
   └─ Log: "Session <id> timed out after <duration>s"

4. EXPLICIT CLOSE (ACTIVE → IDLE)
   ├─ Client: spark.stop()
   ├─ Server: closeSession(sessionId)
   ├─ Session: cleanup()
   ├─ Set serverState = IDLE
   └─ Response: Session closed successfully
```

### 7.3 Singleton DuckDB Connection

**Design**: Single DuckDB connection created at server startup and shared by the single session.

**Rationale**:
- DuckDB is single-user by design
- No pooling overhead (creation, validation, lifecycle)
- Direct connection usage for maximum performance
- Simpler resource management

**Configuration**:
```java
// Server initialization (ConnectServer.java)
public class ConnectServer {
    private final DuckDBConnection duckdbConnection;  // Singleton
    private final QueryExecutor queryExecutor;        // Reused for all queries

    public ConnectServer(int port, String duckdbPath) throws SQLException {
        // Create single DuckDB connection at startup
        this.duckdbConnection = (DuckDBConnection) DriverManager.getConnection(
            "jdbc:duckdb:" + duckdbPath
        );

        // Configure connection
        Statement stmt = duckdbConnection.createStatement();
        stmt.execute("SET memory_limit='16GB'");
        stmt.execute("SET threads=8");
        stmt.close();

        // Create query executor with singleton connection
        DuckDBConnectionManager singletonManager =
            new SingletonConnectionManager(duckdbConnection);
        this.queryExecutor = new QueryExecutor(singletonManager);
    }
}
```

### 7.4 Session Timeout Configuration

**Default Configuration**:
```java
SessionManager config:
- sessionTimeout: 300 seconds (5 minutes)
- timeoutCheckInterval: 30 seconds (background thread checks every 30s)
- timeoutAction: CLEANUP_AND_IDLE (drop temp views, reset state)
```

**Configurable via Environment**:
```bash
# Session timeout in seconds (default: 300)
export CATALYST2SQL_SESSION_TIMEOUT=300

# Timeout check interval in seconds (default: 30)
export CATALYST2SQL_TIMEOUT_CHECK_INTERVAL=30
```

**Timeout Logic**:
```java
public class SessionManager {
    private volatile long lastActivityTime;
    private final long sessionTimeoutMs;

    private final ScheduledExecutorService timeoutChecker;

    public void checkTimeout() {
        long now = System.currentTimeMillis();
        long inactiveDuration = now - lastActivityTime;

        if (inactiveDuration > sessionTimeoutMs) {
            logger.info("Session timeout detected: inactive for {}ms", inactiveDuration);
            closeSession();
            setServerState(ServerState.IDLE);
        }
    }

    public void updateActivity() {
        this.lastActivityTime = System.currentTimeMillis();
    }
}
```

### 7.5 Connection Rejection Logic

**When server is ACTIVE** (has existing session):

```java
public void executePlan(ExecutePlanRequest request,
                        StreamObserver<ExecutePlanResponse> responseObserver) {

    // Check server state
    if (serverState == ServerState.ACTIVE &&
        !request.getSessionId().equals(currentSession.getSessionId())) {

        // Reject with RESOURCE_EXHAUSTED
        responseObserver.onError(
            Status.RESOURCE_EXHAUSTED
                .withDescription(
                    "Server busy - active session in progress. " +
                    "Please try again later or wait for current session to timeout.")
                .asRuntimeException()
        );
        return;
    }

    // Accept request if IDLE or from current session
    // ...
}
```

**gRPC Status Codes**:
| Scenario | Status Code | Description |
|----------|-------------|-------------|
| Server busy | `RESOURCE_EXHAUSTED` | "Server busy - session in progress" |
| Session timeout | `DEADLINE_EXCEEDED` | "Session timed out after {duration}s" |
| Client disconnect | `CANCELLED` | "Client disconnected" |
| Invalid session | `NOT_FOUND` | "Session {id} not found" |

### 7.6 Session Isolation

**Simplified Isolation** (single session):

1. **Data Isolation**: Not applicable (only one session active)
   - All tables/views accessible to the single session
   - No cross-session concerns

2. **Configuration Isolation**: Session-scoped config
   - `spark.sql.ansi.enabled` per session
   - `spark.sql.caseSensitive` per session
   - Reset on session close

3. **Resource Management**: Entire server resources available
   - Memory: Configurable via DuckDB settings (default: 16GB)
   - CPU: All threads available (default: 8)
   - No per-session quotas needed

**Simplified Implementation**:
```java
// No session-specific databases needed
DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:");

// All configuration applied directly
conn.createStatement().execute("SET memory_limit='16GB'");
```

---

## 8. Error Handling

### 8.1 Error Classification

```
┌──────────────────────────────────────────────────────────┐
│               Error Handling Strategy                    │
└──────────────────────────────────────────────────────────┘

1. PROTOCOL ERRORS (gRPC layer)
   ├─ Invalid request format → INVALID_ARGUMENT
   ├─ Session not found → NOT_FOUND
   ├─ Session expired → DEADLINE_EXCEEDED
   └─ Malformed Protobuf → INVALID_ARGUMENT

2. TRANSLATION ERRORS (plan conversion)
   ├─ Unsupported operation → UNIMPLEMENTED
   ├─ Invalid expression → INVALID_ARGUMENT
   ├─ Type mismatch → INVALID_ARGUMENT
   └─ Plan validation failure → FAILED_PRECONDITION

3. EXECUTION ERRORS (query runtime)
   ├─ Table not found → NOT_FOUND
   ├─ Column not found → INVALID_ARGUMENT
   ├─ SQL syntax error → INVALID_ARGUMENT
   ├─ Division by zero → OUT_OF_RANGE
   └─ Query timeout → DEADLINE_EXCEEDED

4. RESOURCE ERRORS (system limits)
   ├─ Out of memory → RESOURCE_EXHAUSTED
   ├─ Connection pool full → RESOURCE_EXHAUSTED
   ├─ Disk full → RESOURCE_EXHAUSTED
   └─ Too many sessions → RESOURCE_EXHAUSTED

5. INTERNAL ERRORS (unexpected)
   ├─ NullPointerException → INTERNAL
   ├─ DuckDB crash → INTERNAL
   └─ Unknown error → INTERNAL
```

### 8.2 Error Handler Implementation

**File**: `connect-server/src/main/java/com/catalyst2sql/protocol/ErrorHandler.java`

```java
package com.catalyst2sql.protocol;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.catalyst2sql.exception.*;
import java.sql.SQLException;

/**
 * Maps exceptions to gRPC status codes.
 */
public class ErrorHandler {

    public static Status toGrpcStatus(Exception e) {
        // SQLException (from DuckDB)
        if (e instanceof SQLException) {
            return handleSQLException((SQLException) e);
        }

        // QueryExecutionException (from QueryExecutor)
        if (e instanceof QueryExecutionException) {
            return handleQueryException((QueryExecutionException) e);
        }

        // IllegalArgumentException (validation)
        if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT
                .withDescription(e.getMessage())
                .withCause(e);
        }

        // UnsupportedOperationException (missing feature)
        if (e instanceof UnsupportedOperationException) {
            return Status.UNIMPLEMENTED
                .withDescription(e.getMessage())
                .withCause(e);
        }

        // Generic fallback
        return Status.INTERNAL
            .withDescription("Internal server error: " + e.getMessage())
            .withCause(e);
    }

    private static Status handleSQLException(SQLException e) {
        String message = e.getMessage().toLowerCase();

        if (message.contains("table") && message.contains("not found")) {
            return Status.NOT_FOUND
                .withDescription("Table not found: " + e.getMessage());
        }

        if (message.contains("column") && message.contains("not found")) {
            return Status.INVALID_ARGUMENT
                .withDescription("Column not found: " + e.getMessage());
        }

        if (message.contains("syntax error")) {
            return Status.INVALID_ARGUMENT
                .withDescription("SQL syntax error: " + e.getMessage());
        }

        // Generic SQL error
        return Status.ABORTED
            .withDescription("Database error: " + e.getMessage())
            .withCause(e);
    }

    private static Status handleQueryException(QueryExecutionException e) {
        // Extract root cause and classify
        Throwable cause = e.getCause();

        if (cause instanceof SQLException) {
            return handleSQLException((SQLException) cause);
        }

        return Status.ABORTED
            .withDescription("Query execution failed: " + e.getMessage())
            .withCause(e);
    }
}
```

### 8.3 Client Error Messages

**Goal**: User-friendly error messages that match Spark's format

```python
# Example: Table not found error
try:
    spark.sql("SELECT * FROM non_existent_table").show()
except Exception as e:
    # Spark: AnalysisException: Table or view not found: non_existent_table
    # catalyst2sql: AnalysisException: Table not found: non_existent_table
    print(e)
```

**Implementation**:
- Map gRPC status codes to Spark exception types
- Preserve error message format (where possible)
- Include query context (SQL, line number)

---

## 9. Performance Considerations

### 9.1 Performance Targets

| Metric | Target | Baseline (Spark 3.5.3) |
|--------|--------|------------------------|
| Query execution | 5-10x faster | Spark Connect local mode |
| Server overhead | < 15% | vs embedded catalyst2sql |
| Session model | Single-session | Multi-session (Spark Connect) |
| Memory usage | < 100 MB base | ~500 MB+ (Spark) |
| Latency (simple query) | < 50 ms | ~200 ms (Spark) |

### 9.2 Optimization Strategies

**1. Query Plan Caching** (Week 15)
```java
// Cache translated SQL for identical queries
ConcurrentHashMap<String, String> planCache = new ConcurrentHashMap<>();

String cachedSQL = planCache.computeIfAbsent(planHash, key -> {
    LogicalPlan plan = planTranslator.translate(sparkPlan);
    return sqlGenerator.generate(plan);
});

// Expected: 80%+ cache hit rate for repeated queries
```

**2. Arrow Batch Optimization**
```java
// Adaptive batch sizing based on data size
int batchSize = result.getRowCount() < 10000
    ? result.getRowCount()  // Small result: single batch
    : 65536;                // Large result: 64K rows per batch

// Expected: 30-50% reduction in serialization overhead
```

**3. Singleton Connection**
```java
// Single DuckDB connection created at server startup
// No pooling overhead (no creation, validation, lifecycle management)
// Direct connection reuse for all queries
// Expected: Zero overhead vs embedded mode
```

**4. Zero-Copy Arrow**
```java
// DuckDB → Arrow (zero-copy via ArrowInterchange)
// Arrow → gRPC (minimal serialization)
// Expected: 3-5x faster than JDBC → JSON → gRPC
```

### 9.3 Bottleneck Analysis

**Potential Bottlenecks** (to monitor):

1. **Plan Translation** (5-10 ms)
   - Mitigation: Plan cache (Week 15)
   - Target: < 5 ms for cached plans

2. **SQL Generation** (2-5 ms)
   - Mitigation: String builder optimization
   - Target: < 2 ms for simple queries

3. **Arrow Serialization** (5-10% of query time)
   - Mitigation: Batch size optimization
   - Target: < 5% overhead

4. **gRPC Overhead** (1-2 ms per RPC)
   - Mitigation: Server-side streaming (single RPC)
   - Target: < 2 ms for schema + data

**Monitoring Plan**:
- Instrument each layer with timing metrics
- Export to Prometheus (Week 16)
- Alert on > 15% overhead vs embedded mode

---

## 10. Security Architecture

### 10.1 Authentication (Week 16)

**Supported Methods**:
1. **No Auth** (development only)
2. **Token-based** (bearer tokens)
3. **TLS Client Certificates** (mTLS)

**Implementation**:
```java
// gRPC interceptor for authentication
public class AuthInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call,
        Metadata headers,
        ServerCallHandler<ReqT, RespT> next) {

        // Extract token from headers
        String token = headers.get(
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
        );

        // Validate token
        if (!validateToken(token)) {
            call.close(Status.UNAUTHENTICATED
                .withDescription("Invalid token"), new Metadata());
            return new ServerCall.Listener<ReqT>() {};
        }

        // Continue with authenticated context
        return next.startCall(call, headers);
    }
}
```

### 10.2 TLS/SSL Configuration

**Server Configuration**:
```java
Server server = ServerBuilder.forPort(15002)
    .useTransportSecurity(
        new File("server-cert.pem"),
        new File("server-key.pem")
    )
    .addService(sparkConnectService)
    .build();
```

**Client Configuration** (PySpark):
```python
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .config("spark.connect.grpc.ssl.enabled", "true") \
    .config("spark.connect.grpc.ssl.trustCertCollectionFilePath", "ca-cert.pem") \
    .getOrCreate()
```

### 10.3 Authorization (Future)

**Row-Level Security** (Phase 6+):
- Filter rows based on user identity
- Integrate with existing authorization systems (Ranger, Sentry)

**Column Masking** (Phase 6+):
- Mask sensitive columns (PII, PHI)
- Dynamic masking based on user roles

---

## 11. Migration Strategy

### 11.1 From Embedded to Server Mode

**Current**: Embedded mode (Week 1-9)
```java
// Application embeds catalyst2sql
DuckDBConnectionManager manager = new DuckDBConnectionManager();
QueryExecutor executor = new QueryExecutor(manager);

LogicalPlan plan = new Project(...);
VectorSchemaRoot result = executor.executeQuery(plan.toSQL());
```

**Target**: Spark Connect mode (Week 10-16)
```python
# Standard Spark client
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

df = spark.sql("SELECT * FROM users")
df.show()
```

**Migration Path**:
1. **Week 10-11**: Server development (embedded mode still works)
2. **Week 12**: TPC-H Q1 via Connect (both modes coexist)
3. **Week 13-16**: Full server implementation (embedded remains supported)
4. **Future**: Deprecate embedded mode (optional)

**Key Insight**: Both modes can coexist indefinitely. Embedded mode is useful for:
- Unit testing
- Benchmarking
- Local development

### 11.2 From Spark to catalyst2sql

**Spark 3.5.3 → catalyst2sql Migration**:

**Step 1**: Install catalyst2sql server
```bash
# Docker deployment
docker run -p 15002:15002 catalyst2sql/connect-server:latest
```

**Step 2**: Update client connection string
```python
# BEFORE (Spark local mode)
spark = SparkSession.builder \
    .master("local[*]") \
    .getOrCreate()

# AFTER (catalyst2sql via Spark Connect)
spark = SparkSession.builder \
    .remote("sc://catalyst2sql-host:15002") \
    .getOrCreate()
```

**Step 3**: Test compatibility
- Run existing Spark SQL queries
- Validate results match Spark 3.5.3
- Measure performance improvement

**Compatibility Matrix**:

| Feature | Spark 3.5.3 | catalyst2sql Week 12 | catalyst2sql Week 16 |
|---------|-------------|----------------------|----------------------|
| SELECT, WHERE, LIMIT | ✅ | ✅ | ✅ |
| JOIN (all types) | ✅ | ✅ | ✅ |
| GROUP BY, Aggregates | ✅ | ✅ | ✅ |
| Window Functions | ✅ | ✅ | ✅ |
| Subqueries | ✅ | ✅ | ✅ |
| CTEs (WITH) | ✅ | ✅ | ✅ |
| UDFs (Python, Scala) | ✅ | ❌ | ❌ (Phase 6+) |
| Streaming | ✅ | ❌ | ❌ (Phase 6+) |
| MLlib | ✅ | ❌ | ❌ (Phase 6+) |

---

## 12. Deployment Architecture

### 12.1 Deployment Topology

**Single-Node Deployment** (Development, Testing):
```
┌────────────────────────────────────────┐
│         catalyst2sql Server            │
│  ┌──────────────────────────────────┐  │
│  │  SparkConnectService (gRPC)      │  │
│  │  Port: 15002                     │  │
│  └──────────────────────────────────┘  │
│  ┌──────────────────────────────────┐  │
│  │  DuckDB (embedded)               │  │
│  │  Max 100 concurrent connections  │  │
│  └──────────────────────────────────┘  │
└────────────────────────────────────────┘
```

**Multi-Node Deployment** (Production, Phase 6+):
```
                Load Balancer
                     │
      ┌──────────────┼──────────────┐
      │              │              │
  ┌───▼───┐      ┌───▼───┐      ┌───▼───┐
  │Server │      │Server │      │Server │
  │  #1   │      │  #2   │      │  #3   │
  └───────┘      └───────┘      └───────┘
      │              │              │
      └──────────────┴──────────────┘
                     │
            Shared Storage (S3)
       (Parquet files, Delta, Iceberg)
```

### 12.2 Docker Deployment

**Dockerfile**:
```dockerfile
FROM eclipse-temurin:17-jre-alpine

# Install DuckDB native libraries
RUN apk add --no-cache libstdc++

# Copy application JAR
COPY target/catalyst2sql-connect-server-0.2.0-SNAPSHOT.jar /app/server.jar

# Expose Spark Connect port
EXPOSE 15002

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=3s \
  CMD nc -z localhost 15002 || exit 1

# Run server
ENTRYPOINT ["java", "-jar", "/app/server.jar"]
```

**Docker Compose** (development):
```yaml
version: '3.8'
services:
  catalyst2sql:
    build: .
    ports:
      - "15002:15002"
    environment:
      - CATALYST2SQL_PORT=15002
      - CATALYST2SQL_MAX_SESSIONS=100
      - CATALYST2SQL_SESSION_TIMEOUT=30m
      - CATALYST2SQL_MEMORY_LIMIT=16GB
    volumes:
      - ./data:/data  # Mount TPC-H data
    restart: unless-stopped
```

### 12.3 Kubernetes Deployment

**Deployment YAML**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: catalyst2sql-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: catalyst2sql
  template:
    metadata:
      labels:
        app: catalyst2sql
    spec:
      containers:
      - name: server
        image: catalyst2sql/connect-server:0.2.0
        ports:
        - containerPort: 15002
          name: grpc
        env:
        - name: CATALYST2SQL_PORT
          value: "15002"
        - name: CATALYST2SQL_MAX_SESSIONS
          value: "100"
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "16Gi"
            cpu: "8"
        livenessProbe:
          tcpSocket:
            port: 15002
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 15002
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: catalyst2sql-service
spec:
  selector:
    app: catalyst2sql
  ports:
  - protocol: TCP
    port: 15002
    targetPort: 15002
  type: LoadBalancer
```

**Horizontal Pod Autoscaling**:
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: catalyst2sql-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: catalyst2sql-server
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

---

## 13. Appendix

### 13.1 Spark Connect Protocol Overview

**Protobuf Messages** (from Spark 3.5.3):
```protobuf
// spark-connect.proto (simplified)

message ExecutePlanRequest {
  string session_id = 1;
  Relation plan = 2;
  map<string, string> client_info = 3;
}

message ExecutePlanResponse {
  oneof response_type {
    ArrowBatch arrow_batch = 1;
    Schema schema = 2;
    Metrics metrics = 3;
  }

  message ArrowBatch {
    bytes data = 1;
    int64 row_count = 2;
  }
}

message Relation {
  oneof rel_type {
    Read read = 1;
    Project project = 2;
    Filter filter = 3;
    Join join = 4;
    Aggregate aggregate = 5;
    Sort sort = 6;
    Limit limit = 7;
    // ... 20+ more types
  }
}
```

### 13.2 Key Interfaces

**SparkConnectService (gRPC)**:
```java
public interface SparkConnectService {
    // Execute query and stream results
    void executePlan(
        ExecutePlanRequest request,
        StreamObserver<ExecutePlanResponse> responseObserver
    );

    // Analyze plan (session creation, schema inference)
    void analyzePlan(
        AnalyzePlanRequest request,
        StreamObserver<AnalyzePlanResponse> responseObserver
    );

    // Config management
    void config(
        ConfigRequest request,
        StreamObserver<ConfigResponse> responseObserver
    );
}
```

**PlanTranslator**:
```java
public interface PlanTranslator {
    LogicalPlan translate(Relation sparkPlan);
}
```

**SessionManager**:
```java
public interface SessionManager {
    Session getOrCreateSession(String sessionId);
    Session getSession(String sessionId);
    void closeSession(String sessionId);
    int getActiveSessionCount();
    void shutdown();
}
```

### 13.3 Performance Benchmarks (Projected)

**TPC-H SF=0.01 (10 MB, Week 12 target)**:

| Query | Spark 3.5.3 | catalyst2sql (projected) | Speedup |
|-------|-------------|--------------------------|---------|
| Q1 | 3.0s | 0.5s | 6.0x |
| Q3 | 8.0s | 1.2s | 6.7x |
| Q6 | 1.0s | 0.12s | 8.3x |

**Server Overhead**:
- Embedded mode: 0.5s
- Server mode: 0.55s
- Overhead: 10% (target: < 15%)

---

## 14. References

### 14.1 Internal Documents

- `/workspaces/catalyst2sql/IMPLEMENTATION_PLAN.md` - Overall project plan
- `/workspaces/catalyst2sql/docs/Testing_Strategy.md` - Testing approach
- `/workspaces/catalyst2sql/benchmarks/README.md` - TPC-H benchmarking

### 14.2 External References

- [Spark Connect Protocol](https://github.com/apache/spark/tree/v3.5.3/connector/connect/common/src/main/protobuf)
- [gRPC Java Tutorial](https://grpc.io/docs/languages/java/basics/)
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- [DuckDB API](https://duckdb.org/docs/api/overview)

### 14.3 Code Examples

All code examples in this document are illustrative. Final implementations will be developed in Weeks 10-16 with full test coverage.

---

**Document End**

**Next Steps**:
1. Review and approve this architecture (Week 10)
2. Create `connect-server` Maven module (Week 10)
3. Set up Protobuf code generation (Week 10)
4. Begin implementation of SparkConnectService (Week 11)

**Questions for Review**:
1. Is the module structure appropriate?
2. Should we use Option 1 or Option 2 for Protobuf setup?
3. Are the performance targets realistic?
4. Any missing security considerations?
5. Is the migration strategy clear?
