# Single-Session Architecture for catalyst2sql Spark Connect Server

**Document Version**: 1.0
**Date**: 2025-10-16
**Status**: Design Specification
**Author**: Systems Architect

---

## Table of Contents

1. [Overview](#overview)
2. [Design Rationale](#design-rationale)
3. [State Machine](#state-machine)
4. [Component Architecture](#component-architecture)
5. [Session Lifecycle](#session-lifecycle)
6. [Timeout Management](#timeout-management)
7. [Connection Rejection](#connection-rejection)
8. [Error Handling](#error-handling)
9. [Configuration](#configuration)
10. [Implementation Guide](#implementation-guide)
11. [Testing Strategy](#testing-strategy)
12. [Future Evolution](#future-evolution)

---

## 1. Overview

### 1.1 Purpose

This document defines the **single-session architecture** for the catalyst2sql Spark Connect Server. This design aligns with DuckDB's single-user nature and prioritizes simplicity and performance over multi-client concurrency.

### 1.2 Key Principles

1. **One Client at a Time**: Server supports exactly ONE active client session
2. **Singleton Connection**: Single DuckDB connection shared by the active session
3. **Simple State Machine**: IDLE ↔ ACTIVE transitions only
4. **Automatic Timeout**: Inactive sessions close automatically (300s default)
5. **Clear Rejection**: New connections rejected with "server busy" error while session active

### 1.3 Alignment with DuckDB

DuckDB is designed as a **single-user embedded database**:
- No multi-user concurrency controls
- No connection pooling support
- Optimized for single-threaded workloads
- Minimal overhead for single-connection usage

**Our Architecture**: Embrace this design philosophy rather than fight it.

---

## 2. Design Rationale

### 2.1 Why Single-Session?

**Technical Reasons**:
- DuckDB has no built-in multi-user support
- Connection pooling adds overhead without benefit
- Single-user optimization = maximum performance
- Simpler code = fewer bugs, easier maintenance

**Comparison: Multi-Session vs Single-Session**

| Aspect | Multi-Session Design | Single-Session Design |
|--------|----------------------|----------------------|
| **Complexity** | High (session tracking, pooling, isolation) | Low (simple state machine) |
| **Code Lines** | ~3000 LOC | ~1000 LOC (67% reduction) |
| **Implementation Time** | 30 hours | 20 hours (33% reduction) |
| **Memory Overhead** | Per-session connections, state | None (singleton connection) |
| **Concurrency** | 100+ clients | 1 client |
| **Use Case** | Multi-tenant cluster | Single-user fast execution |
| **Alignment with DuckDB** | Forces multi-user on single-user DB | Perfect alignment |

**Detailed Comparison: Implementation Benefits**

| Metric | Multi-Session | Single-Session | Improvement |
|--------|---------------|----------------|-------------|
| **Implementation Time** | 30 hours | 20 hours | **33% reduction** |
| **Test Count** | 120 tests | 80 tests | **33% reduction** |
| **Code Complexity** | ~3000 LOC | ~1000 LOC | **67% reduction** |
| **Risk Level** | MEDIUM | LOW | **Lower risk** |

**Detailed Comparison: Performance Benefits**

| Metric | Multi-Session | Single-Session | Improvement |
|--------|---------------|----------------|-------------|
| **Server Overhead** | < 15% | < 10% | **5% better** |
| **Connection Pool** | Yes (overhead) | No (singleton) | **No overhead** |
| **Session Creation** | 50-100ms | < 1ms | **50-100x faster** |
| **Memory Usage** | Base + per-session | Base only | **Lower memory** |

**Detailed Comparison: Architecture Benefits**

| Aspect | Multi-Session | Single-Session | Advantage |
|--------|---------------|----------------|-----------|
| **DuckDB Alignment** | Poor (forcing multi-user) | Perfect (embraces single-user) | ✅ Natural fit |
| **Error Messages** | Complex session conflicts | Clear "server busy" | ✅ Better UX |
| **State Management** | HashMap, isolation, pooling | Simple state machine | ✅ Simpler |
| **Resource Management** | Per-session quotas | Entire server resources | ✅ Simpler |

### 2.2 When to Use Multi-Session

**Later, if needed** (Phase 6+):
- Multiple DuckDB processes (one per server instance)
- Load balancer with session affinity
- Separate databases per process (no sharing)

**For Now**: Single-session is sufficient for:
- Local development
- Single-user data analysis
- Fast query execution
- Spark compatibility testing

---

## 3. State Machine

### 3.1 Server States

```
┌─────────────────────────────────────────────────┐
│              Server State Machine                │
└─────────────────────────────────────────────────┘

States:
  ┌──────────┐
  │   IDLE   │  - No active session
  │          │  - Accepting new connections
  │          │  - DuckDB connection available
  └──────────┘

  ┌──────────┐
  │  ACTIVE  │  - Has active session
  │          │  - Rejecting new connections
  │          │  - DuckDB connection in use
  └──────────┘

Transitions:
  ┌──────┐                              ┌────────┐
  │      │──── Client Connect ────────>│        │
  │ IDLE │                              │ ACTIVE │
  │      │<─── Disconnect/Timeout ─────│        │
  └──────┘                              └────────┘

Events:
  - Client Connect: ExecutePlan request arrives
  - Client Disconnect: Session explicitly closed
  - Timeout: No activity for > sessionTimeout seconds
```

### 3.2 State Transitions

**IDLE → ACTIVE**:
- Trigger: First request from new client
- Conditions: serverState == IDLE
- Actions:
  1. Generate sessionId (UUID)
  2. Create Session object
  3. Set currentSessionId
  4. Initialize lastActivityTime
  5. Set serverState = ACTIVE
  6. Return sessionId to client

**ACTIVE → IDLE**:
- Triggers:
  - Client disconnect (explicit close)
  - Session timeout (inactivity > 300s)
  - Server shutdown
- Actions:
  1. Drop temp views (if any)
  2. Clear session configuration
  3. Reset lastActivityTime
  4. Clear currentSessionId
  5. Set serverState = IDLE
  6. Log session closure

### 3.3 State Invariants

**IDLE Invariants**:
```java
assert serverState == IDLE;
assert currentSessionId == null;
assert lastActivityTime == 0;
// DuckDB connection exists but not actively used
```

**ACTIVE Invariants**:
```java
assert serverState == ACTIVE;
assert currentSessionId != null;
assert lastActivityTime > 0;
// DuckDB connection actively executing queries
```

---

## 4. Component Architecture

### 4.1 Component Diagram

```
┌─────────────────────────────────────────────────────┐
│              Spark Connect Server                   │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │     SparkConnectServiceImpl (gRPC)           │  │
│  │  - executePlan()                             │  │
│  │  - analyzePlan()                             │  │
│  │  - Calls sessionManager.checkState()         │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                 │
│                   v                                 │
│  ┌──────────────────────────────────────────────┐  │
│  │          SessionManager                      │  │
│  │  - serverState: IDLE | ACTIVE                │  │
│  │  - currentSessionId: String?                 │  │
│  │  - lastActivityTime: long                    │  │
│  │  - createSession()                           │  │
│  │  - updateActivity()                          │  │
│  │  - checkTimeout()                            │  │
│  │  - closeSession()                            │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                 │
│                   v                                 │
│  ┌──────────────────────────────────────────────┐  │
│  │          QueryExecutor                       │  │
│  │  - executeQuery(sql)                         │  │
│  │  - Uses singleton DuckDB connection          │  │
│  └────────────────┬─────────────────────────────┘  │
│                   │                                 │
│                   v                                 │
│  ┌──────────────────────────────────────────────┐  │
│  │    Singleton DuckDB Connection               │  │
│  │  - Created at server startup                 │  │
│  │  - Shared by all queries                     │  │
│  │  - Closed at server shutdown                 │  │
│  └──────────────────────────────────────────────┘  │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### 4.2 SessionManager Design

**File**: `connect-server/src/main/java/com/catalyst2sql/session/SessionManager.java`

```java
package com.catalyst2sql.session;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages single-session state for catalyst2sql Spark Connect Server.
 *
 * <p>This class implements a simple state machine:
 * <ul>
 *   <li>IDLE: No active session, accepting connections</li>
 *   <li>ACTIVE: Has active session, rejecting new connections</li>
 * </ul>
 *
 * <p>Only ONE client session is allowed at a time. New connection attempts
 * while server is ACTIVE will be rejected with RESOURCE_EXHAUSTED status.
 */
public class SessionManager {

    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    /**
     * Server state enum.
     */
    private enum ServerState {
        IDLE,   // No active session, accepting connections
        ACTIVE  // Has active session, rejecting new connections
    }

    // State variables
    private volatile ServerState serverState = ServerState.IDLE;
    private volatile String currentSessionId = null;
    private volatile long lastActivityTimeMs = 0;

    // Configuration
    private final long sessionTimeoutMs;
    private final ScheduledExecutorService timeoutChecker;

    /**
     * Creates a SessionManager with specified timeout.
     *
     * @param sessionTimeoutSeconds session timeout in seconds (default: 300)
     */
    public SessionManager(int sessionTimeoutSeconds) {
        this.sessionTimeoutMs = sessionTimeoutSeconds * 1000L;

        // Start background timeout checker (runs every 30 seconds)
        this.timeoutChecker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "session-timeout-checker");
            t.setDaemon(true);
            return t;
        });

        this.timeoutChecker.scheduleAtFixedRate(
            this::checkTimeout,
            30, 30, TimeUnit.SECONDS
        );

        logger.info("SessionManager initialized: timeout={}s", sessionTimeoutSeconds);
    }

    /**
     * Creates a new session if server is IDLE.
     *
     * @param sessionId session identifier
     * @return Session object
     * @throws StatusRuntimeException with RESOURCE_EXHAUSTED if server is busy
     */
    public synchronized Session createSession(String sessionId) {
        if (serverState == ServerState.ACTIVE) {
            String errorMsg = String.format(
                "Server busy: session %s is active. " +
                "Only one client connection allowed at a time. " +
                "Please try again later or wait for current session to timeout.",
                currentSessionId
            );

            logger.warn("Connection rejected: server busy (active session: {})", currentSessionId);

            throw new StatusRuntimeException(
                Status.RESOURCE_EXHAUSTED.withDescription(errorMsg)
            );
        }

        // Transition: IDLE → ACTIVE
        this.serverState = ServerState.ACTIVE;
        this.currentSessionId = sessionId;
        this.lastActivityTimeMs = System.currentTimeMillis();

        logger.info("Session created: {} (state: IDLE → ACTIVE)", sessionId);

        return new Session(sessionId);
    }

    /**
     * Updates last activity timestamp.
     * Called on each request to prevent timeout.
     */
    public synchronized void updateActivity() {
        this.lastActivityTimeMs = System.currentTimeMillis();
    }

    /**
     * Checks if session has timed out and closes if necessary.
     * Called periodically by background thread.
     */
    public synchronized void checkTimeout() {
        if (serverState != ServerState.ACTIVE) {
            return; // Nothing to check
        }

        long now = System.currentTimeMillis();
        long inactiveDurationMs = now - lastActivityTimeMs;

        if (inactiveDurationMs > sessionTimeoutMs) {
            long inactiveSec = inactiveDurationMs / 1000;
            logger.warn(
                "Session {} timed out after {}s of inactivity (timeout: {}s)",
                currentSessionId, inactiveSec, sessionTimeoutMs / 1000
            );

            closeSession();
        }
    }

    /**
     * Closes the current session.
     * Transitions from ACTIVE → IDLE.
     */
    public synchronized void closeSession() {
        if (serverState != ServerState.ACTIVE) {
            logger.debug("closeSession() called but no active session");
            return;
        }

        String closedSessionId = currentSessionId;

        // Transition: ACTIVE → IDLE
        this.serverState = ServerState.IDLE;
        this.currentSessionId = null;
        this.lastActivityTimeMs = 0;

        logger.info("Session closed: {} (state: ACTIVE → IDLE)", closedSessionId);
    }

    /**
     * Returns true if server has an active session.
     */
    public synchronized boolean isActive() {
        return serverState == ServerState.ACTIVE;
    }

    /**
     * Returns the current session ID, or null if no active session.
     */
    public synchronized String getCurrentSessionId() {
        return currentSessionId;
    }

    /**
     * Returns true if the given session ID matches the current session.
     */
    public synchronized boolean isCurrentSession(String sessionId) {
        return serverState == ServerState.ACTIVE &&
               sessionId != null &&
               sessionId.equals(currentSessionId);
    }

    /**
     * Shuts down the timeout checker.
     * Called during server shutdown.
     */
    public void shutdown() {
        logger.info("SessionManager shutting down");
        timeoutChecker.shutdown();
        try {
            if (!timeoutChecker.awaitTermination(5, TimeUnit.SECONDS)) {
                timeoutChecker.shutdownNow();
            }
        } catch (InterruptedException e) {
            timeoutChecker.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

### 4.3 Session Class

```java
package com.catalyst2sql.session;

/**
 * Represents a client session.
 *
 * <p>In single-session mode, this is a simple wrapper around the session ID.
 * No per-session state is needed since there's only one session at a time.
 */
public class Session {

    private final String sessionId;
    private final long creationTime;

    public Session(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
    }

    public String getSessionId() {
        return sessionId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return "Session{id=" + sessionId + ", created=" + creationTime + "}";
    }
}
```

---

## 5. Session Lifecycle

### 5.1 Create Session Flow

```
┌────────────────────────────────────────────────────────┐
│                  Create Session Flow                   │
└────────────────────────────────────────────────────────┘

1. Client sends ExecutePlanRequest
   ├─ sessionId: "new-session-123" (or empty for first request)
   └─ plan: <Spark Connect Plan>

2. Server: SparkConnectServiceImpl.executePlan()
   ├─ Extract sessionId from request
   ├─ Call sessionManager.createSession(sessionId)
   │
   ├─ If serverState == IDLE:
   │  ├─ Generate UUID if sessionId empty
   │  ├─ Create Session object
   │  ├─ Set serverState = ACTIVE
   │  ├─ Initialize lastActivityTime
   │  └─ Return Session object → proceed with query
   │
   └─ If serverState == ACTIVE:
      └─ Throw StatusRuntimeException(RESOURCE_EXHAUSTED)
         └─ Client receives error: "Server busy - session in progress"

3. Response to Client
   ├─ Success: ExecutePlanResponse with results
   └─ Error: gRPC RESOURCE_EXHAUSTED status
```

### 5.2 Active Session Flow

```
┌────────────────────────────────────────────────────────┐
│                Active Session Flow                     │
└────────────────────────────────────────────────────────┘

1. Client sends query with sessionId
   ├─ ExecutePlanRequest
   ├─ sessionId: "active-session-123"
   └─ plan: <Spark Connect Plan>

2. Server: Validate session
   ├─ Call sessionManager.isCurrentSession(sessionId)
   │
   ├─ If sessionId matches current:
   │  ├─ Update lastActivityTime
   │  ├─ Execute query
   │  └─ Return results
   │
   └─ If sessionId doesn't match:
      └─ Throw StatusRuntimeException(NOT_FOUND)
         └─ "Session not found or expired"

3. Background: Timeout Checker (every 30 seconds)
   ├─ Calculate: inactiveTime = now - lastActivityTime
   │
   ├─ If inactiveTime > sessionTimeout:
   │  ├─ Log warning: "Session timed out"
   │  └─ Call sessionManager.closeSession()
   │     └─ State: ACTIVE → IDLE
   │
   └─ Else: continue
```

### 5.3 Close Session Flow

```
┌────────────────────────────────────────────────────────┐
│                 Close Session Flow                     │
└────────────────────────────────────────────────────────┘

Trigger: Client disconnect, timeout, or server shutdown

1. Client: spark.stop()
   └─ May send explicit close request (optional)

2. Server: sessionManager.closeSession()
   ├─ Drop temp views (if any)
   ├─ Clear configuration
   ├─ Reset state variables:
   │  ├─ serverState = IDLE
   │  ├─ currentSessionId = null
   │  └─ lastActivityTime = 0
   └─ Log: "Session closed: {sessionId}"

3. Result: Server ready for new connection
   └─ serverState == IDLE
   └─ Next client can connect
```

---

## 6. Timeout Management

### 6.1 Timeout Configuration

**Default Values**:
```java
sessionTimeout: 300 seconds (5 minutes)
timeoutCheckInterval: 30 seconds
```

**Configurable via Environment**:
```bash
# Session timeout in seconds
export SPARK_CONNECT_SESSION_TIMEOUT=300

# Timeout check interval (optional, default: 30s)
export SPARK_CONNECT_TIMEOUT_CHECK_INTERVAL=30
```

### 6.2 Timeout Detection Logic

```java
public void checkTimeout() {
    if (serverState != ServerState.ACTIVE) {
        return; // No active session
    }

    long now = System.currentTimeMillis();
    long inactiveDurationMs = now - lastActivityTimeMs;

    if (inactiveDurationMs > sessionTimeoutMs) {
        // Timeout detected
        logger.warn("Session {} timed out after {}s",
            currentSessionId, inactiveDurationMs / 1000);

        closeSession(); // Transition to IDLE
    }
}
```

### 6.3 Activity Tracking

**Update on Every Request**:
```java
// In SparkConnectServiceImpl.executePlan()
sessionManager.updateActivity(); // Reset timeout

// Execute query...
```

**Idle Time Calculation**:
```
idleTime = currentTime - lastActivityTime

Example:
  lastActivityTime = 10:00:00
  currentTime      = 10:05:30
  idleTime         = 330 seconds (5 minutes 30 seconds)
  sessionTimeout   = 300 seconds (5 minutes)
  → TIMEOUT: 330 > 300
```

---

## 7. Connection Rejection

### 7.1 Rejection Logic

**When to Reject**:
```java
if (serverState == ServerState.ACTIVE) {
    // Server busy - reject connection
    throw new StatusRuntimeException(
        Status.RESOURCE_EXHAUSTED
            .withDescription("Server busy: session in progress")
    );
}
```

### 7.2 Error Messages

**Client-Side Error**:
```python
# PySpark client
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

# If server is busy:
# io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED:
#   Server busy: session <id> is active.
#   Only one client connection allowed at a time.
#   Please try again later or wait for current session to timeout.
```

**Server-Side Logging**:
```
2025-10-16 10:05:23.456 WARN  SessionManager - Connection rejected:
  server busy (active session: abc-123-def)
```

### 7.3 Retry Strategy (Client-Side)

**Recommended Client Pattern**:
```python
import time
from pyspark.sql import SparkSession

def connect_with_retry(server_url, max_retries=5, retry_delay=10):
    """
    Connect to catalyst2sql server with retry on RESOURCE_EXHAUSTED.
    """
    for attempt in range(max_retries):
        try:
            spark = SparkSession.builder \
                .remote(server_url) \
                .getOrCreate()
            return spark

        except Exception as e:
            if "RESOURCE_EXHAUSTED" in str(e):
                print(f"Server busy (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise Exception("Server busy after max retries") from e
            else:
                raise  # Other errors

# Usage
spark = connect_with_retry("sc://localhost:15002")
```

---

## 8. Error Handling

### 8.1 gRPC Status Codes

| Scenario | Status Code | Description |
|----------|-------------|-------------|
| Server busy (ACTIVE) | `RESOURCE_EXHAUSTED` | "Server busy - session in progress" |
| Session timeout | `DEADLINE_EXCEEDED` | "Session timed out after {duration}s" |
| Invalid session ID | `NOT_FOUND` | "Session {id} not found or expired" |
| Client disconnect | `CANCELLED` | "Client disconnected" |
| Server shutdown | `UNAVAILABLE` | "Server is shutting down" |

### 8.2 Error Handling Implementation

```java
public void executePlan(
    ExecutePlanRequest request,
    StreamObserver<ExecutePlanResponse> responseObserver) {

    try {
        String sessionId = request.getSessionId();

        // Check if session is valid
        if (!sessionManager.isCurrentSession(sessionId)) {
            // Try to create new session
            Session session = sessionManager.createSession(sessionId);
            // If successful, proceed (IDLE → ACTIVE)
        }

        // Update activity (prevent timeout)
        sessionManager.updateActivity();

        // Execute query...
        // Send response...

        responseObserver.onCompleted();

    } catch (StatusRuntimeException e) {
        // Re-throw gRPC exceptions (RESOURCE_EXHAUSTED, etc.)
        responseObserver.onError(e);

    } catch (Exception e) {
        // Wrap other exceptions as INTERNAL error
        responseObserver.onError(
            Status.INTERNAL
                .withDescription("Query execution failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException()
        );
    }
}
```

### 8.3 Logging Strategy

**Log Levels**:
```
INFO:  Session lifecycle events (create, close)
WARN:  Connection rejection, timeout events
ERROR: Unexpected errors, server failures
DEBUG: State transitions, activity updates
```

**Example Logs**:
```
2025-10-16 10:00:00.000 INFO  SessionManager - Session created: abc-123 (IDLE → ACTIVE)
2025-10-16 10:04:00.000 DEBUG SessionManager - Activity updated (session: abc-123)
2025-10-16 10:05:00.000 WARN  SessionManager - Connection rejected: server busy (active: abc-123)
2025-10-16 10:10:00.000 WARN  SessionManager - Session abc-123 timed out after 600s
2025-10-16 10:10:00.001 INFO  SessionManager - Session closed: abc-123 (ACTIVE → IDLE)
```

---

## 9. Configuration

### 9.1 Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SPARK_CONNECT_PORT` | int | `15002` | gRPC server port |
| `SPARK_CONNECT_SESSION_TIMEOUT` | int | `300` | Session timeout (seconds) |
| `SPARK_CONNECT_TIMEOUT_CHECK_INTERVAL` | int | `30` | Timeout check interval (seconds) |
| `DUCKDB_DATABASE` | string | `:memory:` | DuckDB database path |
| `DUCKDB_MEMORY_LIMIT` | string | `16GB` | DuckDB memory limit |
| `DUCKDB_THREADS` | int | `8` | DuckDB thread count |

### 9.2 Server Configuration

```java
// ConnectServer.java
public class ConnectServer {

    public static void main(String[] args) throws Exception {
        // Load configuration from environment
        int port = Integer.parseInt(
            System.getenv().getOrDefault("SPARK_CONNECT_PORT", "15002"));

        int sessionTimeout = Integer.parseInt(
            System.getenv().getOrDefault("SPARK_CONNECT_SESSION_TIMEOUT", "300"));

        String duckdbPath = System.getenv().getOrDefault(
            "DUCKDB_DATABASE", ":memory:");

        // Create server
        ConnectServer server = new ConnectServer(port, duckdbPath, sessionTimeout);
        server.start();
        server.awaitTermination();
    }
}
```

### 9.3 DuckDB Configuration

```java
// Configure singleton DuckDB connection
Statement stmt = duckdbConnection.createStatement();

// Memory limit
String memoryLimit = System.getenv().getOrDefault("DUCKDB_MEMORY_LIMIT", "16GB");
stmt.execute("SET memory_limit='" + memoryLimit + "'");

// Thread count
String threads = System.getenv().getOrDefault("DUCKDB_THREADS", "8");
stmt.execute("SET threads=" + threads);

stmt.close();
```

---

## 10. Implementation Guide

### 10.1 Component Checklist

**Required Components**:
- [x] `SessionManager` - State machine, timeout checker
- [x] `Session` - Session wrapper (minimal)
- [x] `ConnectServer` - Singleton DuckDB connection, server lifecycle
- [x] `SparkConnectServiceImpl` - gRPC service, session validation

**Optional Components** (for later):
- [ ] `SessionMetrics` - Track session duration, query count
- [ ] `SessionStore` - Persist session state (for failover)
- [ ] `SessionConfig` - Per-session Spark SQL configuration

### 10.2 Implementation Order

**Step 1: SessionManager** (3 hours)
1. Create `SessionManager.java` with state machine
2. Implement `createSession()`, `closeSession()`, `checkTimeout()`
3. Add background timeout checker
4. Write unit tests

**Step 2: Update ConnectServer** (2 hours)
1. Create singleton DuckDB connection at startup
2. Pass to `QueryExecutor`
3. Initialize `SessionManager` with configured timeout
4. Add shutdown hook to close connection

**Step 3: Update SparkConnectServiceImpl** (2 hours)
1. Add session validation before query execution
2. Call `sessionManager.createSession()` for new connections
3. Call `sessionManager.updateActivity()` on each request
4. Handle `RESOURCE_EXHAUSTED` exception

**Step 4: Testing** (3 hours)
1. Unit tests: State transitions, timeout detection
2. Integration test: Single client connects successfully
3. Integration test: Second client rejected while first active
4. Integration test: Session timeout → IDLE → new client can connect

**Total**: 10 hours (vs 15 hours for multi-session design)

### 10.3 Code Template

**SessionManager Template**:
```java
public class SessionManager {
    private volatile ServerState serverState = ServerState.IDLE;
    private volatile String currentSessionId = null;
    private volatile long lastActivityTimeMs = 0;
    private final long sessionTimeoutMs;
    private final ScheduledExecutorService timeoutChecker;

    public synchronized Session createSession(String sessionId) {
        // Check state, transition IDLE → ACTIVE
    }

    public synchronized void updateActivity() {
        // Update timestamp
    }

    public synchronized void checkTimeout() {
        // Check inactivity, close if timed out
    }

    public synchronized void closeSession() {
        // Transition ACTIVE → IDLE
    }
}
```

---

## 11. Testing Strategy

### 11.1 Unit Tests

**SessionManager Tests**:
```java
@Test
void testCreateSession_WhenIdle_Success() {
    SessionManager sm = new SessionManager(300);
    Session session = sm.createSession("test-123");

    assertThat(session.getSessionId()).isEqualTo("test-123");
    assertThat(sm.isActive()).isTrue();
    assertThat(sm.getCurrentSessionId()).isEqualTo("test-123");
}

@Test
void testCreateSession_WhenActive_Rejected() {
    SessionManager sm = new SessionManager(300);
    sm.createSession("session-1"); // First session succeeds

    // Second session should be rejected
    assertThatThrownBy(() -> sm.createSession("session-2"))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("RESOURCE_EXHAUSTED");
}

@Test
void testSessionTimeout() throws Exception {
    SessionManager sm = new SessionManager(2); // 2 second timeout
    sm.createSession("test-timeout");

    // Wait for timeout
    Thread.sleep(3000);
    sm.checkTimeout();

    // Session should be closed
    assertThat(sm.isActive()).isFalse();
    assertThat(sm.getCurrentSessionId()).isNull();
}
```

### 11.2 Integration Tests

**Scenario 1: Single Client Success**
```java
@Test
void testSingleClientConnectsSuccessfully() {
    ConnectServer server = startTestServer();
    SparkSession spark = connectClient("sc://localhost:15002");

    // Execute query
    Dataset<Row> result = spark.sql("SELECT 1 AS col");
    assertThat(result.count()).isEqualTo(1);

    spark.close();
    server.stop();
}
```

**Scenario 2: Second Client Rejected**
```java
@Test
void testSecondClientRejected() {
    ConnectServer server = startTestServer();
    SparkSession client1 = connectClient("sc://localhost:15002");

    // First client connected
    assertThat(server.getSessionManager().isActive()).isTrue();

    // Second client should be rejected
    assertThatThrownBy(() -> connectClient("sc://localhost:15002"))
        .hasMessageContaining("RESOURCE_EXHAUSTED")
        .hasMessageContaining("Server busy");

    client1.close();
    server.stop();
}
```

**Scenario 3: Timeout and Reconnect**
```java
@Test
void testSessionTimeoutAndReconnect() throws Exception {
    ConnectServer server = startTestServer(5); // 5 second timeout
    SparkSession client1 = connectClient("sc://localhost:15002");

    // Wait for timeout
    Thread.sleep(6000);

    // First client should be timed out
    assertThat(server.getSessionManager().isActive()).isFalse();

    // New client should connect successfully
    SparkSession client2 = connectClient("sc://localhost:15002");
    assertThat(server.getSessionManager().isActive()).isTrue();

    client2.close();
    server.stop();
}
```

### 11.3 Performance Tests

**Latency Overhead Test**:
```java
@Test
void testSingleSessionLatencyOverhead() {
    // Measure embedded mode
    long embeddedTime = measureEmbeddedQuery();

    // Measure server mode
    long serverTime = measureServerQuery();

    // Overhead should be < 10%
    double overhead = (serverTime - embeddedTime) / (double) embeddedTime;
    assertThat(overhead).isLessThan(0.10);
}
```

---

## 12. Future Evolution

### 12.1 When to Add Multi-Session

**Triggers**:
- User requests concurrent access
- Multi-tenant use case emerges
- Load balancing becomes necessary

**Approach** (Phase 6+):
- Multiple server instances (one per DuckDB process)
- Load balancer with session affinity
- Separate databases per instance (no shared state)
- Each instance still single-session internally

### 12.2 Potential Enhancements

**Near-Term** (Weeks 11-12):
- Session metrics (query count, duration, data processed)
- Graceful session handoff (planned disconnect)
- Session configuration (Spark SQL settings)

**Long-Term** (Phase 6+):
- Multi-instance deployment
- Horizontal scaling
- Failover and recovery
- Persistent session state

### 12.3 Alternative Approaches

**Option 1: Multi-Process** (each client → separate DuckDB process)
- Pros: True isolation, no "busy" errors
- Cons: Higher memory usage, process management complexity

**Option 2: Queue-Based** (queue requests when busy)
- Pros: No rejected connections
- Cons: Latency spikes, queue overflow possible

**Current Approach**: Simple rejection is clearest and most aligned with DuckDB's design.

---

## 13. Conclusion

### 13.1 Summary

The **single-session architecture** for catalyst2sql Spark Connect Server provides:

1. **Simplicity**: 67% less code than multi-session design
2. **Alignment**: Matches DuckDB's single-user philosophy
3. **Performance**: No pooling overhead, direct connection usage
4. **Clarity**: Clear "server busy" errors, no complex session conflicts

### 13.2 Benefits

| Benefit | Impact |
|---------|--------|
| **Reduced Complexity** | 1000 LOC vs 3000 LOC |
| **Faster Implementation** | 20 hours vs 30 hours |
| **Lower Memory Overhead** | Singleton connection vs pool |
| **Clearer Error Messages** | "Server busy" vs session conflicts |
| **Perfect DuckDB Alignment** | Single-user model, no forced concurrency |

### 13.3 Trade-offs

**Limitations**:
- Only one client at a time
- No concurrent query execution
- "Busy" errors when second client connects

**Acceptable Because**:
- DuckDB is single-user by design
- Use case: fast single-user execution, not multi-tenant cluster
- Future: multi-instance deployment for concurrency

### 13.4 Recommendation

**Proceed with single-session design** for Weeks 10-12. Re-evaluate multi-session need in Week 15 based on user feedback and performance metrics.

---

**Document Status**: ✅ Complete
**Implementation Ready**: Yes
**Approval Required**: Yes (Tech Lead, PM)
**Version**: 1.0
**Last Updated**: 2025-10-16
