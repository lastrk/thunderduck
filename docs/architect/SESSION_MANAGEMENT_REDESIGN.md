# Session Management Redesign

This document describes the redesigned session management architecture for Thunderduck's Spark Connect server, addressing the limitations of the current single-session implementation.

## Problem Statement

The current implementation locks to the first session ID and rejects all subsequent connections with different session IDs. This causes:
- E2E test failures (each test creates new SparkSession with new UUID)
- Poor developer experience (server restart required between sessions)
- Incompatibility with standard PySpark client behavior

## Design Goals

1. **Single active execution** - Only one query runs at a time (DuckDB limitation)
2. **Idle session replacement** - New session replaces idle session seamlessly
3. **Wait queue** - New sessions wait if execution in progress (max 10 waiters)
4. **Queue overflow rejection** - 11th client gets "server busy" response
5. **Graceful cancellation** - Clean up on client disconnect or timeout
6. **Spark Connect compatibility** - Follow protocol semantics where applicable

---

## Architecture Overview

### State Management

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SESSION MANAGER STATE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ AtomicReference<ExecutionState>                                 │   │
│  │  - sessionId: String (null if idle)                             │   │
│  │  - session: Session                                             │   │
│  │  - executionThread: Thread (for interruption)                   │   │
│  │  - startTime: long (for timeout enforcement)                    │   │
│  │  - statement: Statement (for DuckDB cancellation)               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ BlockingQueue<WaitingClient> waitQueue (capacity=10)            │   │
│  │  - sessionId: String                                            │   │
│  │  - latch: CountDownLatch (to signal when slot available)        │   │
│  │  - grpcContext: Context (for cancellation detection)            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Configuration

| Parameter | Default | Override Property | Description |
|-----------|---------|-------------------|-------------|
| `MAX_QUEUE_SIZE` | 10 | `thunderduck.maxQueueSize` | Max clients waiting in queue |
| `MAX_EXECUTION_TIME_MS` | 1,800,000 (30 min) | `thunderduck.maxExecutionTimeMs` | Server-side query timeout |

---

## Request Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        REQUEST FLOW                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  New Request (sessionId=X)                                              │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────────┐                                                    │
│  │ Active Execution│                                                    │
│  │    Exists?      │                                                    │
│  └────────┬────────┘                                                    │
│           │                                                             │
│     ┌─────┴─────┐                                                       │
│     │           │                                                       │
│    NO          YES                                                      │
│     │           │                                                       │
│     ▼           ▼                                                       │
│  ┌──────────┐  ┌─────────────────┐                                     │
│  │ CAS:     │  │ Queue.size < 10?│                                     │
│  │ Replace  │  └────────┬────────┘                                     │
│  │ idle     │           │                                              │
│  │ session  │     ┌─────┴─────┐                                        │
│  │ + Start  │     │           │                                        │
│  │ execution│    YES          NO                                       │
│  └──────────┘     │           │                                        │
│                   ▼           ▼                                        │
│            ┌───────────┐  ┌────────────────────┐                       │
│            │ Add to    │  │ RESOURCE_EXHAUSTED │                       │
│            │ queue,    │  │ "Queue full (10)"  │                       │
│            │ wait on   │  └────────────────────┘                       │
│            │ latch     │                                               │
│            └─────┬─────┘                                               │
│                  │                                                     │
│                  ▼                                                     │
│            ┌───────────────────────────────────┐                       │
│            │ Block until:                      │                       │
│            │ - Latch signaled (slot available) │                       │
│            │ - Client disconnects (cleanup)    │                       │
│            │ - Server shutdown (error)         │                       │
│            └───────────┬───────────────────────┘                       │
│                        │                                               │
│                        ▼                                               │
│            ┌───────────────────────────────────┐                       │
│            │ CAS: Acquire execution slot       │                       │
│            │ (may need to retry if another     │                       │
│            │  waiter got there first)          │                       │
│            └───────────────────────────────────┘                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Execution Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     EXECUTION LIFECYCLE                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  startExecution(sessionId)                                              │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────────────────────────────┐                               │
│  │ CAS: Set ExecutionState             │                               │
│  │  - sessionId                        │                               │
│  │  - session (new or reuse existing)  │                               │
│  │  - executionThread = Thread.current │                               │
│  │  - startTime = System.currentTimeMs │                               │
│  │                                     │                               │
│  │ Register gRPC Context.addListener   │                               │
│  │ for client disconnect detection     │                               │
│  └─────────────────┬───────────────────┘                               │
│                    │                                                    │
│                    ▼                                                    │
│  ┌─────────────────────────────────────┐                               │
│  │        QUERY EXECUTION              │                               │
│  │  (protected by try/finally)         │                               │
│  └─────────────────┬───────────────────┘                               │
│                    │                                                    │
│         ┌──────────┼──────────┬─────────────────┐                      │
│         │          │          │                 │                      │
│         ▼          ▼          ▼                 ▼                      │
│     [Success]  [SQL Error] [Client        [Timeout                     │
│         │          │       Disconnect]     30 min]                     │
│         │          │          │                 │                      │
│         │          │          ▼                 ▼                      │
│         │          │    ┌───────────────────────────┐                  │
│         │          │    │ Cancel DuckDB query via   │                  │
│         │          │    │ DuckDBConnection.interrupt│                  │
│         │          │    │ (package-private method)  │                  │
│         │          │    └───────────────────────────┘                  │
│         │          │          │                 │                      │
│         └──────────┴──────────┴─────────────────┘                      │
│                    │                                                    │
│                    ▼                                                    │
│  ┌─────────────────────────────────────┐                               │
│  │ completeExecution() [finally block] │                               │
│  │  - CAS: Clear ExecutionState        │                               │
│  │  - Clean up resources               │                               │
│  │  - Signal next waiter via latch     │                               │
│  └─────────────────────────────────────┘                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Client Disconnect Handling

### Scenario A: Disconnect While Waiting in Queue

```
gRPC Context.CancellationListener fires:
  1. Remove WaitingClient from queue
  2. Free up queue slot for other clients
  3. No execution cleanup needed (query never started)
```

### Scenario B: Disconnect While Executing Query

```
gRPC Context.CancellationListener fires:
  1. Call DuckDBConnection.interrupt() to cancel query
  2. Interrupt execution thread if blocked on I/O
  3. completeExecution() runs in finally block:
     - CAS: Clear ExecutionState
     - Close any open resources (Statement, ResultSet)
     - Signal next waiter in queue
```

---

## Timeout Enforcement

A background watchdog thread monitors execution time:

```java
ScheduledExecutorService watchdog = Executors.newScheduledThreadPool(1);

watchdog.scheduleAtFixedRate(() -> {
    ExecutionState state = executionState.get();
    if (state != null && !state.isIdle()) {
        long elapsed = System.currentTimeMillis() - state.startTime;
        if (elapsed > maxExecutionTimeMs) {
            logger.warn("Execution timeout after {}ms, cancelling", elapsed);
            cancelCurrentExecution();
        }
    }
}, 10, 10, TimeUnit.SECONDS);  // Check every 10 seconds
```

---

## Compare-And-Set (CAS) State Transitions

To prevent race conditions, all state transitions use atomic CAS operations:

```java
private static class ExecutionState {
    final String sessionId;
    final Session session;
    final Thread executionThread;
    final long startTime;

    static final ExecutionState IDLE = new ExecutionState(null, null, null, 0);

    boolean isIdle() { return sessionId == null; }
}

private final AtomicReference<ExecutionState> state =
    new AtomicReference<>(ExecutionState.IDLE);
```

### CAS Loop Pattern

```java
public Session startExecution(String sessionId) throws StatusRuntimeException {
    while (true) {
        ExecutionState current = state.get();
        ExecutionState next;

        if (current.isIdle()) {
            // No active execution - acquire slot
            Session session = new Session(sessionId);
            next = new ExecutionState(sessionId, session, Thread.currentThread(),
                                      System.currentTimeMillis());
        } else {
            // Active execution - must wait in queue
            // (handled separately, see queue logic)
            throw new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
        }

        // Atomic compare-and-set
        if (state.compareAndSet(current, next)) {
            return next.session;
        }
        // CAS failed - another thread modified state, retry loop
    }
}
```

---

## DuckDB Query Cancellation

### Research Findings

Based on analysis of the [DuckDB Java JDBC source code](https://github.com/duckdb/duckdb-java):

| Method | Visibility | Status |
|--------|------------|--------|
| `abort(Executor)` | public | **Not implemented** (throws `SQLFeatureNotSupportedException`) |
| `setNetworkTimeout(Executor, int)` | public | **Not implemented** |
| `interrupt()` | package-private | **Works** - calls native `duckdb_jdbc_interrupt()` |

### Implementation Challenge

The `interrupt()` method is **package-private**, not accessible from outside `org.duckdb` package.

### Workarounds

1. **Reflection** (not recommended - fragile)
   ```java
   Method interruptMethod = DuckDBConnection.class.getDeclaredMethod("interrupt");
   interruptMethod.setAccessible(true);
   interruptMethod.invoke(connection);
   ```

2. **Thread interruption** (partial - may not stop DuckDB native code)
   ```java
   executionThread.interrupt();
   ```

3. **Connection close** (nuclear option - may leave resources in bad state)
   ```java
   connection.close();
   ```

4. **Helper class in org.duckdb package** (recommended)
   Create a small helper class that lives in `org.duckdb` package and exposes `interrupt()`:
   ```java
   package org.duckdb;

   public class DuckDBInterruptHelper {
       public static void interrupt(DuckDBConnection conn) {
           conn.interrupt();
       }
   }
   ```

**Recommendation**: Option 4 (helper class) provides clean access without reflection.

---

## Spark Connect Protocol Compatibility

### Session Handling Semantics

Based on analysis of [Apache Spark Connect source code](https://github.com/apache/spark/blob/v3.5.3/connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala):

| Aspect | Apache Spark | Thunderduck |
|--------|--------------|-------------|
| Session lookup | `getOrCreateIsolatedSession(userId, sessionId)` | Similar - create if not exists |
| Duplicate handling | Cache returns existing session | Replace idle session, queue if busy |
| ALREADY_EXISTS error | **Not used** - cache prevents duplicates | Not needed |
| Session validation | UUID format check | UUID format check |

### Key Insight

Spark Connect does **not** return `ALREADY_EXISTS` for duplicate session IDs. Instead, it uses a cache keyed by `(userId, sessionId)` that naturally returns the existing session for repeat requests.

For Thunderduck's single-session model:
- **Same session ID**: Reuse existing session (if still valid)
- **Different session ID, idle**: Replace with new session
- **Different session ID, busy**: Queue or reject

### Error Codes

| Scenario | gRPC Status | Description |
|----------|-------------|-------------|
| Queue full | `RESOURCE_EXHAUSTED` | "Server busy, queue full (10 waiters)" |
| Invalid session ID format | `INVALID_ARGUMENT` | "Invalid session ID format" |
| Execution timeout | `DEADLINE_EXCEEDED` | "Query execution timeout (30 min)" |
| Client cancelled | `CANCELLED` | "Client disconnected" |

---

## API Design

### SessionManager Interface

```java
public class SessionManager {

    // Configuration
    private static final int MAX_QUEUE_SIZE = 10;
    private final long maxExecutionTimeMs;

    /**
     * Atomically acquire execution slot.
     * - If idle: creates/reuses session and starts execution
     * - If busy and queue not full: adds to queue and blocks
     * - If busy and queue full: throws RESOURCE_EXHAUSTED
     *
     * @param sessionId Client session ID
     * @param grpcContext gRPC context for cancellation detection
     * @return Session for query execution
     * @throws StatusRuntimeException on queue full or cancellation
     */
    public Session startExecution(String sessionId, Context grpcContext)
        throws StatusRuntimeException;

    /**
     * Mark execution complete and signal next waiter.
     * Must be called in finally block after startExecution.
     *
     * @param sessionId Session that completed
     */
    public void completeExecution(String sessionId);

    /**
     * Get session for non-execution operations (config, analyze).
     * Does not acquire execution slot.
     *
     * @param sessionId Session ID to lookup
     * @return Session if exists and matches, null otherwise
     */
    public Session getSession(String sessionId);

    /**
     * Get current state for monitoring.
     *
     * @return Snapshot of session state
     */
    public SessionInfo getSessionInfo();

    /**
     * Shutdown manager, cancel any active execution,
     * and reject all waiting clients.
     */
    public void shutdown();
}
```

### Usage in SparkConnectServiceImpl

```java
@Override
public void executePlan(ExecutePlanRequest request,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
    String sessionId = request.getSessionId();
    Context grpcContext = Context.current();
    Session session = null;

    try {
        // Atomically: acquire slot (may block if queue), start execution
        session = sessionManager.startExecution(sessionId, grpcContext);

        // Execute query...
        executeQuery(request, session, responseObserver);

    } catch (StatusRuntimeException e) {
        responseObserver.onError(e);
    } finally {
        // Always complete execution, even on error
        if (session != null) {
            sessionManager.completeExecution(sessionId);
        }
    }
}
```

---

## Testing Considerations

### Unit Tests

1. **CAS race conditions**: Multiple threads competing for execution slot
2. **Queue overflow**: 11th client should be rejected immediately
3. **Client disconnect while waiting**: Should free queue slot
4. **Client disconnect while executing**: Should cancel query and signal next
5. **Timeout enforcement**: Long query should be cancelled after 30 min
6. **Session replacement**: Idle session should be replaced by new session

### Integration Tests

1. **PySpark client**: Multiple sequential SparkSession creations
2. **Concurrent clients**: Multiple clients with queue behavior
3. **Network interruption**: Client disconnect mid-query

---

## Migration Path

### Phase 1: Core Refactoring
- Replace current `SessionManager` with CAS-based implementation
- Add wait queue with configurable size
- Add gRPC context cancellation listener

### Phase 2: DuckDB Cancellation
- Add `DuckDBInterruptHelper` in `org.duckdb` package
- Integrate with client disconnect handling
- Add execution timeout watchdog

### Phase 3: Testing
- Update E2E tests to work with new session semantics
- Add stress tests for queue behavior
- Add timeout tests

---

## References

- [DuckDB Java JDBC Source](https://github.com/duckdb/duckdb-java)
- [DuckDB Query Timeout Issue #8564](https://github.com/duckdb/duckdb/issues/8564)
- [Apache Spark Connect SparkConnectService.scala](https://github.com/apache/spark/blob/v3.5.3/connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala)
- [Spark Connect Overview - Spark 3.5.3](https://spark.apache.org/docs/3.5.3/spark-connect-overview.html)
- [gRPC Error Handling](https://grpc.io/docs/guides/error/)

---

**Last Updated**: 2025-12-10
