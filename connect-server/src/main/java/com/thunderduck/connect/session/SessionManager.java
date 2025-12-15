package com.thunderduck.connect.session;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages session lifecycle and execution slot for Thunderduck Spark Connect Server.
 *
 * <p>Design principles:
 * <ul>
 *   <li>Single active execution at a time (DuckDB limitation)</li>
 *   <li>Idle session replacement - new sessions replace idle sessions seamlessly</li>
 *   <li>Wait queue - clients wait if execution in progress (FIFO, max 10)</li>
 *   <li>Graceful cancellation on client disconnect or timeout</li>
 *   <li>Lock-free CAS operations for state transitions</li>
 * </ul>
 *
 * <p>This implementation follows Spark Connect protocol semantics:
 * <ul>
 *   <li>Same session ID reuses existing session</li>
 *   <li>Different session ID replaces idle session or waits in queue</li>
 *   <li>No ALREADY_EXISTS errors - sessions are transparently managed</li>
 * </ul>
 *
 * @see ExecutionState
 * @see WaitingClient
 */
public class SessionManager {
    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    /** Default maximum clients waiting in queue */
    private static final int DEFAULT_MAX_QUEUE_SIZE = 10;

    /** Default maximum execution time: 30 minutes */
    private static final long DEFAULT_MAX_EXECUTION_TIME_MS = 30 * 60 * 1000L;

    /** System property for max queue size */
    private static final String PROP_MAX_QUEUE_SIZE = "thunderduck.maxQueueSize";

    /** System property for max execution time */
    private static final String PROP_MAX_EXECUTION_TIME_MS = "thunderduck.maxExecutionTimeMs";

    /** Current execution state (atomic for CAS operations) */
    private final AtomicReference<ExecutionState> executionState =
        new AtomicReference<>(ExecutionState.IDLE);

    /** Queue of clients waiting for execution slot */
    private final BlockingQueue<WaitingClient> waitQueue;

    /** Maximum execution time in milliseconds */
    private final long maxExecutionTimeMs;

    /** Watchdog for execution timeout */
    private final ScheduledExecutorService watchdog;

    /** Flag to track shutdown state */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /** Cache of sessions by session ID - ensures same session ID reuses same DuckDB runtime */
    private final ConcurrentHashMap<String, Session> sessionCache = new ConcurrentHashMap<>();

    /**
     * Create SessionManager with default configuration.
     */
    public SessionManager() {
        this(getConfiguredQueueSize(), getConfiguredMaxExecutionTime());
    }

    /**
     * Create SessionManager with custom execution timeout (for backward compatibility).
     *
     * @param maxExecutionTimeMs Maximum query execution time in milliseconds
     */
    public SessionManager(long maxExecutionTimeMs) {
        this(DEFAULT_MAX_QUEUE_SIZE, maxExecutionTimeMs);
    }

    /**
     * Create SessionManager with custom configuration.
     *
     * @param maxQueueSize Maximum clients in wait queue
     * @param maxExecutionTimeMs Maximum query execution time in milliseconds
     */
    public SessionManager(int maxQueueSize, long maxExecutionTimeMs) {
        this.waitQueue = new LinkedBlockingQueue<>(maxQueueSize);
        this.maxExecutionTimeMs = maxExecutionTimeMs;
        this.watchdog = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "session-watchdog");
            t.setDaemon(true);
            return t;
        });

        // Start watchdog to check for execution timeout
        watchdog.scheduleAtFixedRate(
            this::checkExecutionTimeout,
            10, 10, TimeUnit.SECONDS
        );

        logger.info("SessionManager initialized: maxQueueSize={}, maxExecutionTimeMs={}",
            maxQueueSize, maxExecutionTimeMs);
    }

    /**
     * Acquire execution slot for a session.
     *
     * <p>Behavior:
     * <ul>
     *   <li>If idle: creates/reuses session and starts execution immediately</li>
     *   <li>If busy with same session: waits in queue (allows sequential queries)</li>
     *   <li>If busy with different session: waits in queue</li>
     *   <li>If queue full: throws RESOURCE_EXHAUSTED</li>
     * </ul>
     *
     * @param sessionId Client session ID
     * @param grpcContext gRPC context for cancellation detection (may be null)
     * @return Session for query execution
     * @throws StatusRuntimeException on queue full, cancellation, or shutdown
     */
    public Session startExecution(String sessionId, Context grpcContext)
            throws StatusRuntimeException {

        if (shuttingDown.get()) {
            throw new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("Server is shutting down"));
        }

        // Try to acquire slot immediately
        Session session = tryAcquireSlot(sessionId);
        if (session != null) {
            return session;
        }

        // Slot not available - need to wait in queue
        return waitForSlot(sessionId, grpcContext);
    }

    /**
     * Try to acquire execution slot without waiting.
     *
     * @param sessionId Session ID
     * @return Session if slot acquired, null if busy
     */
    private Session tryAcquireSlot(String sessionId) {
        while (true) {
            ExecutionState current = executionState.get();

            if (current.isIdle()) {
                // Idle - acquire slot with cached or new session
                Session session = getOrCreateCachedSession(sessionId);
                ExecutionState next = ExecutionState.executing(sessionId, session);

                if (executionState.compareAndSet(current, next)) {
                    logger.info("Execution started: session={}", sessionId);
                    return session;
                }
                // CAS failed - retry
                continue;
            }

            // Not idle - check if same session (can reuse)
            if (sessionId.equals(current.getSessionId())) {
                // Same session - reuse existing session object but need to wait
                // for current execution to complete
                return null;
            }

            // Different session and busy - need to wait
            return null;
        }
    }

    /**
     * Get or create a cached session for the given session ID.
     *
     * <p>Sessions are cached by ID to ensure the same DuckDB runtime is reused
     * for all requests with the same session ID.
     *
     * @param sessionId Session ID
     * @return Cached or newly created session
     */
    private Session getOrCreateCachedSession(String sessionId) {
        return sessionCache.computeIfAbsent(sessionId, id -> {
            logger.debug("Creating new cached session: {}", id);
            return new Session(id);
        });
    }

    /**
     * Wait in queue for execution slot.
     *
     * @param sessionId Session ID
     * @param grpcContext gRPC context for cancellation
     * @return Session when slot acquired
     * @throws StatusRuntimeException on queue full, cancellation, or timeout
     */
    private Session waitForSlot(String sessionId, Context grpcContext)
            throws StatusRuntimeException {

        WaitingClient waiter = new WaitingClient(sessionId, grpcContext);

        // Register cancellation listener to remove from queue on disconnect
        if (grpcContext != null) {
            waiter.onClientCancel(context -> {
                logger.info("Client disconnected while waiting: session={}", sessionId);
                waiter.cancel();
                waitQueue.remove(waiter);
            });
        }

        // Try to add to queue
        if (!waitQueue.offer(waiter)) {
            throw new StatusRuntimeException(
                Status.RESOURCE_EXHAUSTED
                    .withDescription(String.format(
                        "Server busy: queue full (%d clients waiting). Try again later.",
                        waitQueue.size())));
        }

        logger.info("Client queued: session={}, queueSize={}", sessionId, waitQueue.size());

        try {
            // Wait for signal
            while (!waiter.isCancelled() && !shuttingDown.get()) {
                // Wait with timeout to allow periodic checks
                if (waiter.await(1, TimeUnit.SECONDS)) {
                    break; // Signaled
                }

                // Check if client disconnected
                if (!waiter.isClientConnected()) {
                    waiter.cancel();
                    waitQueue.remove(waiter);
                    throw new StatusRuntimeException(
                        Status.CANCELLED.withDescription("Client disconnected"));
                }
            }

            // Check cancellation states
            if (shuttingDown.get()) {
                throw new StatusRuntimeException(
                    Status.UNAVAILABLE.withDescription("Server is shutting down"));
            }

            if (waiter.isCancelled()) {
                throw new StatusRuntimeException(
                    Status.CANCELLED.withDescription("Request cancelled"));
            }

            // Try to acquire slot now that we've been signaled
            Session session = tryAcquireSlot(sessionId);
            if (session != null) {
                logger.info("Client acquired slot after waiting {}ms: session={}",
                    waiter.getWaitTimeMs(), sessionId);
                return session;
            }

            // Another waiter got there first - go back to waiting
            // This shouldn't happen often with proper signaling, but handle it
            return waitForSlot(sessionId, grpcContext);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            waitQueue.remove(waiter);
            throw new StatusRuntimeException(
                Status.CANCELLED.withDescription("Wait interrupted"));
        }
    }

    /**
     * Complete execution and signal next waiter.
     *
     * <p>Must be called in finally block after startExecution().
     *
     * @param sessionId Session that completed execution
     */
    public void completeExecution(String sessionId) {
        while (true) {
            ExecutionState current = executionState.get();

            if (current.isIdle()) {
                // Already idle - nothing to do (may have been cancelled)
                logger.debug("completeExecution called but already idle: session={}", sessionId);
                return;
            }

            if (!sessionId.equals(current.getSessionId())) {
                // Different session - shouldn't happen, log warning
                logger.warn("completeExecution session mismatch: expected={}, actual={}",
                    current.getSessionId(), sessionId);
                return;
            }

            // Transition to idle
            if (executionState.compareAndSet(current, ExecutionState.IDLE)) {
                logger.info("Execution completed: session={}, elapsed={}ms",
                    sessionId, current.getElapsedTimeMs());

                // Signal next waiter
                signalNextWaiter();
                return;
            }
            // CAS failed - retry
        }
    }

    /**
     * Signal the next waiter in queue that a slot is available.
     */
    private void signalNextWaiter() {
        // Remove cancelled waiters from front of queue
        while (true) {
            WaitingClient waiter = waitQueue.peek();
            if (waiter == null) {
                return; // Queue empty
            }

            if (waiter.isCancelled() || !waiter.isClientConnected()) {
                // Remove cancelled waiter and check next
                waitQueue.poll();
                continue;
            }

            // Found valid waiter - signal and let them compete for slot
            waiter.signal();
            waitQueue.poll();
            logger.debug("Signaled next waiter: session={}", waiter.getSessionId());
            return;
        }
    }

    /**
     * Cancel current execution (for timeout or client disconnect).
     *
     * <p>This method interrupts the execution thread. The actual DuckDB
     * query cancellation happens at the QueryExecutor level via try-with-resources
     * or when the thread is interrupted.
     */
    public void cancelCurrentExecution() {
        ExecutionState current = executionState.get();
        if (current.isIdle()) {
            return;
        }

        logger.warn("Cancelling execution: session={}, elapsed={}ms",
            current.getSessionId(), current.getElapsedTimeMs());

        // Interrupt execution thread - this will cause JDBC operations to throw
        Thread execThread = current.getExecutionThread();
        if (execThread != null && execThread != Thread.currentThread()) {
            execThread.interrupt();
        }

        // Note: completeExecution will be called by the finally block
        // in the execution code path
    }

    /**
     * Check for execution timeout (called by watchdog).
     */
    private void checkExecutionTimeout() {
        ExecutionState current = executionState.get();
        if (current.isIdle()) {
            return;
        }

        long elapsed = current.getElapsedTimeMs();
        if (elapsed > maxExecutionTimeMs) {
            logger.warn("Execution timeout: session={}, elapsed={}ms, limit={}ms",
                current.getSessionId(), elapsed, maxExecutionTimeMs);
            cancelCurrentExecution();
        }
    }

    /**
     * Get session by ID (for non-execution operations like config, analyze).
     *
     * <p>Returns the current session if it matches the given ID.
     * Does not acquire execution slot.
     *
     * @param sessionId Session ID to lookup
     * @return Session if exists and matches, null otherwise
     */
    public Session getSession(String sessionId) {
        ExecutionState current = executionState.get();
        if (!current.isIdle() && sessionId.equals(current.getSessionId())) {
            return current.getSession();
        }
        return null;
    }

    /**
     * Get or create a session for non-execution operations (config, analyze).
     *
     * <p>This method is used for operations that need a session but don't
     * acquire the execution slot. If the server is idle, it creates a new
     * session. If busy, it returns the current session if IDs match, or
     * creates a temporary session for the request.
     *
     * <p>Following Spark Connect semantics: sessions are transparently managed,
     * no ALREADY_EXISTS errors.
     *
     * @param sessionId Session ID
     * @return Session for the operation
     */
    public Session getOrCreateSessionForMetadata(String sessionId) {
        ExecutionState current = executionState.get();

        // If there's an active session with matching ID, return it
        if (!current.isIdle() && sessionId.equals(current.getSessionId())) {
            return current.getSession();
        }

        // Return cached or new session for metadata operations
        // This ensures temp views and other session state persist
        return getOrCreateCachedSession(sessionId);
    }

    /**
     * Get current session info for monitoring.
     *
     * @return Snapshot of current state
     */
    public SessionInfo getSessionInfo() {
        ExecutionState current = executionState.get();
        return new SessionInfo(
            !current.isIdle(),
            current.getSessionId(),
            current.getElapsedTimeMs(),
            waitQueue.size()
        );
    }

    /**
     * Shutdown the session manager.
     *
     * <p>Cancels current execution and rejects all waiting clients.
     */
    public void shutdown() {
        logger.info("Shutting down SessionManager");
        shuttingDown.set(true);

        // Cancel current execution
        cancelCurrentExecution();

        // Reject all waiting clients
        WaitingClient waiter;
        while ((waiter = waitQueue.poll()) != null) {
            waiter.cancel();
        }

        // Shutdown watchdog
        watchdog.shutdown();
        try {
            if (!watchdog.awaitTermination(5, TimeUnit.SECONDS)) {
                watchdog.shutdownNow();
            }
        } catch (InterruptedException e) {
            watchdog.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("SessionManager shutdown complete");
    }

    // ========== Configuration Helpers ==========

    private static int getConfiguredQueueSize() {
        String value = System.getProperty(PROP_MAX_QUEUE_SIZE);
        if (value != null) {
            try {
                int size = Integer.parseInt(value);
                if (size > 0) {
                    return size;
                }
            } catch (NumberFormatException e) {
                // Ignore, use default
            }
        }
        return DEFAULT_MAX_QUEUE_SIZE;
    }

    private static long getConfiguredMaxExecutionTime() {
        String value = System.getProperty(PROP_MAX_EXECUTION_TIME_MS);
        if (value != null) {
            try {
                long time = Long.parseLong(value);
                if (time > 0) {
                    return time;
                }
            } catch (NumberFormatException e) {
                // Ignore, use default
            }
        }
        return DEFAULT_MAX_EXECUTION_TIME_MS;
    }

    // ========== Session Info ==========

    /**
     * Immutable snapshot of session state for monitoring.
     */
    public static class SessionInfo {
        public final boolean hasActiveExecution;
        public final String sessionId;
        public final long executionTimeMs;
        public final int queueSize;

        public SessionInfo(boolean hasActiveExecution, String sessionId,
                          long executionTimeMs, int queueSize) {
            this.hasActiveExecution = hasActiveExecution;
            this.sessionId = sessionId;
            this.executionTimeMs = executionTimeMs;
            this.queueSize = queueSize;
        }

        @Override
        public String toString() {
            if (hasActiveExecution) {
                return String.format("SessionInfo[session=%s, execTime=%dms, queue=%d]",
                    sessionId, executionTimeMs, queueSize);
            } else {
                return String.format("SessionInfo[IDLE, queue=%d]", queueSize);
            }
        }
    }
}
