package com.thunderduck.connect.session;

/**
 * Immutable snapshot of the current execution state.
 *
 * Used with AtomicReference for lock-free CAS operations.
 * Each state transition creates a new instance.
 */
public final class ExecutionState {

    /** Singleton idle state */
    public static final ExecutionState IDLE = new ExecutionState(null, null, null, 0);

    private final String sessionId;
    private final Session session;
    private final Thread executionThread;
    private final long startTime;

    /**
     * Create a new execution state.
     *
     * @param sessionId Session ID (null for idle state)
     * @param session Session object (null for idle state)
     * @param executionThread Thread executing the query (null for idle state)
     * @param startTime Execution start time in milliseconds (0 for idle state)
     */
    public ExecutionState(String sessionId, Session session, Thread executionThread, long startTime) {
        this.sessionId = sessionId;
        this.session = session;
        this.executionThread = executionThread;
        this.startTime = startTime;
    }

    /**
     * Create execution state for a new execution.
     *
     * @param sessionId Session ID
     * @param session Session object
     * @return New execution state
     */
    public static ExecutionState executing(String sessionId, Session session) {
        return new ExecutionState(
            sessionId,
            session,
            Thread.currentThread(),
            System.currentTimeMillis()
        );
    }

    /**
     * Check if this is the idle state (no active execution).
     *
     * @return true if idle
     */
    public boolean isIdle() {
        return sessionId == null;
    }

    /**
     * Get the session ID.
     *
     * @return Session ID or null if idle
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get the session.
     *
     * @return Session or null if idle
     */
    public Session getSession() {
        return session;
    }

    /**
     * Get the thread executing the query.
     *
     * @return Execution thread or null if idle
     */
    public Thread getExecutionThread() {
        return executionThread;
    }

    /**
     * Get the execution start time.
     *
     * @return Start time in milliseconds since epoch, or 0 if idle
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Get elapsed execution time in milliseconds.
     *
     * @return Elapsed time, or 0 if idle
     */
    public long getElapsedTimeMs() {
        if (isIdle()) {
            return 0;
        }
        return System.currentTimeMillis() - startTime;
    }

    @Override
    public String toString() {
        if (isIdle()) {
            return "ExecutionState[IDLE]";
        }
        return String.format("ExecutionState[session=%s, elapsed=%dms, thread=%s]",
            sessionId, getElapsedTimeMs(),
            executionThread != null ? executionThread.getName() : "null");
    }
}
