package com.thunderduck.connect.session;

import io.grpc.Context;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a client waiting in the queue for an execution slot.
 *
 * Each waiting client has:
 * - A session ID they want to execute with
 * - A latch to wait on until signaled
 * - A gRPC context for cancellation detection
 * - A cancelled flag to track if the wait was cancelled
 */
public final class WaitingClient {

    private final String sessionId;
    private final CountDownLatch latch;
    private final Context grpcContext;
    private final AtomicBoolean cancelled;
    private final long queuedAt;

    /**
     * Create a new waiting client.
     *
     * @param sessionId Session ID the client wants to use
     * @param grpcContext gRPC context for cancellation detection
     */
    public WaitingClient(String sessionId, Context grpcContext) {
        this.sessionId = sessionId;
        this.latch = new CountDownLatch(1);
        this.grpcContext = grpcContext;
        this.cancelled = new AtomicBoolean(false);
        this.queuedAt = System.currentTimeMillis();
    }

    /**
     * Get the session ID.
     *
     * @return Session ID
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get the time this client was queued.
     *
     * @return Queue time in milliseconds since epoch
     */
    public long getQueuedAt() {
        return queuedAt;
    }

    /**
     * Get time spent waiting in queue.
     *
     * @return Wait time in milliseconds
     */
    public long getWaitTimeMs() {
        return System.currentTimeMillis() - queuedAt;
    }

    /**
     * Check if this wait was cancelled (client disconnected).
     *
     * @return true if cancelled
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Mark this wait as cancelled.
     * Called when client disconnects while waiting.
     */
    public void cancel() {
        cancelled.set(true);
        latch.countDown(); // Wake up the waiting thread
    }

    /**
     * Signal that an execution slot is available.
     * The waiting client will wake up and attempt to acquire the slot.
     */
    public void signal() {
        latch.countDown();
    }

    /**
     * Wait for a signal that an execution slot is available.
     *
     * @param timeout Maximum time to wait
     * @param unit Time unit for timeout
     * @return true if signaled, false if timed out
     * @throws InterruptedException if the thread is interrupted
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * Wait indefinitely for a signal.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Check if the gRPC context is still active (client still connected).
     *
     * @return true if client is still connected
     */
    public boolean isClientConnected() {
        return grpcContext == null || !grpcContext.isCancelled();
    }

    /**
     * Register a listener for client cancellation.
     *
     * @param listener Listener to call when client disconnects
     */
    public void onClientCancel(Context.CancellationListener listener) {
        if (grpcContext != null) {
            grpcContext.addListener(listener, Runnable::run);
        }
    }

    @Override
    public String toString() {
        return String.format("WaitingClient[session=%s, waitTime=%dms, cancelled=%s]",
            sessionId, getWaitTimeMs(), cancelled.get());
    }
}
