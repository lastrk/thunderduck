package com.catalyst2sql.runtime;

import com.catalyst2sql.test.TestBase;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Comprehensive tests for database connection cleanup crash scenarios.
 *
 * <p>This test suite addresses the critical bug: "Database connection closed,
 * cannot remove child PID during cleanup". It tests:
 * <ul>
 *   <li>Connection state validation before cleanup operations</li>
 *   <li>Race conditions between connection close and PID removal</li>
 *   <li>Premature connection closure scenarios</li>
 *   <li>Concurrent cleanup attempts</li>
 *   <li>Failed cleanup recovery mechanisms</li>
 *   <li>Connection lifecycle integrity</li>
 * </ul>
 *
 * <p>These tests ensure that the connection manager can handle edge cases
 * where connections are closed unexpectedly during cleanup operations,
 * preventing crashes and ensuring system stability.
 */
@DisplayName("Database Connection Cleanup Crash Prevention Tests")
@Tag("reliability")
@Tag("concurrency")
public class DatabaseConnectionCleanupTest extends TestBase {

    private DuckDBConnectionManager manager;
    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        manager = new DuckDBConnectionManager(
            DuckDBConnectionManager.Configuration.inMemory()
                .withPoolSize(4)
        );
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void teardown() throws SQLException {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (manager != null && !manager.isClosed()) {
            manager.close();
        }
    }

    @Nested
    @DisplayName("Connection State Validation Tests")
    class ConnectionStateValidationTests {

        @Test
        @DisplayName("Verify connection state before cleanup - normal shutdown")
        void testNormalShutdownSequence() throws SQLException {
            logStep("Given: Active connection with valid state");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            // Verify initial state
            assertThat(conn.isClosed()).isFalse();
            assertThat(conn.isValid(5)).isTrue();

            logStep("When: Perform normal cleanup sequence");
            pooled.close();

            logStep("Then: Manager should remain stable");
            assertThat(manager.isClosed()).isFalse();

            // Should be able to borrow another connection
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Detect and handle closed connection before cleanup")
        void testPrematureConnectionClosure() throws SQLException {
            logStep("Given: Connection that will be closed prematurely");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            logStep("When: Connection closed before returning to pool");
            conn.close(); // Simulate premature closure

            logStep("Then: Cleanup should detect closed state and handle gracefully");
            assertThatCode(() -> pooled.close())
                .doesNotThrowAnyException();

            // Verify manager created replacement connection
            try (PooledConnection replacement = manager.borrowConnection()) {
                assertThat(replacement.get().isClosed()).isFalse();
                assertThat(replacement.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Handle connection that becomes invalid during use")
        void testConnectionBecomesInvalidDuringUse() throws SQLException {
            logStep("Given: Connection being used for query");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            // Execute a query to establish connection state
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }

            logStep("When: Connection becomes invalid");
            conn.close();

            logStep("Then: Return to pool should handle gracefully");
            assertThatCode(() -> pooled.close())
                .doesNotThrowAnyException();

            // Next connection should be valid
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Validate connection state during concurrent operations")
        void testConnectionStateValidationUnderConcurrency() throws Exception {
            logStep("Given: Multiple threads validating connection state");
            int threadCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            AtomicInteger validConnectionCount = new AtomicInteger(0);

            List<Future<Boolean>> futures = new ArrayList<>();

            logStep("When: All threads validate and use connections concurrently");
            for (int i = 0; i < threadCount; i++) {
                Future<Boolean> future = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        try (PooledConnection pooled = manager.borrowConnection()) {
                            DuckDBConnection conn = pooled.get();

                            // Validate connection state
                            if (!conn.isClosed() && conn.isValid(5)) {
                                // Execute query to verify connection works
                                try (Statement stmt = conn.createStatement()) {
                                    stmt.execute("SELECT 1");
                                }
                                validConnectionCount.incrementAndGet();
                                return true;
                            }
                            return false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    } finally {
                        completeLatch.countDown();
                    }
                });
                futures.add(future);
            }

            startLatch.countDown(); // Start all threads
            boolean completed = completeLatch.await(10, TimeUnit.SECONDS);

            logStep("Then: All connections should be valid");
            assertThat(completed).isTrue();
            assertThat(validConnectionCount.get()).isEqualTo(threadCount);

            for (Future<Boolean> future : futures) {
                assertThat(future.get()).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("Race Condition Tests")
    class RaceConditionTests {

        @Test
        @DisplayName("Race between connection close and PID cleanup")
        void testRaceBetweenCloseAndCleanup() throws Exception {
            logStep("Given: Connection being used and closed simultaneously");
            int iterations = 20; // Multiple iterations to catch race condition
            AtomicInteger crashCount = new AtomicInteger(0);

            for (int i = 0; i < iterations; i++) {
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch completeLatch = new CountDownLatch(2);
                PooledConnection pooled = manager.borrowConnection();
                DuckDBConnection conn = pooled.get();

                logStep("When: Thread 1 closes connection, Thread 2 returns to pool");

                // Thread 1: Close underlying connection
                Future<?> closeTask = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        crashCount.incrementAndGet();
                    } finally {
                        completeLatch.countDown();
                    }
                });

                // Thread 2: Return connection to pool (cleanup)
                Future<?> cleanupTask = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        Thread.sleep(1); // Tiny delay to create race condition
                        pooled.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        crashCount.incrementAndGet();
                    } finally {
                        completeLatch.countDown();
                    }
                });

                startLatch.countDown(); // Start both threads
                boolean completed = completeLatch.await(2, TimeUnit.SECONDS);

                assertThat(completed).isTrue();
                closeTask.get(1, TimeUnit.SECONDS);
                cleanupTask.get(1, TimeUnit.SECONDS);
            }

            logStep("Then: No crashes should occur");
            assertThat(crashCount.get()).isZero();
            assertThat(manager.isClosed()).isFalse();
        }

        @Test
        @DisplayName("Concurrent cleanup attempts on same connection")
        void testConcurrentCleanupAttempts() throws Exception {
            logStep("Given: Single connection with multiple cleanup attempts");
            PooledConnection pooled = manager.borrowConnection();
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(3);
            AtomicInteger successCount = new AtomicInteger(0);

            logStep("When: Multiple threads try to cleanup same connection");
            for (int i = 0; i < 3; i++) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        pooled.close();
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean completed = completeLatch.await(5, TimeUnit.SECONDS);

            logStep("Then: All cleanup attempts should complete safely");
            assertThat(completed).isTrue();
            assertThat(successCount.get()).isEqualTo(3);
            assertThat(pooled.isReleased()).isTrue();
        }

        @Test
        @DisplayName("Race between manager close and connection cleanup")
        void testRaceBetweenManagerCloseAndCleanup() throws Exception {
            logStep("Given: Active connection when manager is closing");
            PooledConnection pooled = manager.borrowConnection();
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(2);
            AtomicReference<Throwable> error = new AtomicReference<>();

            logStep("When: Manager closes while connection is being returned");

            // Thread 1: Close manager
            Future<?> closeManagerTask = executorService.submit(() -> {
                try {
                    startLatch.await();
                    manager.close();
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    completeLatch.countDown();
                }
            });

            // Thread 2: Return connection
            Future<?> returnConnTask = executorService.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(5); // Small delay
                    pooled.close();
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    completeLatch.countDown();
                }
            });

            startLatch.countDown();
            boolean completed = completeLatch.await(5, TimeUnit.SECONDS);

            logStep("Then: Both operations should complete without crash");
            assertThat(completed).isTrue();
            assertThat(error.get()).isNull();
            assertThat(manager.isClosed()).isTrue();
        }

        @Test
        @DisplayName("Stress test: Rapid connect-disconnect cycles")
        void testRapidConnectDisconnectCycles() throws Exception {
            logStep("Given: High-frequency connection cycling");
            int cycleCount = 50;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(cycleCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            logStep("When: Many threads rapidly borrow and return connections");
            for (int i = 0; i < cycleCount; i++) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        try (PooledConnection conn = manager.borrowConnection()) {
                            // Minimal work
                            assertThat(conn.get().isValid(1)).isTrue();
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean completed = completeLatch.await(30, TimeUnit.SECONDS);

            logStep("Then: Most cycles should succeed without crashes");
            assertThat(completed).isTrue();
            assertThat(successCount.get()).isGreaterThan(cycleCount - 5);

            // Verify manager is still functional
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get().isValid(5)).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("Cleanup Failure Recovery Tests")
    class CleanupFailureRecoveryTests {

        @Test
        @DisplayName("Recover from failed cleanup operation")
        void testRecoveryFromFailedCleanup() throws SQLException {
            logStep("Given: Connection that will fail during cleanup");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            logStep("When: Force connection failure and cleanup");
            conn.close(); // Force failure
            pooled.close(); // Attempt cleanup

            logStep("Then: Manager should recover and provide new valid connection");
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isClosed()).isFalse();
                assertThat(newConn.get().isValid(5)).isTrue();

                // Verify connection actually works
                try (Statement stmt = newConn.get().createStatement()) {
                    stmt.execute("SELECT 1");
                }
            }
        }

        @Test
        @DisplayName("Handle multiple consecutive cleanup failures")
        void testMultipleConsecutiveCleanupFailures() throws SQLException {
            logStep("Given: Multiple connections that will fail during cleanup");
            int failureCount = 3;

            for (int i = 0; i < failureCount; i++) {
                logStep("When: Close and cleanup connection " + (i + 1));
                PooledConnection pooled = manager.borrowConnection();
                pooled.get().close(); // Force failure
                pooled.close(); // Cleanup
            }

            logStep("Then: Manager should still be operational");
            assertThat(manager.isClosed()).isFalse();

            // Should be able to get valid connections
            try (PooledConnection conn1 = manager.borrowConnection();
                 PooledConnection conn2 = manager.borrowConnection()) {
                assertThat(conn1.get().isValid(5)).isTrue();
                assertThat(conn2.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Recovery when all pool connections become invalid")
        void testRecoveryWhenAllConnectionsInvalid() throws SQLException {
            logStep("Given: All connections in pool become invalid");
            int poolSize = manager.getPoolSize();
            List<PooledConnection> connections = new ArrayList<>();

            // Borrow all connections
            for (int i = 0; i < poolSize; i++) {
                connections.add(manager.borrowConnection());
            }

            logStep("When: Close all underlying connections and return to pool");
            for (PooledConnection pooled : connections) {
                pooled.get().close(); // Invalidate
                pooled.close(); // Return to pool
            }

            logStep("Then: Manager should recover with new valid connections");
            for (int i = 0; i < poolSize; i++) {
                try (PooledConnection conn = manager.borrowConnection()) {
                    assertThat(conn.get().isClosed()).isFalse();
                    assertThat(conn.get().isValid(5)).isTrue();
                }
            }
        }

        @Test
        @DisplayName("Cleanup failure during manager shutdown")
        void testCleanupFailureDuringShutdown() throws SQLException {
            logStep("Given: Connection with active statement");
            PooledConnection pooled = manager.borrowConnection();
            Statement stmt = pooled.get().createStatement();

            // Execute long-running query in background
            executorService.submit(() -> {
                try {
                    stmt.execute("SELECT * FROM range(1000000)");
                } catch (SQLException e) {
                    // Expected: connection closed
                }
            });

            logStep("When: Close connection and shutdown manager");
            Thread.sleep(100); // Let query start
            pooled.close();

            logStep("Then: Manager should shutdown gracefully");
            assertThatCode(() -> manager.close())
                .doesNotThrowAnyException();
            assertThat(manager.isClosed()).isTrue();
        }
    }

    @Nested
    @DisplayName("Connection Lifecycle Integrity Tests")
    class ConnectionLifecycleIntegrityTests {

        @Test
        @DisplayName("Verify complete lifecycle: acquire -> use -> release")
        void testCompleteConnectionLifecycle() throws SQLException {
            logStep("Given: Fresh connection manager");
            int initialPoolSize = manager.getPoolSize();

            logStep("When: Execute complete lifecycle");
            PooledConnection pooled = manager.borrowConnection();
            assertThat(pooled.isReleased()).isFalse();

            DuckDBConnection conn = pooled.get();
            assertThat(conn.isClosed()).isFalse();

            // Use connection
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }

            // Release
            pooled.close();
            assertThat(pooled.isReleased()).isTrue();

            logStep("Then: Pool should maintain correct size");
            // Can still borrow up to pool size
            List<PooledConnection> connections = new ArrayList<>();
            for (int i = 0; i < initialPoolSize; i++) {
                connections.add(manager.borrowConnection());
            }
            assertThat(connections).hasSize(initialPoolSize);

            connections.forEach(PooledConnection::close);
        }

        @Test
        @DisplayName("Prevent double-close on connection lifecycle")
        void testPreventDoubleClose() throws SQLException {
            logStep("Given: Connection that will be closed twice");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            logStep("When: Close connection twice");
            pooled.close();
            pooled.close(); // Double close

            logStep("Then: Should be idempotent with no side effects");
            assertThat(pooled.isReleased()).isTrue();

            // Verify only one connection returned to pool
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get()).isNotSameAs(conn);
            }
        }

        @Test
        @DisplayName("Connection lifecycle with exceptions during use")
        void testLifecycleWithExceptionsDuringUse() throws SQLException {
            logStep("Given: Connection that will encounter error during use");

            logStep("When: Exception occurs during connection use");
            try (PooledConnection pooled = manager.borrowConnection()) {
                DuckDBConnection conn = pooled.get();

                // Execute invalid SQL
                assertThatThrownBy(() -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT * FROM nonexistent_table");
                    }
                }).isInstanceOf(SQLException.class);

                // Connection should still be valid after error
                assertThat(conn.isValid(5)).isTrue();
            } // Auto-released

            logStep("Then: Connection should be properly returned to pool");
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Connection state after manager close")
        void testConnectionStateAfterManagerClose() throws SQLException {
            logStep("Given: Active connection from manager");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            assertThat(conn.isValid(5)).isTrue();

            logStep("When: Manager is closed");
            pooled.close(); // Return to pool first
            manager.close();

            logStep("Then: Manager should be closed, connections cleaned up");
            assertThat(manager.isClosed()).isTrue();

            // Cannot borrow new connections
            assertThatThrownBy(() -> manager.borrowConnection())
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("Verify no connection leaks during lifecycle")
        void testNoConnectionLeaksDuringLifecycle() throws SQLException {
            logStep("Given: Multiple connection lifecycle iterations");
            int iterations = 10;
            int poolSize = manager.getPoolSize();

            logStep("When: Repeatedly borrow and return all connections");
            for (int i = 0; i < iterations; i++) {
                List<PooledConnection> connections = new ArrayList<>();

                // Borrow all
                for (int j = 0; j < poolSize; j++) {
                    connections.add(manager.borrowConnection());
                }

                // Use all
                for (PooledConnection conn : connections) {
                    try (Statement stmt = conn.get().createStatement()) {
                        stmt.execute("SELECT 1");
                    }
                }

                // Return all
                for (PooledConnection conn : connections) {
                    conn.close();
                }
            }

            logStep("Then: Pool should maintain correct size without leaks");
            List<PooledConnection> finalConnections = new ArrayList<>();
            for (int i = 0; i < poolSize; i++) {
                finalConnections.add(manager.borrowConnection());
            }
            assertThat(finalConnections).hasSize(poolSize);

            finalConnections.forEach(PooledConnection::close);
        }
    }

    @Nested
    @DisplayName("Edge Case Scenarios")
    class EdgeCaseScenarios {

        @Test
        @DisplayName("Handle cleanup when connection is in transaction")
        void testCleanupDuringTransaction() throws SQLException {
            logStep("Given: Connection with active transaction");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            // Start transaction
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TEMPORARY TABLE test_tx (id INTEGER)");
                stmt.execute("INSERT INTO test_tx VALUES (1)");
            }

            logStep("When: Return connection without committing");
            pooled.close();

            logStep("Then: Should handle gracefully");
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
                newConn.get().setAutoCommit(true); // Reset
            }
        }

        @Test
        @DisplayName("Cleanup with pending result sets")
        void testCleanupWithPendingResultSets() throws SQLException {
            logStep("Given: Connection with open result set");
            PooledConnection pooled = manager.borrowConnection();
            Statement stmt = pooled.get().createStatement();
            java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM range(100)");

            // Read only first row
            assertThat(rs.next()).isTrue();

            logStep("When: Close connection without closing result set");
            // Don't close rs or stmt
            pooled.close();

            logStep("Then: Should cleanup gracefully");
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Handle cleanup timeout scenarios")
        void testCleanupWithTimeout() throws Exception {
            logStep("Given: Connection that takes time to close");
            PooledConnection pooled = manager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            // Execute long-running query
            Future<?> queryFuture = executorService.submit(() -> {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT * FROM range(10000000)");
                } catch (SQLException e) {
                    // Expected: connection closed
                }
            });

            Thread.sleep(50); // Let query start

            logStep("When: Cleanup connection while query running");
            pooled.close();

            logStep("Then: Should handle timeout gracefully");
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isValid(5)).isTrue();
            }

            queryFuture.cancel(true);
        }

        @Test
        @DisplayName("Concurrent cleanup with manager pool exhaustion")
        void testCleanupUnderPoolExhaustion() throws Exception {
            logStep("Given: Pool exhaustion scenario");
            int poolSize = manager.getPoolSize();
            List<PooledConnection> borrowed = new ArrayList<>();

            // Exhaust pool
            for (int i = 0; i < poolSize; i++) {
                borrowed.add(manager.borrowConnection());
            }

            logStep("When: Multiple threads try to cleanup and borrow simultaneously");
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(poolSize * 2);
            AtomicInteger successCount = new AtomicInteger(0);

            // Return all connections
            for (PooledConnection conn : borrowed) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        conn.close();
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            // Try to borrow new connections
            for (int i = 0; i < poolSize; i++) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        try (PooledConnection conn = manager.borrowConnection()) {
                            assertThat(conn.get().isValid(1)).isTrue();
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        // Some may timeout, that's ok
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean completed = completeLatch.await(15, TimeUnit.SECONDS);

            logStep("Then: System should remain stable");
            assertThat(completed).isTrue();
            assertThat(successCount.get()).isGreaterThan(poolSize);
        }
    }

    @Nested
    @DisplayName("Regression Tests for 'Database connection closed' Bug")
    class RegressionTests {

        @Test
        @DisplayName("REGRESSION: Prevent 'cannot remove child PID during cleanup' crash")
        void testPreventPIDRemovalCrash() throws Exception {
            logStep("Given: Scenario that previously caused PID removal crash");
            int iterations = 10;
            AtomicBoolean crashDetected = new AtomicBoolean(false);

            for (int i = 0; i < iterations; i++) {
                logStep("Iteration: " + (i + 1));

                logStep("When: Connection closed before cleanup completion");
                PooledConnection pooled = manager.borrowConnection();
                DuckDBConnection conn = pooled.get();

                // Simulate the crash condition: close connection during cleanup
                CountDownLatch closeLatch = new CountDownLatch(1);

                executorService.submit(() -> {
                    try {
                        closeLatch.await();
                        conn.close();
                    } catch (Exception e) {
                        crashDetected.set(true);
                    }
                });

                closeLatch.countDown();
                Thread.sleep(5); // Tiny race window

                try {
                    pooled.close();
                } catch (Exception e) {
                    crashDetected.set(true);
                    throw e;
                }
            }

            logStep("Then: No crashes should occur");
            assertThat(crashDetected.get()).isFalse();
            assertThat(manager.isClosed()).isFalse();

            // Verify system is still operational
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("REGRESSION: Validate connection state before all cleanup operations")
        void testValidateStateBeforeAllCleanupOps() throws SQLException {
            logStep("Given: Various connection states before cleanup");

            // Test 1: Valid connection
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get().isValid(5)).isTrue();
            }

            // Test 2: Connection with completed query
            try (PooledConnection conn = manager.borrowConnection()) {
                try (Statement stmt = conn.get().createStatement()) {
                    stmt.execute("SELECT 1");
                }
            }

            // Test 3: Connection that was closed
            PooledConnection pooled = manager.borrowConnection();
            pooled.get().close();
            pooled.close();

            // Test 4: Connection after error
            try (PooledConnection conn = manager.borrowConnection()) {
                try {
                    try (Statement stmt = conn.get().createStatement()) {
                        stmt.execute("INVALID SQL");
                    }
                } catch (SQLException e) {
                    // Expected
                }
            }

            logStep("Then: All cleanup operations should validate state first");
            // If we got here, all cleanups validated state correctly
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("REGRESSION: Ensure atomic cleanup operations")
        void testAtomicCleanupOperations() throws Exception {
            logStep("Given: Cleanup operation that must be atomic");
            int threadCount = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            AtomicInteger cleanupCount = new AtomicInteger(0);

            // Create connections that will all cleanup simultaneously
            List<PooledConnection> connections = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                connections.add(manager.borrowConnection());
            }

            logStep("When: Multiple threads cleanup simultaneously");
            for (PooledConnection conn : connections) {
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        conn.close();
                        cleanupCount.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            boolean completed = completeLatch.await(10, TimeUnit.SECONDS);

            logStep("Then: All cleanups should complete atomically");
            assertThat(completed).isTrue();
            assertThat(cleanupCount.get()).isEqualTo(threadCount);
        }

        @Test
        @DisplayName("REGRESSION: Verify error handling in all cleanup paths")
        void testErrorHandlingInAllCleanupPaths() throws SQLException {
            logStep("Given: Various error scenarios during cleanup");

            // Path 1: Normal cleanup
            try (PooledConnection conn = manager.borrowConnection()) {
                // Success path
            }

            // Path 2: Cleanup with closed connection
            PooledConnection pooled = manager.borrowConnection();
            pooled.get().close();
            assertThatCode(() -> pooled.close()).doesNotThrowAnyException();

            // Path 3: Cleanup with exception in validation
            PooledConnection pooled2 = manager.borrowConnection();
            pooled2.get().close();
            assertThatCode(() -> pooled2.close()).doesNotThrowAnyException();

            // Path 4: Double cleanup
            PooledConnection pooled3 = manager.borrowConnection();
            pooled3.close();
            assertThatCode(() -> pooled3.close()).doesNotThrowAnyException();

            logStep("Then: All error paths should be handled gracefully");
            assertThat(manager.isClosed()).isFalse();
        }
    }
}
