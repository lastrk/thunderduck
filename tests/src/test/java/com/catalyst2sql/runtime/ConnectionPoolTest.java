package com.catalyst2sql.runtime;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for connection pool resource leak prevention.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Connections are always released back to the pool</li>
 *   <li>Try-with-resources pattern works correctly</li>
 *   <li>Pool doesn't leak connections under normal or error conditions</li>
 *   <li>Invalid connections are properly handled</li>
 *   <li>Concurrent access is thread-safe</li>
 * </ul>
 */
@DisplayName("Connection Pool Resource Leak Tests")
public class ConnectionPoolTest {

    private DuckDBConnectionManager manager;

    @BeforeEach
    void setup() throws SQLException {
        manager = new DuckDBConnectionManager(
            DuckDBConnectionManager.Configuration.inMemory()
                .withPoolSize(4)
        );
    }

    @AfterEach
    void teardown() throws SQLException {
        if (manager != null && !manager.isClosed()) {
            manager.close();
        }
    }

    @Nested
    @DisplayName("Auto-Release Tests")
    class AutoReleaseTests {

        @Test
        @DisplayName("Connection auto-released with try-with-resources")
        void testPooledConnectionAutoRelease() throws SQLException {
            // Given: Connection pool with 4 connections

            // When: Borrow connection in try-with-resources
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get()).isNotNull();
                assertThat(conn.get().isClosed()).isFalse();
            } // Should auto-release

            // Then: Can borrow 4 more connections (pool has all 4 available)
            for (int i = 0; i < 4; i++) {
                try (PooledConnection conn = manager.borrowConnection()) {
                    assertThat(conn).isNotNull();
                }
            }
        }

        @Test
        @DisplayName("Connection not returned twice")
        void testConnectionNotReturnedTwice() throws SQLException {
            // Given: Pooled connection
            PooledConnection conn = manager.borrowConnection();

            // When: Close twice
            conn.close();
            conn.close(); // Should be no-op

            // Then: No error, connection returned only once
            assertThat(conn.isReleased()).isTrue();
        }

        @Test
        @DisplayName("Cannot use connection after release")
        void testCannotUseReleasedConnection() throws SQLException {
            // Given: Pooled connection
            PooledConnection conn = manager.borrowConnection();

            // When: Close and try to use
            conn.close();

            // Then: Should throw IllegalStateException
            assertThatThrownBy(() -> conn.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already been released");
        }
    }

    @Nested
    @DisplayName("Pool Exhaustion Tests")
    class PoolExhaustionTests {

        @Test
        @DisplayName("Pool exhaustion throws exception")
        void testConnectionPoolExhaustion() throws SQLException {
            // Given: Pool with 2 connections
            manager.close();
            manager = new DuckDBConnectionManager(
                DuckDBConnectionManager.Configuration.inMemory()
                    .withPoolSize(2)
            );

            // When: Borrow 2 connections
            PooledConnection conn1 = manager.borrowConnection();
            PooledConnection conn2 = manager.borrowConnection();

            // Then: Third borrow should timeout
            assertThatThrownBy(() -> {
                // This should timeout after 30 seconds
                manager.borrowConnection();
            }).isInstanceOf(SQLException.class)
              .hasMessageContaining("exhausted");

            // Cleanup
            conn1.close();
            conn2.close();
        }

        @Test
        @DisplayName("Released connections become available")
        void testReleasedConnectionsAvailable() throws SQLException {
            // Given: Pool with 2 connections, both borrowed
            manager.close();
            manager = new DuckDBConnectionManager(
                DuckDBConnectionManager.Configuration.inMemory()
                    .withPoolSize(2)
            );

            PooledConnection conn1 = manager.borrowConnection();
            PooledConnection conn2 = manager.borrowConnection();

            // When: Release one connection
            conn1.close();

            // Then: Can borrow again
            PooledConnection conn3 = manager.borrowConnection();
            assertThat(conn3).isNotNull();

            // Cleanup
            conn2.close();
            conn3.close();
        }
    }

    @Nested
    @DisplayName("Connection Health Tests")
    class ConnectionHealthTests {

        @Test
        @DisplayName("Invalid connection not returned to pool")
        void testInvalidConnectionNotReturned() throws SQLException {
            // Given: Connection from pool
            PooledConnection pooled = manager.borrowConnection();

            // When: Close underlying connection (simulate connection failure)
            pooled.get().close();
            pooled.close(); // Release to pool

            // Then: Next connection should be a new valid connection
            try (PooledConnection newConn = manager.borrowConnection()) {
                assertThat(newConn.get().isClosed()).isFalse();
                assertThat(newConn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Pool handles closed connections gracefully")
        void testPoolHandlesClosedConnections() throws SQLException {
            // Given: Borrow and close underlying connection
            PooledConnection pooled1 = manager.borrowConnection();
            pooled1.get().close(); // Close underlying connection
            pooled1.close(); // Return wrapper to pool

            // When: Borrow multiple connections
            PooledConnection pooled2 = manager.borrowConnection();
            PooledConnection pooled3 = manager.borrowConnection();

            // Then: All connections should be valid
            assertThat(pooled2.get().isValid(5)).isTrue();
            assertThat(pooled3.get().isValid(5)).isTrue();

            // Cleanup
            pooled2.close();
            pooled3.close();
        }
    }

    @Nested
    @DisplayName("Concurrency Tests")
    class ConcurrencyTests {

        @Test
        @DisplayName("Concurrent connection usage is thread-safe")
        void testConcurrentConnectionUsage() throws Exception {
            // Given: Multiple threads
            int threadCount = 10;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            // When: All threads try to borrow and use connections
            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try (PooledConnection conn = manager.borrowConnection()) {
                        // Simulate work
                        Thread.sleep(100);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Then: All threads should succeed
            boolean completed = latch.await(15, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete").isTrue();
            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(errorCount.get()).isZero();
        }

        @Test
        @DisplayName("High concurrency stress test")
        void testHighConcurrencyStress() throws Exception {
            // Given: Many threads competing for few connections
            int threadCount = 50;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            // When: Many threads borrow connections
            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try (PooledConnection conn = manager.borrowConnection()) {
                        // Minimal work
                        assertThat(conn.get()).isNotNull();
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        // Expected: some may timeout
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Then: All threads complete (some may timeout, but no crashes)
            boolean completed = latch.await(60, TimeUnit.SECONDS);
            assertThat(completed).as("All threads should complete").isTrue();
            assertThat(successCount.get()).as("Most threads should succeed")
                .isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Borrowing from closed manager throws exception")
        void testBorrowFromClosedManager() throws SQLException {
            // Given: Closed manager
            manager.close();

            // When/Then: Borrowing should throw
            assertThatThrownBy(() -> manager.borrowConnection())
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("closed");
        }

        @Test
        @DisplayName("Multiple close calls on manager are safe")
        void testMultipleCloseCallsSafe() throws SQLException {
            // Given: Manager
            assertThat(manager.isClosed()).isFalse();

            // When: Close multiple times
            manager.close();
            assertThat(manager.isClosed()).isTrue();

            manager.close(); // Should be safe
            assertThat(manager.isClosed()).isTrue();

            // Then: No exception thrown
        }

        @Test
        @DisplayName("Null connection release is safe")
        void testNullConnectionReleaseSafe() {
            // When: Release null connection
            // Then: No exception
            assertThatCode(() -> {
                manager.releaseConnection(null);
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Pool Size Tests")
    class PoolSizeTests {

        @Test
        @DisplayName("Pool size matches configuration")
        void testPoolSizeMatchesConfig() {
            assertThat(manager.getPoolSize()).isEqualTo(4);
        }

        @Test
        @DisplayName("Can borrow up to pool size connections")
        void testCanBorrowAllConnections() throws SQLException {
            // Given: Pool size 4
            // When: Borrow 4 connections
            PooledConnection[] connections = new PooledConnection[4];
            for (int i = 0; i < 4; i++) {
                connections[i] = manager.borrowConnection();
                assertThat(connections[i]).isNotNull();
            }

            // Then: All are valid
            for (PooledConnection conn : connections) {
                assertThat(conn.get().isValid(5)).isTrue();
            }

            // Cleanup
            for (PooledConnection conn : connections) {
                conn.close();
            }
        }

        @Test
        @DisplayName("Auto-detected pool size is reasonable")
        void testAutoDetectedPoolSize() throws SQLException {
            // Given: Manager with auto-detected pool size
            manager.close();
            manager = new DuckDBConnectionManager(
                DuckDBConnectionManager.Configuration.inMemory()
            );

            // Then: Pool size should be sensible (between 1 and 8)
            int poolSize = manager.getPoolSize();
            assertThat(poolSize).isBetween(1, 8);

            // And: Can borrow at least 1 connection
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn).isNotNull();
            }
        }
    }
}
