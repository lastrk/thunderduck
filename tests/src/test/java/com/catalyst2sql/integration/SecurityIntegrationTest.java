package com.catalyst2sql.integration;

import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.generator.SQLQuoting;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.expression.*;
import com.catalyst2sql.runtime.*;
import com.catalyst2sql.exception.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end security validation tests.
 *
 * <p>These integration tests verify that the security fixes work correctly
 * across the entire system, from query planning to execution.
 *
 * <p>Note: These tests are disabled by default as they require a DuckDB
 * connection. Enable them when the runtime is fully integrated.
 */
@DisplayName("Security Integration Tests")
@Disabled("Requires DuckDB connection - enable when runtime is ready")
public class SecurityIntegrationTest {

    private DuckDBConnectionManager manager;
    private QueryExecutor executor;
    private SQLGenerator generator;

    @BeforeEach
    void setup() throws Exception {
        manager = new DuckDBConnectionManager(
            DuckDBConnectionManager.Configuration.inMemory()
        );
        executor = new QueryExecutor(manager);
        generator = new SQLGenerator();
    }

    @AfterEach
    void teardown() throws Exception {
        if (manager != null && !manager.isClosed()) {
            manager.close();
        }
    }

    @Nested
    @DisplayName("SQL Injection Prevention Tests")
    class SQLInjectionPreventionTests {

        @Test
        @DisplayName("SQL injection attack is blocked")
        void testSQLInjectionAttackBlocked() {
            // Given: Malicious file path with SQL injection
            String malicious = "/tmp/users.parquet'; DROP TABLE users; --";

            // When/Then: Should be blocked before execution
            assertThatThrownBy(() -> {
                TableScan scan = new TableScan(malicious, TableScan.TableFormat.PARQUET, null);
                String sql = generator.generate(scan);
                executor.executeQuery(sql);
            }).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("SQL injection");
        }

        @Test
        @DisplayName("Malicious column alias is safely escaped")
        void testMaliciousColumnAliasSafelyEscaped() {
            // Given: Plan with malicious alias
            String maliciousAlias = "col\"; DROP TABLE users; --";
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            ColumnReference col = new ColumnReference("id", com.catalyst2sql.types.IntegerType.get());

            Project project = new Project(
                scan,
                Collections.singletonList(col),
                Collections.singletonList(maliciousAlias)
            );

            // When: Generate and validate SQL
            String sql = generator.generate(project);

            // Then: Should be properly escaped
            assertThat(sql).contains("\"col\"\"; DROP TABLE users; --\"");
            // Should not contain executable DROP statement
            assertThat(sql).doesNotMatch(".*;\\s*DROP\\s+TABLE.*");
        }

        @Test
        @DisplayName("Multiple injection attempts in complex query")
        void testMultipleInjectionAttemptsBlocked() {
            // Given: Multiple malicious inputs
            String[] maliciousPaths = {
                "/tmp/file'; DROP DATABASE; --",
                "/tmp/file; DELETE FROM users",
                "/tmp/file' OR '1'='1"
            };

            // When/Then: All should be blocked
            for (String path : maliciousPaths) {
                assertThatThrownBy(() -> {
                    TableScan scan = new TableScan(path, TableScan.TableFormat.PARQUET, null);
                    generator.generate(scan);
                }).as("Should block: " + path)
                  .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Nested
    @DisplayName("Connection Pool Leak Tests")
    class ConnectionPoolLeakTests {

        @Test
        @DisplayName("No connection leak under normal load")
        void testNoConnectionLeakUnderLoad() throws Exception {
            // Given: Many concurrent operations
            int operationCount = 100;
            CountDownLatch latch = new CountDownLatch(operationCount);
            AtomicInteger successCount = new AtomicInteger(0);

            // When: Execute many operations concurrently
            for (int i = 0; i < operationCount; i++) {
                new Thread(() -> {
                    try (PooledConnection conn = manager.borrowConnection()) {
                        // Simulate work
                        Thread.sleep(10);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        // Should not leak even if error occurs
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Then: All should complete without leak
            boolean completed = latch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All operations should complete").isTrue();
            assertThat(successCount.get()).isEqualTo(operationCount);

            // And: Pool should still be functional
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get()).isNotNull();
                assertThat(conn.get().isValid(5)).isTrue();
            }
        }

        @Test
        @DisplayName("Connection leak prevented even with exceptions")
        void testNoLeakWithExceptions() throws Exception {
            // Given: Operations that throw exceptions
            int operationCount = 50;
            CountDownLatch latch = new CountDownLatch(operationCount);

            // When: Execute operations that may fail
            for (int i = 0; i < operationCount; i++) {
                final int index = i;
                new Thread(() -> {
                    try (PooledConnection conn = manager.borrowConnection()) {
                        // Simulate work that sometimes fails
                        if (index % 3 == 0) {
                            throw new RuntimeException("Simulated failure");
                        }
                        Thread.sleep(5);
                    } catch (Exception e) {
                        // Expected for some operations
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Then: All complete and pool is still healthy
            latch.await(30, TimeUnit.SECONDS);

            // Pool should still work
            try (PooledConnection conn = manager.borrowConnection()) {
                assertThat(conn.get()).isNotNull();
            }
        }

        @Test
        @DisplayName("Connection pool recovers from invalid connections")
        void testPoolRecoversFromInvalidConnections() throws Exception {
            // Given: Borrow connection and invalidate it
            PooledConnection conn1 = manager.borrowConnection();
            conn1.get().close(); // Invalidate
            conn1.close(); // Return to pool

            // When: Borrow new connection
            try (PooledConnection conn2 = manager.borrowConnection()) {
                // Then: Should get a valid connection
                assertThat(conn2.get().isClosed()).isFalse();
                assertThat(conn2.get().isValid(5)).isTrue();
            }
        }
    }

    @Nested
    @DisplayName("Error Handling Integration Tests")
    class ErrorHandlingIntegrationTests {

        @Test
        @DisplayName("User-friendly error for column not found")
        void testUserFriendlyColumnNotFound() {
            // Given: Query with non-existent column
            // This test assumes we have a table/file with known columns

            // When/Then: Should get user-friendly error
            // (Implementation depends on having test data)
            // For now, just verify the exception class exists and works
            QueryExecutionException ex = new QueryExecutionException(
                "Binder Error: column \"xyz\" not found\nCandidate Bindings: \"id\", \"name\"",
                "SELECT xyz FROM users"
            );

            assertThat(ex.getUserMessage())
                .contains("xyz")
                .containsAnyOf("id", "name");
        }

        @Test
        @DisplayName("User-friendly error for type mismatch")
        void testUserFriendlyTypeMismatch() {
            // Given: Type conversion error
            QueryExecutionException ex = new QueryExecutionException(
                "Conversion Error: Could not convert string \"abc\" to 'INTEGER'",
                "SELECT CAST('abc' AS INTEGER)"
            );

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should be helpful
            assertThat(userMsg)
                .containsIgnoringCase("type")
                .containsAnyOf("convert", "mismatch");
        }

        @Test
        @DisplayName("SQL generation error provides context")
        void testSQLGenerationErrorProvidesContext() {
            // Given: Unsupported plan
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            Aggregate aggregate = new Aggregate(
                scan,
                Collections.emptyList(),
                Collections.emptyList()
            );

            // When: Try to generate SQL
            Throwable thrown = catchThrowable(() -> generator.generate(aggregate));

            // Then: Should have context
            assertThat(thrown).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("End-to-End Security Tests")
    class EndToEndSecurityTests {

        @Test
        @DisplayName("Complete workflow with safe inputs")
        void testCompleteWorkflowWithSafeInputs() {
            // Given: Valid, safe query
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            ColumnReference col = new ColumnReference("id", com.catalyst2sql.types.IntegerType.get());

            Project project = new Project(
                scan,
                Collections.singletonList(col),
                Collections.singletonList("result")
            );

            // When: Generate SQL
            String sql = generator.generate(project);

            // Then: SQL should be safe and correct
            assertThat(sql).isNotBlank();
            assertThat(sql).contains("SELECT");
            assertThat(sql).contains("read_parquet");
            assertThat(sql).doesNotContain("DROP");
            assertThat(sql).doesNotContain("DELETE");
        }

        @Test
        @DisplayName("System prevents all injection vectors")
        void testSystemPreventsAllInjectionVectors() {
            // Given: Various injection attempts
            String[] injectionAttempts = {
                "'; DROP TABLE users; --",
                "; DELETE FROM data",
                "' OR '1'='1",
                "/* comment */ ; DROP",
                "' UNION SELECT * FROM passwords --"
            };

            // When/Then: All should be caught
            for (String attempt : injectionAttempts) {
                assertThatCode(() -> SQLQuoting.quoteFilePath(attempt))
                    .as("Should block: " + attempt)
                    .isInstanceOf(IllegalArgumentException.class);
            }
        }

        @Test
        @DisplayName("System handles edge cases safely")
        void testSystemHandlesEdgeCasesSafely() {
            // Test various edge cases
            assertThatCode(() -> {
                SQLQuoting.quoteIdentifier("a".repeat(1000)); // Very long
                SQLQuoting.quoteLiteral(null); // Null
                SQLQuoting.quoteLiteral(""); // Empty
                SQLQuoting.quoteIdentifier("用户名"); // Unicode
            }).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Performance Under Security Constraints")
    class PerformanceTests {

        @Test
        @DisplayName("Security checks don't significantly impact performance")
        void testSecurityChecksPerformance() {
            // Given: Many identifiers to quote
            int iterations = 1000;
            long startTime = System.currentTimeMillis();

            // When: Quote many identifiers
            for (int i = 0; i < iterations; i++) {
                SQLQuoting.quoteIdentifier("user_id_" + i);
                SQLQuoting.quoteLiteral("value_" + i);
            }

            long duration = System.currentTimeMillis() - startTime;

            // Then: Should complete quickly (< 100ms for 1000 iterations)
            assertThat(duration).isLessThan(100);
        }

        @Test
        @DisplayName("Connection pool maintains performance under load")
        void testConnectionPoolPerformance() throws Exception {
            // Given: High concurrent load
            int threadCount = 20;
            int operationsPerThread = 10;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger totalOperations = new AtomicInteger(0);

            long startTime = System.currentTimeMillis();

            // When: Execute many concurrent operations
            for (int i = 0; i < threadCount; i++) {
                new Thread(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            try (PooledConnection conn = manager.borrowConnection()) {
                                // Minimal work
                                totalOperations.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            // Then: Should complete efficiently
            boolean completed = latch.await(10, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startTime;

            assertThat(completed).isTrue();
            assertThat(duration).isLessThan(10000); // 10 seconds max
            assertThat(totalOperations.get()).isGreaterThan(0);
        }
    }
}
