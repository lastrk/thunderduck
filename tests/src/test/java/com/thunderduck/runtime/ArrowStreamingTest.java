package com.thunderduck.runtime;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Arrow streaming functionality.
 * Tests ArrowStreamingExecutor and ArrowBatchStream with real DuckDB.
 */
@DisplayName("Arrow Streaming Integration Tests")
class ArrowStreamingTest {

    private DuckDBConnectionManager connectionManager;
    private ArrowStreamingExecutor executor;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() throws Exception {
        connectionManager = new DuckDBConnectionManager();
        allocator = new RootAllocator(Long.MAX_VALUE);
        executor = new ArrowStreamingExecutor(connectionManager, allocator, 1024);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        // Note: allocator is closed by executor
    }

    @Test
    @DisplayName("Basic streaming query returns correct data")
    void basicStreamingQuery() throws Exception {
        try (ArrowBatchIterator iter = executor.executeStreaming("SELECT * FROM generate_series(1, 100)")) {
            assertTrue(iter.hasNext(), "Should have at least one batch");

            long totalRows = 0;
            int batchCount = 0;

            while (iter.hasNext()) {
                VectorSchemaRoot batch = iter.next();
                assertNotNull(batch, "Batch should not be null");
                assertTrue(batch.getRowCount() > 0, "Batch should have rows");
                totalRows += batch.getRowCount();
                batchCount++;
            }

            assertEquals(100, totalRows, "Should have 100 total rows");
            assertTrue(batchCount >= 1, "Should have at least one batch");
            assertEquals(totalRows, iter.getTotalRowCount(), "Total row count should match");
            assertEquals(batchCount, iter.getBatchCount(), "Batch count should match");
            assertFalse(iter.hasError(), "Should not have error");
        }
    }

    @Test
    @DisplayName("Streaming query with multiple batches")
    void multiBatchQuery() throws Exception {
        // Use small batch size to force multiple batches
        try (ArrowBatchIterator iter = executor.executeStreaming("SELECT * FROM generate_series(1, 5000)", 512)) {
            int batchCount = 0;
            long totalRows = 0;

            while (iter.hasNext()) {
                VectorSchemaRoot batch = iter.next();
                totalRows += batch.getRowCount();
                batchCount++;
            }

            assertEquals(5000, totalRows, "Should have 5000 total rows");
            assertTrue(batchCount > 1, "Should have multiple batches with batch size 512");
        }
    }

    @Test
    @DisplayName("Schema is available after first batch")
    void schemaAvailable() throws Exception {
        try (ArrowBatchIterator iter = executor.executeStreaming(
                "SELECT 1 as int_col, 'hello' as str_col, 3.14 as double_col")) {

            assertTrue(iter.hasNext(), "Should have data");
            iter.next();  // Load first batch

            Schema schema = iter.getSchema();
            assertNotNull(schema, "Schema should be available");
            assertEquals(3, schema.getFields().size(), "Should have 3 columns");
        }
    }

    @Test
    @DisplayName("Empty result set handled correctly")
    void emptyResultSet() throws Exception {
        try (ArrowBatchIterator iter = executor.executeStreaming(
                "SELECT * FROM generate_series(1, 0)")) {  // Empty range

            // DuckDB may return empty batch or no batches for empty result
            long totalRows = 0;
            while (iter.hasNext()) {
                VectorSchemaRoot batch = iter.next();
                totalRows += batch.getRowCount();
            }

            assertEquals(0, totalRows, "Should have 0 rows for empty result");
            assertFalse(iter.hasError(), "Should not have error");
        }
    }

    @Test
    @DisplayName("Executor uses default batch size")
    void defaultBatchSize() {
        assertEquals(1024, executor.getDefaultBatchSize(),
            "Default batch size should be 1024 (as set in setUp)");
    }

    @Test
    @DisplayName("Iterator is idempotent on close")
    void closeIsIdempotent() throws Exception {
        ArrowBatchIterator iter = executor.executeStreaming("SELECT 1");

        // Consume one batch
        if (iter.hasNext()) {
            iter.next();
        }

        // Multiple closes should not throw
        iter.close();
        iter.close();
        iter.close();
    }

    @Test
    @DisplayName("Error on closed iterator")
    void errorOnClosedIterator() throws Exception {
        ArrowBatchIterator iter = executor.executeStreaming("SELECT 1");
        iter.close();

        // hasNext should return false on closed iterator
        assertFalse(iter.hasNext(), "hasNext should return false after close");
    }

    @Test
    @DisplayName("Large result set streams correctly")
    void largeResultSet() throws Exception {
        // 100k rows should require multiple batches
        try (ArrowBatchIterator iter = executor.executeStreaming(
                "SELECT * FROM generate_series(1, 100000)", 8192)) {

            long totalRows = 0;
            int batchCount = 0;

            while (iter.hasNext()) {
                VectorSchemaRoot batch = iter.next();
                totalRows += batch.getRowCount();
                batchCount++;
            }

            assertEquals(100000, totalRows, "Should have 100k rows");
            assertTrue(batchCount > 1, "Should have multiple batches");
            assertFalse(iter.hasError(), "Should not have error");
        }
    }

    @Test
    @DisplayName("SQL error is properly propagated")
    void sqlErrorPropagated() {
        assertThrows(Exception.class, () -> {
            try (ArrowBatchIterator iter = executor.executeStreaming("SELECT * FROM nonexistent_table")) {
                // Should throw during executeStreaming or when loading first batch
                while (iter.hasNext()) {
                    iter.next();
                }
            }
        }, "Should throw exception for invalid SQL");
    }

    @Test
    @DisplayName("Multiple sequential queries work correctly")
    void multipleSequentialQueries() throws Exception {
        for (int i = 0; i < 3; i++) {
            try (ArrowBatchIterator iter = executor.executeStreaming("SELECT " + i)) {
                assertTrue(iter.hasNext());
                VectorSchemaRoot batch = iter.next();
                assertEquals(1, batch.getRowCount());
            }
        }
    }
}
