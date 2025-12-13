package com.thunderduck.runtime;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Streaming Arrow batch iterator backed by DuckDB's native Arrow export.
 *
 * <p>This class wraps DuckDB's arrowExportStream() to provide batch-by-batch
 * iteration with proper resource management. Uses zero-copy Arrow streaming
 * for efficient large result handling.
 *
 * <p>Usage:
 * <pre>{@code
 * try (ArrowBatchStream stream = new ArrowBatchStream(resultSet, connection, stmt, allocator, 8192)) {
 *     while (stream.hasNext()) {
 *         VectorSchemaRoot batch = stream.next();
 *         // Process batch - do NOT close, owned by stream
 *     }
 * }
 * }</pre>
 *
 * @see <a href="https://duckdb.org/docs/stable/clients/java">DuckDB Java JDBC Client</a>
 */
public class ArrowBatchStream implements ArrowBatchIterator {

    private static final Logger logger = LoggerFactory.getLogger(ArrowBatchStream.class);

    private final ArrowReader reader;
    private final PooledConnection connection;
    private final Statement statement;
    private final ResultSet resultSet;

    private Schema schema;
    private boolean initialized = false;
    private boolean hasMoreBatches = true;
    private boolean batchLoaded = false;  // Track if a batch is loaded and not yet consumed
    private boolean closed = false;
    private long totalRowCount = 0;
    private int batchCount = 0;
    private Exception error = null;

    /**
     * Create a streaming batch iterator from a DuckDB ResultSet.
     *
     * @param resultSet DuckDB ResultSet (will be unwrapped to DuckDBResultSet)
     * @param connection Pooled connection (returned on close)
     * @param statement Statement that created the ResultSet
     * @param allocator Arrow memory allocator
     * @param batchSize Rows per batch hint
     * @throws SQLException if the ResultSet cannot be unwrapped or Arrow export fails
     */
    public ArrowBatchStream(ResultSet resultSet,
                           PooledConnection connection,
                           Statement statement,
                           BufferAllocator allocator,
                           int batchSize) throws SQLException {

        this.resultSet = resultSet;
        this.connection = connection;
        this.statement = statement;

        try {
            // Unwrap to DuckDB-specific ResultSet for Arrow export
            DuckDBResultSet duckRS = resultSet.unwrap(DuckDBResultSet.class);

            // Get native Arrow stream from DuckDB
            // API: arrowExportStream(BufferAllocator allocator, long batchSize)
            this.reader = (ArrowReader) duckRS.arrowExportStream(allocator, batchSize);

            logger.debug("ArrowBatchStream created with batchSize={}", batchSize);
        } catch (SQLException e) {
            // Cleanup on construction failure
            closeQuietly(resultSet, "ResultSet");
            closeQuietly(statement, "Statement");
            if (connection != null) connection.close();
            throw e;
        } catch (Exception e) {
            // Cleanup on construction failure
            closeQuietly(resultSet, "ResultSet");
            closeQuietly(statement, "Statement");
            if (connection != null) connection.close();
            throw new SQLException("Failed to create Arrow stream", e);
        }
    }

    @Override
    public Schema getSchema() {
        ensureInitialized();
        return schema;
    }

    @Override
    public boolean hasNext() {
        if (closed || error != null || !hasMoreBatches) {
            return false;
        }

        // If we already have a batch loaded that hasn't been consumed, return true
        if (batchLoaded) {
            return true;
        }

        try {
            hasMoreBatches = reader.loadNextBatch();
            batchLoaded = hasMoreBatches;

            if (hasMoreBatches && !initialized) {
                schema = reader.getVectorSchemaRoot().getSchema();
                initialized = true;
            }

            return hasMoreBatches;
        } catch (IOException e) {
            error = e;
            logger.error("Error loading Arrow batch", e);
            return false;
        }
    }

    @Override
    public VectorSchemaRoot next() {
        if (closed) {
            throw new IllegalStateException("Stream is closed");
        }
        if (!hasMoreBatches || !batchLoaded) {
            throw new java.util.NoSuchElementException("No more batches");
        }

        try {
            VectorSchemaRoot batch = reader.getVectorSchemaRoot();
            totalRowCount += batch.getRowCount();
            batchCount++;
            batchLoaded = false;  // Mark batch as consumed

            if (logger.isDebugEnabled()) {
                logger.debug("Batch {}: {} rows (total: {})", batchCount, batch.getRowCount(), totalRowCount);
            }

            return batch;  // DO NOT close - owned by reader
        } catch (IOException e) {
            error = e;
            logger.error("Error getting VectorSchemaRoot", e);
            throw new RuntimeException("Failed to get Arrow batch", e);
        }
    }

    @Override
    public long getTotalRowCount() {
        return totalRowCount;
    }

    @Override
    public int getBatchCount() {
        return batchCount;
    }

    @Override
    public boolean hasError() {
        return error != null;
    }

    @Override
    public Exception getError() {
        return error;
    }

    @Override
    public void close() {
        if (closed) {
            return;  // Idempotent
        }
        closed = true;

        logger.debug("Closing ArrowBatchStream: {} batches, {} rows", batchCount, totalRowCount);

        // Close resources in reverse order of acquisition
        closeQuietly(reader, "ArrowReader");
        closeQuietly(resultSet, "ResultSet");
        closeQuietly(statement, "Statement");

        // Return connection to pool
        if (connection != null) {
            connection.close();
        }
    }

    private void ensureInitialized() {
        if (!initialized && !closed) {
            // Try to get schema from VectorSchemaRoot - DuckDB Arrow export
            // provides schema even when there are no rows (empty result set)
            try {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (root != null) {
                    schema = root.getSchema();
                    initialized = true;
                    logger.debug("Schema initialized from VectorSchemaRoot: {} fields",
                                 schema != null ? schema.getFields().size() : 0);
                }
            } catch (IOException e) {
                logger.debug("Could not get schema from VectorSchemaRoot: {}", e.getMessage());
            }

            // Fallback: try loading first batch
            if (schema == null && hasMoreBatches) {
                hasNext();  // This will initialize schema
            }
        }
        if (schema == null) {
            throw new IllegalStateException("Schema not available - no batches loaded");
        }
    }

    private void closeQuietly(AutoCloseable resource, String name) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                logger.warn("Error closing {}: {}", name, e.getMessage());
            }
        }
    }
}
