package com.thunderduck.runtime;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Executes SQL queries and returns streaming Arrow batch iterators.
 *
 * <p>Uses DuckDB's native arrowExportStream() for zero-copy Arrow streaming.
 * This executor manages connection pooling and Arrow memory allocation.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ArrowStreamingExecutor executor = new ArrowStreamingExecutor(connectionManager)) {
 *     try (ArrowBatchIterator iter = executor.executeStreaming("SELECT * FROM large_table")) {
 *         while (iter.hasNext()) {
 *             VectorSchemaRoot batch = iter.next();
 *             processBatch(batch);
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see <a href="https://duckdb.org/docs/stable/clients/java">DuckDB Java JDBC Client</a>
 */
public class ArrowStreamingExecutor implements StreamingQueryExecutor, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowStreamingExecutor.class);

    private final DuckDBConnectionManager connectionManager;
    private final BufferAllocator allocator;
    private final int defaultBatchSize;
    private volatile boolean closed = false;

    /**
     * Create executor with default configuration.
     *
     * @param connectionManager the connection pool manager
     */
    public ArrowStreamingExecutor(DuckDBConnectionManager connectionManager) {
        this(connectionManager, new RootAllocator(Long.MAX_VALUE), StreamingConfig.DEFAULT_BATCH_SIZE);
    }

    /**
     * Create executor with custom allocator and batch size.
     *
     * @param connectionManager the connection pool manager
     * @param allocator Arrow memory allocator (caller retains ownership)
     * @param defaultBatchSize default rows per batch
     */
    public ArrowStreamingExecutor(DuckDBConnectionManager connectionManager,
                                  BufferAllocator allocator,
                                  int defaultBatchSize) {
        this.connectionManager = connectionManager;
        this.allocator = allocator;
        this.defaultBatchSize = StreamingConfig.normalizeBatchSize(defaultBatchSize);

        logger.info("ArrowStreamingExecutor created with defaultBatchSize={}", this.defaultBatchSize);
    }

    @Override
    public ArrowBatchIterator executeStreaming(String sql) throws SQLException {
        return executeStreaming(sql, defaultBatchSize);
    }

    @Override
    public ArrowBatchIterator executeStreaming(String sql, int batchSize) throws SQLException {
        if (closed) {
            throw new SQLException("Executor is closed");
        }

        int normalizedBatchSize = StreamingConfig.normalizeBatchSize(batchSize);

        if (logger.isDebugEnabled()) {
            String truncatedSql = sql.length() > 100 ? sql.substring(0, 100) + "..." : sql;
            logger.debug("Executing streaming query (batchSize={}): {}", normalizedBatchSize, truncatedSql);
        }

        PooledConnection pooled = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            pooled = connectionManager.borrowConnection();
            DuckDBConnection conn = pooled.get();

            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            // Transfer ownership of resources to ArrowBatchStream
            return new ArrowBatchStream(rs, pooled, stmt, allocator, normalizedBatchSize);

        } catch (Exception e) {
            // Cleanup on error - ArrowBatchStream was not created
            closeQuietly(rs);
            closeQuietly(stmt);
            if (pooled != null) {
                pooled.close();
            }

            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Failed to execute streaming query", e);
        }
    }

    /**
     * Get the allocator used by this executor.
     *
     * @return the Arrow buffer allocator
     */
    public BufferAllocator getAllocator() {
        return allocator;
    }

    /**
     * Get the default batch size.
     *
     * @return default rows per batch
     */
    public int getDefaultBatchSize() {
        return defaultBatchSize;
    }

    @Override
    public void close() {
        if (closed) {
            return;  // Idempotent
        }
        closed = true;

        logger.info("Closing ArrowStreamingExecutor");

        // Close the allocator if we own it (created with default constructor)
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                logger.warn("Error closing Arrow allocator: {}", e.getMessage());
            }
        }
    }

    private void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                logger.warn("Error closing resource: {}", e.getMessage());
            }
        }
    }
}
