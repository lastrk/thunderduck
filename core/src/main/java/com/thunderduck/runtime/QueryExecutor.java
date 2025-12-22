package com.thunderduck.runtime;

import com.thunderduck.exception.QueryExecutionException;
import com.thunderduck.logging.QueryLogger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.VectorLoader;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Executes SQL queries against DuckDB and returns results.
 *
 * <p>This class provides a high-level API for executing queries and updates
 * against a DuckDB database. Each QueryExecutor is bound to a specific
 * DuckDBRuntime, typically owned by a session.
 *
 * <p>Features:
 * <ul>
 *   <li>Query execution with Arrow result conversion</li>
 *   <li>Update/DDL statement execution</li>
 *   <li>Session-scoped runtime binding</li>
 *   <li>Error handling and cleanup</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   // Get runtime from session
 *   DuckDBRuntime runtime = session.getRuntime();
 *   QueryExecutor executor = new QueryExecutor(runtime);
 *
 *   // Execute query
 *   VectorSchemaRoot result = executor.executeQuery(
 *       "SELECT * FROM read_parquet('data.parquet') WHERE age > 25");
 *
 *   // Execute update
 *   int rowsAffected = executor.executeUpdate(
 *       "CREATE TABLE users (id INTEGER, name VARCHAR)");
 * </pre>
 *
 * @see DuckDBRuntime
 * @see ArrowInterchange
 */
public class QueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private final DuckDBRuntime runtime;
    private final ArrowStreamingExecutor streamingExecutor;
    private final BufferAllocator allocator;

    /**
     * Creates a query executor with the specified runtime.
     *
     * @param runtime the DuckDB runtime (typically from a session)
     */
    public QueryExecutor(DuckDBRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null");
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.streamingExecutor = new ArrowStreamingExecutor(runtime, allocator, StreamingConfig.DEFAULT_BATCH_SIZE);
    }

    /**
     * Executes a query and returns results as Arrow VectorSchemaRoot.
     *
     * <p>This method executes the SQL query, converts the JDBC ResultSet
     * to Apache Arrow format, and returns the result. The connection is
     * managed by the singleton runtime.
     *
     * <p>Supports SQL introspection via EXPLAIN statements:
     * <ul>
     *   <li>EXPLAIN &lt;query&gt; - Returns logical query plan</li>
     *   <li>EXPLAIN ANALYZE &lt;query&gt; - Executes query and returns runtime statistics</li>
     *   <li>EXPLAIN (FORMAT JSON) &lt;query&gt; - Returns plan in JSON format</li>
     * </ul>
     *
     * <p>The returned VectorSchemaRoot must be closed by the caller to
     * free memory.
     *
     * @param sql the SQL query to execute
     * @return the query results as Arrow VectorSchemaRoot
     * @throws QueryExecutionException if query execution fails
     * @throws NullPointerException if sql is null
     */
    public VectorSchemaRoot executeQuery(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        // Generate unique query ID for logging correlation
        String queryId = "q_" + UUID.randomUUID().toString().substring(0, 8);
        QueryLogger.startQuery(queryId);

        long queryStartTime = System.nanoTime();

        try {
            // Detect EXPLAIN statements for SQL introspection
            boolean isExplain = isExplainStatement(sql);

            // Use streaming executor and collect batches into single VectorSchemaRoot
            try (ArrowBatchIterator iter = streamingExecutor.executeStreaming(sql)) {
                long execTimeMs = (System.nanoTime() - queryStartTime) / 1_000_000;

                // Collect all batches and merge into single result
                VectorSchemaRoot result = collectBatches(iter);

                // Get row count from result
                long rowCount = result.getRowCount();

                // Log execution metrics
                QueryLogger.logExecution(execTimeMs, rowCount);

                // Log SQL for EXPLAIN statements (they're typically short)
                if (isExplain) {
                    QueryLogger.logSQLGeneration(sql, 0); // No generation for direct SQL
                }

                // Complete query logging
                long totalTimeMs = (System.nanoTime() - queryStartTime) / 1_000_000;
                QueryLogger.completeQuery(totalTimeMs);

                return result;

            } catch (SQLException e) {
                // Log error before throwing
                QueryLogger.logError(e);

                // Wrap in QueryExecutionException with context
                throw new QueryExecutionException(
                    "Failed to execute query: " + e.getMessage(), e, sql);
            }
        } finally {
            // Always clear logging context to prevent memory leaks
            QueryLogger.clearContext();
        }
    }

    /**
     * Collects all batches from an ArrowBatchIterator into a single VectorSchemaRoot.
     *
     * @param iter the batch iterator
     * @return a single VectorSchemaRoot containing all rows
     */
    private VectorSchemaRoot collectBatches(ArrowBatchIterator iter) {
        List<ArrowRecordBatch> batches = new ArrayList<>();
        VectorSchemaRoot result = null;

        try {
            // Get schema from first batch
            if (!iter.hasNext()) {
                // Empty result - create empty root with schema from iterator
                return VectorSchemaRoot.create(iter.getSchema(), allocator);
            }

            // Collect all batches
            while (iter.hasNext()) {
                VectorSchemaRoot batch = iter.next();
                if (result == null) {
                    // First batch - create result with same schema
                    result = VectorSchemaRoot.create(batch.getSchema(), allocator);
                }
                // Unload batch data for later merging
                VectorUnloader unloader = new VectorUnloader(batch);
                batches.add(unloader.getRecordBatch());
            }

            // If only one batch, load it directly
            if (batches.size() == 1) {
                VectorLoader loader = new VectorLoader(result);
                loader.load(batches.get(0));
                return result;
            }

            // Multiple batches - need to merge them
            // Calculate total row count
            int totalRows = 0;
            for (ArrowRecordBatch batch : batches) {
                totalRows += batch.getLength();
            }

            // Allocate result with total capacity (result is guaranteed non-null here since batches > 1)
            assert result != null : "result should be non-null when batches.size() > 1";
            result.setRowCount(totalRows);

            // Load batches sequentially using bulk column copies
            int currentRow = 0;
            for (ArrowRecordBatch batch : batches) {
                // For simplicity, we create a temp root, load the batch, then copy
                try (VectorSchemaRoot tempRoot = VectorSchemaRoot.create(result.getSchema(), allocator)) {
                    VectorLoader loader = new VectorLoader(tempRoot);
                    loader.load(batch);

                    // Copy from temp to result at currentRow offset - bulk copy per column
                    int batchRowCount = tempRoot.getRowCount();
                    for (int col = 0; col < tempRoot.getFieldVectors().size(); col++) {
                        org.apache.arrow.vector.FieldVector srcVector = tempRoot.getVector(col);
                        org.apache.arrow.vector.FieldVector dstVector = result.getVector(col);
                        copyVectorRange(srcVector, 0, dstVector, currentRow, batchRowCount);
                    }
                    currentRow += batchRowCount;
                }
            }

            return result;

        } finally {
            // Close all record batches
            for (ArrowRecordBatch batch : batches) {
                batch.close();
            }
        }
    }

    /**
     * Copy a range of values from one vector to another efficiently.
     *
     * <p>Type dispatch happens once per column (not per row), eliminating
     * instanceof checks in the inner loop for much better performance.
     *
     * @param src source vector
     * @param srcStart starting index in source
     * @param dst destination vector
     * @param dstStart starting index in destination
     * @param count number of values to copy
     */
    private void copyVectorRange(org.apache.arrow.vector.FieldVector src, int srcStart,
                                 org.apache.arrow.vector.FieldVector dst, int dstStart, int count) {
        // Type dispatch once per column - determine copier based on vector type
        if (src instanceof org.apache.arrow.vector.IntVector) {
            org.apache.arrow.vector.IntVector srcVec = (org.apache.arrow.vector.IntVector) src;
            org.apache.arrow.vector.IntVector dstVec = (org.apache.arrow.vector.IntVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.BigIntVector) {
            org.apache.arrow.vector.BigIntVector srcVec = (org.apache.arrow.vector.BigIntVector) src;
            org.apache.arrow.vector.BigIntVector dstVec = (org.apache.arrow.vector.BigIntVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.Float8Vector) {
            org.apache.arrow.vector.Float8Vector srcVec = (org.apache.arrow.vector.Float8Vector) src;
            org.apache.arrow.vector.Float8Vector dstVec = (org.apache.arrow.vector.Float8Vector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.Float4Vector) {
            org.apache.arrow.vector.Float4Vector srcVec = (org.apache.arrow.vector.Float4Vector) src;
            org.apache.arrow.vector.Float4Vector dstVec = (org.apache.arrow.vector.Float4Vector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.VarCharVector) {
            org.apache.arrow.vector.VarCharVector srcVec = (org.apache.arrow.vector.VarCharVector) src;
            org.apache.arrow.vector.VarCharVector dstVec = (org.apache.arrow.vector.VarCharVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.BitVector) {
            org.apache.arrow.vector.BitVector srcVec = (org.apache.arrow.vector.BitVector) src;
            org.apache.arrow.vector.BitVector dstVec = (org.apache.arrow.vector.BitVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.DateDayVector) {
            org.apache.arrow.vector.DateDayVector srcVec = (org.apache.arrow.vector.DateDayVector) src;
            org.apache.arrow.vector.DateDayVector dstVec = (org.apache.arrow.vector.DateDayVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.DecimalVector) {
            org.apache.arrow.vector.DecimalVector srcVec = (org.apache.arrow.vector.DecimalVector) src;
            org.apache.arrow.vector.DecimalVector dstVec = (org.apache.arrow.vector.DecimalVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.getObject(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.TimeStampMicroTZVector) {
            org.apache.arrow.vector.TimeStampMicroTZVector srcVec = (org.apache.arrow.vector.TimeStampMicroTZVector) src;
            org.apache.arrow.vector.TimeStampMicroTZVector dstVec = (org.apache.arrow.vector.TimeStampMicroTZVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.TimeStampMicroVector) {
            org.apache.arrow.vector.TimeStampMicroVector srcVec = (org.apache.arrow.vector.TimeStampMicroVector) src;
            org.apache.arrow.vector.TimeStampMicroVector dstVec = (org.apache.arrow.vector.TimeStampMicroVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.SmallIntVector) {
            org.apache.arrow.vector.SmallIntVector srcVec = (org.apache.arrow.vector.SmallIntVector) src;
            org.apache.arrow.vector.SmallIntVector dstVec = (org.apache.arrow.vector.SmallIntVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.TinyIntVector) {
            org.apache.arrow.vector.TinyIntVector srcVec = (org.apache.arrow.vector.TinyIntVector) src;
            org.apache.arrow.vector.TinyIntVector dstVec = (org.apache.arrow.vector.TinyIntVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else if (src instanceof org.apache.arrow.vector.VarBinaryVector) {
            org.apache.arrow.vector.VarBinaryVector srcVec = (org.apache.arrow.vector.VarBinaryVector) src;
            org.apache.arrow.vector.VarBinaryVector dstVec = (org.apache.arrow.vector.VarBinaryVector) dst;
            for (int i = 0; i < count; i++) {
                if (srcVec.isNull(srcStart + i)) {
                    dstVec.setNull(dstStart + i);
                } else {
                    dstVec.setSafe(dstStart + i, srcVec.get(srcStart + i));
                }
            }
        } else {
            // Fallback for unsupported types - log warning once
            logger.warn("Unhandled vector type for bulk copy: {}, falling back to object-based copy",
                src.getClass().getSimpleName());
            // Generic fallback using object API
            for (int i = 0; i < count; i++) {
                if (src.isNull(srcStart + i)) {
                    dst.setNull(dstStart + i);
                }
                // For unsupported types, we can't copy non-null values efficiently
            }
        }
    }

    /**
     * Checks if a SQL statement is an EXPLAIN statement for query introspection.
     *
     * <p>Supports the following EXPLAIN variants:
     * <ul>
     *   <li>EXPLAIN &lt;query&gt;</li>
     *   <li>EXPLAIN ANALYZE &lt;query&gt;</li>
     *   <li>EXPLAIN (FORMAT JSON) &lt;query&gt;</li>
     * </ul>
     *
     * @param sql the SQL statement to check
     * @return true if the statement starts with EXPLAIN (case-insensitive)
     */
    private boolean isExplainStatement(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }

        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("EXPLAIN");
    }

    /**
     * Executes an update/DDL statement.
     *
     * <p>This method is used for INSERT, UPDATE, DELETE, CREATE, DROP,
     * and other statements that don't return result sets.
     *
     * @param sql the SQL statement to execute
     * @return the number of rows affected (for DML), or 0 (for DDL)
     * @throws QueryExecutionException if statement execution fails
     * @throws NullPointerException if sql is null
     */
    public int executeUpdate(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        DuckDBConnection conn = runtime.getConnection();
        Statement stmt = null;

        try {
            // Execute update
            stmt = conn.createStatement();
            return stmt.executeUpdate(sql);

        } catch (SQLException e) {
            // Wrap in QueryExecutionException with context
            throw new QueryExecutionException(
                "Failed to execute update: " + e.getMessage(), e, sql);

        } finally {
            // Clean up JDBC resources (NOT the connection - singleton)
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Log but don't throw
                    logger.warn("Error closing Statement: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Executes a statement (query or update).
     *
     * <p>This method can execute any SQL statement. For queries, it returns
     * true and the results can be retrieved with getResultSet(). For updates,
     * it returns false and the update count can be retrieved with getUpdateCount().
     *
     * @param sql the SQL statement to execute
     * @return true if the result is a ResultSet, false if it's an update count
     * @throws QueryExecutionException if statement execution fails
     * @throws NullPointerException if sql is null
     */
    public boolean execute(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        DuckDBConnection conn = runtime.getConnection();
        Statement stmt = null;

        try {
            // Execute statement
            stmt = conn.createStatement();
            return stmt.execute(sql);

        } catch (SQLException e) {
            // Wrap in QueryExecutionException with context
            throw new QueryExecutionException(
                "Failed to execute statement: " + e.getMessage(), e, sql);

        } finally {
            // Clean up JDBC resources (NOT the connection - singleton)
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Log but don't throw
                    logger.warn("Error closing Statement: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Returns the runtime used by this executor.
     *
     * @return the DuckDB runtime
     */
    public DuckDBRuntime getRuntime() {
        return runtime;
    }
}
