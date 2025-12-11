package com.thunderduck.runtime;

import java.sql.SQLException;

/**
 * Executes SQL queries with streaming Arrow results.
 *
 * <p>Unlike traditional query execution that materializes entire result sets,
 * streaming execution returns results batch-by-batch, enabling:
 * <ul>
 *   <li>Bounded memory usage regardless of result size</li>
 *   <li>Lower latency to first row</li>
 *   <li>Efficient processing of large results</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ArrowBatchIterator iter = executor.executeStreaming("SELECT * FROM large_table")) {
 *     while (iter.hasNext()) {
 *         VectorSchemaRoot batch = iter.next();
 *         processBatch(batch);
 *     }
 * }
 * }</pre>
 */
public interface StreamingQueryExecutor {

    /**
     * Execute query and return streaming batch iterator.
     *
     * <p>Caller must close the returned iterator to release resources.
     * Uses the default batch size from {@link StreamingConfig#DEFAULT_BATCH_SIZE}.
     *
     * @param sql SQL query to execute
     * @return ArrowBatchIterator for streaming results
     * @throws SQLException if query execution fails
     */
    ArrowBatchIterator executeStreaming(String sql) throws SQLException;

    /**
     * Execute query with custom batch size.
     *
     * <p>The batch size will be normalized to be within
     * [{@link StreamingConfig#MIN_BATCH_SIZE}, {@link StreamingConfig#MAX_BATCH_SIZE}].
     *
     * @param sql SQL query to execute
     * @param batchSize rows per batch (will be normalized)
     * @return ArrowBatchIterator for streaming results
     * @throws SQLException if query execution fails
     */
    ArrowBatchIterator executeStreaming(String sql, int batchSize) throws SQLException;
}
