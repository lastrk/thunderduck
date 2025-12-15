package com.thunderduck.runtime;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.Iterator;

/**
 * Iterator over Arrow batches with metadata and resource management.
 *
 * <p>Implementations must properly manage Arrow memory resources. Callers should
 * use try-with-resources to ensure cleanup:
 *
 * <pre>{@code
 * try (ArrowBatchIterator iter = executor.executeStreaming(sql)) {
 *     while (iter.hasNext()) {
 *         VectorSchemaRoot batch = iter.next();
 *         // Process batch - do NOT close, owned by iterator
 *     }
 * }
 * }</pre>
 *
 * <p>Note: The VectorSchemaRoot returned by next() is owned by the iterator
 * and must NOT be closed by the caller. The same VectorSchemaRoot instance
 * may be reused between batches (with different data loaded).
 */
public interface ArrowBatchIterator extends Iterator<VectorSchemaRoot>, AutoCloseable {

    /**
     * Get the Arrow schema for result batches.
     *
     * <p>The schema is available after the first batch is loaded. Calling this
     * before hasNext() returns true may throw IllegalStateException.
     *
     * @return the Arrow schema
     * @throws IllegalStateException if schema is not yet available
     */
    Schema getSchema();

    /**
     * Total rows returned so far across all batches.
     *
     * @return cumulative row count
     */
    long getTotalRowCount();

    /**
     * Number of batches returned so far.
     *
     * @return batch count
     */
    int getBatchCount();

    /**
     * Check if there was an error during iteration.
     *
     * <p>If this returns true, hasNext() will return false and getError()
     * will return the exception that caused the failure.
     *
     * @return true if iteration failed with an error
     */
    boolean hasError();

    /**
     * Get the error that caused iteration to fail.
     *
     * @return the exception, or null if hasError() returns false
     */
    Exception getError();

    /**
     * Close the iterator and release all Arrow resources.
     *
     * <p>This method is idempotent - calling it multiple times has no effect.
     */
    @Override
    void close();
}
