package com.thunderduck.runtime;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Wrapping iterator that returns only the last N rows from a source iterator.
 *
 * <p>This iterator implements the tail operation by buffering batches until
 * the source is exhausted, then yielding only the final batches containing
 * the last N rows. It implements {@link ArrowBatchIterator} so it can be
 * used transparently with {@code StreamingResultHandler}.
 *
 * <h2>How It Works</h2>
 * <p>Unlike a regular pass-through iterator, TailBatchIterator must consume
 * ALL source batches before it can yield ANY results (since we don't know
 * which rows are "last" until we've seen everything). The iteration has two phases:
 * <ol>
 *   <li><b>Collection phase</b>: First call to hasNext() triggers full consumption
 *       of source iterator, keeping only batches needed for tail N rows</li>
 *   <li><b>Yield phase</b>: Subsequent hasNext()/next() calls yield the buffered tail batches</li>
 * </ol>
 *
 * <h2>Memory Efficiency</h2>
 * <p>Memory usage is O(N) where N is the tail limit, not O(total_rows). Old batches
 * are discarded as soon as we have enough rows without them.
 *
 * <h2>Buffer Ownership</h2>
 * <p>Because DuckDB's arrowExportStream() REUSES VectorSchemaRoot between batches,
 * we must copy batch data before advancing. This uses splitAndTransfer for efficiency.
 * The source and target allocators must share the same root allocator.
 *
 * <p>Example:
 * <pre>{@code
 * try (ArrowBatchIterator source = executor.executeStreaming(sql);
 *      ArrowBatchIterator tail = new TailBatchIterator(source, allocator, 100)) {
 *     // tail implements ArrowBatchIterator, yields last 100 rows
 *     while (tail.hasNext()) {
 *         VectorSchemaRoot batch = tail.next();
 *         // process tail batch
 *     }
 * }
 * }</pre>
 */
public class TailBatchIterator implements ArrowBatchIterator {

    private static final Logger logger = LoggerFactory.getLogger(TailBatchIterator.class);

    private final ArrowBatchIterator source;
    private final BufferAllocator allocator;
    private final int tailLimit;

    // Collection phase state
    private boolean collected = false;
    private Deque<BatchEntry> tailBatches = new ArrayDeque<>();
    private long totalRowsCollected = 0;
    private int totalBatchesProcessed = 0;

    // Yield phase state
    private int skipRowsInFirstBatch = 0;
    private BatchEntry currentBatch = null;
    private VectorSchemaRoot currentRoot = null;
    private boolean closed = false;
    private Exception error = null;

    // Stats
    private long totalRowsYielded = 0;
    private int batchesYielded = 0;

    /**
     * Create a tail iterator wrapping another iterator.
     *
     * @param source source iterator to wrap
     * @param allocator Arrow allocator (must share root with source's allocator)
     * @param tailLimit maximum rows to keep (last N)
     */
    public TailBatchIterator(ArrowBatchIterator source, BufferAllocator allocator, int tailLimit) {
        if (tailLimit < 0) {
            throw new IllegalArgumentException("tailLimit must be non-negative");
        }
        this.source = source;
        this.allocator = allocator;
        this.tailLimit = tailLimit;
    }

    @Override
    public Schema getSchema() {
        // Schema is available from source (which gets it from DuckDB)
        return source.getSchema();
    }

    @Override
    public boolean hasNext() {
        if (closed || error != null) {
            return false;
        }

        // First call triggers collection phase
        if (!collected) {
            collectTail();
            collected = true;
        }

        // Check if we have more tail batches to yield
        return currentBatch != null || !tailBatches.isEmpty();
    }

    @Override
    public VectorSchemaRoot next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
        if (!collected) {
            collectTail();
            collected = true;
        }

        // Get next batch to yield
        if (currentBatch == null) {
            if (tailBatches.isEmpty()) {
                throw new java.util.NoSuchElementException("No more batches");
            }
            currentBatch = tailBatches.removeFirst();

            // For first batch, we may need to skip some rows
            if (batchesYielded == 0 && skipRowsInFirstBatch > 0) {
                currentRoot = sliceBatch(currentBatch.root, skipRowsInFirstBatch,
                    currentBatch.rowCount - skipRowsInFirstBatch);
                currentBatch.root.close();  // Close original
            } else {
                currentRoot = currentBatch.root;
            }
        }

        VectorSchemaRoot result = currentRoot;
        totalRowsYielded += result.getRowCount();
        batchesYielded++;

        // Clear current for next call
        currentBatch = null;
        currentRoot = null;

        return result;
    }

    @Override
    public long getTotalRowCount() {
        return totalRowsYielded;
    }

    @Override
    public int getBatchCount() {
        return batchesYielded;
    }

    @Override
    public boolean hasError() {
        return error != null || source.hasError();
    }

    @Override
    public Exception getError() {
        return error != null ? error : source.getError();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Close any remaining buffered batches
        for (BatchEntry entry : tailBatches) {
            try {
                entry.root.close();
            } catch (Exception e) {
                logger.warn("Error closing tail batch", e);
            }
        }
        tailBatches.clear();

        if (currentRoot != null) {
            try {
                currentRoot.close();
            } catch (Exception e) {
                logger.warn("Error closing current root", e);
            }
        }

        // Close source
        try {
            source.close();
        } catch (Exception e) {
            logger.warn("Error closing source iterator", e);
        }

        logger.debug("TailBatchIterator closed: processed {} batches, yielded {} rows",
            totalBatchesProcessed, totalRowsYielded);
    }

    /**
     * Consume all source batches and keep only what's needed for tail N rows.
     */
    private void collectTail() {
        if (tailLimit == 0) {
            // Special case: tail(0) means no rows
            return;
        }

        try {
            long totalRows = 0;

            while (source.hasNext()) {
                VectorSchemaRoot batch = source.next();
                int batchRows = batch.getRowCount();
                totalBatchesProcessed++;

                if (batchRows == 0) {
                    continue;
                }

                // CRITICAL: Copy batch data BEFORE next hasNext()/next() call
                // because arrowExportStream() reuses VectorSchemaRoot
                VectorSchemaRoot copy = copyBatchWithTransfer(batch);
                tailBatches.addLast(new BatchEntry(copy, batchRows));
                totalRows += batchRows;

                // Eagerly trim old batches to bound memory
                while (totalRows > tailLimit && tailBatches.size() > 1) {
                    BatchEntry oldest = tailBatches.peekFirst();
                    if (totalRows - oldest.rowCount >= tailLimit) {
                        tailBatches.removeFirst();
                        oldest.root.close();
                        totalRows -= oldest.rowCount;
                    } else {
                        break;
                    }
                }
            }

            totalRowsCollected = totalRows;

            // Calculate how many rows to skip from first batch
            if (totalRows > tailLimit) {
                skipRowsInFirstBatch = (int) (totalRows - tailLimit);
            }

            logger.debug("TailBatchIterator: processed {} batches, kept {} batches ({} rows), " +
                "skip {} from first, limit {}",
                totalBatchesProcessed, tailBatches.size(), totalRows,
                skipRowsInFirstBatch, tailLimit);

        } catch (Exception e) {
            error = e;
            logger.error("Error collecting tail batches", e);
        }
    }

    /**
     * Copy a batch using efficient splitAndTransfer.
     */
    private VectorSchemaRoot copyBatchWithTransfer(VectorSchemaRoot source) {
        int rowCount = source.getRowCount();
        VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);

        for (int col = 0; col < source.getFieldVectors().size(); col++) {
            FieldVector srcVector = source.getVector(col);
            FieldVector dstVector = copy.getVector(col);
            srcVector.makeTransferPair(dstVector).splitAndTransfer(0, rowCount);
        }

        copy.setRowCount(rowCount);
        return copy;
    }

    /**
     * Create a sliced view of a batch (skip first N rows).
     */
    private VectorSchemaRoot sliceBatch(VectorSchemaRoot source, int offset, int length) {
        VectorSchemaRoot sliced = VectorSchemaRoot.create(source.getSchema(), allocator);

        for (int col = 0; col < source.getFieldVectors().size(); col++) {
            FieldVector srcVector = source.getVector(col);
            FieldVector dstVector = sliced.getVector(col);
            srcVector.makeTransferPair(dstVector).splitAndTransfer(offset, length);
        }

        sliced.setRowCount(length);
        return sliced;
    }

    /**
     * Batch entry for the buffer.
     */
    private static class BatchEntry {
        final VectorSchemaRoot root;
        final int rowCount;

        BatchEntry(VectorSchemaRoot root, int rowCount) {
            this.root = root;
            this.rowCount = rowCount;
        }
    }
}
