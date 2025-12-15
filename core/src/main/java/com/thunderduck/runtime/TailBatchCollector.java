package com.thunderduck.runtime;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Memory-efficient tail collector using a circular buffer of Arrow batches.
 *
 * <p>This collector streams through Arrow batches and keeps only enough data
 * to return the last N rows. Instead of materializing all data in memory,
 * it maintains a sliding window of recent batches.
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Stream through all batches from the source iterator</li>
 *   <li>Keep a deque of recent batches whose total row count >= N</li>
 *   <li>When total rows exceeds N, discard oldest batches</li>
 *   <li>At the end, trim the first batch to return exactly N rows</li>
 * </ol>
 *
 * <p>Memory complexity: O(N) where N is the tail limit, not O(total_rows).
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ArrowBatchIterator source = executor.executeStreaming(sql)) {
 *     TailBatchCollector collector = new TailBatchCollector(allocator, 100);
 *     VectorSchemaRoot result = collector.collect(source);
 *     // result contains the last 100 rows
 * }
 * }</pre>
 */
public class TailBatchCollector {

    private static final Logger logger = LoggerFactory.getLogger(TailBatchCollector.class);

    private final BufferAllocator allocator;
    private final int tailLimit;

    /**
     * Creates a tail collector.
     *
     * @param allocator Arrow memory allocator for result buffers
     * @param tailLimit maximum number of rows to keep (last N)
     */
    public TailBatchCollector(BufferAllocator allocator, int tailLimit) {
        if (tailLimit < 0) {
            throw new IllegalArgumentException("tailLimit must be non-negative");
        }
        this.allocator = allocator;
        this.tailLimit = tailLimit;
    }

    /**
     * Collects the last N rows from the source iterator.
     *
     * <p>This method iterates through all batches, keeping only enough
     * recent data to satisfy the tail limit. The returned VectorSchemaRoot
     * is owned by the caller and must be closed when done.
     *
     * @param source the source batch iterator
     * @return VectorSchemaRoot containing the last N rows
     */
    public VectorSchemaRoot collect(ArrowBatchIterator source) {
        if (tailLimit == 0) {
            // Return empty result with schema
            return VectorSchemaRoot.create(source.getSchema(), allocator);
        }

        // Deque of (VectorSchemaRoot, rowCount) - keeps recent batches
        Deque<BatchEntry> recentBatches = new ArrayDeque<>();
        long totalRows = 0;

        // Stream through all batches, keeping circular buffer
        while (source.hasNext()) {
            VectorSchemaRoot batch = source.next();
            int batchRows = batch.getRowCount();

            if (batchRows == 0) {
                continue;
            }

            // Copy the batch since the iterator may reuse the VectorSchemaRoot
            VectorSchemaRoot copy = copyBatch(batch);
            recentBatches.addLast(new BatchEntry(copy, batchRows));
            totalRows += batchRows;

            // Trim from front if we have more than tailLimit rows
            while (totalRows > tailLimit && recentBatches.size() > 1) {
                BatchEntry oldest = recentBatches.peekFirst();
                if (totalRows - oldest.rowCount >= tailLimit) {
                    // Can remove entire oldest batch
                    recentBatches.removeFirst();
                    oldest.root.close();
                    totalRows -= oldest.rowCount;
                } else {
                    // Need to keep part of oldest batch
                    break;
                }
            }
        }

        if (recentBatches.isEmpty()) {
            // No data - return empty result
            return VectorSchemaRoot.create(source.getSchema(), allocator);
        }

        logger.debug("Tail collector: kept {} batches, {} total rows, limit {}",
            recentBatches.size(), totalRows, tailLimit);

        // Build final result from retained batches
        return buildResult(recentBatches, totalRows, source.getSchema());
    }

    /**
     * Copy a VectorSchemaRoot batch.
     */
    private VectorSchemaRoot copyBatch(VectorSchemaRoot source) {
        VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
        copy.setRowCount(source.getRowCount());

        for (int col = 0; col < source.getFieldVectors().size(); col++) {
            FieldVector srcVector = source.getVector(col);
            FieldVector dstVector = copy.getVector(col);

            // Use transfer pairs for efficient copy
            srcVector.makeTransferPair(dstVector).splitAndTransfer(0, source.getRowCount());
        }

        return copy;
    }

    /**
     * Build the final result from retained batches.
     */
    private VectorSchemaRoot buildResult(Deque<BatchEntry> batches, long totalRows, Schema schema) {
        // Calculate how many rows to skip from the first batch
        int skipRows = (int) Math.max(0, totalRows - tailLimit);

        // Calculate actual result size
        int resultRows = (int) Math.min(totalRows, tailLimit);

        // Create result
        VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
        result.setRowCount(resultRows);

        // Allocate vectors
        for (FieldVector vector : result.getFieldVectors()) {
            vector.allocateNew();
        }

        int destRow = 0;

        for (BatchEntry entry : batches) {
            int srcStart = 0;
            int srcCount = entry.rowCount;

            // For first batch, skip rows if needed
            if (skipRows > 0) {
                if (skipRows >= srcCount) {
                    // Skip entire batch
                    skipRows -= srcCount;
                    continue;
                } else {
                    srcStart = skipRows;
                    srcCount -= skipRows;
                    skipRows = 0;
                }
            }

            // Copy rows from this batch
            for (int col = 0; col < entry.root.getFieldVectors().size(); col++) {
                FieldVector srcVector = entry.root.getVector(col);
                FieldVector dstVector = result.getVector(col);

                for (int row = 0; row < srcCount; row++) {
                    copyValue(srcVector, srcStart + row, dstVector, destRow + row);
                }
            }

            destRow += srcCount;
        }

        // Set final row count
        result.setRowCount(destRow);

        // Close batch copies
        for (BatchEntry entry : batches) {
            entry.root.close();
        }

        return result;
    }

    /**
     * Copy a single value between vectors.
     */
    private void copyValue(FieldVector src, int srcIdx, FieldVector dst, int dstIdx) {
        if (src.isNull(srcIdx)) {
            dst.setNull(dstIdx);
            return;
        }

        if (src instanceof IntVector) {
            ((IntVector) dst).setSafe(dstIdx, ((IntVector) src).get(srcIdx));
        } else if (src instanceof BigIntVector) {
            ((BigIntVector) dst).setSafe(dstIdx, ((BigIntVector) src).get(srcIdx));
        } else if (src instanceof Float8Vector) {
            ((Float8Vector) dst).setSafe(dstIdx, ((Float8Vector) src).get(srcIdx));
        } else if (src instanceof Float4Vector) {
            ((Float4Vector) dst).setSafe(dstIdx, ((Float4Vector) src).get(srcIdx));
        } else if (src instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) src).get(srcIdx);
            ((VarCharVector) dst).setSafe(dstIdx, bytes);
        } else if (src instanceof BitVector) {
            ((BitVector) dst).setSafe(dstIdx, ((BitVector) src).get(srcIdx));
        } else if (src instanceof DateDayVector) {
            ((DateDayVector) dst).setSafe(dstIdx, ((DateDayVector) src).get(srcIdx));
        } else if (src instanceof TimeStampMicroVector) {
            ((TimeStampMicroVector) dst).setSafe(dstIdx, ((TimeStampMicroVector) src).get(srcIdx));
        } else if (src instanceof DecimalVector) {
            java.math.BigDecimal val = ((DecimalVector) src).getObject(srcIdx);
            ((DecimalVector) dst).setSafe(dstIdx, val);
        } else if (src instanceof TinyIntVector) {
            ((TinyIntVector) dst).setSafe(dstIdx, ((TinyIntVector) src).get(srcIdx));
        } else if (src instanceof SmallIntVector) {
            ((SmallIntVector) dst).setSafe(dstIdx, ((SmallIntVector) src).get(srcIdx));
        } else if (src instanceof VarBinaryVector) {
            byte[] bytes = ((VarBinaryVector) src).get(srcIdx);
            ((VarBinaryVector) dst).setSafe(dstIdx, bytes);
        } else {
            // Fallback for other types - try getObject/set pattern
            logger.warn("Unhandled vector type for copy: {}", src.getClass().getSimpleName());
        }
    }

    /**
     * Entry in the batch buffer.
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
