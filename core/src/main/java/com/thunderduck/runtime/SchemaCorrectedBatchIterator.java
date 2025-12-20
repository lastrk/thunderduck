package com.thunderduck.runtime;

import com.thunderduck.types.StructType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapping iterator that corrects nullable flags in Arrow schema.
 *
 * <p>DuckDB returns all columns as nullable=true in Arrow output, but Spark has
 * specific nullable semantics:
 * <ul>
 *   <li>COUNT(*) and COUNT(col) return non-nullable BIGINT</li>
 *   <li>Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) return non-nullable INT</li>
 *   <li>Column references inherit nullable from source schema</li>
 * </ul>
 *
 * <p>This wrapper corrects the Arrow schema nullable flags to match the logical
 * plan's schema, ensuring Spark-compatible type information.
 *
 * <p>Example:
 * <pre>{@code
 * StructType logicalSchema = logicalPlan.schema();  // Has correct nullable
 * try (ArrowBatchIterator source = executor.executeStreaming(sql);
 *      ArrowBatchIterator corrected = new SchemaCorrectedBatchIterator(source, logicalSchema)) {
 *     while (corrected.hasNext()) {
 *         VectorSchemaRoot batch = corrected.next();
 *         // batch.getSchema() now has correct nullable flags
 *     }
 * }
 * }</pre>
 */
public class SchemaCorrectedBatchIterator implements ArrowBatchIterator {

    private static final Logger logger = LoggerFactory.getLogger(SchemaCorrectedBatchIterator.class);

    private final ArrowBatchIterator source;
    private final StructType logicalSchema;
    private final BufferAllocator allocator;

    private Schema correctedSchema;
    private long totalRowCount = 0;
    private int batchCount = 0;
    private boolean closed = false;

    /**
     * Creates a schema-correcting wrapper around a source iterator.
     *
     * @param source the source Arrow batch iterator (typically from DuckDB)
     * @param logicalSchema the logical plan schema with correct nullable flags
     * @param allocator the Arrow allocator to use (should share root with source's allocator)
     */
    public SchemaCorrectedBatchIterator(ArrowBatchIterator source, StructType logicalSchema,
                                        BufferAllocator allocator) {
        this.source = source;
        this.logicalSchema = logicalSchema;
        this.allocator = allocator;
        logger.debug("SchemaCorrectedBatchIterator created with {} fields in logical schema",
            logicalSchema != null ? logicalSchema.size() : 0);
    }

    @Override
    public Schema getSchema() {
        if (correctedSchema != null) {
            return correctedSchema;
        }
        // Build corrected schema from source schema
        return buildCorrectedSchema(source.getSchema());
    }

    @Override
    public boolean hasNext() {
        return !closed && source.hasNext();
    }

    @Override
    public VectorSchemaRoot next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }

        VectorSchemaRoot duckdbRoot = source.next();

        // Build corrected schema on first batch
        if (correctedSchema == null) {
            correctedSchema = buildCorrectedSchema(duckdbRoot.getSchema());
            logger.debug("Built corrected schema with {} fields", correctedSchema.getFields().size());
        }

        // Copy data to new root with corrected schema
        VectorSchemaRoot corrected = copyWithCorrectedSchema(duckdbRoot);
        totalRowCount += corrected.getRowCount();
        batchCount++;

        return corrected;
    }

    /**
     * Builds a corrected Arrow schema using nullable flags from the logical schema.
     *
     * @param duckdbSchema the schema from DuckDB (all nullable=true)
     * @return the corrected schema with proper nullable flags
     */
    private Schema buildCorrectedSchema(Schema duckdbSchema) {
        List<Field> correctedFields = new ArrayList<>();

        for (int i = 0; i < duckdbSchema.getFields().size(); i++) {
            Field duckField = duckdbSchema.getFields().get(i);

            // Get nullable from logical schema if available
            boolean nullable = true;  // Default to nullable
            if (logicalSchema != null && i < logicalSchema.size()) {
                nullable = logicalSchema.fields().get(i).nullable();
            }

            // Create field with correct nullable flag
            FieldType fieldType = new FieldType(
                nullable,
                duckField.getType(),
                duckField.getDictionary(),
                duckField.getMetadata()
            );
            correctedFields.add(new Field(duckField.getName(), fieldType, duckField.getChildren()));
        }

        return new Schema(correctedFields, duckdbSchema.getCustomMetadata());
    }

    /**
     * Copies batch data to a new VectorSchemaRoot with the corrected schema.
     *
     * <p>Uses splitAndTransfer for efficient zero-copy when possible.
     *
     * @param source the source batch from DuckDB
     * @return a new batch with the corrected schema
     */
    private VectorSchemaRoot copyWithCorrectedSchema(VectorSchemaRoot source) {
        VectorSchemaRoot corrected = VectorSchemaRoot.create(correctedSchema, allocator);

        // Copy data using efficient splitAndTransfer
        for (int col = 0; col < source.getFieldVectors().size(); col++) {
            FieldVector srcVector = source.getVector(col);
            FieldVector dstVector = corrected.getVector(col);
            srcVector.makeTransferPair(dstVector).splitAndTransfer(0, source.getRowCount());
        }

        corrected.setRowCount(source.getRowCount());
        return corrected;
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
        return source.hasError();
    }

    @Override
    public Exception getError() {
        return source.getError();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            source.close();
        } catch (Exception e) {
            logger.warn("Error closing source iterator", e);
        }

        // Note: allocator is not closed here as it's owned externally

        logger.debug("SchemaCorrectedBatchIterator closed: {} batches, {} rows",
            batchCount, totalRowCount);
    }
}
