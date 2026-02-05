package com.thunderduck.runtime;

import com.thunderduck.types.ArrayType;
import com.thunderduck.types.BinaryType;
import com.thunderduck.types.BooleanType;
import com.thunderduck.types.ByteType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DateType;
import com.thunderduck.types.DecimalType;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.FloatType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.LongType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.ShortType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TimestampType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
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

            // Get nullable and dataType from logical schema if available
            boolean nullable = true;  // Default to nullable
            DataType logicalType = null;
            if (logicalSchema != null && i < logicalSchema.size()) {
                StructField field = logicalSchema.fields().get(i);
                nullable = field.nullable();
                logicalType = field.dataType();
            }

            // Correct the field including children for complex types
            Field correctedField = correctField(duckField, nullable, logicalType);
            correctedFields.add(correctedField);
        }

        return new Schema(correctedFields, duckdbSchema.getCustomMetadata());
    }

    /**
     * Converts a thunderduck DataType to the corresponding Apache Arrow ArrowType.
     *
     * <p>This ensures that the Arrow schema matches the expected Spark types,
     * not the types that DuckDB returns (which may differ, e.g., BIGINT vs DOUBLE
     * for window function results).
     *
     * @param logicalType the thunderduck logical type
     * @return the corresponding ArrowType, or null if the type should not be converted
     */
    private ArrowType dataTypeToArrowType(DataType logicalType) {
        if (logicalType instanceof IntegerType) {
            return new ArrowType.Int(32, true);
        } else if (logicalType instanceof LongType) {
            return new ArrowType.Int(64, true);
        } else if (logicalType instanceof ShortType) {
            return new ArrowType.Int(16, true);
        } else if (logicalType instanceof ByteType) {
            return new ArrowType.Int(8, true);
        } else if (logicalType instanceof FloatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (logicalType instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (logicalType instanceof BooleanType) {
            return new ArrowType.Bool();
        } else if (logicalType instanceof StringType) {
            return new ArrowType.Utf8();
        } else if (logicalType instanceof BinaryType) {
            return new ArrowType.Binary();
        } else if (logicalType instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        } else if (logicalType instanceof TimestampType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        } else if (logicalType instanceof DecimalType) {
            DecimalType dt = (DecimalType) logicalType;
            return new ArrowType.Decimal(dt.precision(), dt.scale(), 128);
        }
        // For complex types (Array, Map, Struct), keep the original Arrow type
        // as these are handled separately in correctField
        return null;
    }

    /**
     * Recursively corrects a single field's nullable flags and types.
     *
     * @param arrowField the Arrow field from DuckDB
     * @param nullable the correct nullable flag for this field
     * @param logicalType the logical type with correct nullable and type info (may be null)
     * @return the corrected Arrow field
     */
    private Field correctField(Field arrowField, boolean nullable, DataType logicalType) {
        List<Field> correctedChildren = new ArrayList<>();
        List<Field> originalChildren = arrowField.getChildren();

        // Handle complex types that have children
        if (arrowField.getType() instanceof ArrowType.List && logicalType instanceof ArrayType) {
            // Arrow List has one child (element)
            ArrayType arrayType = (ArrayType) logicalType;
            if (originalChildren != null && !originalChildren.isEmpty()) {
                Field elementField = originalChildren.get(0);
                // The containsNull flag determines if elements can be null
                Field correctedElement = correctField(
                    elementField,
                    arrayType.containsNull(),
                    arrayType.elementType()
                );
                correctedChildren.add(correctedElement);
            }
        } else if (arrowField.getType() instanceof ArrowType.Map && logicalType instanceof MapType) {
            // Arrow Map has one child (entries struct with key and value)
            MapType mapType = (MapType) logicalType;
            if (originalChildren != null && !originalChildren.isEmpty()) {
                Field entriesField = originalChildren.get(0);
                List<Field> entryChildren = entriesField.getChildren();
                if (entryChildren != null && entryChildren.size() >= 2) {
                    // Keys can never be null, values use valueContainsNull
                    Field correctedKey = correctField(entryChildren.get(0), false, mapType.keyType());
                    Field correctedValue = correctField(entryChildren.get(1), mapType.valueContainsNull(), mapType.valueType());

                    List<Field> correctedEntryChildren = new ArrayList<>();
                    correctedEntryChildren.add(correctedKey);
                    correctedEntryChildren.add(correctedValue);

                    // Create corrected entries field
                    FieldType entriesType = new FieldType(
                        entriesField.isNullable(),
                        entriesField.getType(),
                        entriesField.getDictionary(),
                        entriesField.getMetadata()
                    );
                    correctedChildren.add(new Field(entriesField.getName(), entriesType, correctedEntryChildren));
                } else {
                    correctedChildren.add(entriesField);
                }
            }
        } else if (arrowField.getType() instanceof ArrowType.Struct && logicalType instanceof StructType) {
            // Arrow Struct has children for each field
            StructType structType = (StructType) logicalType;
            if (originalChildren != null) {
                for (int i = 0; i < originalChildren.size(); i++) {
                    Field child = originalChildren.get(i);
                    if (i < structType.size()) {
                        StructField logicalField = structType.fields().get(i);
                        Field correctedChild = correctField(child, logicalField.nullable(), logicalField.dataType());
                        correctedChildren.add(correctedChild);
                    } else {
                        correctedChildren.add(child);
                    }
                }
            }
        } else {
            // Not a complex type or no logical type info - keep original children
            correctedChildren = originalChildren;
        }

        // Determine the correct Arrow type: use logical type if available, otherwise DuckDB's type
        ArrowType correctedType = null;
        if (logicalType != null) {
            correctedType = dataTypeToArrowType(logicalType);
        }
        if (correctedType == null) {
            // Fall back to DuckDB's type for complex types or when no logical type available
            correctedType = arrowField.getType();
        }

        // Create field with correct nullable flag, corrected type, and corrected children
        FieldType fieldType = new FieldType(
            nullable,
            correctedType,
            arrowField.getDictionary(),
            arrowField.getMetadata()
        );
        return new Field(arrowField.getName(), fieldType, correctedChildren);
    }

    /**
     * Copies batch data to a new VectorSchemaRoot with the corrected schema.
     *
     * <p>Uses splitAndTransfer for efficient zero-copy when types match.
     * For type conversions (e.g., Decimal to Long), copies data value by value.
     *
     * @param source the source batch from DuckDB
     * @return a new batch with the corrected schema
     */
    private VectorSchemaRoot copyWithCorrectedSchema(VectorSchemaRoot source) {
        VectorSchemaRoot corrected = VectorSchemaRoot.create(correctedSchema, allocator);
        int rowCount = source.getRowCount();

        for (int col = 0; col < source.getFieldVectors().size(); col++) {
            FieldVector srcVector = source.getVector(col);
            FieldVector dstVector = corrected.getVector(col);

            // Check if types are compatible for direct transfer
            if (canDirectTransfer(srcVector, dstVector)) {
                // Use efficient splitAndTransfer for same-type vectors
                srcVector.makeTransferPair(dstVector).splitAndTransfer(0, rowCount);
            } else {
                // Handle type conversion (e.g., Decimal to Long)
                copyWithTypeConversion(srcVector, dstVector, rowCount);
            }
        }

        corrected.setRowCount(rowCount);
        return corrected;
    }

    /**
     * Checks if two vectors can use direct transfer (same type or compatible).
     */
    private boolean canDirectTransfer(FieldVector src, FieldVector dst) {
        // Same vector class = direct transfer is safe
        return src.getClass().equals(dst.getClass());
    }

    /**
     * Copies data from source to destination with type conversion.
     * Handles cases like DecimalVector to BigIntVector (Long) or Float8Vector (Double).
     */
    private void copyWithTypeConversion(FieldVector src, FieldVector dst, int rowCount) {
        dst.allocateNew();

        if (src instanceof DecimalVector && dst instanceof BigIntVector) {
            // Decimal to Long conversion
            DecimalVector srcDecimal = (DecimalVector) src;
            BigIntVector dstLong = (BigIntVector) dst;
            for (int i = 0; i < rowCount; i++) {
                if (srcDecimal.isNull(i)) {
                    dstLong.setNull(i);
                } else {
                    BigDecimal value = srcDecimal.getObject(i);
                    dstLong.set(i, value.longValue());
                }
            }
            dstLong.setValueCount(rowCount);
        } else if (src instanceof DecimalVector && dst instanceof Float8Vector) {
            // Decimal to Double conversion
            DecimalVector srcDecimal = (DecimalVector) src;
            Float8Vector dstDouble = (Float8Vector) dst;
            for (int i = 0; i < rowCount; i++) {
                if (srcDecimal.isNull(i)) {
                    dstDouble.setNull(i);
                } else {
                    BigDecimal value = srcDecimal.getObject(i);
                    dstDouble.set(i, value.doubleValue());
                }
            }
            dstDouble.setValueCount(rowCount);
        } else {
            // Fallback: try direct transfer (may fail for incompatible types)
            logger.warn("Attempting direct transfer for potentially incompatible types: {} -> {}",
                src.getClass().getSimpleName(), dst.getClass().getSimpleName());
            src.makeTransferPair(dst).splitAndTransfer(0, rowCount);
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
