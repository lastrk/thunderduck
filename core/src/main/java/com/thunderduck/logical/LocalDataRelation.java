package com.thunderduck.logical;

import com.thunderduck.types.SchemaParser;
import com.thunderduck.types.StructType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing local data sent from Spark as Arrow IPC format.
 *
 * <p>This is used when Spark sends pre-computed results or local data as part of the plan,
 * such as:
 * <ul>
 *   <li>Results from {@code df.count()} operations</li>
 *   <li>Small DataFrames created with {@code spark.createDataFrame(data)}</li>
 *   <li>Cached or materialized local results</li>
 * </ul>
 *
 * <p>The data is stored in Apache Arrow IPC streaming format and will be deserialized
 * and converted to SQL (as VALUES clause or temporary table) during query planning.
 */
public class LocalDataRelation extends LogicalPlan {

    private static final Logger logger = LoggerFactory.getLogger(LocalDataRelation.class);
    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    private final byte[] arrowData;
    private final String schemaStr;
    private VectorSchemaRoot cachedRoot;

    /**
     * Creates a local data relation from Arrow IPC bytes.
     *
     * @param arrowData the Arrow IPC streaming format data (may be null for empty relation)
     * @param schemaStr the schema string in DDL or JSON format (may be null if data contains schema)
     */
    public LocalDataRelation(byte[] arrowData, String schemaStr) {
        super(); // No children
        this.arrowData = arrowData;
        this.schemaStr = schemaStr;
        this.cachedRoot = null;

        logger.debug("Created LocalDataRelation with {} bytes of data, schema: {}",
            arrowData != null ? arrowData.length : 0, schemaStr);
    }

    /**
     * Deserializes the Arrow IPC data into a VectorSchemaRoot.
     * The result is cached for subsequent calls.
     *
     * @return the deserialized VectorSchemaRoot, or null if no data
     * @throws RuntimeException if deserialization fails
     */
    public VectorSchemaRoot deserializeArrowData() {
        if (cachedRoot != null) {
            return cachedRoot;
        }

        if (arrowData == null || arrowData.length == 0) {
            logger.debug("No Arrow data to deserialize");
            return null;
        }

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(arrowData);
            ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);

            // Read the schema
            Schema arrowSchema = reader.getVectorSchemaRoot().getSchema();
            logger.debug("Deserialized Arrow schema: {}", arrowSchema);

            // Create a new VectorSchemaRoot that we own (not tied to the reader's lifecycle)
            VectorSchemaRoot ownedRoot = VectorSchemaRoot.create(arrowSchema, allocator);

            // Load first batch and copy data to our owned root
            boolean hasData = reader.loadNextBatch();
            if (!hasData) {
                logger.debug("No batches loaded from Arrow data");
                reader.close();
                inputStream.close();
                ownedRoot.close();
                return null;
            }

            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
            int rowCount = readerRoot.getRowCount();
            logger.debug("Loaded batch with {} rows", rowCount);

            // Copy data from reader's root to our owned root
            ownedRoot.setRowCount(rowCount);
            for (int i = 0; i < arrowSchema.getFields().size(); i++) {
                org.apache.arrow.vector.FieldVector sourceVector = readerRoot.getVector(i);
                org.apache.arrow.vector.FieldVector targetVector = ownedRoot.getVector(i);

                // Transfer ownership of the vector data
                // This is efficient - it doesn't copy the data, just transfers the buffer ownership
                sourceVector.makeTransferPair(targetVector).transfer();
            }

            // TODO: Handle multiple batches by combining them
            // For now, we only process the first batch
            if (reader.loadNextBatch()) {
                logger.warn("Multiple Arrow batches detected - only first batch will be used");
            }

            // Close the reader (safe now that we've transferred the data)
            reader.close();
            inputStream.close();

            cachedRoot = ownedRoot;
            return cachedRoot;

        } catch (IOException e) {
            logger.error("Failed to deserialize Arrow IPC data", e);
            throw new RuntimeException("Failed to deserialize Arrow IPC data", e);
        }
    }

    /**
     * Returns the Arrow data bytes.
     *
     * @return the Arrow IPC data, or null if empty
     */
    public byte[] getArrowData() {
        return arrowData;
    }

    /**
     * Returns the schema string.
     *
     * @return the schema string, or null if not provided
     */
    public String getSchemaString() {
        return schemaStr;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // The SQL generation is handled by the generator's visitor pattern
        // This method should not be called directly - instead, the generator
        // will call visitLocalDataRelation() when it encounters this node
        throw new UnsupportedOperationException(
            "LocalDataRelation.toSQL() should not be called directly. " +
            "SQL generation is handled by SQLGenerator.visitLocalDataRelation()");
    }

    @Override
    public StructType inferSchema() {
        if (schema != null) {
            return schema;
        }

        // Try to infer from Arrow data
        VectorSchemaRoot root = deserializeArrowData();
        if (root != null) {
            // Convert Arrow schema to StructType
            // TODO: Implement Arrow to StructType conversion
            logger.warn("Schema inference from Arrow not yet implemented");
        }

        // Try to parse schema string (Spark struct format: struct<name:type,...>)
        if (schemaStr != null && !schemaStr.isEmpty()) {
            try {
                schema = SchemaParser.parse(schemaStr);
                logger.debug("Parsed schema from string: {}", schema);
                return schema;
            } catch (Exception e) {
                logger.warn("Failed to parse schema string '{}': {}", schemaStr, e.getMessage());
            }
        }

        // Return empty schema as fallback
        logger.warn("Could not infer schema - returning empty StructType");
        return new StructType(new java.util.ArrayList<>());
    }

    @Override
    public String toString() {
        return String.format("LocalDataRelation(dataSize=%d, schema=%s)",
            arrowData != null ? arrowData.length : 0, schemaStr);
    }

    /**
     * Closes the cached VectorSchemaRoot and releases memory.
     */
    public void close() {
        if (cachedRoot != null) {
            cachedRoot.close();
            cachedRoot = null;
        }
    }
}
