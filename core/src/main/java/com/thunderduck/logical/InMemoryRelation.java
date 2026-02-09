package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing an in-memory collection of rows.
 *
 * <p>This is used for small datasets that are materialized in memory, such as:
 * <ul>
 *   <li>spark.createDataFrame(data)</li>
 *   <li>Cached query results</li>
 *   <li>Small test datasets</li>
 * </ul>
 *
 * <p>For SQL generation, this will typically create a VALUES clause or a temporary table.
 */
public final class InMemoryRelation extends LogicalPlan {

    private final List<Row> data;

    /**
     * Creates an in-memory relation node.
     *
     * @param data the rows of data
     * @param schema the schema
     */
    public InMemoryRelation(List<Row> data, StructType schema) {
        super(); // No children
        this.data = new ArrayList<>(Objects.requireNonNull(data, "data must not be null"));
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
    }

    /**
     * Returns the rows in this relation.
     *
     * @return an unmodifiable list of rows
     */
    public List<Row> data() {
        return Collections.unmodifiableList(data);
    }

    /**
     * Returns the number of rows.
     *
     * @return the row count
     */
    public int size() {
        return data.size();
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // SQL generation will be implemented by the generator
        // Example: VALUES (1, 'a'), (2, 'b'), (3, 'c')
        throw new UnsupportedOperationException("SQL generation not yet implemented");
    }

    @Override
    public StructType inferSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return String.format("InMemoryRelation(rows=%d, schema=%s)", data.size(), schema);
    }
}
