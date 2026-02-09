package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Objects;

/**
 * Logical plan node representing an empty relation.
 *
 * <p>This is used for:
 * <ul>
 *   <li>Empty DataFrames</li>
 *   <li>Base case for certain operations</li>
 *   <li>Placeholder relations</li>
 * </ul>
 *
 * <p>SQL generation will produce a SELECT with no rows:
 * <pre>SELECT * FROM (VALUES ()) AS t WHERE FALSE</pre>
 */
public final class LocalRelation extends LogicalPlan {

    /**
     * Creates an empty local relation.
     *
     * @param schema the schema
     */
    public LocalRelation(StructType schema) {
        super(); // No children
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // SQL generation will be implemented by the generator
        throw new UnsupportedOperationException("SQL generation not yet implemented");
    }

    @Override
    public StructType inferSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return String.format("LocalRelation(schema=%s)", schema);
    }
}
