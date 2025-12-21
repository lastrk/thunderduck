package com.thunderduck.logical;

import com.thunderduck.types.StructType;

/**
 * A relation that represents a raw SQL query.
 *
 * <p>This is used when the user provides SQL directly rather than
 * using DataFrame operations. An optional schema can be provided
 * for operations like PIVOT where the schema is known at plan time
 * and needs to preserve nullable information for Spark compatibility.
 */
public class SQLRelation extends LogicalPlan {

    private final String sql;
    private final StructType schema;

    /**
     * Creates a SQL relation without a known schema.
     * Schema inference will return an empty schema.
     */
    public SQLRelation(String sql) {
        super();
        this.sql = sql;
        this.schema = null;
    }

    /**
     * Creates a SQL relation with a known schema.
     * Used when the output schema is determined at plan time
     * (e.g., PIVOT operations) for correct nullable flag handling.
     */
    public SQLRelation(String sql, StructType schema) {
        super();
        this.sql = sql;
        this.schema = schema;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // Return the SQL as-is (parent nodes will wrap in parens if needed)
        return sql;
    }

    @Override
    public StructType inferSchema() {
        // Return provided schema if available, otherwise empty schema
        if (schema != null) {
            return schema;
        }
        return new StructType(java.util.Collections.emptyList());
    }

    @Override
    public String toString() {
        return "SQLRelation[" + sql + "]";
    }

    public String getSql() {
        return sql;
    }
}