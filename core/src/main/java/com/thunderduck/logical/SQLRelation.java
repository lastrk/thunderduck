package com.thunderduck.logical;

import com.thunderduck.types.StructType;

/**
 * A relation that represents a raw SQL query.
 *
 * <p>This is used when the user provides SQL directly rather than
 * using DataFrame operations.
 */
public class SQLRelation extends LogicalPlan {

    private final String sql;

    public SQLRelation(String sql) {
        super();
        this.sql = sql;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // Return the SQL as-is (parent nodes will wrap in parens if needed)
        return sql;
    }

    @Override
    public StructType inferSchema() {
        // Schema inference would require executing the query
        // For now, return an empty schema
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