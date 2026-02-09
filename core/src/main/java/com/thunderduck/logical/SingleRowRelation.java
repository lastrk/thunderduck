package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Collections;

/**
 * A relation that produces a single row with no columns.
 *
 * <p>This is used for expressions that don't require an input table,
 * such as SELECT 1 or SELECT ABS(-1).
 */
public final class SingleRowRelation extends LogicalPlan {

    public SingleRowRelation() {
        super();
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // DuckDB doesn't require FROM clause for single-row selects
        // But we can use dual table if needed
        return "";
    }

    @Override
    public StructType inferSchema() {
        // Empty schema
        return new StructType(Collections.emptyList());
    }

    @Override
    public String toString() {
        return "SingleRowRelation";
    }
}