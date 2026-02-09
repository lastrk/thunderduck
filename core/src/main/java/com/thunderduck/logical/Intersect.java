package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Arrays;

/**
 * Logical plan node for INTERSECT operations.
 *
 * <p>Returns rows that appear in both left and right relations.
 */
public final class Intersect extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean distinct;

    public Intersect(LogicalPlan left, LogicalPlan right, boolean distinct) {
        super(Arrays.asList(left, right));
        this.left = left;
        this.right = right;
        this.distinct = distinct;
    }

    public LogicalPlan left() {
        return left;
    }

    public LogicalPlan right() {
        return right;
    }

    public boolean distinct() {
        return distinct;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        String leftSql = generator.generate(left);
        String rightSql = generator.generate(right);

        if (distinct) {
            return leftSql + " INTERSECT " + rightSql;
        } else {
            return leftSql + " INTERSECT ALL " + rightSql;
        }
    }

    @Override
    public StructType inferSchema() {
        // INTERSECT uses the schema from the left side
        return left.inferSchema();
    }

    @Override
    public String toString() {
        return "Intersect[distinct=" + distinct + "]";
    }
}