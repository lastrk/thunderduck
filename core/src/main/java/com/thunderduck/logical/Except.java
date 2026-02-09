package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Arrays;

/**
 * Logical plan node for EXCEPT operations.
 *
 * <p>Returns rows from the left relation that don't appear in the right relation.
 */
public final class Except extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean distinct;

    public Except(LogicalPlan left, LogicalPlan right, boolean distinct) {
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
            return leftSql + " EXCEPT " + rightSql;
        } else {
            return leftSql + " EXCEPT ALL " + rightSql;
        }
    }

    @Override
    public StructType inferSchema() {
        // EXCEPT uses the schema from the left side
        return left.inferSchema();
    }

    @Override
    public String toString() {
        return "Except[distinct=" + distinct + "]";
    }
}