package com.catalyst2sql.logical;

import com.catalyst2sql.types.StructType;
import java.util.Arrays;
import java.util.Objects;

/**
 * Logical plan node representing a union operation.
 *
 * <p>This node combines rows from two relations with the same schema.
 *
 * <p>Examples:
 * <pre>
 *   df1.union(df2)           // UNION ALL (includes duplicates)
 *   df1.unionByName(df2)     // UNION ALL with name-based column matching
 *   df1.distinct()           // Can be used after union to remove duplicates
 * </pre>
 *
 * <p>SQL generation:
 * <pre>
 * (left) UNION ALL (right)   // For all=true
 * (left) UNION (right)       // For all=false (distinct)
 * </pre>
 */
public class Union extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean all;

    /**
     * Creates a union node.
     *
     * @param left the left relation
     * @param right the right relation
     * @param all true for UNION ALL (keep duplicates), false for UNION (remove duplicates)
     */
    public Union(LogicalPlan left, LogicalPlan right, boolean all) {
        super(Arrays.asList(left, right));
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.all = all;

        // Verify schemas are compatible (if available)
        StructType leftSchema = left.schema();
        StructType rightSchema = right.schema();
        if (leftSchema != null && rightSchema != null) {
            if (leftSchema.size() != rightSchema.size()) {
                throw new IllegalArgumentException(
                    String.format("Union requires same number of columns: left has %d, right has %d",
                                leftSchema.size(), rightSchema.size()));
            }
            // TODO: Add stricter type compatibility checking
        }
    }

    /**
     * Creates a UNION ALL node (keeps duplicates).
     *
     * @param left the left relation
     * @param right the right relation
     */
    public Union(LogicalPlan left, LogicalPlan right) {
        this(left, right, true);
    }

    /**
     * Returns the left relation.
     *
     * @return the left child
     */
    public LogicalPlan left() {
        return left;
    }

    /**
     * Returns the right relation.
     *
     * @return the right child
     */
    public LogicalPlan right() {
        return right;
    }

    /**
     * Returns whether this is UNION ALL (true) or UNION (false).
     *
     * @return true for UNION ALL, false for UNION
     */
    public boolean all() {
        return all;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        StringBuilder sql = new StringBuilder();

        // Generate left subquery
        sql.append("(");
        sql.append(generator.generate(left));
        sql.append(")");

        // Generate UNION or UNION ALL
        if (all) {
            sql.append(" UNION ALL ");
        } else {
            sql.append(" UNION ");
        }

        // Generate right subquery
        sql.append("(");
        sql.append(generator.generate(right));
        sql.append(")");

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        // Union returns the schema of the left relation
        return left.schema();
    }

    @Override
    public String toString() {
        return String.format("Union(all=%s)", all);
    }
}
