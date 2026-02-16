package com.thunderduck.logical;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
public final class Union extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final boolean all;
    private final boolean byName;
    private final boolean allowMissingColumns;

    /**
     * Creates a union node.
     *
     * @param left the left relation
     * @param right the right relation
     * @param all true for UNION ALL (keep duplicates), false for UNION (remove duplicates)
     */
    public Union(LogicalPlan left, LogicalPlan right, boolean all) {
        this(left, right, all, false, false);
    }

    /**
     * Creates a union node with byName and allowMissingColumns options.
     *
     * @param left the left relation
     * @param right the right relation
     * @param all true for UNION ALL (keep duplicates), false for UNION (remove duplicates)
     * @param byName true to match columns by name instead of position
     * @param allowMissingColumns true to allow missing columns (fill with NULL)
     */
    public Union(LogicalPlan left, LogicalPlan right, boolean all,
                 boolean byName, boolean allowMissingColumns) {
        super(Arrays.asList(left, right));
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.all = all;
        this.byName = byName;
        this.allowMissingColumns = allowMissingColumns;

        // Verify schemas are compatible (if available)
        StructType leftSchema = left.schema();
        StructType rightSchema = right.schema();
        if (leftSchema != null && rightSchema != null) {
            if (!byName && leftSchema.size() != rightSchema.size()) {
                throw new IllegalArgumentException(
                    String.format("Union requires same number of columns: left has %d, right has %d",
                                leftSchema.size(), rightSchema.size()));
            }
            // Type compatibility is validated by QueryValidator.validateUnion().
            // Type widening (e.g., INT + BIGINT -> BIGINT) is handled in inferSchema()
            // and SQL generation adds CASTs when needed.
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

    /**
     * Returns whether this union matches columns by name instead of position.
     *
     * @return true for unionByName, false for positional union
     */
    public boolean byName() {
        return byName;
    }

    /**
     * Returns whether missing columns are allowed (filled with NULL).
     *
     * @return true to allow missing columns, false to require all columns
     */
    public boolean allowMissingColumns() {
        return allowMissingColumns;
    }

    /**
     * Generates SQL for this union node.
     *
     * <p>Note: This is dead code — {@code SQLGenerator.visit()} dispatches Union
     * directly to {@code visitUnion()}, which handles type-widening CASTs.
     * Kept in sync for completeness.
     */
    @Override
    public String toSQL(SQLGenerator generator) {
        // Delegate to generator which handles type-widening CASTs
        return generator.generate(this);
    }

    @Override
    public StructType inferSchema() {
        StructType leftSchema = left.schema();
        StructType rightSchema = right.schema();

        // If either schema is unavailable, fall back to left schema
        if (leftSchema == null || rightSchema == null) {
            return leftSchema;
        }

        // For byName unions or mismatched sizes, fall back to left schema
        // (byName reordering is handled at SQL generation time)
        if (byName || leftSchema.size() != rightSchema.size()) {
            return leftSchema;
        }

        // Compute widened types: for each column position, unify left and right types.
        // Column names always come from the left side (Spark convention).
        List<StructField> widenedFields = new ArrayList<>(leftSchema.size());
        boolean anyWidened = false;

        for (int i = 0; i < leftSchema.size(); i++) {
            StructField leftField = leftSchema.fieldAt(i);
            StructField rightField = rightSchema.fieldAt(i);
            DataType widenedType = TypeInferenceEngine.unifyTypes(
                leftField.dataType(), rightField.dataType());
            boolean widenedNullable = leftField.nullable() || rightField.nullable();

            if (!widenedType.equals(leftField.dataType()) || widenedNullable != leftField.nullable()) {
                anyWidened = true;
            }
            widenedFields.add(new StructField(leftField.name(), widenedType, widenedNullable));
        }

        return anyWidened ? new StructType(widenedFields) : leftSchema;
    }

    @Override
    public String toString() {
        return String.format("Union(all=%s)", all);
    }
}
