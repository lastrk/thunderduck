package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a join operation.
 *
 * <p>This node joins two relations based on a join condition and join type.
 *
 * <p>Examples:
 * <pre>
 *   df1.join(df2, "id")                          // Inner join on id
 *   df1.join(df2, col("df1.id") == col("df2.id")) // Inner join with expression
 *   df1.join(df2, "id", "left")                   // Left outer join
 * </pre>
 *
 * <p>Supported join types:
 * <ul>
 *   <li>INNER - Standard inner join</li>
 *   <li>LEFT - Left outer join</li>
 *   <li>RIGHT - Right outer join</li>
 *   <li>FULL - Full outer join</li>
 *   <li>CROSS - Cartesian product (no condition)</li>
 *   <li>LEFT_SEMI - Left semi join (returns left rows with matches)</li>
 *   <li>LEFT_ANTI - Left anti join (returns left rows without matches)</li>
 * </ul>
 */
public class Join extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final JoinType joinType;
    private final Expression condition;

    /**
     * Creates a join node.
     *
     * @param left the left relation
     * @param right the right relation
     * @param joinType the join type
     * @param condition the join condition (may be null for CROSS join)
     */
    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, Expression condition) {
        super(Arrays.asList(left, right));
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.joinType = Objects.requireNonNull(joinType, "joinType must not be null");
        this.condition = condition;

        if (joinType != JoinType.CROSS && condition == null) {
            throw new IllegalArgumentException("condition is required for non-CROSS joins");
        }
        if (joinType == JoinType.CROSS && condition != null) {
            throw new IllegalArgumentException("condition must be null for CROSS join");
        }
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
     * Returns the join type.
     *
     * @return the join type
     */
    public JoinType joinType() {
        return joinType;
    }

    /**
     * Returns the join condition.
     *
     * @return the condition, or null for CROSS join
     */
    public Expression condition() {
        return condition;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        StringBuilder sql = new StringBuilder();

        // Generate left side
        sql.append("SELECT * FROM ");
        sql.append(generateJoinSide(left, generator));

        // Generate JOIN keyword with type
        sql.append(" ");
        sql.append(getJoinKeyword());
        sql.append(" ");

        // Generate right side
        sql.append(generateJoinSide(right, generator));

        // Generate ON clause (if condition exists)
        if (condition != null) {
            sql.append(" ON ");
            sql.append(condition.toSQL());
        }

        return sql.toString();
    }

    /**
     * Generates SQL for a join side, preserving user-provided aliases.
     *
     * <p>If the plan is an AliasedRelation, use the user's alias directly so that
     * join conditions like col("d1.column") can reference it. Otherwise, wrap
     * in a subquery with a generated alias.
     *
     * @param plan the join side plan
     * @param generator the SQL generator
     * @return the SQL for this join side
     */
    private String generateJoinSide(LogicalPlan plan, SQLGenerator generator) {
        if (plan instanceof AliasedRelation) {
            // User provided an explicit alias - use it directly so join conditions can reference it
            AliasedRelation aliased = (AliasedRelation) plan;
            String childSql = generator.generate(aliased.child());
            return String.format("(%s) AS %s",
                childSql, com.thunderduck.generator.SQLQuoting.quoteIdentifier(aliased.alias()));
        } else {
            // No explicit alias - wrap in subquery with generated alias
            return String.format("(%s) AS %s",
                generator.generate(plan), generator.generateSubqueryAlias());
        }
    }

    /**
     * Returns the SQL keyword for this join type.
     *
     * @return the JOIN keyword
     */
    private String getJoinKeyword() {
        switch (joinType) {
            case INNER:
                return "INNER JOIN";
            case LEFT:
                return "LEFT OUTER JOIN";
            case RIGHT:
                return "RIGHT OUTER JOIN";
            case FULL:
                return "FULL OUTER JOIN";
            case CROSS:
                return "CROSS JOIN";
            case LEFT_SEMI:
                // DuckDB supports LEFT SEMI JOIN
                return "LEFT SEMI JOIN";
            case LEFT_ANTI:
                // DuckDB supports LEFT ANTI JOIN
                return "LEFT ANTI JOIN";
            default:
                throw new IllegalStateException("Unsupported join type: " + joinType);
        }
    }

    @Override
    public StructType inferSchema() {
        // Get child schemas - if either is null, return null to trigger
        // DuckDB-based schema inference fallback
        StructType leftSchema = left.schema();
        StructType rightSchema = right.schema();

        // For semi/anti joins, only include left schema
        if (joinType == JoinType.LEFT_SEMI || joinType == JoinType.LEFT_ANTI) {
            return leftSchema;
        }

        // If either schema is null, we can't compute join schema
        if (leftSchema == null || rightSchema == null) {
            return null;
        }

        // Include all fields from both sides
        List<StructField> fields = new ArrayList<>();
        fields.addAll(leftSchema.fields());
        fields.addAll(rightSchema.fields());

        return new StructType(fields);
    }

    @Override
    public String toString() {
        if (condition != null) {
            return String.format("Join(%s, condition=%s)", joinType, condition);
        } else {
            return String.format("Join(%s)", joinType);
        }
    }

    /**
     * Supported join types.
     */
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS,
        LEFT_SEMI,
        LEFT_ANTI
    }
}
