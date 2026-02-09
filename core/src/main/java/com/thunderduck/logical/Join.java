package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
public final class Join extends LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final JoinType joinType;
    private final Expression condition;
    private final List<String> usingColumns;

    /**
     * Creates a join node.
     *
     * @param left the left relation
     * @param right the right relation
     * @param joinType the join type
     * @param condition the join condition (may be null for CROSS join)
     */
    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, Expression condition) {
        this(left, right, joinType, condition, Collections.emptyList());
    }

    /**
     * Creates a join node with USING columns.
     *
     * <p>USING columns are used for column deduplication in the output schema.
     * When joining with USING, the join column appears only once in the result
     * (from the left side), unlike ON joins where both columns appear.
     *
     * @param left the left relation
     * @param right the right relation
     * @param joinType the join type
     * @param condition the join condition (may be null for CROSS join)
     * @param usingColumns list of column names used in USING clause (for deduplication)
     */
    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, Expression condition,
                List<String> usingColumns) {
        super(Arrays.asList(left, right));
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
        this.joinType = Objects.requireNonNull(joinType, "joinType must not be null");
        this.condition = condition;
        this.usingColumns = usingColumns != null ? usingColumns : Collections.emptyList();

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

    /**
     * Returns the USING columns for this join.
     *
     * <p>USING columns are used for column deduplication - when present,
     * these columns appear only once in the output (from the left side).
     *
     * @return list of USING column names, empty if not a USING join
     */
    public List<String> usingColumns() {
        return usingColumns;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        StringBuilder sql = new StringBuilder();

        // Determine actual aliases to use (respect user-provided aliases)
        String leftAlias = getEffectiveAlias(left, generator);
        String rightAlias = getEffectiveAlias(right, generator);

        // For USING joins, generate explicit SELECT list to deduplicate columns
        if (!usingColumns.isEmpty()) {
            sql.append("SELECT ");
            sql.append(generateUsingSelectList(leftAlias, rightAlias));
            sql.append(" FROM ");
        } else {
            sql.append("SELECT * FROM ");
        }

        sql.append(generateJoinSideWithAlias(left, generator, leftAlias));

        // Generate JOIN keyword with type
        sql.append(" ");
        sql.append(getJoinKeyword());
        sql.append(" ");

        // Generate right side
        sql.append(generateJoinSideWithAlias(right, generator, rightAlias));

        // Generate ON clause (if condition exists)
        if (condition != null) {
            sql.append(" ON ");
            sql.append(condition.toSQL());
        }

        return sql.toString();
    }

    /**
     * Gets the effective alias for a join side.
     *
     * <p>If the plan is an AliasedRelation, returns the user's alias.
     * Otherwise, generates a new alias.
     *
     * @param plan the join side plan
     * @param generator the SQL generator
     * @return the alias to use
     */
    private String getEffectiveAlias(LogicalPlan plan, SQLGenerator generator) {
        if (plan instanceof AliasedRelation) {
            return com.thunderduck.generator.SQLQuoting.quoteIdentifier(
                ((AliasedRelation) plan).alias());
        }
        return generator.generateSubqueryAlias();
    }

    /**
     * Generates explicit SELECT list for USING joins.
     *
     * <p>For USING joins, we need to:
     * 1. Select USING columns with COALESCE for outer joins (to handle NULLs)
     * 2. Select all non-USING columns from left side
     * 3. Select all non-USING columns from right side
     *
     * <p>For INNER/LEFT joins, left side is sufficient for USING columns.
     * For RIGHT/FULL outer joins, we need COALESCE(left.col, right.col) to handle
     * cases where left side is NULL but right side has a value.
     *
     * @param leftAlias alias for left table
     * @param rightAlias alias for right table
     * @return comma-separated SELECT list
     */
    private String generateUsingSelectList(String leftAlias, String rightAlias) {
        List<String> selectItems = new ArrayList<>();
        Set<String> usingSet = new HashSet<>(usingColumns);

        StructType leftSchema = left.schema();
        StructType rightSchema = right.schema();

        // If schemas aren't available, fall back to SELECT *
        if (leftSchema == null || rightSchema == null) {
            return "*";
        }

        // Determine if we need COALESCE for USING columns (RIGHT/FULL outer joins)
        boolean needsCoalesce = (joinType == JoinType.RIGHT || joinType == JoinType.FULL);

        // Add columns from left side
        for (StructField field : leftSchema.fields()) {
            String quotedName = com.thunderduck.generator.SQLQuoting.quoteIdentifier(field.name());
            if (usingSet.contains(field.name()) && needsCoalesce) {
                // For RIGHT/FULL joins, use COALESCE to get value from whichever side has it
                selectItems.add(String.format("COALESCE(%s.%s, %s.%s) AS %s",
                    leftAlias, quotedName, rightAlias, quotedName, quotedName));
            } else {
                selectItems.add(leftAlias + "." + quotedName);
            }
        }

        // Add non-USING columns from right side
        for (StructField field : rightSchema.fields()) {
            if (!usingSet.contains(field.name())) {
                selectItems.add(rightAlias + "." +
                    com.thunderduck.generator.SQLQuoting.quoteIdentifier(field.name()));
            }
        }

        return String.join(", ", selectItems);
    }

    /**
     * Generates SQL for a join side with a specific alias.
     *
     * <p>If the plan is an AliasedRelation, use the user's alias directly so that
     * join conditions like col("d1.column") can reference it. Otherwise, wrap
     * in a subquery with the provided alias.
     *
     * @param plan the join side plan
     * @param generator the SQL generator
     * @param alias the alias to use if no user-provided alias exists
     * @return the SQL for this join side
     */
    private String generateJoinSideWithAlias(LogicalPlan plan, SQLGenerator generator, String alias) {
        if (plan instanceof AliasedRelation) {
            // User provided an explicit alias - use it directly so join conditions can reference it
            AliasedRelation aliased = (AliasedRelation) plan;
            String childSql = generator.generate(aliased.child());
            return String.format("(%s) AS %s",
                childSql, com.thunderduck.generator.SQLQuoting.quoteIdentifier(aliased.alias()));
        } else {
            // No explicit alias - wrap in subquery with provided alias
            return String.format("(%s) AS %s", generator.generate(plan), alias);
        }
    }

    /**
     * Returns the SQL keyword for this join type.
     *
     * @return the JOIN keyword
     */
    private String getJoinKeyword() {
        return switch (joinType) {
            case INNER     -> "INNER JOIN";
            case LEFT      -> "LEFT OUTER JOIN";
            case RIGHT     -> "RIGHT OUTER JOIN";
            case FULL      -> "FULL OUTER JOIN";
            case CROSS     -> "CROSS JOIN";
            case LEFT_SEMI -> "SEMI JOIN";
            case LEFT_ANTI -> "ANTI JOIN";
        };
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

        List<StructField> fields = new ArrayList<>();
        Set<String> usingSet = new HashSet<>(usingColumns);

        // For USING joins, Spark puts USING columns first
        // Column order: USING columns, then non-USING left, then non-USING right
        if (!usingColumns.isEmpty()) {
            // Build a map of left fields by name for quick lookup
            java.util.Map<String, StructField> leftFieldMap = new java.util.HashMap<>();
            for (StructField field : leftSchema.fields()) {
                leftFieldMap.put(field.name(), field);
            }

            // 1. Add USING columns first (from left side, in USING order)
            for (String usingCol : usingColumns) {
                StructField field = leftFieldMap.get(usingCol);
                if (field != null) {
                    fields.add(field);
                }
            }

            // 2. Add non-USING columns from left side
            for (StructField field : leftSchema.fields()) {
                if (!usingSet.contains(field.name())) {
                    fields.add(field);
                }
            }

            // 3. Add non-USING columns from right side
            for (StructField field : rightSchema.fields()) {
                if (!usingSet.contains(field.name())) {
                    fields.add(field);
                }
            }
        } else {
            // Non-USING join: all left columns + all right columns
            fields.addAll(leftSchema.fields());
            fields.addAll(rightSchema.fields());
        }

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
