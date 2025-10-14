package com.catalyst2sql.logical;

import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.types.StructType;
import java.util.Objects;

/**
 * Logical plan node representing a filter (WHERE clause).
 *
 * <p>This node filters rows from its child based on a boolean condition.
 *
 * <p>Examples:
 * <pre>
 *   df.filter("age > 25")
 *   df.where(col("price") > 100 && col("category") == "electronics")
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT * FROM (child) WHERE condition</pre>
 */
public class Filter extends LogicalPlan {

    private final Expression condition;

    /**
     * Creates a filter node.
     *
     * @param child the child node
     * @param condition the filter condition (must evaluate to boolean)
     */
    public Filter(LogicalPlan child, Expression condition) {
        super(child);
        this.condition = Objects.requireNonNull(condition, "condition must not be null");
    }

    /**
     * Returns the filter condition.
     *
     * @return the condition expression
     */
    public Expression condition() {
        return condition;
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        Objects.requireNonNull(generator, "generator must not be null");

        String childSQL = child().toSQL(generator);
        String conditionSQL = condition.toSQL();

        return String.format("SELECT * FROM (%s) AS subquery WHERE %s",
            childSQL, conditionSQL);
    }

    @Override
    public StructType inferSchema() {
        // Filter doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        return String.format("Filter(%s)", condition);
    }
}
