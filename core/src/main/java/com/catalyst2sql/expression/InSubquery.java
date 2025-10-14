package com.catalyst2sql.expression;

import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.types.BooleanType;
import com.catalyst2sql.types.DataType;
import java.util.Objects;

/**
 * IN subquery expression that tests membership.
 *
 * <p>Tests whether a value (or tuple of values) appears in the result set
 * of a subquery.
 *
 * <p>Examples:
 * <pre>
 *   WHERE category_id IN (SELECT id FROM categories WHERE active = true)
 *   WHERE (customer_id, product_id) IN (SELECT customer_id, product_id FROM recent_orders)
 *   WHERE status NOT IN (SELECT name FROM invalid_statuses)
 * </pre>
 *
 * <p>Note: NULL handling in IN subqueries follows SQL semantics:
 * <ul>
 *   <li>If the value is NULL, result is NULL</li>
 *   <li>If subquery contains NULL and value not found, result is NULL (not false)</li>
 * </ul>
 */
public class InSubquery extends SubqueryExpression {

    private final Expression testExpression;
    private final boolean isNegated;

    /**
     * Creates an IN subquery.
     *
     * @param testExpression the expression to test (left side of IN)
     * @param subquery the subquery providing values to test against
     * @param isNegated true for NOT IN, false for IN
     */
    public InSubquery(Expression testExpression, LogicalPlan subquery, boolean isNegated) {
        super(subquery);
        this.testExpression = Objects.requireNonNull(testExpression, "testExpression must not be null");
        this.isNegated = isNegated;
    }

    /**
     * Creates an IN subquery (not negated).
     *
     * @param testExpression the expression to test
     * @param subquery the subquery
     */
    public InSubquery(Expression testExpression, LogicalPlan subquery) {
        this(testExpression, subquery, false);
    }

    /**
     * Returns the test expression (left side of IN).
     *
     * @return the test expression
     */
    public Expression testExpression() {
        return testExpression;
    }

    /**
     * Returns whether this is NOT IN (true) or IN (false).
     *
     * @return true for NOT IN, false for IN
     */
    public boolean isNegated() {
        return isNegated;
    }

    @Override
    public DataType dataType() {
        // IN subquery always returns boolean
        return BooleanType.get();
    }

    @Override
    public String toSQL() {
        // SQL generation will be implemented in Phase 3
        throw new UnsupportedOperationException(
            "IN subquery SQL generation will be implemented in Week 3 Phase 3");
    }

    @Override
    public String toString() {
        String operator = isNegated ? "NOT IN" : "IN";
        return String.format("%s %s (%s)", testExpression, operator, subquery);
    }
}
