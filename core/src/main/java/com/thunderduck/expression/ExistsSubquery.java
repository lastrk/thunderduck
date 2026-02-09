package com.thunderduck.expression;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.types.BooleanType;
import com.thunderduck.types.DataType;

/**
 * EXISTS subquery expression that tests for existence of rows.
 *
 * <p>Tests whether a subquery returns at least one row.
 * Returns true if subquery produces any rows, false if it produces zero rows.
 *
 * <p>Examples:
 * <pre>
 *   WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
 *   WHERE NOT EXISTS (SELECT 1 FROM refunds WHERE refunds.order_id = orders.id)
 *   WHERE EXISTS (SELECT 1 FROM products WHERE category = 'electronics' AND price < 100)
 * </pre>
 *
 * <p>Note: EXISTS ignores the actual values returned by the subquery,
 * only checking whether any rows are returned. It's common to use
 * {@code SELECT 1} as the subquery projection for clarity and potential
 * optimization benefits.
 */
public final class ExistsSubquery extends SubqueryExpression {

    private final boolean isNegated;

    /**
     * Creates an EXISTS subquery.
     *
     * @param subquery the subquery to test for existence
     * @param isNegated true for NOT EXISTS, false for EXISTS
     */
    public ExistsSubquery(LogicalPlan subquery, boolean isNegated) {
        super(subquery);
        this.isNegated = isNegated;
    }

    /**
     * Creates an EXISTS subquery (not negated).
     *
     * @param subquery the subquery to test
     */
    public ExistsSubquery(LogicalPlan subquery) {
        this(subquery, false);
    }

    /**
     * Returns whether this is NOT EXISTS (true) or EXISTS (false).
     *
     * @return true for NOT EXISTS, false for EXISTS
     */
    public boolean isNegated() {
        return isNegated;
    }

    @Override
    public DataType dataType() {
        // EXISTS always returns boolean
        return BooleanType.get();
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();

        // EXISTS or NOT EXISTS
        if (isNegated) {
            sql.append("NOT EXISTS ");
        } else {
            sql.append("EXISTS ");
        }

        // Subquery
        com.thunderduck.generator.SQLGenerator generator =
            new com.thunderduck.generator.SQLGenerator();
        sql.append("(");
        sql.append(generator.generate(subquery));
        sql.append(")");

        return sql.toString();
    }

    @Override
    public String toString() {
        String operator = isNegated ? "NOT EXISTS" : "EXISTS";
        return String.format("%s (%s)", operator, subquery);
    }
}
