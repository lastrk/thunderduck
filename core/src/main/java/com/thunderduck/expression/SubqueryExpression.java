package com.thunderduck.expression;

import com.thunderduck.logical.LogicalPlan;
import java.util.Objects;

/**
 * Base class for subquery expressions.
 *
 * <p>Subqueries are nested queries that can appear in various positions:
 * <ul>
 *   <li>Scalar subquery - Returns single value, used in SELECT or WHERE</li>
 *   <li>IN subquery - Tests membership in subquery results</li>
 *   <li>EXISTS subquery - Tests whether subquery returns any rows</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   SELECT (SELECT MAX(price) FROM products) as max_price
 *   WHERE category_id IN (SELECT id FROM categories WHERE active = true)
 *   WHERE EXISTS (SELECT 1 FROM orders WHERE orders.product_id = products.id)
 * </pre>
 */
public abstract class SubqueryExpression implements Expression {

    protected final LogicalPlan subquery;

    /**
     * Creates a subquery expression.
     *
     * @param subquery the nested logical plan
     */
    public SubqueryExpression(LogicalPlan subquery) {
        this.subquery = Objects.requireNonNull(subquery, "subquery must not be null");
    }

    /**
     * Returns the subquery plan.
     *
     * @return the subquery
     */
    public LogicalPlan subquery() {
        return subquery;
    }

    @Override
    public boolean nullable() {
        // Subqueries can generally produce nulls
        return true;
    }
}
