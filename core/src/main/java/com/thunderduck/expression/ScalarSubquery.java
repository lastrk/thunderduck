package com.thunderduck.expression;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.types.DataType;

/**
 * Scalar subquery expression that returns a single value.
 *
 * <p>A scalar subquery must return exactly one row and one column.
 * If the subquery returns zero rows, the result is NULL.
 * If it returns multiple rows, an error is raised at runtime.
 *
 * <p>Examples:
 * <pre>
 *   SELECT (SELECT MAX(price) FROM products) as max_price
 *   SELECT *, (SELECT AVG(salary) FROM employees) as avg_salary FROM departments
 *   WHERE amount > (SELECT AVG(amount) FROM transactions)
 * </pre>
 */
public final class ScalarSubquery extends SubqueryExpression {

    /**
     * Creates a scalar subquery.
     *
     * @param subquery the subquery that returns a single value
     */
    public ScalarSubquery(LogicalPlan subquery) {
        super(subquery);
    }

    @Override
    public DataType dataType() {
        // The type is the type of the single column returned by the subquery
        if (subquery.schema() != null && !subquery.schema().fields().isEmpty()) {
            return subquery.schema().fields().get(0).dataType();
        }
        return null;
    }

    @Override
    public String toSQL() {
        // Generate subquery SQL using a generator instance
        com.thunderduck.generator.SQLGenerator generator =
            new com.thunderduck.generator.SQLGenerator();
        return "(" + generator.generate(subquery) + ")";
    }

    @Override
    public String toString() {
        return String.format("ScalarSubquery(%s)", subquery);
    }
}
