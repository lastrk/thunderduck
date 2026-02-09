package com.thunderduck.expression;

import com.thunderduck.types.DataType;

/**
 * Base interface for all expressions in the thunderduck translation layer.
 *
 * <p>Expressions represent computations that produce values, such as:
 * <ul>
 *   <li>Literals (constants)</li>
 *   <li>Column references</li>
 *   <li>Arithmetic operations (a + b, a * b)</li>
 *   <li>Comparison operations (a > b, a == b)</li>
 *   <li>Function calls (upper(name), abs(value))</li>
 * </ul>
 *
 * <p>Expressions are used in:
 * <ul>
 *   <li>SELECT clause (projections)</li>
 *   <li>WHERE clause (filters)</li>
 *   <li>GROUP BY clause</li>
 *   <li>ORDER BY clause</li>
 * </ul>
 *
 * <p>All concrete implementations in the expression package are {@code final},
 * preventing further subclassing. The {@link com.thunderduck.logical.Aggregate.AggregateExpression}
 * in the logical package also implements this interface.
 */
public interface Expression {

    /**
     * Returns the data type of the value produced by this expression.
     *
     * @return the data type
     */
    DataType dataType();

    /**
     * Returns whether this expression can produce null values.
     *
     * @return true if nullable, false otherwise
     */
    boolean nullable();

    /**
     * Converts this expression to its SQL string representation.
     *
     * <p>This method generates the SQL text for this expression that can be
     * used in SELECT, WHERE, GROUP BY, ORDER BY, and other SQL clauses.
     *
     * @return the SQL string representation
     */
    String toSQL();
}
