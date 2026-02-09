package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression that casts another expression to a different data type.
 *
 * <p>Examples:
 * <pre>
 *   CAST(amount AS DECIMAL(10,2))
 *   CAST(date_str AS DATE)
 *   CAST(123 AS VARCHAR)
 * </pre>
 */
public final class CastExpression implements Expression {

    private final Expression expression;
    private final DataType targetType;

    /**
     * Creates a cast expression.
     *
     * @param expression the expression to cast
     * @param targetType the target data type
     */
    public CastExpression(Expression expression, DataType targetType) {
        this.expression = Objects.requireNonNull(expression, "expression must not be null");
        this.targetType = Objects.requireNonNull(targetType, "targetType must not be null");
    }

    /**
     * Returns the expression being cast.
     *
     * @return the expression
     */
    public Expression expression() {
        return expression;
    }

    /**
     * Returns the target data type.
     *
     * @return the target type
     */
    public DataType targetType() {
        return targetType;
    }

    @Override
    public DataType dataType() {
        return targetType;
    }

    @Override
    public boolean nullable() {
        // Cast preserves nullability
        return expression.nullable();
    }

    /**
     * Converts this cast expression to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        return String.format("CAST(%s AS %s)", expression, targetType.typeName());
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CastExpression)) return false;
        CastExpression that = (CastExpression) obj;
        return Objects.equals(expression, that.expression) &&
               Objects.equals(targetType, that.targetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, targetType);
    }
}