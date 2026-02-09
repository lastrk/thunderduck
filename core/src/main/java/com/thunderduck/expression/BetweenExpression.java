package com.thunderduck.expression;

import com.thunderduck.types.BooleanType;
import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression representing a BETWEEN predicate.
 *
 * <p>Examples:
 * <pre>
 *   price BETWEEN 10 AND 100
 *   date NOT BETWEEN '2024-01-01' AND '2024-12-31'
 * </pre>
 */
public final class BetweenExpression implements Expression {

    private final Expression value;
    private final Expression lower;
    private final Expression upper;
    private final boolean negated;

    public BetweenExpression(Expression value, Expression lower, Expression upper, boolean negated) {
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.lower = Objects.requireNonNull(lower, "lower must not be null");
        this.upper = Objects.requireNonNull(upper, "upper must not be null");
        this.negated = negated;
    }

    public Expression value() {
        return value;
    }

    public Expression lower() {
        return lower;
    }

    public Expression upper() {
        return upper;
    }

    public boolean negated() {
        return negated;
    }

    @Override
    public DataType dataType() {
        return BooleanType.get();
    }

    @Override
    public boolean nullable() {
        return value.nullable() || lower.nullable() || upper.nullable();
    }

    @Override
    public String toSQL() {
        String not = negated ? " NOT" : "";
        return "(%s%s BETWEEN %s AND %s)".formatted(
            value.toSQL(), not, lower.toSQL(), upper.toSQL());
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BetweenExpression that)) return false;
        return negated == that.negated &&
               Objects.equals(value, that.value) &&
               Objects.equals(lower, that.lower) &&
               Objects.equals(upper, that.upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, lower, upper, negated);
    }
}
