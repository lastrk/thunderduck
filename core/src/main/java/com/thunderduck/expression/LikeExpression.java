package com.thunderduck.expression;

import com.thunderduck.types.BooleanType;
import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression representing a LIKE or ILIKE predicate.
 *
 * <p>Examples:
 * <pre>
 *   name LIKE '%smith'
 *   name NOT LIKE 'A%'
 *   name ILIKE '%SMITH%'
 * </pre>
 */
public final class LikeExpression implements Expression {

    private final Expression value;
    private final Expression pattern;
    private final boolean negated;
    private final boolean caseInsensitive;

    public LikeExpression(Expression value, Expression pattern,
                          boolean negated, boolean caseInsensitive) {
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.pattern = Objects.requireNonNull(pattern, "pattern must not be null");
        this.negated = negated;
        this.caseInsensitive = caseInsensitive;
    }

    public Expression value() {
        return value;
    }

    public Expression pattern() {
        return pattern;
    }

    public boolean negated() {
        return negated;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public DataType dataType() {
        return BooleanType.get();
    }

    @Override
    public boolean nullable() {
        return value.nullable() || pattern.nullable();
    }

    @Override
    public String toSQL() {
        String not = negated ? "NOT " : "";
        String op = caseInsensitive ? "ILIKE" : "LIKE";
        return "(%s %s%s %s)".formatted(value.toSQL(), not, op, pattern.toSQL());
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LikeExpression that)) return false;
        return negated == that.negated &&
               caseInsensitive == that.caseInsensitive &&
               Objects.equals(value, that.value) &&
               Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, pattern, negated, caseInsensitive);
    }
}
