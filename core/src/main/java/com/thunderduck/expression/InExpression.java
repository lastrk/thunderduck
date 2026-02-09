package com.thunderduck.expression;

import com.thunderduck.types.BooleanType;
import com.thunderduck.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Expression representing an IN clause (or NOT IN clause).
 *
 * <p>IN expressions test whether a value is within a set of values. This class
 * preserves the test expression and value list to enable proper nullability
 * analysis and type checking.
 *
 * <p>SQL form: expr IN (val1, val2, val3)
 * <p>SQL form (negated): expr NOT IN (val1, val2, val3)
 * <p>Spark form: col("x").isin(val1, val2, val3)
 *
 * <p>The result is always a boolean type.
 *
 * <p>Nullability follows SQL semantics:
 * <ul>
 *   <li>If testExpr is NULL: result is NULL</li>
 *   <li>If any value in the list is NULL and no exact match: result is NULL</li>
 *   <li>If an exact match is found: result is TRUE</li>
 *   <li>If no match and no NULLs: result is FALSE</li>
 * </ul>
 */
public final class InExpression implements Expression {

    private final Expression testExpr;
    private final List<Expression> values;
    private final boolean negated;

    /**
     * Creates an IN expression.
     *
     * @param testExpr the expression being tested
     * @param values the values to test against
     * @param negated true for NOT IN, false for IN
     * @throws IllegalArgumentException if values is empty
     */
    public InExpression(Expression testExpr, List<Expression> values, boolean negated) {
        Objects.requireNonNull(testExpr, "testExpr must not be null");
        Objects.requireNonNull(values, "values must not be null");

        if (values.isEmpty()) {
            throw new IllegalArgumentException("IN clause requires at least one value");
        }

        this.testExpr = testExpr;
        this.values = new ArrayList<>(values);
        this.negated = negated;
    }

    /**
     * Creates an IN expression (not negated).
     *
     * @param testExpr the expression being tested
     * @param values the values to test against
     */
    public InExpression(Expression testExpr, List<Expression> values) {
        this(testExpr, values, false);
    }

    /**
     * Returns the expression being tested.
     *
     * @return the test expression
     */
    public Expression testExpr() {
        return testExpr;
    }

    /**
     * Returns the values in the IN list.
     *
     * @return an unmodifiable list of value expressions
     */
    public List<Expression> values() {
        return Collections.unmodifiableList(values);
    }

    /**
     * Returns whether this is a NOT IN expression.
     *
     * @return true for NOT IN, false for IN
     */
    public boolean isNegated() {
        return negated;
    }

    /**
     * Returns the data type of this IN expression.
     *
     * <p>IN expressions always return a boolean type.
     *
     * @return BooleanType
     */
    @Override
    public DataType dataType() {
        return BooleanType.get();
    }

    /**
     * Returns whether this expression can produce null values.
     *
     * <p>IN expression is nullable if:
     * <ul>
     *   <li>The test expression is nullable</li>
     *   <li>Any value in the IN list is nullable</li>
     * </ul>
     *
     * @return true if nullable
     */
    @Override
    public boolean nullable() {
        if (testExpr.nullable()) {
            return true;
        }
        return values.stream().anyMatch(Expression::nullable);
    }

    /**
     * Generates the SQL representation of this IN expression.
     *
     * @return SQL string in the form "expr IN (val1, val2, ...)" or "expr NOT IN (...)"
     */
    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append(testExpr.toSQL());

        if (negated) {
            sql.append(" NOT IN (");
        } else {
            sql.append(" IN (");
        }

        sql.append(values.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", ")));

        sql.append(")");
        return sql.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof InExpression)) return false;
        InExpression that = (InExpression) obj;
        return negated == that.negated &&
               Objects.equals(testExpr, that.testExpr) &&
               Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testExpr, values, negated);
    }

    @Override
    public String toString() {
        String op = negated ? "NOT IN" : "IN";
        return "InExpression(" + testExpr + " " + op + " " + values.size() + " values)";
    }
}
