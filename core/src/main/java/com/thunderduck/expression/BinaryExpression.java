package com.thunderduck.expression;

import com.thunderduck.runtime.SparkCompatMode;
import com.thunderduck.types.*;
import java.util.Objects;

/**
 * Expression representing a binary operation (operation with two operands).
 *
 * <p>Binary expressions include:
 * <ul>
 *   <li>Arithmetic: a + b, a - b, a * b, a / b, a % b</li>
 *   <li>Comparison: a > b, a >= b, a < b, a <= b, a == b, a != b</li>
 *   <li>Logical: a AND b, a OR b</li>
 *   <li>String: a || b (concatenation)</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   price * quantity           -- arithmetic
 *   age > 25                   -- comparison
 *   active AND verified        -- logical
 *   first_name || last_name    -- string concatenation
 * </pre>
 */
public final class BinaryExpression implements Expression {

    /**
     * Binary operators.
     */
    public enum Operator {
        // Arithmetic operators
        ADD("+", "addition"),
        SUBTRACT("-", "subtraction"),
        MULTIPLY("*", "multiplication"),
        DIVIDE("/", "division"),
        MODULO("%", "modulo"),

        // Comparison operators
        EQUAL("=", "equal"),
        NOT_EQUAL("!=", "not equal"),
        LESS_THAN("<", "less than"),
        LESS_THAN_OR_EQUAL("<=", "less than or equal"),
        GREATER_THAN(">", "greater than"),
        GREATER_THAN_OR_EQUAL(">=", "greater than or equal"),

        // Logical operators
        AND("AND", "logical AND"),
        OR("OR", "logical OR"),

        // String operators
        CONCAT("||", "concatenation");

        private final String symbol;
        private final String description;

        Operator(String symbol, String description) {
            this.symbol = symbol;
            this.description = description;
        }

        public String symbol() {
            return symbol;
        }

        public String description() {
            return description;
        }

        public boolean isArithmetic() {
            return this == ADD || this == SUBTRACT || this == MULTIPLY ||
                   this == DIVIDE || this == MODULO;
        }

        public boolean isComparison() {
            return this == EQUAL || this == NOT_EQUAL || this == LESS_THAN ||
                   this == LESS_THAN_OR_EQUAL || this == GREATER_THAN ||
                   this == GREATER_THAN_OR_EQUAL;
        }

        public boolean isLogical() {
            return this == AND || this == OR;
        }
    }

    private final Expression left;
    private final Operator operator;
    private final Expression right;

    /**
     * Creates a binary expression.
     *
     * @param left the left operand
     * @param operator the operator
     * @param right the right operand
     */
    public BinaryExpression(Expression left, Operator operator, Expression right) {
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
    }

    /**
     * Returns the left operand.
     *
     * @return the left expression
     */
    public Expression left() {
        return left;
    }

    /**
     * Returns the operator.
     *
     * @return the operator
     */
    public Operator operator() {
        return operator;
    }

    /**
     * Returns the right operand.
     *
     * @return the right expression
     */
    public Expression right() {
        return right;
    }

    @Override
    public DataType dataType() {
        // Comparison and logical operators return boolean
        if (operator.isComparison() || operator.isLogical()) {
            return BooleanType.get();
        }

        // Arithmetic and string operators return the type of the operands
        // (In a real implementation, we'd do proper type resolution)
        return left.dataType();
    }

    @Override
    public boolean nullable() {
        // Result is nullable if either operand is nullable
        return left.nullable() || right.nullable();
    }

    /**
     * Converts this binary expression to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (operator == Operator.DIVIDE && SparkCompatMode.isStrictMode()) {
            return String.format("spark_decimal_div(%s, %s)", left.toSQL(), right.toSQL());
        }
        return String.format("(%s %s %s)", left.toSQL(), operator.symbol(), right.toSQL());
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BinaryExpression)) return false;
        BinaryExpression that = (BinaryExpression) obj;
        return Objects.equals(left, that.left) &&
               operator == that.operator &&
               Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, operator, right);
    }

    // ==================== Factory Methods ====================

    public static BinaryExpression add(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.ADD, right);
    }

    public static BinaryExpression subtract(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.SUBTRACT, right);
    }

    public static BinaryExpression multiply(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.MULTIPLY, right);
    }

    public static BinaryExpression divide(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.DIVIDE, right);
    }

    public static BinaryExpression modulo(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.MODULO, right);
    }

    public static BinaryExpression equal(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.EQUAL, right);
    }

    public static BinaryExpression notEqual(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.NOT_EQUAL, right);
    }

    public static BinaryExpression lessThan(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.LESS_THAN, right);
    }

    public static BinaryExpression greaterThan(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.GREATER_THAN, right);
    }

    public static BinaryExpression and(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.AND, right);
    }

    public static BinaryExpression or(Expression left, Expression right) {
        return new BinaryExpression(left, Operator.OR, right);
    }
}
