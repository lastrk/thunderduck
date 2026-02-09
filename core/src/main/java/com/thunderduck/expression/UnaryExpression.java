package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression representing a unary operation (operation with one operand).
 *
 * <p>Unary expressions include:
 * <ul>
 *   <li>Arithmetic negation: -a</li>
 *   <li>Logical negation: NOT a</li>
 *   <li>IS NULL: a IS NULL</li>
 *   <li>IS NOT NULL: a IS NOT NULL</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   -price                     -- arithmetic negation
 *   NOT active                 -- logical negation
 *   email IS NULL              -- null check
 *   email IS NOT NULL          -- not null check
 * </pre>
 */
public final class UnaryExpression implements Expression {

    /**
     * Unary operators.
     */
    public enum Operator {
        NEGATE("-", "negation"),
        NOT("NOT", "logical NOT"),
        IS_NULL("IS NULL", "null check"),
        IS_NOT_NULL("IS NOT NULL", "not null check");

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

        public boolean isPrefix() {
            return this == NEGATE || this == NOT;
        }

        public boolean isPostfix() {
            return this == IS_NULL || this == IS_NOT_NULL;
        }
    }

    private final Operator operator;
    private final Expression operand;

    /**
     * Creates a unary expression.
     *
     * @param operator the operator
     * @param operand the operand
     */
    public UnaryExpression(Operator operator, Expression operand) {
        this.operator = Objects.requireNonNull(operator, "operator must not be null");
        this.operand = Objects.requireNonNull(operand, "operand must not be null");
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
     * Returns the operand.
     *
     * @return the operand expression
     */
    public Expression operand() {
        return operand;
    }

    @Override
    public DataType dataType() {
        // NOT, IS NULL, IS NOT NULL return boolean
        if (operator == Operator.NOT || operator == Operator.IS_NULL ||
            operator == Operator.IS_NOT_NULL) {
            return com.thunderduck.types.BooleanType.get();
        }

        // NEGATE returns the same type as the operand
        return operand.dataType();
    }

    @Override
    public boolean nullable() {
        // IS NULL and IS NOT NULL always return non-null boolean
        if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
            return false;
        }

        // Other operators preserve nullability
        return operand.nullable();
    }

    /**
     * Converts this unary expression to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (operator.isPrefix()) {
            // For NEGATE, no space between operator and operand
            if (operator == Operator.NEGATE) {
                return String.format("(%s%s)", operator.symbol(), operand.toSQL());
            }
            // For NOT, include space
            return String.format("(%s %s)", operator.symbol(), operand.toSQL());
        } else {
            return String.format("(%s %s)", operand.toSQL(), operator.symbol());
        }
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UnaryExpression)) return false;
        UnaryExpression that = (UnaryExpression) obj;
        return operator == that.operator &&
               Objects.equals(operand, that.operand);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, operand);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a negation expression.
     *
     * @param operand the operand
     * @return the unary expression
     */
    public static UnaryExpression negate(Expression operand) {
        return new UnaryExpression(Operator.NEGATE, operand);
    }

    /**
     * Creates a logical NOT expression.
     *
     * @param operand the operand
     * @return the unary expression
     */
    public static UnaryExpression not(Expression operand) {
        return new UnaryExpression(Operator.NOT, operand);
    }

    /**
     * Creates an IS NULL expression.
     *
     * @param operand the operand
     * @return the unary expression
     */
    public static UnaryExpression isNull(Expression operand) {
        return new UnaryExpression(Operator.IS_NULL, operand);
    }

    /**
     * Creates an IS NOT NULL expression.
     *
     * @param operand the operand
     * @return the unary expression
     */
    public static UnaryExpression isNotNull(Expression operand) {
        return new UnaryExpression(Operator.IS_NOT_NULL, operand);
    }
}
