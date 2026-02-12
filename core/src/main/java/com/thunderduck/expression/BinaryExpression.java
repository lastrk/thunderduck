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
            return generateStrictModeDivision();
        }
        // Spark uses + for string concatenation; DuckDB requires ||
        if (operator == Operator.ADD && isStringConcatenation()) {
            return String.format("(%s || %s)", left.toSQL(), right.toSQL());
        }
        return String.format("(%s %s %s)", left.toSQL(), operator.symbol(), right.toSQL());
    }

    /**
     * Generates type-aware division SQL for strict mode.
     *
     * <p>Spark's division behavior by type:
     * <ul>
     *   <li>DECIMAL / DECIMAL -> spark_decimal_div (extension function)</li>
     *   <li>integer / integer -> DOUBLE (cast both to DOUBLE first)</li>
     *   <li>DECIMAL / integer -> spark_decimal_div with integer cast to DECIMAL</li>
     *   <li>integer / DECIMAL -> spark_decimal_div with integer cast to DECIMAL</li>
     *   <li>Any FLOAT/DOUBLE involved -> native division (returns DOUBLE)</li>
     *   <li>Unknown/unresolved types -> native division (safe fallback)</li>
     * </ul>
     */
    private String generateStrictModeDivision() {
        return generateStrictDivisionSQL(left.toSQL(), right.toSQL(),
                left.dataType(), right.dataType());
    }

    /**
     * Generates type-aware division SQL for strict Spark compatibility mode.
     * Shared by both the expression tree path ({@link #generateStrictModeDivision()})
     * and the {@code SQLGenerator.qualifyCondition()} path.
     *
     * @param leftSQL  the SQL text for the left operand
     * @param rightSQL the SQL text for the right operand
     * @param leftType the data type of the left operand (may be null)
     * @param rightType the data type of the right operand (may be null)
     * @return the division SQL using Spark semantics
     */
    public static String generateStrictDivisionSQL(String leftSQL, String rightSQL,
                                                    DataType leftType, DataType rightType) {
        // If either type is unknown, fall back to native division
        if (leftType == null || rightType == null) {
            return String.format("(%s / %s)", leftSQL, rightSQL);
        }

        boolean leftDecimal = leftType instanceof DecimalType;
        boolean rightDecimal = rightType instanceof DecimalType;
        boolean leftIntegral = isIntegralType(leftType);
        boolean rightIntegral = isIntegralType(rightType);
        boolean leftFloating = isFloatingType(leftType);
        boolean rightFloating = isFloatingType(rightType);

        // Any FLOAT/DOUBLE involved -> native division (Spark returns DOUBLE)
        if (leftFloating || rightFloating) {
            return String.format("(%s / %s)", leftSQL, rightSQL);
        }

        // Both DECIMAL -> spark_decimal_div
        if (leftDecimal && rightDecimal) {
            return String.format("spark_decimal_div(%s, %s)", leftSQL, rightSQL);
        }

        // Both integral -> Spark returns DOUBLE for int/int division
        if (leftIntegral && rightIntegral) {
            return String.format("(CAST(%s AS DOUBLE) / CAST(%s AS DOUBLE))",
                    leftSQL, rightSQL);
        }

        // DECIMAL / integer -> cast integer to DECIMAL, then spark_decimal_div
        if (leftDecimal && rightIntegral) {
            int p = integralPrecision(rightType);
            return String.format("spark_decimal_div(%s, CAST(%s AS DECIMAL(%d,0)))",
                    leftSQL, rightSQL, p);
        }

        // integer / DECIMAL -> cast integer to DECIMAL, then spark_decimal_div
        if (leftIntegral && rightDecimal) {
            int p = integralPrecision(leftType);
            return String.format("spark_decimal_div(CAST(%s AS DECIMAL(%d,0)), %s)",
                    leftSQL, p, rightSQL);
        }

        // Fallback for any other type combination (e.g., string, date, unknown)
        return String.format("(%s / %s)", leftSQL, rightSQL);
    }

    /**
     * Returns true if the type is an integral (byte, short, int, long) type.
     */
    private static boolean isIntegralType(DataType type) {
        return type instanceof ByteType || type instanceof ShortType ||
               type instanceof IntegerType || type instanceof LongType;
    }

    /**
     * Returns true if the type is a floating-point (float, double) type.
     */
    private static boolean isFloatingType(DataType type) {
        return type instanceof FloatType || type instanceof DoubleType;
    }

    /**
     * Returns the appropriate DECIMAL precision for casting an integral type.
     * Maps to the maximum number of digits the type can represent:
     * - ByteType (TINYINT): 3 digits
     * - ShortType (SMALLINT): 5 digits
     * - IntegerType (INT): 10 digits
     * - LongType (BIGINT): 20 digits (19 digits + sign coverage)
     */
    private static int integralPrecision(DataType type) {
        if (type instanceof ByteType) return 3;
        if (type instanceof ShortType) return 5;
        if (type instanceof IntegerType) return 10;
        if (type instanceof LongType) return 20;
        return 10; // safe default
    }

    /**
     * Detects if this ADD expression is actually string concatenation.
     * Returns true if either operand is a string literal or a string function
     * (coalesce/concat wrapping string args).
     */
    private boolean isStringConcatenation() {
        return isStringExpression(left) || isStringExpression(right);
    }

    private static boolean isStringExpression(Expression expr) {
        if (expr instanceof Literal lit) {
            return lit.dataType() instanceof com.thunderduck.types.StringType;
        }
        if (expr instanceof FunctionCall func) {
            String name = func.functionName().toLowerCase();
            return name.equals("coalesce") || name.equals("concat");
        }
        // Recurse into nested ADD (e.g., 'a' + col + 'b')
        if (expr instanceof BinaryExpression bin && bin.operator == Operator.ADD) {
            return isStringExpression(bin.left) || isStringExpression(bin.right);
        }
        return false;
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
