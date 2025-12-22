package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.*;
import java.util.Objects;

/**
 * Expression representing a literal constant value.
 *
 * <p>Literals are fixed values that don't change, such as:
 * <ul>
 *   <li>Numeric literals: 42, 3.14, 100L</li>
 *   <li>String literals: "hello", 'world'</li>
 *   <li>Boolean literals: true, false</li>
 *   <li>Null literal: null</li>
 * </ul>
 *
 * <p>Examples in SQL:
 * <pre>
 *   SELECT 42              -- integer literal
 *   SELECT 'hello'         -- string literal
 *   SELECT 3.14            -- double literal
 *   SELECT true            -- boolean literal
 *   SELECT NULL            -- null literal
 * </pre>
 */
public class Literal extends Expression {

    private final Object value;
    private final DataType dataType;

    /**
     * Creates a literal expression.
     *
     * @param value the literal value (may be null)
     * @param dataType the data type of the literal
     */
    public Literal(Object value, DataType dataType) {
        this.value = value;
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
    }

    /**
     * Returns the literal value.
     *
     * @return the value, or null for NULL literals
     */
    public Object value() {
        return value;
    }

    /**
     * Returns whether this is a NULL literal.
     *
     * @return true if value is null, false otherwise
     */
    public boolean isNull() {
        return value == null;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return value == null;
    }

    /**
     * Converts this literal to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (value == null) {
            return "NULL";
        }

        if (dataType instanceof StringType) {
            // Escape single quotes in strings
            String str = value.toString().replace("'", "''");
            return "'" + str + "'";
        }

        if (dataType instanceof BooleanType) {
            return value.toString().toUpperCase();
        }

        if (dataType instanceof DateType) {
            // LocalDate.toString() returns ISO format like "2025-01-15"
            return "DATE '" + value + "'";
        }

        if (dataType instanceof TimestampType) {
            // Handle Instant objects by converting to LocalDateTime for proper formatting
            if (value instanceof java.time.Instant) {
                java.time.Instant instant = (java.time.Instant) value;
                java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(
                    instant, java.time.ZoneOffset.UTC);
                return "TIMESTAMP '" + ldt.toString().replace("T", " ") + "'";
            }
            return "TIMESTAMP '" + value + "'";
        }

        // Numeric and other types
        return value.toString();
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Literal)) return false;
        Literal that = (Literal) obj;
        return Objects.equals(value, that.value) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates an integer literal.
     *
     * @param value the integer value
     * @return the literal expression
     */
    public static Literal of(int value) {
        return new Literal(value, IntegerType.get());
    }

    /**
     * Creates a long literal.
     *
     * @param value the long value
     * @return the literal expression
     */
    public static Literal of(long value) {
        return new Literal(value, LongType.get());
    }

    /**
     * Creates a float literal.
     *
     * @param value the float value
     * @return the literal expression
     */
    public static Literal of(float value) {
        return new Literal(value, FloatType.get());
    }

    /**
     * Creates a double literal.
     *
     * @param value the double value
     * @return the literal expression
     */
    public static Literal of(double value) {
        return new Literal(value, DoubleType.get());
    }

    /**
     * Creates a string literal.
     *
     * @param value the string value
     * @return the literal expression
     */
    public static Literal of(String value) {
        return new Literal(value, StringType.get());
    }

    /**
     * Creates a boolean literal.
     *
     * @param value the boolean value
     * @return the literal expression
     */
    public static Literal of(boolean value) {
        return new Literal(value, BooleanType.get());
    }

    /**
     * Creates a NULL literal of the given type.
     *
     * @param dataType the data type
     * @return the NULL literal expression
     */
    public static Literal nullValue(DataType dataType) {
        return new Literal(null, dataType);
    }
}
