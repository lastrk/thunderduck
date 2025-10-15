package com.catalyst2sql.differential.validation;

import org.apache.spark.sql.types.*;

import java.math.BigDecimal;

/**
 * Validates numerical values with epsilon-based comparison for floating point types.
 *
 * <p>Handles special cases:
 * <ul>
 *   <li>NaN values</li>
 *   <li>Infinity values</li>
 *   <li>Floating point precision</li>
 *   <li>Decimal precision and scale</li>
 * </ul>
 */
public class NumericalValidator {

    private static final double EPSILON = 1e-10;

    /**
     * Check if a DataType is numerical.
     */
    public boolean isNumericalType(DataType dataType) {
        return dataType instanceof NumericType || dataType instanceof DecimalType;
    }

    /**
     * Compare two numerical values with appropriate precision.
     *
     * @param sparkValue Value from Spark
     * @param jdbcValue Value from JDBC/DuckDB
     * @param dataType Expected data type
     * @return true if values are numerically equal
     */
    public boolean areNumericallyEqual(Object sparkValue, Object jdbcValue, DataType dataType) {
        // Handle integer types with exact comparison
        if (dataType instanceof IntegerType || dataType instanceof LongType ||
                dataType instanceof ShortType || dataType instanceof ByteType) {
            return compareIntegralValues(sparkValue, jdbcValue);
        }

        // Handle floating point types with epsilon comparison
        if (dataType instanceof FloatType || dataType instanceof DoubleType) {
            return compareFloatingPointValues(sparkValue, jdbcValue);
        }

        // Handle decimal types
        if (dataType instanceof DecimalType) {
            return compareDecimalValues(sparkValue, jdbcValue);
        }

        // Fallback to equals
        return sparkValue.equals(jdbcValue);
    }

    /**
     * Compare integral values (byte, short, int, long).
     */
    private boolean compareIntegralValues(Object sparkValue, Object jdbcValue) {
        long sparkLong = ((Number) sparkValue).longValue();
        long jdbcLong = ((Number) jdbcValue).longValue();
        return sparkLong == jdbcLong;
    }

    /**
     * Compare floating point values with epsilon tolerance.
     */
    private boolean compareFloatingPointValues(Object sparkValue, Object jdbcValue) {
        double sparkDouble = ((Number) sparkValue).doubleValue();
        double jdbcDouble = ((Number) jdbcValue).doubleValue();

        // Handle NaN
        if (Double.isNaN(sparkDouble) && Double.isNaN(jdbcDouble)) {
            return true;
        }

        // Handle Infinity
        if (Double.isInfinite(sparkDouble) && Double.isInfinite(jdbcDouble)) {
            return Double.compare(sparkDouble, jdbcDouble) == 0;
        }

        // Handle one NaN or one Infinity
        if (Double.isNaN(sparkDouble) || Double.isNaN(jdbcDouble) ||
                Double.isInfinite(sparkDouble) || Double.isInfinite(jdbcDouble)) {
            return false;
        }

        // Epsilon-based comparison
        return Math.abs(sparkDouble - jdbcDouble) < EPSILON;
    }

    /**
     * Compare decimal values.
     */
    private boolean compareDecimalValues(Object sparkValue, Object jdbcValue) {
        BigDecimal sparkDecimal = toBigDecimal(sparkValue);
        BigDecimal jdbcDecimal = toBigDecimal(jdbcValue);

        if (sparkDecimal == null || jdbcDecimal == null) {
            return sparkDecimal == jdbcDecimal;
        }

        // Use compareTo for decimal comparison (ignores scale if values are numerically equal)
        return sparkDecimal.compareTo(jdbcDecimal) == 0;
    }

    /**
     * Convert value to BigDecimal.
     */
    private BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        return null;
    }
}
