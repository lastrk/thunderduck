package com.thunderduck.types;

import java.util.Objects;

/**
 * Data type representing a fixed-precision decimal number.
 * Maps to DuckDB DECIMAL(precision, scale) and Spark DecimalType.
 *
 * <p>Precision is the total number of digits, scale is the number of digits after the decimal point.
 * Example: DECIMAL(10, 2) can store values like 12345678.90
 *
 * <p>DuckDB and Spark both support:
 * - Precision: 1-38 digits
 * - Scale: 0 to precision
 */
public final class DecimalType implements DataType {

    private final int precision;
    private final int scale;

    /**
     * Creates a decimal type with the given precision and scale.
     *
     * @param precision the total number of digits (1-38)
     * @param scale the number of digits after the decimal point (0 to precision)
     */
    public DecimalType(int precision, int scale) {
        if (precision < 1 || precision > 38) {
            throw new IllegalArgumentException("precision must be between 1 and 38, got: " + precision);
        }
        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException("scale must be between 0 and " + precision + ", got: " + scale);
        }
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Returns the precision (total number of digits).
     *
     * @return the precision
     */
    public int precision() {
        return precision;
    }

    /**
     * Returns the scale (number of digits after decimal point).
     *
     * @return the scale
     */
    public int scale() {
        return scale;
    }

    @Override
    public String typeName() {
        return String.format("decimal(%d,%d)", precision, scale);
    }

    @Override
    public int defaultSize() {
        // Approximate storage size based on precision
        if (precision <= 9) return 4;   // 32-bit
        if (precision <= 18) return 8;  // 64-bit
        return 16;                      // 128-bit
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof DecimalType)) return false;
        DecimalType that = (DecimalType) obj;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(precision, scale);
    }

    @Override
    public String toString() {
        return typeName();
    }
}
