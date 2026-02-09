package com.thunderduck.types;

/**
 * Data type representing a double-precision 64-bit floating point number.
 * Maps to DuckDB DOUBLE and Spark DoubleType.
 */
public final class DoubleType implements DataType {

    private static final DoubleType INSTANCE = new DoubleType();

    private DoubleType() {}

    public static DoubleType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "double";
    }

    @Override
    public int defaultSize() {
        return 8;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DoubleType;
    }

    @Override
    public int hashCode() {
        return typeName().hashCode();
    }

    @Override
    public String toString() {
        return typeName();
    }
}
