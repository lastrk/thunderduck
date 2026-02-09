package com.thunderduck.types;

/**
 * Data type representing a single-precision 32-bit floating point number.
 * Maps to DuckDB FLOAT and Spark FloatType.
 */
public final class FloatType implements DataType {

    private static final FloatType INSTANCE = new FloatType();

    private FloatType() {}

    public static FloatType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "float";
    }

    @Override
    public int defaultSize() {
        return 4;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FloatType;
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
