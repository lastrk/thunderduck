package com.thunderduck.types;

/**
 * Data type representing an 8-bit signed integer.
 * Maps to DuckDB TINYINT and Spark ByteType.
 */
public final class ByteType implements DataType {

    private static final ByteType INSTANCE = new ByteType();

    private ByteType() {}

    public static ByteType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "byte";
    }

    @Override
    public int defaultSize() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ByteType;
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
