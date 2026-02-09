package com.thunderduck.types;

/**
 * Data type representing a 64-bit signed integer.
 * Maps to DuckDB BIGINT and Spark LongType.
 */
public final class LongType implements DataType {

    private static final LongType INSTANCE = new LongType();

    private LongType() {}

    public static LongType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "long";
    }

    @Override
    public int defaultSize() {
        return 8;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LongType;
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
