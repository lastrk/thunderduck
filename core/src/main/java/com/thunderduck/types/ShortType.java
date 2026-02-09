package com.thunderduck.types;

/**
 * Data type representing a 16-bit signed integer.
 * Maps to DuckDB SMALLINT and Spark ShortType.
 */
public final class ShortType implements DataType {

    private static final ShortType INSTANCE = new ShortType();

    private ShortType() {}

    public static ShortType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "short";
    }

    @Override
    public int defaultSize() {
        return 2;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ShortType;
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
