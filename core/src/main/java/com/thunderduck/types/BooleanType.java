package com.thunderduck.types;

/**
 * Data type representing a boolean value (true or false).
 * Maps to DuckDB BOOLEAN and Spark BooleanType.
 */
public final class BooleanType implements DataType {

    private static final BooleanType INSTANCE = new BooleanType();

    private BooleanType() {}

    public static BooleanType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "boolean";
    }

    @Override
    public int defaultSize() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BooleanType;
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
