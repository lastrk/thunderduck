package com.thunderduck.types;

/**
 * Data type representing a 32-bit signed integer.
 */
public final class IntegerType implements DataType {

    private static final IntegerType INSTANCE = new IntegerType();

    private IntegerType() {}

    public static IntegerType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "integer";
    }

    @Override
    public int defaultSize() {
        return 4;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IntegerType;
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
