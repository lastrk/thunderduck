package com.thunderduck.types;

/**
 * Data type representing a variable-length string.
 */
public final class StringType implements DataType {

    private static final StringType INSTANCE = new StringType();

    private StringType() {}

    public static StringType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "string";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringType;
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
