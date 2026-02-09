package com.thunderduck.types;

import java.util.Objects;

/**
 * Data type representing an array (ordered collection of elements of the same type).
 * Maps to DuckDB array type and Spark ArrayType.
 *
 * <p>Example: ARRAY<INTEGER> for an array of integers
 * <p>DuckDB syntax: INTEGER[]
 * <p>Supports nesting: ARRAY<ARRAY<STRING>> → VARCHAR[][]
 */
public final class ArrayType implements DataType {

    private final DataType elementType;
    private final boolean containsNull;

    /**
     * Creates an array type with the given element type.
     *
     * @param elementType the type of elements in the array
     * @param containsNull whether the array can contain null elements
     */
    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = Objects.requireNonNull(elementType, "elementType must not be null");
        this.containsNull = containsNull;
    }

    /**
     * Creates an array type that allows null elements.
     *
     * @param elementType the type of elements in the array
     */
    public ArrayType(DataType elementType) {
        this(elementType, true);
    }

    /**
     * Returns the element type.
     *
     * @return the element type
     */
    public DataType elementType() {
        return elementType;
    }

    /**
     * Returns whether the array can contain null elements.
     *
     * @return true if nulls are allowed, false otherwise
     */
    public boolean containsNull() {
        return containsNull;
    }

    @Override
    public String typeName() {
        return "array<" + elementType.typeName() + ">";
    }

    @Override
    public int defaultSize() {
        return -1; // Variable length
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ArrayType)) return false;
        ArrayType that = (ArrayType) obj;
        return containsNull == that.containsNull &&
               Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementType, containsNull);
    }

    @Override
    public String toString() {
        return typeName();
    }
}
