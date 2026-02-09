package com.thunderduck.types;

import java.util.Objects;

/**
 * Data type representing a map (key-value pairs).
 * Maps to DuckDB MAP type and Spark MapType.
 *
 * <p>Example: MAP<STRING, INTEGER> for a map from strings to integers
 * <p>DuckDB syntax: MAP(VARCHAR, INTEGER)
 *
 * <p>Keys cannot be null in either Spark or DuckDB.
 * Values can be null if valueContainsNull is true.
 */
public final class MapType implements DataType {

    private final DataType keyType;
    private final DataType valueType;
    private final boolean valueContainsNull;

    /**
     * Creates a map type with the given key and value types.
     *
     * @param keyType the type of keys (must not allow nulls)
     * @param valueType the type of values
     * @param valueContainsNull whether values can be null
     */
    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyType = Objects.requireNonNull(keyType, "keyType must not be null");
        this.valueType = Objects.requireNonNull(valueType, "valueType must not be null");
        this.valueContainsNull = valueContainsNull;
    }

    /**
     * Creates a map type that allows null values.
     *
     * @param keyType the type of keys
     * @param valueType the type of values
     */
    public MapType(DataType keyType, DataType valueType) {
        this(keyType, valueType, true);
    }

    /**
     * Returns the key type.
     *
     * @return the key type
     */
    public DataType keyType() {
        return keyType;
    }

    /**
     * Returns the value type.
     *
     * @return the value type
     */
    public DataType valueType() {
        return valueType;
    }

    /**
     * Returns whether values can be null.
     *
     * @return true if values can be null, false otherwise
     */
    public boolean valueContainsNull() {
        return valueContainsNull;
    }

    @Override
    public String typeName() {
        return "map<" + keyType.typeName() + "," + valueType.typeName() + ">";
    }

    @Override
    public int defaultSize() {
        return -1; // Variable length
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof MapType)) return false;
        MapType that = (MapType) obj;
        return valueContainsNull == that.valueContainsNull &&
               Objects.equals(keyType, that.keyType) &&
               Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyType, valueType, valueContainsNull);
    }

    @Override
    public String toString() {
        return typeName();
    }
}
