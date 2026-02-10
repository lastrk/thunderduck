package com.thunderduck.types;

import java.util.Objects;

/**
 * Represents a field in a StructType.
 *
 * <p>Each field has a name, data type, and nullability flag.
 */
public record StructField(String name, DataType dataType, boolean nullable) {

    /**
     * Creates a struct field.
     *
     * @param name the field name
     * @param dataType the field data type
     * @param nullable whether the field can contain null values
     */
    public StructField {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(dataType, "dataType must not be null");
    }

    /**
     * Creates a nullable struct field.
     *
     * @param name the field name
     * @param dataType the field data type
     */
    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    @Override
    public String toString() {
        return name + ": " + dataType + (nullable ? "" : " NOT NULL");
    }
}
