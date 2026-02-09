package com.thunderduck.types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a struct type (row schema) with named fields.
 *
 * <p>This is analogous to Spark's StructType and represents the schema of a DataFrame/Dataset.
 * Each field has a name, data type, and nullability flag.
 */
public final class StructType implements DataType {

    /** Empty struct type with no fields. */
    public static final StructType EMPTY = new StructType(Collections.emptyList());

    private final List<StructField> fields;

    /**
     * Creates a StructType with the given fields.
     *
     * @param fields the fields in this struct
     */
    public StructType(List<StructField> fields) {
        this.fields = new ArrayList<>(fields);
    }

    /**
     * Creates a StructType with the given fields.
     *
     * @param fields the fields in this struct
     */
    public StructType(StructField... fields) {
        this.fields = Arrays.asList(fields);
    }

    /**
     * Returns the fields in this struct.
     *
     * @return an unmodifiable list of fields
     */
    public List<StructField> fields() {
        return Collections.unmodifiableList(fields);
    }

    /**
     * Returns the number of fields in this struct.
     *
     * @return the field count
     */
    public int size() {
        return fields.size();
    }

    /**
     * Returns the field at the given index.
     *
     * @param index the field index
     * @return the field
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    public StructField fieldAt(int index) {
        return fields.get(index);
    }

    /**
     * Returns the field with the given name, or null if not found.
     *
     * @param name the field name
     * @return the field, or null if not found
     */
    public StructField fieldByName(String name) {
        return fields.stream()
            .filter(f -> f.name().equals(name))
            .findFirst()
            .orElse(null);
    }

    /**
     * Returns the index of the field with the given name, or -1 if not found.
     *
     * @param name the field name
     * @return the field index, or -1 if not found
     */
    public int fieldIndex(String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public String typeName() {
        return "struct";
    }

    @Override
    public int defaultSize() {
        return -1; // Variable length
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructType that = (StructType) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        return "StructType(" + fields + ")";
    }
}
