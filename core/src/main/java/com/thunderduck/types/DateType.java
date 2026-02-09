package com.thunderduck.types;

/**
 * Data type representing a date (year, month, day) without time information.
 * Maps to DuckDB DATE and Spark DateType.
 *
 * <p>Stored as days since Unix epoch (1970-01-01).
 */
public final class DateType implements DataType {

    private static final DateType INSTANCE = new DateType();

    private DateType() {}

    public static DateType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "date";
    }

    @Override
    public int defaultSize() {
        return 4; // Stored as 32-bit integer (days since epoch)
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DateType;
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
