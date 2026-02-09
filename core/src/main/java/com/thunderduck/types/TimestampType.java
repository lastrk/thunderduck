package com.thunderduck.types;

/**
 * Data type representing a timestamp (date and time with microsecond precision).
 * Maps to DuckDB TIMESTAMP and Spark TimestampType.
 *
 * <p>Both Spark and DuckDB use microsecond precision, ensuring perfect compatibility.
 * Stored as microseconds since Unix epoch (1970-01-01 00:00:00 UTC).
 */
public final class TimestampType implements DataType {

    private static final TimestampType INSTANCE = new TimestampType();

    private TimestampType() {}

    public static TimestampType get() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "timestamp";
    }

    @Override
    public int defaultSize() {
        return 8; // Stored as 64-bit integer (microseconds since epoch)
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TimestampType;
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
