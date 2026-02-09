package com.thunderduck.types;

/**
 * Sealed interface for all data types in the thunderduck type system.
 *
 * <p>This represents the data type of a column or expression. The type system
 * is designed to be compatible with Spark's DataType hierarchy while mapping
 * cleanly to DuckDB's type system.
 *
 * <p>Common data types include:
 * <ul>
 *   <li>Primitive types: IntegerType, LongType, DoubleType, StringType, etc.</li>
 *   <li>Temporal types: DateType, TimestampType</li>
 *   <li>Complex types: ArrayType, MapType, StructType</li>
 * </ul>
 */
public sealed interface DataType
    permits BooleanType, ByteType, ShortType, IntegerType, LongType,
            FloatType, DoubleType, DecimalType, StringType,
            DateType, TimestampType, BinaryType,
            ArrayType, MapType, StructType, UnresolvedType {

    /**
     * Returns a human-readable name for this data type.
     *
     * @return the type name
     */
    String typeName();

    /**
     * Returns the default size in bytes for values of this type.
     *
     * <p>Returns -1 for variable-length types (e.g., String, Array).
     *
     * @return the default size in bytes, or -1 for variable-length types
     */
    default int defaultSize() {
        return -1;
    }
}
