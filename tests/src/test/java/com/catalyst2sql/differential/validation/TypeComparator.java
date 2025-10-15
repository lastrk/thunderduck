package com.catalyst2sql.differential.validation;

import org.apache.spark.sql.types.*;

import java.sql.Types;

/**
 * Compares and validates type compatibility between Spark and JDBC/DuckDB types.
 *
 * <p>Handles type system differences and compatibility checks between:
 * <ul>
 *   <li>Spark DataTypes</li>
 *   <li>JDBC SQL Types</li>
 *   <li>DuckDB-specific type mappings</li>
 * </ul>
 */
public class TypeComparator {

    /**
     * Check if Spark DataType is compatible with JDBC type code.
     *
     * @param sparkType Spark DataType
     * @param jdbcTypeCode JDBC Types constant
     * @return true if types are compatible
     */
    public boolean areTypesCompatible(DataType sparkType, int jdbcTypeCode) {
        // Integer types
        if (sparkType instanceof IntegerType) {
            return jdbcTypeCode == Types.INTEGER || jdbcTypeCode == Types.BIGINT;
        }

        // Long types
        if (sparkType instanceof LongType) {
            return jdbcTypeCode == Types.BIGINT || jdbcTypeCode == Types.INTEGER;
        }

        // Short types
        if (sparkType instanceof ShortType) {
            return jdbcTypeCode == Types.SMALLINT || jdbcTypeCode == Types.INTEGER;
        }

        // Byte types
        if (sparkType instanceof ByteType) {
            return jdbcTypeCode == Types.TINYINT || jdbcTypeCode == Types.SMALLINT;
        }

        // Float types
        if (sparkType instanceof FloatType) {
            return jdbcTypeCode == Types.FLOAT || jdbcTypeCode == Types.REAL || jdbcTypeCode == Types.DOUBLE;
        }

        // Double types
        if (sparkType instanceof DoubleType) {
            return jdbcTypeCode == Types.DOUBLE || jdbcTypeCode == Types.FLOAT;
        }

        // String types
        if (sparkType instanceof StringType) {
            return jdbcTypeCode == Types.VARCHAR || jdbcTypeCode == Types.CHAR || jdbcTypeCode == Types.LONGVARCHAR;
        }

        // Boolean types
        if (sparkType instanceof BooleanType) {
            return jdbcTypeCode == Types.BOOLEAN || jdbcTypeCode == Types.BIT;
        }

        // Date types
        if (sparkType instanceof DateType) {
            return jdbcTypeCode == Types.DATE;
        }

        // Timestamp types
        if (sparkType instanceof TimestampType) {
            return jdbcTypeCode == Types.TIMESTAMP;
        }

        // Decimal types
        if (sparkType instanceof DecimalType) {
            return jdbcTypeCode == Types.DECIMAL || jdbcTypeCode == Types.NUMERIC;
        }

        // Binary types
        if (sparkType instanceof BinaryType) {
            return jdbcTypeCode == Types.BINARY || jdbcTypeCode == Types.VARBINARY;
        }

        // Null type
        if (sparkType instanceof NullType) {
            return jdbcTypeCode == Types.NULL;
        }

        // For complex types (Array, Map, Struct), just check they're not null
        // DuckDB may represent these differently
        if (sparkType instanceof ArrayType || sparkType instanceof MapType || sparkType instanceof StructType) {
            return jdbcTypeCode == Types.OTHER || jdbcTypeCode == Types.ARRAY || jdbcTypeCode == Types.STRUCT;
        }

        // Unknown type combination
        return false;
    }

    /**
     * Convert JDBC type code to readable string.
     *
     * @param jdbcTypeCode JDBC Types constant
     * @return Human-readable type name
     */
    public String jdbcTypeToString(int jdbcTypeCode) {
        switch (jdbcTypeCode) {
            case Types.BIT:
                return "BIT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.CHAR:
                return "CHAR";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
                return "BINARY";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.NULL:
                return "NULL";
            case Types.OTHER:
                return "OTHER";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.ARRAY:
                return "ARRAY";
            case Types.STRUCT:
                return "STRUCT";
            default:
                return "UNKNOWN(" + jdbcTypeCode + ")";
        }
    }
}
