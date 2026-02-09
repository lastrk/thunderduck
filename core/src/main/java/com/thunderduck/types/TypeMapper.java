package com.thunderduck.types;

/**
 * Maps Spark DataTypes to DuckDB SQL type strings.
 *
 * <p>This class provides bidirectional mapping between Spark's type system
 * and DuckDB's SQL type system, ensuring type compatibility and correctness.
 *
 * <p>Key features:
 * <ul>
 *   <li>Complete coverage of Spark primitive types</li>
 *   <li>Support for complex types (arrays, maps, structs)</li>
 *   <li>Decimal precision/scale preservation</li>
 *   <li>Temporal type microsecond precision alignment</li>
 * </ul>
 *
 * @see DataType
 */
public class TypeMapper {

    /**
     * Converts a thunderduck DataType to a DuckDB SQL type string.
     *
     * <p>Examples:
     * <pre>
     *   IntegerType → "INTEGER"
     *   StringType → "VARCHAR"
     *   DecimalType(10, 2) → "DECIMAL(10,2)"
     *   ArrayType(IntegerType) → "INTEGER[]"
     *   MapType(StringType, IntegerType) → "MAP(VARCHAR, INTEGER)"
     * </pre>
     *
     * @param sparkType the thunderduck data type
     * @return the DuckDB SQL type string
     * @throws UnsupportedOperationException if the type is not supported
     */
    public static String toDuckDBType(DataType sparkType) {
        if (sparkType == null) {
            throw new IllegalArgumentException("sparkType must not be null");
        }

        return switch (sparkType) {
            case BooleanType b   -> "BOOLEAN";
            case ByteType b      -> "TINYINT";
            case ShortType s     -> "SMALLINT";
            case IntegerType i   -> "INTEGER";
            case LongType l      -> "BIGINT";
            case FloatType f     -> "FLOAT";
            case DoubleType db   -> "DOUBLE";
            case StringType s    -> "VARCHAR";
            case DateType d      -> "DATE";
            case TimestampType t -> "TIMESTAMP";
            case BinaryType b    -> "BLOB";
            case DecimalType d   -> String.format("DECIMAL(%d,%d)", d.precision(), d.scale());
            case ArrayType a     -> toDuckDBType(a.elementType()) + "[]";
            case MapType m       -> String.format("MAP(%s, %s)", toDuckDBType(m.keyType()), toDuckDBType(m.valueType()));
            case StructType st   -> throw new UnsupportedOperationException("StructType as column type not supported");
            case UnresolvedType u -> "VARCHAR";
        };
    }

    /**
     * Converts a DuckDB SQL type string to a thunderduck DataType.
     *
     * <p>This is the reverse operation of {@link #toDuckDBType(DataType)}.
     *
     * <p>Examples:
     * <pre>
     *   "INTEGER" → IntegerType
     *   "VARCHAR" → StringType
     *   "DECIMAL(10,2)" → DecimalType(10, 2)
     *   "INTEGER[]" → ArrayType(IntegerType)
     * </pre>
     *
     * @param duckdbType the DuckDB SQL type string
     * @return the thunderduck data type
     * @throws UnsupportedOperationException if the type string is not recognized
     */
    public static DataType toSparkType(String duckdbType) {
        if (duckdbType == null || duckdbType.isEmpty()) {
            throw new IllegalArgumentException("duckdbType must not be null or empty");
        }

        // Normalize to uppercase for comparison
        String normalized = duckdbType.trim().toUpperCase();

        // Handle primitive types
        DataType primitive = switch (normalized) {
            case "TINYINT"                   -> ByteType.get();
            case "SMALLINT"                  -> ShortType.get();
            case "INTEGER", "INT"            -> IntegerType.get();
            case "BIGINT"                    -> LongType.get();
            case "FLOAT", "REAL"             -> FloatType.get();
            case "DOUBLE", "DOUBLE PRECISION" -> DoubleType.get();
            case "VARCHAR", "TEXT", "STRING"  -> StringType.get();
            case "BOOLEAN", "BOOL"           -> BooleanType.get();
            case "DATE"                      -> DateType.get();
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE" -> TimestampType.get();
            case "BLOB", "BYTEA"             -> BinaryType.get();
            default -> null;
        };
        if (primitive != null) {
            return primitive;
        }

        // Handle DECIMAL(p,s)
        if (normalized.startsWith("DECIMAL(") || normalized.startsWith("NUMERIC(")) {
            return parseDecimalType(duckdbType);
        }

        // Handle ARRAY types (e.g., "INTEGER[]")
        if (normalized.endsWith("[]")) {
            String elementTypeStr = duckdbType.substring(0, duckdbType.length() - 2).trim();
            DataType elementType = toSparkType(elementTypeStr);
            return new ArrayType(elementType);
        }

        // Handle MAP types (e.g., "MAP(VARCHAR, INTEGER)")
        if (normalized.startsWith("MAP(")) {
            return parseMapType(duckdbType);
        }

        // Note: STRUCT types are not currently handled as DataType
        // Structs are represented using StructType which is a schema, not a DataType

        throw new UnsupportedOperationException("Unsupported DuckDB type: " + duckdbType);
    }

    /**
     * Checks if a thunderduck DataType is compatible with a DuckDB SQL type string.
     *
     * @param sparkType the thunderduck data type
     * @param duckdbType the DuckDB SQL type string
     * @return true if compatible, false otherwise
     */
    public static boolean isCompatible(DataType sparkType, String duckdbType) {
        try {
            String expectedDuckDBType = toDuckDBType(sparkType);
            String normalizedExpected = expectedDuckDBType.toUpperCase();
            String normalizedActual = duckdbType.toUpperCase().trim();

            // Handle aliases
            if (normalizedExpected.equals("INTEGER") && (normalizedActual.equals("INT") || normalizedActual.equals("INTEGER"))) {
                return true;
            }
            if (normalizedExpected.equals("VARCHAR") && (normalizedActual.equals("TEXT") || normalizedActual.equals("STRING"))) {
                return true;
            }
            if (normalizedExpected.equals("FLOAT") && normalizedActual.equals("REAL")) {
                return true;
            }

            return normalizedExpected.equals(normalizedActual);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parses a DECIMAL type string.
     *
     * @param typeStr the type string (e.g., "DECIMAL(10,2)")
     * @return the DecimalType
     */
    private static DecimalType parseDecimalType(String typeStr) {
        // Extract precision and scale from "DECIMAL(p,s)"
        int start = typeStr.indexOf('(');
        int end = typeStr.indexOf(')');
        if (start == -1 || end == -1) {
            throw new IllegalArgumentException("Invalid DECIMAL type: " + typeStr);
        }

        String params = typeStr.substring(start + 1, end);
        String[] parts = params.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid DECIMAL type: " + typeStr);
        }

        int precision = Integer.parseInt(parts[0].trim());
        int scale = Integer.parseInt(parts[1].trim());
        return new DecimalType(precision, scale);
    }

    /**
     * Parses a MAP type string.
     *
     * @param typeStr the type string (e.g., "MAP(VARCHAR, INTEGER)")
     * @return the MapType
     */
    private static MapType parseMapType(String typeStr) {
        // Extract key and value types from "MAP(keyType, valueType)"
        int start = typeStr.indexOf('(');
        int end = typeStr.lastIndexOf(')');
        if (start == -1 || end == -1) {
            throw new IllegalArgumentException("Invalid MAP type: " + typeStr);
        }

        String params = typeStr.substring(start + 1, end);
        int commaIndex = findTopLevelComma(params);
        if (commaIndex == -1) {
            throw new IllegalArgumentException("Invalid MAP type: " + typeStr);
        }

        String keyTypeStr = params.substring(0, commaIndex).trim();
        String valueTypeStr = params.substring(commaIndex + 1).trim();

        DataType keyType = toSparkType(keyTypeStr);
        DataType valueType = toSparkType(valueTypeStr);
        return new MapType(keyType, valueType);
    }

    /**
     * Finds the top-level comma in a type string (ignoring commas inside parentheses).
     *
     * @param str the string to search
     * @return the index of the comma, or -1 if not found
     */
    private static int findTopLevelComma(String str) {
        int depth = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Splits a string by a delimiter, respecting parentheses nesting.
     *
     * @param str the string to split
     * @param delimiter the delimiter character
     * @return the split parts
     */
    /*private static String[] splitTopLevel(String str, char delimiter) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == delimiter && depth == 0) {
                parts.add(str.substring(start, i));
                start = i + 1;
            }
        }

        // Add the last part
        if (start < str.length()) {
            parts.add(str.substring(start));
        }

        return parts.toArray(new String[0]);
    }*/
}
