package com.thunderduck.schema;

import com.thunderduck.types.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Infers schema from DuckDB by executing DESCRIBE queries.
 *
 * <p>This class is used to determine column names and types when schema
 * information is not available at plan construction time.
 */
public class SchemaInferrer {

    private final Connection connection;

    /**
     * Creates a schema inferrer with the given connection.
     *
     * @param connection the DuckDB connection to use for schema queries
     */
    public SchemaInferrer(Connection connection) {
        this.connection = connection;
    }

    /**
     * Returns the connection used for schema inference.
     *
     * @return the DuckDB connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Infers the schema of a SQL query by executing DESCRIBE.
     *
     * @param sql the SQL query to analyze
     * @return the inferred schema
     * @throws SchemaInferenceException if schema inference fails
     */
    public StructType inferSchema(String sql) {
        // Use DESCRIBE to get schema without executing the full query
        String describeSql = "DESCRIBE (" + sql + ")";

        List<StructField> fields = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(describeSql)) {

            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String columnType = rs.getString("column_type");
                String nullableStr = rs.getString("null");

                boolean nullable = !"NO".equalsIgnoreCase(nullableStr);
                DataType dataType = mapDuckDBType(columnType);

                fields.add(new StructField(columnName, dataType, nullable));
            }

        } catch (SQLException e) {
            throw new SchemaInferenceException("Failed to infer schema for query: " + sql, e);
        }

        return new StructType(fields);
    }

    /**
     * Maps a DuckDB type name to a thunderduck DataType.
     *
     * @param duckdbType the DuckDB type name (e.g., "INTEGER", "VARCHAR", "DOUBLE")
     * @return the corresponding DataType
     */
    public static DataType mapDuckDBType(String duckdbType) {
        if (duckdbType == null) {
            return StringType.get(); // Default fallback
        }

        String upper = duckdbType.toUpperCase().trim();

        // Handle DECIMAL(p,s) or NUMERIC(p,s) with precision and scale
        if (upper.startsWith("DECIMAL(") || upper.startsWith("NUMERIC(")) {
            return parseDecimalType(upper);
        }

        // Handle LIST types: LIST(INTEGER), INTEGER[], VARCHAR[]
        if (upper.startsWith("LIST(") || upper.endsWith("[]")) {
            return parseListType(upper);
        }

        // Normalize: uppercase and remove size specifiers like VARCHAR(255)
        String normalized = upper.replaceAll("\\(.*\\)", "").trim();

        return switch (normalized) {
            case "TINYINT", "INT1"                     -> ByteType.get();
            case "SMALLINT", "INT2", "SHORT"           -> ShortType.get();
            case "INTEGER", "INT", "INT4", "SIGNED"    -> IntegerType.get();
            case "BIGINT", "INT8", "LONG"              -> LongType.get();
            case "HUGEINT", "INT128"                   -> LongType.get();
            case "UTINYINT", "UINT1"                   -> ShortType.get();
            case "USMALLINT", "UINT2"                  -> IntegerType.get();
            case "UINTEGER", "UINT4"                   -> LongType.get();
            case "UBIGINT", "UINT8"                    -> LongType.get();
            case "REAL", "FLOAT", "FLOAT4"             -> FloatType.get();
            case "DOUBLE", "FLOAT8"                    -> DoubleType.get();
            case "DECIMAL", "NUMERIC"                  -> new DecimalType(38, 18);
            case "VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "NAME" -> StringType.get();
            case "BLOB", "BYTEA", "BINARY", "VARBINARY" -> BinaryType.get();
            case "BOOLEAN", "BOOL", "LOGICAL"          -> BooleanType.get();
            case "DATE"                                -> DateType.get();
            case "TIMESTAMP", "DATETIME", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> TimestampType.get();
            case "TIME", "INTERVAL", "UUID", "JSON"    -> StringType.get();
            default                                    -> StringType.get();
        };
    }

    /**
     * Parses a LIST type string to an ArrayType.
     *
     * @param typeStr the type string like "LIST(INTEGER)" or "INTEGER[]"
     * @return the ArrayType with the correct element type
     */
    private static DataType parseListType(String typeStr) {
        String elementTypeStr;

        if (typeStr.endsWith("[]")) {
            // Format: INTEGER[], VARCHAR[], etc.
            elementTypeStr = typeStr.substring(0, typeStr.length() - 2).trim();
        } else if (typeStr.startsWith("LIST(") && typeStr.endsWith(")")) {
            // Format: LIST(INTEGER), LIST(VARCHAR), etc.
            elementTypeStr = typeStr.substring(5, typeStr.length() - 1).trim();
        } else {
            // Fallback - shouldn't happen but return default ArrayType
            return new ArrayType(StringType.get(), true);
        }

        // Recursively resolve element type (handles nested arrays)
        DataType elementType = mapDuckDBType(elementTypeStr);
        return new ArrayType(elementType, true);
    }

    /**
     * Parses a DECIMAL(precision, scale) or NUMERIC(precision, scale) type string.
     *
     * @param typeStr the type string like "DECIMAL(10,2)"
     * @return the DecimalType with extracted precision and scale
     */
    private static DataType parseDecimalType(String typeStr) {
        try {
            // Extract content between parentheses: "DECIMAL(10,2)" -> "10,2"
            int start = typeStr.indexOf('(');
            int end = typeStr.indexOf(')');
            if (start > 0 && end > start) {
                String params = typeStr.substring(start + 1, end).trim();
                String[] parts = params.split(",");
                if (parts.length == 2) {
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = Integer.parseInt(parts[1].trim());
                    return new DecimalType(precision, scale);
                } else if (parts.length == 1) {
                    // DECIMAL(precision) without scale
                    int precision = Integer.parseInt(parts[0].trim());
                    return new DecimalType(precision, 0);
                }
            }
        } catch (NumberFormatException e) {
            // Fall through to default
        }
        // Default if parsing fails
        return new DecimalType(38, 18);
    }
}
