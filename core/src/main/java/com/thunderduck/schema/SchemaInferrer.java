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

        // Normalize: uppercase and remove size specifiers like VARCHAR(255)
        String normalized = duckdbType.toUpperCase().replaceAll("\\(.*\\)", "").trim();

        switch (normalized) {
            // Integer types
            case "TINYINT":
            case "INT1":
                return ByteType.get();
            case "SMALLINT":
            case "INT2":
            case "SHORT":
                return ShortType.get();
            case "INTEGER":
            case "INT":
            case "INT4":
            case "SIGNED":
                return IntegerType.get();
            case "BIGINT":
            case "INT8":
            case "LONG":
                return LongType.get();
            case "HUGEINT":
            case "INT128":
                return LongType.get(); // Best approximation
            case "UTINYINT":
            case "UINT1":
                return ShortType.get(); // Widen to signed
            case "USMALLINT":
            case "UINT2":
                return IntegerType.get();
            case "UINTEGER":
            case "UINT4":
                return LongType.get();
            case "UBIGINT":
            case "UINT8":
                return LongType.get(); // May lose precision

            // Floating point types
            case "REAL":
            case "FLOAT":
            case "FLOAT4":
                return FloatType.get();
            case "DOUBLE":
            case "FLOAT8":
                return DoubleType.get();
            case "DECIMAL":
            case "NUMERIC":
                return DoubleType.get(); // Approximation

            // String types
            case "VARCHAR":
            case "CHAR":
            case "BPCHAR":
            case "TEXT":
            case "STRING":
            case "NAME":
                return StringType.get();

            // Binary types
            case "BLOB":
            case "BYTEA":
            case "BINARY":
            case "VARBINARY":
                return BinaryType.get();

            // Boolean
            case "BOOLEAN":
            case "BOOL":
            case "LOGICAL":
                return BooleanType.get();

            // Date/Time types
            case "DATE":
                return DateType.get();
            case "TIME":
                return StringType.get(); // No direct Time type
            case "TIMESTAMP":
            case "DATETIME":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMPTZ":
                return TimestampType.get();
            case "INTERVAL":
                return StringType.get(); // Represent as string

            // UUID
            case "UUID":
                return StringType.get();

            // JSON
            case "JSON":
                return StringType.get();

            default:
                // For complex types like STRUCT, LIST, MAP - return STRING for now
                if (normalized.startsWith("STRUCT") || normalized.startsWith("MAP") ||
                    normalized.startsWith("LIST") || normalized.contains("[]")) {
                    return StringType.get();
                }
                return StringType.get(); // Safe fallback
        }
    }
}
