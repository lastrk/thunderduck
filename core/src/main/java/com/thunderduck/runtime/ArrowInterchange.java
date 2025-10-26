package com.thunderduck.runtime;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.duckdb.DuckDBConnection;
import java.sql.*;
import java.util.*;

/**
 * Zero-copy Arrow data interchange between DuckDB and Spark.
 *
 * <p>This class provides conversion between JDBC ResultSets and Apache Arrow
 * VectorSchemaRoot for efficient data transfer. Arrow's columnar format enables
 * zero-copy data sharing and high-performance analytics.
 *
 * <p>Features:
 * <ul>
 *   <li>JDBC ResultSet to Arrow VectorSchemaRoot conversion</li>
 *   <li>Arrow VectorSchemaRoot to DuckDB table import</li>
 *   <li>SQL type to Arrow type mapping</li>
 *   <li>Efficient columnar data handling</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   // Convert ResultSet to Arrow
 *   ResultSet rs = stmt.executeQuery("SELECT * FROM users");
 *   VectorSchemaRoot root = ArrowInterchange.fromResultSet(rs);
 *
 *   // Import Arrow data to DuckDB
 *   ArrowInterchange.toTable(root, "temp_table", connection);
 * </pre>
 *
 * @see QueryExecutor
 */
public class ArrowInterchange {

    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    /**
     * Converts JDBC ResultSet to Arrow VectorSchemaRoot.
     *
     * <p>This method reads all rows from the ResultSet and converts them to
     * Arrow's columnar format. The caller is responsible for closing the
     * returned VectorSchemaRoot.
     *
     * @param rs the JDBC ResultSet to convert
     * @return the Arrow VectorSchemaRoot
     * @throws SQLException if conversion fails
     */
    public static VectorSchemaRoot fromResultSet(ResultSet rs) throws SQLException {
        Objects.requireNonNull(rs, "rs must not be null");

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build Arrow schema
        List<Field> fields = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            // Use getColumnLabel() to get aliases (e.g., "sum_qty" from SUM(x) AS "sum_qty")
            // Falls back to getColumnName() if no alias is present
            String name = meta.getColumnLabel(i);
            int sqlType = meta.getColumnType(i);
            FieldType fieldType = sqlTypeToArrowType(sqlType, meta, i);
            fields.add(new Field(name, fieldType, null));
        }
        Schema schema = new Schema(fields);

        // Create vector schema root
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        // Load data into temporary storage
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }

        // Populate vectors
        root.setRowCount(rows.size());
        for (int col = 0; col < columnCount; col++) {
            FieldVector vector = root.getVector(col);
            for (int row = 0; row < rows.size(); row++) {
                Object value = rows.get(row).get(col);
                setVectorValue(vector, row, value);
            }
        }

        return root;
    }

    /**
     * Converts Arrow VectorSchemaRoot to DuckDB table.
     *
     * <p>This method creates a temporary table in DuckDB and imports the
     * Arrow data into it.
     *
     * @param root the Arrow VectorSchemaRoot
     * @param tableName the target table name
     * @param conn the DuckDB connection
     * @throws SQLException if import fails
     */
    public static void toTable(VectorSchemaRoot root, String tableName,
                               DuckDBConnection conn) throws SQLException {
        Objects.requireNonNull(root, "root must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(conn, "conn must not be null");

        // Create table from schema
        String createSQL = generateCreateTable(tableName, root.getSchema());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
        }

        // Insert data
        String insertSQL = generateInsertStatement(tableName, root.getSchema());
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            for (int row = 0; row < root.getRowCount(); row++) {
                for (int col = 0; col < root.getFieldVectors().size(); col++) {
                    FieldVector vector = root.getVector(col);
                    Object value = getVectorValue(vector, row);
                    pstmt.setObject(col + 1, value);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    /**
     * Converts SQL type to Arrow FieldType.
     *
     * @param sqlType the SQL type constant
     * @param meta the ResultSet metadata
     * @param column the column index (1-based)
     * @return the Arrow FieldType
     */
    private static FieldType sqlTypeToArrowType(int sqlType, ResultSetMetaData meta, int column)
            throws SQLException {
        boolean nullable = meta.isNullable(column) != ResultSetMetaData.columnNoNulls;

        switch (sqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.BIT.getType(), null);

            case Types.TINYINT:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType(), null);

            case Types.SMALLINT:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType(), null);

            case Types.INTEGER:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), null);

            case Types.BIGINT:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType(), null);

            case Types.FLOAT:
            case Types.REAL:
                return new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);

            case Types.DOUBLE:
                return new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

            case Types.DECIMAL:
            case Types.NUMERIC:
                int precision = meta.getPrecision(column);
                int scale = meta.getScale(column);
                return new FieldType(nullable, new ArrowType.Decimal(precision, scale, 128), null);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), null);

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType(), null);

            case Types.DATE:
                return new FieldType(nullable, new ArrowType.Date(DateUnit.DAY), null);

            case Types.TIME:
                return new FieldType(nullable, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null);

            case Types.TIMESTAMP:
                return new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null);

            default:
                // Default to VARCHAR for unknown types
                return new FieldType(nullable, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), null);
        }
    }

    /**
     * Sets a value in an Arrow vector at the specified index.
     *
     * @param vector the vector to update
     * @param index the row index
     * @param value the value to set (may be null)
     */
    private static void setVectorValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }

        if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, (Boolean) value ? 1 : 0);
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof DecimalVector) {
            // Handle DECIMAL/NUMERIC types (e.g., SUM on integers returns HUGEINT/DECIMAL)
            if (value instanceof Number) {
                java.math.BigDecimal decimal = new java.math.BigDecimal(value.toString());
                ((DecimalVector) vector).setSafe(index, decimal);
            }
        } else if (vector instanceof Decimal256Vector) {
            // Handle 256-bit decimals
            if (value instanceof Number) {
                java.math.BigDecimal decimal = new java.math.BigDecimal(value.toString());
                ((Decimal256Vector) vector).setSafe(index, decimal);
            }
        } else if (vector instanceof VarCharVector) {
            byte[] bytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vector).setSafe(index, bytes);
        } else if (vector instanceof VarBinaryVector) {
            if (value instanceof byte[]) {
                ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
            }
        } else if (vector instanceof DateDayVector) {
            if (value instanceof java.sql.Date) {
                long days = ((java.sql.Date) value).toLocalDate().toEpochDay();
                ((DateDayVector) vector).setSafe(index, (int) days);
            }
        } else if (vector instanceof TimeStampMicroVector) {
            if (value instanceof java.sql.Timestamp) {
                long micros = ((java.sql.Timestamp) value).getTime() * 1000;
                ((TimeStampMicroVector) vector).setSafe(index, micros);
            }
        } else {
            // Fallback: convert to string
            if (vector instanceof VarCharVector) {
                byte[] bytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ((VarCharVector) vector).setSafe(index, bytes);
            }
        }
    }

    /**
     * Gets a value from an Arrow vector at the specified index.
     *
     * @param vector the vector to read from
     * @param index the row index
     * @return the value (may be null)
     */
    private static Object getVectorValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }

        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index) != 0;
        } else if (vector instanceof TinyIntVector) {
            return ((TinyIntVector) vector).get(index);
        } else if (vector instanceof SmallIntVector) {
            return ((SmallIntVector) vector).get(index);
        } else if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        } else if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof DecimalVector) {
            // Handle DECIMAL/NUMERIC types - convert to double for simplicity
            java.math.BigDecimal decimal = ((DecimalVector) vector).getObject(index);
            return decimal != null ? decimal.doubleValue() : null;
        } else if (vector instanceof Decimal256Vector) {
            // Handle 256-bit decimals
            java.math.BigDecimal decimal = ((Decimal256Vector) vector).getObject(index);
            return decimal != null ? decimal.doubleValue() : null;
        } else if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        } else if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(index);
        } else if (vector instanceof DateDayVector) {
            int days = ((DateDayVector) vector).get(index);
            return java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(days));
        } else if (vector instanceof TimeStampMicroVector) {
            long micros = ((TimeStampMicroVector) vector).get(index);
            return new java.sql.Timestamp(micros / 1000);
        }

        return null;
    }

    /**
     * Generates a CREATE TABLE statement from Arrow schema.
     *
     * @param tableName the table name
     * @param schema the Arrow schema
     * @return the CREATE TABLE SQL
     */
    private static String generateCreateTable(String tableName, Schema schema) {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(tableName).append(" (");

        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            Field field = fields.get(i);
            sql.append(field.getName()).append(" ");
            sql.append(arrowTypeToSQLType(field.getType()));
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Generates an INSERT statement for Arrow schema.
     *
     * @param tableName the table name
     * @param schema the Arrow schema
     * @return the INSERT SQL with placeholders
     */
    private static String generateInsertStatement(String tableName, Schema schema) {
        int fieldCount = schema.getFields().size();
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" VALUES (");

        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * Converts Arrow type to SQL type string.
     *
     * @param type the Arrow type
     * @return the SQL type name
     */
    private static String arrowTypeToSQLType(ArrowType type) {
        switch (type.getTypeID()) {
            case Bool: return "BOOLEAN";
            case Int:
                ArrowType.Int intType = (ArrowType.Int) type;
                switch (intType.getBitWidth()) {
                    case 8: return "TINYINT";
                    case 16: return "SMALLINT";
                    case 32: return "INTEGER";
                    case 64: return "BIGINT";
                    default: return "INTEGER";
                }
            case FloatingPoint:
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) type;
                return fpType.getPrecision() == FloatingPointPrecision.SINGLE ? "FLOAT" : "DOUBLE";
            case Utf8: return "VARCHAR";
            case Binary: return "VARBINARY";
            case Date: return "DATE";
            case Time: return "TIME";
            case Timestamp: return "TIMESTAMP";
            case Decimal: return "DECIMAL";
            default: return "VARCHAR";
        }
    }

    /**
     * Returns the allocator used for Arrow memory management.
     *
     * @return the root allocator
     */
    public static RootAllocator getAllocator() {
        return allocator;
    }
}
