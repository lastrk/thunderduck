package com.thunderduck.connect.converter;

import com.thunderduck.generator.SQLQuoting;
import org.apache.spark.connect.proto.DataType;
import org.apache.spark.connect.proto.DataType.Struct;
import org.apache.spark.connect.proto.DataType.StructField;

/**
 * Converts Spark Connect DataType proto messages to DuckDB DDL.
 *
 * <p>This converter handles the conversion from Spark's type system
 * (as defined in the Connect protocol) to DuckDB SQL type strings.
 *
 * <p>Type mappings:
 * <ul>
 *   <li>BOOLEAN → BOOLEAN</li>
 *   <li>BYTE → TINYINT</li>
 *   <li>SHORT → SMALLINT</li>
 *   <li>INTEGER → INTEGER</li>
 *   <li>LONG → BIGINT</li>
 *   <li>FLOAT → FLOAT</li>
 *   <li>DOUBLE → DOUBLE</li>
 *   <li>STRING → VARCHAR</li>
 *   <li>BINARY → BLOB</li>
 *   <li>DATE → DATE</li>
 *   <li>TIMESTAMP → TIMESTAMP</li>
 *   <li>DECIMAL(p,s) → DECIMAL(p,s)</li>
 *   <li>ARRAY(T) → T[]</li>
 *   <li>MAP(K,V) → MAP(K, V)</li>
 *   <li>STRUCT → STRUCT(...)</li>
 * </ul>
 */
public class SparkDataTypeConverter {

    /**
     * Convert a Spark proto DataType to DuckDB SQL type string.
     *
     * @param protoType Spark Connect DataType proto
     * @return DuckDB SQL type string
     * @throws PlanConversionException if the type is not supported
     */
    public static String toDuckDBType(DataType protoType) {
        switch (protoType.getKindCase()) {
            case BOOLEAN:
                return "BOOLEAN";
            case BYTE:
                return "TINYINT";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "BLOB";
            case DATE:
                return "DATE";
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return "TIMESTAMP";
            case DECIMAL:
                DataType.Decimal decimal = protoType.getDecimal();
                int precision = decimal.hasPrecision() ? decimal.getPrecision() : 38;
                int scale = decimal.hasScale() ? decimal.getScale() : 18;
                return String.format("DECIMAL(%d,%d)", precision, scale);
            case ARRAY:
                String elementType = toDuckDBType(protoType.getArray().getElementType());
                return elementType + "[]";
            case MAP:
                String keyType = toDuckDBType(protoType.getMap().getKeyType());
                String valueType = toDuckDBType(protoType.getMap().getValueType());
                return String.format("MAP(%s, %s)", keyType, valueType);
            case STRUCT:
                return convertStructType(protoType.getStruct());
            case NULL:
                // DuckDB doesn't have a NULL type, use VARCHAR as placeholder
                return "VARCHAR";
            case CHAR:
                DataType.Char charType = protoType.getChar();
                return String.format("CHAR(%d)", charType.getLength());
            case VAR_CHAR:
                DataType.VarChar varcharType = protoType.getVarChar();
                return String.format("VARCHAR(%d)", varcharType.getLength());
            default:
                throw new PlanConversionException("Unsupported DataType: " + protoType.getKindCase());
        }
    }

    /**
     * Convert a Spark STRUCT type to DuckDB STRUCT definition.
     *
     * @param struct Spark Struct proto
     * @return DuckDB STRUCT type string
     */
    private static String convertStructType(Struct struct) {
        StringBuilder sb = new StringBuilder("STRUCT(");
        for (int i = 0; i < struct.getFieldsCount(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            StructField field = struct.getFields(i);
            sb.append(SQLQuoting.quoteIdentifier(field.getName()))
              .append(" ")
              .append(toDuckDBType(field.getDataType()));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Generate CREATE TABLE DDL from a table name and schema.
     *
     * @param tableName Name of the table (will be quoted)
     * @param schema Schema as Struct proto containing column definitions
     * @return CREATE TABLE DDL statement
     */
    public static String generateCreateTableDDL(String tableName, Struct schema) {
        return generateCreateTableDDLInternal(SQLQuoting.quoteIdentifier(tableName), schema);
    }

    /**
     * Generate CREATE TABLE DDL with schema qualification.
     *
     * @param schemaName Database schema name (e.g., "main")
     * @param tableName Table name
     * @param schema Schema as Struct proto containing column definitions
     * @return CREATE TABLE DDL statement
     */
    public static String generateCreateTableDDL(String schemaName, String tableName, Struct schema) {
        String qualifiedName = SQLQuoting.quoteIdentifier(schemaName) + "." + SQLQuoting.quoteIdentifier(tableName);
        return generateCreateTableDDLInternal(qualifiedName, schema);
    }

    /**
     * Internal method to generate CREATE TABLE DDL with a pre-quoted table name.
     *
     * @param quotedTableName Already-quoted table name (may include schema)
     * @param schema Schema as Struct proto containing column definitions
     * @return CREATE TABLE DDL statement
     */
    private static String generateCreateTableDDLInternal(String quotedTableName, Struct schema) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        ddl.append(quotedTableName);
        ddl.append(" (");

        for (int i = 0; i < schema.getFieldsCount(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            StructField field = schema.getFields(i);
            ddl.append(SQLQuoting.quoteIdentifier(field.getName()))
               .append(" ")
               .append(toDuckDBType(field.getDataType()));

            // Add NOT NULL constraint if column is not nullable
            if (!field.getNullable()) {
                ddl.append(" NOT NULL");
            }
        }
        ddl.append(")");

        return ddl.toString();
    }
}
