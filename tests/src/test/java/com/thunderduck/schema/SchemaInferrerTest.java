package com.thunderduck.schema;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Unit tests for SchemaInferrer - the class that infers schema from DuckDB
 * by executing DESCRIBE queries.
 *
 * Tests cover:
 * - Schema inference for simple tables
 * - DuckDB type mapping to thunderduck DataTypes
 * - Handling of nullable columns
 * - Complex queries (subqueries, joins)
 * - Error handling for invalid SQL
 *
 * @see SchemaInferrer
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("SchemaInferrer Unit Tests")
public class SchemaInferrerTest extends TestBase {

    private Connection connection;
    private SchemaInferrer inferrer;

    @Override
    protected void doSetUp() {
        try {
            // Create in-memory DuckDB connection
            connection = DriverManager.getConnection("jdbc:duckdb:");
            inferrer = new SchemaInferrer(connection);

            // Set up test tables
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE test_table (" +
                    "id INTEGER NOT NULL, " +
                    "name VARCHAR, " +
                    "amount DOUBLE, " +
                    "active BOOLEAN, " +
                    "created_at TIMESTAMP)");

                stmt.execute("CREATE TABLE numeric_types (" +
                    "tiny TINYINT, " +
                    "small SMALLINT, " +
                    "int_col INTEGER, " +
                    "big BIGINT, " +
                    "real_col REAL, " +
                    "double_col DOUBLE)");

                stmt.execute("CREATE TABLE string_types (" +
                    "varchar_col VARCHAR(100), " +
                    "text_col TEXT, " +
                    "blob_col BLOB)");

                stmt.execute("CREATE TABLE date_types (" +
                    "date_col DATE, " +
                    "time_col TIME, " +
                    "timestamp_col TIMESTAMP)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set up test database", e);
        }
    }

    @Override
    protected void doTearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Ignore
            }
        }
    }

    @Nested
    @DisplayName("Basic Schema Inference Tests")
    class BasicSchemaInferenceTests {

        @Test
        @DisplayName("Should infer schema from simple SELECT *")
        void testInferSimpleSelectStar() {
            StructType schema = inferrer.inferSchema("SELECT * FROM test_table");

            assertThat(schema.size()).isEqualTo(5);

            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(IntegerType.class);
            assertThat(schema.fieldAt(0).nullable()).isFalse();

            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class);
            assertThat(schema.fieldAt(1).nullable()).isTrue();

            assertThat(schema.fieldAt(2).name()).isEqualTo("amount");
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(DoubleType.class);

            assertThat(schema.fieldAt(3).name()).isEqualTo("active");
            assertThat(schema.fieldAt(3).dataType()).isInstanceOf(BooleanType.class);

            assertThat(schema.fieldAt(4).name()).isEqualTo("created_at");
            assertThat(schema.fieldAt(4).dataType()).isInstanceOf(TimestampType.class);
        }

        @Test
        @DisplayName("Should infer schema from SELECT with specific columns")
        void testInferSelectSpecificColumns() {
            StructType schema = inferrer.inferSchema("SELECT id, name FROM test_table");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
        }

        @Test
        @DisplayName("Should infer schema from SELECT with alias")
        void testInferSelectWithAlias() {
            StructType schema = inferrer.inferSchema("SELECT id AS item_id, name AS item_name FROM test_table");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("item_id");
            assertThat(schema.fieldAt(1).name()).isEqualTo("item_name");
        }
    }

    @Nested
    @DisplayName("Type Mapping Tests")
    class TypeMappingTests {

        @Test
        @DisplayName("Should map DuckDB INTEGER to IntegerType")
        void testMapInteger() {
            DataType type = SchemaInferrer.mapDuckDBType("INTEGER");
            assertThat(type).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Should map DuckDB INT alias to IntegerType")
        void testMapIntAlias() {
            DataType type = SchemaInferrer.mapDuckDBType("INT");
            assertThat(type).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Should map DuckDB BIGINT to LongType")
        void testMapBigint() {
            DataType type = SchemaInferrer.mapDuckDBType("BIGINT");
            assertThat(type).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("Should map DuckDB TINYINT to ByteType")
        void testMapTinyint() {
            DataType type = SchemaInferrer.mapDuckDBType("TINYINT");
            assertThat(type).isInstanceOf(ByteType.class);
        }

        @Test
        @DisplayName("Should map DuckDB SMALLINT to ShortType")
        void testMapSmallint() {
            DataType type = SchemaInferrer.mapDuckDBType("SMALLINT");
            assertThat(type).isInstanceOf(ShortType.class);
        }

        @Test
        @DisplayName("Should map DuckDB REAL to FloatType")
        void testMapReal() {
            DataType type = SchemaInferrer.mapDuckDBType("REAL");
            assertThat(type).isInstanceOf(FloatType.class);
        }

        @Test
        @DisplayName("Should map DuckDB DOUBLE to DoubleType")
        void testMapDouble() {
            DataType type = SchemaInferrer.mapDuckDBType("DOUBLE");
            assertThat(type).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Should map DuckDB VARCHAR to StringType")
        void testMapVarchar() {
            DataType type = SchemaInferrer.mapDuckDBType("VARCHAR");
            assertThat(type).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should map DuckDB VARCHAR(n) to StringType (ignoring size)")
        void testMapVarcharWithSize() {
            DataType type = SchemaInferrer.mapDuckDBType("VARCHAR(255)");
            assertThat(type).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should map DuckDB TEXT to StringType")
        void testMapText() {
            DataType type = SchemaInferrer.mapDuckDBType("TEXT");
            assertThat(type).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should map DuckDB BLOB to BinaryType")
        void testMapBlob() {
            DataType type = SchemaInferrer.mapDuckDBType("BLOB");
            assertThat(type).isInstanceOf(BinaryType.class);
        }

        @Test
        @DisplayName("Should map DuckDB BOOLEAN to BooleanType")
        void testMapBoolean() {
            DataType type = SchemaInferrer.mapDuckDBType("BOOLEAN");
            assertThat(type).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Should map DuckDB DATE to DateType")
        void testMapDate() {
            DataType type = SchemaInferrer.mapDuckDBType("DATE");
            assertThat(type).isInstanceOf(DateType.class);
        }

        @Test
        @DisplayName("Should map DuckDB TIMESTAMP to TimestampType")
        void testMapTimestamp() {
            DataType type = SchemaInferrer.mapDuckDBType("TIMESTAMP");
            assertThat(type).isInstanceOf(TimestampType.class);
        }

        @Test
        @DisplayName("Should handle null input gracefully")
        void testMapNullInput() {
            DataType type = SchemaInferrer.mapDuckDBType(null);
            assertThat(type).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should handle unknown type gracefully")
        void testMapUnknownType() {
            DataType type = SchemaInferrer.mapDuckDBType("UNKNOWN_TYPE");
            assertThat(type).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should be case insensitive")
        void testCaseInsensitive() {
            assertThat(SchemaInferrer.mapDuckDBType("integer"))
                .isInstanceOf(IntegerType.class);
            assertThat(SchemaInferrer.mapDuckDBType("Integer"))
                .isInstanceOf(IntegerType.class);
            assertThat(SchemaInferrer.mapDuckDBType("INTEGER"))
                .isInstanceOf(IntegerType.class);
        }
    }

    @Nested
    @DisplayName("Complex Query Schema Inference Tests")
    class ComplexQueryTests {

        @Test
        @DisplayName("Should infer schema from subquery")
        void testInferSubquery() {
            StructType schema = inferrer.inferSchema(
                "SELECT * FROM (SELECT id, name FROM test_table) AS subq");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
        }

        @Test
        @DisplayName("Should infer schema from expression")
        void testInferExpression() {
            StructType schema = inferrer.inferSchema(
                "SELECT id + 1 AS incremented_id, amount * 2 AS doubled FROM test_table");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("incremented_id");
            assertThat(schema.fieldAt(1).name()).isEqualTo("doubled");
        }

        @Test
        @DisplayName("Should infer schema from CASE expression")
        void testInferCaseExpression() {
            StructType schema = inferrer.inferSchema(
                "SELECT CASE WHEN active THEN 'Y' ELSE 'N' END AS status FROM test_table");

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("status");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should infer schema from COALESCE")
        void testInferCoalesce() {
            StructType schema = inferrer.inferSchema(
                "SELECT COALESCE(name, 'Unknown') AS name FROM test_table");

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("name");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Should infer schema from aggregate function")
        void testInferAggregate() {
            StructType schema = inferrer.inferSchema(
                "SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM test_table");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("cnt");
            assertThat(schema.fieldAt(1).name()).isEqualTo("total");
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should throw SchemaInferenceException for invalid SQL")
        void testInvalidSQL() {
            assertThatThrownBy(() -> inferrer.inferSchema("INVALID SQL"))
                .isInstanceOf(SchemaInferenceException.class);
        }

        @Test
        @DisplayName("Should throw SchemaInferenceException for non-existent table")
        void testNonExistentTable() {
            assertThatThrownBy(() -> inferrer.inferSchema("SELECT * FROM non_existent_table"))
                .isInstanceOf(SchemaInferenceException.class);
        }
    }

    @Nested
    @DisplayName("Numeric Type Schema Inference Tests")
    class NumericTypeTests {

        @Test
        @DisplayName("Should correctly infer all numeric types")
        void testAllNumericTypes() {
            StructType schema = inferrer.inferSchema("SELECT * FROM numeric_types");

            assertThat(schema.size()).isEqualTo(6);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(ByteType.class);    // TINYINT
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(ShortType.class);   // SMALLINT
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(IntegerType.class); // INTEGER
            assertThat(schema.fieldAt(3).dataType()).isInstanceOf(LongType.class);    // BIGINT
            assertThat(schema.fieldAt(4).dataType()).isInstanceOf(FloatType.class);   // REAL
            assertThat(schema.fieldAt(5).dataType()).isInstanceOf(DoubleType.class);  // DOUBLE
        }
    }

    @Nested
    @DisplayName("String Type Schema Inference Tests")
    class StringTypeTests {

        @Test
        @DisplayName("Should correctly infer string types")
        void testStringTypes() {
            StructType schema = inferrer.inferSchema("SELECT * FROM string_types");

            assertThat(schema.size()).isEqualTo(3);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(StringType.class); // VARCHAR
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class); // TEXT
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(BinaryType.class); // BLOB
        }
    }

    @Nested
    @DisplayName("Date/Time Type Schema Inference Tests")
    class DateTimeTypeTests {

        @Test
        @DisplayName("Should correctly infer date/time types")
        void testDateTimeTypes() {
            StructType schema = inferrer.inferSchema("SELECT * FROM date_types");

            assertThat(schema.size()).isEqualTo(3);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(DateType.class);      // DATE
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class);    // TIME (no direct Time type)
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(TimestampType.class); // TIMESTAMP
        }
    }
}
