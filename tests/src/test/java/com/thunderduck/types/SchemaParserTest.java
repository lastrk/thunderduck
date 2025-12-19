package com.thunderduck.types;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for SchemaParser.
 *
 * <p>Tests parsing of Spark schema strings in struct format:
 * {@code struct<name:type,name2:type2>}
 *
 * <p>Test ID prefix: TC-SCHEMA-PARSER-*
 */
@TestCategories.Tier1
@TestCategories.Unit
@TestCategories.TypeMapping
@DisplayName("SchemaParser Tests")
public class SchemaParserTest extends TestBase {

    // ==================== Basic Struct Parsing ====================

    @Nested
    @DisplayName("Basic Struct Parsing")
    class BasicStructParsing {

        @Test
        @DisplayName("TC-SCHEMA-PARSER-001: Parse simple struct with two fields")
        void testParseSimpleStruct() {
            StructType schema = SchemaParser.parse("struct<id:int,name:string>");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(IntegerType.class);
            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-002: Parse struct with single field")
        void testParseSingleField() {
            StructType schema = SchemaParser.parse("struct<value:double>");

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("value");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-003: Parse empty struct")
        void testParseEmptyStruct() {
            StructType schema = SchemaParser.parse("struct<>");

            assertThat(schema.size()).isEqualTo(0);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-004: Parse struct with many fields")
        void testParseManyFields() {
            StructType schema = SchemaParser.parse(
                "struct<a:int,b:long,c:float,d:double,e:string,f:boolean>");

            assertThat(schema.size()).isEqualTo(6);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(IntegerType.class);
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(LongType.class);
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(FloatType.class);
            assertThat(schema.fieldAt(3).dataType()).isInstanceOf(DoubleType.class);
            assertThat(schema.fieldAt(4).dataType()).isInstanceOf(StringType.class);
            assertThat(schema.fieldAt(5).dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-005: Fields are nullable by default")
        void testFieldsNullableByDefault() {
            StructType schema = SchemaParser.parse("struct<id:int,name:string>");

            assertThat(schema.fieldAt(0).nullable()).isTrue();
            assertThat(schema.fieldAt(1).nullable()).isTrue();
        }
    }

    // ==================== Primitive Type Parsing ====================

    @Nested
    @DisplayName("Primitive Type Parsing")
    class PrimitiveTypeParsing {

        @ParameterizedTest(name = "Type {0} parses to {1}")
        @CsvSource({
            "byte, ByteType",
            "tinyint, ByteType",
            "short, ShortType",
            "smallint, ShortType",
            "int, IntegerType",
            "integer, IntegerType",
            "long, LongType",
            "bigint, LongType"
        })
        @DisplayName("TC-SCHEMA-PARSER-006: Integer type aliases")
        void testIntegerTypeAliases(String typeName, String expectedClass) {
            StructType schema = SchemaParser.parse("struct<col:" + typeName + ">");
            DataType dataType = schema.fieldAt(0).dataType();

            assertThat(dataType.getClass().getSimpleName()).isEqualTo(expectedClass);
        }

        @ParameterizedTest(name = "Type {0} parses to {1}")
        @CsvSource({
            "float, FloatType",
            "real, FloatType",
            "double, DoubleType"
        })
        @DisplayName("TC-SCHEMA-PARSER-007: Floating point type aliases")
        void testFloatingPointTypeAliases(String typeName, String expectedClass) {
            StructType schema = SchemaParser.parse("struct<col:" + typeName + ">");
            DataType dataType = schema.fieldAt(0).dataType();

            assertThat(dataType.getClass().getSimpleName()).isEqualTo(expectedClass);
        }

        @ParameterizedTest(name = "Type {0} parses to StringType")
        @ValueSource(strings = {"string", "varchar", "text"})
        @DisplayName("TC-SCHEMA-PARSER-008: String type aliases")
        void testStringTypeAliases(String typeName) {
            StructType schema = SchemaParser.parse("struct<col:" + typeName + ">");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(StringType.class);
        }

        @ParameterizedTest(name = "Type {0} parses to BooleanType")
        @ValueSource(strings = {"boolean", "bool"})
        @DisplayName("TC-SCHEMA-PARSER-009: Boolean type aliases")
        void testBooleanTypeAliases(String typeName) {
            StructType schema = SchemaParser.parse("struct<col:" + typeName + ">");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(BooleanType.class);
        }

        @ParameterizedTest(name = "Type {0} parses to BinaryType")
        @ValueSource(strings = {"binary", "blob"})
        @DisplayName("TC-SCHEMA-PARSER-010: Binary type aliases")
        void testBinaryTypeAliases(String typeName) {
            StructType schema = SchemaParser.parse("struct<col:" + typeName + ">");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(BinaryType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-011: Date type parsing")
        void testDateType() {
            StructType schema = SchemaParser.parse("struct<col:date>");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(DateType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-012: Timestamp type parsing")
        void testTimestampType() {
            StructType schema = SchemaParser.parse("struct<col:timestamp>");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(TimestampType.class);
        }
    }

    // ==================== Complex Type Parsing ====================

    @Nested
    @DisplayName("Complex Type Parsing")
    class ComplexTypeParsing {

        @Test
        @DisplayName("TC-SCHEMA-PARSER-013: Parse array of primitives")
        void testArrayOfPrimitives() {
            StructType schema = SchemaParser.parse("struct<tags:array<string>>");

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("tags");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(ArrayType.class);

            ArrayType arrayType = (ArrayType) schema.fieldAt(0).dataType();
            assertThat(arrayType.elementType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-014: Parse array of integers")
        void testArrayOfIntegers() {
            StructType schema = SchemaParser.parse("struct<ids:array<int>>");

            ArrayType arrayType = (ArrayType) schema.fieldAt(0).dataType();
            assertThat(arrayType.elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-015: Parse map with string keys and int values")
        void testMapStringToInt() {
            StructType schema = SchemaParser.parse("struct<data:map<string,int>>");

            assertThat(schema.size()).isEqualTo(1);
            assertThat(schema.fieldAt(0).name()).isEqualTo("data");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(MapType.class);

            MapType mapType = (MapType) schema.fieldAt(0).dataType();
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-016: Parse map with int keys and string values")
        void testMapIntToString() {
            StructType schema = SchemaParser.parse("struct<lookup:map<int,string>>");

            MapType mapType = (MapType) schema.fieldAt(0).dataType();
            assertThat(mapType.keyType()).isInstanceOf(IntegerType.class);
            assertThat(mapType.valueType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-017: Parse nested array (array of arrays)")
        void testNestedArray() {
            StructType schema = SchemaParser.parse("struct<matrix:array<array<int>>>");

            ArrayType outerArray = (ArrayType) schema.fieldAt(0).dataType();
            assertThat(outerArray.elementType()).isInstanceOf(ArrayType.class);

            ArrayType innerArray = (ArrayType) outerArray.elementType();
            assertThat(innerArray.elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-018: Parse map with array value")
        void testMapWithArrayValue() {
            StructType schema = SchemaParser.parse("struct<groups:map<string,array<int>>>");

            MapType mapType = (MapType) schema.fieldAt(0).dataType();
            assertThat(mapType.keyType()).isInstanceOf(StringType.class);
            assertThat(mapType.valueType()).isInstanceOf(ArrayType.class);

            ArrayType arrayType = (ArrayType) mapType.valueType();
            assertThat(arrayType.elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-019: Parse complex schema with mixed types")
        void testComplexMixedSchema() {
            StructType schema = SchemaParser.parse(
                "struct<id:long,name:string,tags:array<string>,metadata:map<string,int>>");

            assertThat(schema.size()).isEqualTo(4);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(LongType.class);
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class);
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(ArrayType.class);
            assertThat(schema.fieldAt(3).dataType()).isInstanceOf(MapType.class);
        }
    }

    // ==================== Decimal Type Parsing ====================

    @Nested
    @DisplayName("Decimal Type Parsing")
    class DecimalTypeParsing {

        @Test
        @DisplayName("TC-SCHEMA-PARSER-020: Parse decimal with precision and scale")
        void testDecimalWithPrecisionAndScale() {
            StructType schema = SchemaParser.parse("struct<amount:decimal(10,2)>");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(DecimalType.class);
            DecimalType decimalType = (DecimalType) schema.fieldAt(0).dataType();
            assertThat(decimalType.precision()).isEqualTo(10);
            assertThat(decimalType.scale()).isEqualTo(2);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-021: Parse decimal with precision only")
        void testDecimalWithPrecisionOnly() {
            StructType schema = SchemaParser.parse("struct<value:decimal(18)>");

            DecimalType decimalType = (DecimalType) schema.fieldAt(0).dataType();
            assertThat(decimalType.precision()).isEqualTo(18);
            assertThat(decimalType.scale()).isEqualTo(0);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-022: Parse numeric type (alias for decimal)")
        void testNumericType() {
            StructType schema = SchemaParser.parse("struct<price:numeric(15,4)>");

            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(DecimalType.class);
            DecimalType decimalType = (DecimalType) schema.fieldAt(0).dataType();
            assertThat(decimalType.precision()).isEqualTo(15);
            assertThat(decimalType.scale()).isEqualTo(4);
        }
    }

    // ==================== Edge Cases and Error Handling ====================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesAndErrorHandling {

        @Test
        @DisplayName("TC-SCHEMA-PARSER-023: Null schema string throws exception")
        void testNullSchemaString() {
            assertThatThrownBy(() -> SchemaParser.parse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-024: Empty schema string throws exception")
        void testEmptySchemaString() {
            assertThatThrownBy(() -> SchemaParser.parse(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null or empty");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-025: Invalid field definition throws exception")
        void testInvalidFieldDefinition() {
            assertThatThrownBy(() -> SchemaParser.parse("struct<invalid>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid field definition");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-026: Unsupported type throws exception")
        void testUnsupportedType() {
            assertThatThrownBy(() -> SchemaParser.parse("struct<col:unknowntype>"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported type");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-027: Invalid map type throws exception")
        void testInvalidMapType() {
            assertThatThrownBy(() -> SchemaParser.parse("struct<data:map<string>>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid map type");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-028: Case insensitive type parsing")
        void testCaseInsensitiveTypes() {
            // Uppercase should work
            StructType schema1 = SchemaParser.parse("struct<col:INT>");
            assertThat(schema1.fieldAt(0).dataType()).isInstanceOf(IntegerType.class);

            // Mixed case should work
            StructType schema2 = SchemaParser.parse("struct<col:String>");
            assertThat(schema2.fieldAt(0).dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-029: Whitespace handling")
        void testWhitespaceHandling() {
            // With spaces around struct wrapper
            StructType schema1 = SchemaParser.parse("  struct<id:int>  ");
            assertThat(schema1.size()).isEqualTo(1);

            // With spaces in field definitions
            StructType schema2 = SchemaParser.parse("struct< id : int , name : string >");
            assertThat(schema2.size()).isEqualTo(2);
            assertThat(schema2.fieldAt(0).name()).isEqualTo("id");
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-030: Parse without struct wrapper (fallback)")
        void testParseWithoutStructWrapper() {
            // Some Spark versions may send just the field list
            StructType schema = SchemaParser.parse("id:int,name:string");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
        }
    }

    // ==================== Spark Schema Format Compatibility ====================

    @Nested
    @DisplayName("Spark Schema Format Compatibility")
    class SparkSchemaFormatCompatibility {

        @Test
        @DisplayName("TC-SCHEMA-PARSER-031: Typical Spark empty DataFrame schema")
        void testTypicalSparkEmptyDataFrameSchema() {
            // This is the exact format Spark sends for:
            // schema = StructType([StructField("id", IntegerType(), False), StructField("name", StringType(), True)])
            // spark.createDataFrame([], schema)
            StructType schema = SchemaParser.parse("struct<id:int,name:string>");

            assertThat(schema.size()).isEqualTo(2);
            assertThat(schema.fieldAt(0).name()).isEqualTo("id");
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(IntegerType.class);
            assertThat(schema.fieldAt(1).name()).isEqualTo("name");
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-SCHEMA-PARSER-032: Schema with all common Spark types")
        void testSchemaWithCommonSparkTypes() {
            StructType schema = SchemaParser.parse(
                "struct<" +
                "byte_col:byte," +
                "short_col:short," +
                "int_col:int," +
                "long_col:long," +
                "float_col:float," +
                "double_col:double," +
                "string_col:string," +
                "bool_col:boolean," +
                "date_col:date," +
                "ts_col:timestamp" +
                ">");

            assertThat(schema.size()).isEqualTo(10);
            assertThat(schema.fieldAt(0).dataType()).isInstanceOf(ByteType.class);
            assertThat(schema.fieldAt(1).dataType()).isInstanceOf(ShortType.class);
            assertThat(schema.fieldAt(2).dataType()).isInstanceOf(IntegerType.class);
            assertThat(schema.fieldAt(3).dataType()).isInstanceOf(LongType.class);
            assertThat(schema.fieldAt(4).dataType()).isInstanceOf(FloatType.class);
            assertThat(schema.fieldAt(5).dataType()).isInstanceOf(DoubleType.class);
            assertThat(schema.fieldAt(6).dataType()).isInstanceOf(StringType.class);
            assertThat(schema.fieldAt(7).dataType()).isInstanceOf(BooleanType.class);
            assertThat(schema.fieldAt(8).dataType()).isInstanceOf(DateType.class);
            assertThat(schema.fieldAt(9).dataType()).isInstanceOf(TimestampType.class);
        }
    }
}
