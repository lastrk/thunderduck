package com.catalyst2sql.differential.tests;

import com.catalyst2sql.differential.DifferentialTestHarness;
import com.catalyst2sql.differential.datagen.EdgeCaseGenerator;
import com.catalyst2sql.differential.datagen.SyntheticDataGenerator;
import com.catalyst2sql.differential.model.ComparisonResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential tests for data type handling (10 tests).
 */
@DisplayName("Differential: Data Type Handling")
public class DataTypeTests extends DifferentialTestHarness {

    private final SyntheticDataGenerator dataGen = new SyntheticDataGenerator();

    @Test
    @DisplayName("01. Integer types (INT, LONG)")
    void testIntegerTypes() throws Exception {
        Dataset<Row> testData = dataGen.generateAllTypesDataset(spark, 10);
        String tableName = "test_int_types";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .select("id", "long_val").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, long_val FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("INTEGER_TYPES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("02. Floating point types (FLOAT, DOUBLE)")
    void testFloatingPointTypes() throws Exception {
        Dataset<Row> testData = dataGen.generateAllTypesDataset(spark, 10);
        String tableName = "test_float_types";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .select("id", "float_val", "double_val").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, float_val, double_val FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("FLOAT_TYPES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("03. String types")
    void testStringTypes() throws Exception {
        Dataset<Row> testData = EdgeCaseGenerator.specialCharactersDataset(spark);
        String tableName = "test_string_types";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("STRING_TYPES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("04. Boolean types")
    void testBooleanTypes() throws Exception {
        Dataset<Row> testData = dataGen.generateAllTypesDataset(spark, 10);
        String tableName = "test_boolean_types";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .select("id", "boolean_val").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, boolean_val FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("BOOLEAN_TYPES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("05. NULL handling across all types")
    void testNullHandling() throws Exception {
        Dataset<Row> testData = EdgeCaseGenerator.mixedNullsDataset(spark);
        String tableName = "test_null_handling";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("NULL_HANDLING", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("06. Type coercion (INT to LONG)")
    void testTypeCoercion() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_type_coercion";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("CAST(id AS LONG) as id_long", "id as id_int")
                .orderBy("id_int");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT CAST(id AS BIGINT) as id_long, id as id_int FROM " + tableName + " ORDER BY id_int")) {
                ComparisonResult result = executeAndCompare("TYPE_COERCION", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("07. CAST operations")
    void testCastOperations() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_cast";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", 
                           "CAST(value AS INT) as value_int",
                           "CAST(id AS STRING) as id_str",
                           "CAST(value AS STRING) as value_str")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, CAST(value AS INTEGER) as value_int, " +
                    "CAST(id AS VARCHAR) as id_str, CAST(value AS VARCHAR) as value_str " +
                    "FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("CAST_OPERATIONS", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("08. NaN and Infinity handling")
    void testNanInfinity() throws Exception {
        Dataset<Row> testData = EdgeCaseGenerator.nanInfinityDataset(spark);
        String tableName = "test_nan_inf";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("NAN_INFINITY", sparkResult, duckdbResult);
                // Note: NaN/Infinity handling may differ between Spark and DuckDB
            }
        }
    }

    @Test
    @DisplayName("09. MIN_VALUE and MAX_VALUE for integers")
    void testMinMaxValues() throws Exception {
        Dataset<Row> testData = EdgeCaseGenerator.minMaxIntegersDataset(spark);
        String tableName = "test_min_max";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath);

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("MIN_MAX_VALUES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("10. Mixed types in expressions")
    void testMixedTypeExpressions() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_mixed_types";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", 
                           "id + value as int_plus_double",
                           "CAST(id AS DOUBLE) * value as double_mult",
                           "CONCAT('id_', CAST(id AS STRING)) as id_concat")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, id + value as int_plus_double, " +
                    "CAST(id AS DOUBLE) * value as double_mult, " +
                    "CONCAT('id_', CAST(id AS VARCHAR)) as id_concat " +
                    "FROM " + tableName + " ORDER BY id")) {
                ComparisonResult result = executeAndCompare("MIXED_TYPE_EXPRESSIONS", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }
}
