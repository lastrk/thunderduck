package com.catalyst2sql.differential.tests;

import com.catalyst2sql.differential.DifferentialTestHarness;
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
 * Differential tests for basic SELECT operations (10 tests).
 *
 * <p>Tests cover:
 * - Simple SELECT *
 * - SELECT with column projection
 * - SELECT with column aliases
 * - SELECT DISTINCT
 * - SELECT with literals
 * - SELECT with arithmetic expressions
 * - SELECT with LIMIT
 * - SELECT with ORDER BY
 * - SELECT with CAST
 * - SELECT with CASE WHEN
 */
@DisplayName("Differential: Basic SELECT Operations")
public class BasicSelectTests extends DifferentialTestHarness {

    private final SyntheticDataGenerator dataGen = new SyntheticDataGenerator();

    @Test
    @DisplayName("01. Simple SELECT * from table")
    void testSimpleSelectStar() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_select_star";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Execute on both engines
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath);

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery("SELECT * FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_STAR", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("02. SELECT with column projection")
    void testSelectWithProjection() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_projection";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select specific columns
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).select("id", "name");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery("SELECT id, name FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_PROJECTION", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("03. SELECT with column aliases")
    void testSelectWithAliases() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_aliases";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with aliases
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id AS identifier", "name AS full_name", "value AS amount");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id AS identifier, name AS full_name, value AS amount FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_ALIASES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("04. SELECT DISTINCT")
    void testSelectDistinct() throws Exception {
        // Given: Test data with duplicates
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 20, 5);
        String tableName = "test_distinct";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select distinct categories
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .select("category").distinct().orderBy("category");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT DISTINCT category FROM " + tableName + " ORDER BY category")) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_DISTINCT", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("05. SELECT with literals")
    void testSelectWithLiterals() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_literals";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with literals
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", "42 AS constant", "'hello' AS greeting");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, 42 AS constant, 'hello' AS greeting FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_LITERALS", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("06. SELECT with arithmetic expressions")
    void testSelectWithArithmetic() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_arithmetic";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with arithmetic
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", "value * 2 AS doubled", "value + 10 AS added");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, value * 2 AS doubled, value + 10 AS added FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_ARITHMETIC", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("07. SELECT with LIMIT")
    void testSelectWithLimit() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_limit";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with LIMIT
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .orderBy("id").limit(5);

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " ORDER BY id LIMIT 5")) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_LIMIT", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("08. SELECT with ORDER BY single column")
    void testSelectWithOrderBy() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_orderby";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with ORDER BY
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .orderBy(org.apache.spark.sql.functions.desc("value"));

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " ORDER BY value DESC")) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_ORDERBY", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("09. SELECT with CAST operations")
    void testSelectWithCast() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_cast";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with CAST
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", "CAST(value AS INT) AS value_int", "CAST(id AS STRING) AS id_str");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, CAST(value AS INTEGER) AS value_int, CAST(id AS VARCHAR) AS id_str FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_CAST", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("10. SELECT with CASE WHEN")
    void testSelectWithCaseWhen() throws Exception {
        // Given: Test data
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_case";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        // When: Select with CASE WHEN
        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("id", "CASE WHEN value > 50 THEN 'high' ELSE 'low' END AS category");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT id, CASE WHEN value > 50 THEN 'high' ELSE 'low' END AS category FROM " + tableName)) {
                // Then: Compare results
                ComparisonResult result = executeAndCompare("SELECT_CASE_WHEN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }
}
