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
 * Differential tests for WHERE clause and filter operations (10 tests).
 */
@DisplayName("Differential: Filter Operations")
public class FilterTests extends DifferentialTestHarness {

    private final SyntheticDataGenerator dataGen = new SyntheticDataGenerator();

    @Test
    @DisplayName("01. WHERE with equality (=)")
    void testWhereEquality() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_eq";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).where("id = 5");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 5")) {
                ComparisonResult result = executeAndCompare("WHERE_EQUALITY", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("02. WHERE with inequality (<>)")
    void testWhereInequality() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_neq";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath).where("id <> 5").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE id <> 5 ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_INEQUALITY", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("03. WHERE with comparison (<, >, <=, >=)")
    void testWhereComparison() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_comp";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("value > 50 AND value <= 80").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE value > 50 AND value <= 80 ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_COMPARISON", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("04. WHERE with LIKE pattern matching")
    void testWhereLike() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_like";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("name LIKE 'name_1%'").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE name LIKE 'name_1%' ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_LIKE", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("05. WHERE with IN clause")
    void testWhereIn() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_in";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("id IN (1, 5, 10, 15)").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE id IN (1, 5, 10, 15) ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_IN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("06. WHERE with NOT IN")
    void testWhereNotIn() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_where_not_in";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("id NOT IN (1, 2, 3)").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE id NOT IN (1, 2, 3) ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_NOT_IN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("07. WHERE with IS NULL / IS NOT NULL")
    void testWhereNull() throws Exception {
        Dataset<Row> testData = dataGen.generateNullableDataset(spark, 20, 0.3);
        String tableName = "test_where_null";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("name IS NULL").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE name IS NULL ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_NULL", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("08. WHERE with AND/OR combinations")
    void testWhereAndOr() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_and_or";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("(id > 5 AND value < 50) OR (id < 3)").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE (id > 5 AND value < 50) OR (id < 3) ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_AND_OR", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("09. WHERE with BETWEEN")
    void testWhereBetween() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_between";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("value BETWEEN 30 AND 70").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE value BETWEEN 30 AND 70 ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_BETWEEN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("10. WHERE with complex nested conditions")
    void testWhereComplexNested() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_where_complex";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("((id > 3 AND id < 10) OR (id > 15)) AND value > 25").orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT * FROM " + tableName + " WHERE ((id > 3 AND id < 10) OR (id > 15)) AND value > 25 ORDER BY id")) {
                ComparisonResult result = executeAndCompare("WHERE_COMPLEX", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }
}
