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
 * Differential tests for aggregate operations (10 tests).
 */
@DisplayName("Differential: Aggregate Operations")
public class AggregateTests extends DifferentialTestHarness {

    private final SyntheticDataGenerator dataGen = new SyntheticDataGenerator();

    @Test
    @DisplayName("01. COUNT(*)")
    void testCountStar() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_count_star";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("COUNT(*) as count");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery("SELECT COUNT(*) as count FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("COUNT_STAR", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("02. COUNT(column)")
    void testCountColumn() throws Exception {
        Dataset<Row> testData = dataGen.generateNullableDataset(spark, 20, 0.3);
        String tableName = "test_count_col";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("COUNT(name) as count");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery("SELECT COUNT(name) as count FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("COUNT_COLUMN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("03. SUM, AVG, MIN, MAX")
    void testBasicAggregates() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 20);
        String tableName = "test_basic_agg";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("SUM(value) as sum_val", "AVG(value) as avg_val", 
                            "MIN(value) as min_val", "MAX(value) as max_val");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT SUM(value) as sum_val, AVG(value) as avg_val, MIN(value) as min_val, MAX(value) as max_val FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("BASIC_AGGREGATES", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("04. GROUP BY single column")
    void testGroupBySingle() throws Exception {
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 30, 5);
        String tableName = "test_groupby_single";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .groupBy("category")
                .agg(org.apache.spark.sql.functions.sum("amount").as("total"))
                .orderBy("category");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT category, SUM(amount) as total FROM " + tableName + " GROUP BY category ORDER BY category")) {
                ComparisonResult result = executeAndCompare("GROUP_BY_SINGLE", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("05. GROUP BY multiple columns")
    void testGroupByMultiple() throws Exception {
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 30, 5);
        String tableName = "test_groupby_multi";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("category", "amount % 2 as parity", "COUNT(*) as count")
                .groupBy("category", "parity")
                .count()
                .orderBy("category", "parity");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT category, amount % 2 as parity, COUNT(*) as count FROM " + tableName + 
                    " GROUP BY category, parity ORDER BY category, parity")) {
                ComparisonResult result = executeAndCompare("GROUP_BY_MULTIPLE", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("06. HAVING clause")
    void testHaving() throws Exception {
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 30, 5);
        String tableName = "test_having";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .groupBy("category")
                .agg(org.apache.spark.sql.functions.sum("amount").as("total"))
                .where("total > 1000")
                .orderBy("category");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT category, SUM(amount) as total FROM " + tableName + 
                    " GROUP BY category HAVING total > 1000 ORDER BY category")) {
                ComparisonResult result = executeAndCompare("HAVING_CLAUSE", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("07. COUNT DISTINCT")
    void testCountDistinct() throws Exception {
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 30, 5);
        String tableName = "test_count_distinct";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("COUNT(DISTINCT category) as distinct_count");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT COUNT(DISTINCT category) as distinct_count FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("COUNT_DISTINCT", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("08. Aggregates with NULL handling")
    void testAggregatesWithNulls() throws Exception {
        Dataset<Row> testData = dataGen.generateNullableDataset(spark, 20, 0.3);
        String tableName = "test_agg_nulls";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .selectExpr("COUNT(*) as total", "COUNT(value) as non_null", 
                            "SUM(value) as sum_val", "AVG(value) as avg_val");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT COUNT(*) as total, COUNT(value) as non_null, SUM(value) as sum_val, AVG(value) as avg_val FROM " + tableName)) {
                ComparisonResult result = executeAndCompare("AGG_WITH_NULLS", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("09. GROUP BY with ORDER BY")
    void testGroupByOrderBy() throws Exception {
        Dataset<Row> testData = dataGen.generateGroupByDataset(spark, 30, 5);
        String tableName = "test_groupby_orderby";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .groupBy("category")
                .agg(org.apache.spark.sql.functions.sum("amount").as("total"))
                .orderBy(org.apache.spark.sql.functions.desc("total"));

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT category, SUM(amount) as total FROM " + tableName + 
                    " GROUP BY category ORDER BY total DESC")) {
                ComparisonResult result = executeAndCompare("GROUP_BY_ORDER_BY", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("10. Empty group aggregation")
    void testEmptyGroupAggregation() throws Exception {
        Dataset<Row> testData = dataGen.generateSimpleDataset(spark, 10);
        String tableName = "test_empty_group";
        String parquetPath = tempDir.resolve(tableName + ".parquet").toString();
        testData.write().mode("overwrite").parquet(parquetPath);

        Dataset<Row> sparkResult = spark.read().parquet(parquetPath)
                .where("id > 100")
                .selectExpr("COUNT(*) as count", "SUM(value) as sum_val");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                    tableName, parquetPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT COUNT(*) as count, SUM(value) as sum_val FROM " + tableName + " WHERE id > 100")) {
                ComparisonResult result = executeAndCompare("EMPTY_GROUP_AGG", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }
}
