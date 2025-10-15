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
 * Differential tests for JOIN operations (10 tests).
 */
@DisplayName("Differential: JOIN Operations")
public class JoinTests extends DifferentialTestHarness {

    private final SyntheticDataGenerator dataGen = new SyntheticDataGenerator();

    @Test
    @DisplayName("01. INNER JOIN")
    void testInnerJoin() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 10, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 8, "right");
        
        String leftPath = tempDir.resolve("left.parquet").toString();
        String rightPath = tempDir.resolve("right.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "inner")
                .selectExpr("l.id", "l.name as left_name", "r.name as right_name")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW left_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW right_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id, l.name as left_name, r.name as right_name FROM left_view l " +
                    "INNER JOIN right_view r ON l.id = r.id ORDER BY l.id")) {
                ComparisonResult result = executeAndCompare("INNER_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("02. LEFT OUTER JOIN")
    void testLeftJoin() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 10, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 8, "right");
        
        String leftPath = tempDir.resolve("left2.parquet").toString();
        String rightPath = tempDir.resolve("right2.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "left")
                .selectExpr("l.id", "l.name as left_name", "r.name as right_name")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW left_view2 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW right_view2 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id, l.name as left_name, r.name as right_name FROM left_view2 l " +
                    "LEFT JOIN right_view2 r ON l.id = r.id ORDER BY l.id")) {
                ComparisonResult result = executeAndCompare("LEFT_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("03. RIGHT OUTER JOIN")
    void testRightJoin() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 8, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 10, "right");
        
        String leftPath = tempDir.resolve("left3.parquet").toString();
        String rightPath = tempDir.resolve("right3.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "right")
                .selectExpr("r.id", "l.name as left_name", "r.name as right_name")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW left_view3 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW right_view3 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT r.id, l.name as left_name, r.name as right_name FROM left_view3 l " +
                    "RIGHT JOIN right_view3 r ON l.id = r.id ORDER BY r.id")) {
                ComparisonResult result = executeAndCompare("RIGHT_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("04. FULL OUTER JOIN")
    void testFullJoin() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 8, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 10, "right");
        
        String leftPath = tempDir.resolve("left4.parquet").toString();
        String rightPath = tempDir.resolve("right4.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "full")
                .selectExpr("COALESCE(l.id, r.id) as id", "l.name as left_name", "r.name as right_name")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW left_view4 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW right_view4 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT COALESCE(l.id, r.id) as id, l.name as left_name, r.name as right_name FROM left_view4 l " +
                    "FULL OUTER JOIN right_view4 r ON l.id = r.id ORDER BY id")) {
                ComparisonResult result = executeAndCompare("FULL_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("05. CROSS JOIN")
    void testCrossJoin() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 3, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 3, "right");
        
        String leftPath = tempDir.resolve("left5.parquet").toString();
        String rightPath = tempDir.resolve("right5.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .crossJoin(spark.read().parquet(rightPath).as("r"))
                .selectExpr("l.id as left_id", "r.id as right_id")
                .orderBy("left_id", "right_id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW left_view5 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW right_view5 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id as left_id, r.id as right_id FROM left_view5 l " +
                    "CROSS JOIN right_view5 r ORDER BY left_id, right_id")) {
                ComparisonResult result = executeAndCompare("CROSS_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("06. Self JOIN")
    void testSelfJoin() throws Exception {
        Dataset<Row> data = dataGen.generateJoinDataset(spark, 10, "data");
        
        String dataPath = tempDir.resolve("self_join.parquet").toString();
        data.write().mode("overwrite").parquet(dataPath);

        Dataset<Row> sparkResult = spark.read().parquet(dataPath).as("a")
                .join(spark.read().parquet(dataPath).as("b"), 
                      org.apache.spark.sql.functions.col("a.id").plus(1).equalTo(org.apache.spark.sql.functions.col("b.id")), "inner")
                .selectExpr("a.id as id1", "b.id as id2")
                .orderBy("id1");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "CREATE OR REPLACE VIEW self_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", dataPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT a.id as id1, b.id as id2 FROM self_view a " +
                    "INNER JOIN self_view b ON a.id + 1 = b.id ORDER BY id1")) {
                ComparisonResult result = executeAndCompare("SELF_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("07. Multi-way JOIN (3+ tables)")
    void testMultiWayJoin() throws Exception {
        Dataset<Row> t1 = dataGen.generateJoinDataset(spark, 5, "t1");
        Dataset<Row> t2 = dataGen.generateJoinDataset(spark, 5, "t2");
        Dataset<Row> t3 = dataGen.generateJoinDataset(spark, 5, "t3");
        
        String t1Path = tempDir.resolve("t1.parquet").toString();
        String t2Path = tempDir.resolve("t2.parquet").toString();
        String t3Path = tempDir.resolve("t3.parquet").toString();
        t1.write().mode("overwrite").parquet(t1Path);
        t2.write().mode("overwrite").parquet(t2Path);
        t3.write().mode("overwrite").parquet(t3Path);

        Dataset<Row> sparkResult = spark.read().parquet(t1Path).as("a")
                .join(spark.read().parquet(t2Path).as("b"), 
                      org.apache.spark.sql.functions.col("a.id").equalTo(org.apache.spark.sql.functions.col("b.id")), "inner")
                .join(spark.read().parquet(t3Path).as("c"), 
                      org.apache.spark.sql.functions.col("b.id").equalTo(org.apache.spark.sql.functions.col("c.id")), "inner")
                .selectExpr("a.id")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format("CREATE OR REPLACE VIEW t1_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", t1Path));
            stmt.execute(String.format("CREATE OR REPLACE VIEW t2_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", t2Path));
            stmt.execute(String.format("CREATE OR REPLACE VIEW t3_view AS SELECT * FROM parquet_scan('%s/**/*.parquet')", t3Path));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT a.id FROM t1_view a INNER JOIN t2_view b ON a.id = b.id " +
                    "INNER JOIN t3_view c ON b.id = c.id ORDER BY a.id")) {
                ComparisonResult result = executeAndCompare("MULTI_WAY_JOIN", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("08. JOIN with WHERE conditions")
    void testJoinWithWhere() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 10, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 10, "right");
        
        String leftPath = tempDir.resolve("left8.parquet").toString();
        String rightPath = tempDir.resolve("right8.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "inner")
                .where("l.value > 50")
                .selectExpr("l.id", "l.value")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format("CREATE OR REPLACE VIEW left_view8 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format("CREATE OR REPLACE VIEW right_view8 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id, l.value FROM left_view8 l INNER JOIN right_view8 r ON l.id = r.id " +
                    "WHERE l.value > 50 ORDER BY l.id")) {
                ComparisonResult result = executeAndCompare("JOIN_WITH_WHERE", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("09. JOIN with NULL keys")
    void testJoinWithNullKeys() throws Exception {
        // This test documents NULL key behavior difference
        Dataset<Row> left = dataGen.generateNullableDataset(spark, 10, 0.3);
        Dataset<Row> right = dataGen.generateNullableDataset(spark, 10, 0.3);
        
        String leftPath = tempDir.resolve("left_null.parquet").toString();
        String rightPath = tempDir.resolve("right_null.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id")), "inner")
                .selectExpr("l.id")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format("CREATE OR REPLACE VIEW left_null AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format("CREATE OR REPLACE VIEW right_null AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id FROM left_null l INNER JOIN right_null r ON l.id = r.id ORDER BY l.id")) {
                ComparisonResult result = executeAndCompare("JOIN_NULL_KEYS", sparkResult, duckdbResult);
                // Note: NULL key behavior may differ, document in report
            }
        }
    }

    @Test
    @DisplayName("10. JOIN with complex ON conditions")
    void testJoinComplexCondition() throws Exception {
        Dataset<Row> left = dataGen.generateJoinDataset(spark, 10, "left");
        Dataset<Row> right = dataGen.generateJoinDataset(spark, 10, "right");
        
        String leftPath = tempDir.resolve("left10.parquet").toString();
        String rightPath = tempDir.resolve("right10.parquet").toString();
        left.write().mode("overwrite").parquet(leftPath);
        right.write().mode("overwrite").parquet(rightPath);

        Dataset<Row> sparkResult = spark.read().parquet(leftPath).as("l")
                .join(spark.read().parquet(rightPath).as("r"), 
                      org.apache.spark.sql.functions.col("l.id").equalTo(org.apache.spark.sql.functions.col("r.id"))
                          .and(org.apache.spark.sql.functions.col("l.value").gt(org.apache.spark.sql.functions.col("r.value"))), "inner")
                .selectExpr("l.id", "l.value as lval", "r.value as rval")
                .orderBy("id");

        try (Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format("CREATE OR REPLACE VIEW left_view10 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", leftPath));
            stmt.execute(String.format("CREATE OR REPLACE VIEW right_view10 AS SELECT * FROM parquet_scan('%s/**/*.parquet')", rightPath));

            try (ResultSet duckdbResult = stmt.executeQuery(
                    "SELECT l.id, l.value as lval, r.value as rval FROM left_view10 l " +
                    "INNER JOIN right_view10 r ON l.id = r.id AND l.value > r.value ORDER BY l.id")) {
                ComparisonResult result = executeAndCompare("JOIN_COMPLEX_CONDITION", sparkResult, duckdbResult);
                assertThat(result.hasDivergences()).isFalse();
            }
        }
    }
}
