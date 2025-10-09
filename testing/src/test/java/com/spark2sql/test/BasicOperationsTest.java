package com.spark2sql.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for basic DataFrame operations comparing Spark and our implementation.
 */
public class BasicOperationsTest extends DifferentialTestFramework {

    @Test
    public void testBasicSelect() {
        // Create test data
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "Alice", 25, 1000.50),
            RowFactory.create(2, "Bob", 30, 2000.75),
            RowFactory.create(3, "Charlie", 35, 3000.00),
            RowFactory.create(4, "David", 28, 1500.25)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false),
            DataTypes.createStructField("salary", DataTypes.DoubleType, false)
        ));

        // Create DataFrames in both systems
        DataFramePair df = createDataFrame(testData, schema);

        // Test simple select
        DataFramePair result = df.select("id", "name");
        result.assertSameResults();

        // Test select with different order
        DataFramePair result2 = df.select("name", "id", "age");
        result2.assertSameResults();
    }

    @Test
    public void testFilter() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "Alice", 25),
            RowFactory.create(2, "Bob", 30),
            RowFactory.create(3, "Charlie", 35),
            RowFactory.create(4, "David", 28),
            RowFactory.create(5, "Eve", 22)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test simple filter
        DataFramePair result = df.filter("age > 25");
        result.assertSameResults(false);  // Order might differ

        // Test complex filter
        DataFramePair result2 = df.filter("age >= 25 AND age <= 30");
        result2.assertSameResults(false);

        // Test filter with string comparison
        DataFramePair result3 = df.filter("name = 'Alice'");
        result3.assertSameResults();
    }

    @Test
    public void testLimit() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(2, "B"),
            RowFactory.create(3, "C"),
            RowFactory.create(4, "D"),
            RowFactory.create(5, "E")
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test limit
        DataFramePair result = df.limit(3);
        result.assertSameResults(true);  // Order matters for limit

        // Test limit with 0
        DataFramePair result2 = df.limit(0);
        result2.assertSameResults();

        // Test limit larger than data
        DataFramePair result3 = df.limit(10);
        result3.assertSameResults();
    }

    @Test
    public void testArithmeticOperations() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(10, 3),
            RowFactory.create(20, 4),
            RowFactory.create(15, 5),
            RowFactory.create(25, 0),  // Division by zero case
            RowFactory.create(30, 7)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, false),
            DataTypes.createStructField("b", DataTypes.IntegerType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test addition
        df.spark.selectExpr("a + b as sum").show();
        df.embedded.selectExpr("a + b as sum").show();

        assertSameResults(
            df.spark.selectExpr("a + b as sum"),
            df.embedded.selectExpr("a + b as sum")
        );

        // Test subtraction
        assertSameResults(
            df.spark.selectExpr("a - b as diff"),
            df.embedded.selectExpr("a - b as diff")
        );

        // Test multiplication
        assertSameResults(
            df.spark.selectExpr("a * b as product"),
            df.embedded.selectExpr("a * b as product")
        );

        // Test division (including division by zero)
        assertSameResults(
            df.spark.selectExpr("a / b as quotient"),
            df.embedded.selectExpr("a / b as quotient")
        );
    }

    @Test
    public void testIntegerDivisionSemantics() {
        // Critical test for Spark's integer division semantics (truncation, not floor)
        List<Row> testData = Arrays.asList(
            RowFactory.create(10, 3),    // 10/3 = 3
            RowFactory.create(-10, 3),   // -10/3 = -3 (not -4)
            RowFactory.create(10, -3),   // 10/-3 = -3 (not -4)
            RowFactory.create(-10, -3),  // -10/-3 = 3
            RowFactory.create(7, 2),     // 7/2 = 3
            RowFactory.create(-7, 2),    // -7/2 = -3 (not -4)
            RowFactory.create(0, 5),     // 0/5 = 0
            RowFactory.create(5, 0)      // 5/0 = null (division by zero)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, false),
            DataTypes.createStructField("b", DataTypes.IntegerType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test integer division
        assertSameResults(
            df.spark.selectExpr("a / b as result"),
            df.embedded.selectExpr("a / b as result")
        );

        // Test modulo operation
        assertSameResults(
            df.spark.selectExpr("a % b as remainder"),
            df.embedded.selectExpr("a % b as remainder")
        );
    }

    @Test
    public void testNullHandling() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, null, "A"),
            RowFactory.create(2, 10, null),
            RowFactory.create(null, 20, "C"),
            RowFactory.create(null, null, null),
            RowFactory.create(5, 50, "E")
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, true),
            DataTypes.createStructField("b", DataTypes.IntegerType, true),
            DataTypes.createStructField("c", DataTypes.StringType, true)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test null propagation in arithmetic
        assertSameResults(
            df.spark.selectExpr("a + b as sum"),
            df.embedded.selectExpr("a + b as sum")
        );

        // Test null checks
        assertSameResults(
            df.spark.selectExpr("a IS NULL as a_is_null", "b IS NOT NULL as b_not_null"),
            df.embedded.selectExpr("a IS NULL as a_is_null", "b IS NOT NULL as b_not_null")
        );

        // Test null in comparisons
        assertSameResults(
            df.spark.selectExpr("a = b as equal", "a > b as greater"),
            df.embedded.selectExpr("a = b as equal", "a > b as greater")
        );

        // Test filter with null
        assertSameResults(
            df.spark.filter("a IS NOT NULL"),
            df.embedded.filter("a IS NOT NULL"),
            false
        );
    }

    @Test
    public void testDecimalPrecision() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(new BigDecimal("123.45"), new BigDecimal("67.89")),
            RowFactory.create(new BigDecimal("999.99"), new BigDecimal("0.01")),
            RowFactory.create(new BigDecimal("-123.45"), new BigDecimal("123.45")),
            RowFactory.create(new BigDecimal("0.00"), new BigDecimal("100.00"))
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("amount1", DataTypes.createDecimalType(10, 2), false),
            DataTypes.createStructField("amount2", DataTypes.createDecimalType(10, 2), false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test decimal arithmetic
        assertSameResults(
            df.spark.selectExpr("amount1 + amount2 as sum"),
            df.embedded.selectExpr("amount1 + amount2 as sum")
        );

        assertSameResults(
            df.spark.selectExpr("amount1 - amount2 as diff"),
            df.embedded.selectExpr("amount1 - amount2 as diff")
        );

        assertSameResults(
            df.spark.selectExpr("amount1 * amount2 as product"),
            df.embedded.selectExpr("amount1 * amount2 as product")
        );
    }

    @Test
    public void testComplexExpressions() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, 10, 100),
            RowFactory.create(2, 20, 200),
            RowFactory.create(3, 30, 300),
            RowFactory.create(4, 40, 400),
            RowFactory.create(5, 50, 500)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, false),
            DataTypes.createStructField("b", DataTypes.IntegerType, false),
            DataTypes.createStructField("c", DataTypes.IntegerType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test complex arithmetic expression
        assertSameResults(
            df.spark.selectExpr("(a + b) * c as result1", "a + (b * c) as result2"),
            df.embedded.selectExpr("(a + b) * c as result1", "a + (b * c) as result2")
        );

        // Test CASE WHEN
        assertSameResults(
            df.spark.selectExpr(
                "CASE WHEN a < 3 THEN 'small' WHEN a < 5 THEN 'medium' ELSE 'large' END as category"
            ),
            df.embedded.selectExpr(
                "CASE WHEN a < 3 THEN 'small' WHEN a < 5 THEN 'medium' ELSE 'large' END as category"
            )
        );

        // Test complex filter with multiple conditions
        assertSameResults(
            df.spark.filter("(a + b > 20) AND (c < 400)"),
            df.embedded.filter("(a + b > 20) AND (c < 400)"),
            false
        );
    }

    @Test
    public void testCount() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(2, "B"),
            RowFactory.create(3, "C")
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test count
        long sparkCount = df.spark.count();
        long embeddedCount = df.embedded.count();

        assertThat(embeddedCount)
            .as("Count mismatch")
            .isEqualTo(sparkCount);

        // Test count after filter
        long sparkFilteredCount = df.spark.filter("id > 1").count();
        long embeddedFilteredCount = df.embedded.filter("id > 1").count();

        assertThat(embeddedFilteredCount)
            .as("Filtered count mismatch")
            .isEqualTo(sparkFilteredCount);
    }

    @Test
    public void testDistinct() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(2, "B"),
            RowFactory.create(1, "A"),  // Duplicate
            RowFactory.create(3, "C"),
            RowFactory.create(2, "B")   // Duplicate
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test distinct
        assertSameResults(
            df.spark.distinct(),
            df.embedded.distinct(),
            false  // Order doesn't matter
        );
    }

    @Test
    public void testOrderBy() {
        List<Row> testData = Arrays.asList(
            RowFactory.create(3, "Charlie", 35),
            RowFactory.create(1, "Alice", 25),
            RowFactory.create(4, "David", 28),
            RowFactory.create(2, "Bob", 30)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false)
        ));

        DataFramePair df = createDataFrame(testData, schema);

        // Test order by single column
        assertSameResults(
            df.spark.orderBy("id"),
            df.embedded.orderBy("id"),
            true  // Order matters
        );

        // Test order by multiple columns
        assertSameResults(
            df.spark.orderBy("age", "name"),
            df.embedded.orderBy("age", "name"),
            true
        );

        // Test descending order
        assertSameResults(
            df.spark.orderBy(df.spark.col("age").desc()),
            df.embedded.orderBy(df.embedded.col("age").desc()),
            true
        );
    }
}