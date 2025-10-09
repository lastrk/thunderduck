package com.spark2sql.test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for JOIN operations comparing Spark and our implementation.
 */
public class JoinOperationsTest extends DifferentialTestFramework {

    @Test
    public void testInnerJoin() {
        // Create employees dataset
        StructType employeeSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("emp_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("dept_id", DataTypes.IntegerType, true),
            DataTypes.createStructField("salary", DataTypes.DoubleType, false)
        ));

        List<Row> employees = Arrays.asList(
            RowFactory.create(1, "Alice", 10, 75000.0),
            RowFactory.create(2, "Bob", 20, 65000.0),
            RowFactory.create(3, "Charlie", 10, 80000.0),
            RowFactory.create(4, "David", null, 70000.0),
            RowFactory.create(5, "Eve", 30, 90000.0)
        );

        // Create departments dataset
        StructType deptSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("dept_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("dept_name", DataTypes.StringType, false),
            DataTypes.createStructField("location", DataTypes.StringType, false)
        ));

        List<Row> departments = Arrays.asList(
            RowFactory.create(10, "Engineering", "Building A"),
            RowFactory.create(20, "Sales", "Building B"),
            RowFactory.create(30, "HR", "Building C"),
            RowFactory.create(40, "Finance", "Building D")
        );

        DataFramePair empDf = createDataFrame(employees, employeeSchema);
        DataFramePair deptDf = createDataFrame(departments, deptSchema);

        // Perform inner join on both systems
        DataFramePair joined = new DataFramePair(
            empDf.spark.join(deptDf.spark, empDf.spark.col("dept_id").equalTo(deptDf.spark.col("dept_id"))),
            empDf.embedded.join(deptDf.embedded, empDf.embedded.col("dept_id").equalTo(deptDf.embedded.col("dept_id")))
        );

        // Select specific columns and compare
        DataFramePair result = joined.select("name", "dept_name");
        result.assertSameResults();

        // Verify count
        long sparkCount = result.spark.count();
        long embeddedCount = result.embedded.count();
        assertThat(sparkCount).isEqualTo(4L);
        assertThat(embeddedCount).isEqualTo(sparkCount);
    }

    @Test
    public void testLeftOuterJoin() {
        // Create test datasets
        StructType leftSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
        ));

        List<Row> leftData = Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(2, "B"),
            RowFactory.create(3, "C")
        );

        StructType rightSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("score", DataTypes.IntegerType, false)
        ));

        List<Row> rightData = Arrays.asList(
            RowFactory.create(1, 100),
            RowFactory.create(3, 300)
        );

        DataFramePair leftDf = createDataFrame(leftData, leftSchema);
        DataFramePair rightDf = createDataFrame(rightData, rightSchema);

        // Perform left outer join
        DataFramePair joined = new DataFramePair(
            leftDf.spark.join(rightDf.spark, leftDf.spark.col("id").equalTo(rightDf.spark.col("id")), "left"),
            leftDf.embedded.join(rightDf.embedded, leftDf.embedded.col("id").equalTo(rightDf.embedded.col("id")), "left")
        );

        // Compare results
        joined.assertSameResults();

        // Should have all 3 rows from left
        assertThat(joined.spark.count()).isEqualTo(3L);
        assertThat(joined.embedded.count()).isEqualTo(3L);
    }

    @Test
    public void testCrossJoin() {
        // Create small datasets for cross join
        StructType schema1 = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("letter", DataTypes.StringType, false)
        ));

        List<Row> letters = Arrays.asList(
            RowFactory.create("A"),
            RowFactory.create("B")
        );

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("number", DataTypes.IntegerType, false)
        ));

        List<Row> numbers = Arrays.asList(
            RowFactory.create(1),
            RowFactory.create(2),
            RowFactory.create(3)
        );

        DataFramePair lettersDf = createDataFrame(letters, schema1);
        DataFramePair numbersDf = createDataFrame(numbers, schema2);

        // Perform cross join
        DataFramePair crossed = new DataFramePair(
            lettersDf.spark.crossJoin(numbersDf.spark),
            lettersDf.embedded.crossJoin(numbersDf.embedded)
        );

        // Should produce cartesian product (2 x 3 = 6 rows)
        assertThat(crossed.spark.count()).isEqualTo(6L);
        assertThat(crossed.embedded.count()).isEqualTo(6L);

        // Compare sorted results
        DataFramePair sorted = crossed.orderBy("letter", "number");
        sorted.assertSameResults();
    }

    @Test
    public void testJoinWithComplexCondition() {
        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("category", DataTypes.StringType, false),
            DataTypes.createStructField("value", DataTypes.IntegerType, false)
        ));

        List<Row> left = Arrays.asList(
            RowFactory.create(1, "A", 100),
            RowFactory.create(2, "B", 200),
            RowFactory.create(3, "A", 300)
        );

        List<Row> right = Arrays.asList(
            RowFactory.create(1, "A", 150),
            RowFactory.create(2, "B", 250),
            RowFactory.create(3, "B", 350)
        );

        DataFramePair leftDf = createDataFrame(left, schema);
        DataFramePair rightDf = createDataFrame(right, schema);

        // Join with multiple conditions
        DataFramePair joined = new DataFramePair(
            leftDf.spark.join(rightDf.spark,
                leftDf.spark.col("id").equalTo(rightDf.spark.col("id"))
                    .and(leftDf.spark.col("category").equalTo(rightDf.spark.col("category")))),
            leftDf.embedded.join(rightDf.embedded,
                leftDf.embedded.col("id").equalTo(rightDf.embedded.col("id"))
                    .and(leftDf.embedded.col("category").equalTo(rightDf.embedded.col("category"))))
        );

        // Should match only rows with same id AND category
        assertThat(joined.spark.count()).isEqualTo(2L);
        assertThat(joined.embedded.count()).isEqualTo(2L);

        joined.assertSameResults();
    }

    @Test
    public void testJoinUsingColumn() {
        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false)
        ));

        List<Row> data1 = Arrays.asList(
            RowFactory.create(1, "Alice"),
            RowFactory.create(2, "Bob")
        );

        List<Row> data2 = Arrays.asList(
            RowFactory.create(1, "Charlie"),
            RowFactory.create(3, "David")
        );

        DataFramePair df1 = createDataFrame(data1, schema);
        DataFramePair df2 = createDataFrame(data2, schema);

        // Join using column name
        DataFramePair joined = new DataFramePair(
            df1.spark.join(df2.spark, "id"),
            df1.embedded.join(df2.embedded, "id")
        );

        // Should only match id=1
        assertThat(joined.spark.count()).isEqualTo(1L);
        assertThat(joined.embedded.count()).isEqualTo(1L);

        joined.assertSameResults();
    }

    @Test
    public void testJoinWithNulls() {
        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("value", DataTypes.StringType, true)
        ));

        List<Row> left = Arrays.asList(
            RowFactory.create(1, "A"),
            RowFactory.create(null, "B"),
            RowFactory.create(3, "C")
        );

        List<Row> right = Arrays.asList(
            RowFactory.create(1, "X"),
            RowFactory.create(2, "Y"),
            RowFactory.create(null, "Z")
        );

        DataFramePair leftDf = createDataFrame(left, schema);
        DataFramePair rightDf = createDataFrame(right, schema);

        // Inner join - nulls don't match
        DataFramePair joined = new DataFramePair(
            leftDf.spark.join(rightDf.spark, leftDf.spark.col("id").equalTo(rightDf.spark.col("id"))),
            leftDf.embedded.join(rightDf.embedded, leftDf.embedded.col("id").equalTo(rightDf.embedded.col("id")))
        );

        // Only id=1 should match (nulls don't join)
        assertThat(joined.spark.count()).isEqualTo(1L);
        assertThat(joined.embedded.count()).isEqualTo(1L);

        DataFramePair selected = new DataFramePair(
            joined.spark.select(leftDf.spark.col("value").as("left_value"), rightDf.spark.col("value").as("right_value")),
            joined.embedded.select(leftDf.embedded.col("value").as("left_value"), rightDf.embedded.col("value").as("right_value"))
        );

        selected.assertSameResults();
    }
}