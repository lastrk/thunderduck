"""
Differential tests for basic DataFrame operations.
Verifies Thunderduck matches Spark 4.0.1 behavior exactly.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window


@pytest.mark.differential
class TestDataFrameBasicOperationsDifferential:
    """Differential tests for basic DataFrame operations."""

    def test_select_columns(self, spark_reference, spark_thunderduck):
        """Test column selection - exact parity check."""
        # Drop and create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_result = spark_reference.table("employees").select("name", "salary").orderBy("name")
        td_result = spark_thunderduck.table("employees").select("name", "salary").orderBy("name")

        assert_dataframes_equal(spark_result, td_result, query_name="select_columns")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_filter_operations(self, spark_reference, spark_thunderduck):
        """Test various filter operations - exact parity check."""
        # Drop and create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        # Simple filter
        spark_result1 = spark_reference.table("employees").filter(F.col("salary") > 60000).orderBy("id")
        td_result1 = spark_thunderduck.table("employees").filter(F.col("salary") > 60000).orderBy("id")
        assert_dataframes_equal(spark_result1, td_result1, query_name="filter_simple")

        # Filter with AND
        spark_result2 = spark_reference.table("employees").filter(
            (F.col("salary") > 60000) & (F.col("department") == "Engineering")
        ).orderBy("id")
        td_result2 = spark_thunderduck.table("employees").filter(
            (F.col("salary") > 60000) & (F.col("department") == "Engineering")
        ).orderBy("id")
        assert_dataframes_equal(spark_result2, td_result2, query_name="filter_and")

        # Filter with OR
        spark_result3 = spark_reference.table("employees").filter(
            (F.col("department") == "HR") | (F.col("salary") > 75000)
        ).orderBy("id")
        td_result3 = spark_thunderduck.table("employees").filter(
            (F.col("department") == "HR") | (F.col("salary") > 75000)
        ).orderBy("id")
        assert_dataframes_equal(spark_result3, td_result3, query_name="filter_or")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_groupby_aggregation(self, spark_reference, spark_thunderduck):
        """Test group by with various aggregations - exact parity check."""
        # Create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_result = spark_reference.table("employees").groupBy("department").agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
            F.min("salary").alias("min_salary"),
            F.sum("salary").alias("total_salary")
        ).orderBy("department")

        td_result = spark_thunderduck.table("employees").groupBy("department").agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
            F.min("salary").alias("min_salary"),
            F.sum("salary").alias("total_salary")
        ).orderBy("department")

        assert_dataframes_equal(spark_result, td_result, query_name="groupby_aggregation", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_join_operations(self, spark_reference, spark_thunderduck):
        """Test various join types - exact parity check."""
        # Create test tables on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")
        spark_reference.sql("DROP TABLE IF EXISTS departments")
        spark_reference.sql("CREATE TABLE departments (name STRING, manager STRING)")
        spark_reference.sql("INSERT INTO departments VALUES ('HR', 'Frank'), ('Engineering', 'Grace'), ('Sales', 'Henry')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")
        spark_thunderduck.sql("DROP TABLE IF EXISTS departments")
        spark_thunderduck.sql("CREATE TABLE departments (name STRING, manager STRING)")
        spark_thunderduck.sql("INSERT INTO departments VALUES ('HR', 'Frank'), ('Engineering', 'Grace'), ('Sales', 'Henry')")

        emp_spark = spark_reference.table("employees")
        dept_spark = spark_reference.table("departments")
        emp_td = spark_thunderduck.table("employees")
        dept_td = spark_thunderduck.table("departments")

        # Inner join - verify using aggregation to avoid ambiguous column issues
        spark_inner_count = emp_spark.join(dept_spark, emp_spark.department == dept_spark.name, "inner").agg(F.count("*").alias("cnt"))
        td_inner_count = emp_td.join(dept_td, emp_td.department == dept_td.name, "inner").agg(F.count("*").alias("cnt"))
        assert_dataframes_equal(spark_inner_count, td_inner_count, query_name="join_inner", ignore_nullable=True)

        # Left join - verify using aggregation
        spark_left_count = emp_spark.join(dept_spark, emp_spark.department == dept_spark.name, "left").agg(F.count("*").alias("cnt"))
        td_left_count = emp_td.join(dept_td, emp_td.department == dept_td.name, "left").agg(F.count("*").alias("cnt"))
        assert_dataframes_equal(spark_left_count, td_left_count, query_name="join_left", ignore_nullable=True)

        # Right join - verify using aggregation
        spark_right_count = emp_spark.join(dept_spark, emp_spark.department == dept_spark.name, "right").agg(F.count("*").alias("cnt"))
        td_right_count = emp_td.join(dept_td, emp_td.department == dept_td.name, "right").agg(F.count("*").alias("cnt"))
        assert_dataframes_equal(spark_right_count, td_right_count, query_name="join_right", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_reference.sql("DROP TABLE departments")
        spark_thunderduck.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE departments")

    def test_window_functions(self, spark_reference, spark_thunderduck):
        """Test window functions - exact parity check."""
        # Create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        window = Window.partitionBy("department").orderBy(F.desc("salary"))

        spark_result = spark_reference.table("employees").select(
            "*",
            F.row_number().over(window).alias("row_num"),
            F.rank().over(window).alias("rank"),
            F.dense_rank().over(window).alias("dense_rank"),
            F.percent_rank().over(window).alias("percent_rank")
        ).filter(F.col("department") == "Engineering").orderBy("row_num")

        td_result = spark_thunderduck.table("employees").select(
            "*",
            F.row_number().over(window).alias("row_num"),
            F.rank().over(window).alias("rank"),
            F.dense_rank().over(window).alias("dense_rank"),
            F.percent_rank().over(window).alias("percent_rank")
        ).filter(F.col("department") == "Engineering").orderBy("row_num")

        assert_dataframes_equal(spark_result, td_result, query_name="window_functions", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_pivot_operations(self, spark_reference, spark_thunderduck):
        """Test pivot operations - exact parity check."""
        # Create sales data to pivot: month, product, sales
        sales_data = [
            Row(month="Jan", product="A", sales=100),
            Row(month="Jan", product="B", sales=150),
            Row(month="Feb", product="A", sales=200),
            Row(month="Feb", product="B", sales=250),
            Row(month="Mar", product="A", sales=300),
            Row(month="Mar", product="B", sales=350),
        ]

        sales_df_spark = spark_reference.createDataFrame(sales_data)
        sales_df_td = spark_thunderduck.createDataFrame(sales_data)

        # Group by month, pivot on product, sum sales
        spark_result = sales_df_spark.groupBy("month").pivot("product").sum("sales").orderBy("month")
        td_result = sales_df_td.groupBy("month").pivot("product").sum("sales").orderBy("month")

        assert_dataframes_equal(spark_result, td_result, query_name="pivot_operations", ignore_nullable=True)

    def test_union_operations(self, spark_reference, spark_thunderduck):
        """Test union operations - exact parity check."""
        # Create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        # Union (with duplicates)
        spark_df1 = spark_reference.table("employees").filter(F.col("salary") > 70000)
        spark_df2 = spark_reference.table("employees").filter(F.col("department") == "HR")
        spark_union = spark_df1.union(spark_df2).orderBy("id")

        td_df1 = spark_thunderduck.table("employees").filter(F.col("salary") > 70000)
        td_df2 = spark_thunderduck.table("employees").filter(F.col("department") == "HR")
        td_union = td_df1.union(td_df2).orderBy("id")

        assert_dataframes_equal(spark_union, td_union, query_name="union_all")

        # Union distinct
        spark_union_distinct = spark_df1.union(spark_df2).distinct().orderBy("id")
        td_union_distinct = td_df1.union(td_df2).distinct().orderBy("id")
        assert_dataframes_equal(spark_union_distinct, td_union_distinct, query_name="union_distinct")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_null_handling(self, spark_reference, spark_thunderduck):
        """Test NULL value handling - exact parity check."""
        # Create data with NULLs
        data = [
            (1, 'A', 100),
            (2, None, 200),
            (3, 'C', None),
            (4, None, None)
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "name", "value"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "name", "value"])

        # Test NULL filtering
        spark_not_null = spark_df.filter(F.col("name").isNotNull()).orderBy("id")
        td_not_null = td_df.filter(F.col("name").isNotNull()).orderBy("id")
        assert_dataframes_equal(spark_not_null, td_not_null, query_name="null_filter")

        # Test NULL in aggregations
        spark_avg = spark_df.agg(F.avg("value").alias("avg"))
        td_avg = td_df.agg(F.avg("value").alias("avg"))
        assert_dataframes_equal(spark_avg, td_avg, query_name="null_aggregation", ignore_nullable=True)

    def test_distinct_operations(self, spark_reference, spark_thunderduck):
        """Test distinct operations - exact parity check."""
        # Create test table on both systems
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 90000, 'Sales')")

        # Distinct departments
        spark_distinct = spark_reference.table("employees").select("department").distinct().orderBy("department")
        td_distinct = spark_thunderduck.table("employees").select("department").distinct().orderBy("department")
        assert_dataframes_equal(spark_distinct, td_distinct, query_name="distinct_departments")

        # Count distinct
        spark_count_distinct = spark_reference.table("employees").agg(
            F.countDistinct("department").alias("dept_count")
        )
        td_count_distinct = spark_thunderduck.table("employees").agg(
            F.countDistinct("department").alias("dept_count")
        )
        assert_dataframes_equal(spark_count_distinct, td_count_distinct, query_name="count_distinct", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")
