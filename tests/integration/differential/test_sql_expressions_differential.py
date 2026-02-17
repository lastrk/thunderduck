"""
Differential tests for SQL support: spark.sql() and SQL expression strings.
Verifies Thunderduck matches Spark 4.1.1 behavior exactly.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import functions as F
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestSQLExpressionsDifferential:
    """Differential tests for SQL expressions and spark.sql() queries."""

    def test_simple_sql_query(self, spark_reference, spark_thunderduck):
        """Test simple spark.sql() query - exact parity check."""
        spark_result = spark_reference.sql("SELECT 1 AS id, 'hello' AS message")
        td_result = spark_thunderduck.sql("SELECT 1 AS id, 'hello' AS message")

        assert_dataframes_equal(spark_result, td_result, query_name="simple_sql_query", ignore_nullable=True)

    def test_sql_with_temp_view(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with temp view - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_result = spark_reference.sql("SELECT COUNT(*) as count FROM employees")
        td_result = spark_thunderduck.sql("SELECT COUNT(*) as count FROM employees")

        assert_dataframes_equal(spark_result, td_result, query_name="sql_with_temp_view", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_sql_with_where(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with WHERE clause - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_result = spark_reference.sql("SELECT name, salary FROM employees WHERE salary > 70000 ORDER BY salary DESC")
        td_result = spark_thunderduck.sql("SELECT name, salary FROM employees WHERE salary > 70000 ORDER BY salary DESC")

        assert_dataframes_equal(spark_result, td_result, query_name="sql_with_where")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_sql_with_aggregation(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with aggregation - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_result = spark_reference.sql("""
            SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
            ORDER BY department
        """)
        td_result = spark_thunderduck.sql("""
            SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
            ORDER BY department
        """)

        assert_dataframes_equal(spark_result, td_result, query_name="sql_with_aggregation", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_sql_with_join(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with JOIN - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")
        spark_reference.sql("DROP TABLE IF EXISTS departments")
        spark_reference.sql("CREATE TABLE departments (name STRING, location STRING)")
        spark_reference.sql("INSERT INTO departments VALUES ('HR', 'Building C'), ('Engineering', 'Building A'), ('Sales', 'Building B')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")
        spark_thunderduck.sql("DROP TABLE IF EXISTS departments")
        spark_thunderduck.sql("CREATE TABLE departments (name STRING, location STRING)")
        spark_thunderduck.sql("INSERT INTO departments VALUES ('HR', 'Building C'), ('Engineering', 'Building A'), ('Sales', 'Building B')")

        spark_result = spark_reference.sql("""
            SELECT e.name, e.salary, d.location
            FROM employees e
            JOIN departments d ON e.department = d.name
            WHERE e.salary > 60000
            ORDER BY e.salary DESC
        """)
        td_result = spark_thunderduck.sql("""
            SELECT e.name, e.salary, d.location
            FROM employees e
            JOIN departments d ON e.department = d.name
            WHERE e.salary > 60000
            ORDER BY e.salary DESC
        """)

        assert_dataframes_equal(spark_result, td_result, query_name="sql_with_join")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_reference.sql("DROP TABLE departments")
        spark_thunderduck.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE departments")

    def test_filter_with_sql_expression_string(self, spark_reference, spark_thunderduck):
        """Test df.filter() with SQL expression string - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        # Test simple comparison
        spark_df = spark_reference.table("employees")
        td_df = spark_thunderduck.table("employees")

        spark_result1 = spark_df.filter("salary > 70000").agg(F.count("*").alias("cnt"))
        td_result1 = td_df.filter("salary > 70000").agg(F.count("*").alias("cnt"))
        assert_dataframes_equal(spark_result1, td_result1, query_name="filter_simple", ignore_nullable=True)

        # Test complex expression
        spark_result2 = spark_df.filter("salary > 60000 AND department = 'Engineering'").agg(F.count("*").alias("cnt"))
        td_result2 = td_df.filter("salary > 60000 AND department = 'Engineering'").agg(F.count("*").alias("cnt"))
        assert_dataframes_equal(spark_result2, td_result2, query_name="filter_complex", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_selectExpr_with_sql_expressions(self, spark_reference, spark_thunderduck):
        """Test df.selectExpr() with SQL expressions - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_df = spark_reference.table("employees")
        td_df = spark_thunderduck.table("employees")

        # Test arithmetic expression - cast to double to avoid decimal precision differences
        spark_result = spark_df.selectExpr("name", "salary", "CAST(salary * 1.1 AS DOUBLE) as salary_with_raise").orderBy("name")
        td_result = td_df.selectExpr("name", "salary", "CAST(salary * 1.1 AS DOUBLE) as salary_with_raise").orderBy("name")

        assert_dataframes_equal(spark_result, td_result, query_name="selectExpr_arithmetic", epsilon=0.01, ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_selectExpr_with_sql_functions(self, spark_reference, spark_thunderduck):
        """Test df.selectExpr() with SQL functions - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_df = spark_reference.table("employees")
        td_df = spark_thunderduck.table("employees")

        # Test string functions
        spark_result = spark_df.selectExpr("id", "upper(name) as upper_name", "lower(department) as lower_dept").orderBy("id")
        td_result = td_df.selectExpr("id", "upper(name) as upper_name", "lower(department) as lower_dept").orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="selectExpr_string_functions")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_sql_case_expression(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with CASE expression - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_result = spark_reference.sql("""
            SELECT name, salary,
                   CASE
                       WHEN salary >= 80000 THEN 'high'
                       WHEN salary >= 70000 THEN 'medium'
                       ELSE 'low'
                   END as salary_band
            FROM employees
            ORDER BY salary DESC
        """)
        td_result = spark_thunderduck.sql("""
            SELECT name, salary,
                   CASE
                       WHEN salary >= 80000 THEN 'high'
                       WHEN salary >= 70000 THEN 'medium'
                       ELSE 'low'
                   END as salary_band
            FROM employees
            ORDER BY salary DESC
        """)

        assert_dataframes_equal(spark_result, td_result, query_name="sql_case_expression", ignore_nullable=True)

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_sql_with_subquery(self, spark_reference, spark_thunderduck):
        """Test spark.sql() with subquery - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_result = spark_reference.sql("""
            SELECT name, salary
            FROM employees
            WHERE salary > (SELECT AVG(salary) FROM employees)
            ORDER BY salary DESC
        """)
        td_result = spark_thunderduck.sql("""
            SELECT name, salary
            FROM employees
            WHERE salary > (SELECT AVG(salary) FROM employees)
            ORDER BY salary DESC
        """)

        assert_dataframes_equal(spark_result, td_result, query_name="sql_with_subquery")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")

    def test_combined_sql_and_dataframe_api(self, spark_reference, spark_thunderduck):
        """Test combining spark.sql() with DataFrame operations - exact parity check."""
        # Create test data
        spark_reference.sql("DROP TABLE IF EXISTS employees")
        spark_reference.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_reference.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        spark_thunderduck.sql("DROP TABLE IF EXISTS employees")
        spark_thunderduck.sql("CREATE TABLE employees (id INT, name STRING, salary INT, department STRING)")
        spark_thunderduck.sql("INSERT INTO employees VALUES (1, 'Alice', 50000, 'HR'), (2, 'Bob', 80000, 'Engineering'), (3, 'Charlie', 70000, 'Engineering'), (4, 'Diana', 75000, 'Engineering'), (5, 'Eve', 60000, 'Sales')")

        # Start with SQL
        spark_df = spark_reference.sql("SELECT * FROM employees WHERE department = 'Engineering'")
        td_df = spark_thunderduck.sql("SELECT * FROM employees WHERE department = 'Engineering'")

        # Continue with DataFrame API using SQL expression
        spark_result = spark_df.filter("salary >= 75000").selectExpr("name", "salary * 12 as annual_salary").orderBy("name")
        td_result = td_df.filter("salary >= 75000").selectExpr("name", "salary * 12 as annual_salary").orderBy("name")

        assert_dataframes_equal(spark_result, td_result, query_name="combined_sql_dataframe")

        # Cleanup
        spark_reference.sql("DROP TABLE employees")
        spark_thunderduck.sql("DROP TABLE employees")
