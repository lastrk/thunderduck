"""
Differential tests for JOIN operations with ambiguous column names.
Verifies Thunderduck matches Spark 4.1.1 behavior exactly.

These tests verify that Thunderduck correctly handles joins where both
DataFrames have columns with the same name, using DataFrame column references
(df1["id"] == df2["id"]) rather than string column names.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import functions as F
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestJoinAdvancedDifferential:
    """Differential tests for advanced JOIN operations with ambiguous columns."""

    def test_join_same_column_name_both_tables(self, spark_reference, spark_thunderduck):
        """Test join where both tables have an 'id' column - exact parity check.

        This is the core ambiguity case that requires plan_id resolution.
        """
        # Create two DataFrames with same column name 'id'
        employees_data = [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 300),
        ]
        departments_data = [
            (100, "Engineering"),
            (200, "Marketing"),
            (400, "HR"),  # No matching employee
        ]

        employees_spark = spark_reference.createDataFrame(employees_data, ["id", "name", "dept_id"])
        departments_spark = spark_reference.createDataFrame(departments_data, ["id", "dept_name"])

        employees_td = spark_thunderduck.createDataFrame(employees_data, ["id", "name", "dept_id"])
        departments_td = spark_thunderduck.createDataFrame(departments_data, ["id", "dept_name"])

        # Join using DataFrame column references
        spark_joined = employees_spark.join(
            departments_spark,
            employees_spark["dept_id"] == departments_spark["id"],
            "inner"
        )
        td_joined = employees_td.join(
            departments_td,
            employees_td["dept_id"] == departments_td["id"],
            "inner"
        )

        # Verify the join produced correct results
        spark_result = spark_joined.select(
            employees_spark["name"],
            departments_spark["dept_name"]
        ).orderBy("name")

        td_result = td_joined.select(
            employees_td["name"],
            departments_td["dept_name"]
        ).orderBy("name")

        assert_dataframes_equal(spark_result, td_result, query_name="join_same_column_name")

    def test_join_with_select_after_ambiguous_join(self, spark_reference, spark_thunderduck):
        """Test selecting specific columns after a join with ambiguous column names - exact parity check."""
        data1 = [(1, "A"), (2, "B")]
        data2 = [(1, "X"), (2, "Y")]

        df1_spark = spark_reference.createDataFrame(data1, ["id", "value"])
        df2_spark = spark_reference.createDataFrame(data2, ["id", "data"])

        df1_td = spark_thunderduck.createDataFrame(data1, ["id", "value"])
        df2_td = spark_thunderduck.createDataFrame(data2, ["id", "data"])

        # Join on the same-named column using explicit references
        joined_spark = df1_spark.join(df2_spark, df1_spark["id"] == df2_spark["id"], "inner")
        joined_td = df1_td.join(df2_td, df1_td["id"] == df2_td["id"], "inner")

        # Select specific columns including the ambiguous 'id' from df1
        spark_result = joined_spark.select(
            df1_spark["id"].alias("id1"),
            df1_spark["value"],
            df2_spark["id"].alias("id2"),
            df2_spark["data"]
        ).orderBy("id1")

        td_result = joined_td.select(
            df1_td["id"].alias("id1"),
            df1_td["value"],
            df2_td["id"].alias("id2"),
            df2_td["data"]
        ).orderBy("id1")

        assert_dataframes_equal(spark_result, td_result, query_name="join_select_ambiguous")

    def test_self_join_with_filter(self, spark_reference, spark_thunderduck):
        """Test self-join where the same DataFrame is joined with a filtered version - exact parity check.

        Note: This test verifies row count rather than full data comparison due to
        Thunderduck's current limitations with DataFrame aliases in self-joins.
        """
        data = [
            (1, "Alice", 50000),
            (2, "Bob", 75000),
            (3, "Charlie", 60000),
            (4, "Diana", 90000),
        ]

        data_spark = spark_reference.createDataFrame(data, ["id", "name", "salary"])
        data_td = spark_thunderduck.createDataFrame(data, ["id", "name", "salary"])

        # Self-join: find employees with lower salary than high earners (>= 70000)
        # Create temporary tables to work around alias limitations
        data_spark.createOrReplaceTempView("employees_spark")
        data_td.createOrReplaceTempView("employees_td")

        # Use SQL to perform self-join which works better than DataFrame aliases
        spark_result = spark_reference.sql("""
            SELECT COUNT(*) as cnt
            FROM employees_spark e1
            JOIN employees_spark e2 ON e1.salary < e2.salary
            WHERE e2.salary >= 70000
        """)

        td_result = spark_thunderduck.sql("""
            SELECT COUNT(*) as cnt
            FROM employees_td e1
            JOIN employees_td e2 ON e1.salary < e2.salary
            WHERE e2.salary >= 70000
        """)

        assert_dataframes_equal(spark_result, td_result, query_name="self_join_filter", ignore_nullable=True)

    def test_join_complex_condition_with_ambiguous_columns(self, spark_reference, spark_thunderduck):
        """Test join with complex condition (AND/OR) involving ambiguous columns - exact parity check."""
        orders_data = [
            (1, 100, "2024-01-01"),
            (2, 200, "2024-01-15"),
            (3, 100, "2024-02-01"),
        ]
        customers_data = [
            (100, "Alice", "premium"),
            (200, "Bob", "standard"),
            (300, "Charlie", "premium"),
        ]

        orders_spark = spark_reference.createDataFrame(orders_data, ["id", "customer_id", "order_date"])
        customers_spark = spark_reference.createDataFrame(customers_data, ["id", "name", "tier"])

        orders_td = spark_thunderduck.createDataFrame(orders_data, ["id", "customer_id", "order_date"])
        customers_td = spark_thunderduck.createDataFrame(customers_data, ["id", "name", "tier"])

        # Complex join condition with AND
        spark_joined = orders_spark.join(
            customers_spark,
            (orders_spark["customer_id"] == customers_spark["id"]) & (customers_spark["tier"] == "premium"),
            "inner"
        ).select(
            orders_spark["id"].alias("order_id"),
            customers_spark["name"]
        ).orderBy("order_id")

        td_joined = orders_td.join(
            customers_td,
            (orders_td["customer_id"] == customers_td["id"]) & (customers_td["tier"] == "premium"),
            "inner"
        ).select(
            orders_td["id"].alias("order_id"),
            customers_td["name"]
        ).orderBy("order_id")

        assert_dataframes_equal(spark_joined, td_joined, query_name="join_complex_condition")

    def test_three_way_join_with_ambiguous_columns(self, spark_reference, spark_thunderduck):
        """Test three-way join where multiple tables have same column name - exact parity check."""
        data1 = [(1, "A"), (2, "B")]
        data2 = [(1, "X"), (2, "Y")]
        data3 = [(1, "P"), (2, "Q")]

        t1_spark = spark_reference.createDataFrame(data1, ["id", "v1"])
        t2_spark = spark_reference.createDataFrame(data2, ["id", "v2"])
        t3_spark = spark_reference.createDataFrame(data3, ["id", "v3"])

        t1_td = spark_thunderduck.createDataFrame(data1, ["id", "v1"])
        t2_td = spark_thunderduck.createDataFrame(data2, ["id", "v2"])
        t3_td = spark_thunderduck.createDataFrame(data3, ["id", "v3"])

        # Chain joins using DataFrame column references
        spark_result = t1_spark.join(
            t2_spark, t1_spark["id"] == t2_spark["id"], "inner"
        ).join(
            t3_spark, t1_spark["id"] == t3_spark["id"], "inner"
        ).select(
            t1_spark["id"].alias("key"),
            t1_spark["v1"],
            t2_spark["v2"],
            t3_spark["v3"]
        ).orderBy("key")

        td_result = t1_td.join(
            t2_td, t1_td["id"] == t2_td["id"], "inner"
        ).join(
            t3_td, t1_td["id"] == t3_td["id"], "inner"
        ).select(
            t1_td["id"].alias("key"),
            t1_td["v1"],
            t2_td["v2"],
            t3_td["v3"]
        ).orderBy("key")

        assert_dataframes_equal(spark_result, td_result, query_name="three_way_join")

    def test_left_join_with_ambiguous_columns(self, spark_reference, spark_thunderduck):
        """Test left join with ambiguous column names - exact parity check."""
        left_data = [(1, "A"), (2, "B"), (3, "C")]
        right_data = [(1, "X"), (2, "Y")]

        left_spark = spark_reference.createDataFrame(left_data, ["id", "value"])
        right_spark = spark_reference.createDataFrame(right_data, ["id", "data"])

        left_td = spark_thunderduck.createDataFrame(left_data, ["id", "value"])
        right_td = spark_thunderduck.createDataFrame(right_data, ["id", "data"])

        # Left join - row with id=3 should have NULL for right side
        spark_result = left_spark.join(
            right_spark, left_spark["id"] == right_spark["id"], "left"
        ).select(
            left_spark["id"].alias("lid"),
            left_spark["value"],
            right_spark["data"]
        ).orderBy("lid")

        td_result = left_td.join(
            right_td, left_td["id"] == right_td["id"], "left"
        ).select(
            left_td["id"].alias("lid"),
            left_td["value"],
            right_td["data"]
        ).orderBy("lid")

        assert_dataframes_equal(spark_result, td_result, query_name="left_join_ambiguous")

    def test_self_join_with_alias_dataframe_api(self, spark_reference, spark_thunderduck):
        """Test self-join using DataFrame.alias() - NOT SQL workaround.

        This is the key test for verifying that df.alias("lower").join(df.alias("higher"), ...)
        works correctly with column references qualified by the user's alias.
        """
        data = [
            (1, "Alice", 50000),
            (2, "Bob", 75000),
            (3, "Charlie", 60000),
            (4, "Diana", 90000),
        ]

        df_spark = spark_reference.createDataFrame(data, ["id", "name", "salary"])
        df_td = spark_thunderduck.createDataFrame(data, ["id", "name", "salary"])

        # Self-join using DataFrame API with aliases
        # Find all pairs where one person earns less than another
        lower_spark = df_spark.alias("lower")
        higher_spark = df_spark.alias("higher")

        lower_td = df_td.alias("lower")
        higher_td = df_td.alias("higher")

        spark_result = lower_spark.join(
            higher_spark,
            lower_spark["salary"] < higher_spark["salary"]
        ).select(
            lower_spark["name"].alias("lower_earner"),
            lower_spark["salary"].alias("lower_salary"),
            higher_spark["name"].alias("higher_earner"),
            higher_spark["salary"].alias("higher_salary")
        ).orderBy("lower_earner", "higher_earner")

        td_result = lower_td.join(
            higher_td,
            lower_td["salary"] < higher_td["salary"]
        ).select(
            lower_td["name"].alias("lower_earner"),
            lower_td["salary"].alias("lower_salary"),
            higher_td["name"].alias("higher_earner"),
            higher_td["salary"].alias("higher_salary")
        ).orderBy("lower_earner", "higher_earner")

        assert_dataframes_equal(spark_result, td_result, query_name="self_join_alias_df_api")

    def test_self_join_with_filter_using_alias(self, spark_reference, spark_thunderduck):
        """Test self-join with filter using DataFrame.alias().

        This tests the case where we have a filter on top of a self-join with aliases.
        """
        data = [
            (1, "Alice", 50000, "Engineering"),
            (2, "Bob", 75000, "Engineering"),
            (3, "Charlie", 60000, "Sales"),
            (4, "Diana", 90000, "Sales"),
        ]

        df_spark = spark_reference.createDataFrame(data, ["id", "name", "salary", "dept"])
        df_td = spark_thunderduck.createDataFrame(data, ["id", "name", "salary", "dept"])

        # Self-join to find employees in the same department where one earns more
        e1_spark = df_spark.alias("e1")
        e2_spark = df_spark.alias("e2")

        e1_td = df_td.alias("e1")
        e2_td = df_td.alias("e2")

        # Join and filter: same department, e1 earns less
        spark_result = e1_spark.join(
            e2_spark,
            (e1_spark["dept"] == e2_spark["dept"]) & (e1_spark["salary"] < e2_spark["salary"])
        ).select(
            e1_spark["name"].alias("lower_earner"),
            e2_spark["name"].alias("higher_earner"),
            e1_spark["dept"].alias("department")
        ).orderBy("department", "lower_earner")

        td_result = e1_td.join(
            e2_td,
            (e1_td["dept"] == e2_td["dept"]) & (e1_td["salary"] < e2_td["salary"])
        ).select(
            e1_td["name"].alias("lower_earner"),
            e2_td["name"].alias("higher_earner"),
            e1_td["dept"].alias("department")
        ).orderBy("department", "lower_earner")

        assert_dataframes_equal(spark_result, td_result, query_name="self_join_filter_alias")

    def test_right_join_column_resolution(self, spark_reference, spark_thunderduck):
        """Test RIGHT JOIN with column selection from both tables - no explicit aliases.

        This specifically tests the bug fix where:
        - Both tables have a column named 'id'
        - No explicit DataFrame.alias() is used
        - Right join produces unmatched rows (right side has rows with no match on left)
        - Selection must correctly distinguish left.id (NULL) from right.id (has value)

        Bug behavior was: rid = NULL for unmatched rows (incorrectly using left table's id)
        Expected behavior: rid = 3 for unmatched rows (correctly using right table's id)
        """
        left_data = [(1, "A"), (2, "B")]
        right_data = [(2, "Y"), (3, "Z")]

        left_spark = spark_reference.createDataFrame(left_data, ["id", "value"])
        right_spark = spark_reference.createDataFrame(right_data, ["id", "data"])

        left_td = spark_thunderduck.createDataFrame(left_data, ["id", "value"])
        right_td = spark_thunderduck.createDataFrame(right_data, ["id", "data"])

        # Right join - row with id=3 should have NULL for left side columns
        # but should have id=3 from right side
        spark_result = left_spark.join(
            right_spark, left_spark["id"] == right_spark["id"], "right"
        ).select(
            left_spark["value"],
            right_spark["id"].alias("rid"),
            right_spark["data"]
        ).orderBy("rid")

        td_result = left_td.join(
            right_td, left_td["id"] == right_td["id"], "right"
        ).select(
            left_td["value"],
            right_td["id"].alias("rid"),
            right_td["data"]
        ).orderBy("rid")

        assert_dataframes_equal(spark_result, td_result, query_name="right_join_column_resolution")

    def test_full_outer_join_column_resolution(self, spark_reference, spark_thunderduck):
        """Test FULL OUTER JOIN with column selection from both tables - no explicit aliases.

        Similar to right join test but for full outer join where either side can be NULL.
        """
        left_data = [(1, "A"), (2, "B")]
        right_data = [(2, "Y"), (3, "Z")]

        left_spark = spark_reference.createDataFrame(left_data, ["id", "value"])
        right_spark = spark_reference.createDataFrame(right_data, ["id", "data"])

        left_td = spark_thunderduck.createDataFrame(left_data, ["id", "value"])
        right_td = spark_thunderduck.createDataFrame(right_data, ["id", "data"])

        # Full outer join - id=1 has NULL on right, id=3 has NULL on left
        # Use coalesce on the aliased columns in orderBy
        spark_result = left_spark.join(
            right_spark, left_spark["id"] == right_spark["id"], "full"
        ).select(
            left_spark["id"].alias("lid"),
            left_spark["value"],
            right_spark["id"].alias("rid"),
            right_spark["data"]
        ).orderBy(F.coalesce("lid", "rid"))

        td_result = left_td.join(
            right_td, left_td["id"] == right_td["id"], "full"
        ).select(
            left_td["id"].alias("lid"),
            left_td["value"],
            right_td["id"].alias("rid"),
            right_td["data"]
        ).orderBy(F.coalesce("lid", "rid"))

        assert_dataframes_equal(spark_result, td_result, query_name="full_outer_join_column_resolution")
