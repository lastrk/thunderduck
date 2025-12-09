"""DataFrame operation tests for thunderduck.

Tests for LOCAL_RELATION support which enables count() and other operations
on DataFrames created via spark.createDataFrame().
"""

from datetime import date, datetime
from thunderduck_e2e.test_runner import ThunderduckE2ETestBase
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window


class TestDataFrameOperations(ThunderduckE2ETestBase):
    """Test DataFrame operations through Spark Connect."""

    def test_select_columns(self):
        """Test column selection."""
        df = self.spark.table("employees").select("name", "salary")
        result = df.collect()

        self.assertEqual(len(result), 5)
        self.assertEqual(len(result[0]), 2)

        # Check column names
        self.assertEqual(df.columns, ["name", "salary"])

    def test_filter_operations(self):
        """Test various filter operations."""
        # Simple filter
        df1 = self.spark.table("employees").filter("salary > 60000")
        self.assertEqual(df1.count(), 4)

        # Filter with AND
        df2 = self.spark.table("employees").filter(
            (F.col("salary") > 60000) & (F.col("department") == "Engineering")
        )
        self.assertEqual(df2.count(), 3)

        # Filter with OR
        df3 = self.spark.table("employees").filter(
            (F.col("department") == "HR") | (F.col("salary") > 75000)
        )
        self.assertEqual(df3.count(), 3)

    def test_groupby_aggregation(self):
        """Test group by with various aggregations."""
        df = self.spark.table("employees").groupBy("department").agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
            F.min("salary").alias("min_salary"),
            F.sum("salary").alias("total_salary")
        )

        result = df.collect()
        self.assertEqual(len(result), 3)  # 3 departments

        # Verify Engineering department stats
        eng_row = [r for r in result if r["department"] == "Engineering"][0]
        self.assertEqual(eng_row["count"], 3)
        self.assertEqual(eng_row["max_salary"], 80000)
        self.assertEqual(eng_row["min_salary"], 70000)
        self.assertAlmostEqual(eng_row["avg_salary"], 75000, places=2)

    def test_join_operations(self):
        """Test various join types."""
        emp = self.spark.table("employees")
        dept = self.spark.table("departments")

        # Inner join
        inner = emp.join(dept, emp.department == dept.name, "inner")
        self.assertEqual(inner.count(), 5)

        # Left join
        left = emp.join(dept, emp.department == dept.name, "left")
        self.assertEqual(left.count(), 5)

        # Right join (should include all departments)
        right = emp.join(dept, emp.department == dept.name, "right")
        self.assertEqual(right.count(), 5)

    def test_window_functions(self):
        """Test window functions."""
        window = Window.partitionBy("department").orderBy(F.desc("salary"))

        df = self.spark.table("employees").select(
            "*",
            F.row_number().over(window).alias("row_num"),
            F.rank().over(window).alias("rank"),
            F.dense_rank().over(window).alias("dense_rank"),
            F.percent_rank().over(window).alias("percent_rank")
        )

        result = df.filter("department = 'Engineering'").orderBy("row_num").collect()

        # Check rankings for Engineering department
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["salary"], 80000)  # Bob - highest
        self.assertEqual(result[0]["row_num"], 1)
        self.assertEqual(result[1]["salary"], 75000)  # John - second
        self.assertEqual(result[2]["salary"], 70000)  # Charlie - third

    def test_pivot_operations(self):
        """Test pivot operations."""
        df = self.spark.table("employees").groupBy("department").pivot("department").count()

        result = df.collect()
        self.assertGreater(len(result), 0)

    def test_union_operations(self):
        """Test union operations."""
        df1 = self.spark.table("employees").filter("salary > 70000")
        df2 = self.spark.table("employees").filter("department = 'HR'")

        # Union (with duplicates)
        union_all = df1.union(df2)
        self.assertEqual(union_all.count(), 4)  # 3 + 1

        # Union distinct
        union_distinct = df1.union(df2).distinct()
        self.assertEqual(union_distinct.count(), 4)

    def test_null_handling(self):
        """Test NULL value handling."""
        # Create data with NULLs
        df = self.spark.sql("""
            SELECT * FROM VALUES
                (1, 'A', 100),
                (2, NULL, 200),
                (3, 'C', NULL),
                (4, NULL, NULL)
            AS t(id, name, value)
        """)

        # Test NULL filtering
        not_null = df.filter(F.col("name").isNotNull())
        self.assertEqual(not_null.count(), 2)

        # Test NULL in aggregations
        avg_value = df.agg(F.avg("value").alias("avg")).collect()[0]["avg"]
        self.assertAlmostEqual(avg_value, 150.0)  # (100 + 200) / 2

    def test_distinct_operations(self):
        """Test distinct operations."""
        df = self.spark.table("employees")

        # Distinct departments
        distinct_depts = df.select("department").distinct()
        self.assertEqual(distinct_depts.count(), 3)

        # Count distinct
        count_distinct = df.agg(
            F.countDistinct("department").alias("dept_count")
        ).collect()[0]["dept_count"]
        self.assertEqual(count_distinct, 3)


class TestLocalRelationOperations(ThunderduckE2ETestBase):
    """Test operations on locally-created DataFrames (LOCAL_RELATION support).

    These tests validate the LOCAL_RELATION feature which allows count() and
    other operations on DataFrames created via spark.createDataFrame().
    """

    def test_count_on_local_dataframe(self):
        """Test count() on DataFrame created with createDataFrame()."""
        df = self.spark.createDataFrame([
            Row(a=1, b=2.0, c='string1'),
            Row(a=2, b=3.0, c='string2'),
            Row(a=3, b=4.0, c='string3'),
        ])

        count = df.count()
        self.assertEqual(count, 3)

    def test_count_on_single_row_dataframe(self):
        """Test count() on single-row DataFrame with various types."""
        df = self.spark.createDataFrame([
            Row(a=2, b=3.0, c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0))
        ])

        count = df.count()
        self.assertEqual(count, 1)

    def test_count_on_empty_dataframe(self):
        """Test count() on empty DataFrame with schema."""
        schema = "id INT, name STRING"
        df = self.spark.createDataFrame([], schema)

        count = df.count()
        self.assertEqual(count, 0)

    def test_select_on_local_dataframe(self):
        """Test select() operations on local DataFrame."""
        df = self.spark.createDataFrame([
            Row(id=1, name='Alice', salary=50000),
            Row(id=2, name='Bob', salary=60000),
        ])

        result = df.select("name", "salary").collect()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["name"], "Bob")

    def test_filter_on_local_dataframe(self):
        """Test filter() on local DataFrame."""
        df = self.spark.createDataFrame([
            Row(id=1, value=10),
            Row(id=2, value=20),
            Row(id=3, value=30),
        ])

        filtered = df.filter("value > 15")
        self.assertEqual(filtered.count(), 2)

    def test_aggregation_on_local_dataframe(self):
        """Test aggregation functions on local DataFrame."""
        df = self.spark.createDataFrame([
            Row(category='A', amount=100),
            Row(category='A', amount=200),
            Row(category='B', amount=150),
        ])

        result = df.groupBy("category").agg(
            F.sum("amount").alias("total"),
            F.count("*").alias("cnt")
        ).collect()

        self.assertEqual(len(result), 2)

        # Find category A
        cat_a = [r for r in result if r["category"] == "A"][0]
        self.assertEqual(cat_a["total"], 300)
        self.assertEqual(cat_a["cnt"], 2)

    def test_local_dataframe_with_date_types(self):
        """Test local DataFrame with date and timestamp types."""
        df = self.spark.createDataFrame([
            Row(
                id=1,
                event_date=date(2024, 1, 15),
                event_time=datetime(2024, 1, 15, 10, 30, 0)
            ),
            Row(
                id=2,
                event_date=date(2024, 2, 20),
                event_time=datetime(2024, 2, 20, 14, 45, 0)
            ),
        ])

        count = df.count()
        self.assertEqual(count, 2)

        result = df.select("id", "event_date").collect()
        self.assertEqual(result[0]["event_date"], date(2024, 1, 15))

    def test_local_dataframe_with_nulls(self):
        """Test local DataFrame with NULL values."""
        df = self.spark.createDataFrame([
            Row(id=1, name='Alice', value=100),
            Row(id=2, name=None, value=200),
            Row(id=3, name='Charlie', value=None),
        ])

        count = df.count()
        self.assertEqual(count, 3)

        # Filter non-null names
        non_null_names = df.filter(F.col("name").isNotNull())
        self.assertEqual(non_null_names.count(), 2)

    def test_local_dataframe_with_special_characters(self):
        """Test local DataFrame with special characters in strings."""
        df = self.spark.createDataFrame([
            Row(id=1, name="O'Brien"),
            Row(id=2, name='John "Jack" Smith'),
            Row(id=3, name="Line1\nLine2"),
        ])

        count = df.count()
        self.assertEqual(count, 3)

        result = df.filter("id = 1").collect()
        self.assertEqual(result[0]["name"], "O'Brien")

    def test_join_local_dataframes(self):
        """Test joining two locally-created DataFrames."""
        df1 = self.spark.createDataFrame([
            Row(id=1, name='Alice'),
            Row(id=2, name='Bob'),
        ])

        df2 = self.spark.createDataFrame([
            Row(id=1, dept='Engineering'),
            Row(id=2, dept='Marketing'),
        ])

        joined = df1.join(df2, "id")
        self.assertEqual(joined.count(), 2)

    def test_union_local_dataframes(self):
        """Test union of locally-created DataFrames."""
        df1 = self.spark.createDataFrame([
            Row(id=1, name='Alice'),
        ])

        df2 = self.spark.createDataFrame([
            Row(id=2, name='Bob'),
        ])

        union_df = df1.union(df2)
        self.assertEqual(union_df.count(), 2)

    def test_local_dataframe_with_large_values(self):
        """Test local DataFrame with large numeric values."""
        df = self.spark.createDataFrame([
            Row(big_int=2147483647, big_float=1.7976931348623157e+308),
            Row(big_int=-2147483648, big_float=-1.7976931348623157e+308),
        ])

        count = df.count()
        self.assertEqual(count, 2)

    def test_collect_on_local_dataframe(self):
        """Test collect() returns correct data from local DataFrame."""
        df = self.spark.createDataFrame([
            Row(x=1, y='a'),
            Row(x=2, y='b'),
            Row(x=3, y='c'),
        ])

        result = df.collect()

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["x"], 1)
        self.assertEqual(result[0]["y"], "a")
        self.assertEqual(result[2]["x"], 3)
        self.assertEqual(result[2]["y"], "c")
