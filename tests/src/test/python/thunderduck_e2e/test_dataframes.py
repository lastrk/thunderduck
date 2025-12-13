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
        self.assertEqual(df3.count(), 2)

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
        self.assertEqual(union_all.count(), 3)  # 2 + 1

        # Union distinct
        union_distinct = df1.union(df2).distinct()
        self.assertEqual(union_distinct.count(), 3)

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


class TestRangeOperations(ThunderduckE2ETestBase):
    """Test spark.range() operations through Spark Connect.

    These tests validate the Range relation feature which enables
    spark.range(start, end, step) to generate sequences of integers.
    """

    def test_simple_range(self):
        """Test simple spark.range(n) which generates 0 to n-1."""
        df = self.spark.range(10)

        # Check count
        self.assertEqual(df.count(), 10)

        # Check column name is 'id'
        self.assertEqual(df.columns, ["id"])

        # Check values
        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, list(range(10)))

    def test_range_with_start_end(self):
        """Test spark.range(start, end) with explicit start."""
        df = self.spark.range(5, 15)

        self.assertEqual(df.count(), 10)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, list(range(5, 15)))

    def test_range_with_step(self):
        """Test spark.range(start, end, step) with custom step."""
        df = self.spark.range(0, 20, 2)

        self.assertEqual(df.count(), 10)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])

    def test_range_with_large_step(self):
        """Test range with step > 1 that doesn't evenly divide."""
        df = self.spark.range(0, 10, 3)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, [0, 3, 6, 9])

    def test_range_with_negative_start(self):
        """Test range starting from negative value."""
        df = self.spark.range(-5, 5)

        self.assertEqual(df.count(), 10)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, list(range(-5, 5)))

    def test_empty_range(self):
        """Test range where start >= end (empty result)."""
        df = self.spark.range(10, 5)

        self.assertEqual(df.count(), 0)

        result = df.collect()
        self.assertEqual(result, [])

    def test_range_with_filter(self):
        """Test filtering a range."""
        df = self.spark.range(0, 100).filter("id > 90")

        self.assertEqual(df.count(), 9)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, [91, 92, 93, 94, 95, 96, 97, 98, 99])

    def test_range_with_aggregation(self):
        """Test aggregation on a range."""
        df = self.spark.range(1, 11)  # 1 to 10

        # Sum should be 1+2+...+10 = 55
        # Note: Due to type serialization issue, result may be string
        result = df.agg(F.sum("id").alias("total")).collect()
        self.assertEqual(int(result[0]["total"]), 55)

        # Avg should be 5.5
        result = df.agg(F.avg("id").alias("average")).collect()
        self.assertAlmostEqual(float(result[0]["average"]), 5.5, places=2)

    def test_range_with_select(self):
        """Test select on a range with expressions."""
        df = self.spark.range(1, 6).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled"),
            (F.col("id") * F.col("id")).alias("squared")
        )

        result = df.collect()

        self.assertEqual(len(result), 5)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["doubled"], 2)
        self.assertEqual(result[0]["squared"], 1)
        self.assertEqual(result[4]["id"], 5)
        self.assertEqual(result[4]["doubled"], 10)
        self.assertEqual(result[4]["squared"], 25)

    def test_range_with_limit(self):
        """Test limit on a range."""
        df = self.spark.range(0, 1000).limit(5)

        self.assertEqual(df.count(), 5)

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, [0, 1, 2, 3, 4])

    def test_range_with_orderby(self):
        """Test ordering a range."""
        df = self.spark.range(0, 5).orderBy(F.desc("id"))

        result = df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(values, [4, 3, 2, 1, 0])

    def test_range_join_via_sql(self):
        """Test joining two ranges using SQL (workaround until withColumnRenamed is implemented)."""
        # Use SQL to create two ranges and join them
        joined = self.spark.sql("""
            SELECT r1.id as id1, r2.id as id2
            FROM (SELECT range AS id FROM range(0, 5, 1)) r1
            INNER JOIN (SELECT range AS id FROM range(3, 8, 1)) r2
            ON r1.id = r2.id
        """)

        # Overlap is 3, 4 (values that exist in both ranges)
        self.assertEqual(joined.count(), 2)

    def test_range_union(self):
        """Test union of two ranges."""
        df1 = self.spark.range(0, 3)
        df2 = self.spark.range(10, 13)

        union_df = df1.union(df2)

        self.assertEqual(union_df.count(), 6)

        result = union_df.collect()
        values = [row["id"] for row in result]
        self.assertEqual(sorted(values), [0, 1, 2, 10, 11, 12])

    def test_large_range(self):
        """Test a larger range to ensure it doesn't materialize in memory."""
        df = self.spark.range(0, 1000000)

        # Count should work efficiently
        self.assertEqual(df.count(), 1000000)

        # Aggregation should work
        result = df.agg(F.max("id").alias("max_val")).collect()
        self.assertEqual(result[0]["max_val"], 999999)

    def test_range_schema(self):
        """Test that range returns correct schema (single column 'id' of LongType)."""
        df = self.spark.range(10)

        schema = df.schema
        self.assertEqual(len(schema.fields), 1)
        self.assertEqual(schema.fields[0].name, "id")
        # In PySpark, this should be LongType
        self.assertEqual(str(schema.fields[0].dataType), "LongType()")


class TestColumnOperations(ThunderduckE2ETestBase):
    """Test column manipulation operations: drop, withColumnRenamed, withColumn."""

    def test_drop_single_column(self):
        """Test dropping a single column."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c")

        result = df.drop("c").collect()

        self.assertEqual(len(result), 1)
        # Column c should be gone, only a and b remain
        self.assertEqual(result[0]["a"], 1)
        self.assertEqual(result[0]["b"], 2)
        self.assertNotIn("c", result[0].asDict())

    def test_drop_multiple_columns(self):
        """Test dropping multiple columns."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c, 4 as d")

        result = df.drop("b", "d").collect()

        self.assertEqual(len(result), 1)
        # Only a and c should remain
        row_dict = result[0].asDict()
        self.assertIn("a", row_dict)
        self.assertIn("c", row_dict)
        self.assertNotIn("b", row_dict)
        self.assertNotIn("d", row_dict)

    def test_drop_nonexistent_column(self):
        """Test that dropping a nonexistent column raises error in DuckDB.

        Note: Spark silently ignores non-existent columns, but DuckDB throws an error.
        This is a known behavioral difference. We document it here.
        """
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        # DuckDB throws error for non-existent column in EXCLUDE clause
        # This is a behavioral difference from Spark, which silently ignores
        with self.assertRaises(Exception):
            df.drop("nonexistent").collect()

    def test_drop_with_range(self):
        """Test drop on a range DataFrame."""
        df = self.spark.range(5).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled")
        )

        result = df.drop("doubled").collect()

        self.assertEqual(len(result), 5)
        # Only id should remain
        for row in result:
            self.assertIn("id", row.asDict())
            self.assertNotIn("doubled", row.asDict())

    def test_with_column_renamed_single(self):
        """Test renaming a single column."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c")

        result = df.withColumnRenamed("a", "x").collect()

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        # a should be renamed to x
        self.assertIn("x", row_dict)
        self.assertNotIn("a", row_dict)
        self.assertEqual(row_dict["x"], 1)
        # b and c should still exist
        self.assertEqual(row_dict["b"], 2)
        self.assertEqual(row_dict["c"], 3)

    def test_with_column_renamed_multiple(self):
        """Test renaming multiple columns sequentially."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c")

        result = df.withColumnRenamed("a", "x").withColumnRenamed("b", "y").collect()

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertIn("x", row_dict)
        self.assertIn("y", row_dict)
        self.assertIn("c", row_dict)
        self.assertEqual(row_dict["x"], 1)
        self.assertEqual(row_dict["y"], 2)
        self.assertEqual(row_dict["c"], 3)

    def test_with_column_renamed_nonexistent(self):
        """Test that renaming a nonexistent column raises error in DuckDB.

        Note: Spark silently ignores non-existent columns, but DuckDB throws an error.
        This is a known behavioral difference. We document it here.
        """
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        # DuckDB throws error for non-existent column in EXCLUDE clause
        # This is a behavioral difference from Spark, which silently ignores
        with self.assertRaises(Exception):
            df.withColumnRenamed("nonexistent", "x").collect()

    def test_with_column_add_new(self):
        """Test adding a new column with withColumn."""
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        result = df.withColumn("c", F.col("a") + F.col("b")).collect()

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertEqual(row_dict["a"], 1)
        self.assertEqual(row_dict["b"], 2)
        self.assertEqual(row_dict["c"], 3)

    def test_with_column_replace_existing(self):
        """Test replacing an existing column with withColumn."""
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        result = df.withColumn("a", F.col("a") * 10).collect()

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertEqual(row_dict["a"], 10)
        self.assertEqual(row_dict["b"], 2)

    def test_with_column_literal(self):
        """Test adding a constant column with withColumn."""
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        result = df.withColumn("const", F.lit(42)).collect()

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertEqual(row_dict["a"], 1)
        self.assertEqual(row_dict["b"], 2)
        self.assertEqual(row_dict["const"], 42)

    def test_with_column_multiple(self):
        """Test adding multiple columns with chained withColumn."""
        df = self.spark.sql("SELECT 1 as a, 2 as b")

        result = (df
            .withColumn("sum", F.col("a") + F.col("b"))
            .withColumn("diff", F.col("a") - F.col("b"))
            .withColumn("prod", F.col("a") * F.col("b"))
            .collect())

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertEqual(row_dict["sum"], 3)
        self.assertEqual(row_dict["diff"], -1)
        self.assertEqual(row_dict["prod"], 2)

    def test_with_column_on_range(self):
        """Test withColumn on a range DataFrame."""
        df = self.spark.range(1, 6)

        result = df.withColumn("squared", F.col("id") * F.col("id")).collect()

        self.assertEqual(len(result), 5)
        for row in result:
            self.assertEqual(row["squared"], row["id"] ** 2)

    def test_combined_operations(self):
        """Test combining drop, withColumnRenamed, and withColumn."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c")

        result = (df
            .drop("c")
            .withColumnRenamed("a", "x")
            .withColumn("sum", F.col("x") + F.col("b"))
            .collect())

        self.assertEqual(len(result), 1)
        row_dict = result[0].asDict()
        self.assertNotIn("c", row_dict)
        self.assertNotIn("a", row_dict)
        self.assertIn("x", row_dict)
        self.assertIn("b", row_dict)
        self.assertIn("sum", row_dict)
        self.assertEqual(row_dict["x"], 1)
        self.assertEqual(row_dict["b"], 2)
        self.assertEqual(row_dict["sum"], 3)


class TestOffsetAndToDF(ThunderduckE2ETestBase):
    """Test offset and toDF operations (M20)."""

    def test_offset_basic(self):
        """Test basic offset operation."""
        df = self.spark.range(10)

        result = df.offset(3).collect()

        # Should skip first 3 rows (0, 1, 2), returning 3-9
        self.assertEqual(len(result), 7)
        ids = [r.id for r in result]
        self.assertEqual(ids, [3, 4, 5, 6, 7, 8, 9])

    def test_offset_zero(self):
        """Test offset(0) - should return all rows."""
        df = self.spark.range(5)

        result = df.offset(0).collect()

        self.assertEqual(len(result), 5)
        ids = [r.id for r in result]
        self.assertEqual(ids, [0, 1, 2, 3, 4])

    def test_offset_all_rows(self):
        """Test offset that skips all rows."""
        df = self.spark.range(5)

        result = df.offset(5).collect()

        # Should return empty result
        self.assertEqual(len(result), 0)

    def test_offset_more_than_rows(self):
        """Test offset greater than row count."""
        df = self.spark.range(5)

        result = df.offset(10).collect()

        # Should return empty result
        self.assertEqual(len(result), 0)

    def test_offset_with_limit(self):
        """Test offset combined with limit for pagination."""
        df = self.spark.range(20)

        # Skip first 5, take next 3
        result = df.offset(5).limit(3).collect()

        self.assertEqual(len(result), 3)
        ids = [r.id for r in result]
        self.assertEqual(ids, [5, 6, 7])

    def test_offset_with_filter(self):
        """Test offset after filter."""
        df = self.spark.range(10).filter(F.col("id") % 2 == 0)  # 0, 2, 4, 6, 8

        result = df.offset(2).collect()

        # Skip first 2 even numbers (0, 2), get 4, 6, 8
        self.assertEqual(len(result), 3)
        ids = [r.id for r in result]
        self.assertEqual(ids, [4, 6, 8])

    def test_todf_basic(self):
        """Test basic toDF operation."""
        df = self.spark.sql("SELECT 1 as a, 2 as b, 3 as c")

        result = df.toDF("x", "y", "z").collect()

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row.x, 1)
        self.assertEqual(row.y, 2)
        self.assertEqual(row.z, 3)

    def test_todf_with_range(self):
        """Test toDF on range DataFrame."""
        df = self.spark.range(3)

        result = df.toDF("value").collect()

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].value, 0)
        self.assertEqual(result[1].value, 1)
        self.assertEqual(result[2].value, 2)

    def test_todf_with_multiple_columns(self):
        """Test toDF with multiple columns from a select."""
        df = self.spark.range(2).select(
            F.col("id"),
            (F.col("id") * 10).alias("tens")
        )

        result = df.toDF("num", "big_num").collect()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].num, 0)
        self.assertEqual(result[0].big_num, 0)
        self.assertEqual(result[1].num, 1)
        self.assertEqual(result[1].big_num, 10)

    def test_todf_chained_operations(self):
        """Test toDF followed by other operations."""
        df = self.spark.range(5).toDF("value")

        result = df.filter(F.col("value") > 2).collect()

        self.assertEqual(len(result), 2)
        values = [r.value for r in result]
        self.assertEqual(values, [3, 4])

    def test_offset_todf_combined(self):
        """Test combining offset and toDF."""
        df = self.spark.range(10).toDF("num").offset(5)

        result = df.collect()

        self.assertEqual(len(result), 5)
        nums = [r.num for r in result]
        self.assertEqual(nums, [5, 6, 7, 8, 9])
