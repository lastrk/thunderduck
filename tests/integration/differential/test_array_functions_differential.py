"""
Differential tests for array/list functions.
Verifies Thunderduck matches Spark 4.0.1 behavior exactly for array operations.

Tests include: split, collect_list, collect_set, size, explode, array_contains, etc.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal
from pyspark.sql import functions as F


@pytest.mark.differential
class TestArrayFunctionsDifferential:
    """Differential tests for array/list manipulation functions."""

    def test_split_function(self, spark_reference, spark_thunderduck):
        """Test split() function - exact parity check."""
        data = [
            (1, "Alice Smith"),
            (2, "Bob Jones"),
            (3, "Charlie Brown")
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "name"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "name"])

        # Use split function
        spark_result = spark_df.selectExpr("id", "split(name, ' ') as name_parts").orderBy("id")
        td_result = td_df.selectExpr("id", "split(name, ' ') as name_parts").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="split_function",
            ignore_nullable=True
        )

    def test_split_with_limit(self, spark_reference, spark_thunderduck):
        """Test split() function with limit (3rd argument) - exact parity check.

        Spark split(str, pattern, limit) semantics:
          limit > 0: split into at most `limit` pieces; last piece contains the remainder
          limit < 0: same as no limit (split into all pieces, preserving trailing empty strings)
          limit = 0: same as no limit but removes trailing empty strings (default)
        """
        data = [
            (1, "a-b-c-d-e"),
            (2, "one-two-three"),
            (3, "x"),
            (4, "hello-world-foo-bar"),
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "val"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "val"])

        # Test limit > 0: split into at most 2 pieces
        spark_result = spark_df.selectExpr("id", "split(val, '-', 2) as parts").orderBy("id")
        td_result = td_df.selectExpr("id", "split(val, '-', 2) as parts").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="split_with_limit_2",
            ignore_nullable=True
        )

        # Test limit > 0: split into at most 3 pieces
        spark_result = spark_df.selectExpr("id", "split(val, '-', 3) as parts").orderBy("id")
        td_result = td_df.selectExpr("id", "split(val, '-', 3) as parts").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="split_with_limit_3",
            ignore_nullable=True
        )

        # Test limit = -1: same as no limit
        spark_result = spark_df.selectExpr("id", "split(val, '-', -1) as parts").orderBy("id")
        td_result = td_df.selectExpr("id", "split(val, '-', -1) as parts").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="split_with_limit_neg1",
            ignore_nullable=True
        )

    def test_split_with_limit_dataframe_api(self, spark_reference, spark_thunderduck):
        """Test split() with limit via DataFrame API - exact parity check."""
        data = [
            (1, "a,b,c,d"),
            (2, "x,y"),
            (3, "hello,world,foo,bar,baz"),
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "val"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "val"])

        # DataFrame API: F.split(col, pattern, limit)
        spark_result = spark_df.select("id", F.split("val", ",", 2).alias("parts")).orderBy("id")
        td_result = td_df.select("id", F.split("val", ",", 2).alias("parts")).orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="split_with_limit_df_api",
            ignore_nullable=True
        )

    def test_startswith_function(self, spark_reference, spark_thunderduck):
        """Test startswith() function in filter - exact parity check."""
        data = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Andrew")
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "name"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "name"])

        # Use startswith in filter
        spark_result = spark_df.filter("startswith(name, 'A')").orderBy("id")
        td_result = td_df.filter("startswith(name, 'A')").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="startswith_function"
        )

    def test_collect_list_function(self, spark_reference, spark_thunderduck):
        """Test collect_list() aggregation function - exact parity check."""
        data = [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie")
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "name"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "name"])

        # Use collect_list aggregation
        spark_result = spark_df.selectExpr("collect_list(name) as names")
        td_result = td_df.selectExpr("collect_list(name) as names")

        # Note: Order of elements in collect_list may vary, so we compare as sets
        spark_names = set(spark_result.collect()[0]['names'])
        td_names = set(td_result.collect()[0]['names'])

        assert spark_names == td_names, f"collect_list mismatch: {spark_names} != {td_names}"

    def test_collect_set_function(self, spark_reference, spark_thunderduck):
        """Test collect_set() aggregation function - exact parity check."""
        data = [
            (1, "eng"),
            (2, "hr"),
            (3, "eng")
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "dept"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "dept"])

        # Use collect_set aggregation (should deduplicate)
        spark_result = spark_df.selectExpr("collect_set(dept) as depts")
        td_result = td_df.selectExpr("collect_set(dept) as depts")

        # Compare as sets since order doesn't matter
        spark_depts = set(spark_result.collect()[0]['depts'])
        td_depts = set(td_result.collect()[0]['depts'])

        assert spark_depts == td_depts, f"collect_set mismatch: {spark_depts} != {td_depts}"
        assert spark_depts == {'eng', 'hr'}, f"Expected {{'eng', 'hr'}}, got {spark_depts}"

    def test_size_function(self, spark_reference, spark_thunderduck):
        """Test size() function on arrays - exact parity check."""
        data = [
            (1, ["a", "b", "c"]),
            (2, ["x"]),
            (3, ["p", "q"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "tags"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "tags"])

        # Use size() in filter
        spark_result = spark_df.filter("size(tags) > 1").orderBy("id")
        td_result = td_df.filter("size(tags) > 1").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="size_function"
        )

    def test_explode_function(self, spark_reference, spark_thunderduck):
        """Test explode() function to unnest arrays - exact parity check."""
        data = [
            (1, ["a", "b", "c"]),
            (2, ["x", "y"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "items"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "items"])

        # Use explode to unnest arrays
        spark_result = spark_df.selectExpr("id", "explode(items) as item").orderBy("id", "item")
        td_result = td_df.selectExpr("id", "explode(items) as item").orderBy("id", "item")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="explode_function",
            ignore_nullable=True
        )

    def test_array_contains_function(self, spark_reference, spark_thunderduck):
        """Test array_contains() function - exact parity check."""
        data = [
            (1, ["spark", "scala", "java"]),
            (2, ["python", "pandas"]),
            (3, ["spark", "python"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "tags"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "tags"])

        # Use array_contains in filter
        spark_result = spark_df.filter("array_contains(tags, 'spark')").orderBy("id")
        td_result = td_df.filter("array_contains(tags, 'spark')").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="array_contains_function"
        )

    def test_size_in_select_expr(self, spark_reference, spark_thunderduck):
        """Test size() function in selectExpr - exact parity check."""
        data = [
            (1, ["alice", "bob"]),
            (2, ["charlie", "david", "eve"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "names"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "names"])

        # Use size() in selectExpr
        spark_result = spark_df.selectExpr("id", "size(names) as name_count").orderBy("id")
        td_result = td_df.selectExpr("id", "size(names) as name_count").orderBy("id")

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="size_in_select_expr",
            ignore_nullable=True
        )

    def test_collect_list_with_groupby(self, spark_reference, spark_thunderduck):
        """Test collect_list() with GROUP BY in SQL - exact parity check."""
        data = [
            ("eng", "Alice"),
            ("eng", "Bob"),
            ("hr", "Charlie")
        ]

        spark_df = spark_reference.createDataFrame(data, ["dept", "name"])
        td_df = spark_thunderduck.createDataFrame(data, ["dept", "name"])

        # Create temp views for SQL
        spark_df.createOrReplaceTempView("staff_spark")
        td_df.createOrReplaceTempView("staff_td")

        # Use collect_list in SQL with GROUP BY
        spark_result = spark_reference.sql("""
            SELECT dept, collect_list(name) as names
            FROM staff_spark
            GROUP BY dept
            ORDER BY dept
        """)

        td_result = spark_thunderduck.sql("""
            SELECT dept, collect_list(name) as names
            FROM staff_td
            GROUP BY dept
            ORDER BY dept
        """)

        # Compare row counts and dept values
        spark_rows = spark_result.collect()
        td_rows = td_result.collect()

        assert len(spark_rows) == len(td_rows), f"Row count mismatch: {len(spark_rows)} != {len(td_rows)}"

        for spark_row, td_row in zip(spark_rows, td_rows):
            assert spark_row['dept'] == td_row['dept'], f"Dept mismatch: {spark_row['dept']} != {td_row['dept']}"
            # Compare names as sets since order may vary
            spark_names = set(spark_row['names'])
            td_names = set(td_row['names'])
            assert spark_names == td_names, f"Names mismatch for dept {spark_row['dept']}: {spark_names} != {td_names}"

        # Cleanup
        spark_reference.catalog.dropTempView("staff_spark")
        spark_thunderduck.catalog.dropTempView("staff_td")

    def test_size_in_sql_with_filter(self, spark_reference, spark_thunderduck):
        """Test size() function in SQL with WHERE clause - exact parity check."""
        data = [
            (1, ["a", "b", "c"]),
            (2, ["x"]),
            (3, ["p", "q", "r", "s"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "tags"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "tags"])

        # Create temp views
        spark_df.createOrReplaceTempView("items_spark")
        td_df.createOrReplaceTempView("items_td")

        # Use size() in SQL with WHERE
        spark_result = spark_reference.sql("""
            SELECT id, size(tags) as tag_count
            FROM items_spark
            WHERE size(tags) > 1
            ORDER BY id
        """)

        td_result = spark_thunderduck.sql("""
            SELECT id, size(tags) as tag_count
            FROM items_td
            WHERE size(tags) > 1
            ORDER BY id
        """)

        assert_dataframes_equal(
            spark_result, td_result,
            query_name="size_in_sql",
            ignore_nullable=True
        )

        # Cleanup
        spark_reference.catalog.dropTempView("items_spark")
        spark_thunderduck.catalog.dropTempView("items_td")

    def test_case_insensitive_functions(self, spark_reference, spark_thunderduck):
        """Test that array functions are case insensitive - exact parity check."""
        data = [
            (1, ["a", "b"]),
            (2, ["x", "y", "z"])
        ]

        spark_df = spark_reference.createDataFrame(data, ["id", "items"])
        td_df = spark_thunderduck.createDataFrame(data, ["id", "items"])

        # Test with different cases: SIZE, Size, size
        for case_variant in ["SIZE", "Size", "size"]:
            spark_result = spark_df.filter(f"{case_variant}(items) > 2")
            td_result = td_df.filter(f"{case_variant}(items) > 2")

            spark_count = spark_result.count()
            td_count = td_result.count()

            assert spark_count == td_count == 1, f"Case sensitivity issue with {case_variant}: Spark={spark_count}, TD={td_count}"
