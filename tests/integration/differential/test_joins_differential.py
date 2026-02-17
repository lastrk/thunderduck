"""
Differential tests for JOIN operations with explicit ON conditions.

Compares Thunderduck against Apache Spark 4.1.1 for all join types.
This complements test_using_joins_differential.py which tests USING joins.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.fixture(scope="class")
def join_test_data(spark_reference, spark_thunderduck):
    """Test data for join operations - employees and departments"""

    # Left table: employees
    left_data = [
        (1, "Alice", 100),
        (2, "Bob", 100),
        (3, "Charlie", 200),
        (4, "Diana", None),  # NULL dept
        (5, "Eve", 300),     # No matching dept
    ]
    left_schema = StructType([
        StructField("emp_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("dept_id", IntegerType(), True),
    ])

    # Right table: departments
    right_data = [
        (100, "Engineering", 50000),
        (200, "Sales", 40000),
        (400, "Marketing", 35000),  # No employees
    ]
    right_schema = StructType([
        StructField("dept_id", IntegerType(), True),
        StructField("dept_name", StringType(), False),
        StructField("budget", IntegerType(), False),
    ])

    left_ref = spark_reference.createDataFrame(left_data, left_schema)
    left_td = spark_thunderduck.createDataFrame(left_data, left_schema)
    right_ref = spark_reference.createDataFrame(right_data, right_schema)
    right_td = spark_thunderduck.createDataFrame(right_data, right_schema)

    return (left_ref, right_ref), (left_td, right_td)


@pytest.mark.joins
class TestJoinTypes:
    """Tests for all join types with explicit ON conditions."""

    def test_inner_join_on(self, spark_reference, spark_thunderduck, join_test_data):
        """Inner join with explicit ON condition."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "inner"
        ).select("emp_id", "name", "dept_name").orderBy("emp_id")

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "inner"
        ).select("emp_id", "name", "dept_name").orderBy("emp_id")

        assert_dataframes_equal(ref_result, td_result, query_name="inner_join_on")

    def test_left_join_on(self, spark_reference, spark_thunderduck, join_test_data):
        """Left outer join - preserves all left rows."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        # Collect and compare data directly to avoid schema nullable mismatch
        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "left"
        ).select("emp_id", "name", "dept_name").orderBy("emp_id")

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "left"
        ).select("emp_id", "name", "dept_name").orderBy("emp_id")

        # Compare row data (nullable inference differs between Spark/DuckDB for outer joins)
        ref_rows = [tuple(row) for row in ref_result.collect()]
        td_rows = [tuple(row) for row in td_result.collect()]
        assert ref_rows == td_rows, f"left_join_on: rows differ\nRef: {ref_rows}\nTD: {td_rows}"

    def test_right_join_on(self, spark_reference, spark_thunderduck, join_test_data):
        """Right outer join - preserves all right rows."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "right"
        ).select("emp_id", "name", "dept_name").orderBy(
            F.coalesce("dept_name", F.lit("ZZZ")),
            F.coalesce("emp_id", F.lit(999))
        )

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "right"
        ).select("emp_id", "name", "dept_name").orderBy(
            F.coalesce("dept_name", F.lit("ZZZ")),
            F.coalesce("emp_id", F.lit(999))
        )

        # Compare row data (nullable inference differs between Spark/DuckDB for outer joins)
        ref_rows = [tuple(row) for row in ref_result.collect()]
        td_rows = [tuple(row) for row in td_result.collect()]
        assert ref_rows == td_rows, f"right_join_on: rows differ\nRef: {ref_rows}\nTD: {td_rows}"

    def test_full_outer_join_on(self, spark_reference, spark_thunderduck, join_test_data):
        """Full outer join - preserves all rows from both sides."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "full"
        ).select("emp_id", "name", "dept_name").orderBy(
            F.coalesce("emp_id", F.lit(999)),
            F.coalesce("dept_name", F.lit("ZZZ"))
        )

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "full"
        ).select("emp_id", "name", "dept_name").orderBy(
            F.coalesce("emp_id", F.lit(999)),
            F.coalesce("dept_name", F.lit("ZZZ"))
        )

        # Compare row data (nullable inference differs between Spark/DuckDB for outer joins)
        ref_rows = [tuple(row) for row in ref_result.collect()]
        td_rows = [tuple(row) for row in td_result.collect()]
        assert ref_rows == td_rows, f"full_outer_join_on: rows differ\nRef: {ref_rows}\nTD: {td_rows}"

    def test_cross_join(self, spark_reference, spark_thunderduck):
        """Cross join - Cartesian product."""
        left_schema = StructType([StructField("id", IntegerType(), False)])
        right_schema = StructType([StructField("val", StringType(), False)])

        left_ref = spark_reference.createDataFrame([(1,), (2,)], left_schema)
        right_ref = spark_reference.createDataFrame([("a",), ("b",)], right_schema)
        left_td = spark_thunderduck.createDataFrame([(1,), (2,)], left_schema)
        right_td = spark_thunderduck.createDataFrame([("a",), ("b",)], right_schema)

        ref_result = left_ref.crossJoin(right_ref).orderBy("id", "val")
        td_result = left_td.crossJoin(right_td).orderBy("id", "val")

        assert_dataframes_equal(ref_result, td_result, query_name="cross_join")

    def test_left_semi_join(self, spark_reference, spark_thunderduck, join_test_data):
        """Left semi join - returns left rows that have matches."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "left_semi"
        ).orderBy("emp_id")

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "left_semi"
        ).orderBy("emp_id")

        assert_dataframes_equal(ref_result, td_result, query_name="left_semi_join")

    def test_left_anti_join(self, spark_reference, spark_thunderduck, join_test_data):
        """Left anti join - returns left rows that have NO matches."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = left_ref.join(
            right_ref,
            left_ref.dept_id == right_ref.dept_id,
            "left_anti"
        ).orderBy("emp_id")

        td_result = left_td.join(
            right_td,
            left_td.dept_id == right_td.dept_id,
            "left_anti"
        ).orderBy("emp_id")

        assert_dataframes_equal(ref_result, td_result, query_name="left_anti_join")


@pytest.mark.joins
class TestJoinConditions:
    """Tests for complex join conditions."""

    def test_join_composite_key(self, spark_reference, spark_thunderduck):
        """Join on composite key (multiple columns)."""
        schema1 = StructType([
            StructField("id", IntegerType(), False),
            StructField("key", StringType(), False),
            StructField("val1", IntegerType(), False),
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), False),
            StructField("key", StringType(), False),
            StructField("val2", IntegerType(), False),
        ])

        data1 = [(1, "A", 10), (1, "B", 20), (2, "A", 30)]
        data2 = [(1, "A", 100), (1, "C", 200), (2, "A", 300)]

        df1_ref = spark_reference.createDataFrame(data1, schema1)
        df2_ref = spark_reference.createDataFrame(data2, schema2)
        df1_td = spark_thunderduck.createDataFrame(data1, schema1)
        df2_td = spark_thunderduck.createDataFrame(data2, schema2)

        # Join on id AND key
        ref_result = df1_ref.join(
            df2_ref,
            (df1_ref.id == df2_ref.id) & (df1_ref.key == df2_ref.key),
            "inner"
        ).select(df1_ref.id, df1_ref.key, "val1", "val2").orderBy("id", "key")

        td_result = df1_td.join(
            df2_td,
            (df1_td.id == df2_td.id) & (df1_td.key == df2_td.key),
            "inner"
        ).select(df1_td.id, df1_td.key, "val1", "val2").orderBy("id", "key")

        assert_dataframes_equal(ref_result, td_result, query_name="join_composite_key")

    def test_join_inequality(self, spark_reference, spark_thunderduck):
        """Join with inequality condition."""
        schema1 = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", IntegerType(), False),
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), False),
            StructField("threshold", IntegerType(), False),
        ])

        data1 = [(1, 10), (1, 20), (2, 15)]
        data2 = [(1, 15), (2, 10)]

        df1_ref = spark_reference.createDataFrame(data1, schema1)
        df2_ref = spark_reference.createDataFrame(data2, schema2)
        df1_td = spark_thunderduck.createDataFrame(data1, schema1)
        df2_td = spark_thunderduck.createDataFrame(data2, schema2)

        # Join where value > threshold
        ref_result = df1_ref.join(
            df2_ref,
            (df1_ref.id == df2_ref.id) & (df1_ref.value > df2_ref.threshold),
            "inner"
        ).select(df1_ref.id, "value", "threshold").orderBy("id", "value")

        td_result = df1_td.join(
            df2_td,
            (df1_td.id == df2_td.id) & (df1_td.value > df2_td.threshold),
            "inner"
        ).select(df1_td.id, "value", "threshold").orderBy("id", "value")

        assert_dataframes_equal(ref_result, td_result, query_name="join_inequality")

    def test_join_or_condition(self, spark_reference, spark_thunderduck):
        """Join with OR condition."""
        schema1 = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
        ])
        schema2 = StructType([
            StructField("id1", IntegerType(), True),
            StructField("id2", IntegerType(), True),
            StructField("category", StringType(), False),
        ])

        data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        data2 = [(1, None, "A"), (None, 2, "B"), (4, 4, "C")]

        df1_ref = spark_reference.createDataFrame(data1, schema1)
        df2_ref = spark_reference.createDataFrame(data2, schema2)
        df1_td = spark_thunderduck.createDataFrame(data1, schema1)
        df2_td = spark_thunderduck.createDataFrame(data2, schema2)

        # Join where id matches id1 OR id2
        ref_result = df1_ref.join(
            df2_ref,
            (df1_ref.id == df2_ref.id1) | (df1_ref.id == df2_ref.id2),
            "inner"
        ).select("name", "category").orderBy("name")

        td_result = df1_td.join(
            df2_td,
            (df1_td.id == df2_td.id1) | (df1_td.id == df2_td.id2),
            "inner"
        ).select("name", "category").orderBy("name")

        assert_dataframes_equal(ref_result, td_result, query_name="join_or_condition")


@pytest.mark.joins
class TestJoinEdgeCases:
    """Tests for join edge cases."""

    def test_self_join(self, spark_reference, spark_thunderduck):
        """Self join - find employees in same department using USING join."""
        # Create fresh data with different column names for self-join
        emp_schema = StructType([
            StructField("emp_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("dept_id", IntegerType(), True),
        ])

        emp_data = [(1, "Alice", 100), (2, "Bob", 100), (3, "Charlie", 200)]

        emp_ref = spark_reference.createDataFrame(emp_data, emp_schema)
        emp_td = spark_thunderduck.createDataFrame(emp_data, emp_schema)

        # For self-join, use withColumnRenamed to create distinct column names
        emp2_ref = emp_ref.withColumnRenamed("emp_id", "emp_id2").withColumnRenamed("name", "name2")
        emp2_td = emp_td.withColumnRenamed("emp_id", "emp_id2").withColumnRenamed("name", "name2")

        # Join where same dept and emp_id < emp_id2
        ref_result = (emp_ref.join(
                emp2_ref,
                (emp_ref.dept_id == emp2_ref.dept_id) & (emp_ref.emp_id < emp2_ref.emp_id2),
                "inner"
            )
            .select("name", "name2")
            .orderBy("name", "name2"))

        td_result = (emp_td.join(
                emp2_td,
                (emp_td.dept_id == emp2_td.dept_id) & (emp_td.emp_id < emp2_td.emp_id2),
                "inner"
            )
            .select("name", "name2")
            .orderBy("name", "name2"))

        # Compare row data (nullable inference can differ)
        ref_rows = [tuple(row) for row in ref_result.collect()]
        td_rows = [tuple(row) for row in td_result.collect()]
        assert ref_rows == td_rows, f"self_join: rows differ\nRef: {ref_rows}\nTD: {td_rows}"

    def test_join_null_keys(self, spark_reference, spark_thunderduck):
        """Join behavior with NULL keys - NULLs should not match."""
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("val1", StringType(), False),
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("val2", StringType(), False),
        ])

        data1 = [(1, "A"), (None, "B"), (2, "C")]
        data2 = [(1, "X"), (None, "Y"), (3, "Z")]

        df1_ref = spark_reference.createDataFrame(data1, schema1)
        df2_ref = spark_reference.createDataFrame(data2, schema2)
        df1_td = spark_thunderduck.createDataFrame(data1, schema1)
        df2_td = spark_thunderduck.createDataFrame(data2, schema2)

        # NULLs should NOT match each other - use USING join pattern
        ref_result = df1_ref.join(
            df2_ref,
            df1_ref.id == df2_ref.id,
            "inner"
        ).select("val1", "val2").orderBy("val1")

        td_result = df1_td.join(
            df2_td,
            df1_td.id == df2_td.id,
            "inner"
        ).select("val1", "val2").orderBy("val1")

        assert_dataframes_equal(ref_result, td_result, query_name="join_null_keys")

    def test_multi_table_join(self, spark_reference, spark_thunderduck):
        """Three-way join: employees -> departments -> locations."""
        emp_schema = StructType([
            StructField("emp_id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("dept_id", IntegerType(), False),
        ])
        dept_schema = StructType([
            StructField("dept_id", IntegerType(), False),
            StructField("dept_name", StringType(), False),
            StructField("loc_id", IntegerType(), False),
        ])
        loc_schema = StructType([
            StructField("loc_id", IntegerType(), False),
            StructField("city", StringType(), False),
        ])

        emp_data = [(1, "Alice", 100), (2, "Bob", 200)]
        dept_data = [(100, "Engineering", 10), (200, "Sales", 20)]
        loc_data = [(10, "NYC"), (20, "LA")]

        emp_ref = spark_reference.createDataFrame(emp_data, emp_schema)
        dept_ref = spark_reference.createDataFrame(dept_data, dept_schema)
        loc_ref = spark_reference.createDataFrame(loc_data, loc_schema)

        emp_td = spark_thunderduck.createDataFrame(emp_data, emp_schema)
        dept_td = spark_thunderduck.createDataFrame(dept_data, dept_schema)
        loc_td = spark_thunderduck.createDataFrame(loc_data, loc_schema)

        ref_result = (emp_ref
            .join(dept_ref, emp_ref.dept_id == dept_ref.dept_id, "inner")
            .join(loc_ref, dept_ref.loc_id == loc_ref.loc_id, "inner")
            .select("name", "dept_name", "city")
            .orderBy("name"))

        td_result = (emp_td
            .join(dept_td, emp_td.dept_id == dept_td.dept_id, "inner")
            .join(loc_td, dept_td.loc_id == loc_td.loc_id, "inner")
            .select("name", "dept_name", "city")
            .orderBy("name"))

        assert_dataframes_equal(ref_result, td_result, query_name="multi_table_join")

    def test_join_with_filter(self, spark_reference, spark_thunderduck, join_test_data):
        """Join followed by filter."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = (left_ref
            .join(right_ref, left_ref.dept_id == right_ref.dept_id, "inner")
            .filter(F.col("budget") > 40000)
            .select("emp_id", "name", "dept_name")
            .orderBy("emp_id"))

        td_result = (left_td
            .join(right_td, left_td.dept_id == right_td.dept_id, "inner")
            .filter(F.col("budget") > 40000)
            .select("emp_id", "name", "dept_name")
            .orderBy("emp_id"))

        assert_dataframes_equal(ref_result, td_result, query_name="join_with_filter")

    def test_join_with_aggregation(self, spark_reference, spark_thunderduck, join_test_data):
        """Join followed by aggregation."""
        (left_ref, right_ref), (left_td, right_td) = join_test_data

        ref_result = (left_ref
            .join(right_ref, left_ref.dept_id == right_ref.dept_id, "inner")
            .groupBy("dept_name")
            .agg(F.count("*").alias("emp_count"))
            .orderBy("dept_name"))

        td_result = (left_td
            .join(right_td, left_td.dept_id == right_td.dept_id, "inner")
            .groupBy("dept_name")
            .agg(F.count("*").alias("emp_count"))
            .orderBy("dept_name"))

        assert_dataframes_equal(ref_result, td_result, query_name="join_with_aggregation")
