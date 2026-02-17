"""
Differential tests for set operations (union, intersect, except).

Validates that Thunderduck produces identical results to Apache Spark 4.1.1
for all set operation variants.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Union Operations
# =============================================================================


@pytest.mark.differential
class TestUnionOperations:
    """Tests for union and unionByName operations."""

    @pytest.mark.timeout(30)
    def test_union_basic(self, spark_reference, spark_thunderduck):
        """Basic union (UNION ALL - keeps duplicates)"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            data2 = [(2, "b"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # union() in PySpark is UNION ALL (keeps duplicates)
            return df1.union(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_basic")

    @pytest.mark.timeout(30)
    def test_union_with_duplicates(self, spark_reference, spark_thunderduck):
        """Union preserves all duplicates from both sides"""
        def run_test(spark):
            # data1 has (2, "b") twice
            data1 = [(1, "a"), (2, "b"), (2, "b"), (3, "c")]
            # data2 has (3, "c") twice
            data2 = [(2, "b"), (3, "c"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            return df1.union(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_with_duplicates")

    @pytest.mark.timeout(30)
    def test_union_then_distinct(self, spark_reference, spark_thunderduck):
        """Union followed by distinct removes duplicates"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b"), (2, "b")]
            data2 = [(2, "b"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            return df1.union(df2).distinct().orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_then_distinct")

    @pytest.mark.timeout(30)
    def test_union_by_name(self, spark_reference, spark_thunderduck):
        """UnionByName matches columns by name, not position"""
        def run_test(spark):
            # df1 has columns in order: id, value
            data1 = [(1, "a"), (2, "b")]
            schema1 = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            # df2 has columns in reversed order: value, id
            data2 = [("c", 3), ("d", 4)]
            schema2 = StructType([
                StructField("value", StringType(), True),
                StructField("id", IntegerType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.unionByName(df2).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_by_name")


# =============================================================================
# Intersect Operations
# =============================================================================


@pytest.mark.differential
class TestIntersectOperations:
    """Tests for intersect and intersectAll operations."""

    @pytest.mark.timeout(30)
    def test_intersect_basic(self, spark_reference, spark_thunderduck):
        """Basic intersect (returns distinct common rows)"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b"), (2, "b"), (3, "c")]
            data2 = [(2, "b"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # intersect() removes duplicates
            return df1.intersect(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "intersect_basic")

    @pytest.mark.timeout(30)
    def test_intersect_all(self, spark_reference, spark_thunderduck):
        """IntersectAll preserves duplicates (min count from each side)"""
        def run_test(spark):
            # data1 has (2, "b") twice and (3, "c") once
            data1 = [(1, "a"), (2, "b"), (2, "b"), (3, "c")]
            # data2 has (2, "b") once and (3, "c") twice
            data2 = [(2, "b"), (3, "c"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # intersectAll() keeps min(count_left, count_right) for each row
            # (2, "b"): min(2, 1) = 1
            # (3, "c"): min(1, 2) = 1
            return df1.intersectAll(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "intersect_all")

    @pytest.mark.timeout(30)
    def test_intersect_no_overlap(self, spark_reference, spark_thunderduck):
        """Intersect with no common rows returns empty DataFrame"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            data2 = [(3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            return df1.intersect(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "intersect_no_overlap")


# =============================================================================
# Except Operations
# =============================================================================


@pytest.mark.differential
class TestExceptOperations:
    """Tests for except, exceptAll, and subtract operations."""

    @pytest.mark.timeout(30)
    def test_except_basic(self, spark_reference, spark_thunderduck):
        """Basic except (returns distinct rows in left but not in right)"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b"), (2, "b"), (3, "c")]
            data2 = [(2, "b"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # except() removes duplicates and returns rows only in left
            return df1.exceptAll(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        # Use exceptAll to avoid the except keyword conflict
        assert_dataframes_equal(ref, td, "except_basic")

    @pytest.mark.timeout(30)
    def test_except_all(self, spark_reference, spark_thunderduck):
        """ExceptAll preserves duplicate counts (subtracts counts)"""
        def run_test(spark):
            # data1 has (2, "b") three times
            data1 = [(1, "a"), (2, "b"), (2, "b"), (2, "b"), (3, "c")]
            # data2 has (2, "b") once
            data2 = [(2, "b"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # exceptAll() subtracts counts:
            # (1, "a"): 1 - 0 = 1
            # (2, "b"): 3 - 1 = 2
            # (3, "c"): 1 - 1 = 0 (removed)
            return df1.exceptAll(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "except_all")

    @pytest.mark.timeout(30)
    def test_subtract(self, spark_reference, spark_thunderduck):
        """Subtract is an alias for except"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b"), (3, "c")]
            data2 = [(2, "b"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            return df1.subtract(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "subtract")


# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.differential
class TestSetOperationEdgeCases:
    """Edge cases for set operations."""

    @pytest.mark.timeout(30)
    def test_set_op_with_nulls(self, spark_reference, spark_thunderduck):
        """Set operations treat NULLs as equal"""
        def run_test(spark):
            # Both have (None, "x") - should appear in intersect
            data1 = [(1, "a"), (None, "x"), (2, "b")]
            data2 = [(None, "x"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            return df1.intersect(df2).orderBy(F.col("id").asc_nulls_first(), "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "set_op_with_nulls")

    @pytest.mark.timeout(30)
    def test_chained_set_operations(self, spark_reference, spark_thunderduck):
        """Multiple chained set operations"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b"), (3, "c")]
            data2 = [(2, "b"), (3, "c"), (4, "d")]
            data3 = [(3, "c"), (4, "d"), (5, "e")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            df3 = spark.createDataFrame(data3, schema)
            # union df1 and df2, then intersect with df3
            # df1.union(df2) = [(1,a), (2,b), (3,c), (2,b), (3,c), (4,d)]
            # intersect with df3 = [(3,c), (4,d)]
            return df1.union(df2).intersect(df3).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "chained_set_operations")

    @pytest.mark.timeout(30)
    def test_union_empty_dataframe(self, spark_reference, spark_thunderduck):
        """Union with empty DataFrame"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df_empty = spark.createDataFrame([], schema)
            return df1.union(df_empty).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_empty_dataframe")

    @pytest.mark.timeout(30)
    def test_except_all_rows_removed(self, spark_reference, spark_thunderduck):
        """Except where all rows are removed"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            data2 = [(1, "a"), (2, "b"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema)
            df2 = spark.createDataFrame(data2, schema)
            # All rows in df1 exist in df2, so result is empty
            return df1.subtract(df2).orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "except_all_rows_removed")


# =============================================================================
# Union Type Coercion
# =============================================================================


@pytest.mark.differential
class TestUnionTypeCoercion:
    """Tests for UNION type widening/coercion between different column types."""

    @pytest.mark.timeout(30)
    def test_union_int_and_long(self, spark_reference, spark_thunderduck):
        """Union of INT and BIGINT columns should widen to BIGINT"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            schema1 = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            data2 = [(3, "c"), (4, "d")]
            schema2 = StructType([
                StructField("id", LongType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.union(df2).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_int_and_long")

    @pytest.mark.timeout(30)
    def test_union_int_and_double(self, spark_reference, spark_thunderduck):
        """Union of INT and DOUBLE columns should widen to DOUBLE"""
        def run_test(spark):
            data1 = [(1, "a"), (2, "b")]
            schema1 = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            data2 = [(3.5, "c"), (4.5, "d")]
            schema2 = StructType([
                StructField("id", DoubleType(), True),
                StructField("value", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.union(df2).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_int_and_double")

    @pytest.mark.timeout(30)
    def test_union_float_and_double(self, spark_reference, spark_thunderduck):
        """Union of FLOAT and DOUBLE columns should widen to DOUBLE"""
        def run_test(spark):
            data1 = [(1.0, "a"), (2.0, "b")]
            schema1 = StructType([
                StructField("val", FloatType(), True),
                StructField("label", StringType(), True)
            ])
            data2 = [(3.0, "c"), (4.0, "d")]
            schema2 = StructType([
                StructField("val", DoubleType(), True),
                StructField("label", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.union(df2).orderBy("val")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_float_and_double")

    @pytest.mark.timeout(30)
    def test_union_long_and_double(self, spark_reference, spark_thunderduck):
        """Union of BIGINT and DOUBLE columns should widen to DOUBLE"""
        def run_test(spark):
            data1 = [(100, "a"), (200, "b")]
            schema1 = StructType([
                StructField("amount", LongType(), True),
                StructField("label", StringType(), True)
            ])
            data2 = [(3.14, "c"), (2.72, "d")]
            schema2 = StructType([
                StructField("amount", DoubleType(), True),
                StructField("label", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.union(df2).orderBy("amount")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_long_and_double")

    @pytest.mark.timeout(30)
    def test_union_multiple_type_mismatches(self, spark_reference, spark_thunderduck):
        """Union where multiple columns need type widening"""
        def run_test(spark):
            data1 = [(1, 10.0, "x")]
            schema1 = StructType([
                StructField("id", IntegerType(), True),
                StructField("score", FloatType(), True),
                StructField("tag", StringType(), True)
            ])
            data2 = [(2, 20.0, "y")]
            schema2 = StructType([
                StructField("id", LongType(), True),
                StructField("score", DoubleType(), True),
                StructField("tag", StringType(), True)
            ])
            df1 = spark.createDataFrame(data1, schema1)
            df2 = spark.createDataFrame(data2, schema2)
            return df1.union(df2).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "union_multiple_type_mismatches")
