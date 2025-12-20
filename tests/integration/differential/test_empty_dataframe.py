"""
Empty DataFrame Differential Tests

Tests that empty DataFrames with schemas are handled identically between
Thunderduck and Apache Spark 4.0.1.

This tests the fix for: "No analyze result found!" error when creating
empty DataFrames with spark.createDataFrame([], schema).
"""

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType,
    DoubleType, FloatType, BooleanType
)
from pyspark.sql import functions as F


class TestEmptyDataFrameCreation:
    """Tests for creating empty DataFrames with explicit schemas"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_simple_schema(self, spark_reference, spark_thunderduck):
        """
        Create empty DataFrame with simple schema and compare results.
        """
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema)
        td_df = spark_thunderduck.createDataFrame([], schema)

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_simple_schema")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_all_primitive_types(self, spark_reference, spark_thunderduck):
        """
        Create empty DataFrame with all primitive types and compare.
        """
        schema = StructType([
            StructField("int_col", IntegerType(), True),
            StructField("long_col", LongType(), True),
            StructField("float_col", FloatType(), True),
            StructField("double_col", DoubleType(), True),
            StructField("string_col", StringType(), True),
            StructField("bool_col", BooleanType(), True),
        ])

        ref_df = spark_reference.createDataFrame([], schema)
        td_df = spark_thunderduck.createDataFrame([], schema)

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_all_primitive_types")


class TestEmptyDataFrameOperations:
    """Tests for operations on empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_select(self, spark_reference, spark_thunderduck):
        """Select columns from empty DataFrame"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).select("id", "name")
        td_df = spark_thunderduck.createDataFrame([], schema).select("id", "name")

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_select")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_filter(self, spark_reference, spark_thunderduck):
        """Filter empty DataFrame"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).filter(F.col("value") > 0)
        td_df = spark_thunderduck.createDataFrame([], schema).filter(F.col("value") > 0)

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_filter")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_sum(self, spark_reference, spark_thunderduck):
        """
        Aggregate (sum) on empty DataFrame.
        SUM on empty DataFrame should return NULL.
        """
        schema = StructType([
            StructField("value", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).agg(F.sum("value").alias("total"))
        td_df = spark_thunderduck.createDataFrame([], schema).agg(F.sum("value").alias("total"))

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_aggregate_sum")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_count(self, spark_reference, spark_thunderduck):
        """
        Aggregate (count) on empty DataFrame.
        COUNT on empty DataFrame should return 0.
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).agg(F.count("id").alias("cnt"))
        td_df = spark_thunderduck.createDataFrame([], schema).agg(F.count("id").alias("cnt"))

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_aggregate_count")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_avg(self, spark_reference, spark_thunderduck):
        """
        Aggregate (avg) on empty DataFrame.
        AVG on empty DataFrame should return NULL.
        """
        schema = StructType([
            StructField("value", DoubleType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).agg(F.avg("value").alias("average"))
        td_df = spark_thunderduck.createDataFrame([], schema).agg(F.avg("value").alias("average"))

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_aggregate_avg")


class TestEmptyDataFrameWithColumn:
    """Tests for withColumn operations on empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_with_column(self, spark_reference, spark_thunderduck):
        """Add column to empty DataFrame"""
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).withColumn("doubled", F.col("id") * 2)
        td_df = spark_thunderduck.createDataFrame([], schema).withColumn("doubled", F.col("id") * 2)

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_with_column")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_with_column_literal(self, spark_reference, spark_thunderduck):
        """Add literal column to empty DataFrame"""
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).withColumn("constant", F.lit(42))
        td_df = spark_thunderduck.createDataFrame([], schema).withColumn("constant", F.lit(42))

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_with_column_literal")


class TestEmptyDataFrameJoin:
    """Tests for joining with empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_join_empty_with_non_empty(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """
        Join empty DataFrame with non-empty DataFrame.
        Result should be empty.
        """
        left_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True)
        ])

        # Create empty left side on both
        ref_left = spark_reference.createDataFrame([], left_schema)
        td_left = spark_thunderduck.createDataFrame([], left_schema)

        # Load non-empty right side on both
        ref_right = spark_reference.read.parquet(str(tpch_data_dir / "nation.parquet"))
        ref_right = ref_right.withColumnRenamed("n_nationkey", "id")

        td_right = spark_thunderduck.read.parquet(str(tpch_data_dir / "nation.parquet"))
        td_right = td_right.withColumnRenamed("n_nationkey", "id")

        # Join - result should be empty
        ref_df = ref_left.join(ref_right, on="id", how="inner")
        td_df = td_left.join(td_right, on="id", how="inner")

        assert_dataframes_equal(ref_df, td_df, "join_empty_with_non_empty")

    @pytest.mark.timeout(30)
    def test_join_two_empty_dataframes(self, spark_reference, spark_thunderduck):
        """
        Join two empty DataFrames.
        Result should be empty.
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        ref_df1 = spark_reference.createDataFrame([], schema1)
        ref_df2 = spark_reference.createDataFrame([], schema2)
        ref_result = ref_df1.join(ref_df2, on="id", how="inner")

        td_df1 = spark_thunderduck.createDataFrame([], schema1)
        td_df2 = spark_thunderduck.createDataFrame([], schema2)
        td_result = td_df1.join(td_df2, on="id", how="inner")

        assert_dataframes_equal(ref_result, td_result, "join_two_empty_dataframes")


class TestEmptyDataFrameUnion:
    """Tests for union operations with empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_union_empty_with_non_empty(self, spark_reference, spark_thunderduck):
        """Union empty DataFrame with non-empty DataFrame"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])

        ref_empty = spark_reference.createDataFrame([], schema)
        ref_non_empty = spark_reference.createDataFrame([(1, "a"), (2, "b")], schema)
        ref_df = ref_empty.union(ref_non_empty)

        td_empty = spark_thunderduck.createDataFrame([], schema)
        td_non_empty = spark_thunderduck.createDataFrame([(1, "a"), (2, "b")], schema)
        td_df = td_empty.union(td_non_empty)

        assert_dataframes_equal(ref_df, td_df, "union_empty_with_non_empty")

    @pytest.mark.timeout(30)
    def test_union_two_empty_dataframes(self, spark_reference, spark_thunderduck):
        """Union two empty DataFrames"""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", DoubleType(), True)
        ])

        ref_df1 = spark_reference.createDataFrame([], schema)
        ref_df2 = spark_reference.createDataFrame([], schema)
        ref_result = ref_df1.union(ref_df2)

        td_df1 = spark_thunderduck.createDataFrame([], schema)
        td_df2 = spark_thunderduck.createDataFrame([], schema)
        td_result = td_df1.union(td_df2)

        assert_dataframes_equal(ref_result, td_result, "union_two_empty_dataframes")


class TestEmptyDataFrameGroupBy:
    """Tests for groupBy operations on empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_groupby_count(self, spark_reference, spark_thunderduck):
        """GroupBy count on empty DataFrame"""
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).groupBy("category").count()
        td_df = spark_thunderduck.createDataFrame([], schema).groupBy("category").count()

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_groupby_count")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_groupby_agg(self, spark_reference, spark_thunderduck):
        """GroupBy with multiple aggregations on empty DataFrame"""
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("value", IntegerType(), True)
        ])

        ref_df = spark_reference.createDataFrame([], schema).groupBy("category").agg(
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
            F.count("value").alias("cnt")
        )
        td_df = spark_thunderduck.createDataFrame([], schema).groupBy("category").agg(
            F.sum("value").alias("total"),
            F.avg("value").alias("average"),
            F.count("value").alias("cnt")
        )

        assert_dataframes_equal(ref_df, td_df, "empty_dataframe_groupby_agg")
