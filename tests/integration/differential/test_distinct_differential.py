"""
Differential tests for distinct and dropDuplicates operations.

Validates that Thunderduck produces identical results to Apache Spark 4.0.1
for all distinct operation variants.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Distinct Operations
# =============================================================================


@pytest.mark.differential
class TestDistinctOperations:
    """Tests for distinct() operation."""

    @pytest.mark.timeout(30)
    def test_distinct_basic(self, spark_reference, spark_thunderduck):
        """Basic distinct removes duplicate rows"""
        def run_test(spark):
            data = [(1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.distinct().orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_basic")

    @pytest.mark.timeout(30)
    def test_distinct_no_duplicates(self, spark_reference, spark_thunderduck):
        """Distinct on DataFrame with all unique rows returns same rows"""
        def run_test(spark):
            data = [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.distinct().orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_no_duplicates")

    @pytest.mark.timeout(30)
    def test_distinct_all_duplicates(self, spark_reference, spark_thunderduck):
        """Distinct on DataFrame where all rows are identical returns single row"""
        def run_test(spark):
            data = [(1, "a"), (1, "a"), (1, "a"), (1, "a")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.distinct().orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_all_duplicates")

    @pytest.mark.timeout(30)
    def test_distinct_with_nulls(self, spark_reference, spark_thunderduck):
        """Distinct treats NULL values as equal"""
        def run_test(spark):
            # Multiple rows with NULL should collapse to one NULL row
            data = [(1, "a"), (None, "b"), (None, "b"), (2, "c"), (None, "b")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.distinct().orderBy(F.col("id").asc_nulls_first(), "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_with_nulls")

    @pytest.mark.timeout(30)
    def test_distinct_multiple_columns(self, spark_reference, spark_thunderduck):
        """Distinct considers all columns for uniqueness"""
        def run_test(spark):
            data = [
                (1, "a", 100),
                (1, "a", 100),  # duplicate
                (1, "a", 200),  # different value
                (1, "b", 100),  # different name
                (2, "a", 100),  # different id
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.distinct().orderBy("id", "name", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_multiple_columns")

    @pytest.mark.timeout(30)
    def test_distinct_empty_dataframe(self, spark_reference, spark_thunderduck):
        """Distinct on empty DataFrame returns empty DataFrame"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame([], schema)
            return df.distinct().orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_empty_dataframe")


# =============================================================================
# DropDuplicates Operations
# =============================================================================


@pytest.mark.differential
class TestDropDuplicatesOperations:
    """Tests for dropDuplicates() operation."""

    @pytest.mark.timeout(30)
    def test_drop_duplicates_alias(self, spark_reference, spark_thunderduck):
        """dropDuplicates() without args is alias for distinct()"""
        def run_test(spark):
            data = [(1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.dropDuplicates().orderBy("id", "value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "drop_duplicates_alias")

    @pytest.mark.timeout(30)
    def test_drop_duplicates_subset_single(self, spark_reference, spark_thunderduck):
        """dropDuplicates on single column keeps first occurrence"""
        def run_test(spark):
            # Deduplicating on 'id' keeps first row for each unique id
            data = [
                (1, "a", 100),
                (1, "b", 200),  # same id, different values - dropped
                (2, "c", 300),
                (2, "d", 400),  # same id, different values - dropped
                (3, "e", 500),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Note: dropDuplicates keeps arbitrary row for each key
            # We just check that we get one row per unique id
            result = df.dropDuplicates(["id"])
            return result.select("id").orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "drop_duplicates_subset_single")

    @pytest.mark.timeout(30)
    def test_drop_duplicates_subset_multiple(self, spark_reference, spark_thunderduck):
        """dropDuplicates on multiple columns"""
        def run_test(spark):
            data = [
                (1, "a", 100),
                (1, "a", 200),  # same (id, name) - dropped
                (1, "b", 300),  # different name
                (2, "a", 400),  # different id
                (2, "a", 500),  # same (id, name) - dropped
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Note: dropDuplicates keeps arbitrary row for each key
            # We just check the unique (id, name) combinations
            result = df.dropDuplicates(["id", "name"])
            return result.select("id", "name").orderBy("id", "name")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "drop_duplicates_subset_multiple")

    @pytest.mark.timeout(30)
    def test_drop_duplicates_with_nulls_in_subset(self, spark_reference, spark_thunderduck):
        """dropDuplicates handles NULL in subset columns"""
        def run_test(spark):
            data = [
                (1, "a"),
                (None, "b"),
                (None, "c"),  # same NULL id - should be dropped
                (2, "d"),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            result = df.dropDuplicates(["id"])
            return result.select("id").orderBy(F.col("id").asc_nulls_first())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "drop_duplicates_with_nulls_in_subset")


# =============================================================================
# Combined Operations
# =============================================================================


@pytest.mark.differential
class TestDistinctCombinations:
    """Tests for distinct combined with other operations."""

    @pytest.mark.timeout(30)
    def test_distinct_with_filter(self, spark_reference, spark_thunderduck):
        """Distinct combined with filter"""
        def run_test(spark):
            data = [(1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.filter(F.col("id") > 1).distinct().orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "distinct_with_filter")

    @pytest.mark.timeout(30)
    def test_filter_then_distinct(self, spark_reference, spark_thunderduck):
        """Filter then distinct (order matters for performance, not result)"""
        def run_test(spark):
            data = [(1, "a"), (1, "a"), (2, "b"), (2, "b"), (3, "c"), (3, "c")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Filter first, then distinct
            return df.distinct().filter(F.col("id") > 1).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "filter_then_distinct")
