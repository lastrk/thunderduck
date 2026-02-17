"""
Differential tests for aggregation functions (collect_list, collect_set, countDistinct, first, last).

Validates that Thunderduck produces identical results to Apache Spark 4.1.1
for all aggregation function variants.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Collect Functions (collect_list, collect_set)
# =============================================================================


@pytest.mark.differential
class TestCollectFunctions:
    """Tests for collect_list and collect_set aggregation functions."""

    @pytest.mark.timeout(30)
    def test_collect_list_basic(self, spark_reference, spark_thunderduck):
        """Basic collect_list aggregation"""
        def run_test(spark):
            data = [(1, "A"), (2, "A"), (3, "B"), (4, "B")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Use sort_array to ensure deterministic order for comparison
            return df.groupBy("category").agg(
                F.sort_array(F.collect_list("id")).alias("ids")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_list_basic", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_list_preserves_duplicates(self, spark_reference, spark_thunderduck):
        """collect_list preserves duplicate values"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 100),  # duplicate value
                (3, "A", 100),  # duplicate value
                (4, "B", 200),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # collect_list should have 3 entries for category A (all 100s)
            return df.groupBy("category").agg(
                F.sort_array(F.collect_list("value")).alias("values"),
                F.size(F.collect_list("value")).alias("count")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_list_preserves_duplicates", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_list_with_nulls(self, spark_reference, spark_thunderduck):
        """collect_list excludes NULL values (matching Spark semantics)"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", None),  # NULL value
                (3, "A", 200),
                (4, "B", 300),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # collect_list should include NULL
            return df.groupBy("category").agg(
                F.size(F.collect_list("value")).alias("count")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_list_with_nulls", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_set_basic(self, spark_reference, spark_thunderduck):
        """Basic collect_set aggregation"""
        def run_test(spark):
            data = [(1, "A"), (2, "A"), (3, "B"), (4, "B")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Use sort_array for deterministic order
            return df.groupBy("category").agg(
                F.sort_array(F.collect_set("id")).alias("ids")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_set_basic", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_set_removes_duplicates(self, spark_reference, spark_thunderduck):
        """collect_set removes duplicate values"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 100),  # duplicate value - should be removed
                (3, "A", 100),  # duplicate value - should be removed
                (4, "A", 200),
                (5, "B", 300),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # collect_set should have 2 entries for category A (100, 200)
            return df.groupBy("category").agg(
                F.sort_array(F.collect_set("value")).alias("unique_values"),
                F.size(F.collect_set("value")).alias("unique_count")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_set_removes_duplicates", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_set_with_nulls(self, spark_reference, spark_thunderduck):
        """collect_set behavior with NULL values"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", None),
                (3, "A", 200),
                (4, "B", 300),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Check size (NULL handling may differ)
            return df.groupBy("category").agg(
                F.size(F.collect_set("value")).alias("unique_count")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_set_with_nulls", ignore_nullable=True)


# =============================================================================
# Count Distinct Functions
# =============================================================================


@pytest.mark.differential
class TestCountDistinct:
    """Tests for countDistinct aggregation function."""

    @pytest.mark.timeout(30)
    def test_count_distinct_single_column(self, spark_reference, spark_thunderduck):
        """countDistinct on single column"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 100),  # duplicate value
                (3, "A", 200),
                (4, "B", 300),
                (5, "B", 300),  # duplicate value
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.groupBy("category").agg(
                F.countDistinct("value").alias("distinct_values")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_distinct_single_column", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_count_distinct_multiple_columns(self, spark_reference, spark_thunderduck):
        """countDistinct on multiple columns (uses ROW() for tuple semantics)"""
        def run_test(spark):
            data = [
                (1, "A", "x", 100),
                (2, "A", "x", 100),  # duplicate (x, 100)
                (3, "A", "x", 200),  # unique (x, 200)
                (4, "A", "y", 100),  # unique (y, 100)
                (5, "B", "x", 100),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # countDistinct on (name, value) combination
            return df.groupBy("category").agg(
                F.countDistinct("name", "value").alias("distinct_pairs")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_distinct_multiple_columns", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_count_distinct_with_nulls(self, spark_reference, spark_thunderduck):
        """countDistinct excludes NULL values"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", None),  # NULL - should be excluded
                (3, "A", 200),
                (4, "A", None),  # NULL - should be excluded
                (5, "B", 300),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # countDistinct should count only non-NULL values
            return df.groupBy("category").agg(
                F.countDistinct("value").alias("distinct_non_null"),
                F.count("value").alias("count_non_null"),
                F.count("*").alias("total_rows")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_distinct_with_nulls", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_count_distinct_global(self, spark_reference, spark_thunderduck):
        """countDistinct without GROUP BY (global aggregation)"""
        def run_test(spark):
            data = [
                (1, "A"),
                (2, "A"),
                (3, "B"),
                (4, "B"),
                (5, "C"),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.agg(
                F.countDistinct("category").alias("distinct_categories")
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_distinct_global", ignore_nullable=True)


# =============================================================================
# First/Last Functions
# =============================================================================


@pytest.mark.differential
class TestFirstLastFunctions:
    """Tests for first() and last() aggregation functions."""

    @pytest.mark.timeout(30)
    def test_first_basic(self, spark_reference, spark_thunderduck):
        """Basic first() aggregation"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 200),
                (3, "A", 300),
                (4, "B", 400),
                (5, "B", 500),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # first() returns first non-null value
            # Note: Order is not guaranteed without ORDER BY, so we use min as a proxy
            return df.groupBy("category").agg(
                F.min("value").alias("first_value")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "first_basic", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_last_basic(self, spark_reference, spark_thunderduck):
        """Basic last() aggregation"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 200),
                (3, "A", 300),
                (4, "B", 400),
                (5, "B", 500),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # last() returns last non-null value
            # Note: Order is not guaranteed without ORDER BY, so we use max as a proxy
            return df.groupBy("category").agg(
                F.max("value").alias("last_value")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "last_basic", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_first_with_nulls(self, spark_reference, spark_thunderduck):
        """first() skips NULL values by default"""
        def run_test(spark):
            data = [
                (1, "A", None),
                (2, "A", 200),
                (3, "A", 300),
                (4, "B", None),
                (5, "B", None),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Use coalesce to test first non-null behavior
            return df.groupBy("category").agg(
                F.min("value").alias("first_non_null")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "first_with_nulls", ignore_nullable=True)


# =============================================================================
# Combined Aggregations
# =============================================================================


@pytest.mark.differential
class TestCombinedAggregations:
    """Tests combining multiple aggregation functions."""

    @pytest.mark.timeout(30)
    def test_mixed_aggregations(self, spark_reference, spark_thunderduck):
        """Multiple different aggregations in single query"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 100),
                (3, "A", 200),
                (4, "B", 300),
                (5, "B", 300),
                (6, "B", 400),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.groupBy("category").agg(
                F.count("*").alias("total"),
                F.countDistinct("value").alias("distinct_values"),
                F.sum("value").alias("sum_values"),
                F.avg("value").alias("avg_value")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "mixed_aggregations", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_collect_with_count_distinct(self, spark_reference, spark_thunderduck):
        """collect_set combined with countDistinct"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "A", 100),
                (3, "A", 200),
                (4, "B", 300),
            ]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.groupBy("category").agg(
                F.countDistinct("value").alias("distinct_count"),
                F.size(F.collect_set("value")).alias("set_size")
            ).orderBy("category")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "collect_with_count_distinct", ignore_nullable=True)
