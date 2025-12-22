"""
Differential tests for sorting edge cases.

Validates that Thunderduck produces identical results to Apache Spark 4.0.1
for null ordering (nullsFirst/nullsLast) and multi-column sorting scenarios.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Null Ordering Tests
# =============================================================================


@pytest.mark.differential
class TestNullOrdering:
    """Tests for nullsFirst/nullsLast options."""

    @pytest.mark.timeout(30)
    def test_asc_nulls_first(self, spark_reference, spark_thunderduck):
        """ORDER BY col ASC NULLS FIRST - NULLs appear first"""
        def run_test(spark):
            data = [(1, 100), (2, None), (3, 200), (4, None), (5, 50)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("value").asc_nulls_first())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "asc_nulls_first", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_asc_nulls_last(self, spark_reference, spark_thunderduck):
        """ORDER BY col ASC NULLS LAST - NULLs appear last"""
        def run_test(spark):
            data = [(1, 100), (2, None), (3, 200), (4, None), (5, 50)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("value").asc_nulls_last())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "asc_nulls_last", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_desc_nulls_first(self, spark_reference, spark_thunderduck):
        """ORDER BY col DESC NULLS FIRST - NULLs appear first"""
        def run_test(spark):
            data = [(1, 100), (2, None), (3, 200), (4, None), (5, 50)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("value").desc_nulls_first())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "desc_nulls_first", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_desc_nulls_last(self, spark_reference, spark_thunderduck):
        """ORDER BY col DESC NULLS LAST - NULLs appear last"""
        def run_test(spark):
            data = [(1, 100), (2, None), (3, 200), (4, None), (5, 50)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("value").desc_nulls_last())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "desc_nulls_last", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_string_nulls_first(self, spark_reference, spark_thunderduck):
        """ORDER BY string col ASC NULLS FIRST"""
        def run_test(spark):
            data = [(1, "Alice"), (2, None), (3, "Bob"), (4, None), (5, "Charlie")]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("name").asc_nulls_first())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "string_nulls_first", ignore_nullable=True)


# =============================================================================
# Multi-Column Sorting Tests
# =============================================================================


@pytest.mark.differential
class TestMultiColumnSorting:
    """Tests for multi-column mixed asc/desc sorting."""

    @pytest.mark.timeout(30)
    def test_multi_column_mixed_asc_desc(self, spark_reference, spark_thunderduck):
        """ORDER BY col1 ASC, col2 DESC"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, "B", 200),
                (3, "A", 150),
                (4, "B", 100),
                (5, "A", 200),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("category", StringType(), False),
                StructField("value", IntegerType(), False)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("category").asc(), F.col("value").desc())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "multi_column_mixed_asc_desc", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_multi_column_mixed_nulls(self, spark_reference, spark_thunderduck):
        """ORDER BY col1 ASC NULLS FIRST, col2 DESC NULLS LAST"""
        def run_test(spark):
            data = [
                (1, "A", 100),
                (2, None, 200),
                (3, "A", None),
                (4, None, None),
                (5, "B", 150),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(
                F.col("category").asc_nulls_first(),
                F.col("value").desc_nulls_last()
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "multi_column_mixed_nulls", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_three_column_sort(self, spark_reference, spark_thunderduck):
        """ORDER BY col1 DESC, col2 ASC, col3 DESC"""
        def run_test(spark):
            data = [
                (1, "A", "X", 100),
                (2, "B", "Y", 200),
                (3, "A", "X", 150),
                (4, "A", "Y", 100),
                (5, "B", "X", 100),
                (6, "A", "X", 100),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("cat1", StringType(), False),
                StructField("cat2", StringType(), False),
                StructField("value", IntegerType(), False)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(
                F.col("cat1").desc(),
                F.col("cat2").asc(),
                F.col("value").desc()
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "three_column_sort", ignore_nullable=True)


# =============================================================================
# Edge Cases
# =============================================================================


@pytest.mark.differential
class TestSortingEdgeCases:
    """Edge case tests for sorting."""

    @pytest.mark.timeout(30)
    def test_sort_all_nulls(self, spark_reference, spark_thunderduck):
        """Sort column with all NULL values"""
        def run_test(spark):
            data = [(1, None), (2, None), (3, None)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Order by the all-null column, then by id for deterministic results
            return df.orderBy(F.col("value").asc_nulls_first(), F.col("id").asc())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "sort_all_nulls", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_sort_with_ties_and_nulls(self, spark_reference, spark_thunderduck):
        """Sort with ties and NULL values - use secondary sort for determinism"""
        def run_test(spark):
            data = [
                (1, 100),
                (2, None),
                (3, 100),  # Tie with row 1
                (4, None),  # Tie with row 2 (both NULL)
                (5, 200),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            # Use id as secondary sort to break ties deterministically
            return df.orderBy(
                F.col("value").asc_nulls_first(),
                F.col("id").asc()
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "sort_with_ties_and_nulls", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_sort_empty_dataframe(self, spark_reference, spark_thunderduck):
        """Sort empty DataFrame"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame([], schema)
            return df.orderBy(F.col("value").desc_nulls_last())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "sort_empty_dataframe", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_sort_single_row(self, spark_reference, spark_thunderduck):
        """Sort single row DataFrame"""
        def run_test(spark):
            data = [(1, 100)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.orderBy(F.col("value").asc_nulls_first())

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "sort_single_row", ignore_nullable=True)
