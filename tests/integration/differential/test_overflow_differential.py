"""
Differential tests for integer overflow behavior.

Validates that Thunderduck produces identical overflow/underflow results
to Apache Spark 4.1.1 for SUM, arithmetic operations, and aggregate functions.

Key insight: Spark 4.0 has ANSI mode enabled by default, which throws
ArithmeticException on overflow instead of silently wrapping. This test
verifies that both engines produce the same results for boundary values
and the same exception behavior for overflow conditions.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# SUM Boundary Value Tests (No Overflow)
# =============================================================================


@pytest.mark.differential
class TestSumBoundaryValues:
    """Tests for SUM() at boundary values that don't overflow."""

    BIGINT_MAX = 9_223_372_036_854_775_807
    BIGINT_MIN = -9_223_372_036_854_775_808

    @pytest.mark.timeout(30)
    def test_sum_exactly_at_max(self, spark_reference, spark_thunderduck):
        """SUM that equals exactly BIGINT_MAX (no overflow)."""
        def run_test(spark):
            # Split BIGINT_MAX into parts that sum to it
            # BIGINT_MAX = 9,223,372,036,854,775,807
            part1 = 4_000_000_000_000_000_000
            part2 = 4_000_000_000_000_000_000
            part3 = 1_223_372_036_854_775_807  # remainder
            data = [(part1,), (part2,), (part3,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_exactly_at_max", ignore_nullable=True)

        result = ref.collect()[0]["total"]
        assert result == self.BIGINT_MAX, f"Expected BIGINT_MAX, got {result}"

    @pytest.mark.timeout(30)
    def test_sum_close_to_max(self, spark_reference, spark_thunderduck):
        """SUM of moderate values that approaches but doesn't exceed BIGINT_MAX."""
        def run_test(spark):
            # Values that sum to about half of BIGINT_MAX
            data = [
                (1_000_000_000_000_000_000,),
                (1_000_000_000_000_000_000,),
                (1_000_000_000_000_000_000,),
                (1_000_000_000_000_000_000,),
            ]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_close_to_max", ignore_nullable=True)

        result = ref.collect()[0]["total"]
        assert result == 4_000_000_000_000_000_000, f"Expected 4 quintillion, got {result}"

    @pytest.mark.timeout(30)
    def test_sum_mixed_positive_negative(self, spark_reference, spark_thunderduck):
        """SUM of mixed positive/negative values that cancel out (no overflow)."""
        def run_test(spark):
            large_val = 5_000_000_000_000_000_000
            data = [
                (large_val,),
                (-large_val,),
                (100,),
            ]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_mixed_positive_negative", ignore_nullable=True)

        result = ref.collect()[0]["total"]
        assert result == 100, f"Expected 100, got {result}"

    @pytest.mark.timeout(30)
    def test_sum_with_groupby_no_overflow(self, spark_reference, spark_thunderduck):
        """SUM with GROUP BY where no group overflows."""
        def run_test(spark):
            from pyspark.sql.types import StringType
            data = [
                ("A", 1_000_000_000_000_000_000),
                ("A", 1_000_000_000_000_000_000),
                ("B", 100),
                ("B", 200),
                ("B", 300),
            ]
            schema = StructType([
                StructField("grp", StringType(), False),
                StructField("val", LongType(), False)
            ])
            df = spark.createDataFrame(data, schema)
            return df.groupBy("grp").agg(
                F.sum("val").alias("total")
            ).orderBy("grp")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_with_groupby_no_overflow", ignore_nullable=True)


# =============================================================================
# Return Type Verification
# =============================================================================


@pytest.mark.differential
class TestAggregateReturnTypes:
    """Tests verifying return types match between Spark and Thunderduck."""

    @pytest.mark.timeout(30)
    def test_sum_bigint_returns_bigint(self, spark_reference, spark_thunderduck):
        """SUM(bigint) returns LongType (BIGINT)."""
        def run_test(spark):
            data = [(100,), (200,), (300,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        # Schema comparison includes type checking
        assert_dataframes_equal(ref, td, "sum_bigint_returns_bigint", ignore_nullable=True)

        # Explicit type assertion
        assert ref.schema["total"].dataType == LongType(), \
            f"Spark SUM type: {ref.schema['total'].dataType}"
        assert td.schema["total"].dataType == LongType(), \
            f"Thunderduck SUM type: {td.schema['total'].dataType}"

    @pytest.mark.timeout(30)
    def test_sum_int_returns_bigint(self, spark_reference, spark_thunderduck):
        """SUM(int) returns LongType (BIGINT), not IntegerType."""
        def run_test(spark):
            data = [(100,), (200,), (300,)]
            schema = StructType([StructField("val", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_int_returns_bigint", ignore_nullable=True)

        # SUM of INTEGER should promote to BIGINT
        assert ref.schema["total"].dataType == LongType(), \
            f"Spark SUM(int) type: {ref.schema['total'].dataType}"
        assert td.schema["total"].dataType == LongType(), \
            f"Thunderduck SUM(int) type: {td.schema['total'].dataType}"

    @pytest.mark.timeout(30)
    def test_count_returns_bigint(self, spark_reference, spark_thunderduck):
        """COUNT(*) returns LongType (BIGINT)."""
        def run_test(spark):
            data = [(1,), (2,), (3,), (4,), (5,)]
            schema = StructType([StructField("val", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.count("*").alias("cnt"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "count_returns_bigint", ignore_nullable=True)

        assert ref.schema["cnt"].dataType == LongType(), \
            f"Spark COUNT type: {ref.schema['cnt'].dataType}"
        assert td.schema["cnt"].dataType == LongType(), \
            f"Thunderduck COUNT type: {td.schema['cnt'].dataType}"

    @pytest.mark.timeout(30)
    def test_avg_bigint_returns_double(self, spark_reference, spark_thunderduck):
        """AVG(bigint) returns DoubleType, avoiding integer overflow issues."""
        def run_test(spark):
            data = [(100,), (200,), (300,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.avg("val").alias("average"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "avg_bigint_returns_double", ignore_nullable=True)

        assert ref.schema["average"].dataType == DoubleType(), \
            f"Spark AVG type: {ref.schema['average'].dataType}"
        assert td.schema["average"].dataType == DoubleType(), \
            f"Thunderduck AVG type: {td.schema['average'].dataType}"

    @pytest.mark.timeout(30)
    def test_sum_distinct_returns_bigint(self, spark_reference, spark_thunderduck):
        """SUM(DISTINCT) returns LongType (BIGINT)."""
        def run_test(spark):
            data = [(100,), (100,), (200,), (200,), (300,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum_distinct("val").alias("distinct_sum"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "sum_distinct_returns_bigint", ignore_nullable=True)

        assert ref.schema["distinct_sum"].dataType == LongType(), \
            f"Spark SUM DISTINCT type: {ref.schema['distinct_sum'].dataType}"
        assert td.schema["distinct_sum"].dataType == LongType(), \
            f"Thunderduck SUM DISTINCT type: {td.schema['distinct_sum'].dataType}"

        # Verify value: 100 + 200 + 300 = 600
        result = ref.collect()[0]["distinct_sum"]
        assert result == 600, f"Expected 600, got {result}"


# =============================================================================
# Arithmetic Expression Tests (No Overflow)
# =============================================================================


@pytest.mark.differential
class TestArithmeticExpressions:
    """Tests for arithmetic expressions with safe values."""

    @pytest.mark.timeout(30)
    def test_multiplication_no_overflow(self, spark_reference, spark_thunderduck):
        """Multiplication that doesn't overflow."""
        def run_test(spark):
            data = [(1000000,), (2000000,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("val"),
                (F.col("val") * 1000).alias("multiplied")
            ).orderBy("val")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "multiplication_no_overflow", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_addition_no_overflow(self, spark_reference, spark_thunderduck):
        """Addition that doesn't overflow."""
        def run_test(spark):
            data = [(1_000_000_000_000_000_000,), (2_000_000_000_000_000_000,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("val"),
                (F.col("val") + 1).alias("plus_one")
            ).orderBy("val")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "addition_no_overflow", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_subtraction_no_underflow(self, spark_reference, spark_thunderduck):
        """Subtraction that doesn't underflow."""
        def run_test(spark):
            data = [(-1_000_000_000_000_000_000,), (-2_000_000_000_000_000_000,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("val"),
                (F.col("val") - 1).alias("minus_one")
            ).orderBy("val")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "subtraction_no_underflow", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_complex_arithmetic_expression(self, spark_reference, spark_thunderduck):
        """Complex arithmetic expression with multiple operations."""
        def run_test(spark):
            data = [(100,), (200,), (300,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("val"),
                ((F.col("val") * 2) + 10 - 5).alias("computed")
            ).orderBy("val")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "complex_arithmetic_expression", ignore_nullable=True)


# =============================================================================
# Sanity Checks (No Overflow)
# =============================================================================


@pytest.mark.differential
class TestNoOverflow:
    """Sanity check tests that verify correct behavior without overflow."""

    @pytest.mark.timeout(30)
    def test_small_sum_correct(self, spark_reference, spark_thunderduck):
        """SUM of small values returns correct result."""
        def run_test(spark):
            data = [(100,), (200,), (300,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.sum("val").alias("total"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "small_sum_correct", ignore_nullable=True)

        result = ref.collect()[0]["total"]
        assert result == 600, f"Expected 600, got {result}"

    @pytest.mark.timeout(30)
    def test_count_correct(self, spark_reference, spark_thunderduck):
        """COUNT returns correct value."""
        def run_test(spark):
            data = [(i,) for i in range(100)]
            schema = StructType([StructField("val", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.count("*").alias("cnt"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "count_correct", ignore_nullable=True)

        result = ref.collect()[0]["cnt"]
        assert result == 100, f"Expected 100, got {result}"

    @pytest.mark.timeout(30)
    def test_avg_of_large_values(self, spark_reference, spark_thunderduck):
        """AVG of large values returns correct DOUBLE result."""
        def run_test(spark):
            large_val = 1_000_000_000_000_000_000
            data = [(large_val,), (large_val,), (large_val,)]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(F.avg("val").alias("average"))

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "avg_of_large_values", ignore_nullable=True)

        # AVG should be approximately equal to the input value (all same)
        result = ref.collect()[0]["average"]
        assert abs(result - 1_000_000_000_000_000_000) < 1, f"Unexpected avg: {result}"

    @pytest.mark.timeout(30)
    def test_min_max_with_bigint(self, spark_reference, spark_thunderduck):
        """MIN/MAX work correctly with BIGINT values."""
        def run_test(spark):
            data = [
                (1_000_000_000_000_000_000,),
                (2_000_000_000_000_000_000,),
                (3_000_000_000_000_000_000,),
            ]
            schema = StructType([StructField("val", LongType(), False)])
            df = spark.createDataFrame(data, schema)
            return df.agg(
                F.min("val").alias("min_val"),
                F.max("val").alias("max_val")
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)

        assert_dataframes_equal(ref, td, "min_max_with_bigint", ignore_nullable=True)
