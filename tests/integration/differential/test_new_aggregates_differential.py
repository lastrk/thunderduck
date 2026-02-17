"""
Differential tests for newly added aggregate and statistical functions.

Validates that Thunderduck produces identical results to Apache Spark
for percentile, kurtosis, skewness, regression, and other aggregate functions.

Uses spark.sql() with temp views to ensure functions go through
SparkSQLParser → FunctionRegistry translation path.
"""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


def _create_int_data(spark, view_name="int_data"):
    data = [
        (1, 10), (2, 30), (3, 55), (4, 60), (5, 75),
        (6, 80), (7, 90), (8, 25), (9, 50), (10, 100),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_double_data(spark, view_name="dbl_data"):
    data = [
        (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0),
        (6, 60.0), (7, 70.0), (8, 80.0), (9, 90.0), (10, 100.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_bool_data(spark, all_true=True, view_name="bool_data"):
    if all_true:
        data = [
            (1, True), (2, True), (3, True), (4, True), (5, True),
        ]
    else:
        data = [
            (1, True), (2, False), (3, True), (4, True), (5, True),
        ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("flag", BooleanType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_bool_or_data(spark, any_true=True, view_name="bool_or_data"):
    if any_true:
        data = [
            (1, False), (2, False), (3, True), (4, False), (5, False),
        ]
    else:
        data = [
            (1, False), (2, False), (3, False), (4, False), (5, False),
        ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("flag", BooleanType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_bit_data(spark, view_name="bit_data"):
    data = [
        (1, 15), (2, 7), (3, 3), (4, 1),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_name_val_data(spark, view_name="name_val_data"):
    data = [
        (1, "Alice", 85), (2, "Bob", 92), (3, "Charlie", 78),
        (4, "Diana", 95), (5, "Eve", 88),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("val", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_stats_data(spark, view_name="stats_data"):
    data = [
        (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0),
        (6, 60.0), (7, 70.0), (8, 80.0), (9, 90.0), (10, 100.0),
        (11, 15.0), (12, 25.0), (13, 55.0), (14, 75.0), (15, 95.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_regression_data(spark, view_name="regr_data"):
    data = [
        (1, 1.0, 12.1), (2, 2.0, 14.3), (3, 3.0, 15.8),
        (4, 4.0, 18.2), (5, 5.0, 20.1), (6, 6.0, 22.4),
        (7, 7.0, 23.9), (8, 8.0, 26.1), (9, 9.0, 28.3),
        (10, 10.0, 30.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("x", DoubleType(), False),
        StructField("y", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_grouped_data(spark, view_name="grouped_data"):
    data = [
        (1, "A", 10), (2, "A", 60), (3, "A", 70), (4, "A", 80),
        (5, "A", 30), (6, "A", 90), (7, "A", 55), (8, "A", 45),
        (9, "B", 20), (10, "B", 75), (11, "B", 85), (12, "B", 95),
        (13, "B", 40), (14, "B", 65), (15, "B", 15), (16, "B", 50),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("val", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_grouped_bool_data(spark, view_name="grouped_bool_data"):
    data = [
        (1, "A", True), (2, "A", True), (3, "A", True), (4, "A", True),
        (5, "B", True), (6, "B", False), (7, "B", True), (8, "B", True),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("flag", BooleanType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_grouped_stats_data(spark, view_name="grouped_stats_data"):
    data = [
        # Group A: roughly uniform distribution
        (1, "A", 10.0), (2, "A", 20.0), (3, "A", 30.0), (4, "A", 40.0),
        (5, "A", 50.0), (6, "A", 60.0), (7, "A", 70.0), (8, "A", 80.0),
        (9, "A", 90.0), (10, "A", 100.0),
        # Group B: values clustered near center with outliers
        (11, "B", 5.0), (12, "B", 48.0), (13, "B", 49.0), (14, "B", 50.0),
        (15, "B", 50.0), (16, "B", 51.0), (17, "B", 52.0), (18, "B", 95.0),
        (19, "B", 50.0), (20, "B", 50.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("val", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


# =============================================================================
# Basic Aggregate Functions
# =============================================================================


@pytest.mark.differential
class TestNewAggregates_Differential:
    """Tests for newly added basic aggregate functions (count_if, median, bool_and/or, bit ops, max_by/min_by)."""

    @pytest.mark.timeout(30)
    def test_count_if(self, spark_reference, spark_thunderduck):
        """count_if counts rows matching a predicate."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT count_if(val > 50) as cnt FROM int_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_if", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_median(self, spark_reference, spark_thunderduck):
        """median returns the 50th percentile of numeric data."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql("SELECT median(val) as med FROM dbl_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "median", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bool_and(self, spark_reference, spark_thunderduck):
        """bool_and (every) returns true only if all values are true."""
        def run_test(spark):
            _create_bool_data(spark, all_true=True)
            return spark.sql("SELECT bool_and(flag) as all_true FROM bool_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bool_and_all_true", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bool_and_with_false(self, spark_reference, spark_thunderduck):
        """bool_and returns false when at least one value is false."""
        def run_test(spark):
            _create_bool_data(spark, all_true=False)
            return spark.sql("SELECT bool_and(flag) as all_true FROM bool_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bool_and_with_false", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bool_or(self, spark_reference, spark_thunderduck):
        """bool_or (some/any) returns true if any value is true."""
        def run_test(spark):
            _create_bool_or_data(spark, any_true=True)
            return spark.sql("SELECT bool_or(flag) as any_true FROM bool_or_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bool_or", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bool_or_all_false(self, spark_reference, spark_thunderduck):
        """bool_or returns false when all values are false."""
        def run_test(spark):
            _create_bool_or_data(spark, any_true=False)
            return spark.sql("SELECT bool_or(flag) as any_true FROM bool_or_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bool_or_all_false", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bit_and(self, spark_reference, spark_thunderduck):
        """bit_and computes bitwise AND across all values."""
        def run_test(spark):
            _create_bit_data(spark)
            return spark.sql("SELECT bit_and(val) as result FROM bit_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_and", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bit_or(self, spark_reference, spark_thunderduck):
        """bit_or computes bitwise OR across all values."""
        def run_test(spark):
            data = [(1, 1), (2, 2), (3, 4), (4, 8)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("val", IntegerType(), False),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("bit_or_data")
            return spark.sql("SELECT bit_or(val) as result FROM bit_or_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_or", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bit_xor(self, spark_reference, spark_thunderduck):
        """bit_xor computes bitwise XOR across all values."""
        def run_test(spark):
            data = [(1, 5), (2, 3), (3, 7), (4, 1)]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("val", IntegerType(), False),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("bit_xor_data")
            return spark.sql("SELECT bit_xor(val) as result FROM bit_xor_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_xor", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_max_by(self, spark_reference, spark_thunderduck):
        """max_by returns the value of the first column at the row where the second column is maximum."""
        def run_test(spark):
            _create_name_val_data(spark)
            return spark.sql("SELECT max_by(name, val) as result FROM name_val_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "max_by", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_min_by(self, spark_reference, spark_thunderduck):
        """min_by returns the value of the first column at the row where the second column is minimum."""
        def run_test(spark):
            _create_name_val_data(spark)
            return spark.sql("SELECT min_by(name, val) as result FROM name_val_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "min_by", ignore_nullable=True)


# =============================================================================
# Statistical Aggregate Functions
# =============================================================================


@pytest.mark.differential
class TestStatisticalAggregates_Differential:
    """Tests for statistical aggregate functions (kurtosis, skewness, percentile, percentile_approx)."""

    @pytest.mark.timeout(30)
    def test_kurtosis(self, spark_reference, spark_thunderduck):
        """kurtosis computes the excess kurtosis of a numeric column (requires >= 4 rows)."""
        def run_test(spark):
            _create_stats_data(spark)
            return spark.sql("SELECT kurtosis(val) as kurt FROM stats_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "kurtosis", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_skewness(self, spark_reference, spark_thunderduck):
        """skewness computes the skewness of a numeric column (requires >= 3 rows).

        Skipped in relaxed/auto mode: DuckDB's built-in skewness() uses sample
        bias correction while Spark uses population skewness — different algorithm.
        In strict mode, spark_skewness() extension matches exactly.
        """
        compat_mode = os.environ.get('THUNDERDUCK_COMPAT_MODE', 'auto').lower()
        if compat_mode != 'strict':
            pytest.skip("skewness: DuckDB uses sample formula, Spark uses population formula (strict mode required)")

        def run_test(spark):
            _create_stats_data(spark)
            return spark.sql("SELECT skewness(val) as skew FROM stats_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "skewness", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_percentile_p50(self, spark_reference, spark_thunderduck):
        """percentile at 0.5 returns the median."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql("SELECT percentile(val, 0.5) as p50 FROM dbl_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "percentile_p50", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_percentile_p25(self, spark_reference, spark_thunderduck):
        """percentile at 0.25 returns the first quartile."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql("SELECT percentile(val, 0.25) as p25 FROM dbl_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "percentile_p25", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_percentile_p75(self, spark_reference, spark_thunderduck):
        """percentile at 0.75 returns the third quartile."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql("SELECT percentile(val, 0.75) as p75 FROM dbl_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "percentile_p75", ignore_nullable=True)

    @pytest.mark.skip(reason="DuckDB approx_quantile returns 55.0, Spark percentile_approx returns 50.0 — different approximation algorithms")
    @pytest.mark.timeout(30)
    def test_percentile_approx(self, spark_reference, spark_thunderduck):
        """percentile_approx returns an approximate percentile."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql(
                "SELECT percentile_approx(val, 0.5) as p50_approx FROM dbl_data"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "percentile_approx", ignore_nullable=True)


# =============================================================================
# Regression Aggregate Functions
# =============================================================================


@pytest.mark.differential
class TestRegressionAggregates_Differential:
    """Tests for regression aggregate functions (regr_count, regr_r2, regr_slope, etc.)."""

    @pytest.mark.timeout(30)
    def test_regr_count(self, spark_reference, spark_thunderduck):
        """regr_count returns the count of non-null (y, x) pairs."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT CAST(regr_count(y, x) AS BIGINT) as cnt FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_count", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_r2(self, spark_reference, spark_thunderduck):
        """regr_r2 returns the R-squared (coefficient of determination)."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_r2(y, x) as r2 FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_r2", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_slope(self, spark_reference, spark_thunderduck):
        """regr_slope returns the slope of the least-squares regression line."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_slope(y, x) as slope FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_slope", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_intercept(self, spark_reference, spark_thunderduck):
        """regr_intercept returns the y-intercept of the least-squares regression line."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_intercept(y, x) as intercept FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_intercept", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_avgx(self, spark_reference, spark_thunderduck):
        """regr_avgx returns the average of x for non-null pairs."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_avgx(y, x) as avgx FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_avgx", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_avgy(self, spark_reference, spark_thunderduck):
        """regr_avgy returns the average of y for non-null pairs."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_avgy(y, x) as avgy FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_avgy", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_sxx(self, spark_reference, spark_thunderduck):
        """regr_sxx returns the sum of squares of x deviations."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_sxx(y, x) as sxx FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_sxx", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_syy(self, spark_reference, spark_thunderduck):
        """regr_syy returns the sum of squares of y deviations."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_syy(y, x) as syy FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_syy", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_regr_sxy(self, spark_reference, spark_thunderduck):
        """regr_sxy returns the sum of cross products of x and y deviations."""
        def run_test(spark):
            _create_regression_data(spark)
            return spark.sql("SELECT regr_sxy(y, x) as sxy FROM regr_data")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "regr_sxy", ignore_nullable=True)


# =============================================================================
# Grouped Aggregate Functions
# =============================================================================


@pytest.mark.differential
class TestGroupedNewAggregates_Differential:
    """Tests for newly added aggregate functions with GROUP BY."""

    @pytest.mark.timeout(30)
    def test_count_if_grouped(self, spark_reference, spark_thunderduck):
        """count_if with GROUP BY counts matching rows per group."""
        def run_test(spark):
            _create_grouped_data(spark)
            return spark.sql(
                "SELECT category, count_if(val > 50) as cnt_above_50 "
                "FROM grouped_data GROUP BY category ORDER BY category"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "count_if_grouped", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bool_and_grouped(self, spark_reference, spark_thunderduck):
        """bool_and with GROUP BY checks if all values are true per group."""
        def run_test(spark):
            _create_grouped_bool_data(spark)
            return spark.sql(
                "SELECT category, bool_and(flag) as all_true "
                "FROM grouped_bool_data GROUP BY category ORDER BY category"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bool_and_grouped", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_kurtosis_grouped(self, spark_reference, spark_thunderduck):
        """kurtosis with GROUP BY computes excess kurtosis per group (requires >= 4 rows per group)."""
        def run_test(spark):
            _create_grouped_stats_data(spark)
            return spark.sql(
                "SELECT category, kurtosis(val) as kurt "
                "FROM grouped_stats_data GROUP BY category ORDER BY category"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "kurtosis_grouped", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_percentile_grouped(self, spark_reference, spark_thunderduck):
        """percentile with GROUP BY computes the 50th percentile per group."""
        def run_test(spark):
            _create_grouped_stats_data(spark)
            return spark.sql(
                "SELECT category, percentile(val, 0.5) as p50 "
                "FROM grouped_stats_data GROUP BY category ORDER BY category"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "percentile_grouped", ignore_nullable=True)
