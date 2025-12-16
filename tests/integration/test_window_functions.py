"""
Window Functions Differential Tests

Tests DataFrame API window functions for parity between Apache Spark 4.0.1 and Thunderduck.

Categories tested:
- Ranking functions: rank(), dense_rank(), row_number(), ntile(), percent_rank(), cume_dist()
- Analytic functions: lag(), lead(), first(), last(), nth_value()
- Frame specifications: ROWS BETWEEN, RANGE BETWEEN
- Aggregate window functions: sum(), avg(), min(), max() over windows
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture(scope="class")
def employee_data(spark_reference, spark_thunderduck):
    """Create employee test data for window function tests"""
    data = [
        ("Engineering", "Alice", 120000),
        ("Engineering", "Bob", 110000),
        ("Engineering", "Charlie", 110000),
        ("Engineering", "Diana", 95000),
        ("Sales", "Eve", 100000),
        ("Sales", "Frank", 95000),
        ("Sales", "Grace", 85000),
        ("Marketing", "Henry", 90000),
        ("Marketing", "Ivy", 85000),
        ("Marketing", "Jack", 80000),
    ]
    schema = StructType([
        StructField("department", StringType(), False),
        StructField("name", StringType(), False),
        StructField("salary", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def time_series_data(spark_reference, spark_thunderduck):
    """Create time series test data for lag/lead tests"""
    data = [
        ("Product A", 1, 100),
        ("Product A", 2, 120),
        ("Product A", 3, 115),
        ("Product A", 4, 130),
        ("Product A", 5, 125),
        ("Product B", 1, 200),
        ("Product B", 2, 210),
        ("Product B", 3, 195),
        ("Product B", 4, 220),
        ("Product B", 5, 230),
    ]
    schema = StructType([
        StructField("product", StringType(), False),
        StructField("month", IntegerType(), False),
        StructField("sales", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def stock_data(spark_reference, spark_thunderduck):
    """Create stock price data for frame specification tests"""
    data = [
        ("AAPL", 1, 150.0),
        ("AAPL", 2, 152.0),
        ("AAPL", 3, 148.0),
        ("AAPL", 4, 155.0),
        ("AAPL", 5, 160.0),
        ("AAPL", 6, 158.0),
        ("AAPL", 7, 162.0),
        ("GOOG", 1, 2800.0),
        ("GOOG", 2, 2850.0),
        ("GOOG", 3, 2780.0),
        ("GOOG", 4, 2900.0),
        ("GOOG", 5, 2950.0),
        ("GOOG", 6, 2920.0),
        ("GOOG", 7, 3000.0),
    ]
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("day", IntegerType(), False),
        StructField("price", DoubleType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


# ============================================================================
# Ranking Function Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.window
class TestRankingFunctions:
    """Tests for ranking window functions"""

    def test_row_number(self, spark_reference, spark_thunderduck, employee_data):
        """Test row_number() - assigns unique sequential numbers"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("row_num", F.row_number().over(window))
            .orderBy("department", "row_num"))

        result_td = (df_td
            .withColumn("row_num", F.row_number().over(window))
            .orderBy("department", "row_num"))

        assert_dataframes_equal(result_ref, result_td, "row_number")

    def test_rank(self, spark_reference, spark_thunderduck, employee_data):
        """Test rank() - same rank for ties, gaps after"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("rank", F.rank().over(window))
            .orderBy("department", "rank", "name"))

        result_td = (df_td
            .withColumn("rank", F.rank().over(window))
            .orderBy("department", "rank", "name"))

        assert_dataframes_equal(result_ref, result_td, "rank")

    def test_dense_rank(self, spark_reference, spark_thunderduck, employee_data):
        """Test dense_rank() - same rank for ties, no gaps"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("dense_rank", F.dense_rank().over(window))
            .orderBy("department", "dense_rank", "name"))

        result_td = (df_td
            .withColumn("dense_rank", F.dense_rank().over(window))
            .orderBy("department", "dense_rank", "name"))

        assert_dataframes_equal(result_ref, result_td, "dense_rank")

    def test_ntile(self, spark_reference, spark_thunderduck, employee_data):
        """Test ntile() - divides rows into n buckets"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("quartile", F.ntile(4).over(window))
            .orderBy("department", "quartile", "name"))

        result_td = (df_td
            .withColumn("quartile", F.ntile(4).over(window))
            .orderBy("department", "quartile", "name"))

        assert_dataframes_equal(result_ref, result_td, "ntile")

    def test_percent_rank(self, spark_reference, spark_thunderduck, employee_data):
        """Test percent_rank() - relative rank as percentage"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("pct_rank", F.percent_rank().over(window))
            .orderBy("department", "pct_rank", "name"))

        result_td = (df_td
            .withColumn("pct_rank", F.percent_rank().over(window))
            .orderBy("department", "pct_rank", "name"))

        assert_dataframes_equal(result_ref, result_td, "percent_rank", epsilon=1e-10)

    def test_cume_dist(self, spark_reference, spark_thunderduck, employee_data):
        """Test cume_dist() - cumulative distribution"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("cume_dist", F.cume_dist().over(window))
            .orderBy("department", "cume_dist", "name"))

        result_td = (df_td
            .withColumn("cume_dist", F.cume_dist().over(window))
            .orderBy("department", "cume_dist", "name"))

        assert_dataframes_equal(result_ref, result_td, "cume_dist", epsilon=1e-10)

    def test_multiple_ranking_functions(self, spark_reference, spark_thunderduck, employee_data):
        """Test multiple ranking functions together"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("row_num", F.row_number().over(window))
            .withColumn("rank", F.rank().over(window))
            .withColumn("dense_rank", F.dense_rank().over(window))
            .orderBy("department", "row_num"))

        result_td = (df_td
            .withColumn("row_num", F.row_number().over(window))
            .withColumn("rank", F.rank().over(window))
            .withColumn("dense_rank", F.dense_rank().over(window))
            .orderBy("department", "row_num"))

        assert_dataframes_equal(result_ref, result_td, "multiple_ranking")


# ============================================================================
# Analytic Function Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.window
class TestAnalyticFunctions:
    """Tests for analytic window functions (lag, lead, first, last)"""

    def test_lag_default(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lag() - access previous row value"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("prev_sales", F.lag("sales").over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("prev_sales", F.lag("sales").over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lag_default")

    def test_lag_with_offset(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lag() with custom offset"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("sales_2_months_ago", F.lag("sales", 2).over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("sales_2_months_ago", F.lag("sales", 2).over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lag_with_offset")

    def test_lag_with_default(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lag() with default value for nulls"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("prev_sales", F.lag("sales", 1, 0).over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("prev_sales", F.lag("sales", 1, 0).over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lag_with_default")

    def test_lead_default(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lead() - access next row value"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("next_sales", F.lead("sales").over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("next_sales", F.lead("sales").over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lead_default")

    def test_lead_with_offset(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lead() with custom offset"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("sales_2_months_ahead", F.lead("sales", 2).over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("sales_2_months_ahead", F.lead("sales", 2).over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lead_with_offset")

    def test_lead_with_default(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lead() with default value for nulls"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("next_sales", F.lead("sales", 1, -1).over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("next_sales", F.lead("sales", 1, -1).over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lead_with_default")

    def test_first(self, spark_reference, spark_thunderduck, employee_data):
        """Test first() - get first value in window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("top_earner_salary", F.first("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("top_earner_salary", F.first("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "first")

    def test_last(self, spark_reference, spark_thunderduck, employee_data):
        """Test last() - get last value in window"""
        df_ref, df_td = employee_data

        # Need a frame that includes all rows to get true last
        window = (Window.partitionBy("department")
            .orderBy(F.col("salary").desc())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

        result_ref = (df_ref
            .withColumn("lowest_salary", F.last("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("lowest_salary", F.last("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "last")

    def test_nth_value(self, spark_reference, spark_thunderduck, employee_data):
        """Test nth_value() - get nth value in window"""
        df_ref, df_td = employee_data

        window = (Window.partitionBy("department")
            .orderBy(F.col("salary").desc())
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

        result_ref = (df_ref
            .withColumn("second_highest", F.nth_value("salary", 2).over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("second_highest", F.nth_value("salary", 2).over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "nth_value")

    def test_lag_lead_combined(self, spark_reference, spark_thunderduck, time_series_data):
        """Test lag and lead together for period-over-period analysis"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("prev_sales", F.lag("sales").over(window))
            .withColumn("next_sales", F.lead("sales").over(window))
            .withColumn("change_from_prev", F.col("sales") - F.lag("sales").over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("prev_sales", F.lag("sales").over(window))
            .withColumn("next_sales", F.lead("sales").over(window))
            .withColumn("change_from_prev", F.col("sales") - F.lag("sales").over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "lag_lead_combined")


# ============================================================================
# Frame Specification Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.window
class TestFrameSpecifications:
    """Tests for window frame specifications (ROWS BETWEEN, RANGE BETWEEN)"""

    def test_rows_between_unbounded_preceding_current(self, spark_reference, spark_thunderduck, stock_data):
        """Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)"""
        df_ref, df_td = stock_data

        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))

        result_ref = (df_ref
            .withColumn("running_total", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("running_total", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "rows_running_total", epsilon=1e-6)

    def test_rows_between_fixed_preceding(self, spark_reference, spark_thunderduck, stock_data):
        """Test ROWS BETWEEN N PRECEDING AND CURRENT ROW (moving average)"""
        df_ref, df_td = stock_data

        # 3-day moving average
        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rowsBetween(-2, Window.currentRow))

        result_ref = (df_ref
            .withColumn("moving_avg_3day", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("moving_avg_3day", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "rows_moving_avg", epsilon=1e-6)

    def test_rows_between_preceding_following(self, spark_reference, spark_thunderduck, stock_data):
        """Test ROWS BETWEEN N PRECEDING AND N FOLLOWING (centered window)"""
        df_ref, df_td = stock_data

        # Centered 5-day window (-2 to +2)
        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rowsBetween(-2, 2))

        result_ref = (df_ref
            .withColumn("centered_avg", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("centered_avg", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "rows_centered", epsilon=1e-6)

    def test_rows_between_current_unbounded_following(self, spark_reference, spark_thunderduck, stock_data):
        """Test ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"""
        df_ref, df_td = stock_data

        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rowsBetween(Window.currentRow, Window.unboundedFollowing))

        result_ref = (df_ref
            .withColumn("remaining_sum", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("remaining_sum", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "rows_remaining_sum", epsilon=1e-6)

    def test_rows_unbounded_both(self, spark_reference, spark_thunderduck, stock_data):
        """Test ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (partition total)"""
        df_ref, df_td = stock_data

        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))

        result_ref = (df_ref
            .withColumn("partition_total", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("partition_total", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "rows_partition_total", epsilon=1e-6)

    def test_range_between_unbounded_current(self, spark_reference, spark_thunderduck, stock_data):
        """Test RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"""
        df_ref, df_td = stock_data

        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rangeBetween(Window.unboundedPreceding, Window.currentRow))

        result_ref = (df_ref
            .withColumn("range_sum", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("range_sum", F.sum("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "range_sum", epsilon=1e-6)

    def test_range_between_fixed(self, spark_reference, spark_thunderduck, stock_data):
        """Test RANGE BETWEEN with fixed boundaries"""
        df_ref, df_td = stock_data

        # Range based on value (day -1 to day +1)
        window = (Window.partitionBy("symbol")
            .orderBy("day")
            .rangeBetween(-1, 1))

        result_ref = (df_ref
            .withColumn("nearby_avg", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        result_td = (df_td
            .withColumn("nearby_avg", F.avg("price").over(window))
            .orderBy("symbol", "day"))

        assert_dataframes_equal(result_ref, result_td, "range_nearby", epsilon=1e-6)


# ============================================================================
# Aggregate Window Function Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.window
class TestAggregateWindowFunctions:
    """Tests for aggregate functions used in window context"""

    def test_sum_over_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test sum() over window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_total_salary", F.sum("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_total_salary", F.sum("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "sum_window")

    def test_avg_over_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test avg() over window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_avg_salary", F.avg("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_avg_salary", F.avg("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "avg_window", epsilon=1e-6)

    def test_min_max_over_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test min() and max() over window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_min_salary", F.min("salary").over(window))
            .withColumn("dept_max_salary", F.max("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_min_salary", F.min("salary").over(window))
            .withColumn("dept_max_salary", F.max("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "min_max_window")

    def test_count_over_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test count() over window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_count", F.count("*").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_count", F.count("*").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "count_window")

    def test_stddev_over_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test stddev() over window"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_stddev", F.stddev("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_stddev", F.stddev("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "stddev_window", epsilon=1e-6)

    def test_ratio_to_partition(self, spark_reference, spark_thunderduck, employee_data):
        """Test calculating ratio to partition total"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department")

        result_ref = (df_ref
            .withColumn("dept_total", F.sum("salary").over(window))
            .withColumn("salary_ratio", F.col("salary") / F.sum("salary").over(window))
            .orderBy("department", "name"))

        result_td = (df_td
            .withColumn("dept_total", F.sum("salary").over(window))
            .withColumn("salary_ratio", F.col("salary") / F.sum("salary").over(window))
            .orderBy("department", "name"))

        assert_dataframes_equal(result_ref, result_td, "ratio_to_partition", epsilon=1e-10)


# ============================================================================
# Advanced/Combined Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.window
class TestAdvancedWindowFunctions:
    """Advanced window function tests combining multiple features"""

    def test_multiple_windows(self, spark_reference, spark_thunderduck, employee_data):
        """Test using multiple different windows in same query"""
        df_ref, df_td = employee_data

        dept_window = Window.partitionBy("department")
        rank_window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("dept_avg", F.avg("salary").over(dept_window))
            .withColumn("dept_rank", F.rank().over(rank_window))
            .withColumn("diff_from_avg", F.col("salary") - F.avg("salary").over(dept_window))
            .orderBy("department", "dept_rank"))

        result_td = (df_td
            .withColumn("dept_avg", F.avg("salary").over(dept_window))
            .withColumn("dept_rank", F.rank().over(rank_window))
            .withColumn("diff_from_avg", F.col("salary") - F.avg("salary").over(dept_window))
            .orderBy("department", "dept_rank"))

        assert_dataframes_equal(result_ref, result_td, "multiple_windows", epsilon=1e-6)

    def test_window_with_filter(self, spark_reference, spark_thunderduck, employee_data):
        """Test window functions combined with filter"""
        df_ref, df_td = employee_data

        window = Window.partitionBy("department").orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= 2)
            .orderBy("department", "rank"))

        result_td = (df_td
            .withColumn("rank", F.rank().over(window))
            .filter(F.col("rank") <= 2)
            .orderBy("department", "rank"))

        assert_dataframes_equal(result_ref, result_td, "window_with_filter")

    def test_running_difference(self, spark_reference, spark_thunderduck, time_series_data):
        """Test calculating running difference (change from previous)"""
        df_ref, df_td = time_series_data

        window = Window.partitionBy("product").orderBy("month")

        result_ref = (df_ref
            .withColumn("prev_sales", F.lag("sales", 1, 0).over(window))
            .withColumn("change", F.col("sales") - F.lag("sales", 1, 0).over(window))
            .withColumn("pct_change",
                (F.col("sales") - F.lag("sales").over(window)) / F.lag("sales").over(window) * 100)
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("prev_sales", F.lag("sales", 1, 0).over(window))
            .withColumn("change", F.col("sales") - F.lag("sales", 1, 0).over(window))
            .withColumn("pct_change",
                (F.col("sales") - F.lag("sales").over(window)) / F.lag("sales").over(window) * 100)
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "running_difference", epsilon=1e-6)

    def test_cumulative_metrics(self, spark_reference, spark_thunderduck, time_series_data):
        """Test cumulative sum and average"""
        df_ref, df_td = time_series_data

        window = (Window.partitionBy("product")
            .orderBy("month")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))

        result_ref = (df_ref
            .withColumn("cumulative_sales", F.sum("sales").over(window))
            .withColumn("cumulative_avg", F.avg("sales").over(window))
            .orderBy("product", "month"))

        result_td = (df_td
            .withColumn("cumulative_sales", F.sum("sales").over(window))
            .withColumn("cumulative_avg", F.avg("sales").over(window))
            .orderBy("product", "month"))

        assert_dataframes_equal(result_ref, result_td, "cumulative_metrics", epsilon=1e-6)

    def test_global_window(self, spark_reference, spark_thunderduck, employee_data):
        """Test window without partition (global window)"""
        df_ref, df_td = employee_data

        window = Window.orderBy(F.col("salary").desc())

        result_ref = (df_ref
            .withColumn("global_rank", F.rank().over(window))
            .orderBy("global_rank", "name"))

        result_td = (df_td
            .withColumn("global_rank", F.rank().over(window))
            .orderBy("global_rank", "name"))

        assert_dataframes_equal(result_ref, result_td, "global_window")
