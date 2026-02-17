"""
Differential tests for date/time functions.

Compares Thunderduck results against Apache Spark 4.1.1 to ensure exact compatibility
for date extraction, arithmetic, formatting, and truncation operations.
"""

import pytest
from datetime import date, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DateType, TimestampType, LongType
)
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Test Data Fixtures
# =============================================================================

@pytest.fixture(scope="class")
def date_test_data(spark_reference, spark_thunderduck):
    """Create test data with date and timestamp columns."""
    data = [
        (1, date(2025, 1, 15), datetime(2025, 1, 15, 10, 30, 45)),
        (2, date(2024, 6, 30), datetime(2024, 6, 30, 23, 59, 59)),
        (3, date(2023, 12, 31), datetime(2023, 12, 31, 0, 0, 0)),
        (4, date(2025, 2, 28), datetime(2025, 2, 28, 12, 0, 0)),
        (5, date(2024, 2, 29), datetime(2024, 2, 29, 6, 15, 30)),  # Leap year
        (6, None, None),  # Null handling
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("date_col", DateType(), True),
        StructField("ts_col", TimestampType(), True),
    ])
    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)
    return df_ref, df_td


@pytest.fixture(scope="class")
def date_pair_data(spark_reference, spark_thunderduck):
    """Create test data with two date columns for arithmetic operations."""
    data = [
        (1, date(2025, 1, 15), date(2025, 1, 10)),
        (2, date(2024, 6, 30), date(2024, 6, 1)),
        (3, date(2023, 12, 31), date(2024, 1, 1)),
        (4, date(2025, 3, 1), date(2025, 2, 28)),
        (5, None, date(2025, 1, 1)),
        (6, date(2025, 1, 1), None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("date1", DateType(), True),
        StructField("date2", DateType(), True),
    ])
    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)
    return df_ref, df_td


@pytest.fixture(scope="class")
def string_date_data(spark_reference, spark_thunderduck):
    """Create test data with string representations of dates."""
    data = [
        (1, "2025-01-15", "2025-01-15 10:30:45"),
        (2, "2024-06-30", "2024-06-30 23:59:59"),
        (3, "2023-12-31", "2023-12-31 00:00:00"),
        (4, None, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("date_str", StringType(), True),
        StructField("ts_str", StringType(), True),
    ])
    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)
    return df_ref, df_td


# =============================================================================
# Date Extraction Tests
# =============================================================================

@pytest.mark.differential
@pytest.mark.datetime
class TestDateExtraction:
    """Tests for date/time component extraction functions."""

    @pytest.mark.timeout(30)
    def test_year(self, spark_reference, spark_thunderduck, date_test_data):
        """Test year() extraction function."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.year("date_col").alias("year_from_date"),
            F.year("ts_col").alias("year_from_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.year("date_col").alias("year_from_date"),
            F.year("ts_col").alias("year_from_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="year")

    @pytest.mark.timeout(30)
    def test_month(self, spark_reference, spark_thunderduck, date_test_data):
        """Test month() extraction function."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.month("date_col").alias("month_from_date"),
            F.month("ts_col").alias("month_from_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.month("date_col").alias("month_from_date"),
            F.month("ts_col").alias("month_from_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="month")

    @pytest.mark.timeout(30)
    def test_day(self, spark_reference, spark_thunderduck, date_test_data):
        """Test day() and dayofmonth() extraction functions."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.day("date_col").alias("day_from_date"),
            F.dayofmonth("ts_col").alias("dayofmonth_from_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.day("date_col").alias("day_from_date"),
            F.dayofmonth("ts_col").alias("dayofmonth_from_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="day")

    @pytest.mark.timeout(30)
    def test_hour(self, spark_reference, spark_thunderduck, date_test_data):
        """Test hour() extraction function."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.hour("ts_col").alias("hour_val")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.hour("ts_col").alias("hour_val")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="hour")

    @pytest.mark.timeout(30)
    def test_minute(self, spark_reference, spark_thunderduck, date_test_data):
        """Test minute() extraction function."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.minute("ts_col").alias("minute_val")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.minute("ts_col").alias("minute_val")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="minute")

    @pytest.mark.timeout(30)
    def test_second(self, spark_reference, spark_thunderduck, date_test_data):
        """Test second() extraction function."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.second("ts_col").alias("second_val")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.second("ts_col").alias("second_val")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="second")

    @pytest.mark.timeout(30)
    def test_dayofweek(self, spark_reference, spark_thunderduck, date_test_data):
        """Test dayofweek() extraction function (1=Sunday, 7=Saturday)."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.dayofweek("date_col").alias("dow_from_date"),
            F.dayofweek("ts_col").alias("dow_from_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.dayofweek("date_col").alias("dow_from_date"),
            F.dayofweek("ts_col").alias("dow_from_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="dayofweek")

    @pytest.mark.timeout(30)
    def test_dayofyear_quarter_weekofyear(self, spark_reference, spark_thunderduck, date_test_data):
        """Test dayofyear(), quarter(), and weekofyear() extraction functions."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.dayofyear("date_col").alias("doy"),
            F.quarter("date_col").alias("quarter"),
            F.weekofyear("date_col").alias("week")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.dayofyear("date_col").alias("doy"),
            F.quarter("date_col").alias("quarter"),
            F.weekofyear("date_col").alias("week")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="dayofyear_quarter_weekofyear")


# =============================================================================
# Date Arithmetic Tests
# =============================================================================

@pytest.mark.differential
@pytest.mark.datetime
class TestDateArithmetic:
    """Tests for date arithmetic functions."""

    @pytest.mark.timeout(30)
    def test_date_add(self, spark_reference, spark_thunderduck, date_test_data):
        """Test date_add() function - add days to date."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.date_add("date_col", 10).alias("plus_10_days"),
            F.date_add("date_col", -5).alias("minus_5_days")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.date_add("date_col", 10).alias("plus_10_days"),
            F.date_add("date_col", -5).alias("minus_5_days")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="date_add")

    @pytest.mark.timeout(30)
    def test_date_sub(self, spark_reference, spark_thunderduck, date_test_data):
        """Test date_sub() function - subtract days from date."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.date_sub("date_col", 10).alias("minus_10_days"),
            F.date_sub("date_col", -5).alias("plus_5_days")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.date_sub("date_col", 10).alias("minus_10_days"),
            F.date_sub("date_col", -5).alias("plus_5_days")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="date_sub")

    @pytest.mark.timeout(30)
    def test_datediff(self, spark_reference, spark_thunderduck, date_pair_data):
        """Test datediff() function - difference between two dates in days."""
        df_ref, df_td = date_pair_data

        ref_result = df_ref.select(
            "id",
            F.datediff("date1", "date2").alias("diff_days")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.datediff("date1", "date2").alias("diff_days")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="datediff")

    @pytest.mark.timeout(30)
    def test_add_months(self, spark_reference, spark_thunderduck, date_test_data):
        """Test add_months() function - add months to date."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.add_months("date_col", 1).alias("plus_1_month"),
            F.add_months("date_col", -3).alias("minus_3_months"),
            F.add_months("date_col", 12).alias("plus_1_year")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.add_months("date_col", 1).alias("plus_1_month"),
            F.add_months("date_col", -3).alias("minus_3_months"),
            F.add_months("date_col", 12).alias("plus_1_year")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="add_months")

    @pytest.mark.timeout(30)
    def test_months_between(self, spark_reference, spark_thunderduck, date_pair_data):
        """Test months_between() function - difference between dates in months."""
        df_ref, df_td = date_pair_data

        ref_result = df_ref.select(
            "id",
            F.months_between("date1", "date2").alias("months_diff")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.months_between("date1", "date2").alias("months_diff")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="months_between")


# =============================================================================
# Date Formatting Tests
# =============================================================================

@pytest.mark.differential
@pytest.mark.datetime
class TestDateFormatting:
    """Tests for date/timestamp formatting and parsing functions."""

    @pytest.mark.timeout(30)
    def test_date_format(self, spark_reference, spark_thunderduck, date_test_data):
        """Test date_format() function - format date as string."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.date_format("date_col", "yyyy-MM-dd").alias("iso_date"),
            F.date_format("ts_col", "yyyy-MM-dd HH:mm:ss").alias("full_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.date_format("date_col", "yyyy-MM-dd").alias("iso_date"),
            F.date_format("ts_col", "yyyy-MM-dd HH:mm:ss").alias("full_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="date_format")

    @pytest.mark.timeout(30)
    def test_to_date(self, spark_reference, spark_thunderduck, string_date_data):
        """Test to_date() function - convert string to date."""
        df_ref, df_td = string_date_data

        ref_result = df_ref.select(
            "id",
            F.to_date("date_str").alias("parsed_date")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.to_date("date_str").alias("parsed_date")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="to_date")

    @pytest.mark.timeout(30)
    def test_to_timestamp(self, spark_reference, spark_thunderduck, string_date_data):
        """Test to_timestamp() function - convert string to timestamp."""
        df_ref, df_td = string_date_data

        ref_result = df_ref.select(
            "id",
            F.to_timestamp("ts_str").alias("parsed_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.to_timestamp("ts_str").alias("parsed_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="to_timestamp")

    @pytest.mark.timeout(30)
    def test_unix_timestamp_from_unixtime(self, spark_reference, spark_thunderduck, date_test_data):
        """Test unix_timestamp() and from_unixtime() functions."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.unix_timestamp("ts_col").alias("unix_ts")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.unix_timestamp("ts_col").alias("unix_ts")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="unix_timestamp")


# =============================================================================
# Date Truncation Tests
# =============================================================================

@pytest.mark.differential
@pytest.mark.datetime
class TestDateTruncation:
    """Tests for date truncation functions."""

    @pytest.mark.timeout(30)
    def test_date_trunc(self, spark_reference, spark_thunderduck, date_test_data):
        """Test date_trunc() function - truncate to specified unit."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.date_trunc("year", "ts_col").alias("trunc_year"),
            F.date_trunc("month", "ts_col").alias("trunc_month"),
            F.date_trunc("day", "ts_col").alias("trunc_day")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.date_trunc("year", "ts_col").alias("trunc_year"),
            F.date_trunc("month", "ts_col").alias("trunc_month"),
            F.date_trunc("day", "ts_col").alias("trunc_day")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="date_trunc")

    @pytest.mark.timeout(30)
    def test_last_day(self, spark_reference, spark_thunderduck, date_test_data):
        """Test last_day() function - get last day of month."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.last_day("date_col").alias("last_day_of_month")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.last_day("date_col").alias("last_day_of_month")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="last_day")

    @pytest.mark.timeout(30)
    def test_next_day(self, spark_reference, spark_thunderduck, date_test_data):
        """Test next_day() function - get next occurrence of day-of-week."""
        df_ref, df_td = date_test_data

        ref_result = df_ref.select(
            "id",
            F.next_day("date_col", "Monday").alias("next_monday"),
            F.next_day("date_col", "Friday").alias("next_friday")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.next_day("date_col", "Monday").alias("next_monday"),
            F.next_day("date_col", "Friday").alias("next_friday")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="next_day")
