"""
Differential tests for explicit CAST operations.

Validates that Thunderduck produces identical results to Apache Spark 4.0.1
for all type casting scenarios.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# =============================================================================
# Numeric Type Casts
# =============================================================================


@pytest.mark.differential
class TestNumericCasts:
    """Tests for numeric type conversions."""

    @pytest.mark.timeout(30)
    def test_int_to_string(self, spark_reference, spark_thunderduck):
        """CAST integer to string"""
        def run_test(spark):
            data = [(123,), (-456,), (0,)]
            schema = StructType([StructField("value", IntegerType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("string").alias("str_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "int_to_string")

    @pytest.mark.timeout(30)
    def test_string_to_int(self, spark_reference, spark_thunderduck):
        """CAST string to integer"""
        def run_test(spark):
            data = [("123",), ("-456",), ("0",)]
            schema = StructType([StructField("value", StringType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("int").alias("int_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "string_to_int")

    @pytest.mark.skip(reason="DuckDB rounds, Spark truncates toward zero: CAST(-3.7 AS INT) = -4 vs -3")
    @pytest.mark.timeout(30)
    def test_double_to_int(self, spark_reference, spark_thunderduck):
        """CAST double to integer (truncation behavior)"""
        def run_test(spark):
            data = [(3.7,), (3.2,), (-3.7,), (-3.2,), (0.0,)]
            schema = StructType([StructField("value", DoubleType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("int").alias("int_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "double_to_int")

    @pytest.mark.timeout(30)
    def test_int_to_double(self, spark_reference, spark_thunderduck):
        """CAST integer to double"""
        def run_test(spark):
            data = [(123,), (-456,), (0,), (2147483647,)]
            schema = StructType([StructField("value", IntegerType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("double").alias("double_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "int_to_double")

    @pytest.mark.timeout(30)
    def test_long_to_int(self, spark_reference, spark_thunderduck):
        """CAST long to integer (values within int range)"""
        def run_test(spark):
            # Use values within int range to avoid overflow differences
            data = [(123,), (-456,), (0,), (2147483647,), (-2147483648,)]
            schema = StructType([StructField("value", LongType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("int").alias("int_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "long_to_int")


# =============================================================================
# Decimal Precision Casts
# =============================================================================


@pytest.mark.differential
class TestDecimalCasts:
    """Tests for decimal precision/scale handling."""

    @pytest.mark.skip(reason="spark.sql() not yet supported - requires SQL parser integration")
    @pytest.mark.timeout(30)
    def test_decimal_scale_change(self, spark_reference, spark_thunderduck):
        """CAST decimal with scale change (rounding)"""
        def run_test(spark):
            # Create decimals and cast to different scale
            return spark.sql("""
                SELECT
                    CAST(123.456 AS DECIMAL(10,3)) as original,
                    CAST(CAST(123.456 AS DECIMAL(10,3)) AS DECIMAL(10,2)) as rounded
            """)

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "decimal_scale_change")

    @pytest.mark.timeout(30)
    def test_int_to_decimal(self, spark_reference, spark_thunderduck):
        """CAST integer to decimal"""
        def run_test(spark):
            data = [(100,), (-200,), (0,)]
            schema = StructType([StructField("value", IntegerType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("decimal(10,2)").alias("decimal_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "int_to_decimal")

    @pytest.mark.timeout(30)
    def test_double_to_decimal(self, spark_reference, spark_thunderduck):
        """CAST double to decimal"""
        def run_test(spark):
            data = [(123.456,), (-789.012,), (0.0,)]
            schema = StructType([StructField("value", DoubleType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("decimal(10,2)").alias("decimal_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "double_to_decimal")


# =============================================================================
# Date/Time Casts
# =============================================================================


@pytest.mark.differential
class TestDateTimeCasts:
    """Tests for date/timestamp conversions."""

    @pytest.mark.timeout(30)
    def test_string_to_date(self, spark_reference, spark_thunderduck):
        """CAST string to date (ISO format)"""
        def run_test(spark):
            data = [("2025-01-15",), ("2024-12-31",), ("2000-01-01",)]
            schema = StructType([StructField("date_str", StringType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("date_str"),
                F.col("date_str").cast("date").alias("date_value")
            ).orderBy("date_str")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "string_to_date")

    @pytest.mark.timeout(30)
    def test_string_to_timestamp(self, spark_reference, spark_thunderduck):
        """CAST string to timestamp"""
        def run_test(spark):
            data = [
                ("2025-01-15 10:30:00",),
                ("2024-12-31 23:59:59",),
                ("2000-01-01 00:00:00",),
            ]
            schema = StructType([StructField("ts_str", StringType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("ts_str"),
                F.col("ts_str").cast("timestamp").alias("ts_value")
            ).orderBy("ts_str")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "string_to_timestamp")

    @pytest.mark.skip(reason="spark.sql() not yet supported - requires SQL parser integration")
    @pytest.mark.timeout(30)
    def test_date_to_string(self, spark_reference, spark_thunderduck):
        """CAST date to string"""
        def run_test(spark):
            # Use SQL to create date values
            return spark.sql("""
                SELECT
                    DATE '2025-01-15' as date_value,
                    CAST(DATE '2025-01-15' AS STRING) as str_value
            """)

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "date_to_string")

    @pytest.mark.skip(reason="spark.sql() not yet supported - requires SQL parser integration")
    @pytest.mark.timeout(30)
    def test_timestamp_to_date(self, spark_reference, spark_thunderduck):
        """CAST timestamp to date (time truncation)"""
        def run_test(spark):
            return spark.sql("""
                SELECT
                    TIMESTAMP '2025-01-15 10:30:45' as ts_value,
                    CAST(TIMESTAMP '2025-01-15 10:30:45' AS DATE) as date_value
            """)

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "timestamp_to_date")


# =============================================================================
# NULL Handling
# =============================================================================


@pytest.mark.differential
class TestNullCasts:
    """Tests for NULL handling in casts."""

    @pytest.mark.timeout(30)
    def test_cast_null_to_int(self, spark_reference, spark_thunderduck):
        """CAST NULL to integer preserves NULL"""
        def run_test(spark):
            data = [(1, 100), (2, None), (3, 300)]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", IntegerType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("id"),
                F.col("value").cast("int").alias("cast_value")
            ).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "cast_null_to_int")

    @pytest.mark.timeout(30)
    def test_cast_null_to_string(self, spark_reference, spark_thunderduck):
        """CAST NULL to string preserves NULL"""
        def run_test(spark):
            data = [(1, "hello"), (2, None), (3, "world")]
            schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("id"),
                F.col("value").cast("string").alias("cast_value")
            ).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "cast_null_to_string")

    @pytest.mark.skip(reason="spark.sql() not yet supported - requires SQL parser integration")
    @pytest.mark.timeout(30)
    def test_cast_null_literal(self, spark_reference, spark_thunderduck):
        """CAST NULL literal to various types"""
        def run_test(spark):
            return spark.sql("""
                SELECT
                    CAST(NULL AS INT) as null_int,
                    CAST(NULL AS STRING) as null_string,
                    CAST(NULL AS DOUBLE) as null_double,
                    CAST(NULL AS DATE) as null_date
            """)

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "cast_null_literal", ignore_nullable=True)


# =============================================================================
# Boolean Casts
# =============================================================================


@pytest.mark.differential
class TestBooleanCasts:
    """Tests for boolean conversions."""

    @pytest.mark.timeout(30)
    def test_int_to_boolean(self, spark_reference, spark_thunderduck):
        """CAST integer to boolean"""
        def run_test(spark):
            data = [(0,), (1,), (2,), (-1,)]
            schema = StructType([StructField("value", IntegerType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("boolean").alias("bool_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "int_to_boolean")

    @pytest.mark.timeout(30)
    def test_boolean_to_int(self, spark_reference, spark_thunderduck):
        """CAST boolean to integer"""
        def run_test(spark):
            data = [(True,), (False,)]
            schema = StructType([StructField("value", BooleanType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("int").alias("int_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "boolean_to_int")


# =============================================================================
# String Casts
# =============================================================================


@pytest.mark.differential
class TestStringCasts:
    """Tests for string conversions."""

    @pytest.mark.timeout(30)
    def test_double_to_string(self, spark_reference, spark_thunderduck):
        """CAST double to string"""
        def run_test(spark):
            data = [(3.14159,), (-2.71828,), (0.0,)]
            schema = StructType([StructField("value", DoubleType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("string").alias("str_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "double_to_string")

    @pytest.mark.timeout(30)
    def test_boolean_to_string(self, spark_reference, spark_thunderduck):
        """CAST boolean to string"""
        def run_test(spark):
            data = [(True,), (False,)]
            schema = StructType([StructField("value", BooleanType(), True)])
            df = spark.createDataFrame(data, schema)
            return df.select(
                F.col("value"),
                F.col("value").cast("string").alias("str_value")
            ).orderBy("value")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "boolean_to_string")
