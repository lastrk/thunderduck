"""
Differential tests for math, bitwise, and date/time functions.

Validates that Thunderduck produces identical results to Apache Spark
for newly added function mappings.

Uses spark.sql() with temp views to ensure functions go through
SparkSQLParser → FunctionRegistry translation path.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
)


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


def _create_int_data(spark, view_name="int_data"):
    data = [(i,) for i in range(1, 9)]
    schema = StructType([StructField("id", IntegerType(), False)])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_double_data(spark, view_name="double_data"):
    data = [
        (1, 0.0), (2, 1.0), (3, 8.0), (4, 27.0),
        (5, 64.0), (6, -27.0), (7, 125.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_mixed_data(spark, view_name="mixed_data"):
    data = [
        (1, 10.5), (2, -3.2), (3, 0.0), (4, 99.9), (5, -50.0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", DoubleType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_date_data(spark, view_name="date_data"):
    data = [
        (1, 1, 15), (2, 3, 1), (3, 6, 30), (4, 9, 10), (5, 12, 31),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("month_val", IntegerType(), False),
        StructField("day_val", IntegerType(), False),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


# =============================================================================
# Math Functions
# =============================================================================


@pytest.mark.differential
class TestMathFunctions_Differential:
    """Tests for math functions: factorial, cbrt, width_bucket, bin, hex, unhex, negative, positive."""

    @pytest.mark.timeout(30)
    def test_factorial(self, spark_reference, spark_thunderduck):
        """Test factorial() on small integers."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT id, factorial(id) as fact FROM int_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "factorial", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_cbrt(self, spark_reference, spark_thunderduck):
        """Test cbrt() cube root on double values."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql("SELECT id, cbrt(val) as cube_root FROM double_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "cbrt", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_width_bucket(self, spark_reference, spark_thunderduck):
        """Test width_bucket() distributes values into equi-width histogram buckets."""
        def run_test(spark):
            data = [
                (1, 5.0), (2, 15.0), (3, 25.0), (4, 50.0),
                (5, 75.0), (6, 95.0), (7, 100.0),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("val", DoubleType(), False),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("bucket_data")
            return spark.sql(
                "SELECT id, CAST(width_bucket(val, 0, 100, 10) AS BIGINT) as bucket "
                "FROM bucket_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "width_bucket", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bin(self, spark_reference, spark_thunderduck):
        """Test bin() converts integers to binary string representation."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT id, bin(id) as binary_str FROM int_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bin", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_hex(self, spark_reference, spark_thunderduck):
        """Test hex() converts integers to hexadecimal string representation."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT id, hex(id) as hex_str FROM int_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "hex", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_unhex(self, spark_reference, spark_thunderduck):
        """Test unhex(hex(id)) round-trip returns original binary representation."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT id, unhex(hex(id)) as raw FROM int_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "unhex", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_negative(self, spark_reference, spark_thunderduck):
        """Test negative() negates numeric values."""
        def run_test(spark):
            _create_mixed_data(spark)
            return spark.sql(
                "SELECT id, negative(val) as neg_val, negative(id) as neg_id "
                "FROM mixed_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "negative", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_positive(self, spark_reference, spark_thunderduck):
        """Test positive() returns the value unchanged (identity for numerics)."""
        def run_test(spark):
            _create_mixed_data(spark)
            return spark.sql(
                "SELECT id, positive(val) as pos FROM mixed_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "positive", ignore_nullable=True)


# =============================================================================
# Bitwise Functions
# =============================================================================


@pytest.mark.differential
class TestBitwiseFunctions_Differential:
    """Tests for bitwise functions: bit_count, bit_get/getbit, shiftleft, shiftright."""

    @pytest.mark.timeout(30)
    def test_bit_count(self, spark_reference, spark_thunderduck):
        """Test bit_count() returns the number of set bits in an integer."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql("SELECT id, bit_count(id) as bits FROM int_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_count", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bit_get(self, spark_reference, spark_thunderduck):
        """Test bit_get() returns the bit value at a given position."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql(
                "SELECT id, bit_get(id, 0) as bit0, bit_get(id, 1) as bit1, "
                "bit_get(id, 2) as bit2 FROM int_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_get", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_shiftleft(self, spark_reference, spark_thunderduck):
        """Test shiftleft() performs bitwise left shift."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql(
                "SELECT id, shiftleft(id, 2) as shifted FROM int_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "shiftleft", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_shiftright(self, spark_reference, spark_thunderduck):
        """Test shiftright() performs bitwise right shift."""
        def run_test(spark):
            _create_int_data(spark)
            return spark.sql(
                "SELECT id, shiftright(id, 1) as shifted FROM int_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "shiftright", ignore_nullable=True)


# =============================================================================
# Date Functions
# =============================================================================


@pytest.mark.differential
class TestDateFunctions_Differential:
    """Tests for date functions: make_date, dayname, monthname."""

    @pytest.mark.timeout(30)
    def test_make_date(self, spark_reference, spark_thunderduck):
        """Test make_date() constructs a date from year, month, day components."""
        def run_test(spark):
            _create_date_data(spark)
            return spark.sql(
                "SELECT id, make_date(2024, month_val, day_val) as dt "
                "FROM date_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "make_date", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_dayname(self, spark_reference, spark_thunderduck):
        """Test dayname() returns the day-of-week name for a date."""
        def run_test(spark):
            _create_date_data(spark)
            return spark.sql(
                "SELECT id, dayname(make_date(2024, month_val, day_val)) as day_name "
                "FROM date_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "dayname", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_monthname(self, spark_reference, spark_thunderduck):
        """Test monthname() returns the month name for a date."""
        def run_test(spark):
            _create_date_data(spark)
            return spark.sql(
                "SELECT id, monthname(make_date(2024, month_val, day_val)) as month_name "
                "FROM date_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "monthname", ignore_nullable=True)
