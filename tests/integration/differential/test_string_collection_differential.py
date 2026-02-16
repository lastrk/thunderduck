"""
Differential tests for string and collection functions.

Validates that Thunderduck produces identical results to Apache Spark
for newly added function mappings.

Uses spark.sql() with temp views to ensure functions go through
SparkSQLParser → FunctionRegistry translation path.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


def _create_string_data(spark, view_name="str_data"):
    data = [
        (1, "Robert"),
        (2, "Rupert"),
        (3, "Ashcraft"),
        (4, "Ashcroft"),
        (5, "Tymczak"),
        (6, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_pair_data(spark, view_name="pair_data"):
    data = [
        (1, "kitten", "sitting"),
        (2, "hello", "hallo"),
        (3, "abc", "abc"),
        (4, "", "test"),
        (5, "spark", "park"),
        (6, None, "test"),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name1", StringType()),
        StructField("name2", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_padded_data(spark, view_name="padded_data"):
    data = [
        (1, "  hello  "),
        (2, "  world"),
        (3, "test  "),
        (4, "  "),
        (5, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("padded", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_email_data(spark, view_name="email_data"):
    data = [
        (1, "alice@example.com"),
        (2, "bob@company.org"),
        (3, "charlie@test.net"),
        (4, "no-at-sign"),
        (5, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("email", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_domain_data(spark, view_name="domain_data"):
    data = [
        (1, "www.example.com"),
        (2, "mail.server.example.org"),
        (3, "nodots"),
        (4, "one.dot"),
        (5, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("domain", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_double_data(spark, view_name="dbl_data"):
    data = [
        (1, 12345.6789),
        (2, 0.5),
        (3, 1000000.0),
        (4, -9876.54),
        (5, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("val", DoubleType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_date_data(spark, view_name="dt_data"):
    from datetime import date
    data = [
        (1, date(2025, 1, 15)),
        (2, date(2024, 6, 30)),
        (3, date(2023, 12, 31)),
        (4, date(2020, 2, 29)),
        (5, None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("dt", DateType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


# =============================================================================
# String Functions
# =============================================================================


@pytest.mark.differential
class TestStringFunctions_Differential:
    """Differential tests for string functions: soundex, levenshtein, overlay,
    left, right, split_part, translate, btrim, char_length, octet_length,
    bit_length, format_number, substring_index, to_char, encode, decode."""

    @pytest.mark.timeout(30)
    def test_soundex(self, spark_reference, spark_thunderduck):
        """Test soundex() phonetic encoding of strings."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql("SELECT id, soundex(name) as sdx FROM str_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "soundex", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_levenshtein(self, spark_reference, spark_thunderduck):
        """Test levenshtein() edit distance between string pairs."""
        def run_test(spark):
            _create_pair_data(spark)
            return spark.sql(
                "SELECT id, levenshtein(name1, name2) as dist FROM pair_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "levenshtein", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_overlay(self, spark_reference, spark_thunderduck):
        """Test overlay() replacing a substring at a position."""
        def run_test(spark):
            data = [
                (1, "abcdefgh"),
                (2, "hello world"),
                (3, "short"),
                (4, "ABCDEFGHIJ"),
                (5, None),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("str_val", StringType()),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("overlay_data")
            return spark.sql(
                "SELECT id, overlay(str_val PLACING 'XX' FROM 2 FOR 3) as result "
                "FROM overlay_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "overlay", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_left(self, spark_reference, spark_thunderduck):
        """Test left() extracting leftmost N characters."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql("SELECT id, left(name, 3) as l FROM str_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "left", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_right(self, spark_reference, spark_thunderduck):
        """Test right() extracting rightmost N characters."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql("SELECT id, right(name, 3) as r FROM str_data ORDER BY id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "right", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_split_part(self, spark_reference, spark_thunderduck):
        """Test split_part() extracting part of a delimited string."""
        def run_test(spark):
            _create_email_data(spark)
            return spark.sql(
                "SELECT id, split_part(email, '@', 1) as user_part "
                "FROM email_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "split_part", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_translate(self, spark_reference, spark_thunderduck):
        """Test translate() character-by-character replacement."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, translate(name, 'aeiou', '12345') as translated "
                "FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "translate", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_btrim(self, spark_reference, spark_thunderduck):
        """Test btrim() trimming whitespace from both sides."""
        def run_test(spark):
            _create_padded_data(spark)
            return spark.sql(
                "SELECT id, btrim(padded) as trimmed FROM padded_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "btrim", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_char_length(self, spark_reference, spark_thunderduck):
        """Test char_length() / character_length() returning character count."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, char_length(name) as len, character_length(name) as len2 "
                "FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "char_length", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_octet_length(self, spark_reference, spark_thunderduck):
        """Test octet_length() returning byte count of string."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, octet_length(name) as octets FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "octet_length", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_bit_length(self, spark_reference, spark_thunderduck):
        """Test bit_length() returning bit count of string."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, bit_length(name) as bits FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "bit_length", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_format_number(self, spark_reference, spark_thunderduck):
        """Test format_number() formatting numeric values with decimal places."""
        def run_test(spark):
            _create_double_data(spark)
            return spark.sql(
                "SELECT id, format_number(val, 2) as formatted FROM dbl_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "format_number", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_substring_index(self, spark_reference, spark_thunderduck):
        """Test substring_index() extracting substring before Nth delimiter."""
        def run_test(spark):
            _create_domain_data(spark)
            return spark.sql(
                "SELECT id, substring_index(domain, '.', 2) as result "
                "FROM domain_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "substring_index", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_to_char(self, spark_reference, spark_thunderduck):
        """Test to_char() formatting a date column as a string."""
        def run_test(spark):
            _create_date_data(spark)
            return spark.sql(
                "SELECT id, to_char(dt, 'yyyy-MM-dd') as formatted "
                "FROM dt_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "to_char", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_encode(self, spark_reference, spark_thunderduck):
        """Test encode() converting string to binary with specified charset."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, encode(name, 'UTF-8') as encoded FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "encode", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_decode_roundtrip(self, spark_reference, spark_thunderduck):
        """Test decode(encode(...)) round-trip preserving original string."""
        def run_test(spark):
            _create_string_data(spark)
            return spark.sql(
                "SELECT id, decode(encode(name, 'UTF-8'), 'UTF-8') as decoded "
                "FROM str_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "decode_roundtrip", ignore_nullable=True)


# =============================================================================
# Collection Functions
# =============================================================================


@pytest.mark.differential
class TestCollectionFunctions_Differential:
    """Differential tests for collection functions: cardinality, array_append,
    array_prepend, array_remove, array_compact, sequence."""

    @pytest.mark.timeout(30)
    def test_cardinality(self, spark_reference, spark_thunderduck):
        """Test cardinality() returning the number of elements in an array."""
        def run_test(spark):
            data = [(1,), (2,), (3,), (4,), (5,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("card_data")
            return spark.sql(
                "SELECT id, cardinality(array(1, 2, 3)) as card FROM card_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "cardinality", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_array_append(self, spark_reference, spark_thunderduck):
        """Test array_append() adding an element to the end of an array."""
        def run_test(spark):
            data = [(1,), (2,), (3,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("arr_data")
            return spark.sql(
                "SELECT id, array_append(array(1, 2, 3), 99) as result "
                "FROM arr_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "array_append", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_array_prepend(self, spark_reference, spark_thunderduck):
        """Test array_prepend() adding an element to the start of an array."""
        def run_test(spark):
            data = [(1,), (2,), (3,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("arr_data2")
            return spark.sql(
                "SELECT id, array_prepend(array(1, 2, 3), 0) as result "
                "FROM arr_data2 ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "array_prepend", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_array_remove(self, spark_reference, spark_thunderduck):
        """Test array_remove() removing all occurrences of a value from an array."""
        def run_test(spark):
            data = [(1,), (2,), (3,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("arr_data3")
            return spark.sql(
                "SELECT id, array_remove(array(1, 2, 3, 2), 2) as result "
                "FROM arr_data3 ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "array_remove", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_array_compact(self, spark_reference, spark_thunderduck):
        """Test array_compact() removing null values from an array."""
        def run_test(spark):
            data = [(1,), (2,), (3,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("arr_data4")
            return spark.sql(
                "SELECT id, array_compact(array(1, CAST(NULL AS INT), 3)) as result "
                "FROM arr_data4 ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "array_compact", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_sequence(self, spark_reference, spark_thunderduck):
        """Test sequence() generating a sequence of integers from 1 to id."""
        def run_test(spark):
            data = [(1,), (2,), (3,), (4,), (5,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("seq_data")
            return spark.sql(
                "SELECT id, sequence(1, id) as seq FROM seq_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "sequence", ignore_nullable=True)
