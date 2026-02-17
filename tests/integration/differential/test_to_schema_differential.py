"""
Differential Tests for ToSchema operation (M49).

Tests cover DataFrame.to(schema) which:
- Reorders columns by name to match target schema
- Projects away columns not in target schema
- Casts columns to match target data types

Converted from test_to_schema.py.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestToSchemaBasic_Differential:
    """Basic ToSchema operation tests"""

    @pytest.mark.timeout(30)
    def test_column_reorder(self, spark_reference, spark_thunderduck):
        """Test that ToSchema reorders columns to match target schema"""
        def run_test(spark):
            df = spark.createDataFrame(
                [(1, "x", 10.0), (2, "y", 20.0)],
                ["a", "b", "c"]
            )
            target_schema = StructType([
                StructField("c", DoubleType()),
                StructField("a", LongType()),
                StructField("b", StringType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "column_reorder")

    @pytest.mark.timeout(30)
    def test_column_projection(self, spark_reference, spark_thunderduck):
        """Test that ToSchema projects away columns not in target schema"""
        def run_test(spark):
            df = spark.createDataFrame(
                [(1, "x", 10.0, True)],
                ["a", "b", "c", "d"]
            )
            target_schema = StructType([
                StructField("a", LongType()),
                StructField("c", DoubleType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "column_projection")

    @pytest.mark.timeout(30)
    def test_identical_schema(self, spark_reference, spark_thunderduck):
        """Test ToSchema with identical schema (pass-through)"""
        def run_test(spark):
            schema = StructType([
                StructField("id", LongType()),
                StructField("name", StringType())
            ])
            df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema)
            return df.to(schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "identical_schema")

    @pytest.mark.timeout(30)
    def test_empty_dataframe(self, spark_reference, spark_thunderduck):
        """Test ToSchema on empty DataFrame"""
        def run_test(spark):
            schema = StructType([
                StructField("x", IntegerType()),
                StructField("y", StringType())
            ])
            df = spark.createDataFrame([], schema)
            target_schema = StructType([
                StructField("y", StringType()),
                StructField("x", LongType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "empty_dataframe")


@pytest.mark.differential
class TestToSchemaTypeCasting_Differential:
    """Tests for type casting in ToSchema"""

    @pytest.mark.timeout(30)
    def test_int_to_bigint(self, spark_reference, spark_thunderduck):
        """Test widening cast from INT to BIGINT"""
        def run_test(spark):
            df = spark.createDataFrame([(1,), (2,)], ["value"])
            target_schema = StructType([
                StructField("value", LongType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "int_to_bigint")

    @pytest.mark.timeout(30)
    def test_float_to_double(self, spark_reference, spark_thunderduck):
        """Test widening cast from FLOAT to DOUBLE"""
        def run_test(spark):
            schema = StructType([StructField("value", FloatType())])
            df = spark.createDataFrame([(1.5,), (2.5,)], schema)
            target_schema = StructType([
                StructField("value", DoubleType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "float_to_double", epsilon=0.01)

    @pytest.mark.timeout(30)
    def test_double_to_float(self, spark_reference, spark_thunderduck):
        """Test narrowing cast from DOUBLE to FLOAT"""
        def run_test(spark):
            df = spark.createDataFrame([(1.5,), (2.5,)], ["value"])
            target_schema = StructType([
                StructField("value", FloatType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "double_to_float", epsilon=0.01)

    @pytest.mark.timeout(30)
    def test_bigint_to_int(self, spark_reference, spark_thunderduck):
        """Test narrowing cast from BIGINT to INT"""
        def run_test(spark):
            df = spark.createDataFrame([(100,), (200,)], ["value"])
            target_schema = StructType([
                StructField("value", IntegerType())
            ])
            return df.to(target_schema)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "bigint_to_int")


@pytest.mark.differential
class TestToSchemaMultipleRows_Differential:
    """Tests with multiple rows to ensure consistency"""

    @pytest.mark.timeout(30)
    def test_multiple_rows_reorder(self, spark_reference, spark_thunderduck):
        """Test column reordering across multiple rows"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "Alice", 100.0),
                (2, "Bob", 200.0),
                (3, "Carol", 300.0)
            ], ["id", "name", "score"])
            target_schema = StructType([
                StructField("score", DoubleType()),
                StructField("name", StringType()),
                StructField("id", LongType())
            ])
            return df.to(target_schema).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "multiple_rows_reorder")

    @pytest.mark.timeout(30)
    def test_multiple_rows_with_nulls(self, spark_reference, spark_thunderduck):
        """Test ToSchema preserves null values"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "Alice"),
                (2, None),
                (None, "Carol")
            ], ["id", "name"])
            target_schema = StructType([
                StructField("name", StringType()),
                StructField("id", LongType())
            ])
            return df.to(target_schema).orderBy("name")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "multiple_rows_with_nulls")


@pytest.mark.differential
class TestToSchemaChaining_Differential:
    """Tests for chaining ToSchema with other operations"""

    @pytest.mark.timeout(30)
    def test_filter_then_to_schema(self, spark_reference, spark_thunderduck):
        """Test ToSchema after filter operation"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "Alice", 100),
                (2, "Bob", 200),
                (3, "Carol", 150)
            ], ["id", "name", "score"])
            target_schema = StructType([
                StructField("name", StringType()),
                StructField("score", LongType())
            ])
            return df.filter("score > 100").to(target_schema).orderBy("name")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_then_to_schema")

    @pytest.mark.timeout(30)
    def test_to_schema_then_select(self, spark_reference, spark_thunderduck):
        """Test select after ToSchema"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "Alice", 100.0),
                (2, "Bob", 200.0)
            ], ["id", "name", "score"])
            target_schema = StructType([
                StructField("score", DoubleType()),
                StructField("name", StringType()),
                StructField("id", LongType())
            ])
            return df.to(target_schema).select("name", "score").orderBy("name")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "to_schema_then_select")
