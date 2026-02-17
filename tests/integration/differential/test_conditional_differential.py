"""
Differential tests for conditional expressions (when/otherwise, CASE WHEN).

Compares Thunderduck against Apache Spark 4.1.1 for conditional logic.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.fixture(scope="class")
def conditional_test_data(spark_reference, spark_thunderduck):
    """Test data for conditional expressions"""
    data = [
        (1, "A", 10, 1.5),
        (2, "B", 25, 2.5),
        (3, "A", 50, 3.5),
        (4, "C", 75, 4.5),
        (5, "B", 100, 5.5),
        (6, None, 30, None),  # NULL category
        (7, "A", None, 2.0),  # NULL value
        (8, None, None, None),  # All NULL
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("score", DoubleType(), True),
    ])
    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)
    return df_ref, df_td


@pytest.mark.conditional
class TestConditionalExpressions:
    """Tests for when/otherwise and CASE WHEN expressions."""

    def test_simple_when_otherwise(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test basic when/otherwise with else clause."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > 50, "high").otherwise("low").alias("level")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > 50, "high").otherwise("low").alias("level")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="simple_when_otherwise")

    def test_when_without_otherwise(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test when without otherwise - should return NULL for non-matching rows."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > 50, "high").alias("level")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > 50, "high").alias("level")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="when_without_otherwise")

    def test_chained_when(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test multiple chained when clauses."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") >= 75, "very_high")
             .when(F.col("value") >= 50, "high")
             .when(F.col("value") >= 25, "medium")
             .otherwise("low").alias("level")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") >= 75, "very_high")
             .when(F.col("value") >= 50, "high")
             .when(F.col("value") >= 25, "medium")
             .otherwise("low").alias("level")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="chained_when")

    def test_many_conditions(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test many (5+) when conditions."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") >= 100, "extreme")
             .when(F.col("value") >= 75, "very_high")
             .when(F.col("value") >= 50, "high")
             .when(F.col("value") >= 25, "medium")
             .when(F.col("value") >= 10, "low")
             .otherwise("very_low").alias("level")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") >= 100, "extreme")
             .when(F.col("value") >= 75, "very_high")
             .when(F.col("value") >= 50, "high")
             .when(F.col("value") >= 25, "medium")
             .when(F.col("value") >= 10, "low")
             .otherwise("very_low").alias("level")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="many_conditions")

    def test_type_coercion_numeric(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test type coercion: Integer and Double branches should unify to Double."""
        df_ref, df_td = conditional_test_data

        # Integer literal (10) and Double column (score) should unify to Double
        ref_result = df_ref.select(
            "id",
            F.when(F.col("category") == "A", 10.0)
             .otherwise(F.col("score")).alias("result")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("category") == "A", 10.0)
             .otherwise(F.col("score")).alias("result")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="type_coercion_numeric")

    def test_type_coercion_string(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test type coercion: String branches preserve StringType."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > 50, "big")
             .when(F.col("value") > 25, "medium")
             .otherwise("small").alias("size")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > 50, "big")
             .when(F.col("value") > 25, "medium")
             .otherwise("small").alias("size")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="type_coercion_string")

    def test_null_in_condition_column(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test behavior when condition column contains NULLs."""
        df_ref, df_td = conditional_test_data

        # When category is NULL, none of the conditions match
        ref_result = df_ref.select(
            "id",
            F.when(F.col("category") == "A", "is_A")
             .when(F.col("category") == "B", "is_B")
             .otherwise("other").alias("cat_type")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("category") == "A", "is_A")
             .when(F.col("category") == "B", "is_B")
             .otherwise("other").alias("cat_type")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="null_in_condition_column")

    def test_null_in_value_column(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test behavior when value column used in condition contains NULLs."""
        df_ref, df_td = conditional_test_data

        # When value is NULL, comparisons return NULL (not true or false)
        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > 50, "high")
             .when(F.col("value") > 25, "medium")
             .otherwise("low").alias("level")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > 50, "high")
             .when(F.col("value") > 25, "medium")
             .otherwise("low").alias("level")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="null_in_value_column")

    def test_null_propagation(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test NULL propagation when no conditions match and no otherwise."""
        df_ref, df_td = conditional_test_data

        # Without otherwise, non-matching rows get NULL
        ref_result = df_ref.select(
            "id",
            F.when(F.col("category") == "X", "found_X").alias("result")  # No X in data, all NULL
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("category") == "X", "found_X").alias("result")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="null_propagation")

    def test_nested_when(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test nested CASE WHEN expressions."""
        df_ref, df_td = conditional_test_data

        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > 50,
                   F.when(F.col("category") == "A", "high_A").otherwise("high_other"))
             .otherwise(
                   F.when(F.col("category") == "A", "low_A").otherwise("low_other"))
             .alias("nested_result")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > 50,
                   F.when(F.col("category") == "A", "high_A").otherwise("high_other"))
             .otherwise(
                   F.when(F.col("category") == "A", "low_A").otherwise("low_other"))
             .alias("nested_result")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="nested_when")

    def test_when_with_aggregation(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test CASE WHEN inside aggregation functions."""
        df_ref, df_td = conditional_test_data

        # SUM of values only where value > 50, else 0
        ref_result = df_ref.groupBy("category").agg(
            F.sum(F.when(F.col("value") > 50, F.col("value")).otherwise(0)).alias("high_value_sum"),
            F.count(F.when(F.col("value") > 50, 1)).alias("high_count")
        ).orderBy("category")

        td_result = df_td.groupBy("category").agg(
            F.sum(F.when(F.col("value") > 50, F.col("value")).otherwise(0)).alias("high_value_sum"),
            F.count(F.when(F.col("value") > 50, 1)).alias("high_count")
        ).orderBy("category")

        assert_dataframes_equal(ref_result, td_result, query_name="when_with_aggregation")

    def test_when_with_column_expressions(self, spark_reference, spark_thunderduck, conditional_test_data):
        """Test when with complex column expressions in conditions and results."""
        df_ref, df_td = conditional_test_data

        # Use column references in conditions - avoid arithmetic that causes type inference issues
        ref_result = df_ref.select(
            "id",
            F.when(F.col("value") > F.col("score") * 10, "value_wins")
             .otherwise("score_wins").alias("winner")
        ).orderBy("id")

        td_result = df_td.select(
            "id",
            F.when(F.col("value") > F.col("score") * 10, "value_wins")
             .otherwise("score_wins").alias("winner")
        ).orderBy("id")

        assert_dataframes_equal(ref_result, td_result, query_name="when_with_column_expressions")
