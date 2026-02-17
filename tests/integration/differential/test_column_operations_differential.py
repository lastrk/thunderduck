"""
Differential tests for column manipulation operations: drop, withColumnRenamed, withColumn.
Verifies Thunderduck matches Spark 4.1.1 behavior exactly.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))
from pyspark.sql import functions as F
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestColumnOperationsDifferential:
    """Differential tests for column manipulation operations."""

    def test_drop_single_column(self, spark_reference, spark_thunderduck):
        """Test dropping a single column - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

        spark_result = spark_df.drop("c")
        td_result = td_df.drop("c")

        assert_dataframes_equal(spark_result, td_result, query_name="drop_single_column")

    def test_drop_multiple_columns(self, spark_reference, spark_thunderduck):
        """Test dropping multiple columns - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3, 4)], ["a", "b", "c", "d"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3, 4)], ["a", "b", "c", "d"])

        spark_result = spark_df.drop("b", "d")
        td_result = td_df.drop("b", "d")

        assert_dataframes_equal(spark_result, td_result, query_name="drop_multiple_columns")

    def test_drop_with_range(self, spark_reference, spark_thunderduck):
        """Test drop on a range DataFrame - exact parity check."""
        spark_df = spark_reference.range(5).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled")
        )
        td_df = spark_thunderduck.range(5).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled")
        )

        spark_result = spark_df.drop("doubled").orderBy("id")
        td_result = td_df.drop("doubled").orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="drop_with_range")

    def test_with_column_renamed_single(self, spark_reference, spark_thunderduck):
        """Test renaming a single column - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

        spark_result = spark_df.withColumnRenamed("a", "x")
        td_result = td_df.withColumnRenamed("a", "x")

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_renamed_single")

    def test_with_column_renamed_multiple(self, spark_reference, spark_thunderduck):
        """Test renaming multiple columns sequentially - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

        spark_result = spark_df.withColumnRenamed("a", "x").withColumnRenamed("b", "y")
        td_result = td_df.withColumnRenamed("a", "x").withColumnRenamed("b", "y")

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_renamed_multiple")

    def test_with_column_add_new(self, spark_reference, spark_thunderduck):
        """Test adding a new column with withColumn - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2)], ["a", "b"])
        td_df = spark_thunderduck.createDataFrame([(1, 2)], ["a", "b"])

        spark_result = spark_df.withColumn("c", F.col("a") + F.col("b"))
        td_result = td_df.withColumn("c", F.col("a") + F.col("b"))

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_add_new")

    def test_with_column_replace_existing(self, spark_reference, spark_thunderduck):
        """Test replacing an existing column with withColumn - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2)], ["a", "b"])
        td_df = spark_thunderduck.createDataFrame([(1, 2)], ["a", "b"])

        spark_result = spark_df.withColumn("a", F.col("a") * 10)
        td_result = td_df.withColumn("a", F.col("a") * 10)

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_replace_existing")

    def test_with_column_literal(self, spark_reference, spark_thunderduck):
        """Test adding a constant column with withColumn - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2)], ["a", "b"])
        td_df = spark_thunderduck.createDataFrame([(1, 2)], ["a", "b"])

        spark_result = spark_df.withColumn("const", F.lit(42))
        td_result = td_df.withColumn("const", F.lit(42))

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_literal")

    def test_with_column_multiple(self, spark_reference, spark_thunderduck):
        """Test adding multiple columns with chained withColumn - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2)], ["a", "b"])
        td_df = spark_thunderduck.createDataFrame([(1, 2)], ["a", "b"])

        spark_result = (spark_df
            .withColumn("sum", F.col("a") + F.col("b"))
            .withColumn("diff", F.col("a") - F.col("b"))
            .withColumn("prod", F.col("a") * F.col("b"))
        )

        td_result = (td_df
            .withColumn("sum", F.col("a") + F.col("b"))
            .withColumn("diff", F.col("a") - F.col("b"))
            .withColumn("prod", F.col("a") * F.col("b"))
        )

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_multiple")

    def test_with_column_on_range(self, spark_reference, spark_thunderduck):
        """Test withColumn on a range DataFrame - exact parity check."""
        spark_df = spark_reference.range(1, 6)
        td_df = spark_thunderduck.range(1, 6)

        spark_result = spark_df.withColumn("squared", F.col("id") * F.col("id")).orderBy("id")
        td_result = td_df.withColumn("squared", F.col("id") * F.col("id")).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="with_column_on_range")

    def test_combined_operations(self, spark_reference, spark_thunderduck):
        """Test combining drop, withColumnRenamed, and withColumn - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

        spark_result = (spark_df
            .drop("c")
            .withColumnRenamed("a", "x")
            .withColumn("sum", F.col("x") + F.col("b"))
        )

        td_result = (td_df
            .drop("c")
            .withColumnRenamed("a", "x")
            .withColumn("sum", F.col("x") + F.col("b"))
        )

        assert_dataframes_equal(spark_result, td_result, query_name="combined_operations")
