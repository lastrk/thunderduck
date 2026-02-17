"""
Differential tests for offset and toDF operations.
Verifies Thunderduck matches Spark 4.1.1 behavior exactly.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal
from pyspark.sql import functions as F


@pytest.mark.differential
class TestOffsetOperationsDifferential:
    """Differential tests for offset and toDF operations."""

    def test_offset_basic(self, spark_reference, spark_thunderduck):
        """Test basic offset operation - exact parity check."""
        spark_result = spark_reference.range(10).offset(3)
        td_result = spark_thunderduck.range(10).offset(3)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_basic")

    def test_offset_zero(self, spark_reference, spark_thunderduck):
        """Test offset(0) - should return all rows - exact parity check."""
        spark_result = spark_reference.range(5).offset(0)
        td_result = spark_thunderduck.range(5).offset(0)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_zero")

    def test_offset_all_rows(self, spark_reference, spark_thunderduck):
        """Test offset that skips all rows - exact parity check."""
        spark_result = spark_reference.range(5).offset(5)
        td_result = spark_thunderduck.range(5).offset(5)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_all_rows")

    def test_offset_more_than_rows(self, spark_reference, spark_thunderduck):
        """Test offset greater than row count - exact parity check."""
        spark_result = spark_reference.range(5).offset(10)
        td_result = spark_thunderduck.range(5).offset(10)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_more_than_rows")

    def test_offset_with_limit(self, spark_reference, spark_thunderduck):
        """Test offset combined with limit for pagination - exact parity check."""
        spark_result = spark_reference.range(20).offset(5).limit(3)
        td_result = spark_thunderduck.range(20).offset(5).limit(3)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_with_limit")

    def test_offset_with_filter(self, spark_reference, spark_thunderduck):
        """Test offset after filter - exact parity check."""
        spark_result = spark_reference.range(10).filter(F.col("id") % 2 == 0).offset(2)
        td_result = spark_thunderduck.range(10).filter(F.col("id") % 2 == 0).offset(2)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_with_filter")

    def test_todf_basic(self, spark_reference, spark_thunderduck):
        """Test basic toDF operation - exact parity check."""
        spark_df = spark_reference.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        td_df = spark_thunderduck.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

        spark_result = spark_df.toDF("x", "y", "z")
        td_result = td_df.toDF("x", "y", "z")

        assert_dataframes_equal(spark_result, td_result, query_name="todf_basic")

    def test_todf_with_range(self, spark_reference, spark_thunderduck):
        """Test toDF on range DataFrame - exact parity check."""
        spark_result = spark_reference.range(3).toDF("value")
        td_result = spark_thunderduck.range(3).toDF("value")

        assert_dataframes_equal(spark_result, td_result, query_name="todf_with_range")

    def test_todf_with_multiple_columns(self, spark_reference, spark_thunderduck):
        """Test toDF with multiple columns from a select - exact parity check."""
        spark_df = spark_reference.range(2).select(
            F.col("id"),
            (F.col("id") * 10).alias("tens")
        )
        td_df = spark_thunderduck.range(2).select(
            F.col("id"),
            (F.col("id") * 10).alias("tens")
        )

        spark_result = spark_df.toDF("num", "big_num").orderBy("num")
        td_result = td_df.toDF("num", "big_num").orderBy("num")

        assert_dataframes_equal(spark_result, td_result, query_name="todf_with_multiple_columns")

    def test_todf_chained_operations(self, spark_reference, spark_thunderduck):
        """Test toDF followed by other operations - exact parity check."""
        spark_result = spark_reference.range(5).toDF("value").filter(F.col("value") > 2)
        td_result = spark_thunderduck.range(5).toDF("value").filter(F.col("value") > 2)

        assert_dataframes_equal(spark_result, td_result, query_name="todf_chained_operations")

    def test_offset_todf_combined(self, spark_reference, spark_thunderduck):
        """Test combining offset and toDF - exact parity check."""
        spark_result = spark_reference.range(10).toDF("num").offset(5)
        td_result = spark_thunderduck.range(10).toDF("num").offset(5)

        assert_dataframes_equal(spark_result, td_result, query_name="offset_todf_combined")
