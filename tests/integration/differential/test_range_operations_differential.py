"""
Differential tests for spark.range() operations.
Verifies Thunderduck matches Spark 4.0.1 behavior exactly.
"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal
from pyspark.sql import functions as F


@pytest.mark.differential
class TestRangeOperationsDifferential:
    """Differential tests for spark.range() operations."""

    def test_simple_range(self, spark_reference, spark_thunderduck):
        """Test simple spark.range(n) which generates 0 to n-1 - exact parity check."""
        # Execute on both systems
        spark_result = spark_reference.range(10).orderBy("id")
        td_result = spark_thunderduck.range(10).orderBy("id")

        # Verify exact parity
        assert_dataframes_equal(spark_result, td_result, query_name="simple_range")

    def test_range_with_start_end(self, spark_reference, spark_thunderduck):
        """Test spark.range(start, end) with explicit start - exact parity check."""
        spark_result = spark_reference.range(5, 15).orderBy("id")
        td_result = spark_thunderduck.range(5, 15).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_start_end")

    def test_range_with_step(self, spark_reference, spark_thunderduck):
        """Test spark.range(start, end, step) with custom step - exact parity check."""
        spark_result = spark_reference.range(0, 20, 2).orderBy("id")
        td_result = spark_thunderduck.range(0, 20, 2).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_step")

    def test_range_with_large_step(self, spark_reference, spark_thunderduck):
        """Test range with step > 1 that doesn't evenly divide - exact parity check."""
        spark_result = spark_reference.range(0, 10, 3).orderBy("id")
        td_result = spark_thunderduck.range(0, 10, 3).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_large_step")

    def test_range_with_negative_start(self, spark_reference, spark_thunderduck):
        """Test range starting from negative value - exact parity check."""
        spark_result = spark_reference.range(-5, 5).orderBy("id")
        td_result = spark_thunderduck.range(-5, 5).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_negative_start")

    def test_empty_range(self, spark_reference, spark_thunderduck):
        """Test range where start >= end (empty result) - exact parity check."""
        spark_result = spark_reference.range(10, 5).orderBy("id")
        td_result = spark_thunderduck.range(10, 5).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="empty_range")

    def test_range_with_filter(self, spark_reference, spark_thunderduck):
        """Test filtering a range - exact parity check."""
        spark_result = spark_reference.range(0, 100).filter(F.col("id") > 90).orderBy("id")
        td_result = spark_thunderduck.range(0, 100).filter(F.col("id") > 90).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_filter")

    def test_range_with_aggregation(self, spark_reference, spark_thunderduck):
        """Test aggregation on a range - exact parity check."""
        # Sum should be 1+2+...+10 = 55
        spark_result = spark_reference.range(1, 11).agg(F.sum("id").alias("total"))
        td_result = spark_thunderduck.range(1, 11).agg(F.sum("id").alias("total"))

        assert_dataframes_equal(spark_result, td_result, query_name="range_sum_aggregation")

        # Avg should be 5.5
        spark_result = spark_reference.range(1, 11).agg(F.avg("id").alias("average"))
        td_result = spark_thunderduck.range(1, 11).agg(F.avg("id").alias("average"))

        assert_dataframes_equal(spark_result, td_result, query_name="range_avg_aggregation")

    def test_range_with_select(self, spark_reference, spark_thunderduck):
        """Test select on a range with expressions - exact parity check."""
        spark_result = spark_reference.range(1, 6).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled"),
            (F.col("id") * F.col("id")).alias("squared")
        ).orderBy("id")

        td_result = spark_thunderduck.range(1, 6).select(
            F.col("id"),
            (F.col("id") * 2).alias("doubled"),
            (F.col("id") * F.col("id")).alias("squared")
        ).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_select")

    def test_range_with_limit(self, spark_reference, spark_thunderduck):
        """Test limit on a range - exact parity check."""
        spark_result = spark_reference.range(0, 1000).limit(5)
        td_result = spark_thunderduck.range(0, 1000).limit(5)

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_limit")

    def test_range_with_orderby(self, spark_reference, spark_thunderduck):
        """Test ordering a range - exact parity check."""
        spark_result = spark_reference.range(0, 5).orderBy(F.desc("id"))
        td_result = spark_thunderduck.range(0, 5).orderBy(F.desc("id"))

        assert_dataframes_equal(spark_result, td_result, query_name="range_with_orderby")

    def test_range_join_via_sql(self, spark_reference, spark_thunderduck):
        """Test joining two ranges using DataFrame API - exact parity check."""
        # Create two ranges with column name "id"
        r1_spark = spark_reference.range(0, 5)  # 0, 1, 2, 3, 4
        r2_spark = spark_reference.range(3, 8)  # 3, 4, 5, 6, 7

        r1_td = spark_thunderduck.range(0, 5)
        r2_td = spark_thunderduck.range(3, 8)

        # Join them on id and select with aliases
        spark_result = r1_spark.join(r2_spark, r1_spark["id"] == r2_spark["id"], "inner").select(
            r1_spark["id"].alias("id1"),
            r2_spark["id"].alias("id2")
        ).orderBy("id1")

        td_result = r1_td.join(r2_td, r1_td["id"] == r2_td["id"], "inner").select(
            r1_td["id"].alias("id1"),
            r2_td["id"].alias("id2")
        ).orderBy("id1")

        assert_dataframes_equal(spark_result, td_result, query_name="range_join")

    def test_range_union(self, spark_reference, spark_thunderduck):
        """Test union of two ranges - exact parity check."""
        spark_df1 = spark_reference.range(0, 3)
        spark_df2 = spark_reference.range(10, 13)
        spark_result = spark_df1.union(spark_df2).orderBy("id")

        td_df1 = spark_thunderduck.range(0, 3)
        td_df2 = spark_thunderduck.range(10, 13)
        td_result = td_df1.union(td_df2).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_union")

    def test_large_range(self, spark_reference, spark_thunderduck):
        """Test a larger range to ensure it doesn't materialize in memory - exact parity check."""
        spark_result = spark_reference.range(0, 1000000).agg(F.max("id").alias("max_val"))
        td_result = spark_thunderduck.range(0, 1000000).agg(F.max("id").alias("max_val"))

        assert_dataframes_equal(spark_result, td_result, query_name="large_range_max")

    def test_range_schema(self, spark_reference, spark_thunderduck):
        """Test that range returns correct schema (single column 'id' of LongType) - exact parity check."""
        spark_df = spark_reference.range(10)
        td_df = spark_thunderduck.range(10)

        # Verify schemas match
        spark_schema = spark_df.schema
        td_schema = td_df.schema

        assert len(spark_schema.fields) == len(td_schema.fields), "Schema field count mismatch"
        assert spark_schema.fields[0].name == td_schema.fields[0].name, "Schema field name mismatch"
        assert str(spark_schema.fields[0].dataType) == str(td_schema.fields[0].dataType), "Schema dataType mismatch"

        # Also verify data
        spark_result = spark_df.limit(5).orderBy("id")
        td_result = td_df.limit(5).orderBy("id")
        assert_dataframes_equal(spark_result, td_result, query_name="range_schema_verification")

    def test_range_zero_start_explicit(self, spark_reference, spark_thunderduck):
        """Test range(0, n) matches range(n) - exact parity check."""
        # These should be equivalent
        spark_result1 = spark_reference.range(10).orderBy("id")
        spark_result2 = spark_reference.range(0, 10).orderBy("id")

        td_result1 = spark_thunderduck.range(10).orderBy("id")
        td_result2 = spark_thunderduck.range(0, 10).orderBy("id")

        # Verify both Spark versions match
        assert_dataframes_equal(spark_result1, spark_result2, query_name="range_zero_start_spark")

        # Verify both Thunderduck versions match
        assert_dataframes_equal(td_result1, td_result2, query_name="range_zero_start_thunderduck")

        # Verify cross-system parity
        assert_dataframes_equal(spark_result1, td_result1, query_name="range_zero_start_parity")

    def test_range_negative_values(self, spark_reference, spark_thunderduck):
        """Test range with negative start and end - exact parity check."""
        spark_result = spark_reference.range(-10, -5).orderBy("id")
        td_result = spark_thunderduck.range(-10, -5).orderBy("id")

        assert_dataframes_equal(spark_result, td_result, query_name="range_negative_values")
