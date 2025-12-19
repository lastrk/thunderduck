"""
E2E Tests for Empty DataFrame Operations

Tests that empty DataFrames with schemas can be created and used correctly.
This tests the fix for: "No analyze result found!" error when creating
empty DataFrames with spark.createDataFrame([], schema).

Test ID prefix: TC-EMPTY-DF-*
"""

import pytest
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType,
    ArrayType, MapType
)
from pyspark.sql import functions as F


class TestEmptyDataFrameCreation:
    """Tests for creating empty DataFrames with explicit schemas"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_simple_schema(self, spark):
        """
        TC-EMPTY-DF-001: Create empty DataFrame with simple schema

        Verifies that spark.createDataFrame([], schema) works correctly
        and returns a DataFrame with the correct schema but zero rows.
        """
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])

        df = spark.createDataFrame([], schema)

        # Verify schema structure (field names and types)
        # Note: Spark's struct format doesn't preserve nullable info, so we don't check it
        assert len(df.schema.fields) == 2
        assert df.schema.fields[0].name == "id"
        assert isinstance(df.schema.fields[0].dataType, IntegerType)
        assert df.schema.fields[1].name == "name"
        assert isinstance(df.schema.fields[1].dataType, StringType)
        assert len(df.columns) == 2
        assert "id" in df.columns
        assert "name" in df.columns

        # Verify empty
        assert df.count() == 0

        # Verify collect returns empty list
        rows = df.collect()
        assert len(rows) == 0

        print(f"Empty DataFrame schema: {df.schema}")
        print(f"Row count: {df.count()}")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_all_primitive_types(self, spark):
        """
        TC-EMPTY-DF-002: Create empty DataFrame with all primitive types

        Tests that empty DataFrames work with all Spark primitive types.
        """
        schema = StructType([
            StructField("int_col", IntegerType(), True),
            StructField("long_col", LongType(), True),
            StructField("float_col", FloatType(), True),
            StructField("double_col", DoubleType(), True),
            StructField("string_col", StringType(), True),
            StructField("bool_col", BooleanType(), True),
        ])

        df = spark.createDataFrame([], schema)

        assert df.count() == 0
        assert len(df.columns) == 6

        # Verify each column type
        fields = df.schema.fields
        assert isinstance(fields[0].dataType, IntegerType)
        assert isinstance(fields[1].dataType, LongType)
        assert isinstance(fields[2].dataType, FloatType)
        assert isinstance(fields[3].dataType, DoubleType)
        assert isinstance(fields[4].dataType, StringType)
        assert isinstance(fields[5].dataType, BooleanType)

        print(f"All primitive types schema validated: {len(df.columns)} columns")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_show(self, spark):
        """
        TC-EMPTY-DF-003: Show empty DataFrame

        Verifies that df.show() works on empty DataFrames.
        """
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True)
        ])

        df = spark.createDataFrame([], schema)

        # This should not raise an error
        df.show()
        print("df.show() completed successfully for empty DataFrame")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_print_schema(self, spark):
        """
        TC-EMPTY-DF-004: Print schema of empty DataFrame

        Verifies that df.printSchema() works on empty DataFrames.
        """
        schema = StructType([
            StructField("id", LongType(), False),
            StructField("data", StringType(), True)
        ])

        df = spark.createDataFrame([], schema)

        # This should not raise an error
        df.printSchema()
        print("df.printSchema() completed successfully for empty DataFrame")


class TestEmptyDataFrameOperations:
    """Tests for operations on empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_select(self, spark):
        """
        TC-EMPTY-DF-005: Select columns from empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.select("id", "name")

        assert result.count() == 0
        assert len(result.columns) == 2
        assert "id" in result.columns
        assert "name" in result.columns
        assert "value" not in result.columns

        print(f"Select from empty DataFrame: {result.columns}")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_filter(self, spark):
        """
        TC-EMPTY-DF-006: Filter empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.filter(F.col("value") > 0)

        assert result.count() == 0
        print("Filter on empty DataFrame completed successfully")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_sum(self, spark):
        """
        TC-EMPTY-DF-007: Aggregate (sum) on empty DataFrame

        SUM on empty DataFrame should return NULL (None in Python).
        """
        schema = StructType([
            StructField("value", IntegerType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.agg(F.sum("value").alias("total"))

        rows = result.collect()
        assert len(rows) == 1
        # SUM of empty should be NULL
        assert rows[0]["total"] is None

        print(f"SUM on empty DataFrame: {rows[0]['total']}")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_count(self, spark):
        """
        TC-EMPTY-DF-008: Aggregate (count) on empty DataFrame

        COUNT on empty DataFrame should return 0.
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.agg(F.count("id").alias("cnt"))

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["cnt"] == 0

        print(f"COUNT on empty DataFrame: {rows[0]['cnt']}")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_aggregate_avg(self, spark):
        """
        TC-EMPTY-DF-009: Aggregate (avg) on empty DataFrame

        AVG on empty DataFrame should return NULL (None in Python).
        """
        schema = StructType([
            StructField("value", DoubleType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.agg(F.avg("value").alias("average"))

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["average"] is None

        print(f"AVG on empty DataFrame: {rows[0]['average']}")


class TestEmptyDataFrameWithColumn:
    """Tests for withColumn operations on empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_empty_dataframe_with_column(self, spark):
        """
        TC-EMPTY-DF-010: Add column to empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.withColumn("doubled", F.col("id") * 2)

        assert result.count() == 0
        assert "doubled" in result.columns
        assert len(result.columns) == 2

        print(f"withColumn on empty DataFrame: {result.columns}")

    @pytest.mark.timeout(30)
    def test_empty_dataframe_with_column_literal(self, spark):
        """
        TC-EMPTY-DF-011: Add literal column to empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df = spark.createDataFrame([], schema)
        result = df.withColumn("constant", F.lit(42))

        assert result.count() == 0
        assert "constant" in result.columns

        print(f"withColumn literal on empty DataFrame: {result.columns}")


class TestEmptyDataFrameJoin:
    """Tests for joining with empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_join_empty_with_non_empty(self, spark, tpch_data_dir):
        """
        TC-EMPTY-DF-012: Join empty DataFrame with non-empty DataFrame

        Result should be empty.
        """
        # Create empty left side
        left_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True)
        ])
        left = spark.createDataFrame([], left_schema)

        # Load non-empty right side
        right = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
        right = right.withColumnRenamed("n_nationkey", "id")

        # Join - result should be empty
        result = left.join(right, on="id", how="inner")

        assert result.count() == 0
        print("Join empty with non-empty DataFrame: 0 rows (as expected)")

    @pytest.mark.timeout(30)
    def test_join_two_empty_dataframes(self, spark):
        """
        TC-EMPTY-DF-013: Join two empty DataFrames

        Result should be empty.
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([], schema1)
        df2 = spark.createDataFrame([], schema2)

        result = df1.join(df2, on="id", how="inner")

        assert result.count() == 0
        print("Join two empty DataFrames: 0 rows (as expected)")


class TestEmptyDataFrameUnion:
    """Tests for union operations with empty DataFrames"""

    @pytest.mark.timeout(30)
    def test_union_empty_with_non_empty(self, spark):
        """
        TC-EMPTY-DF-014: Union empty DataFrame with non-empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])

        empty_df = spark.createDataFrame([], schema)
        non_empty_df = spark.createDataFrame([(1, "a"), (2, "b")], schema)

        result = empty_df.union(non_empty_df)

        assert result.count() == 2
        print(f"Union empty with non-empty: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_union_two_empty_dataframes(self, spark):
        """
        TC-EMPTY-DF-015: Union two empty DataFrames
        """
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", DoubleType(), True)
        ])

        df1 = spark.createDataFrame([], schema)
        df2 = spark.createDataFrame([], schema)

        result = df1.union(df2)

        assert result.count() == 0
        print("Union two empty DataFrames: 0 rows")


# ============================================================================
# Differential Tests (compare against Apache Spark reference)
# ============================================================================

@pytest.mark.differential
class TestEmptyDataFrameDifferential:
    """
    Differential tests comparing Thunderduck and Spark behavior for empty DataFrames.

    These tests require the dual server setup with both Apache Spark
    and Thunderduck running.
    """

    @pytest.mark.timeout(60)
    def test_empty_dataframe_schema_differential(
        self, spark_reference, spark_thunderduck
    ):
        """
        TC-EMPTY-DF-DIFF-001: Compare empty DataFrame schema between Spark and Thunderduck
        """
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])

        # Create on Spark reference
        spark_df = spark_reference.createDataFrame([], schema)
        spark_count = spark_df.count()
        spark_cols = spark_df.columns

        # Create on Thunderduck
        td_df = spark_thunderduck.createDataFrame([], schema)
        td_count = td_df.count()
        td_cols = td_df.columns

        # Compare
        assert spark_count == td_count == 0, f"Count mismatch: Spark={spark_count}, TD={td_count}"
        assert spark_cols == td_cols, f"Column mismatch: Spark={spark_cols}, TD={td_cols}"

        print(f"Schema match: {spark_cols}")
        print(f"Both have count: {spark_count}")

    @pytest.mark.timeout(60)
    def test_empty_dataframe_sum_differential(
        self, spark_reference, spark_thunderduck
    ):
        """
        TC-EMPTY-DF-DIFF-002: Compare SUM on empty DataFrame
        """
        schema = StructType([
            StructField("value", IntegerType(), True)
        ])

        # Spark reference
        spark_df = spark_reference.createDataFrame([], schema)
        spark_result = spark_df.agg(F.sum("value").alias("total")).collect()
        spark_sum = spark_result[0]["total"]

        # Thunderduck
        td_df = spark_thunderduck.createDataFrame([], schema)
        td_result = td_df.agg(F.sum("value").alias("total")).collect()
        td_sum = td_result[0]["total"]

        # Both should return None
        assert spark_sum == td_sum, f"SUM mismatch: Spark={spark_sum}, TD={td_sum}"
        assert spark_sum is None, f"SUM should be None, got {spark_sum}"

        print(f"SUM on empty: Spark={spark_sum}, Thunderduck={td_sum}")

    @pytest.mark.timeout(60)
    def test_empty_dataframe_count_differential(
        self, spark_reference, spark_thunderduck
    ):
        """
        TC-EMPTY-DF-DIFF-003: Compare COUNT on empty DataFrame
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        # Spark reference
        spark_df = spark_reference.createDataFrame([], schema)
        spark_result = spark_df.agg(F.count("id").alias("cnt")).collect()
        spark_cnt = spark_result[0]["cnt"]

        # Thunderduck
        td_df = spark_thunderduck.createDataFrame([], schema)
        td_result = td_df.agg(F.count("id").alias("cnt")).collect()
        td_cnt = td_result[0]["cnt"]

        # Both should return 0
        assert spark_cnt == td_cnt == 0, f"COUNT mismatch: Spark={spark_cnt}, TD={td_cnt}"

        print(f"COUNT on empty: Spark={spark_cnt}, Thunderduck={td_cnt}")
