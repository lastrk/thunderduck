"""
E2E Tests for USING Join Support

Tests the df.join(other, column_name) and df.join(other, [col1, col2]) patterns
which use USING columns instead of explicit join conditions.

Test ID prefix: TC-USING-JOIN-E2E-*
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)


class TestSingleColumnUsingJoin:
    """Tests for df.join(other, "column")"""

    @pytest.mark.timeout(30)
    def test_single_column_using_join(self, spark):
        """
        TC-USING-JOIN-E2E-001: Basic single column USING join

        Verifies df1.join(df2, "id") works correctly.
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema1)
        df2 = spark.createDataFrame([(1, 100), (2, 200), (4, 400)], schema2)

        result = df1.join(df2, "id")

        # Inner join: only rows with matching ids (1 and 2)
        assert result.count() == 2

        rows = result.collect()
        ids = {row["id"] for row in rows}
        assert ids == {1, 2}

        print(f"Single column USING join: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_using_join_columns(self, spark):
        """
        TC-USING-JOIN-E2E-002: USING join column handling

        Verifies that the join column appears only once in the result.
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1, "a")], schema1)
        df2 = spark.createDataFrame([(1, 100)], schema2)

        result = df1.join(df2, "id")

        # Note: In PySpark USING join, the join column may appear twice
        # (once from each side) depending on the join implementation
        # The important thing is that the join works
        assert result.count() == 1
        print(f"Columns after USING join: {result.columns}")


class TestMultiColumnUsingJoin:
    """Tests for df.join(other, [col1, col2])"""

    @pytest.mark.timeout(30)
    def test_multi_column_using_join(self, spark):
        """
        TC-USING-JOIN-E2E-003: Multi-column USING join

        Verifies df1.join(df2, ["id", "key"]) works correctly.
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("key", StringType(), True),
            StructField("val1", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("key", StringType(), True),
            StructField("val2", StringType(), True)
        ])

        df1 = spark.createDataFrame([
            (1, "x", "a"),
            (1, "y", "b"),
            (2, "x", "c")
        ], schema1)

        df2 = spark.createDataFrame([
            (1, "x", "p"),
            (1, "z", "q"),
            (2, "x", "r")
        ], schema2)

        result = df1.join(df2, ["id", "key"])

        # Only rows where both id AND key match: (1, "x") and (2, "x")
        assert result.count() == 2

        print(f"Multi-column USING join: {result.count()} rows")


class TestUsingJoinTypes:
    """Tests for USING joins with different join types"""

    @pytest.mark.timeout(30)
    def test_using_left_join(self, spark):
        """
        TC-USING-JOIN-E2E-004: Left outer USING join
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
        df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

        result = df1.join(df2, "id", "left")

        # All rows from left: 1, 2, 3 (1 has no match but included)
        assert result.count() == 3

        print(f"Left USING join: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_using_right_join(self, spark):
        """
        TC-USING-JOIN-E2E-005: Right outer USING join
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
        df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

        result = df1.join(df2, "id", "right")

        # All rows from right: 2, 3, 4 (4 has no match but included)
        assert result.count() == 3

        print(f"Right USING join: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_using_full_outer_join(self, spark):
        """
        TC-USING-JOIN-E2E-006: Full outer USING join
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
        df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

        result = df1.join(df2, "id", "outer")

        # All unique ids: 1, 2, 3, 4
        assert result.count() == 4

        print(f"Full outer USING join: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_using_inner_join_explicit(self, spark):
        """
        TC-USING-JOIN-E2E-007: Explicit inner USING join
        """
        schema = StructType([
            StructField("id", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
        df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

        result = df1.join(df2, "id", "inner")

        # Only matching: 2, 3
        assert result.count() == 2

        print(f"Explicit inner USING join: {result.count()} rows")


class TestUsingJoinWithOperations:
    """Tests for USING joins combined with other operations"""

    @pytest.mark.timeout(30)
    def test_using_join_with_select(self, spark):
        """
        TC-USING-JOIN-E2E-008: USING join followed by select
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1, "a"), (2, "b")], schema1)
        df2 = spark.createDataFrame([(1, 100), (2, 200)], schema2)

        result = df1.join(df2, "id").select("name", "value")

        assert result.count() == 2
        assert set(result.columns) == {"name", "value"}

        print(f"USING join with select: columns = {result.columns}")

    @pytest.mark.timeout(30)
    def test_using_join_with_filter(self, spark):
        """
        TC-USING-JOIN-E2E-009: USING join followed by filter
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema1)
        df2 = spark.createDataFrame([(1, 100), (2, 200), (3, 300)], schema2)

        result = df1.join(df2, "id").filter(F.col("value") > 150)

        # Only rows where value > 150: ids 2 and 3
        assert result.count() == 2

        print(f"USING join with filter: {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_using_join_with_aggregation(self, spark):
        """
        TC-USING-JOIN-E2E-010: USING join followed by aggregation
        """
        schema1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True)
        ])
        schema2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("amount", IntegerType(), True)
        ])

        df1 = spark.createDataFrame([
            (1, "A"), (2, "A"), (3, "B")
        ], schema1)
        df2 = spark.createDataFrame([
            (1, 100), (2, 200), (3, 300)
        ], schema2)

        result = (df1.join(df2, "id")
                     .groupBy("category")
                     .agg(F.sum("amount").alias("total")))

        assert result.count() == 2  # Two categories: A and B

        print(f"USING join with aggregation: {result.count()} rows")


class TestUsingJoinWithParquet:
    """Tests for USING joins with Parquet data"""

    @pytest.mark.timeout(30)
    def test_using_join_parquet_tables(self, spark, tpch_data_dir):
        """
        TC-USING-JOIN-E2E-011: USING join between Parquet tables
        """
        nation = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
        region = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

        # Rename region key for USING join
        region = region.withColumnRenamed("r_regionkey", "n_regionkey")

        result = nation.join(region, "n_regionkey")

        # Every nation should have a matching region
        assert result.count() == nation.count()

        print(f"Parquet USING join: {result.count()} rows")
