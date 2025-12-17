"""
Integration tests for ToSchema operation (M49).

Tests cover DataFrame.to(schema) which:
- Reorders columns by name to match target schema
- Projects away columns not in target schema
- Casts columns to match target data types

NOTE: ToSchema is available in PySpark 3.4+.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType, TimestampType
)


class TestToSchemaBasic:
    """Basic ToSchema operation tests"""

    @pytest.mark.timeout(30)
    def test_column_reorder(self, spark):
        """Test that ToSchema reorders columns to match target schema"""
        # Create DataFrame with columns [a, b, c]
        df = spark.createDataFrame(
            [(1, "x", 10.0), (2, "y", 20.0)],
            ["a", "b", "c"]
        )

        # Target schema with columns [c, a, b]
        target_schema = StructType([
            StructField("c", DoubleType()),
            StructField("a", LongType()),
            StructField("b", StringType())
        ])

        result = df.to(target_schema)
        rows = result.collect()

        # Verify column order
        assert result.columns == ["c", "a", "b"]
        assert rows[0]["c"] == 10.0
        assert rows[0]["a"] == 1
        assert rows[0]["b"] == "x"
        print("\n ToSchema column reorder works")

    @pytest.mark.timeout(30)
    def test_column_projection(self, spark):
        """Test that ToSchema projects away columns not in target schema"""
        # Create DataFrame with columns [a, b, c, d]
        df = spark.createDataFrame(
            [(1, "x", 10.0, True)],
            ["a", "b", "c", "d"]
        )

        # Target schema with only [a, c]
        target_schema = StructType([
            StructField("a", LongType()),
            StructField("c", DoubleType())
        ])

        result = df.to(target_schema)
        rows = result.collect()

        # Verify only selected columns are present
        assert result.columns == ["a", "c"]
        assert "b" not in result.columns
        assert "d" not in result.columns
        assert rows[0]["a"] == 1
        assert rows[0]["c"] == 10.0
        print("\n ToSchema column projection works")

    @pytest.mark.timeout(30)
    def test_identical_schema(self, spark):
        """Test ToSchema with identical schema (pass-through)"""
        schema = StructType([
            StructField("id", LongType()),
            StructField("name", StringType())
        ])
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema)

        result = df.to(schema)
        rows = result.collect()

        assert result.columns == ["id", "name"]
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        print("\n ToSchema identical schema works")

    @pytest.mark.timeout(30)
    def test_empty_dataframe(self, spark):
        """Test ToSchema on empty DataFrame"""
        schema = StructType([
            StructField("x", IntegerType()),
            StructField("y", StringType())
        ])
        df = spark.createDataFrame([], schema)

        # Different target schema order
        target_schema = StructType([
            StructField("y", StringType()),
            StructField("x", LongType())  # Also change type
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert result.columns == ["y", "x"]
        assert len(rows) == 0
        print("\n ToSchema empty DataFrame works")


class TestToSchemaTypeCasting:
    """Tests for type casting in ToSchema"""

    @pytest.mark.timeout(30)
    def test_int_to_bigint(self, spark):
        """Test widening cast from INT to BIGINT"""
        df = spark.createDataFrame([(1,), (2,)], ["value"])

        target_schema = StructType([
            StructField("value", LongType())  # INT -> BIGINT
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert rows[0]["value"] == 1
        assert rows[1]["value"] == 2
        print("\n ToSchema INT to BIGINT works")

    @pytest.mark.timeout(30)
    def test_float_to_double(self, spark):
        """Test widening cast from FLOAT to DOUBLE"""
        schema = StructType([StructField("value", FloatType())])
        df = spark.createDataFrame([(1.5,), (2.5,)], schema)

        target_schema = StructType([
            StructField("value", DoubleType())  # FLOAT -> DOUBLE
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert abs(rows[0]["value"] - 1.5) < 0.01
        assert abs(rows[1]["value"] - 2.5) < 0.01
        print("\n ToSchema FLOAT to DOUBLE works")

    @pytest.mark.timeout(30)
    def test_double_to_float(self, spark):
        """Test narrowing cast from DOUBLE to FLOAT (DuckDB allows this)"""
        df = spark.createDataFrame([(1.5,), (2.5,)], ["value"])

        target_schema = StructType([
            StructField("value", FloatType())  # DOUBLE -> FLOAT
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert abs(rows[0]["value"] - 1.5) < 0.01
        print("\n ToSchema DOUBLE to FLOAT works")

    @pytest.mark.timeout(30)
    def test_bigint_to_int(self, spark):
        """Test narrowing cast from BIGINT to INT (DuckDB allows within range)"""
        df = spark.createDataFrame([(100,), (200,)], ["value"])

        target_schema = StructType([
            StructField("value", IntegerType())  # BIGINT -> INT
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert rows[0]["value"] == 100
        assert rows[1]["value"] == 200
        print("\n ToSchema BIGINT to INT works")


class TestToSchemaErrors:
    """Tests for error conditions in ToSchema"""

    @pytest.mark.timeout(30)
    def test_missing_column_fails(self, spark):
        """Test that ToSchema fails when target has column not in input"""
        df = spark.createDataFrame([(1,)], ["a"])

        target_schema = StructType([
            StructField("a", LongType()),
            StructField("b", StringType())  # 'b' doesn't exist in input
        ])

        # Should fail when trying to collect
        with pytest.raises(Exception) as exc_info:
            df.to(target_schema).collect()

        # Error message should mention the missing column
        error_msg = str(exc_info.value).lower()
        assert "b" in error_msg or "column" in error_msg or "not found" in error_msg
        print("\n ToSchema correctly fails on missing column")


class TestToSchemaMultipleRows:
    """Tests with multiple rows to ensure consistency"""

    @pytest.mark.timeout(30)
    def test_multiple_rows_reorder(self, spark):
        """Test column reordering across multiple rows"""
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

        result = df.to(target_schema)
        rows = result.collect()

        assert len(rows) == 3
        assert result.columns == ["score", "name", "id"]
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"
        assert rows[2]["name"] == "Carol"
        print("\n ToSchema multiple rows reorder works")

    @pytest.mark.timeout(30)
    def test_multiple_rows_with_nulls(self, spark):
        """Test ToSchema preserves null values"""
        df = spark.createDataFrame([
            (1, "Alice"),
            (2, None),
            (None, "Carol")
        ], ["id", "name"])

        target_schema = StructType([
            StructField("name", StringType()),
            StructField("id", LongType())
        ])

        result = df.to(target_schema)
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] is None
        assert rows[2]["id"] is None
        print("\n ToSchema preserves nulls correctly")


class TestToSchemaChaining:
    """Tests for chaining ToSchema with other operations"""

    @pytest.mark.timeout(30)
    def test_filter_then_to_schema(self, spark):
        """Test ToSchema after filter operation"""
        df = spark.createDataFrame([
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Carol", 150)
        ], ["id", "name", "score"])

        target_schema = StructType([
            StructField("name", StringType()),
            StructField("score", LongType())
        ])

        result = df.filter("score > 100").to(target_schema)
        rows = result.collect()

        assert len(rows) == 2
        assert result.columns == ["name", "score"]
        names = [r["name"] for r in rows]
        assert "Bob" in names
        assert "Carol" in names
        print("\n ToSchema after filter works")

    @pytest.mark.timeout(30)
    def test_to_schema_then_select(self, spark):
        """Test select after ToSchema"""
        df = spark.createDataFrame([
            (1, "Alice", 100.0),
            (2, "Bob", 200.0)
        ], ["id", "name", "score"])

        target_schema = StructType([
            StructField("score", DoubleType()),
            StructField("name", StringType()),
            StructField("id", LongType())
        ])

        # ToSchema then select specific columns
        result = df.to(target_schema).select("name", "score")
        rows = result.collect()

        assert result.columns == ["name", "score"]
        assert rows[0]["name"] == "Alice"
        print("\n ToSchema then select works")
