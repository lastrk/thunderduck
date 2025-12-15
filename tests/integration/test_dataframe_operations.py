"""
E2E Tests for DataFrame Operations (M19-M28)

Tests DataFrame API operations implemented in milestones M19-M28.
Each test validates the operation works end-to-end through Spark Connect.
"""

import pytest
import tempfile
import os
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when


class TestColumnOperations:
    """M19: Drop, WithColumn, WithColumnRenamed

    Known issue: df.columns returns empty list after drop/withColumn operations.
    The operations execute but schema is not properly returned.
    """

    @pytest.mark.xfail(reason="df.columns returns empty list - schema not returned")
    @pytest.mark.timeout(30)
    def test_drop_single_column(self, spark, tpch_data_dir):
        """Test dropping a single column"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
        original_cols = df.columns

        result = df.drop("n_comment")
        result_cols = result.columns

        assert "n_comment" not in result_cols
        assert len(result_cols) == len(original_cols) - 1
        assert result.count() == df.count()
        print(f"drop single column: {original_cols} -> {result_cols}")

    @pytest.mark.xfail(reason="df.columns returns empty list - schema not returned")
    @pytest.mark.timeout(30)
    def test_drop_multiple_columns(self, spark, tpch_data_dir):
        """Test dropping multiple columns"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.drop("n_comment", "n_regionkey")
        result_cols = result.columns

        assert "n_comment" not in result_cols
        assert "n_regionkey" not in result_cols
        assert "n_nationkey" in result_cols
        assert "n_name" in result_cols
        print(f"drop multiple columns: {result_cols}")

    @pytest.mark.xfail(reason="df.columns returns empty list - schema not returned")
    @pytest.mark.timeout(30)
    def test_with_column_new(self, spark, tpch_data_dir):
        """Test adding a new column with withColumn"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.withColumn("doubled_key", col("n_nationkey") * 2)

        assert "doubled_key" in result.columns
        rows = result.collect()
        for row in rows:
            assert row["doubled_key"] == row["n_nationkey"] * 2
        print(f"withColumn new: added doubled_key column")

    @pytest.mark.xfail(reason="withColumn value calculation incorrect")
    @pytest.mark.timeout(30)
    def test_with_column_replace(self, spark, tpch_data_dir):
        """Test replacing an existing column with withColumn"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.withColumn("n_nationkey", col("n_nationkey") + 100)

        rows = result.collect()
        original_rows = df.collect()
        for i, row in enumerate(rows):
            assert row["n_nationkey"] == original_rows[i]["n_nationkey"] + 100
        print(f"withColumn replace: modified n_nationkey column")

    @pytest.mark.xfail(reason="df.columns returns empty list - schema not returned")
    @pytest.mark.timeout(30)
    def test_with_column_renamed(self, spark, tpch_data_dir):
        """Test renaming a column"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.withColumnRenamed("n_name", "nation_name")

        assert "nation_name" in result.columns
        assert "n_name" not in result.columns
        assert result.count() == df.count()
        print(f"withColumnRenamed: n_name -> nation_name")


class TestOffsetAndToDF:
    """M20: Offset, ToDF"""

    @pytest.mark.timeout(30)
    def test_offset(self, spark, tpch_data_dir):
        """Test offset operation"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
        total = df.count()

        result = df.offset(10)
        result_count = result.count()

        assert result_count == total - 10
        print(f"offset(10): {total} -> {result_count} rows")

    @pytest.mark.timeout(30)
    def test_offset_with_limit(self, spark, tpch_data_dir):
        """Test offset combined with limit (pagination)"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet")).orderBy("n_nationkey")

        # Get rows 5-9 (0-indexed)
        result = df.offset(5).limit(5)
        rows = result.collect()

        assert len(rows) == 5
        # First row should be nation with key 5
        assert rows[0]["n_nationkey"] == 5
        print(f"offset(5).limit(5): got rows 5-9")

    @pytest.mark.xfail(reason="df.columns returns empty list - schema not returned")
    @pytest.mark.timeout(30)
    def test_toDF(self, spark, tpch_data_dir):
        """Test toDF to rename all columns"""
        df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

        result = df.toDF("region_id", "region_name", "region_comment")

        assert result.columns == ["region_id", "region_name", "region_comment"]
        assert result.count() == df.count()
        print(f"toDF: renamed columns to {result.columns}")


class TestTail:
    """M21: Tail operation

    Known issue: Tail operation causes Arrow memory leak error.
    """

    @pytest.mark.xfail(reason="Memory leak error from Arrow allocator")
    @pytest.mark.timeout(30)
    def test_tail(self, spark, tpch_data_dir):
        """Test tail operation returns last N rows"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet")).orderBy("n_nationkey")

        # tail() returns a list of Row objects, not a DataFrame
        rows = df.tail(5)

        assert len(rows) == 5
        # Last row should be highest nationkey (24 for 25 nations, 0-indexed)
        assert rows[-1]["n_nationkey"] == 24
        print(f"tail(5): got last 5 rows, last nationkey={rows[-1]['n_nationkey']}")

    @pytest.mark.xfail(reason="Memory leak error from Arrow allocator")
    @pytest.mark.timeout(30)
    def test_tail_more_than_rows(self, spark, tpch_data_dir):
        """Test tail with n > row count"""
        df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))  # 5 rows

        rows = df.tail(100)

        assert len(rows) == 5  # Should return all rows
        print(f"tail(100) on 5-row table: got {len(rows)} rows")


class TestSample:
    """M23: Sample operation"""

    @pytest.mark.timeout(30)
    def test_sample_fraction(self, spark, tpch_data_dir):
        """Test sampling with fraction"""
        df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        total = df.count()

        result = df.sample(fraction=0.1, seed=42)
        sample_count = result.count()

        # Sample should be roughly 10% (with some variance)
        assert sample_count > 0
        assert sample_count < total
        print(f"sample(0.1): {total} -> {sample_count} rows (~{100*sample_count/total:.1f}%)")

    @pytest.mark.timeout(30)
    def test_sample_deterministic(self, spark, tpch_data_dir):
        """Test that sample with same seed gives same results"""
        df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        result1 = df.sample(fraction=0.1, seed=12345)
        result2 = df.sample(fraction=0.1, seed=12345)

        count1 = result1.count()
        count2 = result2.count()

        assert count1 == count2
        print(f"sample deterministic: both samples have {count1} rows")


class TestWriteOperation:
    """M24: Write operations"""

    @pytest.mark.timeout(60)
    def test_write_parquet(self, spark, tpch_data_dir):
        """Test writing DataFrame to Parquet"""
        df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test_output.parquet")
            df.write.mode("overwrite").parquet(output_path)

            # Verify by reading back
            result = spark.read.parquet(output_path)
            assert result.count() == df.count()
            assert set(result.columns) == set(df.columns)
        print(f"write parquet: wrote and verified {df.count()} rows")

    @pytest.mark.timeout(60)
    def test_write_csv(self, spark, tpch_data_dir):
        """Test writing DataFrame to CSV"""
        df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test_output.csv")
            df.write.mode("overwrite").option("header", "true").csv(output_path)

            # Verify file exists
            assert os.path.exists(output_path)
        print(f"write csv: wrote {df.count()} rows")

    @pytest.mark.timeout(60)
    def test_write_json(self, spark, tpch_data_dir):
        """Test writing DataFrame to JSON"""
        df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test_output.json")
            df.write.mode("overwrite").json(output_path)

            # Verify file exists
            assert os.path.exists(output_path)
        print(f"write json: wrote {df.count()} rows")


class TestHintAndRepartition:
    """M25: Hint and Repartition (no-ops in DuckDB, but should not error)"""

    @pytest.mark.timeout(30)
    def test_hint_broadcast(self, spark, tpch_data_dir):
        """Test hint operation (no-op passthrough)"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.hint("BROADCAST")

        # Should work without error and return same data
        assert result.count() == df.count()
        print(f"hint('BROADCAST'): passed through, {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_repartition(self, spark, tpch_data_dir):
        """Test repartition operation (no-op passthrough)"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.repartition(4)

        # Should work without error and return same data
        assert result.count() == df.count()
        print(f"repartition(4): passed through, {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_coalesce(self, spark, tpch_data_dir):
        """Test coalesce operation (no-op passthrough)"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.coalesce(1)

        # Should work without error and return same data
        assert result.count() == df.count()
        print(f"coalesce(1): passed through, {result.count()} rows")


class TestNAFunctions:
    """M26: NA drop, fill, replace

    Known issue: NA functions cause errors with createDataFrame or don't work as expected.
    """

    @pytest.fixture
    def df_with_nulls(self, spark):
        """Create a DataFrame with NULL values for testing"""
        return spark.createDataFrame([
            (1, "a", 1.0),
            (2, None, 2.0),
            (3, "c", None),
            (None, "d", 4.0),
            (5, None, None),
        ], ["id", "name", "value"])

    @pytest.mark.xfail(reason="NA functions error - likely createDataFrame or na.drop issue")
    @pytest.mark.timeout(30)
    def test_na_drop_any(self, spark, df_with_nulls):
        """Test dropping rows with any NULL"""
        result = df_with_nulls.na.drop("any")

        # Only row (1, "a", 1.0) has no nulls
        assert result.count() == 1
        row = result.collect()[0]
        assert row["id"] == 1
        print(f"na.drop('any'): 5 -> {result.count()} rows")

    @pytest.mark.xfail(reason="NA functions error - likely createDataFrame or na.drop issue")
    @pytest.mark.timeout(30)
    def test_na_drop_all(self, spark, df_with_nulls):
        """Test dropping rows where all values are NULL"""
        # Add a row with all nulls
        df = df_with_nulls.union(spark.createDataFrame([(None, None, None)], ["id", "name", "value"]))

        result = df.na.drop("all")

        # Should drop only the all-null row
        assert result.count() == 5
        print(f"na.drop('all'): 6 -> {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_na_drop_subset(self, spark, df_with_nulls):
        """Test dropping rows with NULL in specific columns"""
        result = df_with_nulls.na.drop(subset=["name"])

        # Rows 2 and 5 have NULL in name column
        assert result.count() == 3
        print(f"na.drop(subset=['name']): 5 -> {result.count()} rows")

    @pytest.mark.xfail(reason="NA functions error - likely createDataFrame or na.fill issue")
    @pytest.mark.timeout(30)
    def test_na_fill_value(self, spark, df_with_nulls):
        """Test filling NULL values with a scalar"""
        result = df_with_nulls.na.fill({"name": "UNKNOWN", "value": 0.0})

        rows = result.collect()
        for row in rows:
            assert row["name"] is not None
            assert row["value"] is not None
        print(f"na.fill: filled NULLs with defaults")

    @pytest.mark.xfail(reason="NA functions error - likely createDataFrame or na.replace issue")
    @pytest.mark.timeout(30)
    def test_na_replace(self, spark, df_with_nulls):
        """Test replacing specific values"""
        result = df_with_nulls.na.replace(["a"], ["A"], subset=["name"])

        rows = result.filter(col("id") == 1).collect()
        assert rows[0]["name"] == "A"
        print(f"na.replace: replaced 'a' with 'A'")


class TestUnpivot:
    """M27: Unpivot operation"""

    @pytest.mark.xfail(reason="createDataFrame or unpivot issue")
    @pytest.mark.timeout(30)
    def test_unpivot_basic(self, spark):
        """Test basic unpivot (melt) operation"""
        df = spark.createDataFrame([
            (1, 10, 20, 30),
            (2, 40, 50, 60),
        ], ["id", "jan", "feb", "mar"])

        result = df.unpivot(
            ids=["id"],
            values=["jan", "feb", "mar"],
            variableColumnName="month",
            valueColumnName="amount"
        )

        # 2 rows * 3 value columns = 6 rows
        assert result.count() == 6
        assert set(result.columns) == {"id", "month", "amount"}
        print(f"unpivot: 2 rows -> {result.count()} rows")

    @pytest.mark.timeout(30)
    def test_unpivot_with_filter(self, spark):
        """Test unpivot followed by filter"""
        df = spark.createDataFrame([
            (1, 10, 20, 30),
            (2, 40, 50, 60),
        ], ["id", "jan", "feb", "mar"])

        result = df.unpivot(
            ids=["id"],
            values=["jan", "feb", "mar"],
            variableColumnName="month",
            valueColumnName="amount"
        ).filter(col("amount") > 25)

        rows = result.collect()
        for row in rows:
            assert row["amount"] > 25
        print(f"unpivot + filter: {len(rows)} rows with amount > 25")


class TestSubqueryAlias:
    """M28: Subquery alias"""

    @pytest.mark.timeout(30)
    def test_alias_basic(self, spark, tpch_data_dir):
        """Test basic alias operation"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        result = df.alias("n")

        # Should work and return same data
        assert result.count() == df.count()
        print(f"alias('n'): {result.count()} rows")

    @pytest.mark.xfail(reason="Self-join with aliased columns fails")
    @pytest.mark.timeout(30)
    def test_alias_in_join(self, spark, tpch_data_dir):
        """Test alias used in self-join"""
        df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))

        n1 = df.alias("n1")
        n2 = df.alias("n2")

        # Self-join nations that share the same region
        result = n1.join(
            n2,
            (col("n1.n_regionkey") == col("n2.n_regionkey")) &
            (col("n1.n_nationkey") < col("n2.n_nationkey"))
        ).select("n1.n_name", "n2.n_name")

        rows = result.collect()
        assert len(rows) > 0
        print(f"alias in self-join: {len(rows)} nation pairs in same region")


class TestChainedOperations:
    """Test chaining multiple operations"""

    @pytest.mark.xfail(reason="Chained drop/withColumn operations fail - schema not returned")
    @pytest.mark.timeout(60)
    def test_complex_chain(self, spark, tpch_data_dir):
        """Test chaining multiple M19-M28 operations"""
        df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        result = (df
            .drop("l_comment")
            .withColumn("total", col("l_extendedprice") * col("l_quantity"))
            .withColumnRenamed("l_orderkey", "order_id")
            .sample(fraction=0.1, seed=42)
            .filter(col("total") > 1000)
            .hint("BROADCAST")
            .alias("line_items")
        )

        rows = result.collect()
        assert len(rows) > 0
        assert "l_comment" not in result.columns
        assert "total" in result.columns
        assert "order_id" in result.columns
        print(f"complex chain: {len(rows)} rows after filtering")
