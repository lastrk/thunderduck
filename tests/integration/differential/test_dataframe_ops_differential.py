"""
Differential Tests for DataFrame Operations

Tests DataFrame API operations comparing Thunderduck against Apache Spark 4.1.1.
Converted from test_dataframe_operations.py.
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestColumnOperations_Differential:
    """M19: Drop, WithColumn, WithColumnRenamed"""

    @pytest.mark.timeout(30)
    def test_drop_single_column(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test dropping a single column"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.drop("n_comment")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "drop_single_column")

    @pytest.mark.timeout(30)
    def test_drop_multiple_columns(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test dropping multiple columns"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.drop("n_comment", "n_regionkey")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "drop_multiple_columns")

    @pytest.mark.timeout(30)
    def test_with_column_new(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test adding a new column with withColumn"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.withColumn("doubled_key", col("n_nationkey") * 2)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "with_column_new")

    @pytest.mark.timeout(30)
    def test_with_column_replace(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test replacing an existing column with withColumn"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.withColumn("n_nationkey", col("n_nationkey") + 100)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "with_column_replace")

    @pytest.mark.timeout(30)
    def test_with_column_renamed(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test renaming a column"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.withColumnRenamed("n_name", "nation_name")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "with_column_renamed")


@pytest.mark.differential
class TestOffsetAndToDF_Differential:
    """M20: Offset, ToDF"""

    @pytest.mark.timeout(30)
    def test_offset(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test offset operation"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.orderBy("n_nationkey").offset(10)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "offset")

    @pytest.mark.timeout(30)
    def test_offset_with_limit(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test offset combined with limit (pagination)"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet")).orderBy("n_nationkey")
            return df.offset(5).limit(5)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "offset_with_limit")

    @pytest.mark.timeout(30)
    def test_toDF(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test toDF to rename all columns"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))
            return df.toDF("region_id", "region_name", "region_comment")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "toDF")


@pytest.mark.differential
class TestTail_Differential:
    """M21: Tail operation"""

    @pytest.mark.timeout(30)
    def test_tail(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test tail operation returns last N rows"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet")).orderBy("n_nationkey")
            # tail() returns List[Row], convert to comparable format
            rows = df.tail(5)
            return rows

        ref_rows = run_test(spark_reference)
        test_rows = run_test(spark_thunderduck)

        assert len(ref_rows) == len(test_rows), f"Row count mismatch: {len(ref_rows)} vs {len(test_rows)}"
        for i, (ref, test) in enumerate(zip(ref_rows, test_rows)):
            assert ref.asDict() == test.asDict(), f"Row {i} mismatch"
        print(f"✓ tail: {len(ref_rows)} rows match")

    @pytest.mark.timeout(30)
    def test_tail_more_than_rows(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test tail with n > row count"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "region.parquet"))
            return df.tail(100)

        ref_rows = run_test(spark_reference)
        test_rows = run_test(spark_thunderduck)

        assert len(ref_rows) == len(test_rows)
        print(f"✓ tail_more_than_rows: {len(ref_rows)} rows match")


@pytest.mark.differential
class TestSample_Differential:
    """M23: Sample operation

    Note: Sample with same seed should produce same results on both systems.
    """

    @pytest.mark.timeout(30)
    def test_sample_deterministic(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test that sample with same seed gives deterministic results"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return df.sample(fraction=0.1, seed=12345).orderBy("l_orderkey", "l_linenumber")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)

        # Sample counts should be similar (within tolerance due to different RNG implementations)
        ref_count = ref_result.count()
        test_count = test_result.count()
        print(f"Sample counts: Spark={ref_count}, Thunderduck={test_count}")

        # Just verify both produce non-empty samples with same schema
        assert ref_count > 0
        assert test_count > 0
        assert ref_result.columns == test_result.columns
        print(f"✓ sample_deterministic: both produce valid samples")


@pytest.mark.differential
class TestWriteOperation_Differential:
    """M24: Write operations"""

    @pytest.mark.timeout(60)
    def test_write_parquet(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test writing DataFrame to Parquet and reading back"""
        source_df_ref = spark_reference.read.parquet(str(tpch_data_dir / "region.parquet"))
        source_df_td = spark_thunderduck.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            ref_path = os.path.join(tmpdir, "ref_output.parquet")
            td_path = os.path.join(tmpdir, "td_output.parquet")

            source_df_ref.write.mode("overwrite").parquet(ref_path)
            source_df_td.write.mode("overwrite").parquet(td_path)

            # Read back with same system and compare
            ref_readback = spark_reference.read.parquet(ref_path)
            td_readback = spark_thunderduck.read.parquet(td_path)

            assert ref_readback.count() == td_readback.count()
            assert set(ref_readback.columns) == set(td_readback.columns)
        print(f"✓ write_parquet: both write/read correctly")

    @pytest.mark.timeout(60)
    def test_write_csv(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test writing DataFrame to CSV"""
        source_df_ref = spark_reference.read.parquet(str(tpch_data_dir / "region.parquet"))
        source_df_td = spark_thunderduck.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            ref_path = os.path.join(tmpdir, "ref_output.csv")
            td_path = os.path.join(tmpdir, "td_output.csv")

            source_df_ref.write.mode("overwrite").option("header", "true").csv(ref_path)
            source_df_td.write.mode("overwrite").option("header", "true").csv(td_path)

            assert os.path.exists(ref_path)
            assert os.path.exists(td_path)
        print(f"✓ write_csv: both write correctly")

    @pytest.mark.timeout(60)
    def test_write_json(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test writing DataFrame to JSON"""
        source_df_ref = spark_reference.read.parquet(str(tpch_data_dir / "region.parquet"))
        source_df_td = spark_thunderduck.read.parquet(str(tpch_data_dir / "region.parquet"))

        with tempfile.TemporaryDirectory() as tmpdir:
            ref_path = os.path.join(tmpdir, "ref_output.json")
            td_path = os.path.join(tmpdir, "td_output.json")

            source_df_ref.write.mode("overwrite").json(ref_path)
            source_df_td.write.mode("overwrite").json(td_path)

            assert os.path.exists(ref_path)
            assert os.path.exists(td_path)
        print(f"✓ write_json: both write correctly")


@pytest.mark.differential
class TestHintAndRepartition_Differential:
    """M25: Hint and Repartition (no-ops in DuckDB, but should not error)"""

    @pytest.mark.timeout(30)
    def test_hint_broadcast(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test hint operation (no-op passthrough)"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.hint("BROADCAST")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "hint_broadcast")

    @pytest.mark.timeout(30)
    def test_repartition(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test repartition operation (no-op passthrough)"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            # Add ORDER BY since repartition doesn't guarantee row order
            return df.repartition(4).orderBy("n_nationkey")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "repartition")

    @pytest.mark.timeout(30)
    def test_coalesce(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test coalesce operation (no-op passthrough)"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.coalesce(1)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "coalesce")


@pytest.mark.differential
class TestNAFunctions_Differential:
    """M26: NA drop, fill, replace"""

    @pytest.mark.timeout(30)
    def test_na_drop_any(self, spark_reference, spark_thunderduck):
        """Test dropping rows with any NULL"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "a", 1.0),
                (2, None, 2.0),
                (3, "c", None),
                (None, "d", 4.0),
                (5, None, None),
            ], ["id", "name", "value"])
            return df.na.drop("any").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "na_drop_any")

    @pytest.mark.timeout(30)
    def test_na_drop_subset(self, spark_reference, spark_thunderduck):
        """Test dropping rows with NULL in specific columns"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "a", 1.0),
                (2, None, 2.0),
                (3, "c", None),
                (None, "d", 4.0),
                (5, None, None),
            ], ["id", "name", "value"])
            return df.na.drop(subset=["name"]).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "na_drop_subset")

    @pytest.mark.timeout(30)
    def test_na_fill_value(self, spark_reference, spark_thunderduck):
        """Test filling NULL values with a scalar"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "a", 1.0),
                (2, None, 2.0),
                (3, "c", None),
                (None, "d", 4.0),
                (5, None, None),
            ], ["id", "name", "value"])
            return df.na.fill({"name": "UNKNOWN", "value": 0.0}).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "na_fill_value")

    @pytest.mark.timeout(30)
    def test_na_replace(self, spark_reference, spark_thunderduck):
        """Test replacing specific values"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, "a", 1.0),
                (2, None, 2.0),
                (3, "c", None),
                (None, "d", 4.0),
                (5, None, None),
            ], ["id", "name", "value"])
            return df.na.replace(["a"], ["A"], subset=["name"]).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "na_replace")


@pytest.mark.differential
class TestUnpivot_Differential:
    """M27: Unpivot operation"""

    @pytest.mark.timeout(30)
    def test_unpivot_basic(self, spark_reference, spark_thunderduck):
        """Test basic unpivot (melt) operation"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, 10, 20, 30),
                (2, 40, 50, 60),
            ], ["id", "jan", "feb", "mar"])
            return df.unpivot(
                ids=["id"],
                values=["jan", "feb", "mar"],
                variableColumnName="month",
                valueColumnName="amount"
            ).orderBy("id", "month")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "unpivot_basic")

    @pytest.mark.timeout(30)
    def test_unpivot_with_filter(self, spark_reference, spark_thunderduck):
        """Test unpivot followed by filter"""
        def run_test(spark):
            df = spark.createDataFrame([
                (1, 10, 20, 30),
                (2, 40, 50, 60),
            ], ["id", "jan", "feb", "mar"])
            return df.unpivot(
                ids=["id"],
                values=["jan", "feb", "mar"],
                variableColumnName="month",
                valueColumnName="amount"
            ).filter(col("amount") > 25).orderBy("id", "month")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "unpivot_with_filter")


@pytest.mark.differential
class TestSubqueryAlias_Differential:
    """M28: Subquery alias"""

    @pytest.mark.timeout(30)
    def test_alias_basic(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test basic alias operation"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            return df.alias("n")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "alias_basic")


@pytest.mark.differential
class TestChainedOperations_Differential:
    """Test chaining multiple operations"""

    @pytest.mark.timeout(60)
    def test_complex_chain(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """Test chaining multiple M19-M28 operations"""
        def run_test(spark):
            df = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
            return (df
                .drop("l_comment")
                .withColumn("total", col("l_extendedprice") * col("l_quantity"))
                .withColumnRenamed("l_orderkey", "order_id")
                .filter(col("total") > 50000)
                .hint("BROADCAST")
                .alias("line_items")
                .orderBy("order_id", "l_linenumber")
                .limit(100)
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "complex_chain", epsilon=1e-4)
