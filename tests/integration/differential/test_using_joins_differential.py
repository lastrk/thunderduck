"""
Differential Tests for USING Join Support

Tests the df.join(other, column_name) and df.join(other, [col1, col2]) patterns
which use USING columns instead of explicit join conditions.

Converted from test_using_joins.py.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestSingleColumnUsingJoin_Differential:
    """Tests for df.join(other, "column")"""

    @pytest.mark.timeout(30)
    def test_single_column_using_join(self, spark_reference, spark_thunderduck):
        """Basic single column USING join"""
        def run_test(spark):
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

            return df1.join(df2, "id").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "single_column_using_join")

    @pytest.mark.timeout(30)
    def test_using_join_columns(self, spark_reference, spark_thunderduck):
        """USING join column handling"""
        def run_test(spark):
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

            return df1.join(df2, "id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_join_columns")


@pytest.mark.differential
class TestMultiColumnUsingJoin_Differential:
    """Tests for df.join(other, [col1, col2])"""

    @pytest.mark.timeout(30)
    def test_multi_column_using_join(self, spark_reference, spark_thunderduck):
        """Multi-column USING join"""
        def run_test(spark):
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

            return df1.join(df2, ["id", "key"]).orderBy("id", "key")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "multi_column_using_join")


@pytest.mark.differential
class TestUsingJoinTypes_Differential:
    """Tests for USING joins with different join types"""

    @pytest.mark.timeout(30)
    def test_using_left_join(self, spark_reference, spark_thunderduck):
        """Left outer USING join"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), True)
            ])

            df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
            df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

            return df1.join(df2, "id", "left").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_left_join")

    @pytest.mark.timeout(30)
    def test_using_right_join(self, spark_reference, spark_thunderduck):
        """Right outer USING join"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), True)
            ])

            df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
            df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

            return df1.join(df2, "id", "right").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_right_join")

    @pytest.mark.timeout(30)
    def test_using_full_outer_join(self, spark_reference, spark_thunderduck):
        """Full outer USING join"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), True)
            ])

            df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
            df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

            return df1.join(df2, "id", "outer").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_full_outer_join")

    @pytest.mark.timeout(30)
    def test_using_inner_join_explicit(self, spark_reference, spark_thunderduck):
        """Explicit inner USING join"""
        def run_test(spark):
            schema = StructType([
                StructField("id", IntegerType(), True)
            ])

            df1 = spark.createDataFrame([(1,), (2,), (3,)], schema)
            df2 = spark.createDataFrame([(2,), (3,), (4,)], schema)

            return df1.join(df2, "id", "inner").orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_inner_join_explicit")


@pytest.mark.differential
class TestUsingJoinWithOperations_Differential:
    """Tests for USING joins combined with other operations"""

    @pytest.mark.timeout(30)
    def test_using_join_with_select(self, spark_reference, spark_thunderduck):
        """USING join followed by select"""
        def run_test(spark):
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

            return df1.join(df2, "id").select("name", "value").orderBy("name")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_join_with_select")

    @pytest.mark.timeout(30)
    def test_using_join_with_filter(self, spark_reference, spark_thunderduck):
        """USING join followed by filter"""
        def run_test(spark):
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

            return df1.join(df2, "id").filter(F.col("value") > 150).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_join_with_filter")

    @pytest.mark.timeout(30)
    def test_using_join_with_aggregation(self, spark_reference, spark_thunderduck):
        """USING join followed by aggregation"""
        def run_test(spark):
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

            return (df1.join(df2, "id")
                       .groupBy("category")
                       .agg(F.sum("amount").alias("total"))
                       .orderBy("category"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_join_with_aggregation")


@pytest.mark.differential
class TestUsingJoinWithParquet_Differential:
    """Tests for USING joins with Parquet data"""

    @pytest.mark.timeout(30)
    def test_using_join_parquet_tables(self, spark_reference, spark_thunderduck, tpch_data_dir):
        """USING join between Parquet tables"""
        def run_test(spark):
            nation = spark.read.parquet(str(tpch_data_dir / "nation.parquet"))
            region = spark.read.parquet(str(tpch_data_dir / "region.parquet"))

            # Rename region key for USING join
            region = region.withColumnRenamed("r_regionkey", "n_regionkey")

            return nation.join(region, "n_regionkey").orderBy("n_nationkey")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "using_join_parquet_tables")
