"""
Differential Tests for Lambda function expressions and higher-order functions.

Tests cover:
- Basic lambda syntax (transform, filter)
- Array HOFs (exists, forall, aggregate)
- Nested lambdas

Converted from test_lambda_functions.py.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestTransformFunction_Differential:
    """Tests for transform (list_transform) higher-order function"""

    @pytest.mark.timeout(30)
    def test_transform_add_one(self, spark_reference, spark_thunderduck):
        """Test transform with simple addition"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3) AS arr")
            df = spark.table("arr_view")
            return df.select(F.transform("arr", lambda x: x + 1).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "transform_add_one")

    @pytest.mark.timeout(30)
    def test_transform_multiply(self, spark_reference, spark_thunderduck):
        """Test transform with multiplication"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4) AS arr")
            df = spark.table("arr_view")
            return df.select(F.transform("arr", lambda x: x * 2).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "transform_multiply")

    @pytest.mark.timeout(30)
    def test_transform_from_subquery(self, spark_reference, spark_thunderduck):
        """Test transform using array() function in SQL"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(10, 20, 30) AS numbers")
            df = spark.table("arr_view")
            return df.select(F.transform("numbers", lambda x: x + 5).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "transform_from_subquery")


@pytest.mark.differential
class TestFilterFunction_Differential:
    """Tests for filter (list_filter) higher-order function"""

    @pytest.mark.timeout(30)
    def test_filter_greater_than(self, spark_reference, spark_thunderduck):
        """Test filter with greater than predicate"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4, 5) AS arr")
            df = spark.table("arr_view")
            return df.select(F.filter("arr", lambda x: x > 2).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_greater_than")

    @pytest.mark.timeout(30)
    def test_filter_even_numbers(self, spark_reference, spark_thunderduck):
        """Test filter for even numbers"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4, 5, 6) AS arr")
            df = spark.table("arr_view")
            return df.select(F.filter("arr", lambda x: x % 2 == 0).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_even_numbers")

    @pytest.mark.timeout(30)
    def test_filter_all_pass(self, spark_reference, spark_thunderduck):
        """Test filter where all elements pass"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(10, 20, 30) AS arr")
            df = spark.table("arr_view")
            return df.select(F.filter("arr", lambda x: x > 5).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_all_pass")

    @pytest.mark.timeout(30)
    def test_filter_none_pass(self, spark_reference, spark_thunderduck):
        """Test filter where no elements pass"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3) AS arr")
            df = spark.table("arr_view")
            return df.select(F.filter("arr", lambda x: x > 10).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_none_pass")


@pytest.mark.differential
class TestExistsFunction_Differential:
    """Tests for exists (list_any) higher-order function"""

    @pytest.mark.timeout(30)
    def test_exists_true(self, spark_reference, spark_thunderduck):
        """Test exists returns true when condition matches"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4, 5) AS arr")
            df = spark.table("arr_view")
            return df.select(F.exists("arr", lambda x: x > 3).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "exists_true")

    @pytest.mark.timeout(30)
    def test_exists_false(self, spark_reference, spark_thunderduck):
        """Test exists returns false when no match"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3) AS arr")
            df = spark.table("arr_view")
            return df.select(F.exists("arr", lambda x: x > 10).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "exists_false")


@pytest.mark.differential
class TestForallFunction_Differential:
    """Tests for forall (list_all) higher-order function"""

    @pytest.mark.timeout(30)
    def test_forall_true(self, spark_reference, spark_thunderduck):
        """Test forall returns true when all match"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(10, 20, 30) AS arr")
            df = spark.table("arr_view")
            return df.select(F.forall("arr", lambda x: x > 5).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "forall_true")

    @pytest.mark.timeout(30)
    def test_forall_false(self, spark_reference, spark_thunderduck):
        """Test forall returns false when not all match"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4, 5) AS arr")
            df = spark.table("arr_view")
            return df.select(F.forall("arr", lambda x: x > 2).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "forall_false")


@pytest.mark.differential
class TestAggregateFunction_Differential:
    """Tests for aggregate (list_reduce) higher-order function"""

    @pytest.mark.timeout(30)
    def test_aggregate_sum(self, spark_reference, spark_thunderduck):
        """Test aggregate to compute sum"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4) AS arr")
            df = spark.table("arr_view")
            return df.select(
                F.aggregate("arr", F.lit(0), lambda acc, x: acc + x).alias("result")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "aggregate_sum")

    @pytest.mark.timeout(30)
    def test_aggregate_product(self, spark_reference, spark_thunderduck):
        """Test aggregate to compute product"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4) AS arr")
            df = spark.table("arr_view")
            return df.select(
                F.aggregate("arr", F.lit(1), lambda acc, x: acc * x).alias("result")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "aggregate_product")

    @pytest.mark.timeout(30)
    def test_aggregate_with_init(self, spark_reference, spark_thunderduck):
        """Test aggregate with non-zero initial value"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3) AS arr")
            df = spark.table("arr_view")
            return df.select(
                F.aggregate("arr", F.lit(100), lambda acc, x: acc + x).alias("result")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "aggregate_with_init")


@pytest.mark.differential
class TestNestedLambdas_Differential:
    """Tests for nested lambda expressions"""

    @pytest.mark.timeout(30)
    def test_nested_transform(self, spark_reference, spark_thunderduck):
        """Test nested transform for 2D array"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(ARRAY(1, 2), ARRAY(3, 4)) AS arr")
            df = spark.table("arr_view")
            return df.select(
                F.transform("arr", lambda arr: F.transform(arr, lambda x: x * 2)).alias("result")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "nested_transform")

    @pytest.mark.timeout(30)
    def test_transform_then_filter(self, spark_reference, spark_thunderduck):
        """Test chaining transform and filter"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(1, 2, 3, 4, 5) AS arr")
            df = spark.table("arr_view")
            transformed = df.select(F.transform("arr", lambda x: x * 2).alias("doubled"))
            return transformed.select(F.filter("doubled", lambda y: y > 5).alias("result"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "transform_then_filter")


@pytest.mark.differential
class TestCombinedOperations_Differential:
    """Tests combining lambda functions with other DataFrame operations"""

    @pytest.mark.timeout(30)
    def test_transform_multiple_rows(self, spark_reference, spark_thunderduck):
        """Test transform on multiple rows"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW multi_arr_view AS
                SELECT 1 AS id, ARRAY(1, 2, 3) AS numbers
                UNION ALL
                SELECT 2 AS id, ARRAY(4, 5, 6) AS numbers
            """)
            df = spark.table("multi_arr_view")
            return df.select(
                "id",
                F.transform("numbers", lambda x: x + 10).alias("transformed")
            ).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "transform_multiple_rows")

    @pytest.mark.timeout(30)
    def test_filter_in_where(self, spark_reference, spark_thunderduck):
        """Test using exists in filter"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW multi_arr_view AS
                SELECT 1 AS id, ARRAY(1, 2, 3) AS numbers
                UNION ALL
                SELECT 2 AS id, ARRAY(10, 20, 30) AS numbers
            """)
            df = spark.table("multi_arr_view")
            return df.filter(F.exists("numbers", lambda x: x > 15)).select("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "filter_in_where")


# ==================== Raw SQL Lambda Tests ====================


@pytest.mark.differential
class TestSQLLambda_Differential:
    """Tests for lambda expressions in raw SparkSQL queries"""

    @pytest.mark.timeout(30)
    def test_sql_transform(self, spark_reference, spark_thunderduck):
        """Test transform with lambda in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT transform(array(1, 2, 3), x -> x + 1) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_transform")

    @pytest.mark.timeout(30)
    def test_sql_filter(self, spark_reference, spark_thunderduck):
        """Test filter with lambda in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_filter")

    @pytest.mark.timeout(30)
    def test_sql_exists(self, spark_reference, spark_thunderduck):
        """Test exists with lambda in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT exists(array(1, 2, 3), x -> x > 2) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_exists")

    @pytest.mark.timeout(30)
    def test_sql_forall(self, spark_reference, spark_thunderduck):
        """Test forall with lambda in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT forall(array(10, 20, 30), x -> x > 5) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_forall")

    @pytest.mark.timeout(30)
    def test_sql_aggregate(self, spark_reference, spark_thunderduck):
        """Test aggregate with lambda in raw SQL"""
        def run_test(spark):
            return spark.sql(
                "SELECT aggregate(array(1, 2, 3, 4), 0, (acc, x) -> acc + x) AS result"
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_aggregate")

    @pytest.mark.timeout(30)
    def test_sql_transform_multiply(self, spark_reference, spark_thunderduck):
        """Test transform with multiplication lambda in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT transform(array(1, 2, 3), x -> x * 10) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_transform_multiply")

    @pytest.mark.timeout(30)
    def test_sql_filter_even(self, spark_reference, spark_thunderduck):
        """Test filter for even numbers in raw SQL"""
        def run_test(spark):
            return spark.sql("SELECT filter(array(1, 2, 3, 4, 5, 6), x -> x % 2 = 0) AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_filter_even")

    @pytest.mark.timeout(30)
    def test_sql_aggregate_product(self, spark_reference, spark_thunderduck):
        """Test aggregate product in raw SQL"""
        def run_test(spark):
            return spark.sql(
                "SELECT aggregate(array(1, 2, 3, 4), 1, (acc, x) -> acc * x) AS result"
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_aggregate_product")

    @pytest.mark.timeout(30)
    def test_sql_transform_with_table(self, spark_reference, spark_thunderduck):
        """Test transform on table data in raw SQL"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW sql_arr_view AS
                SELECT 1 AS id, ARRAY(1, 2, 3) AS numbers
                UNION ALL
                SELECT 2 AS id, ARRAY(4, 5, 6) AS numbers
            """)
            return spark.sql(
                "SELECT id, transform(numbers, x -> x + 100) AS result "
                "FROM sql_arr_view ORDER BY id"
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "sql_transform_with_table")
