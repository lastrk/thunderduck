"""
Differential tests for JSON functions.

Validates that Thunderduck produces identical results to Apache Spark
for JSON parsing, extraction, and manipulation functions.

Uses spark.sql() with temp views to ensure functions go through
SparkSQLParser → FunctionRegistry translation path.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


def _create_json_data(spark, view_name="json_data"):
    data = [
        (1, '{"name": "Alice", "age": 30, "city": "NYC"}'),
        (2, '{"name": "Bob", "age": 25, "city": "LA"}'),
        (3, '{"name": "Charlie", "age": 35, "city": "Chicago"}'),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("json_str", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_json_array_data(spark, view_name="json_arr_data"):
    data = [
        (1, "[1, 2, 3]"),
        (2, "[10, 20]"),
        (3, "[]"),
        (4, "[1, 2, 3, 4, 5]"),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("json_arr", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


def _create_json_null_data(spark, view_name="json_null_data"):
    data = [
        (1, '{"name": "Alice", "age": 30}'),
        (2, None),
        (3, '{"name": "Charlie", "age": 35}'),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("json_str", StringType()),
    ])
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView(view_name)
    return df


@pytest.mark.differential
class TestJsonExtraction_Differential:
    """Tests for JSON extraction functions: get_json_object, json_tuple."""

    @pytest.mark.timeout(30)
    def test_get_json_object_name(self, spark_reference, spark_thunderduck):
        """Test get_json_object extracting a top-level string field."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, get_json_object(json_str, '$.name') as name "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "get_json_object_name", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_get_json_object_nested_age(self, spark_reference, spark_thunderduck):
        """Test get_json_object extracting a numeric field (returned as string)."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, get_json_object(json_str, '$.age') as age "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "get_json_object_age", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_json_tuple(self, spark_reference, spark_thunderduck):
        """Test json_tuple extracting multiple fields via SQL.

        json_tuple is a generator function that must be used in SQL context
        with the LATERAL VIEW or direct SELECT syntax.
        """
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, json_tuple(json_str, 'name', 'age') AS (name, age) "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "json_tuple", ignore_nullable=True)


@pytest.mark.differential
class TestJsonInfo_Differential:
    """Tests for JSON info functions: json_array_length, json_object_keys."""

    @pytest.mark.timeout(30)
    def test_json_array_length(self, spark_reference, spark_thunderduck):
        """Test json_array_length on JSON array strings."""
        def run_test(spark):
            _create_json_array_data(spark)
            return spark.sql(
                "SELECT id, CAST(json_array_length(json_arr) AS INT) as len "
                "FROM json_arr_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "json_array_length", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_json_object_keys(self, spark_reference, spark_thunderduck):
        """Test json_object_keys returning array of keys from a JSON object."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, json_object_keys(json_str) as keys "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "json_object_keys", ignore_nullable=True)


@pytest.mark.differential
class TestJsonConversion_Differential:
    """Tests for JSON conversion functions: to_json, schema_of_json."""

    @pytest.mark.timeout(30)
    def test_to_json(self, spark_reference, spark_thunderduck):
        """Test to_json converting a struct column to a JSON string."""
        def run_test(spark):
            data = [
                (1, "Alice", 30),
                (2, "Bob", 25),
                (3, "Charlie", 35),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("to_json_data")
            return spark.sql(
                "SELECT to_json(named_struct('id', id, 'name', name)) as json_out "
                "FROM to_json_data ORDER BY json_out"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "to_json", ignore_nullable=True)

    @pytest.mark.skip_relaxed(reason="schema_of_json format differs in relaxed mode; strict mode uses spark_schema_of_json extension")
    @pytest.mark.timeout(30)
    def test_schema_of_json(self, spark_reference, spark_thunderduck):
        """Test schema_of_json inferring schema from a JSON string literal."""
        def run_test(spark):
            data = [(1,)]
            schema = StructType([StructField("id", IntegerType(), False)])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("dummy_data")
            return spark.sql(
                """SELECT schema_of_json('{"a": 1, "b": "hello"}') as schema_str """
                "FROM dummy_data"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "schema_of_json", ignore_nullable=True)


@pytest.mark.differential
class TestFromJsonDifferential:
    """Tests for from_json: parsing JSON strings into typed structs using a schema."""

    @pytest.mark.timeout(30)
    def test_from_json_simple_ddl(self, spark_reference, spark_thunderduck):
        """Test from_json with a simple DDL schema string."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, "
                "from_json(json_str, 'name STRING, age INT').name as name, "
                "from_json(json_str, 'name STRING, age INT').age as age "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "from_json_simple_ddl", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_from_json_missing_keys(self, spark_reference, spark_thunderduck):
        """Test from_json when JSON is missing keys from the schema — should return NULL."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, "
                "from_json(json_str, 'name STRING, salary DOUBLE').name as name, "
                "from_json(json_str, 'name STRING, salary DOUBLE').salary as salary "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "from_json_missing_keys", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_from_json_null_input(self, spark_reference, spark_thunderduck):
        """Test from_json with NULL JSON input — should return NULL struct."""
        def run_test(spark):
            _create_json_null_data(spark)
            return spark.sql(
                "SELECT id, "
                "from_json(json_str, 'name STRING, age INT').name as name, "
                "from_json(json_str, 'name STRING, age INT').age as age "
                "FROM json_null_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "from_json_null_input", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_from_json_array_field(self, spark_reference, spark_thunderduck):
        """Test from_json with an array-typed field in the schema."""
        def run_test(spark):
            data = [
                (1, '{"tags": ["a", "b"], "count": 2}'),
                (2, '{"tags": ["x"], "count": 1}'),
                (3, '{"tags": [], "count": 0}'),
            ]
            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("json_str", StringType()),
            ])
            df = spark.createDataFrame(data, schema)
            df.createOrReplaceTempView("json_array_field_data")
            return spark.sql(
                "SELECT id, "
                "from_json(json_str, 'tags ARRAY<STRING>, count INT').count as cnt "
                "FROM json_array_field_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "from_json_array_field", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_from_json_dataframe_api(self, spark_reference, spark_thunderduck):
        """Test from_json via the DataFrame API with a StructType schema."""
        def run_test(spark):
            _create_json_data(spark)
            df = spark.table("json_data")
            parse_schema = StructType([
                StructField("name", StringType()),
                StructField("age", IntegerType()),
            ])
            return df.select(
                "id",
                F.from_json(F.col("json_str"), parse_schema).getField("name").alias("name"),
                F.from_json(F.col("json_str"), parse_schema).getField("age").alias("age"),
            ).orderBy("id")

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "from_json_dataframe_api", ignore_nullable=True)


@pytest.mark.differential
class TestJsonNullHandling_Differential:
    """Tests for JSON functions with NULL inputs and missing keys."""

    @pytest.mark.timeout(30)
    def test_get_json_object_with_null_json(self, spark_reference, spark_thunderduck):
        """Test get_json_object when the JSON string itself is NULL."""
        def run_test(spark):
            _create_json_null_data(spark)
            return spark.sql(
                "SELECT id, get_json_object(json_str, '$.name') as name "
                "FROM json_null_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "get_json_object_null_json", ignore_nullable=True)

    @pytest.mark.timeout(30)
    def test_get_json_object_missing_key(self, spark_reference, spark_thunderduck):
        """Test get_json_object with a path that does not exist in the JSON."""
        def run_test(spark):
            _create_json_data(spark)
            return spark.sql(
                "SELECT id, get_json_object(json_str, '$.nonexistent') as missing_val "
                "FROM json_data ORDER BY id"
            )

        ref = run_test(spark_reference)
        td = run_test(spark_thunderduck)
        assert_dataframes_equal(ref, td, "get_json_object_missing_key", ignore_nullable=True)
