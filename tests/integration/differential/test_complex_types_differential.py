"""
Differential Tests for Complex Type Expressions.

Tests cover:
- UnresolvedExtractValue: struct.field, arr[index], map[key]
- UpdateFields: col.withField() for struct manipulation

Converted from test_complex_types.py.
Uses Spark-compatible SQL syntax for struct/array/map literals.
"""

import pytest
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    ArrayType, MapType
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestStructFieldAccess_Differential:
    """Tests for struct field access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_struct_field_dot_notation(self, spark_reference, spark_thunderduck):
        """Test accessing struct field with dot notation"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW struct_view AS
                SELECT NAMED_STRUCT('name', 'Alice', 'age', 30) AS person
            """)
            df = spark.table("struct_view")
            return df.select(col("person.name").alias("name"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_field_dot_notation")

    @pytest.mark.timeout(30)
    def test_struct_field_bracket_notation(self, spark_reference, spark_thunderduck):
        """Test accessing struct field with bracket notation"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW struct_view AS
                SELECT NAMED_STRUCT('name', 'Bob', 'age', 25) AS person
            """)
            df = spark.table("struct_view")
            return df.select(col("person")["age"].alias("age"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_field_bracket_notation")

    @pytest.mark.timeout(30)
    def test_nested_struct_access(self, spark_reference, spark_thunderduck):
        """Test accessing nested struct fields"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW nested_view AS
                SELECT NAMED_STRUCT(
                    'name', 'Alice',
                    'address', NAMED_STRUCT('street', '123 Main St', 'city', 'NYC')
                ) AS person
            """)
            df = spark.table("nested_view")
            return df.select(col("person.address.city").alias("city"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "nested_struct_access")


@pytest.mark.differential
class TestArrayIndexing_Differential:
    """Tests for array element access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_array_first_element(self, spark_reference, spark_thunderduck):
        """Test accessing first element (index 0)"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(10, 20, 30) AS arr")
            df = spark.table("arr_view")
            return df.select(col("arr")[0].alias("first"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_first_element")

    @pytest.mark.timeout(30)
    def test_array_middle_element(self, spark_reference, spark_thunderduck):
        """Test accessing middle element"""
        def run_test(spark):
            spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT ARRAY(10, 20, 30, 40, 50) AS arr")
            df = spark.table("arr_view")
            return df.select(col("arr")[2].alias("middle"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_middle_element")


@pytest.mark.differential
class TestMapKeyAccess_Differential:
    """Tests for map key access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_map_string_key(self, spark_reference, spark_thunderduck):
        """Test accessing map value by string key"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW map_view AS
                SELECT MAP('a', 1, 'b', 2, 'c', 3) AS m
            """)
            df = spark.table("map_view")
            return df.select(col("m")["b"].alias("value"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_string_key")

    @pytest.mark.timeout(30)
    def test_map_missing_key(self, spark_reference, spark_thunderduck):
        """Test accessing map with missing key returns null"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW map_view AS
                SELECT MAP('a', 1) AS m
            """)
            df = spark.table("map_view")
            return df.select(col("m")["z"].alias("value"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_missing_key")


@pytest.mark.differential
class TestUpdateFields_Differential:
    """Tests for struct field manipulation using UpdateFields"""

    @pytest.mark.timeout(30)
    def test_with_field_add_new(self, spark_reference, spark_thunderduck):
        """Test adding a new field to a struct"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW struct_view AS
                SELECT NAMED_STRUCT('name', 'Alice') AS person
            """)
            df = spark.table("struct_view")
            return df.select(
                col("person").withField("age", lit(30)).alias("person_with_age")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "with_field_add_new")

    @pytest.mark.timeout(30)
    def test_with_field_add_multiple(self, spark_reference, spark_thunderduck):
        """Test adding multiple new fields to a struct"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW struct_view AS
                SELECT NAMED_STRUCT('name', 'Alice') AS person
            """)
            df = spark.table("struct_view")
            return df.select(
                col("person").withField("age", lit(30)).withField("city", lit("NYC")).alias("expanded_person")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "with_field_add_multiple")


@pytest.mark.differential
class TestDropFields_Differential:
    """Tests for struct field drop operations using dropFields"""

    @pytest.mark.timeout(30)
    def test_drop_single_field(self, spark_reference, spark_thunderduck):
        """Test dropping a single field from a struct"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW drop_struct_view AS
                SELECT NAMED_STRUCT('name', 'Alice', 'age', 30, 'city', 'NYC') AS person
            """)
            df = spark.table("drop_struct_view")
            return df.select(
                col("person").dropFields("age").alias("person_no_age")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "drop_single_field")


@pytest.mark.differential
class TestChainedExtraction_Differential:
    """Tests for chained extraction operations"""

    @pytest.mark.timeout(30)
    def test_array_of_structs(self, spark_reference, spark_thunderduck):
        """Test accessing field from array of structs"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW arr_struct_view AS
                SELECT ARRAY(
                    NAMED_STRUCT('name', 'Alice', 'age', 30),
                    NAMED_STRUCT('name', 'Bob', 'age', 25)
                ) AS people
            """)
            df = spark.table("arr_struct_view")
            return df.select(col("people")[0]["name"].alias("first_name"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_of_structs")

    @pytest.mark.timeout(30)
    def test_struct_with_array(self, spark_reference, spark_thunderduck):
        """Test accessing array element within a struct"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW struct_arr_view AS
                SELECT NAMED_STRUCT(
                    'name', 'Alice',
                    'scores', ARRAY(100, 200, 300)
                ) AS student
            """)
            df = spark.table("struct_arr_view")
            return df.select(col("student.scores")[1].alias("second_score"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_with_array")


@pytest.mark.differential
class TestMultipleRows_Differential:
    """Tests with multiple rows to ensure consistency"""

    @pytest.mark.timeout(30)
    def test_struct_access_multiple_rows(self, spark_reference, spark_thunderduck):
        """Test struct field access across multiple rows"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW multi_struct_view AS
                SELECT 1 AS id, NAMED_STRUCT('name', 'Alice', 'age', 30) AS person
                UNION ALL
                SELECT 2 AS id, NAMED_STRUCT('name', 'Bob', 'age', 25) AS person
                UNION ALL
                SELECT 3 AS id, NAMED_STRUCT('name', 'Carol', 'age', 35) AS person
            """)
            df = spark.table("multi_struct_view")
            return df.select("id", col("person.name").alias("name")).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_access_multiple_rows")

    @pytest.mark.timeout(30)
    def test_array_index_multiple_rows(self, spark_reference, spark_thunderduck):
        """Test array indexing across multiple rows"""
        def run_test(spark):
            spark.sql("""
                CREATE OR REPLACE TEMP VIEW multi_arr_view AS
                SELECT 1 AS id, ARRAY(10, 20, 30) AS arr
                UNION ALL
                SELECT 2 AS id, ARRAY(40, 50, 60) AS arr
            """)
            df = spark.table("multi_arr_view")
            return df.select("id", col("arr")[0].alias("first")).orderBy("id")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_index_multiple_rows")
