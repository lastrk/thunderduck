"""
DataFrame Functions Differential Tests

Tests DataFrame API functions for parity between Apache Spark 4.0.1 and Thunderduck.
Based on Apache Spark's DataFrameFunctionsSuite.

Categories tested:
- Collection Functions (Arrays)
- Collection Functions (Maps)
- Struct Functions
- Null Handling Functions
- String Functions
- Math Functions
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, ArrayType, MapType, BooleanType
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture(scope="class")
def array_test_data(spark_reference, spark_thunderduck):
    """Create test data with array columns"""
    data = [
        (1, [1, 2, 3], [3, 4, 5]),
        (2, [4, 5, 6], [5, 6, 7]),
        (3, [1, 1, 2], [2, 2, 3]),
        (4, None, [1, 2]),
        (5, [7, 8, 9], None),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("arr1", ArrayType(IntegerType()), True),
        StructField("arr2", ArrayType(IntegerType()), True),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def map_test_data(spark_reference, spark_thunderduck):
    """Create test data with map columns"""
    data = [
        (1, {"a": 1, "b": 2}),
        (2, {"c": 3, "d": 4}),
        (3, {"a": 5}),
        (4, None),
        (5, {}),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("map_col", MapType(StringType(), IntegerType()), True),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def null_test_data(spark_reference, spark_thunderduck):
    """Create test data for null handling functions"""
    data = [
        (1, "a", 10),
        (2, None, 20),
        (3, "c", None),
        (4, None, None),
        (5, "e", 0),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("str_col", StringType(), True),
        StructField("int_col", IntegerType(), True),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def string_test_data(spark_reference, spark_thunderduck):
    """Create test data for string functions"""
    data = [
        (1, "hello", "world"),
        (2, "SPARK", "sql"),
        (3, "  trim me  ", "padded"),
        (4, None, "value"),
        (5, "a,b,c,d", "b"),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("str1", StringType(), True),
        StructField("str2", StringType(), True),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def math_test_data(spark_reference, spark_thunderduck):
    """Create test data for math functions"""
    data = [
        (1, 10, 3),
        (2, -5, 2),
        (3, 0, 1),
        (4, 100, 7),
        (5, None, 5),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("num1", IntegerType(), True),
        StructField("num2", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


# ============================================================================
# Array Functions Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
class TestArrayFunctions:
    """Tests for array manipulation functions"""

    def test_array_contains(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_contains function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.array_contains("arr1", 2).alias("contains_2"))
        td_result = df_td.select("id", F.array_contains("arr1", 2).alias("contains_2"))

        assert_dataframes_equal(ref_result, td_result, query_name="array_contains")

    def test_array_size(self, spark_reference, spark_thunderduck, array_test_data):
        """Test size/array_size function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.size("arr1").alias("arr_size"))
        td_result = df_td.select("id", F.size("arr1").alias("arr_size"))

        assert_dataframes_equal(ref_result, td_result, query_name="array_size")

    def test_array_sort(self, spark_reference, spark_thunderduck, array_test_data):
        """Test sort_array function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.sort_array("arr1").alias("sorted"))
        td_result = df_td.select("id", F.sort_array("arr1").alias("sorted"))

        assert_dataframes_equal(ref_result, td_result, query_name="sort_array")

    def test_array_sort_desc(self, spark_reference, spark_thunderduck, array_test_data):
        """Test sort_array descending"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.sort_array("arr1", asc=False).alias("sorted_desc"))
        td_result = df_td.select("id", F.sort_array("arr1", asc=False).alias("sorted_desc"))

        assert_dataframes_equal(ref_result, td_result, query_name="sort_array_desc")

    def test_array_distinct(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_distinct function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.array_distinct("arr1").alias("distinct"))
        td_result = df_td.select("id", F.array_distinct("arr1").alias("distinct"))

        assert_dataframes_equal(ref_result, td_result, query_name="array_distinct")

    def test_array_union(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_union function"""
        df_ref, df_td = array_test_data

        # Filter out nulls for this test
        ref_result = (df_ref
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_union("arr1", "arr2").alias("union")))
        td_result = (df_td
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_union("arr1", "arr2").alias("union")))

        assert_dataframes_equal(ref_result, td_result, query_name="array_union")

    def test_array_intersect(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_intersect function"""
        df_ref, df_td = array_test_data

        ref_result = (df_ref
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_intersect("arr1", "arr2").alias("intersect")))
        td_result = (df_td
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_intersect("arr1", "arr2").alias("intersect")))

        assert_dataframes_equal(ref_result, td_result, query_name="array_intersect")

    def test_array_except(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_except function"""
        df_ref, df_td = array_test_data

        ref_result = (df_ref
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_except("arr1", "arr2").alias("except")))
        td_result = (df_td
            .filter(F.col("arr1").isNotNull() & F.col("arr2").isNotNull())
            .select("id", F.array_except("arr1", "arr2").alias("except")))

        assert_dataframes_equal(ref_result, td_result, query_name="array_except")

    def test_arrays_overlap(self, spark_reference, spark_thunderduck, array_test_data):
        """Test arrays_overlap function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.arrays_overlap("arr1", "arr2").alias("overlap"))
        td_result = df_td.select("id", F.arrays_overlap("arr1", "arr2").alias("overlap"))

        assert_dataframes_equal(ref_result, td_result, query_name="arrays_overlap")

    def test_array_position(self, spark_reference, spark_thunderduck, array_test_data):
        """Test array_position function (1-indexed)"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.array_position("arr1", 2).alias("position"))
        td_result = df_td.select("id", F.array_position("arr1", 2).alias("position"))

        assert_dataframes_equal(ref_result, td_result, query_name="array_position")

    def test_element_at(self, spark_reference, spark_thunderduck, array_test_data):
        """Test element_at function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.element_at("arr1", 2).alias("second_element"))
        td_result = df_td.select("id", F.element_at("arr1", 2).alias("second_element"))

        assert_dataframes_equal(ref_result, td_result, query_name="element_at")

    def test_explode(self, spark_reference, spark_thunderduck, array_test_data):
        """Test explode function"""
        df_ref, df_td = array_test_data

        ref_result = (df_ref
            .filter(F.col("arr1").isNotNull())
            .select("id", F.explode("arr1").alias("exploded"))
            .orderBy("id", "exploded"))
        td_result = (df_td
            .filter(F.col("arr1").isNotNull())
            .select("id", F.explode("arr1").alias("exploded"))
            .orderBy("id", "exploded"))

        assert_dataframes_equal(ref_result, td_result, query_name="explode")

    def test_explode_outer(self, spark_reference, spark_thunderduck, array_test_data):
        """Test explode_outer function (preserves nulls)"""
        df_ref, df_td = array_test_data

        ref_result = (df_ref
            .select("id", F.explode_outer("arr1").alias("exploded"))
            .orderBy("id", F.col("exploded").asc_nulls_last()))
        td_result = (df_td
            .select("id", F.explode_outer("arr1").alias("exploded"))
            .orderBy("id", F.col("exploded").asc_nulls_last()))

        assert_dataframes_equal(ref_result, td_result, query_name="explode_outer")

    def test_flatten(self, spark_reference, spark_thunderduck):
        """Test flatten function for nested arrays"""
        data = [
            (1, [[1, 2], [3, 4]]),
            (2, [[5], [6, 7, 8]]),
            (3, [[], [9]]),
        ]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("nested", ArrayType(ArrayType(IntegerType())), True),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id", F.flatten("nested").alias("flat"))
        td_result = df_td.select("id", F.flatten("nested").alias("flat"))

        assert_dataframes_equal(ref_result, td_result, query_name="flatten")

    def test_reverse_array(self, spark_reference, spark_thunderduck, array_test_data):
        """Test reverse function on arrays"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.reverse("arr1").alias("reversed"))
        td_result = df_td.select("id", F.reverse("arr1").alias("reversed"))

        assert_dataframes_equal(ref_result, td_result, query_name="reverse_array")

    def test_slice(self, spark_reference, spark_thunderduck, array_test_data):
        """Test slice function"""
        df_ref, df_td = array_test_data

        ref_result = df_ref.select("id", F.slice("arr1", 1, 2).alias("sliced"))
        td_result = df_td.select("id", F.slice("arr1", 1, 2).alias("sliced"))

        assert_dataframes_equal(ref_result, td_result, query_name="slice")

    def test_array_join(self, spark_reference, spark_thunderduck):
        """Test array_join function"""
        data = [
            (1, ["a", "b", "c"]),
            (2, ["x", "y"]),
            (3, ["single"]),
        ]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("arr", ArrayType(StringType()), True),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id", F.array_join("arr", ",").alias("joined"))
        td_result = df_td.select("id", F.array_join("arr", ",").alias("joined"))

        assert_dataframes_equal(ref_result, td_result, query_name="array_join")


# ============================================================================
# Map Functions Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
class TestMapFunctions:
    """Tests for map manipulation functions"""

    def test_map_keys(self, spark_reference, spark_thunderduck, map_test_data):
        """Test map_keys function"""
        df_ref, df_td = map_test_data

        ref_result = df_ref.select("id", F.map_keys("map_col").alias("keys"))
        td_result = df_td.select("id", F.map_keys("map_col").alias("keys"))

        assert_dataframes_equal(ref_result, td_result, query_name="map_keys")

    def test_map_values(self, spark_reference, spark_thunderduck, map_test_data):
        """Test map_values function"""
        df_ref, df_td = map_test_data

        ref_result = df_ref.select("id", F.map_values("map_col").alias("values"))
        td_result = df_td.select("id", F.map_values("map_col").alias("values"))

        assert_dataframes_equal(ref_result, td_result, query_name="map_values")

    def test_map_entries(self, spark_reference, spark_thunderduck, map_test_data):
        """Test map_entries function"""
        df_ref, df_td = map_test_data

        ref_result = df_ref.select("id", F.map_entries("map_col").alias("entries"))
        td_result = df_td.select("id", F.map_entries("map_col").alias("entries"))

        assert_dataframes_equal(ref_result, td_result, query_name="map_entries")

    def test_size_map(self, spark_reference, spark_thunderduck, map_test_data):
        """Test size function on maps"""
        df_ref, df_td = map_test_data

        ref_result = df_ref.select("id", F.size("map_col").alias("map_size"))
        td_result = df_td.select("id", F.size("map_col").alias("map_size"))

        assert_dataframes_equal(ref_result, td_result, query_name="size_map")

    def test_element_at_map(self, spark_reference, spark_thunderduck, map_test_data):
        """Test element_at function on maps"""
        df_ref, df_td = map_test_data

        ref_result = df_ref.select("id", F.element_at("map_col", "a").alias("value_a"))
        td_result = df_td.select("id", F.element_at("map_col", "a").alias("value_a"))

        assert_dataframes_equal(ref_result, td_result, query_name="element_at_map")

    def test_map_from_arrays(self, spark_reference, spark_thunderduck):
        """Test map_from_arrays function"""
        data = [
            (1, ["a", "b"], [1, 2]),
            (2, ["x", "y", "z"], [10, 20, 30]),
        ]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("keys", ArrayType(StringType()), True),
            StructField("values", ArrayType(IntegerType()), True),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id", F.map_from_arrays("keys", "values").alias("map"))
        td_result = df_td.select("id", F.map_from_arrays("keys", "values").alias("map"))

        assert_dataframes_equal(ref_result, td_result, query_name="map_from_arrays")

    def test_explode_map(self, spark_reference, spark_thunderduck, map_test_data):
        """Test explode on maps"""
        df_ref, df_td = map_test_data

        ref_result = (df_ref
            .filter(F.col("map_col").isNotNull() & (F.size("map_col") > 0))
            .select("id", F.explode("map_col"))
            .orderBy("id", "key"))
        td_result = (df_td
            .filter(F.col("map_col").isNotNull() & (F.size("map_col") > 0))
            .select("id", F.explode("map_col"))
            .orderBy("id", "key"))

        assert_dataframes_equal(ref_result, td_result, query_name="explode_map")


# ============================================================================
# Null Handling Functions Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
class TestNullFunctions:
    """Tests for null handling functions"""

    def test_coalesce(self, spark_reference, spark_thunderduck, null_test_data):
        """Test coalesce function"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.coalesce("str_col", F.lit("default")).alias("result"))
        td_result = df_td.select("id", F.coalesce("str_col", F.lit("default")).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="coalesce")

    def test_isnull(self, spark_reference, spark_thunderduck, null_test_data):
        """Test isnull function"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.isnull("str_col").alias("is_null"))
        td_result = df_td.select("id", F.isnull("str_col").alias("is_null"))

        assert_dataframes_equal(ref_result, td_result, query_name="isnull")

    def test_isnotnull(self, spark_reference, spark_thunderduck, null_test_data):
        """Test isnotnull via Column.isNotNull()"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.col("str_col").isNotNull().alias("is_not_null"))
        td_result = df_td.select("id", F.col("str_col").isNotNull().alias("is_not_null"))

        assert_dataframes_equal(ref_result, td_result, query_name="isnotnull")

    def test_ifnull(self, spark_reference, spark_thunderduck, null_test_data):
        """Test ifnull function"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.ifnull("str_col", F.lit("replaced")).alias("result"))
        td_result = df_td.select("id", F.ifnull("str_col", F.lit("replaced")).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="ifnull")

    def test_nvl(self, spark_reference, spark_thunderduck, null_test_data):
        """Test nvl function"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.nvl("int_col", F.lit(-1)).alias("result"))
        td_result = df_td.select("id", F.nvl("int_col", F.lit(-1)).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="nvl")

    def test_nvl2(self, spark_reference, spark_thunderduck, null_test_data):
        """Test nvl2 function (returns val1 if not null, else val2)"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id",
            F.nvl2("str_col", F.lit("has_value"), F.lit("is_null")).alias("result"))
        td_result = df_td.select("id",
            F.nvl2("str_col", F.lit("has_value"), F.lit("is_null")).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="nvl2")

    def test_nullif(self, spark_reference, spark_thunderduck, null_test_data):
        """Test nullif function (returns null if values equal)"""
        df_ref, df_td = null_test_data

        ref_result = df_ref.select("id", F.nullif("int_col", F.lit(0)).alias("result"))
        td_result = df_td.select("id", F.nullif("int_col", F.lit(0)).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="nullif")

    def test_nanvl(self, spark_reference, spark_thunderduck):
        """Test nanvl function (replace NaN with value)"""
        data = [
            (1, 1.0),
            (2, float('nan')),
            (3, 3.0),
            (4, None),
        ]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("val", DoubleType(), True),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id", F.nanvl("val", F.lit(0.0)).alias("result"))
        td_result = df_td.select("id", F.nanvl("val", F.lit(0.0)).alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="nanvl")


# ============================================================================
# String Functions Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
class TestStringFunctions:
    """Tests for string manipulation functions"""

    def test_concat(self, spark_reference, spark_thunderduck, string_test_data):
        """Test concat function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.concat("str1", F.lit("-"), "str2").alias("result"))
        td_result = df_td.select("id", F.concat("str1", F.lit("-"), "str2").alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="concat")

    def test_concat_ws(self, spark_reference, spark_thunderduck, string_test_data):
        """Test concat_ws function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.concat_ws("-", "str1", "str2").alias("result"))
        td_result = df_td.select("id", F.concat_ws("-", "str1", "str2").alias("result"))

        assert_dataframes_equal(ref_result, td_result, query_name="concat_ws")

    def test_upper_lower(self, spark_reference, spark_thunderduck, string_test_data):
        """Test upper and lower functions"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id",
            F.upper("str1").alias("upper"),
            F.lower("str1").alias("lower"))
        td_result = df_td.select("id",
            F.upper("str1").alias("upper"),
            F.lower("str1").alias("lower"))

        assert_dataframes_equal(ref_result, td_result, query_name="upper_lower")

    def test_trim(self, spark_reference, spark_thunderduck, string_test_data):
        """Test trim, ltrim, rtrim functions"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id",
            F.trim("str1").alias("trimmed"),
            F.ltrim("str1").alias("ltrimmed"),
            F.rtrim("str1").alias("rtrimmed"))
        td_result = df_td.select("id",
            F.trim("str1").alias("trimmed"),
            F.ltrim("str1").alias("ltrimmed"),
            F.rtrim("str1").alias("rtrimmed"))

        assert_dataframes_equal(ref_result, td_result, query_name="trim")

    def test_length(self, spark_reference, spark_thunderduck, string_test_data):
        """Test length function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.length("str1").alias("len"))
        td_result = df_td.select("id", F.length("str1").alias("len"))

        assert_dataframes_equal(ref_result, td_result, query_name="length")

    def test_substring(self, spark_reference, spark_thunderduck, string_test_data):
        """Test substring function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.substring("str1", 1, 3).alias("sub"))
        td_result = df_td.select("id", F.substring("str1", 1, 3).alias("sub"))

        assert_dataframes_equal(ref_result, td_result, query_name="substring")

    def test_instr(self, spark_reference, spark_thunderduck, string_test_data):
        """Test instr function (find substring position)"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.instr("str1", "l").alias("pos"))
        td_result = df_td.select("id", F.instr("str1", "l").alias("pos"))

        assert_dataframes_equal(ref_result, td_result, query_name="instr")

    def test_locate(self, spark_reference, spark_thunderduck, string_test_data):
        """Test locate function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.locate("l", "str1").alias("pos"))
        td_result = df_td.select("id", F.locate("l", "str1").alias("pos"))

        assert_dataframes_equal(ref_result, td_result, query_name="locate")

    def test_lpad_rpad(self, spark_reference, spark_thunderduck, string_test_data):
        """Test lpad and rpad functions"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id",
            F.lpad("str2", 10, "*").alias("lpadded"),
            F.rpad("str2", 10, "*").alias("rpadded"))
        td_result = df_td.select("id",
            F.lpad("str2", 10, "*").alias("lpadded"),
            F.rpad("str2", 10, "*").alias("rpadded"))

        assert_dataframes_equal(ref_result, td_result, query_name="lpad_rpad")

    def test_repeat(self, spark_reference, spark_thunderduck, string_test_data):
        """Test repeat function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.repeat("str2", 3).alias("repeated"))
        td_result = df_td.select("id", F.repeat("str2", 3).alias("repeated"))

        assert_dataframes_equal(ref_result, td_result, query_name="repeat")

    def test_reverse_string(self, spark_reference, spark_thunderduck, string_test_data):
        """Test reverse function on strings"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.reverse("str1").alias("reversed"))
        td_result = df_td.select("id", F.reverse("str1").alias("reversed"))

        assert_dataframes_equal(ref_result, td_result, query_name="reverse_string")

    def test_split(self, spark_reference, spark_thunderduck, string_test_data):
        """Test split function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.split("str1", ",").alias("parts"))
        td_result = df_td.select("id", F.split("str1", ",").alias("parts"))

        assert_dataframes_equal(ref_result, td_result, query_name="split")

    def test_replace(self, spark_reference, spark_thunderduck, string_test_data):
        """Test replace function"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.regexp_replace("str1", "l", "L").alias("replaced"))
        td_result = df_td.select("id", F.regexp_replace("str1", "l", "L").alias("replaced"))

        assert_dataframes_equal(ref_result, td_result, query_name="regexp_replace")

    def test_initcap(self, spark_reference, spark_thunderduck, string_test_data):
        """Test initcap function (capitalize first letter of each word)"""
        df_ref, df_td = string_test_data

        ref_result = df_ref.select("id", F.initcap("str1").alias("initcap"))
        td_result = df_td.select("id", F.initcap("str1").alias("initcap"))

        assert_dataframes_equal(ref_result, td_result, query_name="initcap")


# ============================================================================
# Math Functions Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
class TestMathFunctions:
    """Tests for mathematical functions"""

    def test_abs(self, spark_reference, spark_thunderduck, math_test_data):
        """Test abs function"""
        df_ref, df_td = math_test_data

        ref_result = df_ref.select("id", F.abs("num1").alias("abs_val"))
        td_result = df_td.select("id", F.abs("num1").alias("abs_val"))

        assert_dataframes_equal(ref_result, td_result, query_name="abs")

    def test_ceil_floor(self, spark_reference, spark_thunderduck):
        """Test ceil and floor functions"""
        data = [(1, 1.5), (2, 2.3), (3, -1.7), (4, 3.0)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("val", DoubleType(), False),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id",
            F.ceil("val").alias("ceiling"),
            F.floor("val").alias("floored"))
        td_result = df_td.select("id",
            F.ceil("val").alias("ceiling"),
            F.floor("val").alias("floored"))

        assert_dataframes_equal(ref_result, td_result, query_name="ceil_floor")

    def test_round(self, spark_reference, spark_thunderduck):
        """Test round function"""
        data = [(1, 1.456), (2, 2.345), (3, -1.789), (4, 3.555)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("val", DoubleType(), False),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id",
            F.round("val", 2).alias("round2"),
            F.round("val", 0).alias("round0"))
        td_result = df_td.select("id",
            F.round("val", 2).alias("round2"),
            F.round("val", 0).alias("round0"))

        assert_dataframes_equal(ref_result, td_result, query_name="round")

    def test_sqrt(self, spark_reference, spark_thunderduck, math_test_data):
        """Test sqrt function"""
        df_ref, df_td = math_test_data

        ref_result = (df_ref
            .filter(F.col("num1") >= 0)
            .select("id", F.sqrt("num1").alias("sqrt_val")))
        td_result = (df_td
            .filter(F.col("num1") >= 0)
            .select("id", F.sqrt("num1").alias("sqrt_val")))

        assert_dataframes_equal(ref_result, td_result, query_name="sqrt", epsilon=1e-10)

    def test_pow(self, spark_reference, spark_thunderduck, math_test_data):
        """Test pow function"""
        df_ref, df_td = math_test_data

        ref_result = df_ref.select("id", F.pow("num1", "num2").alias("power"))
        td_result = df_td.select("id", F.pow("num1", "num2").alias("power"))

        assert_dataframes_equal(ref_result, td_result, query_name="pow", epsilon=1e-10)

    def test_mod(self, spark_reference, spark_thunderduck, math_test_data):
        """Test mod function"""
        df_ref, df_td = math_test_data

        ref_result = (df_ref
            .filter(F.col("num1").isNotNull())
            .select("id", (F.col("num1") % F.col("num2")).alias("mod_val")))
        td_result = (df_td
            .filter(F.col("num1").isNotNull())
            .select("id", (F.col("num1") % F.col("num2")).alias("mod_val")))

        assert_dataframes_equal(ref_result, td_result, query_name="mod")

    def test_pmod(self, spark_reference, spark_thunderduck, math_test_data):
        """Test pmod function (positive modulo)"""
        df_ref, df_td = math_test_data

        ref_result = (df_ref
            .filter(F.col("num1").isNotNull())
            .select("id", F.pmod("num1", "num2").alias("pmod_val")))
        td_result = (df_td
            .filter(F.col("num1").isNotNull())
            .select("id", F.pmod("num1", "num2").alias("pmod_val")))

        assert_dataframes_equal(ref_result, td_result, query_name="pmod")

    def test_greatest_least(self, spark_reference, spark_thunderduck, math_test_data):
        """Test greatest and least functions"""
        df_ref, df_td = math_test_data

        ref_result = df_ref.select("id",
            F.greatest("num1", "num2").alias("max_val"),
            F.least("num1", "num2").alias("min_val"))
        td_result = df_td.select("id",
            F.greatest("num1", "num2").alias("max_val"),
            F.least("num1", "num2").alias("min_val"))

        assert_dataframes_equal(ref_result, td_result, query_name="greatest_least")

    def test_log(self, spark_reference, spark_thunderduck):
        """Test log functions"""
        data = [(1, 10.0), (2, 100.0), (3, 1.0), (4, 2.718281828)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("val", DoubleType(), False),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id",
            F.log("val").alias("ln"),
            F.log10("val").alias("log10"),
            F.log2("val").alias("log2"))
        td_result = df_td.select("id",
            F.log("val").alias("ln"),
            F.log10("val").alias("log10"),
            F.log2("val").alias("log2"))

        assert_dataframes_equal(ref_result, td_result, query_name="log", epsilon=1e-10)

    def test_exp(self, spark_reference, spark_thunderduck):
        """Test exp function"""
        data = [(1, 0.0), (2, 1.0), (3, 2.0), (4, -1.0)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("val", DoubleType(), False),
        ])

        df_ref = spark_reference.createDataFrame(data, schema)
        df_td = spark_thunderduck.createDataFrame(data, schema)

        ref_result = df_ref.select("id", F.exp("val").alias("exp_val"))
        td_result = df_td.select("id", F.exp("val").alias("exp_val"))

        assert_dataframes_equal(ref_result, td_result, query_name="exp", epsilon=1e-10)

    def test_sign(self, spark_reference, spark_thunderduck, math_test_data):
        """Test sign/signum function"""
        df_ref, df_td = math_test_data

        ref_result = df_ref.select("id", F.signum("num1").alias("sign"))
        td_result = df_td.select("id", F.signum("num1").alias("sign"))

        assert_dataframes_equal(ref_result, td_result, query_name="sign")
