"""
Differential Tests for Type Literals.

Tests cover proto literal conversions for:
- TimestampNTZ: Timestamp without timezone
- YearMonthInterval: Year-month intervals
- DayTimeInterval: Day-time intervals
- Array literals: Lists with recursive element conversion
- Map literals: Key-value pair collections
- Struct literals: Named field collections

Converted from test_type_literals.py.
Uses Spark-compatible SQL syntax.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql.functions import array, create_map, lit, struct


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


@pytest.mark.differential
class TestTimestampNTZLiterals_Differential:
    """Tests for TimestampNTZ literal support"""

    @pytest.mark.timeout(30)
    def test_timestamp_ntz_via_sql(self, spark_reference, spark_thunderduck):
        """Test TimestampNTZ created via SQL"""
        def run_test(spark):
            return spark.sql("SELECT TIMESTAMP '2025-01-15 10:30:00' AS ts")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "timestamp_ntz_via_sql")

    @pytest.mark.timeout(30)
    def test_timestamp_ntz_arithmetic(self, spark_reference, spark_thunderduck):
        """Test TimestampNTZ with interval arithmetic"""
        def run_test(spark):
            return spark.sql("""
                SELECT
                    TIMESTAMP '2025-01-15 10:30:00' AS ts,
                    TIMESTAMP '2025-01-15 10:30:00' + INTERVAL '1' DAY AS next_day
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "timestamp_ntz_arithmetic")


@pytest.mark.differential
class TestIntervalLiterals_Differential:
    """Tests for interval literal types via arithmetic operations"""

    @pytest.mark.timeout(30)
    def test_year_month_interval_in_arithmetic(self, spark_reference, spark_thunderduck):
        """Test YearMonthInterval via date arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT DATE '2025-01-15' + INTERVAL '2' YEAR AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "year_month_interval_arithmetic")

    @pytest.mark.timeout(30)
    def test_year_month_interval_months_arithmetic(self, spark_reference, spark_thunderduck):
        """Test YearMonthInterval months via date arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT DATE '2025-01-15' + INTERVAL '15' MONTH AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "year_month_interval_months_arithmetic")

    @pytest.mark.timeout(30)
    def test_day_time_interval_days_arithmetic(self, spark_reference, spark_thunderduck):
        """Test DayTimeInterval with days via timestamp arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT TIMESTAMP '2025-01-15 10:00:00' + INTERVAL '5' DAY AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "day_time_interval_days_arithmetic")

    @pytest.mark.timeout(30)
    def test_day_time_interval_hours_arithmetic(self, spark_reference, spark_thunderduck):
        """Test DayTimeInterval with hours via timestamp arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT TIMESTAMP '2025-01-15 10:00:00' + INTERVAL '10' HOUR AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "day_time_interval_hours_arithmetic")

    @pytest.mark.timeout(30)
    def test_day_time_interval_compound_arithmetic(self, spark_reference, spark_thunderduck):
        """Test compound DayTimeInterval via timestamp arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT TIMESTAMP '2025-01-15 10:00:00' + INTERVAL '2' DAY + INTERVAL '3' HOUR AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "day_time_interval_compound_arithmetic")

    @pytest.mark.timeout(30)
    def test_interval_date_arithmetic(self, spark_reference, spark_thunderduck):
        """Test interval arithmetic with dates"""
        def run_test(spark):
            return spark.sql("""
                SELECT
                    DATE '2025-01-15' AS d,
                    DATE '2025-01-15' + INTERVAL '1' MONTH AS next_month
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "interval_date_arithmetic")


@pytest.mark.differential
class TestArrayLiterals_Differential:
    """Tests for Array literal support"""

    @pytest.mark.timeout(30)
    def test_array_literal_via_sql(self, spark_reference, spark_thunderduck):
        """Test array literal created via SQL"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY(1, 2, 3) AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_literal_via_sql")

    @pytest.mark.timeout(30)
    def test_array_with_strings(self, spark_reference, spark_thunderduck):
        """Test array with string elements"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY('a', 'b', 'c') AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_with_strings")

    @pytest.mark.timeout(30)
    def test_array_with_null(self, spark_reference, spark_thunderduck):
        """Test array with NULL element"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY(1, NULL, 3) AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_with_null")

    @pytest.mark.timeout(30)
    def test_nested_array(self, spark_reference, spark_thunderduck):
        """Test nested array (array of arrays)"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY(ARRAY(1, 2), ARRAY(3, 4)) AS nested")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "nested_array")

    @pytest.mark.timeout(30)
    def test_empty_array(self, spark_reference, spark_thunderduck):
        """Test empty array"""
        def run_test(spark):
            # Spark requires type annotation for empty arrays
            return spark.sql("SELECT CAST(ARRAY() AS ARRAY<INT>) AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "empty_array")

    @pytest.mark.timeout(30)
    def test_array_pyspark_literal(self, spark_reference, spark_thunderduck):
        """Test array created via PySpark lit() with Python list"""
        def run_test(spark):
            return spark.range(1).select(array(lit(100), lit(200), lit(300)).alias("arr"))

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_pyspark_literal")


@pytest.mark.differential
class TestMapLiterals_Differential:
    """Tests for Map literal support"""

    @pytest.mark.timeout(30)
    def test_map_literal_via_sql(self, spark_reference, spark_thunderduck):
        """Test map literal created via SQL"""
        def run_test(spark):
            return spark.sql("SELECT MAP('a', 1, 'b', 2) AS m")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_literal_via_sql")

    @pytest.mark.timeout(30)
    def test_map_from_arrays(self, spark_reference, spark_thunderduck):
        """Test map from arrays"""
        def run_test(spark):
            return spark.sql("SELECT MAP_FROM_ARRAYS(ARRAY('x', 'y'), ARRAY(10, 20)) AS m")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_from_arrays")

    @pytest.mark.timeout(30)
    def test_map_key_access(self, spark_reference, spark_thunderduck):
        """Test map key access"""
        def run_test(spark):
            return spark.sql("SELECT MAP('a', 100)['a'] AS val")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_key_access")

    @pytest.mark.timeout(30)
    def test_map_missing_key(self, spark_reference, spark_thunderduck):
        """Test map missing key returns NULL"""
        def run_test(spark):
            return spark.sql("SELECT MAP('a', 1)['z'] AS val")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_missing_key")

    @pytest.mark.timeout(30)
    def test_map_pyspark_create_map(self, spark_reference, spark_thunderduck):
        """Test map created via PySpark create_map()"""
        def run_test(spark):
            return spark.range(1).select(
                create_map(lit("key1"), lit(42), lit("key2"), lit(84)).alias("m")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_pyspark_create_map")


@pytest.mark.differential
class TestStructLiterals_Differential:
    """Tests for Struct literal support"""

    @pytest.mark.timeout(30)
    def test_struct_literal_via_sql(self, spark_reference, spark_thunderduck):
        """Test struct literal created via SQL"""
        def run_test(spark):
            return spark.sql("SELECT NAMED_STRUCT('name', 'Alice', 'age', 30) AS person")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_literal_via_sql")

    @pytest.mark.timeout(30)
    def test_struct_field_access(self, spark_reference, spark_thunderduck):
        """Test struct field access"""
        def run_test(spark):
            return spark.sql("SELECT NAMED_STRUCT('x', 100, 'y', 200).x AS val")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_field_access")

    @pytest.mark.timeout(30)
    def test_nested_struct(self, spark_reference, spark_thunderduck):
        """Test nested struct"""
        def run_test(spark):
            return spark.sql("""
                SELECT NAMED_STRUCT(
                    'name', 'Alice',
                    'address', NAMED_STRUCT('street', '123 Main', 'city', 'NYC')
                ) AS person
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "nested_struct")

    @pytest.mark.timeout(30)
    def test_struct_pyspark_literal(self, spark_reference, spark_thunderduck):
        """Test struct created via PySpark struct()"""
        def run_test(spark):
            return spark.range(1).select(
                struct(lit(99).alias("id"), lit("Carol").alias("name")).alias("s")
            )

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_pyspark_literal")


@pytest.mark.differential
class TestComplexNestedTypes_Differential:
    """Tests for complex nested type combinations"""

    @pytest.mark.timeout(30)
    def test_array_of_structs(self, spark_reference, spark_thunderduck):
        """Test array containing structs"""
        def run_test(spark):
            return spark.sql("""
                SELECT ARRAY(
                    NAMED_STRUCT('name', 'Alice', 'age', 30),
                    NAMED_STRUCT('name', 'Bob', 'age', 25)
                ) AS people
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "array_of_structs")

    @pytest.mark.timeout(30)
    def test_struct_with_array(self, spark_reference, spark_thunderduck):
        """Test struct containing array"""
        def run_test(spark):
            return spark.sql("""
                SELECT NAMED_STRUCT(
                    'name', 'Alice',
                    'scores', ARRAY(90, 85, 95)
                ) AS student
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "struct_with_array")

    @pytest.mark.timeout(30)
    def test_map_with_array_values(self, spark_reference, spark_thunderduck):
        """Test map with array values"""
        def run_test(spark):
            return spark.sql("""
                SELECT MAP(
                    'evens', ARRAY(2, 4, 6),
                    'odds', ARRAY(1, 3, 5)
                ) AS groups
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "map_with_array_values")

    @pytest.mark.timeout(30)
    def test_deeply_nested(self, spark_reference, spark_thunderduck):
        """Test deeply nested structure"""
        def run_test(spark):
            return spark.sql("""
                SELECT NAMED_STRUCT(
                    'org', 'Tech Corp',
                    'departments', ARRAY(
                        NAMED_STRUCT(
                            'name', 'Engineering',
                            'employees', ARRAY(
                                NAMED_STRUCT('name', 'Alice', 'level', 5),
                                NAMED_STRUCT('name', 'Bob', 'level', 3)
                            )
                        )
                    )
                ) AS company
            """)

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "deeply_nested")


@pytest.mark.differential
class TestEdgeCases_Differential:
    """Tests for edge cases in type literals"""

    @pytest.mark.timeout(30)
    def test_null_in_struct(self, spark_reference, spark_thunderduck):
        """Test struct with NULL field"""
        def run_test(spark):
            return spark.sql("SELECT NAMED_STRUCT('name', 'Alice', 'age', CAST(NULL AS INT)) AS person")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "null_in_struct")

    @pytest.mark.timeout(30)
    def test_zero_interval(self, spark_reference, spark_thunderduck):
        """Test zero interval via timestamp arithmetic"""
        def run_test(spark):
            return spark.sql("SELECT TIMESTAMP '2025-01-15 10:00:00' + INTERVAL '0' DAY AS result")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "zero_interval")

    @pytest.mark.timeout(30)
    def test_large_array(self, spark_reference, spark_thunderduck):
        """Test larger array"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "large_array")

    @pytest.mark.timeout(30)
    def test_single_element_array(self, spark_reference, spark_thunderduck):
        """Test single element array"""
        def run_test(spark):
            return spark.sql("SELECT ARRAY(42) AS arr")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "single_element_array")

    @pytest.mark.timeout(30)
    def test_single_field_struct(self, spark_reference, spark_thunderduck):
        """Test single field struct"""
        def run_test(spark):
            return spark.sql("SELECT NAMED_STRUCT('only_field', 123) AS s")

        ref_result = run_test(spark_reference)
        test_result = run_test(spark_thunderduck)
        assert_dataframes_equal(ref_result, test_result, "single_field_struct")
