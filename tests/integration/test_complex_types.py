"""
Integration tests for complex type expressions.

Tests cover:
- UnresolvedExtractValue: struct.field, arr[index], map[key]
- UnresolvedRegex: df.colRegex() for regex column selection (limited)
- UpdateFields: col.withField() for struct manipulation

NOTE: These tests use SQL for creating complex types due to serialization
limitations with createDataFrame for complex types.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit


class TestStructFieldAccess:
    """Tests for struct field access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_struct_field_dot_notation(self, spark):
        """Test accessing struct field with dot notation"""
        # DuckDB uses {'name': value} syntax for struct literals
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW struct_view AS
            SELECT {'name': 'Alice', 'age': 30} AS person
        """)
        df = spark.table("struct_view")
        result = df.select(col("person.name").alias("name"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['name'] == 'Alice'
        print("\n struct.field dot notation works")

    @pytest.mark.timeout(30)
    def test_struct_field_bracket_notation(self, spark):
        """Test accessing struct field with bracket notation"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW struct_view AS
            SELECT {'name': 'Bob', 'age': 25} AS person
        """)
        df = spark.table("struct_view")
        result = df.select(col("person")["age"].alias("age"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['age'] == 25
        print("\n struct['field'] bracket notation works")

    @pytest.mark.timeout(30)
    def test_nested_struct_access(self, spark):
        """Test accessing nested struct fields"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW nested_view AS
            SELECT {
                'name': 'Alice',
                'address': {'street': '123 Main St', 'city': 'NYC'}
            } AS person
        """)
        df = spark.table("nested_view")
        result = df.select(col("person.address.city").alias("city"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['city'] == 'NYC'
        print("\n nested struct access (person.address.city) works")


class TestArrayIndexing:
    """Tests for array element access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_array_first_element(self, spark):
        """Test accessing first element (index 0)"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30] AS arr")
        df = spark.table("arr_view")
        result = df.select(col("arr")[0].alias("first"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['first'] == 10
        print("\n arr[0] first element access works")

    @pytest.mark.timeout(30)
    def test_array_middle_element(self, spark):
        """Test accessing middle element"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30, 40, 50] AS arr")
        df = spark.table("arr_view")
        result = df.select(col("arr")[2].alias("middle"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['middle'] == 30
        print("\n arr[2] middle element access works")

    @pytest.mark.timeout(30)
    def test_array_negative_index_last(self, spark):
        """Test accessing last element with negative index"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30] AS arr")
        df = spark.table("arr_view")
        result = df.select(col("arr")[-1].alias("last"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['last'] == 30
        print("\n arr[-1] last element access works")

    @pytest.mark.timeout(30)
    def test_array_negative_index_second_last(self, spark):
        """Test accessing second-to-last element"""
        spark.sql("CREATE OR REPLACE TEMP VIEW arr_view AS SELECT [10, 20, 30, 40] AS arr")
        df = spark.table("arr_view")
        result = df.select(col("arr")[-2].alias("second_last"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['second_last'] == 30
        print("\n arr[-2] second-to-last element access works")


class TestMapKeyAccess:
    """Tests for map key access using UnresolvedExtractValue"""

    @pytest.mark.timeout(30)
    def test_map_string_key(self, spark):
        """Test accessing map value by string key"""
        # DuckDB uses MAP {'key': value} syntax
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW map_view AS
            SELECT MAP {'a': 1, 'b': 2, 'c': 3} AS m
        """)
        df = spark.table("map_view")
        # Map access in Spark uses bracket notation with string key
        result = df.select(col("m")["b"].alias("value"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['value'] == 2
        print("\n map['key'] string key access works")

    @pytest.mark.timeout(30)
    def test_map_missing_key(self, spark):
        """Test accessing map with missing key returns null"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW map_view AS
            SELECT MAP {'a': 1} AS m
        """)
        df = spark.table("map_view")
        result = df.select(col("m")["z"].alias("value"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['value'] is None
        print("\n map['missing'] returns null correctly")


class TestUpdateFields:
    """Tests for struct field manipulation using UpdateFields"""

    @pytest.mark.timeout(30)
    def test_with_field_add_new(self, spark):
        """Test adding a new field to a struct"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW struct_view AS
            SELECT {'name': 'Alice'} AS person
        """)
        df = spark.table("struct_view")
        # withField adds a new field
        result = df.select(
            col("person").withField("age", lit(30)).alias("person_with_age")
        )
        rows = result.collect()
        assert len(rows) == 1
        person = rows[0]['person_with_age']
        # The struct should now have both name and age
        assert person['name'] == 'Alice'
        assert person['age'] == 30
        print("\n col.withField() add new field works")

    @pytest.mark.timeout(30)
    def test_with_field_add_multiple(self, spark):
        """Test adding multiple new fields to a struct"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW struct_view AS
            SELECT {'name': 'Alice'} AS person
        """)
        df = spark.table("struct_view")
        # Add multiple new fields with chained withField calls
        result = df.select(
            col("person").withField("age", lit(30)).withField("city", lit("NYC")).alias("expanded_person")
        )
        rows = result.collect()
        assert len(rows) == 1
        person = rows[0]['expanded_person']
        assert person['name'] == 'Alice'
        assert person['age'] == 30
        assert person['city'] == 'NYC'
        print("\n col.withField() chained add works")

    # NOTE: Replacing existing struct fields is not supported by DuckDB struct_insert
    # It would require extracting all fields and reconstructing the struct, which
    # needs schema introspection. This is a known limitation.


class TestChainedExtraction:
    """Tests for chained extraction operations"""

    @pytest.mark.timeout(30)
    def test_array_of_structs(self, spark):
        """Test accessing field from array of structs"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW arr_struct_view AS
            SELECT [
                {'name': 'Alice', 'age': 30},
                {'name': 'Bob', 'age': 25}
            ] AS people
        """)
        df = spark.table("arr_struct_view")
        # Access first person's name
        result = df.select(col("people")[0]["name"].alias("first_name"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['first_name'] == 'Alice'
        print("\n array_of_structs[0]['name'] chained access works")

    @pytest.mark.timeout(30)
    def test_struct_with_array(self, spark):
        """Test accessing array element within a struct"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW struct_arr_view AS
            SELECT {
                'name': 'Alice',
                'scores': [100, 200, 300]
            } AS student
        """)
        df = spark.table("struct_arr_view")
        # Access student's second score
        result = df.select(col("student.scores")[1].alias("second_score"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['second_score'] == 200
        print("\n struct.array[index] chained access works")


class TestMultipleRows:
    """Tests with multiple rows to ensure consistency"""

    @pytest.mark.timeout(30)
    def test_struct_access_multiple_rows(self, spark):
        """Test struct field access across multiple rows"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW multi_struct_view AS
            SELECT 1 AS id, {'name': 'Alice', 'age': 30} AS person
            UNION ALL
            SELECT 2 AS id, {'name': 'Bob', 'age': 25} AS person
            UNION ALL
            SELECT 3 AS id, {'name': 'Carol', 'age': 35} AS person
        """)
        df = spark.table("multi_struct_view")
        result = df.select("id", col("person.name").alias("name")).orderBy("id")
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0]['name'] == 'Alice'
        assert rows[1]['name'] == 'Bob'
        assert rows[2]['name'] == 'Carol'
        print("\n struct field access across multiple rows works")

    @pytest.mark.timeout(30)
    def test_array_index_multiple_rows(self, spark):
        """Test array indexing across multiple rows"""
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW multi_arr_view AS
            SELECT 1 AS id, [10, 20, 30] AS arr
            UNION ALL
            SELECT 2 AS id, [40, 50, 60] AS arr
        """)
        df = spark.table("multi_arr_view")
        result = df.select("id", col("arr")[0].alias("first")).orderBy("id")
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]['first'] == 10
        assert rows[1]['first'] == 40
        print("\n array indexing across multiple rows works")
