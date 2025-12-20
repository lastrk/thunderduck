"""
Catalog Operations Differential Tests

Tests for Spark Connect catalog operations comparing Thunderduck against Apache Spark 4.0.1.

Note: Only operations that have consistent behavior between Spark and DuckDB are tested
differentially. Operations specific to DuckDB storage (table creation, database management)
have been removed as they cannot be meaningfully compared.
"""

import pytest


class TestTableExists:
    """Test spark.catalog.tableExists() - differential tests"""

    def test_table_exists_false(self, spark_reference, spark_thunderduck):
        """Non-existent table should return False on both systems."""
        ref_result = spark_reference.catalog.tableExists("nonexistent_table_xyz_12345")
        td_result = spark_thunderduck.catalog.tableExists("nonexistent_table_xyz_12345")
        assert ref_result == td_result == False, f"Mismatch: Spark={ref_result}, TD={td_result}"


class TestDatabaseExists:
    """Test spark.catalog.databaseExists() - differential tests"""

    def test_nonexistent_database(self, spark_reference, spark_thunderduck):
        """Non-existent database should return False on both systems."""
        ref_result = spark_reference.catalog.databaseExists("nonexistent_db_xyz_12345")
        td_result = spark_thunderduck.catalog.databaseExists("nonexistent_db_xyz_12345")
        assert ref_result == td_result == False, f"Mismatch: Spark={ref_result}, TD={td_result}"


class TestDropTempView:
    """Test spark.catalog.dropTempView() - differential tests"""

    def test_drop_nonexistent_temp_view(self, spark_reference, spark_thunderduck):
        """Dropping non-existent temp view should return False on both systems."""
        ref_result = spark_reference.catalog.dropTempView("nonexistent_temp_view_xyz_12345")
        td_result = spark_thunderduck.catalog.dropTempView("nonexistent_temp_view_xyz_12345")
        assert ref_result == td_result == False, f"Mismatch: Spark={ref_result}, TD={td_result}"

    def test_drop_existing_temp_view(self, spark_reference, spark_thunderduck):
        """Dropping existing temp view should return True on both systems."""
        # Create temp views on both
        ref_df = spark_reference.range(10)
        ref_df.createOrReplaceTempView("temp_view_to_drop_test")

        td_df = spark_thunderduck.range(10)
        td_df.createOrReplaceTempView("temp_view_to_drop_test")

        # Drop on both
        ref_result = spark_reference.catalog.dropTempView("temp_view_to_drop_test")
        td_result = spark_thunderduck.catalog.dropTempView("temp_view_to_drop_test")

        assert ref_result == td_result == True, f"Mismatch: Spark={ref_result}, TD={td_result}"


class TestCurrentDatabase:
    """Test spark.catalog.currentDatabase() - differential tests"""

    def test_current_database_returns_string(self, spark_reference, spark_thunderduck):
        """currentDatabase should return a non-empty string on both systems."""
        ref_result = spark_reference.catalog.currentDatabase()
        td_result = spark_thunderduck.catalog.currentDatabase()

        assert isinstance(ref_result, str), f"Spark currentDatabase should return string, got {type(ref_result)}"
        assert isinstance(td_result, str), f"TD currentDatabase should return string, got {type(td_result)}"
        assert len(ref_result) > 0, "Spark currentDatabase should not be empty"
        assert len(td_result) > 0, "TD currentDatabase should not be empty"


class TestFunctionExists:
    """Test spark.catalog.functionExists() - differential tests"""

    def test_function_exists_true(self, spark_reference, spark_thunderduck):
        """functionExists should return True for 'abs' on both systems."""
        ref_result = spark_reference.catalog.functionExists("abs")
        td_result = spark_thunderduck.catalog.functionExists("abs")
        assert ref_result == td_result == True, f"Mismatch for 'abs': Spark={ref_result}, TD={td_result}"

    def test_function_exists_false(self, spark_reference, spark_thunderduck):
        """functionExists should return False for non-existent function on both systems."""
        ref_result = spark_reference.catalog.functionExists("nonexistent_function_xyz_12345")
        td_result = spark_thunderduck.catalog.functionExists("nonexistent_function_xyz_12345")
        assert ref_result == td_result == False, f"Mismatch: Spark={ref_result}, TD={td_result}"

    def test_function_exists_common_functions(self, spark_reference, spark_thunderduck):
        """Common SQL functions should exist on both systems."""
        common_funcs = ['sum', 'count', 'max', 'min', 'avg', 'concat']
        for func in common_funcs:
            ref_result = spark_reference.catalog.functionExists(func)
            td_result = spark_thunderduck.catalog.functionExists(func)
            assert ref_result == td_result == True, f"Function '{func}' mismatch: Spark={ref_result}, TD={td_result}"


class TestListFunctions:
    """Test spark.catalog.listFunctions() - differential tests"""

    def test_list_functions_includes_common_functions(self, spark_reference, spark_thunderduck):
        """Both systems should include common SQL functions."""
        ref_functions = spark_reference.catalog.listFunctions()
        ref_names = [f.name.lower() for f in ref_functions]

        td_functions = spark_thunderduck.catalog.listFunctions()
        td_names = [f.name.lower() for f in td_functions]

        # Check common functions exist in both
        common_funcs = ['abs', 'sum', 'count', 'max', 'min', 'avg', 'concat', 'length']
        for func in common_funcs:
            assert func in ref_names, f"Spark should include '{func}' function"
            assert func in td_names, f"Thunderduck should include '{func}' function"


class TestGetFunction:
    """Test spark.catalog.getFunction() - differential tests"""

    def test_get_function_exists(self, spark_reference, spark_thunderduck):
        """getFunction should return metadata for 'abs' on both systems."""
        ref_func = spark_reference.catalog.getFunction("abs")
        td_func = spark_thunderduck.catalog.getFunction("abs")

        assert ref_func is not None, "Spark getFunction should return function object"
        assert td_func is not None, "TD getFunction should return function object"
        assert ref_func.name == "abs", "Spark function name should be 'abs'"
        assert td_func.name == "abs", "TD function name should be 'abs'"


class TestTempViewDifferential:
    """Test temp view operations differentially"""

    def test_temp_view_appears_in_table_exists(self, spark_reference, spark_thunderduck):
        """Temp view created should be found by tableExists on both systems."""
        # Create temp views on both
        ref_df = spark_reference.range(5)
        ref_df.createOrReplaceTempView("diff_temp_view_test")

        td_df = spark_thunderduck.range(5)
        td_df.createOrReplaceTempView("diff_temp_view_test")

        # Check existence on both
        ref_exists = spark_reference.catalog.tableExists("diff_temp_view_test")
        td_exists = spark_thunderduck.catalog.tableExists("diff_temp_view_test")

        assert ref_exists == td_exists == True, f"Temp view exists mismatch: Spark={ref_exists}, TD={td_exists}"

        # Cleanup
        spark_reference.catalog.dropTempView("diff_temp_view_test")
        spark_thunderduck.catalog.dropTempView("diff_temp_view_test")

    def test_temp_view_not_found_after_drop(self, spark_reference, spark_thunderduck):
        """After dropping, temp view should not be found on both systems."""
        # Create and drop on both
        spark_reference.range(3).createOrReplaceTempView("temp_view_lifecycle_test")
        spark_thunderduck.range(3).createOrReplaceTempView("temp_view_lifecycle_test")

        spark_reference.catalog.dropTempView("temp_view_lifecycle_test")
        spark_thunderduck.catalog.dropTempView("temp_view_lifecycle_test")

        # Both should return False
        ref_exists = spark_reference.catalog.tableExists("temp_view_lifecycle_test")
        td_exists = spark_thunderduck.catalog.tableExists("temp_view_lifecycle_test")

        assert ref_exists == td_exists == False, f"After drop mismatch: Spark={ref_exists}, TD={td_exists}"


class TestCurrentCatalog:
    """Test spark.catalog.currentCatalog() - differential tests"""

    def test_current_catalog_returns_string(self, spark_reference, spark_thunderduck):
        """currentCatalog should return a non-empty string on both systems."""
        ref_result = spark_reference.catalog.currentCatalog()
        td_result = spark_thunderduck.catalog.currentCatalog()

        assert isinstance(ref_result, str), f"Spark currentCatalog should return string, got {type(ref_result)}"
        assert isinstance(td_result, str), f"TD currentCatalog should return string, got {type(td_result)}"
        assert len(ref_result) > 0, "Spark currentCatalog should not be empty"
        assert len(td_result) > 0, "TD currentCatalog should not be empty"
