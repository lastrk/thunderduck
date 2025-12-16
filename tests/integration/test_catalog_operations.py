"""
Catalog Operations Tests

Tests for Spark Connect catalog operations (listTables, tableExists, etc.)
comparing Thunderduck against Apache Spark 4.0.1.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@pytest.fixture(scope="module")
def setup_test_tables(spark):
    """Create test tables for catalog operations testing."""
    # Create a simple test table
    spark.sql("CREATE OR REPLACE TABLE test_catalog_table (id INT, name STRING)")
    spark.sql("INSERT INTO test_catalog_table VALUES (1, 'Alice'), (2, 'Bob')")

    # Create a view
    spark.sql("CREATE OR REPLACE VIEW test_catalog_view AS SELECT * FROM test_catalog_table WHERE id > 0")

    yield

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS test_catalog_table")
    spark.sql("DROP VIEW IF EXISTS test_catalog_view")


class TestTableExists:
    """Test spark.catalog.tableExists()"""

    def test_table_exists_true(self, spark, setup_test_tables):
        """Table that exists should return True."""
        result = spark.catalog.tableExists("test_catalog_table")
        assert result is True, "tableExists should return True for existing table"

    def test_table_exists_false(self, spark):
        """Non-existent table should return False."""
        result = spark.catalog.tableExists("nonexistent_table_xyz")
        assert result is False, "tableExists should return False for non-existent table"

    def test_view_exists(self, spark, setup_test_tables):
        """Views should also be found by tableExists."""
        result = spark.catalog.tableExists("test_catalog_view")
        assert result is True, "tableExists should return True for views"


class TestDatabaseExists:
    """Test spark.catalog.databaseExists()"""

    def test_main_database_exists(self, spark):
        """Main/default database should exist."""
        # DuckDB uses 'main' as default schema
        result = spark.catalog.databaseExists("main")
        assert result is True, "main database should exist"

    def test_nonexistent_database(self, spark):
        """Non-existent database should return False."""
        result = spark.catalog.databaseExists("nonexistent_db_xyz")
        assert result is False, "databaseExists should return False for non-existent db"


class TestListTables:
    """Test spark.catalog.listTables()"""

    def test_list_tables_returns_dataframe(self, spark, setup_test_tables):
        """listTables should return a DataFrame-like result."""
        tables = spark.catalog.listTables()
        # Convert to list to iterate
        table_list = list(tables)
        assert len(table_list) >= 2, "Should have at least test table and view"

    def test_list_tables_schema(self, spark, setup_test_tables):
        """listTables result should have expected columns."""
        tables = spark.catalog.listTables()
        table_list = list(tables)

        if len(table_list) > 0:
            first = table_list[0]
            # Check for expected attributes
            assert hasattr(first, 'name'), "Should have 'name' attribute"
            assert hasattr(first, 'tableType'), "Should have 'tableType' attribute"

    def test_list_tables_finds_test_table(self, spark, setup_test_tables):
        """listTables should include our test table."""
        tables = spark.catalog.listTables()
        table_names = [t.name for t in tables]
        assert "test_catalog_table" in table_names, "Should find test_catalog_table"


class TestListDatabases:
    """Test spark.catalog.listDatabases()"""

    def test_list_databases_returns_results(self, spark):
        """listDatabases should return at least one database."""
        databases = spark.catalog.listDatabases()
        db_list = list(databases)
        assert len(db_list) >= 1, "Should have at least one database"

    def test_list_databases_includes_main(self, spark):
        """listDatabases should include 'main' schema."""
        databases = spark.catalog.listDatabases()
        db_names = [d.name for d in databases]
        assert "main" in db_names, "Should include 'main' database"


class TestListColumns:
    """Test spark.catalog.listColumns()"""

    def test_list_columns_for_table(self, spark, setup_test_tables):
        """listColumns should return columns for a table."""
        columns = spark.catalog.listColumns("test_catalog_table")
        col_list = list(columns)
        assert len(col_list) == 2, "test_catalog_table should have 2 columns"

    def test_list_columns_names(self, spark, setup_test_tables):
        """listColumns should return correct column names."""
        columns = spark.catalog.listColumns("test_catalog_table")
        col_names = [c.name for c in columns]
        assert "id" in col_names, "Should have 'id' column"
        assert "name" in col_names, "Should have 'name' column"

    def test_list_columns_for_view(self, spark, setup_test_tables):
        """listColumns should work for views too."""
        columns = spark.catalog.listColumns("test_catalog_view")
        col_list = list(columns)
        assert len(col_list) == 2, "test_catalog_view should have 2 columns"


class TestDropTempView:
    """Test spark.catalog.dropTempView()"""

    def test_drop_existing_temp_view(self, spark):
        """Dropping existing temp view should return True."""
        # Create a temp view
        df = spark.range(10)
        df.createOrReplaceTempView("temp_view_to_drop")

        # Drop it
        result = spark.catalog.dropTempView("temp_view_to_drop")
        assert result is True, "dropTempView should return True for existing view"

    def test_drop_nonexistent_temp_view(self, spark):
        """Dropping non-existent temp view should return False."""
        result = spark.catalog.dropTempView("nonexistent_temp_view_xyz")
        assert result is False, "dropTempView should return False for non-existent view"

    def test_view_no_longer_exists_after_drop(self, spark):
        """After dropping, the view should no longer exist."""
        # Create and drop
        df = spark.range(5)
        df.createOrReplaceTempView("temp_view_lifecycle")
        spark.catalog.dropTempView("temp_view_lifecycle")

        # Should not exist anymore
        result = spark.catalog.tableExists("temp_view_lifecycle")
        assert result is False, "View should not exist after being dropped"


class TestCurrentDatabase:
    """Test spark.catalog.currentDatabase()"""

    def test_current_database_returns_string(self, spark):
        """currentDatabase should return a string."""
        result = spark.catalog.currentDatabase()
        assert isinstance(result, str), "currentDatabase should return a string"
        assert len(result) > 0, "currentDatabase should not be empty"


# Differential test class for comparing Spark vs Thunderduck
class TestCatalogDifferential:
    """Differential tests comparing catalog operations between Spark and Thunderduck."""

    @pytest.mark.differential
    def test_table_exists_differential(self, spark_session, thunderduck_session):
        """tableExists should match between Spark and Thunderduck."""
        # Both should return same result for non-existent table
        spark_result = spark_session.catalog.tableExists("nonexistent_table_abc")
        td_result = thunderduck_session.catalog.tableExists("nonexistent_table_abc")
        assert spark_result == td_result, "tableExists should match for non-existent table"

    @pytest.mark.differential
    def test_database_exists_differential(self, spark_session, thunderduck_session):
        """databaseExists should match between Spark and Thunderduck."""
        # Both should return False for non-existent database
        spark_result = spark_session.catalog.databaseExists("nonexistent_db_abc")
        td_result = thunderduck_session.catalog.databaseExists("nonexistent_db_abc")
        assert spark_result == td_result, "databaseExists should match for non-existent db"
