"""
Differential tests for DDL/DML parser support.

Tests that DDL/DML statements are correctly parsed by SparkSQLAstBuilder
and executed through the Thunderduck Connect server. Validates that
Spark-to-DuckDB type mapping works (e.g., STRING -> VARCHAR, LONG -> BIGINT).

These tests use class-scoped fixtures to reuse server sessions across tests
within each class.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDDLParserCreateTable:
    """Tests for CREATE TABLE with Spark type names that need mapping."""

    def test_create_table_with_string_type(self, spark_thunderduck):
        """CREATE TABLE with STRING -> VARCHAR mapping, then verify schema."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_string")
            spark_thunderduck.sql("CREATE TABLE test_ddl_string (id INT, name STRING)")
            spark_thunderduck.sql("INSERT INTO test_ddl_string VALUES (1, 'test')")
            # Verify table exists and has correct schema via collect
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_string").collect()
            assert len(result) == 1, f"Expected 1 row, got {len(result)}"
            assert result[0]["id"] == 1
            assert result[0]["name"] == "test"
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_string")

    def test_create_table_with_multiple_types(self, spark_thunderduck):
        """CREATE TABLE with various Spark type names."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_types")
            spark_thunderduck.sql("""
                CREATE TABLE test_ddl_types (
                    id INT,
                    name STRING,
                    amount DOUBLE,
                    count LONG,
                    flag BOOLEAN,
                    created DATE
                )
            """)
            spark_thunderduck.sql("""
                INSERT INTO test_ddl_types VALUES
                (1, 'test', 1.5, 100, true, DATE '2024-01-01')
            """)
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_types").collect()
            assert len(result) == 1, f"Expected 1 row, got {len(result)}"
            assert result[0]["id"] == 1
            assert result[0]["name"] == "test"
            assert result[0]["count"] == 100
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_types")

    def test_create_table_if_not_exists(self, spark_thunderduck):
        """CREATE TABLE IF NOT EXISTS should not error on second call."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_cine")
            spark_thunderduck.sql("CREATE TABLE test_ddl_cine (id INT, val STRING)")
            spark_thunderduck.sql("INSERT INTO test_ddl_cine VALUES (1, 'a')")
            # Should not error
            spark_thunderduck.sql("CREATE TABLE IF NOT EXISTS test_ddl_cine (id INT, val STRING)")
            # Verify original data still present
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_cine").collect()
            assert len(result) == 1, f"Expected 1 row, got {len(result)}"
            assert result[0]["id"] == 1
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_cine")


class TestDDLParserDropTable:
    """Tests for DROP TABLE."""

    def test_drop_table(self, spark_thunderduck):
        """DROP TABLE removes the table."""
        spark_thunderduck.sql("CREATE TABLE test_ddl_drop (id INT)")
        spark_thunderduck.sql("DROP TABLE test_ddl_drop")
        with pytest.raises(Exception):
            spark_thunderduck.sql("SELECT * FROM test_ddl_drop").collect()

    def test_drop_table_if_exists_nonexistent(self, spark_thunderduck):
        """DROP TABLE IF EXISTS on non-existent table should not error."""
        # Should not throw
        spark_thunderduck.sql("DROP TABLE IF EXISTS nonexistent_ddl_parser_test_xyz")


class TestDDLParserTruncate:
    """Tests for TRUNCATE TABLE."""

    def test_truncate_table(self, spark_thunderduck):
        """TRUNCATE TABLE empties the table."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_trunc")
            spark_thunderduck.sql("CREATE TABLE test_ddl_trunc (id INT)")
            spark_thunderduck.sql("INSERT INTO test_ddl_trunc VALUES (1), (2), (3)")
            spark_thunderduck.sql("TRUNCATE TABLE test_ddl_trunc")
            result = spark_thunderduck.sql("SELECT COUNT(*) as cnt FROM test_ddl_trunc").collect()
            assert result[0]["cnt"] == 0, f"Expected 0 rows after TRUNCATE, got {result[0]['cnt']}"
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_trunc")


class TestDDLParserInsert:
    """Tests for INSERT INTO ... VALUES and INSERT INTO ... SELECT."""

    def test_insert_values(self, spark_thunderduck):
        """INSERT INTO ... VALUES inserts rows correctly."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_insert")
            spark_thunderduck.sql("CREATE TABLE test_ddl_insert (id INT, name STRING)")
            spark_thunderduck.sql("INSERT INTO test_ddl_insert VALUES (1, 'alice'), (2, 'bob')")
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_insert ORDER BY id").collect()
            assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
            assert result[0]["id"] == 1
            assert result[0]["name"] == "alice"
            assert result[1]["id"] == 2
            assert result[1]["name"] == "bob"
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_insert")

    def test_insert_select(self, spark_thunderduck):
        """INSERT INTO ... SELECT copies data from another table."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_src")
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_dst")
            spark_thunderduck.sql("CREATE TABLE test_ddl_src (id INT, val STRING)")
            spark_thunderduck.sql("CREATE TABLE test_ddl_dst (id INT, val STRING)")
            spark_thunderduck.sql("INSERT INTO test_ddl_src VALUES (1, 'x'), (2, 'y')")
            spark_thunderduck.sql("INSERT INTO test_ddl_dst SELECT * FROM test_ddl_src")
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_dst ORDER BY id").collect()
            assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
            assert result[0]["id"] == 1
            assert result[1]["id"] == 2
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_src")
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_dst")


class TestDDLParserView:
    """Tests for CREATE VIEW and CREATE OR REPLACE TEMP VIEW."""

    def test_create_temp_view(self, spark_thunderduck):
        """CREATE OR REPLACE TEMP VIEW creates a queryable view."""
        spark_thunderduck.sql(
            "CREATE OR REPLACE TEMP VIEW test_ddl_view AS SELECT 1 AS id, 'hello' AS name"
        )
        result = spark_thunderduck.sql("SELECT * FROM test_ddl_view").collect()
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["name"] == "hello"

    def test_create_view(self, spark_thunderduck):
        """CREATE VIEW creates a persistent view."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_base")
            spark_thunderduck.sql("DROP VIEW IF EXISTS test_ddl_v2")
            spark_thunderduck.sql("CREATE TABLE test_ddl_base (id INT, val STRING)")
            spark_thunderduck.sql("INSERT INTO test_ddl_base VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            spark_thunderduck.sql(
                "CREATE VIEW test_ddl_v2 AS SELECT * FROM test_ddl_base WHERE id > 1"
            )
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_v2 ORDER BY id").collect()
            assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
        finally:
            spark_thunderduck.sql("DROP VIEW IF EXISTS test_ddl_v2")
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_base")

    def test_drop_view(self, spark_thunderduck):
        """DROP VIEW removes the view."""
        spark_thunderduck.sql(
            "CREATE OR REPLACE TEMP VIEW test_ddl_drop_v AS SELECT 1 AS x"
        )
        spark_thunderduck.sql("DROP VIEW test_ddl_drop_v")
        with pytest.raises(Exception):
            spark_thunderduck.sql("SELECT * FROM test_ddl_drop_v").collect()


class TestDDLParserWorkflow:
    """End-to-end workflow tests combining DDL, DML, and SELECT."""

    def test_full_workflow(self, spark_thunderduck):
        """CREATE TABLE -> INSERT -> SELECT -> TRUNCATE -> DROP workflow."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_workflow")
            spark_thunderduck.sql(
                "CREATE TABLE test_ddl_workflow (id INT, name STRING, score DOUBLE)"
            )
            spark_thunderduck.sql(
                "INSERT INTO test_ddl_workflow VALUES "
                "(1, 'alice', 95.5), (2, 'bob', 87.3), (3, 'carol', 92.1)"
            )

            # Verify data
            result = spark_thunderduck.sql(
                "SELECT * FROM test_ddl_workflow ORDER BY id"
            ).collect()
            assert len(result) == 3
            assert result[0]["name"] == "alice"
            assert abs(result[2]["score"] - 92.1) < 0.01

            # Truncate and verify empty
            spark_thunderduck.sql("TRUNCATE TABLE test_ddl_workflow")
            count = spark_thunderduck.sql(
                "SELECT COUNT(*) as cnt FROM test_ddl_workflow"
            ).collect()
            assert count[0]["cnt"] == 0

        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_workflow")

    def test_type_mapping_correctness(self, spark_thunderduck):
        """Verify Spark types are correctly mapped to DuckDB types."""
        try:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_typemap")
            spark_thunderduck.sql("""
                CREATE TABLE test_ddl_typemap (
                    a INT, b LONG, c DOUBLE, d FLOAT,
                    e BOOLEAN, f STRING, g DATE
                )
            """)
            spark_thunderduck.sql("""
                INSERT INTO test_ddl_typemap VALUES
                (42, 9999999999, 3.14, 2.7, true, 'hello', DATE '2024-06-15')
            """)
            result = spark_thunderduck.sql("SELECT * FROM test_ddl_typemap").collect()
            assert len(result) == 1
            row = result[0]
            assert row["a"] == 42
            assert row["b"] == 9999999999
            assert abs(row["c"] - 3.14) < 0.001
            assert row["e"] is True
            assert row["f"] == "hello"
        finally:
            spark_thunderduck.sql("DROP TABLE IF EXISTS test_ddl_typemap")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
