"""
Corrected differential test - using proper Spark SQL syntax.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


class TestInsertDifferentialCorrected:
    """Test with correct SQL syntax for both Spark and DuckDB."""

    def test_insert_with_correct_syntax(self, spark_reference, spark_thunderduck):
        """INSERT with VARCHAR(n) syntax - correct for Spark."""
        # Setup with proper VARCHAR size
        spark_reference.sql("CREATE TABLE test_insert_corrected (id INT, name VARCHAR(50))")
        spark_thunderduck.sql("CREATE TABLE test_insert_corrected (id INT, name VARCHAR(50))")

        # Execute INSERT
        spark_reference.sql("INSERT INTO test_insert_corrected VALUES (1, 'Alice')")
        spark_thunderduck.sql("INSERT INTO test_insert_corrected VALUES (1, 'Alice')")

        # Verify exact parity
        spark_result = spark_reference.sql("SELECT * FROM test_insert_corrected ORDER BY id")
        td_result = spark_thunderduck.sql("SELECT * FROM test_insert_corrected ORDER BY id")

        assert_dataframes_equal(spark_result, td_result, query_name="INSERT with correct syntax")

        # Cleanup
        spark_reference.sql("DROP TABLE test_insert_corrected")
        spark_thunderduck.sql("DROP TABLE test_insert_corrected")

    def test_insert_using_string_type(self, spark_reference, spark_thunderduck):
        """INSERT using STRING type (Spark's variable-length string)."""
        # Setup with STRING type (Spark's alternative to VARCHAR)
        spark_reference.sql("CREATE TABLE test_string (id INT, name STRING)")
        spark_thunderduck.sql("CREATE TABLE test_string (id INT, name STRING)")

        # Execute INSERT
        spark_reference.sql("INSERT INTO test_string VALUES (1, 'Alice'), (2, 'Bob')")
        spark_thunderduck.sql("INSERT INTO test_string VALUES (1, 'Alice'), (2, 'Bob')")

        # Verify exact parity
        spark_result = spark_reference.sql("SELECT * FROM test_string ORDER BY id")
        td_result = spark_thunderduck.sql("SELECT * FROM test_string ORDER BY id")

        assert_dataframes_equal(spark_result, td_result, query_name="INSERT with STRING type")

        # Cleanup
        spark_reference.sql("DROP TABLE test_string")
        spark_thunderduck.sql("DROP TABLE test_string")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
