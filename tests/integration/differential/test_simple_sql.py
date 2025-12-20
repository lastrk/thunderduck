"""
Simple SQL Differential Tests

Tests basic SQL connectivity comparing Thunderduck against Apache Spark 4.0.1.
"""

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


class TestSimpleSQL:
    """Basic SQL differential tests to ensure servers work identically"""

    @pytest.mark.timeout(30)
    def test_select_1(self, spark_reference, spark_thunderduck):
        """Test simple SELECT 1"""
        ref_df = spark_reference.sql("SELECT 1 AS col")
        td_df = spark_thunderduck.sql("SELECT 1 AS col")
        assert_dataframes_equal(ref_df, td_df, "test_select_1")

    @pytest.mark.timeout(30)
    def test_select_multiple_columns(self, spark_reference, spark_thunderduck):
        """Test SELECT with multiple columns"""
        ref_df = spark_reference.sql("SELECT 42 AS answer, 'hello' AS greeting")
        td_df = spark_thunderduck.sql("SELECT 42 AS answer, 'hello' AS greeting")
        assert_dataframes_equal(ref_df, td_df, "test_select_multiple_columns")

    @pytest.mark.timeout(30)
    def test_select_from_values(self, spark_reference, spark_thunderduck):
        """Test SELECT from VALUES"""
        query = "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"
        ref_df = spark_reference.sql(query)
        td_df = spark_thunderduck.sql(query)
        assert_dataframes_equal(ref_df, td_df, "test_select_from_values")
