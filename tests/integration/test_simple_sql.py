"""Simple SQL test to verify server connectivity"""

import pytest


class TestSimpleSQL:
    """Basic SQL tests to ensure server is working"""

    @pytest.mark.timeout(30)
    def test_select_1(self, spark):
        """Test simple SELECT 1"""
        result = spark.sql("SELECT 1 AS col")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['col'] == 1
        print("\n✓ SELECT 1 works")

    @pytest.mark.timeout(30)
    def test_select_multiple_columns(self, spark):
        """Test SELECT with multiple columns"""
        result = spark.sql("SELECT 42 AS answer, 'hello' AS greeting")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]['answer'] == 42
        assert rows[0]['greeting'] == 'hello'
        print("\n✓ SELECT with multiple columns works")

    @pytest.mark.timeout(30)
    def test_select_from_values(self, spark):
        """Test SELECT from VALUES"""
        result = spark.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
        rows = result.collect()
        assert len(rows) == 3
        print(f"\n✓ SELECT from VALUES works: {len(rows)} rows")
