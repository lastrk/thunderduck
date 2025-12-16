"""
Multi-dimensional Aggregation Differential Tests

Tests DataFrame API multi-dimensional aggregation functions for parity
between Apache Spark 4.0.1 and Thunderduck.

Categories tested:
- pivot() - Rotate rows to columns with aggregation
- unpivot() - Rotate columns to rows (Spark 3.4+)
- cube() - All combinations of grouping columns
- rollup() - Hierarchical aggregations with subtotals
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType
)

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "utils"))
from dataframe_diff import assert_dataframes_equal


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture(scope="class")
def sales_data(spark_reference, spark_thunderduck):
    """Create sales test data for pivot/aggregation tests"""
    data = [
        ("US", "Electronics", 2023, 1000),
        ("US", "Electronics", 2024, 1200),
        ("US", "Clothing", 2023, 500),
        ("US", "Clothing", 2024, 600),
        ("UK", "Electronics", 2023, 800),
        ("UK", "Electronics", 2024, 900),
        ("UK", "Clothing", 2023, 400),
        ("UK", "Clothing", 2024, 450),
        ("DE", "Electronics", 2023, 700),
        ("DE", "Electronics", 2024, 850),
        ("DE", "Clothing", 2023, 350),
        ("DE", "Clothing", 2024, 400),
    ]
    schema = StructType([
        StructField("country", StringType(), False),
        StructField("category", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("sales", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def employee_data(spark_reference, spark_thunderduck):
    """Create employee test data for cube/rollup tests"""
    data = [
        ("Engineering", "Senior", 120000),
        ("Engineering", "Senior", 130000),
        ("Engineering", "Junior", 80000),
        ("Engineering", "Junior", 75000),
        ("Sales", "Senior", 100000),
        ("Sales", "Senior", 110000),
        ("Sales", "Junior", 60000),
        ("Sales", "Junior", 65000),
        ("Marketing", "Senior", 95000),
        ("Marketing", "Junior", 55000),
    ]
    schema = StructType([
        StructField("department", StringType(), False),
        StructField("level", StringType(), False),
        StructField("salary", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


@pytest.fixture(scope="class")
def quarterly_data(spark_reference, spark_thunderduck):
    """Create quarterly sales data for unpivot tests"""
    data = [
        ("Product A", 100, 150, 200, 180),
        ("Product B", 80, 90, 110, 100),
        ("Product C", 200, 180, 220, 250),
    ]
    schema = StructType([
        StructField("product", StringType(), False),
        StructField("Q1", IntegerType(), False),
        StructField("Q2", IntegerType(), False),
        StructField("Q3", IntegerType(), False),
        StructField("Q4", IntegerType(), False),
    ])

    df_ref = spark_reference.createDataFrame(data, schema)
    df_td = spark_thunderduck.createDataFrame(data, schema)

    return df_ref, df_td


# ============================================================================
# Pivot Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.aggregations
class TestPivotFunctions:
    """Tests for pivot() aggregation"""

    def test_pivot_simple(self, spark_reference, spark_thunderduck, sales_data):
        """Test simple pivot with sum aggregation"""
        df_ref, df_td = sales_data

        # Pivot: countries as rows, years as columns, sum of sales
        result_ref = (df_ref
            .groupBy("country")
            .pivot("year")
            .agg(F.sum("sales"))
            .orderBy("country"))

        result_td = (df_td
            .groupBy("country")
            .pivot("year")
            .agg(F.sum("sales"))
            .orderBy("country"))

        assert_dataframes_equal(result_ref, result_td, "pivot_simple")

    def test_pivot_with_values(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot with explicit values list"""
        df_ref, df_td = sales_data

        # Pivot with explicit column values
        result_ref = (df_ref
            .groupBy("country")
            .pivot("year", [2023, 2024])
            .agg(F.sum("sales"))
            .orderBy("country"))

        result_td = (df_td
            .groupBy("country")
            .pivot("year", [2023, 2024])
            .agg(F.sum("sales"))
            .orderBy("country"))

        assert_dataframes_equal(result_ref, result_td, "pivot_with_values")

    def test_pivot_multiple_agg(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot with multiple aggregations"""
        df_ref, df_td = sales_data

        # Pivot with sum and count (using count on column, not "*" which is invalid in pivot)
        result_ref = (df_ref
            .groupBy("country")
            .pivot("year")
            .agg(F.sum("sales").alias("total"), F.count("sales").alias("count"))
            .orderBy("country"))

        result_td = (df_td
            .groupBy("country")
            .pivot("year")
            .agg(F.sum("sales").alias("total"), F.count("sales").alias("count"))
            .orderBy("country"))

        assert_dataframes_equal(result_ref, result_td, "pivot_multiple_agg")

    def test_pivot_avg(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot with average aggregation"""
        df_ref, df_td = sales_data

        result_ref = (df_ref
            .groupBy("category")
            .pivot("country")
            .agg(F.avg("sales"))
            .orderBy("category"))

        result_td = (df_td
            .groupBy("category")
            .pivot("country")
            .agg(F.avg("sales"))
            .orderBy("category"))

        assert_dataframes_equal(result_ref, result_td, "pivot_avg", epsilon=1e-6)

    def test_pivot_max_min(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot with max and min aggregations"""
        df_ref, df_td = sales_data

        result_ref = (df_ref
            .groupBy("category")
            .pivot("year")
            .agg(F.max("sales").alias("max_sales"), F.min("sales").alias("min_sales"))
            .orderBy("category"))

        result_td = (df_td
            .groupBy("category")
            .pivot("year")
            .agg(F.max("sales").alias("max_sales"), F.min("sales").alias("min_sales"))
            .orderBy("category"))

        assert_dataframes_equal(result_ref, result_td, "pivot_max_min")

    def test_pivot_multiple_groupby(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot with multiple groupBy columns"""
        df_ref, df_td = sales_data

        result_ref = (df_ref
            .groupBy("country", "category")
            .pivot("year")
            .agg(F.sum("sales"))
            .orderBy("country", "category"))

        result_td = (df_td
            .groupBy("country", "category")
            .pivot("year")
            .agg(F.sum("sales"))
            .orderBy("country", "category"))

        assert_dataframes_equal(result_ref, result_td, "pivot_multiple_groupby")


# ============================================================================
# Unpivot Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.aggregations
class TestUnpivotFunctions:
    """Tests for unpivot() - available in Spark 3.4+"""

    def test_unpivot_basic(self, spark_reference, spark_thunderduck, quarterly_data):
        """Test basic unpivot operation"""
        df_ref, df_td = quarterly_data

        # Unpivot quarterly columns into rows
        result_ref = (df_ref
            .unpivot(
                ids=["product"],
                values=["Q1", "Q2", "Q3", "Q4"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        result_td = (df_td
            .unpivot(
                ids=["product"],
                values=["Q1", "Q2", "Q3", "Q4"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        assert_dataframes_equal(result_ref, result_td, "unpivot_basic")

    def test_unpivot_subset_columns(self, spark_reference, spark_thunderduck, quarterly_data):
        """Test unpivot with subset of columns"""
        df_ref, df_td = quarterly_data

        # Only unpivot Q1 and Q2
        result_ref = (df_ref
            .unpivot(
                ids=["product"],
                values=["Q1", "Q2"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        result_td = (df_td
            .unpivot(
                ids=["product"],
                values=["Q1", "Q2"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        assert_dataframes_equal(result_ref, result_td, "unpivot_subset")

    def test_melt_alias(self, spark_reference, spark_thunderduck, quarterly_data):
        """Test melt() as alias for unpivot()"""
        df_ref, df_td = quarterly_data

        # melt is an alias for unpivot
        result_ref = (df_ref
            .melt(
                ids=["product"],
                values=["Q1", "Q2", "Q3", "Q4"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        result_td = (df_td
            .melt(
                ids=["product"],
                values=["Q1", "Q2", "Q3", "Q4"],
                variableColumnName="quarter",
                valueColumnName="sales"
            )
            .orderBy("product", "quarter"))

        assert_dataframes_equal(result_ref, result_td, "melt_alias")


# ============================================================================
# Cube Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.aggregations
class TestCubeFunctions:
    """Tests for cube() - all combinations of grouping columns"""

    def test_cube_single_column(self, spark_reference, spark_thunderduck, employee_data):
        """Test cube with single grouping column"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .cube("department")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        result_td = (df_td
            .cube("department")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        assert_dataframes_equal(result_ref, result_td, "cube_single")

    def test_cube_two_columns(self, spark_reference, spark_thunderduck, employee_data):
        """Test cube with two grouping columns"""
        df_ref, df_td = employee_data

        # Cube generates all combinations: (dept, level), (dept, null), (null, level), (null, null)
        result_ref = (df_ref
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.avg("salary").alias("avg_salary")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.avg("salary").alias("avg_salary")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "cube_two_columns", epsilon=1e-6)

    def test_cube_with_grouping(self, spark_reference, spark_thunderduck, employee_data):
        """Test cube with grouping() function to identify subtotals"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping("department").alias("dept_grouping"),
                F.grouping("level").alias("level_grouping")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping("department").alias("dept_grouping"),
                F.grouping("level").alias("level_grouping")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "cube_with_grouping")

    def test_cube_with_grouping_id(self, spark_reference, spark_thunderduck, employee_data):
        """Test cube with grouping_id() function"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping_id("department", "level").alias("grouping_id")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .cube("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping_id("department", "level").alias("grouping_id")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "cube_with_grouping_id")


# ============================================================================
# Rollup Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.aggregations
class TestRollupFunctions:
    """Tests for rollup() - hierarchical aggregations with subtotals"""

    def test_rollup_single_column(self, spark_reference, spark_thunderduck, employee_data):
        """Test rollup with single grouping column"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .rollup("department")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        result_td = (df_td
            .rollup("department")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        assert_dataframes_equal(result_ref, result_td, "rollup_single")

    def test_rollup_two_columns(self, spark_reference, spark_thunderduck, employee_data):
        """Test rollup with two grouping columns (hierarchical)"""
        df_ref, df_td = employee_data

        # Rollup generates: (dept, level), (dept, null), (null, null)
        # Unlike cube, it doesn't generate (null, level)
        result_ref = (df_ref
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.avg("salary").alias("avg_salary")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.avg("salary").alias("avg_salary")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "rollup_two_columns", epsilon=1e-6)

    def test_rollup_with_grouping(self, spark_reference, spark_thunderduck, employee_data):
        """Test rollup with grouping() function"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping("department").alias("dept_grouping"),
                F.grouping("level").alias("level_grouping")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.grouping("department").alias("dept_grouping"),
                F.grouping("level").alias("level_grouping")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "rollup_with_grouping")

    def test_rollup_three_columns(self, spark_reference, spark_thunderduck, sales_data):
        """Test rollup with three grouping columns"""
        df_ref, df_td = sales_data

        # Three level hierarchy: country -> category -> year
        result_ref = (df_ref
            .rollup("country", "category", "year")
            .agg(F.sum("sales").alias("total_sales"))
            .orderBy(
                F.col("country").asc_nulls_first(),
                F.col("category").asc_nulls_first(),
                F.col("year").asc_nulls_first()
            ))

        result_td = (df_td
            .rollup("country", "category", "year")
            .agg(F.sum("sales").alias("total_sales"))
            .orderBy(
                F.col("country").asc_nulls_first(),
                F.col("category").asc_nulls_first(),
                F.col("year").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "rollup_three_columns")

    def test_rollup_with_filter(self, spark_reference, spark_thunderduck, employee_data):
        """Test rollup combined with filter"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .filter(F.col("salary") >= 70000)
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        result_td = (df_td
            .filter(F.col("salary") >= 70000)
            .rollup("department", "level")
            .agg(
                F.sum("salary").alias("total_salary"),
                F.count("*").alias("count")
            )
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(result_ref, result_td, "rollup_with_filter")


# ============================================================================
# Combined/Advanced Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.functions
@pytest.mark.aggregations
class TestAdvancedAggregations:
    """Advanced aggregation tests combining multiple features"""

    def test_cube_vs_rollup_difference(self, spark_reference, spark_thunderduck, employee_data):
        """Verify cube produces more rows than rollup (all combinations vs hierarchy)"""
        df_ref, df_td = employee_data

        # Cube result
        cube_ref = (df_ref
            .cube("department", "level")
            .agg(F.count("*").alias("count"))
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        cube_td = (df_td
            .cube("department", "level")
            .agg(F.count("*").alias("count"))
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(cube_ref, cube_td, "cube_vs_rollup_cube")

        # Rollup result
        rollup_ref = (df_ref
            .rollup("department", "level")
            .agg(F.count("*").alias("count"))
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        rollup_td = (df_td
            .rollup("department", "level")
            .agg(F.count("*").alias("count"))
            .orderBy(
                F.col("department").asc_nulls_first(),
                F.col("level").asc_nulls_first()
            ))

        assert_dataframes_equal(rollup_ref, rollup_td, "cube_vs_rollup_rollup")

    def test_pivot_then_aggregate(self, spark_reference, spark_thunderduck, sales_data):
        """Test pivot followed by additional aggregation"""
        df_ref, df_td = sales_data

        # Pivot by year, then calculate row totals
        result_ref = (df_ref
            .groupBy("country")
            .pivot("year", [2023, 2024])
            .agg(F.sum("sales"))
            .withColumn("total", F.col("2023") + F.col("2024"))
            .orderBy("country"))

        result_td = (df_td
            .groupBy("country")
            .pivot("year", [2023, 2024])
            .agg(F.sum("sales"))
            .withColumn("total", F.col("2023") + F.col("2024"))
            .orderBy("country"))

        assert_dataframes_equal(result_ref, result_td, "pivot_then_aggregate")

    def test_multiple_aggregations_same_column(self, spark_reference, spark_thunderduck, employee_data):
        """Test multiple different aggregations on same column in cube"""
        df_ref, df_td = employee_data

        result_ref = (df_ref
            .cube("department")
            .agg(
                F.sum("salary").alias("sum_salary"),
                F.avg("salary").alias("avg_salary"),
                F.min("salary").alias("min_salary"),
                F.max("salary").alias("max_salary"),
                F.count("salary").alias("count_salary"),
                F.stddev("salary").alias("stddev_salary")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        result_td = (df_td
            .cube("department")
            .agg(
                F.sum("salary").alias("sum_salary"),
                F.avg("salary").alias("avg_salary"),
                F.min("salary").alias("min_salary"),
                F.max("salary").alias("max_salary"),
                F.count("salary").alias("count_salary"),
                F.stddev("salary").alias("stddev_salary")
            )
            .orderBy(F.col("department").asc_nulls_first()))

        assert_dataframes_equal(result_ref, result_td, "multiple_agg_same_column", epsilon=1e-6)
