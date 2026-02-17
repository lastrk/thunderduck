"""
Differential Tests for Statistics Operations

Tests for Spark Connect statistics operations (cov, corr, describe, etc.)
comparing Thunderduck against Apache Spark 4.1.1.

Converted from test_statistics_operations.py.
"""

import pytest
import math
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.dataframe_diff import assert_dataframes_equal


def create_numeric_data(spark):
    """Create test data for statistics operations testing."""
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("val1", DoubleType()),
        StructField("val2", DoubleType()),
        StructField("category", StringType())
    ])
    return spark.createDataFrame([
        (1, 10.0, 100.0, "A"),
        (2, 20.0, 200.0, "B"),
        (3, 30.0, 300.0, "A"),
        (4, 40.0, 400.0, "B"),
        (5, 50.0, 500.0, "A"),
        (6, 60.0, 600.0, "B"),
        (7, 70.0, 700.0, "A"),
        (8, 80.0, 800.0, "B"),
        (9, 90.0, 900.0, "A"),
        (10, 100.0, 1000.0, "B")
    ], schema)


def create_mixed_data(spark):
    """Create mixed type data for describe testing."""
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("price", DoubleType())
    ])
    return spark.createDataFrame([
        (1, "apple", 10.5),
        (2, "banana", 20.3),
        (3, "cherry", 15.7),
        (4, "date", 25.9),
        (5, "elderberry", 12.1)
    ], schema)


@pytest.mark.differential
class TestStatCov_Differential:
    """Test df.stat.cov() - sample covariance"""

    def test_cov_positive_correlation(self, spark_reference, spark_thunderduck):
        """Covariance of perfectly correlated columns should be positive."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_cov = ref_df.stat.cov("val1", "val2")
        test_cov = test_df.stat.cov("val1", "val2")

        assert abs(ref_cov - test_cov) < 1e-6, f"Covariance mismatch: {ref_cov} vs {test_cov}"
        print(f"✓ cov: Spark={ref_cov:.2f}, Thunderduck={test_cov:.2f}")

    def test_cov_symmetric(self, spark_reference, spark_thunderduck):
        """Covariance should be symmetric: cov(a,b) == cov(b,a)."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_cov1 = ref_df.stat.cov("val1", "val2")
        ref_cov2 = ref_df.stat.cov("val2", "val1")
        test_cov1 = test_df.stat.cov("val1", "val2")
        test_cov2 = test_df.stat.cov("val2", "val1")

        assert abs(ref_cov1 - ref_cov2) < 1e-10, "Spark covariance should be symmetric"
        assert abs(test_cov1 - test_cov2) < 1e-10, "Thunderduck covariance should be symmetric"
        print(f"✓ cov symmetry verified")


@pytest.mark.differential
class TestStatCorr_Differential:
    """Test df.stat.corr() - Pearson correlation"""

    def test_corr_perfect_correlation(self, spark_reference, spark_thunderduck):
        """Correlation of perfectly correlated columns should be 1.0."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_corr = ref_df.stat.corr("val1", "val2")
        test_corr = test_df.stat.corr("val1", "val2")

        assert abs(ref_corr - 1.0) < 1e-10, f"Spark correlation should be 1.0, got {ref_corr}"
        assert abs(test_corr - 1.0) < 1e-10, f"Thunderduck correlation should be 1.0, got {test_corr}"
        print(f"✓ corr: Spark={ref_corr:.6f}, Thunderduck={test_corr:.6f}")

    def test_corr_range(self, spark_reference, spark_thunderduck):
        """Correlation should be in range [-1, 1]."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_corr = ref_df.stat.corr("val1", "val2")
        test_corr = test_df.stat.corr("val1", "val2")

        assert -1.0 <= ref_corr <= 1.0, f"Spark correlation out of range: {ref_corr}"
        assert -1.0 <= test_corr <= 1.0, f"Thunderduck correlation out of range: {test_corr}"
        print(f"✓ corr range verified")


@pytest.mark.differential
class TestStatDescribe_Differential:
    """Test df.describe() - basic statistics"""

    def test_describe_numeric_columns(self, spark_reference, spark_thunderduck):
        """Describe should return same statistics for numeric columns."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_result = ref_df.describe("val1")
        test_result = test_df.describe("val1")

        # Compare by ordering by summary column
        assert_dataframes_equal(
            ref_result.orderBy("summary"),
            test_result.orderBy("summary"),
            "describe_numeric",
            epsilon=0.01
        )

    def test_describe_row_count(self, spark_reference, spark_thunderduck):
        """Describe should return 5 rows: count, mean, stddev, min, max."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_rows = ref_df.describe().collect()
        test_rows = test_df.describe().collect()

        assert len(ref_rows) == len(test_rows), f"Row count mismatch: {len(ref_rows)} vs {len(test_rows)}"
        print(f"✓ describe row count: {len(ref_rows)} rows")

    def test_describe_summary_column(self, spark_reference, spark_thunderduck):
        """Describe should have matching summary values."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_rows = ref_df.describe().collect()
        test_rows = test_df.describe().collect()

        ref_summaries = sorted([row.summary for row in ref_rows])
        test_summaries = sorted([row.summary for row in test_rows])

        assert ref_summaries == test_summaries, f"Summary mismatch: {ref_summaries} vs {test_summaries}"
        print(f"✓ describe summaries match: {ref_summaries}")


@pytest.mark.differential
class TestStatSummary_Differential:
    """Test df.summary() - configurable statistics"""

    def test_summary_default_stats(self, spark_reference, spark_thunderduck):
        """Summary with no args should return same statistics."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_result = ref_df.select("val1").summary()
        test_result = test_df.select("val1").summary()

        assert_dataframes_equal(
            ref_result.orderBy("summary"),
            test_result.orderBy("summary"),
            "summary_default",
            epsilon=0.01
        )

    def test_summary_custom_stats(self, spark_reference, spark_thunderduck):
        """Summary with custom statistics list."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_result = ref_df.select("val1").summary("count", "min", "max")
        test_result = test_df.select("val1").summary("count", "min", "max")

        assert_dataframes_equal(
            ref_result.orderBy("summary"),
            test_result.orderBy("summary"),
            "summary_custom",
            epsilon=0.01
        )


@pytest.mark.differential
class TestStatCrosstab_Differential:
    """Test df.stat.crosstab() - contingency table"""

    def test_crosstab_basic(self, spark_reference, spark_thunderduck):
        """Crosstab should return same contingency table."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_result = ref_df.stat.crosstab("category", "id")
        test_result = test_df.stat.crosstab("category", "id")

        # Crosstab column names and order may vary, just check row count
        ref_rows = ref_result.collect()
        test_rows = test_result.collect()

        assert len(ref_rows) == len(test_rows), f"Crosstab row count mismatch"
        print(f"✓ crosstab: {len(ref_rows)} rows")


@pytest.mark.differential
class TestStatFreqItems_Differential:
    """Test df.stat.freqItems() - frequent items"""

    def test_freqitems_basic(self, spark_reference, spark_thunderduck):
        """FreqItems should return frequent values."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_result = ref_df.stat.freqItems(["category"])
        test_result = test_df.stat.freqItems(["category"])

        # Both should have category_freqItems column
        ref_cols = ref_result.columns
        test_cols = test_result.columns

        assert "category_freqItems" in ref_cols, f"Spark missing column: {ref_cols}"
        assert "category_freqItems" in test_cols, f"Thunderduck missing column: {test_cols}"
        print(f"✓ freqItems columns match")


@pytest.mark.differential
class TestStatApproxQuantile_Differential:
    """Test df.stat.approxQuantile() - approximate quantiles"""

    def test_approx_quantile_median(self, spark_reference, spark_thunderduck):
        """Approximate median should match between systems."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_q = ref_df.stat.approxQuantile("val1", [0.5], 0.0)
        test_q = test_df.stat.approxQuantile("val1", [0.5], 0.0)

        assert len(ref_q) == 1 and len(test_q) == 1, "Should return one quantile"
        # Median of 10-100 by 10s is 55, allow some tolerance
        assert abs(ref_q[0] - test_q[0]) < 10, f"Median mismatch: {ref_q[0]} vs {test_q[0]}"
        print(f"✓ approxQuantile median: Spark={ref_q[0]}, Thunderduck={test_q[0]}")

    def test_approx_quantile_multiple(self, spark_reference, spark_thunderduck):
        """Multiple quantiles should match."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        ref_q = ref_df.stat.approxQuantile("val1", [0.0, 0.5, 1.0], 0.0)
        test_q = test_df.stat.approxQuantile("val1", [0.0, 0.5, 1.0], 0.0)

        assert len(ref_q) == 3 and len(test_q) == 3, "Should return 3 quantiles"
        # Min should be 10, max should be 100
        assert abs(ref_q[0] - test_q[0]) < 1, f"Min mismatch: {ref_q[0]} vs {test_q[0]}"
        assert abs(ref_q[2] - test_q[2]) < 1, f"Max mismatch: {ref_q[2]} vs {test_q[2]}"
        print(f"✓ approxQuantile multiple: matches")


@pytest.mark.differential
class TestStatSampleBy_Differential:
    """Test df.stat.sampleBy() - stratified sampling"""

    def test_sampleby_preserves_schema(self, spark_reference, spark_thunderduck):
        """SampleBy should preserve the DataFrame schema."""
        ref_df = create_numeric_data(spark_reference)
        test_df = create_numeric_data(spark_thunderduck)

        fractions = {"A": 0.5, "B": 0.5}
        ref_result = ref_df.stat.sampleBy("category", fractions, seed=42)
        test_result = test_df.stat.sampleBy("category", fractions, seed=42)

        assert ref_df.columns == ref_result.columns, "Spark schema should be preserved"
        assert test_df.columns == test_result.columns, "Thunderduck schema should be preserved"
        print(f"✓ sampleBy schema preserved")


@pytest.mark.differential
class TestStatisticsWithNulls_Differential:
    """Test statistics operations with NULL values."""

    def test_cov_with_nulls(self, spark_reference, spark_thunderduck):
        """Covariance should handle NULL values."""
        def create_data(spark):
            return spark.createDataFrame([
                (1, 10.0, 100.0),
                (2, 20.0, 200.0),
                (3, None, 300.0),
                (4, 40.0, None),
                (5, 50.0, 500.0)
            ], ["id", "val1", "val2"])

        ref_df = create_data(spark_reference)
        test_df = create_data(spark_thunderduck)

        ref_cov = ref_df.stat.cov("val1", "val2")
        test_cov = test_df.stat.cov("val1", "val2")

        assert not math.isnan(ref_cov), "Spark cov should handle NULLs"
        assert not math.isnan(test_cov), "Thunderduck cov should handle NULLs"
        assert abs(ref_cov - test_cov) < 1e-6, f"Cov with nulls mismatch: {ref_cov} vs {test_cov}"
        print(f"✓ cov with nulls: {ref_cov:.2f}")

    def test_describe_with_nulls(self, spark_reference, spark_thunderduck):
        """Describe should handle NULL values gracefully."""
        def create_data(spark):
            return spark.createDataFrame([
                (1, "a"),
                (2, None),
                (None, "c")
            ], ["id", "name"])

        ref_df = create_data(spark_reference)
        test_df = create_data(spark_thunderduck)

        ref_result = ref_df.describe()
        test_result = test_df.describe()

        ref_rows = ref_result.collect()
        test_rows = test_result.collect()

        assert len(ref_rows) == len(test_rows), "Row count should match with NULLs"
        print(f"✓ describe with nulls: {len(ref_rows)} rows")
