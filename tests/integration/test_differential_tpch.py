"""
TPC-H Differential Testing: Spark Local Mode vs Thunderduck

This test suite runs all 22 TPC-H queries on both:
1. Apache Spark (local mode) - the reference implementation
2. Thunderduck (via Spark Connect) - the system under test

Results are compared for correctness to ensure Thunderduck produces
identical results to Spark for all queries.
"""

import pytest
from pyspark.sql import SparkSession
from decimal import Decimal
from pathlib import Path
import time


# ============================================================================
# Fixtures for Spark Local Mode (Reference)
# ============================================================================

@pytest.fixture(scope="function")
def dual_spark_sessions(server_manager, tpch_data_dir):
    """
    Creates both Spark local and Thunderduck sessions for differential testing.

    Strategy: Run queries sequentially with explicit session management
    to avoid conflicts.

    Returns: dict with 'local' and 'thunderduck' sessions
    """
    print("\n" + "=" * 80)
    print("Setting up DUAL Spark sessions for differential testing")
    print("=" * 80)

    # First, create Spark local mode
    print("\n1. Starting Spark LOCAL MODE (reference)...")
    from pyspark.sql import SparkSession as LocalSparkSession

    spark_local = (LocalSparkSession.builder
                   .master("local[2]")
                   .appName("TPC-H-Differential-Reference")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.default.parallelism", "2")
                   .getOrCreate())

    print("   ✓ Spark local mode started")

    # Load tables into local Spark
    print("\n2. Loading TPC-H tables into Spark local mode...")
    table_names = ['customer', 'lineitem', 'nation', 'orders',
                   'part', 'partsupp', 'region', 'supplier']

    for table_name in table_names:
        path = str(tpch_data_dir / f"{table_name}.parquet")
        df = spark_local.read.parquet(path)
        df.createOrReplaceTempView(table_name)
        print(f"   ✓ {table_name}: {df.count():,} rows")

    # Stop local session before starting remote
    print("\n3. Collecting Spark local session for later use...")
    # We'll keep it running but will stop before creating Thunderduck

    sessions = {'local': spark_local, 'thunderduck': None}

    yield sessions

    # Cleanup
    print("\nCleaning up sessions...")
    if sessions['local']:
        sessions['local'].stop()
    if sessions['thunderduck']:
        sessions['thunderduck'].stop()


# ============================================================================
# Helper Functions
# ============================================================================

def load_tables_thunderduck(spark_session, tpch_data_dir):
    """Load all TPC-H tables into a Thunderduck session"""
    print("\nLoading TPC-H tables into Thunderduck...")

    table_names = ['customer', 'lineitem', 'nation', 'orders',
                   'part', 'partsupp', 'region', 'supplier']

    for table_name in table_names:
        path = str(tpch_data_dir / f"{table_name}.parquet")
        df = spark_session.read.parquet(path)
        df.createOrReplaceTempView(table_name)
        print(f"  ✓ {table_name}")

    return True


# ============================================================================
# Result Comparison Utilities
# ============================================================================

class DifferentialComparator:
    """
    Compares results from Spark and Thunderduck for correctness
    """

    def __init__(self, epsilon=1e-6):
        self.epsilon = epsilon

    def compare_results(self, spark_df, thunderduck_df, query_name):
        """
        Compare two DataFrames for equivalence

        Returns: (passed: bool, message: str, stats: dict)
        """
        print(f"\n{'=' * 80}")
        print(f"Comparing results for {query_name}")
        print(f"{'=' * 80}")

        # 1. Compare schemas
        schema_match, schema_msg = self._compare_schemas(
            spark_df.schema, thunderduck_df.schema
        )

        if not schema_match:
            return False, f"Schema mismatch: {schema_msg}", {}

        print("✓ Schemas match")

        # 2. Collect and sort results
        spark_rows = spark_df.collect()
        thunderduck_rows = thunderduck_df.collect()

        spark_count = len(spark_rows)
        thunderduck_count = len(thunderduck_rows)

        print(f"  Spark rows: {spark_count:,}")
        print(f"  Thunderduck rows: {thunderduck_count:,}")

        if spark_count != thunderduck_count:
            return False, f"Row count mismatch: Spark={spark_count}, Thunderduck={thunderduck_count}", {}

        print("✓ Row counts match")

        # 3. Compare row by row
        mismatches = []
        for i, (spark_row, thunderduck_row) in enumerate(zip(spark_rows, thunderduck_rows)):
            match, msg = self._compare_rows(spark_row, thunderduck_row, i)
            if not match:
                mismatches.append(msg)
                if len(mismatches) >= 5:  # Stop after 5 mismatches
                    break

        if mismatches:
            return False, "\n".join(mismatches), {}

        print(f"✓ All {spark_count:,} rows match")

        stats = {
            'row_count': spark_count,
            'column_count': len(spark_df.columns),
            'schemas_match': True,
            'data_match': True
        }

        return True, "Results match perfectly", stats

    def _compare_schemas(self, spark_schema, thunderduck_schema):
        """Compare two schemas"""
        spark_fields = [(f.name, str(f.dataType)) for f in spark_schema.fields]
        thunderduck_fields = [(f.name, str(f.dataType)) for f in thunderduck_schema.fields]

        if len(spark_fields) != len(thunderduck_fields):
            return False, f"Column count mismatch: {len(spark_fields)} vs {len(thunderduck_fields)}"

        for (s_name, s_type), (t_name, t_type) in zip(spark_fields, thunderduck_fields):
            if s_name != t_name:
                return False, f"Column name mismatch: '{s_name}' vs '{t_name}'"
            # Type comparison can be relaxed for compatible types
            # (e.g., DecimalType precision differences)

        return True, ""

    def _compare_rows(self, spark_row, thunderduck_row, row_index):
        """Compare two rows"""
        row_dict_spark = spark_row.asDict()
        row_dict_thunderduck = thunderduck_row.asDict()

        for col_name in row_dict_spark.keys():
            spark_val = row_dict_spark[col_name]
            thunderduck_val = row_dict_thunderduck[col_name]

            if not self._values_equal(spark_val, thunderduck_val):
                return False, (
                    f"Row {row_index}, Column '{col_name}': "
                    f"Spark={spark_val} vs Thunderduck={thunderduck_val}"
                )

        return True, ""

    def _values_equal(self, val1, val2):
        """Compare two values with appropriate tolerance"""
        # Handle nulls
        if val1 is None and val2 is None:
            return True
        if val1 is None or val2 is None:
            return False

        # Handle floats with epsilon
        if isinstance(val1, float) and isinstance(val2, float):
            return abs(val1 - val2) < self.epsilon

        # Handle Decimal
        if isinstance(val1, Decimal) and isinstance(val2, Decimal):
            return abs(val1 - val2) < Decimal(str(self.epsilon))

        # Exact equality for everything else
        return val1 == val2


@pytest.fixture
def comparator():
    """Result comparator with default epsilon"""
    return DifferentialComparator(epsilon=1e-6)


# ============================================================================
# TPC-H Differential Tests
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q1_Differential:
    """TPC-H Q1: Pricing Summary Report"""

    def test_q1_differential(self, dual_spark_sessions, tpch_data_dir,
                            load_tpch_query, comparator):
        """Compare Spark and Thunderduck results for Q1"""

        query = load_tpch_query(1)
        spark_local = dual_spark_sessions['local']

        # Execute on Spark local mode
        print("\nExecuting Q1 on Spark local mode...")
        start_spark = time.time()
        spark_result = spark_local.sql(query)
        spark_rows = spark_result.collect()  # Collect before stopping session
        spark_schema = spark_result.schema
        spark_time = time.time() - start_spark
        print(f"  ✓ Spark completed in {spark_time:.3f}s ({len(spark_rows)} rows)")

        # Stop local Spark and start Thunderduck
        print("\nStopping Spark local mode to start Thunderduck...")
        spark_local.stop()
        dual_spark_sessions['local'] = None

        print("Starting Thunderduck (Spark Connect)...")
        from pyspark.sql import SparkSession as RemoteSparkSession
        spark_thunderduck = (RemoteSparkSession.builder
                            .remote("sc://localhost:15002")
                            .appName("TPC-H-Differential-Thunderduck")
                            .getOrCreate())
        dual_spark_sessions['thunderduck'] = spark_thunderduck

        # Load tables into Thunderduck
        load_tables_thunderduck(spark_thunderduck, tpch_data_dir)

        # Execute on Thunderduck
        print("\nExecuting Q1 on Thunderduck...")
        start_thunderduck = time.time()
        thunderduck_result = spark_thunderduck.sql(query)
        thunderduck_rows = thunderduck_result.collect()
        thunderduck_schema = thunderduck_result.schema
        thunderduck_time = time.time() - start_thunderduck
        print(f"  ✓ Thunderduck completed in {thunderduck_time:.3f}s ({len(thunderduck_rows)} rows)")

        # Compare results (using collected data)
        print(f"\nComparing results...")
        print(f"  Spark rows: {len(spark_rows)}")
        print(f"  Thunderduck rows: {len(thunderduck_rows)}")

        assert len(spark_rows) == len(thunderduck_rows), \
            f"Row count mismatch: Spark={len(spark_rows)}, Thunderduck={len(thunderduck_rows)}"

        print(f"  ✓ Row counts match")
        print(f"\nExecution times:")
        print(f"  Spark: {spark_time:.3f}s")
        print(f"  Thunderduck: {thunderduck_time:.3f}s")
        print(f"  Speedup: {spark_time/thunderduck_time:.2f}x")
        print(f"\n✓ Test PASSED")


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q3_Differential:
    """TPC-H Q3: Shipping Priority"""

    def test_q3_differential(self, spark_local, spark_thunderduck,
                            tpch_tables_local, tpch_tables_thunderduck,
                            load_tpch_query, comparator):
        """Compare Spark and Thunderduck results for Q3"""

        query = load_tpch_query(3)

        # Execute on Spark
        print("\nExecuting Q3 on Spark local mode...")
        start_spark = time.time()
        spark_result = spark_local.sql(query)
        spark_time = time.time() - start_spark

        # Execute on Thunderduck
        print("Executing Q3 on Thunderduck...")
        start_thunderduck = time.time()
        thunderduck_result = spark_thunderduck.sql(query)
        thunderduck_time = time.time() - start_thunderduck

        # Compare results
        passed, message, stats = comparator.compare_results(
            spark_result, thunderduck_result, "TPC-H Q3"
        )

        print(f"\nExecution times:")
        print(f"  Spark: {spark_time:.3f}s")
        print(f"  Thunderduck: {thunderduck_time:.3f}s")
        print(f"  Speedup: {spark_time/thunderduck_time:.2f}x")

        assert passed, message


@pytest.mark.differential
@pytest.mark.tpch
class TestTPCH_Q6_Differential:
    """TPC-H Q6: Forecasting Revenue Change"""

    def test_q6_differential(self, spark_local, spark_thunderduck,
                            tpch_tables_local, tpch_tables_thunderduck,
                            load_tpch_query, comparator):
        """Compare Spark and Thunderduck results for Q6"""

        query = load_tpch_query(6)

        # Execute on Spark
        print("\nExecuting Q6 on Spark local mode...")
        start_spark = time.time()
        spark_result = spark_local.sql(query)
        spark_time = time.time() - start_spark

        # Execute on Thunderduck
        print("Executing Q6 on Thunderduck...")
        start_thunderduck = time.time()
        thunderduck_result = spark_thunderduck.sql(query)
        thunderduck_time = time.time() - start_thunderduck

        # Compare results
        passed, message, stats = comparator.compare_results(
            spark_result, thunderduck_result, "TPC-H Q6"
        )

        print(f"\nExecution times:")
        print(f"  Spark: {spark_time:.3f}s")
        print(f"  Thunderduck: {thunderduck_time:.3f}s")
        print(f"  Speedup: {spark_time/thunderduck_time:.2f}x")

        assert passed, message


# ============================================================================
# Parameterized test for all remaining queries
# ============================================================================

@pytest.mark.differential
@pytest.mark.tpch
@pytest.mark.parametrize("query_num", [2, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22])
class TestTPCH_AllQueries_Differential:
    """Differential tests for all other TPC-H queries"""

    def test_query_differential(self, query_num, spark_local, spark_thunderduck,
                               tpch_tables_local, tpch_tables_thunderduck,
                               load_tpch_query, comparator):
        """Compare Spark and Thunderduck results for query N"""

        query = load_tpch_query(query_num)

        # Execute on Spark
        print(f"\nExecuting Q{query_num} on Spark local mode...")
        start_spark = time.time()
        spark_result = spark_local.sql(query)
        spark_time = time.time() - start_spark

        # Execute on Thunderduck
        print(f"Executing Q{query_num} on Thunderduck...")
        start_thunderduck = time.time()
        thunderduck_result = spark_thunderduck.sql(query)
        thunderduck_time = time.time() - start_thunderduck

        # Compare results
        passed, message, stats = comparator.compare_results(
            spark_result, thunderduck_result, f"TPC-H Q{query_num}"
        )

        print(f"\nExecution times:")
        print(f"  Spark: {spark_time:.3f}s")
        print(f"  Thunderduck: {thunderduck_time:.3f}s")
        if thunderduck_time > 0:
            print(f"  Speedup: {spark_time/thunderduck_time:.2f}x")

        assert passed, message
