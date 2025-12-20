"""
Pytest configuration for Thunderduck Spark Connect integration tests
"""

import pytest
from pyspark.sql import SparkSession
from pathlib import Path
import sys
import atexit
import signal
import subprocess
import time
import threading

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent / "utils"))


# ------------------------------------------------------------------------------
# Global cleanup functions for handling interrupts
# ------------------------------------------------------------------------------

def kill_all_servers():
    """Kill any running Spark/Thunderduck server processes"""
    # Kill Spark Connect server
    subprocess.run(
        ["pkill", "-9", "-f", "org.apache.spark.sql.connect.service.SparkConnectServer"],
        capture_output=True
    )
    # Kill Thunderduck server
    subprocess.run(
        ["pkill", "-9", "-f", "thunderduck-connect-server"],
        capture_output=True
    )


def signal_handler(signum, frame):
    """Handle interrupt signals"""
    print(f"\n\nReceived signal {signum}, cleaning up servers...")
    kill_all_servers()
    sys.exit(1)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Register atexit handler as fallback
atexit.register(kill_all_servers)


def stop_spark_with_timeout(spark, timeout=10):
    """
    Stop SparkSession with a timeout to prevent hanging.

    PySpark's spark.stop() can block indefinitely waiting for a clean gRPC
    disconnection. This wrapper ensures cleanup continues even if the
    disconnection hangs.
    """
    def stop_func():
        try:
            spark.stop()
        except Exception:
            pass  # Ignore errors during cleanup

    thread = threading.Thread(target=stop_func)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        print(f"  Warning: SparkSession.stop() did not complete within {timeout}s, continuing...")


from server_manager import ServerManager
from result_validator import ResultValidator
from dual_server_manager import DualServerManager


# Server fixtures

@pytest.fixture(scope="session")
def server_manager():
    """
    Session-scoped fixture that starts server once for all tests
    """
    manager = ServerManager(host="localhost", port=15002)

    print("\n" + "="*80)
    print("Starting Spark Connect server for integration tests...")
    print("="*80)

    if not manager.start(timeout=60):
        pytest.exit("Failed to start Spark Connect server", returncode=1)

    yield manager

    print("\n" + "="*80)
    print("Stopping Spark Connect server...")
    print("="*80)
    manager.stop()


@pytest.fixture(scope="class")
def spark_session(spark_thunderduck):
    """
    Class-scoped alias for spark_thunderduck.

    For backward compatibility with tests expecting spark_session.
    """
    return spark_thunderduck


@pytest.fixture(scope="class")
def spark(spark_thunderduck):
    """
    Class-scoped alias for spark_thunderduck.

    This allows tests written for single-server mode to work
    in the dual-server testing environment.
    """
    return spark_thunderduck


# Validator fixtures

@pytest.fixture
def validator():
    """
    Result validator with default epsilon
    """
    return ResultValidator(epsilon=1e-6)


# Data path fixtures

@pytest.fixture(scope="session")
def workspace_dir():
    """Path to workspace root directory"""
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="session")
def tpch_data_dir():
    """Path to TPC-H data directory (co-located with integration tests)"""
    data_dir = Path(__file__).parent / "tpch_sf001"
    if not data_dir.exists():
        pytest.skip(f"TPC-H data not found at {data_dir}. Please ensure data files exist.")
    return data_dir


@pytest.fixture(scope="session")
def tpch_queries_dir():
    """Path to TPC-H queries directory"""
    queries_dir = Path(__file__).parent / "sql" / "tpch_queries"
    if not queries_dir.exists():
        pytest.skip(f"TPC-H queries not found at {queries_dir}")
    return queries_dir


@pytest.fixture
def lineitem_df(spark, tpch_data_dir):
    """Load lineitem table as DataFrame"""
    path = str(tpch_data_dir / "lineitem.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def orders_df(spark, tpch_data_dir):
    """Load orders table as DataFrame"""
    path = str(tpch_data_dir / "orders.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def customer_df(spark, tpch_data_dir):
    """Load customer table as DataFrame"""
    path = str(tpch_data_dir / "customer.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def part_df(spark, tpch_data_dir):
    """Load part table as DataFrame"""
    path = str(tpch_data_dir / "part.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def supplier_df(spark, tpch_data_dir):
    """Load supplier table as DataFrame"""
    path = str(tpch_data_dir / "supplier.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def partsupp_df(spark, tpch_data_dir):
    """Load partsupp table as DataFrame"""
    path = str(tpch_data_dir / "partsupp.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def nation_df(spark, tpch_data_dir):
    """Load nation table as DataFrame"""
    path = str(tpch_data_dir / "nation.parquet")
    return spark.read.parquet(path)


@pytest.fixture
def region_df(spark, tpch_data_dir):
    """Load region table as DataFrame"""
    path = str(tpch_data_dir / "region.parquet")
    return spark.read.parquet(path)


@pytest.fixture(scope="session")
def tpch_tables(spark_session, tpch_data_dir):
    """
    Verify TPC-H tables are available

    NOTE: Temporary view registration is not yet supported by the server
    (createOrReplaceTempView sends a COMMAND type that's unimplemented).
    For now, DataFrame API tests will load data directly, and SQL tests
    will need to use read_parquet() in queries or be skipped.
    """
    tables = [
        'lineitem', 'orders', 'customer', 'part',
        'supplier', 'partsupp', 'nation', 'region'
    ]

    print("\nLoading TPC-H tables and creating temp views...")
    for table in tables:
        parquet_path = tpch_data_dir / f"{table}.parquet"
        if not parquet_path.exists():
            pytest.skip(f"TPC-H table not found: {parquet_path}")

        # Load table and create temp view
        df = spark_session.read.parquet(str(parquet_path))
        df.createOrReplaceTempView(table)
        row_count = df.count()
        print(f"  ✓ {table}: {row_count:,} rows")

    print(f"✓ All {len(tables)} TPC-H tables registered as temp views")

    return tables


# Utility functions

def load_query(query_num: int, queries_dir: Path) -> str:
    """
    Load a TPC-H query from file

    Args:
        query_num: Query number (1-22)
        queries_dir: Path to queries directory

    Returns:
        Query SQL string
    """
    query_file = queries_dir / f"q{query_num}.sql"
    if not query_file.exists():
        pytest.skip(f"Query file not found: {query_file}")

    with open(query_file, 'r') as f:
        return f.read()


@pytest.fixture
def load_tpch_query(tpch_queries_dir):
    """
    Fixture that returns a function to load TPC-H queries
    """
    def _load(query_num: int) -> str:
        return load_query(query_num, tpch_queries_dir)
    return _load


# Pytest configuration

def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "tpch: mark test as TPC-H benchmark test"
    )
    config.addinivalue_line(
        "markers", "tpcds: mark test as TPC-DS benchmark test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "dataframe: mark test as using DataFrame API"
    )
    config.addinivalue_line(
        "markers", "sql: mark test as using SQL"
    )
    config.addinivalue_line(
        "markers", "differential: mark test as differential test (Spark vs Thunderduck)"
    )
    config.addinivalue_line(
        "markers", "quick: mark test as quick sanity test"
    )
    config.addinivalue_line(
        "markers", "functions: mark test as DataFrame function parity test"
    )
    config.addinivalue_line(
        "markers", "aggregations: mark test as multi-dimensional aggregation test"
    )
    config.addinivalue_line(
        "markers", "window: mark test as window function test"
    )


def pytest_collection_modifyitems(config, items):
    """Add markers automatically based on test names"""
    for item in items:
        # Add tpch marker to tests with 'tpch' in name
        if 'tpch' in item.nodeid.lower():
            item.add_marker(pytest.mark.tpch)

        # Add dataframe marker to tests with 'dataframe' in name
        if 'dataframe' in item.nodeid.lower():
            item.add_marker(pytest.mark.dataframe)

        # Add sql marker to tests with 'sql' in name
        if '_sql' in item.nodeid.lower():
            item.add_marker(pytest.mark.sql)


# TPC-DS Fixtures

@pytest.fixture(scope="session")
def tpcds_data_dir():
    """Path to TPC-DS test data"""
    data_dir = Path("/workspace/data/tpcds_sf1")
    if not data_dir.exists():
        pytest.skip(f"TPC-DS data not found at {data_dir}")
    return data_dir


@pytest.fixture(scope="session")
def tpcds_queries_dir():
    """Path to TPC-DS queries directory"""
    queries_dir = Path(__file__).parent / "sql" / "tpcds_queries"
    if not queries_dir.exists():
        pytest.skip(f"TPC-DS queries not found at {queries_dir}")
    return queries_dir


@pytest.fixture(scope="session")
def tpcds_tables(spark_session, tpcds_data_dir):
    """Load all TPC-DS tables as temp views"""
    tables = sorted([f.stem for f in tpcds_data_dir.glob("*.parquet")])

    print(f"\nLoading {len(tables)} TPC-DS tables...")
    for table in tables:
        path = str(tpcds_data_dir / f"{table}.parquet")
        df = spark_session.read.parquet(path)
        df.createOrReplaceTempView(table)

    print(f"✓ All {len(tables)} TPC-DS tables registered")
    return tables


@pytest.fixture
def load_tpcds_query(tpcds_queries_dir):
    """Load TPC-DS query by number or variant name"""
    def _load_query(qnum):
        """
        Load a TPC-DS query.

        Args:
            qnum: Query number (1-99) or variant string ('14a', '14b', '23a', etc.)
        """
        query_file = tpcds_queries_dir / f"q{qnum}.sql"
        if not query_file.exists():
            pytest.skip(f"Query file not found: {query_file}")
        return query_file.read_text()
    return _load_query


# ============================================================================
# Differential Testing Fixtures (Spark Reference vs Thunderduck)
# ============================================================================
# Consolidated infrastructure with:
# - Low timeouts to detect deadlocks/blocking/failures fast
# - Timing measurements for connect/query plan/collect phases
# - Error classification (hard errors vs soft errors)
# - Diagnostic collection on failures
#
# Configuration via environment variables:
#   SPARK_PORT=15003              - Spark Reference server port
#   THUNDERDUCK_PORT=15002        - Thunderduck server port
#   CONNECT_TIMEOUT=10            - Session creation timeout (seconds)
#   QUERY_PLAN_TIMEOUT=5          - Query plan building timeout
#   COLLECT_TIMEOUT=10            - Result collection timeout
#   HEALTH_CHECK_TIMEOUT=2        - Server health check timeout
#   SERVER_STARTUP_TIMEOUT=60     - Server startup timeout
#   SPARK_MEMORY=4g               - Spark driver memory
#   THUNDERDUCK_MEMORY=2g         - Thunderduck JVM heap
#   THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true  - Continue on hard errors
# ============================================================================

import os
from utils.test_orchestrator import TestOrchestrator
from utils.exceptions import HardError


def _get_orchestrator_config():
    """Build orchestrator config from environment variables."""
    return {
        # Ports
        'spark_port': int(os.environ.get('SPARK_PORT', 15003)),
        'thunderduck_port': int(os.environ.get('THUNDERDUCK_PORT', 15002)),
        # Timeouts (low by default to detect issues fast)
        'connect_timeout': int(os.environ.get('CONNECT_TIMEOUT', 10)),
        'query_plan_timeout': int(os.environ.get('QUERY_PLAN_TIMEOUT', 5)),
        'collect_timeout': int(os.environ.get('COLLECT_TIMEOUT', 10)),
        'health_check_timeout': int(os.environ.get('HEALTH_CHECK_TIMEOUT', 2)),
        'server_startup_timeout': int(os.environ.get('SERVER_STARTUP_TIMEOUT', 60)),
        # Memory
        'spark_memory': os.environ.get('SPARK_MEMORY', '4g'),
        'thunderduck_memory': os.environ.get('THUNDERDUCK_MEMORY', '2g'),
        # Behavior
        'continue_on_error': os.environ.get('THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR', '').lower() == 'true',
        # Workspace
        'workspace_dir': str(Path(__file__).parent.parent.parent),
    }


@pytest.fixture(scope="session")
def dual_server_manager():
    """
    Session-scoped fixture that starts both servers for differential testing.

    Starts:
    - Apache Spark Connect 4.0.1 (reference) on port 15003 (native installation)
    - Thunderduck Connect (test) on port 15002

    Both servers are started fresh and killed on teardown (even on interrupt).
    """
    # Kill any existing servers first to ensure clean slate
    kill_all_servers()

    manager = DualServerManager(
        thunderduck_port=15002,
        spark_reference_port=15003
    )

    print("\n" + "="*80)
    print("Starting DUAL servers for differential testing...")
    print("="*80)

    spark_ok, thunderduck_ok = manager.start_both(timeout=120)

    if not (spark_ok and thunderduck_ok):
        manager.stop_both()
        pytest.exit("Failed to start both servers for differential testing", returncode=1)

    yield manager

    print("\n" + "="*80)
    print("Stopping both servers...")
    print("="*80)
    manager.stop_both()


@pytest.fixture(scope="session")
def orchestrator(dual_server_manager):
    """
    Session-scoped orchestrator that manages sessions with monitoring.

    Provides:
    - Low timeouts to detect deadlocks/blocking/failures fast
    - Timing measurements for performance analysis
    - Diagnostic collection on hard errors

    Depends on dual_server_manager to ensure servers are running.
    """
    config = _get_orchestrator_config()
    orch = TestOrchestrator(config)

    yield orch

    # Print timing summary at end of test session
    print(orch.timings.get_summary())


# Primary session fixtures (class-scoped for isolation)
@pytest.fixture(scope="class")
def spark_reference(orchestrator):
    """
    Class-scoped Spark session connected to Apache Spark Connect (reference).

    This is the reference implementation (official Apache Spark 4.0.1).
    Fresh session per test class provides isolation while being efficient.
    """
    session = orchestrator.create_spark_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


@pytest.fixture(scope="class")
def spark_thunderduck(orchestrator):
    """
    Class-scoped Spark session connected to Thunderduck Connect (test).

    This is the system under test (Thunderduck implementation).
    Fresh session per test class provides isolation while being efficient.
    """
    session = orchestrator.create_thunderduck_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


# Function-scoped sessions (for tests that need per-test isolation)
@pytest.fixture
def spark_reference_isolated(orchestrator):
    """
    Function-scoped: fresh PySpark session per test.

    Use for tests that modify state (temp views, settings) and need
    a clean session for each test.
    """
    session = orchestrator.create_spark_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


@pytest.fixture
def thunderduck_isolated(orchestrator):
    """
    Function-scoped: fresh PySpark session per test.

    Use for tests that modify state (temp views, settings) and need
    a clean session for each test.
    """
    session = orchestrator.create_thunderduck_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


# Fresh server fixtures (kills and restarts server)
@pytest.fixture
def fresh_spark_server(orchestrator):
    """
    Restarts Spark Reference server, returns new session.

    Use when you need a completely fresh server state.
    WARNING: This is slow (~60s). Only use when absolutely necessary.
    """
    orchestrator.restart_spark_server()
    session = orchestrator.create_spark_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


@pytest.fixture
def fresh_thunderduck_server(orchestrator):
    """
    Restarts Thunderduck server, returns new session.

    Use when you need a completely fresh server state.
    WARNING: This is slow (~30s). Only use when absolutely necessary.
    """
    orchestrator.restart_thunderduck_server()
    session = orchestrator.create_thunderduck_session()
    yield session
    stop_spark_with_timeout(session, timeout=5)


# ============================================================================
# TPC-H Differential Testing Fixtures
# ============================================================================

@pytest.fixture(scope="class")
def tpch_tables_reference(spark_reference, tpch_data_dir):
    """
    Load TPC-H tables into Spark Reference session (class-scoped).
    """
    tables = [
        'lineitem', 'orders', 'customer', 'part',
        'supplier', 'partsupp', 'nation', 'region'
    ]

    print("\nLoading TPC-H tables into Spark Reference...")
    for table in tables:
        parquet_path = tpch_data_dir / f"{table}.parquet"
        if not parquet_path.exists():
            pytest.skip(f"TPC-H table not found: {parquet_path}")

        df = spark_reference.read.parquet(str(parquet_path))
        df.createOrReplaceTempView(table)

    print(f"✓ All {len(tables)} TPC-H tables loaded into Spark Reference")
    return tables


@pytest.fixture(scope="class")
def tpch_tables_thunderduck(spark_thunderduck, tpch_data_dir):
    """
    Load TPC-H tables into Thunderduck session (class-scoped).
    """
    tables = [
        'lineitem', 'orders', 'customer', 'part',
        'supplier', 'partsupp', 'nation', 'region'
    ]

    print("\nLoading TPC-H tables into Thunderduck...")
    for table in tables:
        parquet_path = tpch_data_dir / f"{table}.parquet"
        if not parquet_path.exists():
            pytest.skip(f"TPC-H table not found: {parquet_path}")

        df = spark_thunderduck.read.parquet(str(parquet_path))
        df.createOrReplaceTempView(table)

    print(f"✓ All {len(tables)} TPC-H tables loaded into Thunderduck")
    return tables


# ============================================================================
# TPC-DS Differential Testing Fixtures
# ============================================================================

# List of all TPC-DS tables
TPCDS_TABLES = [
    'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item',
    'promotion', 'reason', 'ship_mode', 'store', 'store_returns', 'store_sales',
    'time_dim', 'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site'
]


@pytest.fixture(scope="class")
def tpcds_tables_reference(spark_reference, tpcds_data_dir):
    """
    Load TPC-DS tables into Spark Reference session (class-scoped).
    """
    print(f"\nLoading {len(TPCDS_TABLES)} TPC-DS tables into Spark Reference...")
    for table in TPCDS_TABLES:
        parquet_path = tpcds_data_dir / f"{table}.parquet"
        if not parquet_path.exists():
            pytest.skip(f"TPC-DS table not found: {parquet_path}")

        df = spark_reference.read.parquet(str(parquet_path))
        df.createOrReplaceTempView(table)

    print(f"✓ All {len(TPCDS_TABLES)} TPC-DS tables loaded into Spark Reference")
    return TPCDS_TABLES


@pytest.fixture(scope="class")
def tpcds_tables_thunderduck(spark_thunderduck, tpcds_data_dir):
    """
    Load TPC-DS tables into Thunderduck session (class-scoped).
    """
    print(f"\nLoading {len(TPCDS_TABLES)} TPC-DS tables into Thunderduck...")
    for table in TPCDS_TABLES:
        parquet_path = tpcds_data_dir / f"{table}.parquet"
        if not parquet_path.exists():
            pytest.skip(f"TPC-DS table not found: {parquet_path}")

        df = spark_thunderduck.read.parquet(str(parquet_path))
        df.createOrReplaceTempView(table)

    print(f"✓ All {len(TPCDS_TABLES)} TPC-DS tables loaded into Thunderduck")
    return TPCDS_TABLES
