#!/usr/bin/env bash
# Run differential tests using the V2 framework
# This script activates the venv, runs tests, and ensures proper cleanup
#
# Compatible with both bash 4+ and zsh
#
# Usage:
#   ./run-differential-tests-v2.sh [test-group] [pytest-args...]
#
# Test groups:
#   all         - Run all differential tests (default)
#   tpch        - TPC-H SQL and DataFrame tests (27 tests)
#   tpcds       - TPC-DS SQL and DataFrame tests (126 tests)
#   functions   - DataFrame function parity tests (57 tests)
#   aggregations - Multi-dimensional aggregation tests (21 tests)
#   window      - Window function tests (35 tests)
#
# Environment variables (all optional):
#   SPARK_PORT=15003              - Spark Reference server port
#   THUNDERDUCK_PORT=15002        - Thunderduck server port
#   CONNECT_TIMEOUT=10            - Session creation timeout (seconds)
#   QUERY_PLAN_TIMEOUT=5          - Query plan building timeout
#   COLLECT_TIMEOUT=10            - Result collection timeout
#   HEALTH_CHECK_TIMEOUT=2        - Server health check timeout
#   SERVER_STARTUP_TIMEOUT=60     - Server startup timeout
#   SPARK_MEMORY=4g               - Spark driver memory
#   THUNDERDUCK_MEMORY=2g         - Thunderduck JVM heap
#   THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true  - Continue on hard errors (for CI/CD)
#   VERBOSE_FAILURES=true             - Use long tracebacks for failures (--tb=long)
#
# Examples:
#   ./run-differential-tests-v2.sh              # Run all tests
#   ./run-differential-tests-v2.sh tpch         # Run only TPC-H tests
#   ./run-differential-tests-v2.sh window -x    # Run window tests, stop on first failure
#   COLLECT_TIMEOUT=30 ./run-differential-tests-v2.sh tpcds  # Longer timeout for TPC-DS
#   THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true ./run-differential-tests-v2.sh  # CI/CD mode

# Detect shell and set compatibility mode
if [ -n "$ZSH_VERSION" ]; then
    # Running in zsh - enable bash-like array behavior
    emulate -L sh
    setopt SH_WORD_SPLIT
    SCRIPT_PATH="${(%):-%x}"
elif [ -n "$BASH_VERSION" ]; then
    # Running in bash - check version
    if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
        echo "ERROR: This script requires bash 4.0 or later (found: $BASH_VERSION)"
        echo "Please upgrade bash or run on a system with bash 4+"
        exit 1
    fi
    SCRIPT_PATH="${BASH_SOURCE[0]}"
else
    echo "ERROR: This script requires bash 4+ or zsh"
    echo "Please run with: bash $0 $* (or zsh $0 $*)"
    exit 1
fi

set -e

SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
SPARK_HOME="${SPARK_HOME:-$HOME/spark/current}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test group definitions - shell-portable approach
# Format: "group:files:description"
get_test_files() {
    case "$1" in
        tpch)
            echo "differential/test_differential_v2.py"
            ;;
        tpcds)
            echo "differential/test_tpcds_differential.py"
            ;;
        functions)
            echo "differential/test_dataframe_functions.py"
            ;;
        aggregations)
            echo "differential/test_multidim_aggregations.py"
            ;;
        window)
            echo "differential/test_window_functions.py"
            ;;
        operations)
            echo "differential/test_dataframe_ops_differential.py"
            ;;
        lambda)
            echo "differential/test_lambda_differential.py"
            ;;
        joins)
            echo "differential/test_using_joins_differential.py"
            ;;
        statistics)
            echo "differential/test_statistics_differential.py"
            ;;
        types)
            echo "differential/test_complex_types_differential.py differential/test_type_literals_differential.py"
            ;;
        schema)
            echo "differential/test_to_schema_differential.py"
            ;;
        dataframe)
            echo "differential/test_tpcds_dataframe_differential.py"
            ;;
        all)
            echo "differential/"
            ;;
        *)
            echo ""
            ;;
    esac
}

get_test_description() {
    case "$1" in
        tpch)
            echo "TPC-H SQL and DataFrame tests"
            ;;
        tpcds)
            echo "TPC-DS SQL and DataFrame tests"
            ;;
        functions)
            echo "DataFrame function parity tests"
            ;;
        aggregations)
            echo "Multi-dimensional aggregation tests"
            ;;
        window)
            echo "Window function tests"
            ;;
        operations)
            echo "DataFrame operations tests"
            ;;
        lambda)
            echo "Lambda/HOF function tests"
            ;;
        joins)
            echo "USING join tests"
            ;;
        statistics)
            echo "Statistics operations (cov, corr, describe)"
            ;;
        types)
            echo "Complex types and type literals"
            ;;
        schema)
            echo "ToSchema df.to(schema) tests"
            ;;
        dataframe)
            echo "TPC-DS DataFrame API tests"
            ;;
        all)
            echo "All differential tests"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Track if we started servers (for cleanup)
SPARK_STARTED=false
THUNDERDUCK_STARTED=false

# ------------------------------------------------------------------------------
# Cleanup function - called on exit/interrupt
# ------------------------------------------------------------------------------
cleanup() {
    echo ""
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${BLUE}Cleaning up...${NC}"
    echo -e "${BLUE}================================================================${NC}"

    # Kill Spark Connect server
    if pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer" > /dev/null 2>&1; then
        echo "  Stopping Spark Connect server..."
        pkill -9 -f "org.apache.spark.sql.connect.service.SparkConnectServer" 2>/dev/null || true
        sleep 1
    fi

    # Kill Thunderduck server
    if pgrep -f "thunderduck-connect-server" > /dev/null 2>&1; then
        echo "  Stopping Thunderduck server..."
        pkill -9 -f "thunderduck-connect-server" 2>/dev/null || true
        sleep 1
    fi

    echo -e "${GREEN}  Cleanup complete${NC}"
}

# Set trap for cleanup on exit, interrupt, terminate
trap cleanup EXIT INT TERM

# ------------------------------------------------------------------------------
# Main Script
# ------------------------------------------------------------------------------
echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Differential Tests V2: Apache Spark 4.0.1 vs Thunderduck${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""

# ------------------------------------------------------------------------------
# Check prerequisites
# ------------------------------------------------------------------------------
echo -e "${BLUE}[1/3] Checking prerequisites...${NC}"

# Check Spark installation
if [ ! -d "$SPARK_HOME" ] || [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    echo -e "${RED}ERROR: Apache Spark not found at $SPARK_HOME${NC}"
    echo ""
    echo "Please run the setup script first:"
    echo "  $SCRIPT_DIR/setup-differential-testing.sh"
    exit 1
fi
echo -e "${GREEN}  Spark found at: $SPARK_HOME${NC}"

# Check TPC-H data
if [ ! -d "$WORKSPACE_DIR/tests/integration/tpch_sf001" ]; then
    echo -e "${RED}ERROR: TPC-H data not found at $WORKSPACE_DIR/tests/integration/tpch_sf001${NC}"
    echo "Please ensure TPC-H data files exist in tests/integration/tpch_sf001/"
    exit 1
fi
echo -e "${GREEN}  TPC-H data found${NC}"

# Check Thunderduck server JAR
SERVER_JAR=$(ls "$WORKSPACE_DIR/connect-server/target/thunderduck-connect-server-"*.jar 2>/dev/null | head -1)
if [ -z "$SERVER_JAR" ] || [ ! -f "$SERVER_JAR" ]; then
    echo -e "${YELLOW}  Thunderduck server JAR not found. Building...${NC}"
    cd "$WORKSPACE_DIR"
    mvn clean package -DskipTests -q
    SERVER_JAR=$(ls "$WORKSPACE_DIR/connect-server/target/thunderduck-connect-server-"*.jar 2>/dev/null | head -1)
    if [ -z "$SERVER_JAR" ]; then
        echo -e "${RED}ERROR: Failed to build Thunderduck server${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}  Thunderduck server JAR found${NC}"

# ------------------------------------------------------------------------------
# Kill any existing servers
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[2/3] Stopping any existing servers...${NC}"

if pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer" > /dev/null 2>&1; then
    echo "  Killing existing Spark Connect server..."
    pkill -9 -f "org.apache.spark.sql.connect.service.SparkConnectServer" 2>/dev/null || true
    sleep 2
fi

if pgrep -f "thunderduck-connect-server" > /dev/null 2>&1; then
    echo "  Killing existing Thunderduck server..."
    pkill -9 -f "thunderduck-connect-server" 2>/dev/null || true
    sleep 2
fi

echo -e "${GREEN}  Clean slate confirmed${NC}"

# ------------------------------------------------------------------------------
# Parse arguments and resolve test group
# ------------------------------------------------------------------------------
show_help() {
    echo "Usage: $0 [test-group] [pytest-args...]"
    echo ""
    echo "Test groups:"
    echo "  all          - All differential tests (default)"
    echo "  tpch         - TPC-H SQL and DataFrame tests"
    echo "  tpcds        - TPC-DS SQL and DataFrame tests"
    echo "  functions    - DataFrame function parity tests"
    echo "  aggregations - Multi-dimensional aggregation tests"
    echo "  window       - Window function tests"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests"
    echo "  $0 tpch               # Run only TPC-H tests"
    echo "  $0 window -x          # Run window tests, stop on first failure"
    echo "  $0 all --tb=long      # Run all with verbose traceback"
    exit 0
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
fi

# Determine test group
TEST_GROUP="${1:-all}"
PYTEST_ARGS="${@:2}"

# Check if first arg is a valid test group or a pytest arg
TEST_FILES="$(get_test_files "$TEST_GROUP")"
if [ -z "$TEST_FILES" ]; then
    # Not a known test group - might be a pytest arg or file path
    if [[ "$TEST_GROUP" == -* || "$TEST_GROUP" == *.py ]]; then
        # It's a pytest arg or file, use 'all' as default group
        PYTEST_ARGS="$@"
        TEST_GROUP="all"
        TEST_FILES="$(get_test_files "$TEST_GROUP")"
    else
        echo -e "${RED}ERROR: Unknown test group '$TEST_GROUP'${NC}"
        echo ""
        echo "Available test groups:"
        for group in tpch tpcds functions aggregations window all; do
            echo "  $group - $(get_test_description "$group")"
        done
        exit 1
    fi
fi

# ------------------------------------------------------------------------------
# Run tests
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[3/3] Running tests...${NC}"
echo ""
echo -e "  ${CYAN}Test group:${NC} $TEST_GROUP ($(get_test_description "$TEST_GROUP"))"
echo -e "  ${CYAN}Test files:${NC} $TEST_FILES"
if [ -n "$PYTEST_ARGS" ]; then
    echo -e "  ${CYAN}Pytest args:${NC} $PYTEST_ARGS"
fi
echo ""
echo -e "  ${CYAN}Configuration:${NC}"
echo -e "    Spark port:       ${SPARK_PORT:-15003}"
echo -e "    Thunderduck port: ${THUNDERDUCK_PORT:-15002}"
echo -e "    Connect timeout:  ${CONNECT_TIMEOUT:-10}s"
echo -e "    Collect timeout:  ${COLLECT_TIMEOUT:-10}s"
echo -e "    Continue on error: ${THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR:-false}"
echo -e "    Verbose failures:  ${VERBOSE_FAILURES:-false}"
echo ""

cd "$WORKSPACE_DIR/tests/integration"

# Export SPARK_HOME for the tests
export SPARK_HOME

# Set traceback style based on VERBOSE_FAILURES
if [ "${VERBOSE_FAILURES:-false}" = "true" ]; then
    TB_STYLE="--tb=long"
else
    TB_STYLE="--tb=short"
fi

# Run pytest with all test files
# shellcheck disable=SC2086
python3 -m pytest \
    $TEST_FILES \
    -v \
    $TB_STYLE \
    $PYTEST_ARGS

TEST_EXIT_CODE=$?

# ------------------------------------------------------------------------------
# Report results
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Test Group: ${CYAN}$TEST_GROUP${NC}"
echo -e "${BLUE}================================================================${NC}"
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
else
    echo -e "${RED}SOME TESTS FAILED${NC}"
fi
echo -e "${BLUE}================================================================${NC}"

exit $TEST_EXIT_CODE
