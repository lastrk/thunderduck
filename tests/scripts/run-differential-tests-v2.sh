#!/usr/bin/env bash
# Run differential tests using the V2 framework
# This script auto-detects the project venv, runs tests, and ensures proper cleanup
#
# Compatible with both bash 4+ and zsh
#
# Usage:
#   ./run-differential-tests-v2.sh [--ci] [test-group] [pytest-args...]
#
# Flags:
#   --ci        - CI mode: sets CONTINUE_ON_ERROR=true and COLLECT_TIMEOUT=30
#
# Test groups:
#   all         - Run all differential tests (default)
#   tpch        - TPC-H SQL and DataFrame tests
#   tpcds       - TPC-DS SQL and DataFrame tests
#   functions   - DataFrame function parity tests
#   aggregations - Multi-dimensional aggregation tests
#   window      - Window function tests
#   datetime    - Date/time function tests
#   conditional - Conditional expressions (when/otherwise)
#   operations  - DataFrame operations tests
#   lambda      - Lambda/HOF function tests
#   joins       - USING join tests
#   statistics  - Statistics operations
#   types       - Complex types and type literals
#   schema      - ToSchema tests
#   dataframe   - TPC-DS DataFrame API tests
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
#   THUNDERDUCK_VENV_DIR=.venv         - Override venv location (default: <project-root>/.venv)
#   THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true  - Continue on hard errors (for CI/CD)
#   VERBOSE_FAILURES=true             - Use long tracebacks for failures (--tb=long)
#
# Examples:
#   ./run-differential-tests-v2.sh              # Run all tests
#   ./run-differential-tests-v2.sh tpch         # Run only TPC-H tests
#   ./run-differential-tests-v2.sh window -x    # Run window tests, stop on first failure
#   ./run-differential-tests-v2.sh --ci tpch    # CI mode: TPC-H with CI defaults
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

# ------------------------------------------------------------------------------
# Handle --ci flag
# ------------------------------------------------------------------------------
if [[ "$1" == "--ci" ]]; then
    shift
    export THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR="${THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR:-true}"
    export COLLECT_TIMEOUT="${COLLECT_TIMEOUT:-30}"
fi

# ------------------------------------------------------------------------------
# Resolve Python interpreter (venv auto-detection)
# ------------------------------------------------------------------------------
VENV_DIR="${THUNDERDUCK_VENV_DIR:-$WORKSPACE_DIR/.venv}"

if [ -n "$VIRTUAL_ENV" ]; then
    # User already activated a venv -- use their python3
    PYTHON="python3"
elif [ -x "$VENV_DIR/bin/python3" ]; then
    # Auto-detect project venv
    PYTHON="$VENV_DIR/bin/python3"
elif command -v python3 &> /dev/null; then
    # System python3 fallback (CI environments)
    PYTHON="python3"
else
    echo "ERROR: No Python interpreter found."
    echo ""
    echo "Either:"
    echo "  1. Run the setup script first:  $SCRIPT_DIR/setup-differential-testing.sh"
    echo "  2. Or activate your own venv:   source <your-venv>/bin/activate"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# PID file written by conftest.py for server tracking
PID_FILE="$WORKSPACE_DIR/tests/integration/logs/.server-pids"
PYTEST_PID=""

# Test group definitions - shell-portable approach
# Format: "group:files:description"
get_test_files() {
    case "$1" in
        tpch)
            echo "differential/test_differential_v2.py differential/test_tpch_differential.py"
            ;;
        tpcds)
            echo "differential/test_tpcds_differential.py differential/test_tpcds_dataframe_differential.py"
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
            echo "differential/test_joins_differential.py differential/test_using_joins_differential.py"
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
        datetime)
            echo "differential/test_datetime_functions_differential.py"
            ;;
        conditional)
            echo "differential/test_conditional_differential.py"
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
        datetime)
            echo "Date/time function tests"
            ;;
        conditional)
            echo "Conditional expressions (when/otherwise)"
            ;;
        all)
            echo "All differential tests"
            ;;
        *)
            echo ""
            ;;
    esac
}

# ------------------------------------------------------------------------------
# Cleanup function - called on exit/interrupt
# Uses PID file written by conftest.py for targeted server cleanup
# ------------------------------------------------------------------------------
cleanup() {
    echo ""
    echo -e "${BLUE}Cleaning up...${NC}"

    # Kill pytest if still running
    if [ -n "$PYTEST_PID" ] && kill -0 "$PYTEST_PID" 2>/dev/null; then
        kill "$PYTEST_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$PYTEST_PID" 2>/dev/null || true
    fi

    # Kill servers tracked in PID file (written by conftest.py)
    if [ -f "$PID_FILE" ]; then
        while IFS=: read -r name port pid; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping $name (PID: $pid, port: $port)..."
                # Server used setsid, so PID == PGID; kill entire process group
                kill -- -"$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi

    echo -e "${GREEN}  Cleanup complete${NC}"
}

# Set trap for cleanup on exit, interrupt, terminate
trap cleanup EXIT INT TERM

# ------------------------------------------------------------------------------
# Main Script
# ------------------------------------------------------------------------------
echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Differential Tests V2: Apache Spark 4.1.1 vs Thunderduck${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""

# ------------------------------------------------------------------------------
# Check prerequisites
# ------------------------------------------------------------------------------
echo -e "${BLUE}[1/2] Checking prerequisites...${NC}"

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
# Parse arguments and resolve test group
# ------------------------------------------------------------------------------
show_help() {
    echo "Usage: $0 [--ci] [test-group] [pytest-args...]"
    echo ""
    echo "Flags:"
    echo "  --ci         - CI mode: sets CONTINUE_ON_ERROR=true and COLLECT_TIMEOUT=30"
    echo ""
    echo "Test groups:"
    echo "  all          - All differential tests (default)"
    echo "  tpch         - TPC-H SQL and DataFrame tests"
    echo "  tpcds        - TPC-DS SQL and DataFrame tests"
    echo "  functions    - DataFrame function parity tests"
    echo "  aggregations - Multi-dimensional aggregation tests"
    echo "  window       - Window function tests"
    echo "  datetime     - Date/time function tests"
    echo "  conditional  - Conditional expressions (when/otherwise)"
    echo "  operations   - DataFrame operations tests"
    echo "  lambda       - Lambda/HOF function tests"
    echo "  joins        - USING join tests"
    echo "  statistics   - Statistics operations"
    echo "  types        - Complex types and type literals"
    echo "  schema       - ToSchema tests"
    echo "  dataframe    - TPC-DS DataFrame API tests"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all tests"
    echo "  $0 tpch               # Run only TPC-H tests"
    echo "  $0 --ci tpch          # CI mode: TPC-H with CI defaults"
    echo "  $0 window -x          # Run window tests, stop on first failure"
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
        for group in tpch tpcds functions aggregations window datetime conditional operations lambda joins statistics types schema dataframe all; do
            echo "  $group - $(get_test_description "$group")"
        done
        exit 1
    fi
fi

# ------------------------------------------------------------------------------
# Run tests
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[2/2] Running tests...${NC}"
echo ""
echo -e "  ${CYAN}Test group:${NC} $TEST_GROUP ($(get_test_description "$TEST_GROUP"))"
echo -e "  ${CYAN}Test files:${NC} $TEST_FILES"
if [ -n "$PYTEST_ARGS" ]; then
    echo -e "  ${CYAN}Pytest args:${NC} $PYTEST_ARGS"
fi
echo ""
echo -e "  ${CYAN}Configuration:${NC}"
echo -e "    Python:            $PYTHON"
echo -e "    Spark port:        ${SPARK_PORT:-auto}"
echo -e "    Thunderduck port:  ${THUNDERDUCK_PORT:-auto}"
echo -e "    Compat mode:       ${THUNDERDUCK_COMPAT_MODE:-auto}"
echo -e "    Connect timeout:   ${CONNECT_TIMEOUT:-10}s"
echo -e "    Collect timeout:   ${COLLECT_TIMEOUT:-10}s"
echo -e "    Continue on error: ${THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR:-false}"
echo -e "    Verbose failures:  ${VERBOSE_FAILURES:-false}"
echo ""

cd "$WORKSPACE_DIR/tests/integration"

# Export SPARK_HOME for the tests
export SPARK_HOME

# Build extra pytest args: only override --tb when VERBOSE_FAILURES is set
# (pyproject.toml already provides -v and --tb=short as defaults)
TB_STYLE=""
if [ "${VERBOSE_FAILURES:-false}" = "true" ]; then
    TB_STYLE="--tb=long"
fi

# Run pytest in background so we can capture its PID for cleanup
# shellcheck disable=SC2086
set +e
$PYTHON -m pytest \
    $TEST_FILES \
    $TB_STYLE \
    $PYTEST_ARGS &
PYTEST_PID=$!
wait $PYTEST_PID
TEST_EXIT_CODE=$?
set -e

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
