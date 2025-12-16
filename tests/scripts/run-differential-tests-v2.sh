#!/bin/bash
# Run differential tests using the V2 framework
# This script activates the venv, runs tests, and ensures proper cleanup
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
# Examples:
#   ./run-differential-tests-v2.sh              # Run all tests
#   ./run-differential-tests-v2.sh tpch         # Run only TPC-H tests
#   ./run-differential-tests-v2.sh window -x    # Run window tests, stop on first failure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV_DIR="$WORKSPACE_DIR/tests/integration/.venv"
SPARK_HOME="${SPARK_HOME:-$HOME/spark/current}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test group definitions
declare -A TEST_GROUPS
TEST_GROUPS[tpch]="test_differential_v2.py"
TEST_GROUPS[tpcds]="test_tpcds_differential.py"
TEST_GROUPS[functions]="test_dataframe_functions.py"
TEST_GROUPS[aggregations]="test_multidim_aggregations.py"
TEST_GROUPS[window]="test_window_functions.py"
TEST_GROUPS[all]="test_differential_v2.py test_tpcds_differential.py test_dataframe_functions.py test_multidim_aggregations.py test_window_functions.py"

# Test group descriptions for help
declare -A TEST_DESCRIPTIONS
TEST_DESCRIPTIONS[tpch]="TPC-H SQL and DataFrame tests"
TEST_DESCRIPTIONS[tpcds]="TPC-DS SQL and DataFrame tests"
TEST_DESCRIPTIONS[functions]="DataFrame function parity tests"
TEST_DESCRIPTIONS[aggregations]="Multi-dimensional aggregation tests"
TEST_DESCRIPTIONS[window]="Window function tests"
TEST_DESCRIPTIONS[all]="All differential tests"

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
echo -e "${BLUE}[1/4] Checking prerequisites...${NC}"

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}ERROR: Virtual environment not found at $VENV_DIR${NC}"
    echo ""
    echo "Please run the setup script first:"
    echo "  $SCRIPT_DIR/setup-differential-testing.sh"
    exit 1
fi
echo -e "${GREEN}  Virtual environment found${NC}"

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
if [ ! -d "$WORKSPACE_DIR/data/tpch_sf001" ]; then
    echo -e "${RED}ERROR: TPC-H data not found at $WORKSPACE_DIR/data/tpch_sf001${NC}"
    echo "Please generate TPC-H data first"
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
echo -e "${BLUE}[2/4] Stopping any existing servers...${NC}"

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
# Activate virtual environment
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[3/4] Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"
echo -e "${GREEN}  Python: $(which python)${NC}"
echo -e "${GREEN}  PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')${NC}"

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
if [[ -z "${TEST_GROUPS[$TEST_GROUP]}" ]]; then
    # Not a known test group - might be a pytest arg or file path
    if [[ "$TEST_GROUP" == -* || "$TEST_GROUP" == *.py ]]; then
        # It's a pytest arg or file, use 'all' as default group
        PYTEST_ARGS="$@"
        TEST_GROUP="all"
    else
        echo -e "${RED}ERROR: Unknown test group '$TEST_GROUP'${NC}"
        echo ""
        echo "Available test groups:"
        for group in tpch tpcds functions aggregations window all; do
            echo "  $group - ${TEST_DESCRIPTIONS[$group]}"
        done
        exit 1
    fi
fi

TEST_FILES="${TEST_GROUPS[$TEST_GROUP]}"

# ------------------------------------------------------------------------------
# Run tests
# ------------------------------------------------------------------------------
echo ""
echo -e "${BLUE}[4/4] Running tests...${NC}"
echo ""
echo -e "  ${CYAN}Test group:${NC} $TEST_GROUP (${TEST_DESCRIPTIONS[$TEST_GROUP]})"
echo -e "  ${CYAN}Test files:${NC} $TEST_FILES"
if [ -n "$PYTEST_ARGS" ]; then
    echo -e "  ${CYAN}Pytest args:${NC} $PYTEST_ARGS"
fi
echo ""

cd "$WORKSPACE_DIR/tests/integration"

# Export SPARK_HOME for the tests
export SPARK_HOME

# Run pytest with all test files
# shellcheck disable=SC2086
python -m pytest \
    $TEST_FILES \
    -v \
    --tb=short \
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
