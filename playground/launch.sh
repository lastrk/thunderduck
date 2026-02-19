#!/usr/bin/env bash
#
# Thunderduck Interactive Playground Launcher
#
# Starts both Thunderduck and Spark reference servers, then launches
# a Marimo notebook for interactive comparison.
#
# Usage: ./playground/launch.sh [bench] [OPTIONS]
#
# Positional:
#   bench              Launch the TPC-DS compute benchmark notebook instead of the playground
#
# Options:
#   --no-build         Skip Maven build even if JAR missing
#   --rebuild          Force rebuild even if JAR exists
#   --force            Kill existing processes on ports
#   --thunderduck-only Only start Thunderduck (skip Spark reference)
#   --help             Show this help message
#

set -e

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

THUNDERDUCK_PORT=15002
SPARK_PORT=15003
MARIMO_PORT=2718
BENCH_THREADS=$(nproc)
TOTAL_MEM_MB=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 16384)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m'

# ------------------------------------------------------------------------------
# Resolve paths
# ------------------------------------------------------------------------------

# Get script directory (works in bash and zsh)
if [ -n "$ZSH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
elif [ -n "$BASH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PLAYGROUND_DIR="$SCRIPT_DIR"
VENV_DIR="$PLAYGROUND_DIR/.venv"
LOG_DIR="$PLAYGROUND_DIR/logs"

# JAR location
THUNDERDUCK_JAR="$PROJECT_ROOT/connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar"

# Data directories
TPCH_DATA_DIR="$PROJECT_ROOT/tests/integration/tpch_sf001"
TPCDS_DATA_DIR="$PLAYGROUND_DIR/data"  # TPC-DS data generated on demand under playground/data/

# PIDs for cleanup
THUNDERDUCK_PID=""
SPARK_STARTED=false

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------

NO_BUILD=false
FORCE_REBUILD=false
FORCE_KILL=false
THUNDERDUCK_ONLY=false
NOTEBOOK="thunderduck_playground.py"

while [[ $# -gt 0 ]]; do
    case $1 in
        bench)
            NOTEBOOK="tpch_benchmark.py"  # filename kept for compatibility
            shift
            ;;
        --no-build)
            NO_BUILD=true
            shift
            ;;
        --rebuild)
            FORCE_REBUILD=true
            shift
            ;;
        --force)
            FORCE_KILL=true
            shift
            ;;
        --thunderduck-only)
            THUNDERDUCK_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Thunderduck Interactive Playground"
            echo ""
            echo "Usage: ./playground/launch.sh [bench] [OPTIONS]"
            echo ""
            echo "Positional:"
            echo "  bench              Launch the TPC-DS compute benchmark notebook"
            echo ""
            echo "Options:"
            echo "  --no-build         Skip Maven build even if JAR missing"
            echo "  --rebuild          Force rebuild even if JAR exists"
            echo "  --force            Kill existing processes on ports"
            echo "  --thunderduck-only Only start Thunderduck (skip Spark reference)"
            echo "  --help             Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# ------------------------------------------------------------------------------
# Cleanup function
# ------------------------------------------------------------------------------

cleanup() {
    echo ""
    echo -e "${BLUE}Shutting down servers...${NC}"

    # Kill Thunderduck
    if [ -n "$THUNDERDUCK_PID" ] && kill -0 "$THUNDERDUCK_PID" 2>/dev/null; then
        echo "Stopping Thunderduck (PID: $THUNDERDUCK_PID)..."
        kill -TERM "$THUNDERDUCK_PID" 2>/dev/null || true
        sleep 2
        kill -9 "$THUNDERDUCK_PID" 2>/dev/null || true
    fi

    # Kill Spark reference
    if [ "$SPARK_STARTED" = true ]; then
        echo "Stopping Spark reference server..."
        "$PROJECT_ROOT/tests/scripts/stop-spark-4.1.1-reference.sh" 2>/dev/null || true
    fi

    # Force cleanup any remaining processes
    pkill -9 -f "thunderduck-connect-server" 2>/dev/null || true

    echo -e "${GREEN}Cleanup complete${NC}"
}

trap cleanup EXIT INT TERM

# ------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================${NC}"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}✗ $1 is not installed${NC}"
        echo "  Please install $1 to continue"
        return 1
    fi
    return 0
}

# ------------------------------------------------------------------------------
# Step 1: Check prerequisites
# ------------------------------------------------------------------------------

check_prerequisites() {
    print_header "Checking Prerequisites"

    local failed=false

    # Check Java
    if check_command java; then
        java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
        if [ "$java_version" -ge 17 ] 2>/dev/null; then
            echo -e "${GREEN}✓ Java $java_version found${NC}"
        else
            echo -e "${RED}✗ Java 17+ required (found: $java_version)${NC}"
            failed=true
        fi
    else
        failed=true
    fi

    # Check Python
    if check_command python3; then
        python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
        echo -e "${GREEN}✓ Python $python_version found${NC}"
    else
        failed=true
    fi

    # Check Maven (only if we might need to build)
    if [ "$NO_BUILD" = false ]; then
        if check_command mvn; then
            echo -e "${GREEN}✓ Maven found${NC}"
        else
            echo -e "${YELLOW}! Maven not found - will skip build${NC}"
        fi
    fi

    if [ "$failed" = true ]; then
        echo -e "${RED}Prerequisites check failed${NC}"
        exit 1
    fi
}

# ------------------------------------------------------------------------------
# Step 2: Ensure git submodules are initialized
# ------------------------------------------------------------------------------

ensure_submodules() {
    print_header "Checking Git Submodules"

    if [ -f "$PROJECT_ROOT/thunderduck-duckdb-extension/CMakeLists.txt" ]; then
        echo -e "${GREEN}✓ Submodules already initialized${NC}"
    else
        echo "Initializing git submodules..."
        cd "$PROJECT_ROOT"
        git submodule update --init --recursive
        if [ -f "$PROJECT_ROOT/thunderduck-duckdb-extension/CMakeLists.txt" ]; then
            echo -e "${GREEN}✓ Submodules initialized${NC}"
        else
            echo -e "${YELLOW}! Submodule init failed — extension build will be skipped${NC}"
        fi
    fi
}

# ------------------------------------------------------------------------------
# Step 3: Setup Python virtual environment
# ------------------------------------------------------------------------------

ensure_python_deps() {
    print_header "Setting up Python Environment"

    # Create venv if it doesn't exist
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment at $VENV_DIR..."
        python3 -m venv "$VENV_DIR"
    fi

    # Activate venv
    source "$VENV_DIR/bin/activate"

    # Install/upgrade dependencies
    echo "Installing Python dependencies..."
    pip install -q --upgrade pip
    pip install -q -r "$PLAYGROUND_DIR/requirements.txt"

    echo -e "${GREEN}✓ Python environment ready${NC}"
}

# ------------------------------------------------------------------------------
# Step 4: Build Thunderduck if needed
# ------------------------------------------------------------------------------

can_build_extension() {
    # Extension build requires cmake, a C++ compiler, and initialized submodule
    if ! command -v cmake &> /dev/null; then
        return 1
    fi
    if ! command -v c++ &> /dev/null && ! command -v g++ &> /dev/null; then
        return 1
    fi
    if [ ! -f "$PROJECT_ROOT/thunderduck-duckdb-extension/CMakeLists.txt" ]; then
        return 1
    fi
    return 0
}

build_if_needed() {
    print_header "Checking Thunderduck Build"

    if [ "$FORCE_REBUILD" = true ]; then
        echo "Force rebuild requested..."
        cd "$PROJECT_ROOT"
        if can_build_extension; then
            echo "C++ toolchain found — building with Spark compatibility extension..."
            mvn clean package -DskipTests -Pbuild-extension
            echo -e "${GREEN}✓ Build complete (strict mode enabled)${NC}"
        else
            echo "No C++ toolchain — building without extension (relaxed mode only)..."
            mvn clean package -DskipTests -pl connect-server -am
            echo -e "${GREEN}✓ Build complete (relaxed mode only)${NC}"
        fi
        return
    fi

    if [ -f "$THUNDERDUCK_JAR" ]; then
        echo -e "${GREEN}✓ Thunderduck JAR found${NC}"
        return
    fi

    if [ "$NO_BUILD" = true ]; then
        echo -e "${RED}✗ JAR not found and --no-build specified${NC}"
        echo "  Expected: $THUNDERDUCK_JAR"
        echo "  Run: mvn package -DskipTests -pl connect-server -am"
        exit 1
    fi

    echo "Building Thunderduck (this may take a minute)..."
    cd "$PROJECT_ROOT"
    if can_build_extension; then
        echo "C++ toolchain found — building with Spark compatibility extension..."
        mvn clean package -DskipTests -Pbuild-extension
        echo -e "${GREEN}✓ Build complete (strict mode enabled)${NC}"
    else
        echo "No C++ toolchain — building without extension (relaxed mode only)..."
        echo "  Install cmake + C++ compiler for strict Spark type parity"
        mvn package -DskipTests -pl connect-server -am
        if [ -f "$THUNDERDUCK_JAR" ]; then
            echo -e "${GREEN}✓ Build complete (relaxed mode only)${NC}"
        else
            echo -e "${RED}✗ Build failed - JAR not found${NC}"
            exit 1
        fi
    fi
}

# ------------------------------------------------------------------------------
# Step 5: Check and clear ports
# ------------------------------------------------------------------------------

check_ports() {
    print_header "Checking Ports"

    # Activate venv for Python utility
    source "$VENV_DIR/bin/activate"

    # Check Thunderduck port
    if python3 "$PLAYGROUND_DIR/setup_playground.py" --check-port $THUNDERDUCK_PORT 2>/dev/null; then
        echo -e "${GREEN}✓ Port $THUNDERDUCK_PORT (Thunderduck) available${NC}"
    else
        if [ "$FORCE_KILL" = true ]; then
            echo -e "${YELLOW}! Port $THUNDERDUCK_PORT in use, killing process...${NC}"
            python3 "$PLAYGROUND_DIR/setup_playground.py" --kill-port $THUNDERDUCK_PORT
        else
            echo -e "${RED}✗ Port $THUNDERDUCK_PORT is in use${NC}"
            echo "  Use --force to kill existing process"
            exit 1
        fi
    fi

    # Check Spark port (if needed)
    if [ "$THUNDERDUCK_ONLY" = false ]; then
        if python3 "$PLAYGROUND_DIR/setup_playground.py" --check-port $SPARK_PORT 2>/dev/null; then
            echo -e "${GREEN}✓ Port $SPARK_PORT (Spark) available${NC}"
        else
            if [ "$FORCE_KILL" = true ]; then
                echo -e "${YELLOW}! Port $SPARK_PORT in use, killing process...${NC}"
                python3 "$PLAYGROUND_DIR/setup_playground.py" --kill-port $SPARK_PORT
            else
                echo -e "${RED}✗ Port $SPARK_PORT is in use${NC}"
                echo "  Use --force to kill existing process"
                exit 1
            fi
        fi
    fi
}

# ------------------------------------------------------------------------------
# Step 6: Start Thunderduck server
# ------------------------------------------------------------------------------

start_thunderduck() {
    print_header "Starting Thunderduck Server"

    mkdir -p "$LOG_DIR"

    # JVM flags for Apache Arrow — no artificial resource caps
    local JAVA_OPTS="
        -Xmx${TOTAL_MEM_MB}m
        -XX:+UseG1GC
        -XX:MaxGCPauseMillis=200
        --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
        --add-opens=java.base/java.nio=ALL-UNNAMED
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    "

    echo "Starting Thunderduck on port $THUNDERDUCK_PORT..."

    java $JAVA_OPTS -jar "$THUNDERDUCK_JAR" \
        > "$LOG_DIR/thunderduck.log" 2>&1 &

    THUNDERDUCK_PID=$!
    echo "Started with PID: $THUNDERDUCK_PID"

    # Wait for server to be ready
    source "$VENV_DIR/bin/activate"
    if python3 "$PLAYGROUND_DIR/setup_playground.py" --wait-port $THUNDERDUCK_PORT --timeout 60; then
        echo -e "${GREEN}✓ Thunderduck ready on sc://localhost:$THUNDERDUCK_PORT${NC}"
    else
        echo -e "${RED}✗ Thunderduck failed to start${NC}"
        echo "Check logs at: $LOG_DIR/thunderduck.log"
        tail -20 "$LOG_DIR/thunderduck.log" 2>/dev/null || true
        exit 1
    fi
}

# ------------------------------------------------------------------------------
# Step 7: Start Spark reference server
# ------------------------------------------------------------------------------

start_spark_reference() {
    if [ "$THUNDERDUCK_ONLY" = true ]; then
        echo -e "${YELLOW}Skipping Spark reference server (--thunderduck-only)${NC}"
        return
    fi

    print_header "Starting Spark Reference Server"

    # Check if Spark is installed
    SPARK_HOME="${SPARK_HOME:-$HOME/spark/current}"
    if [ ! -d "$SPARK_HOME" ]; then
        echo -e "${YELLOW}! Spark not found at $SPARK_HOME${NC}"
        echo "  Skipping Spark reference server"
        echo "  To install: ./tests/scripts/setup-differential-testing.sh"
        return
    fi

    # No artificial resource caps — let Spark use all available hardware
    export SPARK_MASTER="local[${BENCH_THREADS}]"
    export SPARK_DRIVER_MEMORY="${TOTAL_MEM_MB}m"

    echo "Starting Spark reference on port $SPARK_PORT (${SPARK_DRIVER_MEMORY} driver memory, local[${BENCH_THREADS}])..."

    export SPARK_AQE_ENABLED=true
    export SPARK_BROADCAST_THRESHOLD=10485760  # 10MB (Spark default)

    "$PROJECT_ROOT/tests/scripts/start-spark-4.1.1-reference.sh"
    SPARK_STARTED=true

    echo -e "${GREEN}✓ Spark reference ready on sc://localhost:$SPARK_PORT${NC}"
}

# ------------------------------------------------------------------------------
# Step 8: Validate data directories
# ------------------------------------------------------------------------------

validate_data() {
    print_header "Checking Data Directories"

    if [ -d "$TPCH_DATA_DIR" ]; then
        echo -e "${GREEN}✓ TPC-H data found at $TPCH_DATA_DIR${NC}"
    else
        echo -e "${YELLOW}! TPC-H data not found (optional — playground notebook uses it)${NC}"
    fi

    # Check for any pre-generated TPC-DS data (generated on demand by benchmark notebook)
    local tpcds_found=false
    for sf_dir in "$TPCDS_DATA_DIR"/tpcds_sf*; do
        if [ -d "$sf_dir" ] && [ -f "$sf_dir/store_sales.parquet" ]; then
            echo -e "${GREEN}✓ TPC-DS data found at $sf_dir${NC}"
            tpcds_found=true
            break
        fi
    done
    if [ "$tpcds_found" = false ]; then
        echo -e "${YELLOW}! TPC-DS data not found (will be generated on first benchmark run)${NC}"
    fi
}

# ------------------------------------------------------------------------------
# Step 9: Launch Marimo notebook
# ------------------------------------------------------------------------------

launch_marimo() {
    print_header "Launching Marimo Notebook"

    # Activate venv
    source "$VENV_DIR/bin/activate"

    # Set environment variables for the notebook
    export THUNDERDUCK_URL="sc://localhost:$THUNDERDUCK_PORT"
    export SPARK_URL="sc://localhost:$SPARK_PORT"
    export TPCH_DATA_DIR="$TPCH_DATA_DIR"
    export TPCDS_DATA_DIR="$TPCDS_DATA_DIR"
    export BENCH_THREADS="$BENCH_THREADS"

    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  Thunderduck Playground is ready!                              ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║  Thunderduck: sc://localhost:$THUNDERDUCK_PORT                          ║${NC}"
    if [ "$THUNDERDUCK_ONLY" = false ] && [ "$SPARK_STARTED" = true ]; then
    echo -e "${GREEN}║  Spark:       sc://localhost:$SPARK_PORT                          ║${NC}"
    fi
    echo -e "${GREEN}║                                                                ║${NC}"
    echo -e "${GREEN}║  Opening notebook at: http://localhost:$MARIMO_PORT                  ║${NC}"
    echo -e "${GREEN}║                                                                ║${NC}"
    echo -e "${GREEN}║  Press Ctrl+C to stop all servers and exit                     ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # Launch Marimo
    echo -e "${BLUE}Notebook: $NOTEBOOK${NC}"
    marimo edit "$PLAYGROUND_DIR/$NOTEBOOK" --port $MARIMO_PORT
}

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

main() {
    print_header "Thunderduck Interactive Playground"

    check_prerequisites
    ensure_submodules
    ensure_python_deps
    build_if_needed
    check_ports
    start_thunderduck
    start_spark_reference
    validate_data
    launch_marimo
}

main
