#!/usr/bin/env bash
# Setup script for Differential Testing V2
# Creates a reproducible environment for running differential tests
# between Apache Spark 4.0.1 and Thunderduck
#
# Compatible with both bash and zsh

set -e

# Detect shell and get script directory
if [ -n "$ZSH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
elif [ -n "$BASH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    # Fallback for POSIX shells
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
SPARK_VERSION="4.0.1"
SPARK_INSTALL_DIR="${SPARK_INSTALL_DIR:-$HOME/spark}"
SPARK_HOME="$SPARK_INSTALL_DIR/spark-$SPARK_VERSION"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Differential Testing V2 - Environment Setup${NC}"
echo -e "${BLUE}================================================================${NC}"
echo ""

# ------------------------------------------------------------------------------
# Step 1: Check Java
# ------------------------------------------------------------------------------
echo -e "${BLUE}[1/5] Checking Java...${NC}"

if ! command -v java &> /dev/null; then
    echo -e "${RED}ERROR: Java is not installed${NC}"
    echo "Please install Java 17+ first:"
    echo "  Ubuntu/Debian: sudo apt-get install openjdk-17-jdk"
    echo "  Fedora: sudo dnf install java-17-openjdk"
    echo "  macOS: brew install openjdk@17"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}' | cut -d'.' -f1)
if [ -z "$JAVA_VERSION" ]; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk '{print $3}' | tr -d '"' | cut -d'.' -f1)
fi

echo -e "${GREEN}  Java version: $(java -version 2>&1 | head -n 1)${NC}"

if [ "$JAVA_VERSION" -lt 17 ] 2>/dev/null; then
    echo -e "${YELLOW}  WARNING: Java 17+ recommended, found version $JAVA_VERSION${NC}"
fi

# ------------------------------------------------------------------------------
# Step 2: Check Python and pip
# ------------------------------------------------------------------------------
echo -e "${BLUE}[2/5] Checking Python and pip...${NC}"

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 is not installed${NC}"
    echo "Please install Python 3.8+ first"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo -e "${GREEN}  Python version: $PYTHON_VERSION${NC}"

if ! command -v pip3 &> /dev/null && ! python3 -m pip --version &> /dev/null; then
    echo -e "${RED}ERROR: pip is not installed${NC}"
    echo "Please install pip first:"
    echo "  Ubuntu/Debian: sudo apt-get install python3-pip"
    echo "  Fedora: sudo dnf install python3-pip"
    echo "  macOS: python3 -m ensurepip"
    exit 1
fi

PIP_VERSION=$(python3 -m pip --version 2>/dev/null | awk '{print $2}')
echo -e "${GREEN}  pip version: $PIP_VERSION${NC}"

# ------------------------------------------------------------------------------
# Step 3: Install Python dependencies
# ------------------------------------------------------------------------------
echo -e "${BLUE}[3/5] Installing Python dependencies...${NC}"

PYSPARK_VERSION="4.0.1"

echo "  Installing pytest, pyspark, pandas, pyarrow, grpcio..."
python3 -m pip install --quiet --upgrade pip
python3 -m pip install --quiet \
    pytest \
    pytest-timeout \
    "pyspark==$PYSPARK_VERSION" \
    pandas \
    "pyarrow>=11.0.0" \
    "grpcio>=1.48.1" \
    "grpcio-status>=1.48.1" \
    "googleapis-common-protos>=1.56.4"

# Verify PySpark installation
PYSPARK_INSTALLED=$(python3 -m pip show pyspark 2>/dev/null | grep Version | awk '{print $2}')
if [ -z "$PYSPARK_INSTALLED" ]; then
    echo -e "${RED}ERROR: PySpark installation failed${NC}"
    exit 1
fi
echo -e "${GREEN}  PySpark version: $PYSPARK_INSTALLED${NC}"

# ------------------------------------------------------------------------------
# Step 4: Download and Install Apache Spark
# ------------------------------------------------------------------------------
echo -e "${BLUE}[4/5] Setting up Apache Spark ${SPARK_VERSION}...${NC}"

if [ -d "$SPARK_HOME" ] && [ -f "$SPARK_HOME/bin/spark-submit" ]; then
    echo -e "${GREEN}  Spark already installed at: $SPARK_HOME${NC}"
else
    echo "  Downloading Apache Spark ${SPARK_VERSION}..."

    mkdir -p "$SPARK_INSTALL_DIR"
    SPARK_TARBALL="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
    SPARK_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"

    # Download to temp directory
    TEMP_DIR=$(mktemp -d)
    cd "$TEMP_DIR"

    if ! curl -fsSL "$SPARK_URL" -o "$SPARK_TARBALL"; then
        echo -e "${RED}ERROR: Failed to download Spark from $SPARK_URL${NC}"
        rm -rf "$TEMP_DIR"
        exit 1
    fi

    echo "  Extracting Spark..."
    tar -xzf "$SPARK_TARBALL"
    mv "spark-${SPARK_VERSION}-bin-hadoop3" "$SPARK_HOME"

    # Cleanup
    rm -rf "$TEMP_DIR"

    echo -e "${GREEN}  Spark installed at: $SPARK_HOME${NC}"
fi

# Create symlink for convenience
ln -sfn "$SPARK_HOME" "$SPARK_INSTALL_DIR/current"

# Verify Spark installation
if ! "$SPARK_HOME/bin/spark-submit" --version &>/dev/null; then
    echo -e "${RED}ERROR: Spark installation verification failed${NC}"
    exit 1
fi

echo -e "${GREEN}  Spark version: $("$SPARK_HOME/bin/spark-submit" --version 2>&1 | grep -i version | head -1)${NC}"

# ------------------------------------------------------------------------------
# Step 5: Build Thunderduck
# ------------------------------------------------------------------------------
echo -e "${BLUE}[5/5] Building Thunderduck...${NC}"

cd "$WORKSPACE_DIR"

if [ ! -f "pom.xml" ]; then
    echo -e "${RED}ERROR: Not in Thunderduck workspace (pom.xml not found)${NC}"
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo -e "${RED}ERROR: Maven is not installed${NC}"
    echo "Please install Maven first"
    exit 1
fi

echo "  Running: mvn clean package -DskipTests"
mvn clean package -DskipTests -q

if [ ! -f "$WORKSPACE_DIR/connect-server/target/thunderduck-connect-server-"*".jar" ]; then
    echo -e "${RED}ERROR: Thunderduck build failed - JAR not found${NC}"
    exit 1
fi

echo -e "${GREEN}  Thunderduck built successfully${NC}"

# ------------------------------------------------------------------------------
# Write environment configuration
# ------------------------------------------------------------------------------
ENV_FILE="$WORKSPACE_DIR/tests/integration/.env"
cat > "$ENV_FILE" << EOF
# Differential Testing V2 Environment Configuration
# Generated by setup-differential-testing.sh on $(date)

export SPARK_HOME="$SPARK_HOME"
export SPARK_VERSION="$SPARK_VERSION"
export WORKSPACE_DIR="$WORKSPACE_DIR"
EOF

echo ""
echo -e "${GREEN}================================================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}================================================================${NC}"
echo ""
echo -e "Configuration saved to: ${BLUE}$ENV_FILE${NC}"
echo ""
echo -e "${YELLOW}To run differential tests:${NC}"
echo ""
echo "  cd $WORKSPACE_DIR/tests/integration"
echo "  python3 -m pytest differential/ -v"
echo ""
echo "  # Or use the run script:"
echo "  $SCRIPT_DIR/run-differential-tests-v2.sh"
echo ""
echo -e "${GREEN}================================================================${NC}"
