#!/usr/bin/env bash
# Start Apache Spark 4.0.1 Connect server on port 15003 for differential testing
#
# Compatible with both bash and zsh

set -e

SPARK_HOME="${SPARK_HOME:-/home/vscode/spark/current}"
SPARK_VERSION="4.0.1"
SPARK_PORT=15003

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}Starting Apache Spark ${SPARK_VERSION} Connect Reference Server${NC}"
echo -e "${BLUE}================================================================${NC}"

# Check if Spark is installed
if [ ! -d "$SPARK_HOME" ]; then
    echo -e "${RED}ERROR: Spark not found at $SPARK_HOME${NC}"
    exit 1
fi

echo -e "${GREEN}Using Spark at: $SPARK_HOME${NC}"

# Check if server is already running
if pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer.*${SPARK_PORT}" > /dev/null; then
    echo -e "${GREEN}✓ Spark Connect server already running on port ${SPARK_PORT}${NC}"
    exit 0
fi

# Check if port is in use
if lsof -Pi :${SPARK_PORT} -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${RED}ERROR: Port ${SPARK_PORT} is already in use${NC}"
    exit 1
fi

# Create log directory
SPARK_LOG_DIR="${SPARK_HOME}/work/logs"
mkdir -p "$SPARK_LOG_DIR"

echo -e "${BLUE}Starting Spark Connect server on port ${SPARK_PORT}...${NC}"

# Set required JVM options for Apache Arrow (Spark 4.0.x requirement)
export SPARK_SUBMIT_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"

# Start Spark Connect server (no pipe to avoid subprocess issues)
"$SPARK_HOME/sbin/start-connect-server.sh" \
    --master "local[*]" \
    --driver-memory 4g \
    --conf spark.driver.host=localhost \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.connect.grpc.binding.port=${SPARK_PORT} \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4041 \
    > "${SPARK_LOG_DIR}/start.log" 2>&1

# Wait for server to start
echo -e "${BLUE}Waiting for server to start...${NC}"
MAX_WAIT=60
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer" > /dev/null; then
        # Check if port is listening
        if lsof -Pi :${SPARK_PORT} -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Spark Connect server started successfully${NC}"
            echo -e "${GREEN}  - Port: ${SPARK_PORT}${NC}"
            echo -e "${GREEN}  - URL: sc://localhost:${SPARK_PORT}${NC}"
            echo -e "${GREEN}  - Spark UI: http://localhost:4041${NC}"
            echo -e "${BLUE}================================================================${NC}"
            exit 0
        fi
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done

echo -e "${RED}✗ Failed to start Spark Connect server within ${MAX_WAIT}s${NC}"
echo -e "${RED}Check logs at: ${SPARK_LOG_DIR}/start.log${NC}"
exit 1
