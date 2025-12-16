#!/usr/bin/env bash
# Stop Apache Spark 4.0.1 Connect reference server
#
# Compatible with both bash and zsh

SPARK_HOME="${SPARK_HOME:-/home/vscode/spark/current}"
SPARK_PORT=15003

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Stopping Apache Spark Connect Reference Server (port ${SPARK_PORT})...${NC}"

# Find and kill the Spark Connect server process
PIDS=$(pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer")

if [ -z "$PIDS" ]; then
    echo -e "${GREEN}No Spark Connect server running${NC}"
    exit 0
fi

# Try graceful shutdown first
if [ -f "$SPARK_HOME/sbin/stop-connect-server.sh" ]; then
    "$SPARK_HOME/sbin/stop-connect-server.sh"
    sleep 2
fi

# Check if still running
PIDS=$(pgrep -f "org.apache.spark.sql.connect.service.SparkConnectServer")
if [ -n "$PIDS" ]; then
    echo "Forcefully killing processes: $PIDS"
    kill -9 $PIDS 2>/dev/null || true
    sleep 1
fi

echo -e "${GREEN}✓ Spark Connect reference server stopped${NC}"
