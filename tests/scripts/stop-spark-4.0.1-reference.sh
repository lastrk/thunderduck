#!/usr/bin/env bash
# Stop Apache Spark 4.1.1 Connect reference server
#
# Compatible with both bash and zsh
# Reads SPARK_PORT from env (default: 15003) and only kills the process on that port.

SPARK_HOME="${SPARK_HOME:-/home/vscode/spark/current}"
SPARK_PORT="${SPARK_PORT:-15003}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Stopping Apache Spark Connect Reference Server (port ${SPARK_PORT})...${NC}"

# Find processes listening on our specific port
PIDS=$(lsof -ti :${SPARK_PORT} 2>/dev/null)

if [ -z "$PIDS" ]; then
    echo -e "${GREEN}No process listening on port ${SPARK_PORT}${NC}"
    exit 0
fi

# Try graceful shutdown first via Spark's stop script
if [ -f "$SPARK_HOME/sbin/stop-connect-server.sh" ]; then
    "$SPARK_HOME/sbin/stop-connect-server.sh" 2>/dev/null
    sleep 2
fi

# Check if still running on our port
PIDS=$(lsof -ti :${SPARK_PORT} 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "Forcefully killing processes on port ${SPARK_PORT}: $PIDS"
    kill -9 $PIDS 2>/dev/null || true
    sleep 1
fi

echo -e "${GREEN}✓ Spark Connect reference server stopped (port ${SPARK_PORT})${NC}"
