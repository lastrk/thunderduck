#!/bin/bash
# Start Spark Connect Server with required JVM args for Apache Arrow
# Run from project root: ./tests/scripts/start-server.sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED "

echo "Starting Spark Connect Server..."
echo "Note: This script includes JVM args required for Apache Arrow (all platforms)"
echo ""

mvn exec:java -pl connect-server \
    -Dexec.mainClass="com.thunderduck.connect.server.SparkConnectServer"
