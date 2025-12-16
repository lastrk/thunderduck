#!/usr/bin/env bash
# Start Spark Connect Server with required JVM args for Apache Arrow
# Run from project root: ./tests/scripts/start-server.sh
#
# Compatible with both bash and zsh

# Get the directory where this script is located
if [ -n "$ZSH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
elif [ -n "$BASH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
else
    # Fallback for POSIX shells
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Change to project root
cd "$PROJECT_ROOT"

export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED "

echo "Starting Spark Connect Server..."
echo "Note: This script includes JVM args required for Apache Arrow (all platforms)"
echo ""

mvn exec:java -pl connect-server \
    -Dexec.mainClass="com.thunderduck.connect.server.SparkConnectServer"
