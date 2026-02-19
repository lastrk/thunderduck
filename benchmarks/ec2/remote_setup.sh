#!/bin/bash
# Remote Setup Script for EC2 Benchmark Instances
#
# This script runs on EC2 instances to prepare the environment for benchmarking.
# It installs Java, Python, DuckDB, and optionally Spark.
#
# Usage:
#   ./remote_setup.sh thunderduck          # Setup for Thunderduck
#   ./remote_setup.sh spark-3.5.3 3.5.3    # Setup for Spark 3.5.3
#   ./remote_setup.sh spark-4.1.1 4.1.1    # Setup for Spark 4.1.1

set -e

ENGINE="${1:-thunderduck}"
SPARK_VERSION="${2:-}"

echo "=============================================="
echo "Setting up EC2 instance for: $ENGINE"
echo "=============================================="

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS="unknown"
fi

echo "Detected OS: $OS"

# Install packages based on OS
install_packages() {
    case $OS in
        amzn|amazon)
            # Amazon Linux
            sudo yum update -y
            sudo yum install -y java-17-amazon-corretto-devel python3 python3-pip aws-cli tar gzip
            ;;
        ubuntu)
            # Ubuntu
            sudo apt-get update
            sudo apt-get install -y openjdk-17-jdk python3 python3-pip awscli tar gzip
            ;;
        *)
            echo "Warning: Unknown OS $OS, attempting generic install"
            ;;
    esac
}

# Install Python packages
install_python_packages() {
    pip3 install --user pyspark duckdb pyarrow pandas
}

# Install DuckDB CLI
install_duckdb() {
    if ! command -v duckdb &> /dev/null; then
        echo "Installing DuckDB CLI..."
        ARCH=$(uname -m)
        case $ARCH in
            x86_64)
                DUCKDB_ARCH="amd64"
                ;;
            aarch64)
                DUCKDB_ARCH="aarch64"
                ;;
            *)
                echo "Unknown architecture: $ARCH"
                return 1
                ;;
        esac

        DUCKDB_VERSION="1.4.4"
        DUCKDB_URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${DUCKDB_ARCH}.zip"

        curl -L -o /tmp/duckdb.zip "$DUCKDB_URL"
        unzip -o /tmp/duckdb.zip -d /tmp
        sudo mv /tmp/duckdb /usr/local/bin/
        rm /tmp/duckdb.zip

        echo "DuckDB installed: $(duckdb --version)"
    else
        echo "DuckDB already installed: $(duckdb --version)"
    fi
}

# Install Spark
install_spark() {
    local version="$1"
    local spark_home="/opt/spark-${version}"

    if [ -d "$spark_home" ]; then
        echo "Spark $version already installed at $spark_home"
        return 0
    fi

    echo "Installing Spark $version..."

    # Determine Scala version based on Spark version
    local scala_version="2.13"
    if [[ "$version" == "3.5"* ]]; then
        scala_version="2.13"
    elif [[ "$version" == "4."* ]]; then
        scala_version="2.13"
    fi

    local spark_tarball="spark-${version}-bin-hadoop3-scala${scala_version}.tgz"
    local spark_url="https://dlcdn.apache.org/spark/spark-${version}/${spark_tarball}"

    # Try download from Apache mirror
    if ! curl -L -o "/tmp/${spark_tarball}" "$spark_url" 2>/dev/null; then
        # Fallback to archive
        spark_url="https://archive.apache.org/dist/spark/spark-${version}/${spark_tarball}"
        curl -L -o "/tmp/${spark_tarball}" "$spark_url"
    fi

    sudo tar -xzf "/tmp/${spark_tarball}" -C /opt
    sudo mv "/opt/spark-${version}-bin-hadoop3-scala${scala_version}" "$spark_home"
    rm "/tmp/${spark_tarball}"

    # Set environment
    echo "export SPARK_HOME=$spark_home" >> ~/.bashrc
    echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc

    echo "Spark $version installed at $spark_home"
}

# Setup Thunderduck
setup_thunderduck() {
    echo "Setting up Thunderduck..."

    # The JAR should be uploaded by the driver script
    if [ -f /tmp/thunderduck.jar ]; then
        sudo mkdir -p /opt/thunderduck
        sudo mv /tmp/thunderduck.jar /opt/thunderduck/
        echo "Thunderduck JAR installed at /opt/thunderduck/thunderduck.jar"
    else
        echo "Warning: Thunderduck JAR not found at /tmp/thunderduck.jar"
    fi
}

# Setup Spark
setup_spark() {
    local version="$1"

    if [ -z "$version" ]; then
        echo "Error: Spark version required"
        return 1
    fi

    install_spark "$version"
}

# Start Thunderduck Connect Server
start_thunderduck_server() {
    echo "Starting Thunderduck Connect Server..."

    # Kill any existing server
    pkill -f thunderduck.jar || true
    sleep 2

    # Start server
    nohup java \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        -jar /opt/thunderduck/thunderduck.jar \
        > /tmp/thunderduck.log 2>&1 &

    # Wait for server to start
    echo "Waiting for Thunderduck server to start..."
    for i in {1..30}; do
        if netstat -tlnp 2>/dev/null | grep -q ":15002"; then
            echo "Thunderduck server started on port 15002"
            return 0
        fi
        sleep 1
    done

    echo "Warning: Thunderduck server may not have started"
    cat /tmp/thunderduck.log
    return 1
}

# Start Spark Connect Server
start_spark_server() {
    local version="$1"
    local spark_home="/opt/spark-${version}"

    echo "Starting Spark Connect Server (version $version)..."

    # Kill any existing server
    pkill -f "spark-connect" || true
    sleep 2

    # Configure Spark for local mode
    export SPARK_HOME="$spark_home"

    # Start Spark Connect server
    nohup "$spark_home/sbin/start-connect-server.sh" \
        --packages org.apache.spark:spark-connect_2.13:${version} \
        --conf spark.master=local[*] \
        --conf spark.driver.memory=8g \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.connect.grpc.binding.port=15002 \
        > /tmp/spark-connect.log 2>&1 &

    # Wait for server to start
    echo "Waiting for Spark Connect server to start..."
    for i in {1..60}; do
        if netstat -tlnp 2>/dev/null | grep -q ":15002"; then
            echo "Spark Connect server started on port 15002"
            return 0
        fi
        sleep 1
    done

    echo "Warning: Spark Connect server may not have started"
    cat /tmp/spark-connect.log
    return 1
}

# Main setup
main() {
    echo "Installing packages..."
    install_packages

    echo "Installing Python packages..."
    install_python_packages

    echo "Installing DuckDB..."
    install_duckdb

    case $ENGINE in
        thunderduck)
            setup_thunderduck
            ;;
        spark-*)
            if [ -z "$SPARK_VERSION" ]; then
                SPARK_VERSION="${ENGINE#spark-}"
            fi
            setup_spark "$SPARK_VERSION"
            ;;
        *)
            echo "Unknown engine: $ENGINE"
            exit 1
            ;;
    esac

    echo ""
    echo "=============================================="
    echo "Setup complete for: $ENGINE"
    echo "=============================================="

    # Print environment info
    echo "Java version:"
    java -version 2>&1 | head -1

    echo "Python version:"
    python3 --version

    echo "DuckDB version:"
    duckdb --version 2>/dev/null || echo "DuckDB not installed"
}

main "$@"
