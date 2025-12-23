#!/usr/bin/env python3
"""
Playground setup utilities for Thunderduck interactive notebooks.

Provides health checking and environment validation for the playground launcher.
Can be called from bash scripts via CLI interface.
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Tuple


def get_project_root() -> Path:
    """Get the Thunderduck project root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pom.xml").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find project root (no pom.xml found)")


def wait_for_port(host: str, port: int, timeout: int = 60) -> bool:
    """
    Wait for a TCP port to become available.

    Args:
        host: Hostname to connect to
        port: Port number to check
        timeout: Maximum time to wait in seconds

    Returns:
        True if port became available, False on timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex((host, port))
                if result == 0:
                    # Port is open, give it a moment to fully initialize
                    time.sleep(2)
                    return True
        except (socket.error, socket.timeout):
            pass
        time.sleep(1)
    return False


def check_server_health(url: str, timeout: int = 30) -> Tuple[bool, Optional[str]]:
    """
    Check if a Spark Connect server is responding to queries.

    Args:
        url: Spark Connect URL (e.g., sc://localhost:15002)
        timeout: Maximum time to wait in seconds

    Returns:
        Tuple of (success, error_message)
    """
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.remote(url).getOrCreate()
        result = spark.sql("SELECT 1 as health_check").collect()
        spark.stop()

        if result and result[0][0] == 1:
            return True, None
        else:
            return False, "Health check query returned unexpected result"
    except Exception as e:
        return False, str(e)


def is_port_in_use(host: str, port: int) -> bool:
    """Check if a port is currently in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except OSError:
            return True


def kill_process_on_port(port: int) -> bool:
    """
    Kill any process listening on the specified port.

    Args:
        port: Port number

    Returns:
        True if process was killed or port was free, False on error
    """
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    print(f"Killed process {pid} on port {port}")
                except ProcessLookupError:
                    pass
            time.sleep(1)
            return True
        return True  # Port was already free
    except FileNotFoundError:
        # lsof not available
        print("Warning: lsof not available, cannot kill process")
        return False


def validate_data_directories() -> dict:
    """
    Check that required data directories exist.

    Returns:
        Dict with 'tpch' and 'tpcds' paths (or None if not found)
    """
    project_root = get_project_root()

    tpch_dir = project_root / "tests/integration/tpch_sf001"
    tpcds_dir = project_root / "data/tpcds_sf1"

    return {
        'tpch': tpch_dir if tpch_dir.exists() else None,
        'tpcds': tpcds_dir if tpcds_dir.exists() else None
    }


def print_status(message: str, success: bool = True):
    """Print a status message with checkmark or X."""
    symbol = "✓" if success else "✗"
    print(f"{symbol} {message}")


def main():
    """CLI interface for bash script integration."""
    parser = argparse.ArgumentParser(description="Playground setup utilities")
    parser.add_argument(
        "--wait-port",
        type=int,
        metavar="PORT",
        help="Wait for port to become available"
    )
    parser.add_argument(
        "--check-health",
        type=str,
        metavar="URL",
        help="Check server health (e.g., sc://localhost:15002)"
    )
    parser.add_argument(
        "--kill-port",
        type=int,
        metavar="PORT",
        help="Kill process on port"
    )
    parser.add_argument(
        "--check-port",
        type=int,
        metavar="PORT",
        help="Check if port is in use"
    )
    parser.add_argument(
        "--project-root",
        action="store_true",
        help="Print project root path"
    )
    parser.add_argument(
        "--validate-data",
        action="store_true",
        help="Validate data directories exist"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host for port operations (default: localhost)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout in seconds (default: 60)"
    )

    args = parser.parse_args()

    if args.wait_port:
        if wait_for_port(args.host, args.wait_port, args.timeout):
            print_status(f"Port {args.wait_port} is available")
            sys.exit(0)
        else:
            print_status(f"Timeout waiting for port {args.wait_port}", success=False)
            sys.exit(1)

    elif args.check_health:
        success, error = check_server_health(args.check_health, args.timeout)
        if success:
            print_status(f"Server at {args.check_health} is healthy")
            sys.exit(0)
        else:
            print_status(f"Server health check failed: {error}", success=False)
            sys.exit(1)

    elif args.kill_port:
        if kill_process_on_port(args.kill_port):
            print_status(f"Port {args.kill_port} cleared")
            sys.exit(0)
        else:
            print_status(f"Failed to clear port {args.kill_port}", success=False)
            sys.exit(1)

    elif args.check_port:
        if is_port_in_use(args.host, args.check_port):
            print(f"Port {args.check_port} is in use")
            sys.exit(1)
        else:
            print(f"Port {args.check_port} is available")
            sys.exit(0)

    elif args.project_root:
        print(get_project_root())
        sys.exit(0)

    elif args.validate_data:
        data = validate_data_directories()
        if data['tpch']:
            print_status(f"TPC-H data found at {data['tpch']}")
        else:
            print_status("TPC-H data not found", success=False)
        if data['tpcds']:
            print_status(f"TPC-DS data found at {data['tpcds']}")
        else:
            print_status("TPC-DS data not found (optional)", success=False)
        sys.exit(0 if data['tpch'] else 1)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
