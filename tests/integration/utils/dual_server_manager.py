"""
Dual Server Manager for Differential Testing

Manages both Apache Spark Connect (reference) and Thunderduck Connect servers
for differential testing.
"""

import os
import shutil
import subprocess
import tempfile
import time
import socket
from pathlib import Path
from typing import Optional, Tuple
from server_manager import ServerManager
from port_utils import is_port_listening, wait_for_port as _wait_for_port


class DualServerManager:
    """Manages both Spark Connect servers for differential testing"""

    def __init__(
        self,
        thunderduck_port: int = 15002,
        spark_reference_port: int = 15003,
        compat_mode: Optional[str] = None
    ):
        """
        Initialize dual server manager

        Args:
            thunderduck_port: Port for Thunderduck server (default 15002)
            spark_reference_port: Port for Spark reference server (default 15003)
            compat_mode: Spark compat mode ("strict", "relaxed", or None for auto)
        """
        self.thunderduck_port = thunderduck_port
        self.spark_reference_port = spark_reference_port
        self.workspace_dir = Path(__file__).parent.parent.parent.parent

        # Thunderduck server manager (existing)
        self.thunderduck_manager = ServerManager(
            host="localhost",
            port=thunderduck_port,
            compat_mode=compat_mode
        )

        # Spark Connect container name
        self.spark_container_name = "spark-connect-reference"
        self.spark_container_running = False

        # Create a unique temporary warehouse directory for this session.
        # This prevents LOCATION_ALREADY_EXISTS errors when a stale
        # spark-warehouse/ directory exists from a previous test run.
        self._spark_warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")

    def is_spark_process_running(self) -> bool:
        """Check if Spark Connect process is running"""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "org.apache.spark.sql.connect.service.SparkConnectServer"],
                capture_output=True,
                text=True
            )
            return result.returncode == 0 and len(result.stdout.strip()) > 0
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    def is_port_available(self, port: int) -> bool:
        """Check if a port is available"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", port))
                return True
            except OSError:
                return False

    def wait_for_port(self, port: int, timeout: int = 60) -> bool:
        """Wait for a port to be accepting connections"""
        return _wait_for_port(port, host='localhost', timeout=timeout)

    def start_spark_reference(self, timeout: int = 60) -> bool:
        """
        Start Apache Spark Connect reference server (native)

        Args:
            timeout: Maximum time to wait for server to start

        Returns:
            True if server started successfully
        """
        print("\n" + "=" * 80)
        print("Starting Apache Spark Connect Reference Server (Native)")
        print("=" * 80)

        # Check if process is already running
        if self.is_spark_process_running():
            print(f"✓ Spark Connect process already running")
            if self.wait_for_port(self.spark_reference_port, timeout=10):
                print(f"✓ Spark Connect server ready on port {self.spark_reference_port}")
                self.spark_container_running = True
                return True
            else:
                print("✗ Process running but server not responding, restarting...")
                self.stop_spark_reference()

        # Start the native Spark server
        script_path = self.workspace_dir / "tests/scripts/start-spark-4.0.1-reference.sh"
        if not script_path.exists():
            raise FileNotFoundError(f"Spark Connect start script not found: {script_path}")

        print(f"Starting Spark Connect server...")
        print(f"  Warehouse dir: {self._spark_warehouse_dir}")
        try:
            env = os.environ.copy()
            env['SPARK_PORT'] = str(self.spark_reference_port)
            env['SPARK_WAREHOUSE_DIR'] = self._spark_warehouse_dir
            result = subprocess.run(
                [str(script_path)],
                cwd=str(self.workspace_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env
            )

            if result.returncode != 0:
                print(f"✗ Failed to start Spark Connect server")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"✗ Timeout starting Spark Connect server")
            return False
        except Exception as e:
            print(f"✗ Error starting Spark Connect server: {e}")
            return False

        # Verify server is ready
        if self.wait_for_port(self.spark_reference_port, timeout):
            print(f"✓ Spark Connect reference server ready on port {self.spark_reference_port}")
            self.spark_container_running = True
            return True
        else:
            print(f"✗ Spark Connect server did not start within {timeout}s")
            return False

    def stop_spark_reference(self):
        """Stop Apache Spark Connect reference server"""
        if not self.spark_container_running and not self.is_spark_process_running():
            return

        print("\nStopping Apache Spark Connect reference server...")
        script_path = self.workspace_dir / "tests/scripts/stop-spark-4.0.1-reference.sh"

        if script_path.exists():
            try:
                env = os.environ.copy()
                env['SPARK_PORT'] = str(self.spark_reference_port)
                subprocess.run(
                    [str(script_path)],
                    cwd=str(self.workspace_dir),
                    capture_output=True,
                    timeout=30,
                    env=env
                )
                print(f"✓ Spark Connect reference server stopped (port {self.spark_reference_port})")
            except Exception as e:
                print(f"Warning: Error stopping Spark Connect server: {e}")

        self.spark_container_running = False

    def start_thunderduck(self, timeout: int = 60) -> bool:
        """
        Start Thunderduck Connect server

        Args:
            timeout: Maximum time to wait for server to start

        Returns:
            True if server started successfully
        """
        print("\n" + "=" * 80)
        print("Starting Thunderduck Connect Server")
        print("=" * 80)

        return self.thunderduck_manager.start(timeout=timeout)

    def stop_thunderduck(self):
        """Stop Thunderduck Connect server"""
        print("\nStopping Thunderduck Connect server...")
        self.thunderduck_manager.stop()

    def start_both(self, timeout: int = 60) -> Tuple[bool, bool]:
        """
        Start both servers

        Args:
            timeout: Maximum time to wait for each server to start

        Returns:
            Tuple of (spark_started, thunderduck_started)
        """
        print("\n" + "=" * 80)
        print("Starting Both Servers for Differential Testing")
        print("=" * 80)

        # Start Spark Connect reference first
        spark_ok = self.start_spark_reference(timeout=timeout)
        if not spark_ok:
            print("✗ Failed to start Spark Connect reference server")
            return False, False

        # Start Thunderduck
        thunderduck_ok = self.start_thunderduck(timeout=timeout)
        if not thunderduck_ok:
            print("✗ Failed to start Thunderduck server")
            self.stop_spark_reference()
            return spark_ok, False

        print("\n" + "=" * 80)
        print("✓ Both servers started successfully")
        print("=" * 80)
        print(f"  Spark Reference:  sc://localhost:{self.spark_reference_port}")
        print(f"  Thunderduck:      sc://localhost:{self.thunderduck_port}")
        print("=" * 80)

        return spark_ok, thunderduck_ok

    def stop_both(self):
        """Stop both servers and clean up temporary resources"""
        print("\n" + "=" * 80)
        print("Stopping Both Servers")
        print("=" * 80)

        self.stop_thunderduck()
        self.stop_spark_reference()

        # Clean up temporary warehouse directory
        if self._spark_warehouse_dir and os.path.exists(self._spark_warehouse_dir):
            try:
                shutil.rmtree(self._spark_warehouse_dir)
                print(f"  Cleaned up warehouse dir: {self._spark_warehouse_dir}")
            except Exception as e:
                print(f"  Warning: Could not clean up warehouse dir: {e}")

        print("✓ Both servers stopped")

    def __enter__(self):
        """Context manager entry"""
        spark_ok, thunderduck_ok = self.start_both()
        if not (spark_ok and thunderduck_ok):
            self.stop_both()
            raise RuntimeError("Failed to start both servers")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop_both()


if __name__ == "__main__":
    # Test the dual server manager
    print("Testing DualServerManager...")

    manager = DualServerManager()

    try:
        spark_ok, td_ok = manager.start_both(timeout=120)
        if spark_ok and td_ok:
            print("\n✓ Both servers started successfully")
            print("Press Ctrl+C to stop...")
            time.sleep(300)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        manager.stop_both()
