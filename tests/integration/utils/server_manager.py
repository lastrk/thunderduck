"""
Server Manager for Thunderduck Spark Connect Integration Tests

Manages the lifecycle of the Spark Connect server for integration testing.
"""

import subprocess
import time
import socket
import os
import signal
from pathlib import Path
from typing import Optional

from port_utils import is_port_listening, wait_for_port


class ServerManager:
    """Manages Spark Connect server lifecycle for integration tests"""

    def __init__(self, host: str = "localhost", port: int = 15002, compat_mode: Optional[str] = None):
        self.host = host
        self.port = port
        self.compat_mode = compat_mode  # "strict", "relaxed", or None (auto)
        self.process: Optional[subprocess.Popen] = None
        self.workspace_dir = Path(__file__).parent.parent.parent.parent
        # Allow overriding the JAR directory via environment variable
        jar_dir = os.environ.get("THUNDERDUCK_JAR_DIR")
        if jar_dir:
            self.server_jar = Path(jar_dir) / "thunderduck-connect-server-0.1.0-SNAPSHOT.jar"
        else:
            self.server_jar = self.workspace_dir / "connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar"

    def is_port_available(self) -> bool:
        """Check if the port is available (tolerates TIME_WAIT connections)"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((self.host, self.port))
                return True
            except OSError:
                return False

    def is_server_ready(self, timeout: int = 30) -> bool:
        """Check if server is ready to accept connections"""
        return wait_for_port(self.port, host=self.host, timeout=timeout)

    def start(self, timeout: int = 60) -> bool:
        """
        Start the Spark Connect server

        Args:
            timeout: Maximum time to wait for server to start (seconds)

        Returns:
            True if server started successfully, False otherwise
        """
        if not self.server_jar.exists():
            raise FileNotFoundError(
                f"Server JAR not found: {self.server_jar}\n"
                f"Please build the project first: mvn clean package -DskipTests"
            )

        # Check if port is already in use
        if not self.is_port_available():
            print(f"Port {self.port} is already in use. Attempting to kill existing process...")
            self.kill_existing_server()
            time.sleep(2)

            if not self.is_port_available():
                raise RuntimeError(f"Port {self.port} is still in use after cleanup")

        # Start the server with required JVM args for ARM64 and Apache Arrow
        java_cmd = [
            "java",
            "-Xmx2g",
            "-Xms1g",
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=200",
            "-XX:ParallelGCThreads=4",
            "-XX:ConcGCThreads=2",
            "-XX:InitiatingHeapOccupancyPercent=45",
            "-XX:G1HeapRegionSize=32m",
            "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "-Djava.security.properties=" + str(self.workspace_dir / "duckdb.security"),
        ]

        # Add compat mode system property if specified
        if self.compat_mode:
            java_cmd.append(f"-Dthunderduck.spark.compat.mode={self.compat_mode}")

        java_cmd.extend(["-jar", str(self.server_jar), str(self.port)])

        print(f"Starting Spark Connect server on {self.host}:{self.port}...")
        print(f"Command: {' '.join(java_cmd)}")

        # Start server process with output redirected to files for debugging
        log_dir = self.workspace_dir / "tests/integration/logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        stdout_file = log_dir / "server_stdout.log"
        stderr_file = log_dir / "server_stderr.log"

        with open(stdout_file, 'w') as stdout, open(stderr_file, 'w') as stderr:
            self.process = subprocess.Popen(
                java_cmd,
                cwd=str(self.workspace_dir),
                stdout=stdout,
                stderr=stderr,
                preexec_fn=os.setsid  # Create new process group for clean shutdown
            )

        print(f"Server process started (PID: {self.process.pid})")
        print(f"Logs: stdout={stdout_file}, stderr={stderr_file}")

        # Wait for server to be ready
        if self.is_server_ready(timeout):
            print(f"✓ Server ready on {self.host}:{self.port}")
            return True
        else:
            print(f"✗ Server failed to start within {timeout} seconds")
            self.stop()
            # Print last lines of error log
            if stderr_file.exists():
                print("\nLast 20 lines of stderr:")
                with open(stderr_file, 'r') as f:
                    lines = f.readlines()
                    for line in lines[-20:]:
                        print(f"  {line.rstrip()}")
            return False

    def stop(self):
        """Stop the Spark Connect server"""
        if self.process:
            print(f"Stopping server (PID: {self.process.pid})...")
            try:
                # Send SIGTERM to process group to kill all child processes
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)

                # Wait for graceful shutdown
                try:
                    self.process.wait(timeout=10)
                    print("✓ Server stopped gracefully")
                except subprocess.TimeoutExpired:
                    print("Server didn't stop gracefully, forcing kill...")
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    self.process.wait()
                    print("✓ Server killed")
            except ProcessLookupError:
                print("Server process already terminated")
            except Exception as e:
                print(f"Error stopping server: {e}")
            finally:
                self.process = None

    def kill_existing_server(self):
        """Kill any existing server process on the port"""
        try:
            # Find process using the port
            result = subprocess.run(
                ["lsof", "-ti", f":{self.port}"],
                capture_output=True,
                text=True
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        print(f"Killed process {pid} using port {self.port}")
                    except ProcessLookupError:
                        pass
        except FileNotFoundError:
            # lsof not available, try netstat
            try:
                result = subprocess.run(
                    ["ss", "-lptn", f"sport = :{self.port}"],
                    capture_output=True,
                    text=True
                )
                # Parse output to find PIDs (this is more complex, skip for now)
                print("Could not automatically kill process, please kill manually")
            except FileNotFoundError:
                print("Could not find process management tools")

    def __enter__(self):
        """Context manager entry"""
        if not self.start():
            raise RuntimeError("Failed to start server")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


if __name__ == "__main__":
    # Simple test of server manager
    print("Testing ServerManager...")
    manager = ServerManager()

    try:
        if manager.start():
            print("\n✓ Server started successfully")
            print("Press Ctrl+C to stop...")
            time.sleep(300)  # Keep alive for 5 minutes
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        manager.stop()
