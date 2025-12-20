"""
Test orchestrator for robust differential testing infrastructure.

This module provides:
- ServerSupervisor: Manages server processes with health monitoring
- TimingCollector: Measures and reports performance metrics
- TestOrchestrator: Central coordinator for differential tests

All operations have low timeouts to detect deadlocks/blocking/failures fast.
"""

import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, Row, SparkSession

from .exceptions import (
    HardError,
    HealthCheckError,
    QueryTimeoutError,
    ServerConnectionError,
    ServerCrashError,
    ServerStartupError,
)


# =============================================================================
# Timing Infrastructure
# =============================================================================

@dataclass
class TimingStats:
    """Statistics for a timing category."""
    samples: List[float] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.samples)

    @property
    def total(self) -> float:
        return sum(self.samples)

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count > 0 else 0.0

    @property
    def min(self) -> float:
        return min(self.samples) if self.samples else 0.0

    @property
    def max(self) -> float:
        return max(self.samples) if self.samples else 0.0


class TimingCollector:
    """Collects and reports timing measurements for test execution."""

    def __init__(self):
        self.spark_connect_times = TimingStats()
        self.thunderduck_connect_times = TimingStats()
        self.spark_query_plan_times = TimingStats()
        self.thunderduck_query_plan_times = TimingStats()
        self.spark_collect_times = TimingStats()
        self.thunderduck_collect_times = TimingStats()

    def record_connect_time(self, server: str, duration: float) -> None:
        """Record time to establish PySpark session."""
        if server == "spark":
            self.spark_connect_times.samples.append(duration)
        else:
            self.thunderduck_connect_times.samples.append(duration)

    def record_query_plan_time(self, server: str, duration: float) -> None:
        """Record time to build query plan (usually negligible)."""
        if server == "spark":
            self.spark_query_plan_times.samples.append(duration)
        else:
            self.thunderduck_query_plan_times.samples.append(duration)

    def record_collect_time(self, server: str, duration: float) -> None:
        """Record time to collect results (materialize + transfer)."""
        if server == "spark":
            self.spark_collect_times.samples.append(duration)
        else:
            self.thunderduck_collect_times.samples.append(duration)

    def get_summary(self) -> str:
        """Generate timing summary report."""
        lines = [
            "",
            "=" * 70,
            "TIMING SUMMARY",
            "=" * 70,
            "",
            "Connect Times (seconds):",
            f"  Spark Reference:  n={self.spark_connect_times.count:4d}  "
            f"mean={self.spark_connect_times.mean:.3f}  "
            f"min={self.spark_connect_times.min:.3f}  "
            f"max={self.spark_connect_times.max:.3f}  "
            f"total={self.spark_connect_times.total:.3f}",
            f"  Thunderduck:      n={self.thunderduck_connect_times.count:4d}  "
            f"mean={self.thunderduck_connect_times.mean:.3f}  "
            f"min={self.thunderduck_connect_times.min:.3f}  "
            f"max={self.thunderduck_connect_times.max:.3f}  "
            f"total={self.thunderduck_connect_times.total:.3f}",
            "",
            "Query Plan Times (seconds) - time to build query plan:",
            f"  Spark Reference:  n={self.spark_query_plan_times.count:4d}  "
            f"mean={self.spark_query_plan_times.mean:.3f}  "
            f"min={self.spark_query_plan_times.min:.3f}  "
            f"max={self.spark_query_plan_times.max:.3f}  "
            f"total={self.spark_query_plan_times.total:.3f}",
            f"  Thunderduck:      n={self.thunderduck_query_plan_times.count:4d}  "
            f"mean={self.thunderduck_query_plan_times.mean:.3f}  "
            f"min={self.thunderduck_query_plan_times.min:.3f}  "
            f"max={self.thunderduck_query_plan_times.max:.3f}  "
            f"total={self.thunderduck_query_plan_times.total:.3f}",
            "",
            "Collect Times (seconds) - time to materialize + transfer:",
            f"  Spark Reference:  n={self.spark_collect_times.count:4d}  "
            f"mean={self.spark_collect_times.mean:.3f}  "
            f"min={self.spark_collect_times.min:.3f}  "
            f"max={self.spark_collect_times.max:.3f}  "
            f"total={self.spark_collect_times.total:.3f}",
            f"  Thunderduck:      n={self.thunderduck_collect_times.count:4d}  "
            f"mean={self.thunderduck_collect_times.mean:.3f}  "
            f"min={self.thunderduck_collect_times.min:.3f}  "
            f"max={self.thunderduck_collect_times.max:.3f}  "
            f"total={self.thunderduck_collect_times.total:.3f}",
            "",
            "Total Times (seconds):",
            f"  Spark Reference:  {self.spark_connect_times.total + self.spark_query_plan_times.total + self.spark_collect_times.total:.3f}",
            f"  Thunderduck:      {self.thunderduck_connect_times.total + self.thunderduck_query_plan_times.total + self.thunderduck_collect_times.total:.3f}",
            "=" * 70,
            "",
        ]
        return "\n".join(lines)


# =============================================================================
# Server Supervision
# =============================================================================

class ServerSupervisor:
    """Manages server processes with health monitoring."""

    def __init__(self, name: str, port: int, start_cmd: List[str], working_dir: str = None):
        self.name = name
        self.port = port
        self.start_cmd = start_cmd
        self.working_dir = working_dir or os.getcwd()
        self.process: Optional[subprocess.Popen] = None
        self.stdout_log: List[str] = []
        self.stderr_log: List[str] = []
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        self._log_lock = threading.Lock()

    def start(self, timeout: int = 60) -> bool:
        """Start server and verify it's ready.

        Args:
            timeout: Maximum seconds to wait for server to be ready

        Returns:
            True if server started successfully

        Raises:
            ServerStartupError: If server fails to start within timeout
        """
        if self.is_alive():
            print(f"[{self.name}] Server already running on port {self.port}")
            return True

        print(f"[{self.name}] Starting server on port {self.port}...")
        print(f"[{self.name}] Command: {' '.join(self.start_cmd)}")

        try:
            self.process = subprocess.Popen(
                self.start_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
            )
        except Exception as e:
            raise ServerStartupError(f"[{self.name}] Failed to start process: {e}")

        # Start monitoring thread
        self._stop_monitoring.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_output, daemon=True)
        self._monitor_thread.start()

        # Wait for server to be ready (port listening)
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if process died
            if self.process.poll() is not None:
                logs = self.get_logs(tail=50)
                raise ServerStartupError(
                    f"[{self.name}] Server process died with exit code {self.process.returncode}.\n"
                    f"stdout: {logs.get('stdout', [])[-10:]}\n"
                    f"stderr: {logs.get('stderr', [])[-10:]}"
                )

            # Check if port is listening
            if self._is_port_listening():
                print(f"[{self.name}] Server ready on port {self.port}")
                return True

            time.sleep(0.5)

        raise ServerStartupError(
            f"[{self.name}] Server failed to start within {timeout}s. "
            f"Port {self.port} not listening."
        )

    def _monitor_output(self) -> None:
        """Background thread to capture stdout/stderr."""
        while not self._stop_monitoring.is_set() and self.process:
            if self.process.stdout:
                try:
                    # Non-blocking read
                    line = self.process.stdout.readline()
                    if line:
                        with self._log_lock:
                            self.stdout_log.append(line.decode('utf-8', errors='replace').rstrip())
                            # Keep last 1000 lines
                            if len(self.stdout_log) > 1000:
                                self.stdout_log = self.stdout_log[-1000:]
                except Exception:
                    pass

            if self.process.stderr:
                try:
                    line = self.process.stderr.readline()
                    if line:
                        with self._log_lock:
                            self.stderr_log.append(line.decode('utf-8', errors='replace').rstrip())
                            if len(self.stderr_log) > 1000:
                                self.stderr_log = self.stderr_log[-1000:]
                except Exception:
                    pass

            time.sleep(0.01)

    def _is_port_listening(self) -> bool:
        """Check if the server port is accepting connections."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', self.port))
            sock.close()
            return result == 0
        except Exception:
            return False

    def is_alive(self) -> bool:
        """Check if server process is still running."""
        return self.process is not None and self.process.poll() is None

    def check_health(self, timeout: int = 2) -> Tuple[bool, Optional[str]]:
        """Active health check - verify server responds.

        Args:
            timeout: Maximum seconds to wait for health check

        Returns:
            Tuple of (healthy, error_message)
        """
        if not self.is_alive():
            return False, f"Server process not running (exit code: {self.process.returncode if self.process else 'N/A'})"

        if not self._is_port_listening():
            return False, f"Port {self.port} not listening"

        return True, None

    def get_logs(self, tail: int = 100) -> Dict[str, List[str]]:
        """Get recent stdout/stderr logs.

        Args:
            tail: Number of lines to return from each stream

        Returns:
            Dict with 'stdout' and 'stderr' lists
        """
        with self._log_lock:
            return {
                'stdout': self.stdout_log[-tail:] if self.stdout_log else [],
                'stderr': self.stderr_log[-tail:] if self.stderr_log else [],
            }

    def stop(self) -> None:
        """Stop the server process."""
        self._stop_monitoring.set()

        if self.process:
            print(f"[{self.name}] Stopping server...")
            try:
                # Try graceful shutdown first
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                else:
                    self.process.terminate()

                # Wait up to 5 seconds for graceful shutdown
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill
                    if hasattr(os, 'killpg'):
                        os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    else:
                        self.process.kill()
                    self.process.wait(timeout=2)

                print(f"[{self.name}] Server stopped")
            except Exception as e:
                print(f"[{self.name}] Error stopping server: {e}")

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=1)

    def restart(self, timeout: int = 60) -> bool:
        """Kill and restart the server.

        Args:
            timeout: Maximum seconds to wait for server restart

        Returns:
            True if restart successful

        Raises:
            ServerStartupError: If server fails to restart
        """
        print(f"[{self.name}] Restarting server...")
        self.stop()
        time.sleep(1)  # Brief pause before restart
        return self.start(timeout=timeout)


# =============================================================================
# Test Orchestrator
# =============================================================================

class TestOrchestrator:
    """Central coordinator for differential tests.

    All operations have low timeouts to detect deadlocks/blocking/failures fast:
    - Connect timeout: 10s (session creation)
    - Query plan timeout: 5s (building plan should be instant)
    - Collect timeout: 10s (can override per-query for slow queries)
    - Health check timeout: 2s (quick ping)
    """

    # Default timeouts (seconds) - low to detect issues fast
    DEFAULT_CONNECT_TIMEOUT = 10
    DEFAULT_QUERY_PLAN_TIMEOUT = 5
    DEFAULT_COLLECT_TIMEOUT = 10
    DEFAULT_HEALTH_CHECK_TIMEOUT = 2
    DEFAULT_SERVER_STARTUP_TIMEOUT = 60

    def __init__(self, config: Dict[str, Any]):
        """Initialize the orchestrator.

        Args:
            config: Configuration dict with keys:
                - spark_port: Port for Spark Reference server (default: 15003)
                - thunderduck_port: Port for Thunderduck server (default: 15002)
                - connect_timeout: Session creation timeout (default: 10s)
                - query_plan_timeout: Query plan timeout (default: 5s)
                - collect_timeout: Collection timeout (default: 10s)
                - health_check_timeout: Health check timeout (default: 2s)
                - server_startup_timeout: Server startup timeout (default: 60s)
                - spark_memory: Spark driver memory (default: '4g')
                - thunderduck_memory: Thunderduck heap (default: '2g')
                - continue_on_error: Continue on hard errors (default: False)
                - workspace_dir: Base workspace directory
                - spark_start_cmd: Custom Spark start command (optional)
                - thunderduck_start_cmd: Custom Thunderduck start command (optional)
        """
        self.config = config

        # Extract config with defaults
        self.spark_port = config.get('spark_port', 15003)
        self.thunderduck_port = config.get('thunderduck_port', 15002)
        self.connect_timeout = config.get('connect_timeout', self.DEFAULT_CONNECT_TIMEOUT)
        self.query_plan_timeout = config.get('query_plan_timeout', self.DEFAULT_QUERY_PLAN_TIMEOUT)
        self.collect_timeout = config.get('collect_timeout', self.DEFAULT_COLLECT_TIMEOUT)
        self.health_check_timeout = config.get('health_check_timeout', self.DEFAULT_HEALTH_CHECK_TIMEOUT)
        self.server_startup_timeout = config.get('server_startup_timeout', self.DEFAULT_SERVER_STARTUP_TIMEOUT)
        self.continue_on_error = config.get('continue_on_error', False)
        self.workspace_dir = config.get('workspace_dir', '/workspace')

        # Server supervisors
        self.spark_supervisor: Optional[ServerSupervisor] = None
        self.thunderduck_supervisor: Optional[ServerSupervisor] = None

        # Timing collector
        self.timings = TimingCollector()

        # Track active sessions for cleanup
        self._active_sessions: List[SparkSession] = []

        # Diagnostic log directory
        self.log_dir = Path(self.workspace_dir) / 'tests' / 'integration' / 'logs'
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def start_servers(self) -> None:
        """Start both servers with monitoring.

        Raises:
            ServerStartupError: If either server fails to start
        """
        print("=" * 70)
        print("Starting Differential Test Servers")
        print("=" * 70)

        # Build start commands
        spark_cmd = self.config.get('spark_start_cmd') or [
            str(Path(self.workspace_dir) / 'tests' / 'scripts' / 'start-spark-4.0.1-reference.sh')
        ]

        thunderduck_cmd = self.config.get('thunderduck_start_cmd') or [
            str(Path(self.workspace_dir) / 'tests' / 'scripts' / 'start-server.sh')
        ]

        # Start Spark Reference
        self.spark_supervisor = ServerSupervisor(
            name="SparkReference",
            port=self.spark_port,
            start_cmd=spark_cmd,
            working_dir=self.workspace_dir,
        )
        self.spark_supervisor.start(timeout=self.server_startup_timeout)

        # Start Thunderduck
        self.thunderduck_supervisor = ServerSupervisor(
            name="Thunderduck",
            port=self.thunderduck_port,
            start_cmd=thunderduck_cmd,
            working_dir=self.workspace_dir,
        )
        self.thunderduck_supervisor.start(timeout=self.server_startup_timeout)

        print("=" * 70)
        print("Both servers started successfully")
        print(f"  Spark Reference: sc://localhost:{self.spark_port}")
        print(f"  Thunderduck:     sc://localhost:{self.thunderduck_port}")
        print("=" * 70)

    def stop_servers(self) -> None:
        """Stop servers and print timing summary."""
        # Print timing summary
        print(self.timings.get_summary())

        # Stop active sessions
        for session in self._active_sessions:
            try:
                session.stop()
            except Exception:
                pass
        self._active_sessions.clear()

        # Stop servers
        if self.thunderduck_supervisor:
            self.thunderduck_supervisor.stop()
        if self.spark_supervisor:
            self.spark_supervisor.stop()

    def create_spark_session(self, timeout: int = None) -> SparkSession:
        """Create fresh PySpark session to Spark Reference with timeout.

        Args:
            timeout: Connection timeout in seconds (default: connect_timeout config)

        Returns:
            New SparkSession connected to Spark Reference

        Raises:
            ServerConnectionError: If connection fails or times out
        """
        timeout = timeout or self.connect_timeout
        start = time.perf_counter()

        try:
            session = self._create_session_with_timeout(
                f"sc://localhost:{self.spark_port}",
                timeout,
                "SparkReference"
            )
            self.timings.record_connect_time("spark", time.perf_counter() - start)
            self._active_sessions.append(session)
            return session
        except Exception as e:
            raise ServerConnectionError(f"Failed to connect to Spark Reference: {e}")

    def create_thunderduck_session(self, timeout: int = None) -> SparkSession:
        """Create fresh PySpark session to Thunderduck with timeout.

        Args:
            timeout: Connection timeout in seconds (default: connect_timeout config)

        Returns:
            New SparkSession connected to Thunderduck

        Raises:
            ServerConnectionError: If connection fails or times out
        """
        timeout = timeout or self.connect_timeout
        start = time.perf_counter()

        try:
            session = self._create_session_with_timeout(
                f"sc://localhost:{self.thunderduck_port}",
                timeout,
                "Thunderduck"
            )
            self.timings.record_connect_time("thunderduck", time.perf_counter() - start)
            self._active_sessions.append(session)
            return session
        except Exception as e:
            raise ServerConnectionError(f"Failed to connect to Thunderduck: {e}")

    def _create_session_with_timeout(self, remote_url: str, timeout: int, name: str) -> SparkSession:
        """Create session with timeout to detect connection hangs.

        Args:
            remote_url: Spark Connect URL
            timeout: Connection timeout in seconds
            name: Server name for error messages

        Returns:
            New SparkSession

        Raises:
            ServerConnectionError: If connection fails or times out
        """
        result = [None]
        exception = [None]

        def connect():
            try:
                # Use create() instead of getOrCreate() to ensure we get a new session
                # getOrCreate() can reuse existing sessions, causing both fixtures
                # to point to the same server
                result[0] = SparkSession.builder.remote(remote_url).create()
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=connect, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            raise ServerConnectionError(
                f"Connection to {name} timed out after {timeout}s. "
                "Server may be unresponsive or deadlocked."
            )

        if exception[0]:
            raise ServerConnectionError(f"Connection to {name} failed: {exception[0]}")

        return result[0]

    def execute_query(self, session: SparkSession, query: str, server: str,
                      timeout: int = None) -> DataFrame:
        """Build query plan with timeout.

        Args:
            session: PySpark session to use
            query: SQL query string
            server: "spark" or "thunderduck" for timing recording
            timeout: Query plan timeout (default: query_plan_timeout config)

        Returns:
            DataFrame (not yet materialized)

        Raises:
            QueryTimeoutError: If query plan building times out
        """
        timeout = timeout or self.query_plan_timeout
        start = time.perf_counter()

        df = self._execute_with_timeout(session, query, timeout, server)
        self.timings.record_query_plan_time(server, time.perf_counter() - start)
        return df

    def _execute_with_timeout(self, session: SparkSession, query: str,
                               timeout: int, server: str) -> DataFrame:
        """Execute SQL with timeout to detect deadlocks/blocking.

        Args:
            session: PySpark session
            query: SQL query string
            timeout: Timeout in seconds
            server: Server name for error messages

        Returns:
            DataFrame

        Raises:
            QueryTimeoutError: If execution times out
        """
        result = [None]
        exception = [None]

        def execute():
            try:
                result[0] = session.sql(query)
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            raise QueryTimeoutError(
                f"Query plan building on {server} timed out after {timeout}s. "
                f"Query: {query[:100]}..."
            )

        if exception[0]:
            raise exception[0]

        return result[0]

    def collect_result(self, df: DataFrame, server: str, timeout: int = None) -> List[Row]:
        """Collect DataFrame with timeout and timing.

        Args:
            df: DataFrame to collect
            server: "spark" or "thunderduck" for timing recording
            timeout: Collection timeout (default: collect_timeout config)

        Returns:
            List of Row objects

        Raises:
            QueryTimeoutError: If collection times out
        """
        timeout = timeout or self.collect_timeout
        start = time.perf_counter()

        result = self._collect_with_timeout(df, timeout, server)
        self.timings.record_collect_time(server, time.perf_counter() - start)
        return result

    def _collect_with_timeout(self, df: DataFrame, timeout: int, server: str) -> List[Row]:
        """Collect with timeout to detect server hangs.

        Args:
            df: DataFrame to collect
            timeout: Timeout in seconds
            server: Server name for error messages

        Returns:
            List of Row objects

        Raises:
            QueryTimeoutError: If collection times out
        """
        result = []
        exception = [None]

        def collect():
            try:
                result.extend(df.collect())
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            raise QueryTimeoutError(
                f"Result collection on {server} timed out after {timeout}s. "
                "Server may be computing a large result or be deadlocked."
            )

        if exception[0]:
            raise exception[0]

        return result

    def check_servers_healthy(self, timeout: int = None) -> None:
        """Quick health check - verify both servers respond.

        Args:
            timeout: Health check timeout (default: health_check_timeout config)

        Raises:
            HealthCheckError: If either server is unhealthy
        """
        timeout = timeout or self.health_check_timeout

        if self.spark_supervisor:
            healthy, error = self.spark_supervisor.check_health(timeout)
            if not healthy:
                raise HealthCheckError(f"Spark Reference health check failed: {error}")

        if self.thunderduck_supervisor:
            healthy, error = self.thunderduck_supervisor.check_health(timeout)
            if not healthy:
                raise HealthCheckError(f"Thunderduck health check failed: {error}")

    def restart_spark_server(self, timeout: int = None) -> None:
        """Restart Spark Reference server.

        Args:
            timeout: Startup timeout (default: server_startup_timeout config)

        Raises:
            ServerStartupError: If restart fails
        """
        timeout = timeout or self.server_startup_timeout
        if self.spark_supervisor:
            self.spark_supervisor.restart(timeout)

    def restart_thunderduck_server(self, timeout: int = None) -> None:
        """Restart Thunderduck server.

        Args:
            timeout: Startup timeout (default: server_startup_timeout config)

        Raises:
            ServerStartupError: If restart fails
        """
        timeout = timeout or self.server_startup_timeout
        if self.thunderduck_supervisor:
            self.thunderduck_supervisor.restart(timeout)

    def on_hard_error(self, error: Exception, test_name: str = None, query: str = None) -> None:
        """Capture diagnostics on hard error.

        Args:
            error: The exception that occurred
            test_name: Name of the failing test (optional)
            query: Query that caused the failure (optional)
        """
        report_path = self.log_dir / 'failure_report.txt'

        lines = [
            "=" * 70,
            "HARD ERROR - TEST SUITE FAILURE",
            "=" * 70,
            "",
            f"Error Type: {type(error).__name__}",
            f"Error Message: {str(error)}",
            f"Test Name: {test_name or 'Unknown'}",
            f"Query: {query[:500] if query else 'N/A'}...",
            "",
            "=" * 70,
            "SPARK REFERENCE LOGS",
            "=" * 70,
        ]

        if self.spark_supervisor:
            logs = self.spark_supervisor.get_logs(tail=100)
            lines.append("--- stdout ---")
            lines.extend(logs.get('stdout', ['(empty)']))
            lines.append("")
            lines.append("--- stderr ---")
            lines.extend(logs.get('stderr', ['(empty)']))
        else:
            lines.append("(server not running)")

        lines.extend([
            "",
            "=" * 70,
            "THUNDERDUCK LOGS",
            "=" * 70,
        ])

        if self.thunderduck_supervisor:
            logs = self.thunderduck_supervisor.get_logs(tail=100)
            lines.append("--- stdout ---")
            lines.extend(logs.get('stdout', ['(empty)']))
            lines.append("")
            lines.append("--- stderr ---")
            lines.extend(logs.get('stderr', ['(empty)']))
        else:
            lines.append("(server not running)")

        lines.extend([
            "",
            "=" * 70,
            "TIMING DATA AT FAILURE",
            "=" * 70,
            self.timings.get_summary(),
        ])

        report_content = "\n".join(lines)

        # Write to file
        with open(report_path, 'w') as f:
            f.write(report_content)

        # Also print to stderr
        print(report_content, file=sys.stderr)

        print(f"\nFailure report written to: {report_path}", file=sys.stderr)

        # If not continuing on error, we should signal pytest to stop
        if not self.continue_on_error:
            print("\nAborting test suite due to hard error.", file=sys.stderr)
            print("Set THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true to continue.", file=sys.stderr)
