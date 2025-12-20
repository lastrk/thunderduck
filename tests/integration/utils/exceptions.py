"""
Exception types for differential test infrastructure.

Hard errors halt the test suite immediately (unless THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR is set).
Soft errors (like ResultMismatchError) are logged but allow the suite to continue.
"""


class HardError(Exception):
    """Base class for errors that should halt the test suite.

    Hard errors indicate infrastructure failures (crashes, timeouts, connection issues)
    rather than test result differences.
    """
    pass


class ServerCrashError(HardError):
    """Server process died unexpectedly.

    This indicates the server process exited with a non-zero exit code
    or was killed by the OS (e.g., OOM).
    """
    pass


class QueryTimeoutError(HardError):
    """Query execution or collection exceeded timeout.

    This indicates a potential deadlock, blocking operation, or
    server hang. Default timeouts are intentionally low to detect
    these issues quickly.
    """
    pass


class ServerConnectionError(HardError):
    """Failed to connect to server.

    This indicates the server is not running, not accepting connections,
    or has a network issue.
    """
    pass


class ServerStartupError(HardError):
    """Server failed to start within timeout.

    This indicates the server process failed to start or become ready
    within the configured startup timeout.
    """
    pass


class HealthCheckError(HardError):
    """Server health check failed.

    This indicates the server is running but not responding to health
    checks within the timeout.
    """
    pass


class ResultMismatchError(Exception):
    """Soft error - results differ between Spark and Thunderduck.

    This is a test failure, not an infrastructure failure. The suite
    should continue running other tests.
    """

    def __init__(self, message: str, spark_result=None, thunderduck_result=None, query_name: str = None):
        super().__init__(message)
        self.spark_result = spark_result
        self.thunderduck_result = thunderduck_result
        self.query_name = query_name
