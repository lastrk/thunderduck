"""
Canonical port-checking utilities for test infrastructure.

Consolidates the duplicated port-checking implementations from conftest.py,
server_manager.py, test_orchestrator.py, and dual_server_manager.py into
a single shared module.
"""

import socket
import time


def is_port_listening(port, host='localhost', timeout=1):
    """Check if a port is accepting connections.

    Attempts a TCP connection to the given host:port. Returns True if the
    connection succeeds within the timeout, False otherwise.

    Args:
        port: Port number to check.
        host: Hostname to connect to (default: 'localhost').
        timeout: Socket timeout in seconds (default: 1).

    Returns:
        True if the port is accepting connections, False otherwise.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def wait_for_port(port, host='localhost', timeout=60):
    """Wait for a port to start accepting connections.

    Polls is_port_listening() every second until the port is accepting
    connections or the timeout expires.

    Args:
        port: Port number to wait for.
        host: Hostname to connect to (default: 'localhost').
        timeout: Maximum seconds to wait (default: 60).

    Returns:
        True if the port started accepting connections within the timeout,
        False if the timeout expired.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_listening(port, host=host, timeout=1):
            return True
        time.sleep(1)
    return False
