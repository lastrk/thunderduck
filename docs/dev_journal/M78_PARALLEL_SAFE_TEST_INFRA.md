# M78: Parallel-Safe Test Infrastructure & Session Deadlock Fix

**Date**: 2026-02-10
**Scope**: Test infrastructure reliability and parallel-run support

## Problem

The differential test suite (839 tests) had three interrelated infrastructure problems:

1. **Cross-run interference**: `kill_all_servers()` used `pkill` to kill ALL Java/Spark processes on the machine, including those from parallel test runs in other worktrees.

2. **No auto port allocation**: Parallel runs required manually setting `THUNDERDUCK_PORT` and `SPARK_PORT` env vars to avoid conflicts.

3. **Session deadlock at ~50%**: The suite consistently timed out around test 420/839 due to a PySpark session cleanup deadlock, causing cascading failures in all subsequent tests.

## Root Cause Analysis: PySpark Session Deadlock

PySpark's `ExecutePlanResponseReattachableIterator` has a **class-level (process-global)** `ThreadPoolExecutor` and `RLock` shared across ALL `SparkSession` instances:

```python
class ExecutePlanResponseReattachableIterator(Generator):
    _lock: ClassVar[RLock] = RLock()
    _release_thread_pool_instance: Optional[ThreadPoolExecutor] = None
```

The deadlock chain:

1. Class-scoped session S1 finishes, fixture calls `stop_spark_with_timeout(S1, timeout=5)`
2. Daemon thread calls `S1.stop()` -> `client.close()` -> `shutdown()`
3. `shutdown()` acquires `_lock`, calls `pool.shutdown(wait=True)` -- blocks if any `ReleaseExecute` RPC is hung
4. After 5s timeout, main thread continues, but daemon thread STILL holds `_lock`
5. Next test class creates S2 -> first query -> `_release_until()` -> tries to acquire `_lock` -> **DEADLOCKED**

Additionally, Python's GC calling `__del__` on orphaned sessions triggers the same `close()` -> `shutdown()` path.

## Solution

### 1. Safe Session Cleanup (`_release_session_without_shutdown`)

Replaced `stop_spark_with_timeout()` in all fixtures with a surgical cleanup:
- Calls `release_session()` to tell server to free resources
- Closes gRPC channel directly (`_channel.close()`) bypassing `shutdown()`
- Monkey-patches `client.close = lambda: None` to prevent GC `__del__` from triggering the deadlock

The global `ThreadPoolExecutor` stays alive for the entire test session and is naturally cleaned up at process exit.

### 2. Dynamic Port Allocation

When `THUNDERDUCK_PORT`/`SPARK_PORT` env vars are not set, ports are auto-allocated from the OS:

```python
def _allocate_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]
```

### 3. Port-Scoped Cleanup

Replaced `kill_all_servers()` (which used `pkill` to kill ALL Java processes) with `_kill_process_on_port(port)` that only kills processes listening on our specific allocated ports. Signal handlers and atexit handlers use the same port-scoped approach.

### 4. Stop Script Fix

`stop-spark-4.0.1-reference.sh` now reads `SPARK_PORT` from env (default 15003) and kills by `lsof -ti :${SPARK_PORT}` instead of `pgrep -f SparkConnectServer`.

### 5. Active Sessions as Set

Changed `_active_sessions` in `TestOrchestrator` from `List` to `Set` -- `discard()` is no-raise (unlike `list.remove()`) and prevents duplicates.

## Files Changed

| File | Change |
|------|--------|
| `tests/integration/conftest.py` | Safe session cleanup, auto port allocation, port-scoped cleanup |
| `tests/integration/utils/dual_server_manager.py` | Pass SPARK_PORT env to stop script |
| `tests/integration/utils/test_orchestrator.py` | `_active_sessions` List -> Set |
| `tests/scripts/stop-spark-4.0.1-reference.sh` | Port-aware stop logic |
| `CLAUDE.md` | Updated docs for auto ports, parallel runs, server management |

## Results

| Metric | Before | After |
|--------|--------|-------|
| Tests completed | ~421/839 (timeout at 50%) | 839/839 |
| Duration | 300s+ (hung) | 165s |
| Infrastructure failures | ~88 cascading | 0 |
| Parallel-safe | No (manual ports, global pkill) | Yes (auto ports, port-scoped) |

## Key Lesson

PySpark Spark Connect's session lifecycle has a fundamental design flaw: `session.stop()` destroys a process-global resource (`ThreadPoolExecutor`) that other sessions depend on. In multi-session test environments, NEVER use `session.stop()` for intermediate sessions. Instead, release the server session and close the channel directly, bypassing the shared pool shutdown.
