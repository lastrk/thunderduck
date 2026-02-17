# M87: Unified CI and Local Test Execution

**Date**: 2026-02-17
**Status**: Complete

## Summary

Unified CI and local test execution so both use the same entrypoint: `run-differential-tests-v2.sh`. Previously, CI invoked `python3 -m pytest` directly with hardcoded paths/env vars while the run script had its own test groups, venv detection, and cleanup. Now the run script is the single canonical way to run differential tests.

## Problem

- CI and local diverged: CI hardcoded test file paths and env vars, while the run script had test groups and venv auto-detection.
- The run script used blanket `pkill` cleanup that killed ALL Spark/Thunderduck processes system-wide, breaking parallel runs.
- Both Java servers escape the pytest process group (`setsid` for Thunderduck, Spark's own daemonization), so process-group-based cleanup from the shell couldn't catch them.
- Hard crashes (SIGKILL, OOM) left orphan servers with no cleanup mechanism on next run.

## Solution: PID File Mechanism

### How it works

**PID file**: `tests/integration/logs/.server-pids` (format: `name:port:pid`)

**Write path** (conftest.py): After `dual_server_manager` starts both servers, it writes a PID file with Thunderduck PID (from `process.pid`) and Spark PID (from `lsof` on port). On teardown, it deletes the PID file.

**Read path** (run script): Cleanup function reads PID file, kills listed process groups (`kill -- -$pid`), then deletes the file.

**Stale cleanup**: On startup, `dual_server_manager` calls `_cleanup_pid_file()` which reads any existing PID file and kills orphan processes from crashed previous runs.

### All entrypoints covered

| Entrypoint | Cleanup |
|------------|---------|
| Run script (`./run-differential-tests-v2.sh tpch`) | conftest cleanup + run script PID-file cleanup |
| Direct pytest (`python3 -m pytest ...`) | conftest cleanup only (PID file still written/cleaned) |
| CI (`./run-differential-tests-v2.sh --ci tpch`) | Same as run script |
| Hard crash (SIGKILL, OOM) | PID file survives; next run's conftest kills orphans |

## Changes

### `tests/integration/conftest.py`
- Added `_PID_FILE`, `_write_pid_file()`, `_read_pid_file()`, `_cleanup_pid_file()`, `_get_pid_on_port()`
- `dual_server_manager` fixture: cleans stale PIDs on startup, writes PID file after servers start, reports compat mode, deletes PID file on teardown
- `_cleanup_allocated_ports()` also deletes PID file

### `tests/scripts/run-differential-tests-v2.sh`
- Added `--ci` flag (sets `CONTINUE_ON_ERROR=true`, `COLLECT_TIMEOUT=30`)
- Added system `python3` fallback for CI environments
- Replaced blanket `pkill` with PID-file-based cleanup
- Removed `[2/3] Stopping existing servers` step (conftest handles it)
- Fixed test groups: `tpch` includes DataFrame tests, `tpcds` includes DataFrame tests
- Stopped duplicating pyproject.toml defaults (`-v`, `--tb=short`)
- Runs pytest in background to capture PID for cleanup
- Shows compat mode in config banner

### `.github/workflows/ci.yml`
- Replaced raw `python3 -m pytest` with `./tests/scripts/run-differential-tests-v2.sh --ci tpch`

### `README.md`
- Updated compat mode examples to use run script
- Added "Advanced: Direct pytest" subsection

### `CLAUDE.md`
- Noted run script as canonical test entrypoint

## Verification

- 51/51 TPC-H tests pass (29 SQL + 22 DataFrame) via `./run-differential-tests-v2.sh tpch`
