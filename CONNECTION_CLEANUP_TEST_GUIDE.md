# Connection Cleanup Testing Quick Reference Guide

## Overview
Quick reference for running and understanding the database connection cleanup tests that prevent the critical "cannot remove child PID during cleanup" crash.

---

## Quick Start

### Run All Tests
```bash
mvn test -Dtest=DatabaseConnectionCleanupTest
```

### Run Specific Test Category
```bash
# Race condition tests (most critical)
mvn test -Dtest=DatabaseConnectionCleanupTest\$RaceConditionTests

# Regression tests (bug prevention)
mvn test -Dtest=DatabaseConnectionCleanupTest\$RegressionTests

# State validation tests
mvn test -Dtest=DatabaseConnectionCleanupTest\$ConnectionStateValidationTests

# Recovery tests
mvn test -Dtest=DatabaseConnectionCleanupTest\$CleanupFailureRecoveryTests

# Lifecycle tests
mvn test -Dtest=DatabaseConnectionCleanupTest\$ConnectionLifecycleIntegrityTests

# Edge cases
mvn test -Dtest=DatabaseConnectionCleanupTest\$EdgeCaseScenarios
```

### Run Single Test
```bash
mvn test -Dtest=DatabaseConnectionCleanupTest#testPreventPIDRemovalCrash
```

---

## Test Categories

### 1. State Validation (4 tests)
**Purpose**: Verify connection state before cleanup

**Critical Tests**:
- `testNormalShutdownSequence` - Normal flow validation
- `testPrematureConnectionClosure` - Detects closed connections
- `testConnectionBecomesInvalidDuringUse` - Handles mid-operation failures
- `testConnectionStateValidationUnderConcurrency` - Concurrent state checking

**When to Run**: After any connection management changes

---

### 2. Race Conditions (5 tests)
**Purpose**: Catch timing-related crashes

**Critical Tests**:
- `testRaceBetweenCloseAndCleanup` ⚠️ **MOST CRITICAL** - Reproduces the bug
- `testConcurrentCleanupAttempts` - Multiple cleanup attempts
- `testRaceBetweenManagerCloseAndCleanup` - Shutdown race
- `testRapidConnectDisconnectCycles` - High-frequency operations
- `testHighConcurrencyStress` - Extreme load testing

**When to Run**: Before every release, after concurrency changes

---

### 3. Cleanup Recovery (4 tests)
**Purpose**: Ensure graceful failure handling

**Critical Tests**:
- `testRecoveryFromFailedCleanup` - Single failure recovery
- `testMultipleConsecutiveCleanupFailures` - Repeated failures
- `testRecoveryWhenAllConnectionsInvalid` - Total pool recovery
- `testCleanupFailureDuringShutdown` - Shutdown with active ops

**When to Run**: After error handling changes

---

### 4. Lifecycle Integrity (5 tests)
**Purpose**: Verify complete connection lifecycle

**Critical Tests**:
- `testCompleteConnectionLifecycle` - Full lifecycle validation
- `testPreventDoubleClose` - Idempotency check
- `testLifecycleWithExceptionsDuringUse` - Exception handling
- `testConnectionStateAfterManagerClose` - Post-shutdown state
- `testNoConnectionLeaksDuringLifecycle` - Leak detection

**When to Run**: After pool management changes

---

### 5. Edge Cases (5 tests)
**Purpose**: Handle uncommon scenarios

**Critical Tests**:
- `testCleanupDuringTransaction` - Uncommitted transactions
- `testCleanupWithPendingResultSets` - Open resources
- `testCleanupWithTimeout` - Timeout handling
- `testCleanupUnderPoolExhaustion` - Exhaustion scenarios

**When to Run**: During comprehensive testing

---

### 6. Regression Tests (4 tests) ⚠️ CRITICAL
**Purpose**: Prevent bug recurrence

**Critical Tests**:
- `testPreventPIDRemovalCrash` ⚠️ **MUST PASS** - Direct bug test
- `testValidateStateBeforeAllCleanupOps` - State validation enforcement
- `testAtomicCleanupOperations` - Atomicity verification
- `testErrorHandlingInAllCleanupPaths` - Error path coverage

**When to Run**: EVERY BUILD - These prevent the critical bug

---

## Test Interpretation

### Success Indicators
```
[INFO] Tests run: 27, Failures: 0, Errors: 0, Skipped: 0
```
✅ All tests passed - System is stable

### Failure Indicators

#### Race Condition Failure
```
testRaceBetweenCloseAndCleanup - FAILED
AssertionError: crashCount.get() expected: <0> but was: <1>
```
🚨 **CRITICAL** - Connection cleanup race condition detected
**Action**: Review connection close and cleanup coordination

#### State Validation Failure
```
testPrematureConnectionClosure - FAILED
SQLException: Connection already closed
```
⚠️ **WARNING** - Connection state not validated before operation
**Action**: Add state checks before accessing connection

#### Recovery Failure
```
testRecoveryFromFailedCleanup - FAILED
SQLException: Pool exhausted
```
⚠️ **WARNING** - Recovery mechanism not creating replacement connections
**Action**: Review connection replacement logic

#### Regression Failure
```
testPreventPIDRemovalCrash - FAILED
```
🚨 **CRITICAL BUG RECURRENCE** - The original bug has returned
**Action**: Immediately review recent connection management changes

---

## Common Issues and Solutions

### Issue 1: Intermittent Test Failures
**Symptom**: Tests pass sometimes, fail other times
**Cause**: Race conditions not properly handled
**Solution**:
```java
// Add proper synchronization
synchronized(this) {
    if (!released) {
        released = true;
        cleanup();
    }
}
```

### Issue 2: Connection Leaks
**Symptom**: `testNoConnectionLeaksDuringLifecycle` fails
**Cause**: Connections not returned to pool
**Solution**: Always use try-with-resources
```java
try (PooledConnection conn = manager.borrowConnection()) {
    // Use connection
} // Automatically released
```

### Issue 3: Cleanup Crashes
**Symptom**: Tests crash during cleanup
**Cause**: Not validating connection state
**Solution**: Always validate before operations
```java
if (conn == null || conn.isClosed()) {
    return; // Don't attempt cleanup
}
```

### Issue 4: Manager Won't Close
**Symptom**: `manager.close()` hangs or times out
**Cause**: Active connections blocking shutdown
**Solution**: Implement graceful shutdown
```java
// Set closed flag first
closed = true;

// Then cleanup connections
while (!pool.isEmpty()) {
    conn = pool.poll();
    if (conn != null) {
        try { conn.close(); }
        catch (SQLException e) { /* log */ }
    }
}
```

---

## Pre-Commit Checklist

Before committing connection management changes:

- [ ] Run `DatabaseConnectionCleanupTest` - all pass
- [ ] Run regression tests specifically
- [ ] Run race condition tests 10 times
- [ ] Check for new connection acquisition points
- [ ] Verify all connections use try-with-resources
- [ ] Validate state before cleanup operations
- [ ] Review error handling in cleanup paths
- [ ] Test under concurrent load

---

## CI/CD Integration

### Required CI Checks

```yaml
# .github/workflows/tests.yml
- name: Run Connection Cleanup Tests
  run: mvn test -Dtest=DatabaseConnectionCleanupTest

- name: Run Regression Tests (Critical)
  run: mvn test -Dtest=DatabaseConnectionCleanupTest$RegressionTests

- name: Run Race Condition Tests (3x for reliability)
  run: |
    for i in 1 2 3; do
      mvn test -Dtest=DatabaseConnectionCleanupTest$RaceConditionTests
    done
```

### Build Gates

**BLOCK MERGE** if:
- Any regression test fails
- Race condition test fails more than once in 3 runs
- Any test crashes (not just fails)

**WARN** if:
- Edge case tests fail
- Recovery tests fail

---

## Performance Benchmarks

### Expected Test Times

| Test Category | Expected Duration | Warning Threshold |
|--------------|-------------------|-------------------|
| State Validation | < 5 seconds | > 10 seconds |
| Race Conditions | 10-30 seconds | > 60 seconds |
| Recovery | < 5 seconds | > 10 seconds |
| Lifecycle | < 5 seconds | > 10 seconds |
| Edge Cases | 10-20 seconds | > 40 seconds |
| Regression | 5-15 seconds | > 30 seconds |
| **Full Suite** | **30-60 seconds** | **> 120 seconds** |

If tests exceed warning thresholds, investigate:
- Connection pool sizing
- Timeout configuration
- System resource constraints
- Thread pool exhaustion

---

## Debugging Failed Tests

### Enable Detailed Logging
```bash
mvn test -Dtest=DatabaseConnectionCleanupTest -X -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

### Run Single Test with Breakpoints
```bash
# In IDE, set breakpoints in:
# - PooledConnection.close()
# - DuckDBConnectionManager.releaseConnection()
# - Test failure point

# Then run specific test with debugger
```

### Check Thread States
```java
// Add to test
Thread.getAllStackTraces().forEach((thread, stack) -> {
    System.out.println(thread.getName() + " state: " + thread.getState());
});
```

### Monitor Connection Pool
```java
// Add temporary logging
System.out.println("Pool size: " + manager.getPoolSize());
System.out.println("Available: " + connectionPool.size());
```

---

## Contact and Support

### Test Ownership
- **Component**: Runtime / Connection Management
- **Test Suite**: `DatabaseConnectionCleanupTest.java`
- **Critical Bug**: "Database connection closed, cannot remove child PID during cleanup"

### Getting Help

1. **Test Failures**: Check this guide first
2. **Persistent Issues**: Review `DATABASE_CONNECTION_CLEANUP_ANALYSIS.md`
3. **Bug Recurrence**: Escalate immediately to senior developers

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-10-14 | Initial test suite and guide |

---

**Remember**: These tests prevent a CRITICAL crash bug. Never skip them!
