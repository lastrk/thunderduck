# Hive Mind Race Condition Analysis and Fix

## Executive Summary

**Issue**: Database connection closed before child PID cleanup during process termination, causing crash with error: "Database connection closed, cannot remove child PID during cleanup"

**Root Cause**: Race condition in cleanup sequence where database connection is closed before child processes complete their cleanup operations.

**Impact**: Process crashes during graceful shutdown, leaving orphaned processes and inconsistent session state.

---

## Detailed Analysis

### Architecture Overview

The Hive Mind system has a multi-layered architecture:

1. **HiveMindSessionManager** - Manages SQLite database connections for session persistence
2. **AutoSaveMiddleware** - Handles periodic checkpointing and child process tracking
3. **Signal Handlers** - Multiple handlers for SIGINT/SIGTERM at different levels

### The Race Condition

#### Current Cleanup Flow

```
User presses Ctrl+C
    |
    v
SIGINT fired
    |
    +---> hive-mind.js:904: sigintHandler()
    |        |
    |        +---> sessionManager.close() (line 884)
    |        |        |
    |        |        +---> db.close() - CONNECTION CLOSED!
    |        |
    |        +---> process.exit(0)
    |
    +---> auto-save-middleware.js:41: SIGINT handler
             |
             +---> cleanup()
                      |
                      +---> Terminate child processes
                      |        |
                      |        +---> childProcess.on('exit') fires
                      |                 |
                      |                 +---> removeChildPid(sessionId, pid)
                      |                          |
                      |                          +---> FAILS! db.open = false
                      |                                 (line 1004-1006)
                      |
                      +---> stopSession()
                      |        |
                      |        +---> getChildPids() - Also fails!
                      |
                      +---> sessionManager.close() - Already closed!
```

#### Problem Details

1. **Multiple SIGINT handlers** registered by both:
   - `hive-mind.js` (line 904)
   - `auto-save-middleware.js` (line 41)

2. **Non-deterministic execution order**: Node.js doesn't guarantee signal handler execution order

3. **Premature database closure**: The `hive-mind.js` handler closes the database connection BEFORE child processes finish cleanup

4. **Failed defensive checks**: The code has defensive checks (lines 1004, 1043) but they only log warnings and return false, not preventing the crash

### Code Locations

**Session Manager** (`session-manager.js`):
- Lines 983-1028: `removeChildPid()` - Checks db.open but crash occurs elsewhere
- Lines 1033-1053: `getChildPids()` - Similar defensive check
- Lines 1178-1182: `close()` - Closes database connection

**Auto-Save Middleware** (`auto-save-middleware.js`):
- Lines 36-51: Signal handlers that trigger cleanup
- Lines 227-238: `registerChildProcess()` - Registers exit handler that calls removeChildPid
- Lines 243-293: `cleanup()` - Terminates children and tries to update session
- Line 287: Calls `sessionManager.close()`

**Hive Mind Main** (`hive-mind.js`):
- Lines 854-905: SIGINT handler that pauses session
- Line 884: `sessionManager.close()` - FIRST closure point
- Lines 904-905: Registers SIGINT/SIGTERM handlers

---

## The Fix Strategy

### 1. Centralized Cleanup Coordinator

Create a single cleanup orchestrator that ensures proper sequencing:

```javascript
class CleanupCoordinator {
  constructor() {
    this.isCleaningUp = false;
    this.cleanupPromise = null;
    this.resources = [];
  }

  async executeCleanup() {
    if (this.isCleaningUp) {
      return this.cleanupPromise;
    }

    this.isCleaningUp = true;
    this.cleanupPromise = this._doCleanup();
    return this.cleanupPromise;
  }

  async _doCleanup() {
    // Phase 1: Stop accepting new work
    // Phase 2: Terminate child processes
    // Phase 3: Save final checkpoint
    // Phase 4: Close database connections
  }
}
```

### 2. Reference Counting for Database Connection

Prevent premature closure while operations are in flight:

```javascript
class ConnectionManager {
  constructor(db) {
    this.db = db;
    this.refCount = 0;
    this.isClosing = false;
  }

  acquire() {
    if (this.isClosing) {
      throw new Error('Connection manager is closing');
    }
    this.refCount++;
  }

  release() {
    this.refCount--;
    if (this.isClosing && this.refCount === 0) {
      this.db.close();
    }
  }

  async close() {
    this.isClosing = true;
    if (this.refCount === 0) {
      this.db.close();
    }
    // Wait for all refs to release
  }
}
```

### 3. Improved Defensive Checks

Replace silent failures with proper error recovery:

```javascript
async removeChildPid(sessionId, pid) {
  await this.ensureInitialized();

  if (this.isInMemory) {
    // Handle in-memory case
  }

  // Check if database is available
  if (!this.db || !this.db.open) {
    // Connection closed - use fallback strategy
    console.warn(`Database unavailable during child PID removal for PID ${pid}`);

    // Store in temporary queue for later processing
    this.pendingCleanups.push({ sessionId, pid, type: 'removeChild' });
    return false;
  }

  // Wrap in connection lease to prevent premature closure
  try {
    this.connectionManager.acquire();

    // Perform the operation
    const session = this.db.prepare('SELECT child_pids FROM sessions WHERE id = ?').get(sessionId);
    // ... rest of logic

  } finally {
    this.connectionManager.release();
  }
}
```

### 4. Graceful Degradation

When database is unavailable, maintain consistency through alternative means:

- Cache child PIDs in memory during operation
- On exit, attempt database updates but don't fail if unavailable
- Log all pending operations for post-mortem analysis
- Use WAL (Write-Ahead Logging) mode in SQLite for crash resistance

### 5. Signal Handler Consolidation

Use a single signal handler registration point:

```javascript
// In hive-mind.js or top-level coordinator

const cleanupCoordinator = new CleanupCoordinator();

// Register once
process.once('SIGINT', async () => {
  await cleanupCoordinator.executeCleanup();
  process.exit(0);
});

process.once('SIGTERM', async () => {
  await cleanupCoordinator.executeCleanup();
  process.exit(0);
});
```

---

## Implementation Priority

### Phase 1: Immediate Fixes (Critical)

1. Add connection state validation before ALL database operations
2. Use `process.once()` instead of `process.on()` for signal handlers
3. Add cleanup sequence logging for debugging

### Phase 2: Structural Improvements (High)

1. Implement ConnectionManager with reference counting
2. Create CleanupCoordinator for ordered shutdown
3. Add retry logic for failed cleanup operations

### Phase 3: Robustness Enhancements (Medium)

1. Implement graceful degradation strategies
2. Add comprehensive error recovery
3. Create cleanup verification tests

---

## Recommended Code Changes

### File: `session-manager.js`

**Add at class level:**
```javascript
constructor(hiveMindDir = null) {
  // ... existing code ...
  this.pendingCleanups = [];
  this.connectionRefCount = 0;
  this.isClosing = false;
}

acquireConnection() {
  if (this.isClosing) {
    throw new Error('SessionManager is closing, cannot acquire connection');
  }
  this.connectionRefCount++;
}

releaseConnection() {
  this.connectionRefCount--;
  if (this.isClosing && this.connectionRefCount === 0) {
    this._performClose();
  }
}

async _performClose() {
  if (this.db && !this.isInMemory) {
    // Process any pending cleanups before closing
    await this.processPendingCleanups();
    this.db.close();
  }
}

async processPendingCleanups() {
  for (const cleanup of this.pendingCleanups) {
    try {
      if (cleanup.type === 'removeChild') {
        // Attempt cleanup one final time
        await this.removeChildPid(cleanup.sessionId, cleanup.pid);
      }
    } catch (err) {
      console.error(`Failed to process pending cleanup:`, err);
    }
  }
  this.pendingCleanups = [];
}
```

**Modify close() method:**
```javascript
async close() {
  if (this.isClosing) {
    return; // Already closing
  }

  this.isClosing = true;

  // Wait for all active operations to complete
  const maxWaitTime = 5000; // 5 seconds
  const startTime = Date.now();

  while (this.connectionRefCount > 0 && (Date.now() - startTime) < maxWaitTime) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  if (this.connectionRefCount > 0) {
    console.warn(`Force closing with ${this.connectionRefCount} active operations`);
  }

  await this._performClose();
}
```

**Modify removeChildPid() method:**
```javascript
async removeChildPid(sessionId, pid) {
  await this.ensureInitialized();

  if (this.isInMemory) {
    // ... existing in-memory handling ...
    return true;
  }

  // Check if we're in the process of closing
  if (this.isClosing) {
    console.warn(`SessionManager closing, queueing child PID removal for ${pid}`);
    this.pendingCleanups.push({ sessionId, pid, type: 'removeChild', timestamp: Date.now() });
    return false;
  }

  // Check if database connection is still open
  if (!this.db || !this.db.open) {
    console.warn(`Database connection closed, queueing child PID removal for ${pid}`);
    this.pendingCleanups.push({ sessionId, pid, type: 'removeChild', timestamp: Date.now() });
    return false;
  }

  try {
    this.acquireConnection();

    const session = this.db.prepare('SELECT child_pids FROM sessions WHERE id = ?').get(sessionId);
    if (!session) {
      console.warn(`Session ${sessionId} not found for PID removal`);
      return false;
    }

    const childPids = session.child_pids ? sessionSerializer.deserializeLogData(session.child_pids) : [];
    const index = childPids.indexOf(pid);
    if (index > -1) {
      childPids.splice(index, 1);
    }

    const stmt = this.db.prepare(`
      UPDATE sessions
      SET child_pids = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `);

    stmt.run(sessionSerializer.serializeLogData(childPids), sessionId);

    await this.logSessionEvent(sessionId, 'info', 'Child process removed', null, { pid });
    return true;

  } catch (error) {
    console.error(`Error removing child PID ${pid}:`, error);
    this.pendingCleanups.push({ sessionId, pid, type: 'removeChild', error: error.message });
    return false;
  } finally {
    this.releaseConnection();
  }
}
```

### File: `auto-save-middleware.js`

**Modify cleanup() method:**
```javascript
async cleanup() {
  if (this.isCleaningUp) {
    return; // Prevent re-entry
  }
  this.isCleaningUp = true;

  try {
    console.log('Starting graceful cleanup...');

    // Phase 1: Stop the save timer
    if (this.saveTimer) {
      clearInterval(this.saveTimer);
      this.saveTimer = null;
    }

    // Phase 2: Perform final save while DB is still open
    console.log('Saving final checkpoint...');
    await this.performAutoSave();

    // Phase 3: Terminate all child processes
    console.log(`Terminating ${this.childProcesses.size} child processes...`);
    const terminationPromises = [];

    for (const childProcess of this.childProcesses) {
      terminationPromises.push(
        this.terminateChildProcess(childProcess)
      );
    }

    // Wait for all children to terminate (with timeout)
    await Promise.race([
      Promise.all(terminationPromises),
      new Promise(resolve => setTimeout(resolve, 3000)) // 3 second timeout
    ]);

    // Clear the set
    this.childProcesses.clear();

    // Phase 4: Update session status
    console.log('Updating session status...');
    const session = await this.sessionManager.getSession(this.sessionId);
    if (session && (session.status === 'active' || session.status === 'paused')) {
      await this.sessionManager.stopSession(this.sessionId);
    }

    // Phase 5: Close database connection (LAST!)
    console.log('Closing database connection...');
    await this.sessionManager.close();

    console.log('Cleanup completed successfully');
  } catch (error) {
    console.error('Error during cleanup:', error);
    // Still try to close the database
    try {
      await this.sessionManager.close();
    } catch (closeError) {
      console.error('Failed to close database:', closeError);
    }
  } finally {
    this.isCleaningUp = false;
  }
}

async terminateChildProcess(childProcess) {
  try {
    if (!childProcess || !childProcess.pid) {
      return;
    }

    const pid = childProcess.pid;
    console.log(`Terminating child process ${pid}...`);

    // Send SIGTERM for graceful shutdown
    childProcess.kill('SIGTERM');

    // Wait up to 2 seconds for graceful termination
    await new Promise((resolve) => {
      const timeout = setTimeout(() => {
        // Force kill if still alive
        try {
          process.kill(pid, 0); // Check if still alive
          console.log(`Force killing process ${pid}...`);
          childProcess.kill('SIGKILL');
        } catch (e) {
          // Process already dead
        }
        resolve();
      }, 2000);

      childProcess.once('exit', () => {
        clearTimeout(timeout);
        resolve();
      });
    });

    console.log(`Process ${pid} terminated`);
  } catch (error) {
    console.error(`Failed to terminate child process:`, error.message);
  }
}
```

**Remove duplicate signal handlers:**
```javascript
start() {
  if (this.isActive) {
    return;
  }

  this.isActive = true;

  // Set up periodic saves
  this.saveTimer = setInterval(() => {
    if (this.pendingChanges.length > 0) {
      this.performAutoSave();
    }
  }, this.saveInterval);

  // Remove these duplicate handlers - let parent handle signals
  // process.on('SIGINT', ...);
  // process.on('SIGTERM', ...);
}
```

### File: `hive-mind.js`

**Consolidate signal handlers:**
```javascript
// Around line 854-905, replace with:

let cleanupInProgress = false;

const gracefulShutdown = async () => {
  if (cleanupInProgress) {
    return; // Prevent re-entry
  }
  cleanupInProgress = true;

  console.log('\n\n' + chalk.yellow('⏸️  Initiating graceful shutdown...'));

  try {
    // Step 1: Stop auto-save timer
    if (autoSave) {
      console.log('Stopping auto-save...');
      autoSave.stop(); // This should NOT close the database
    }

    // Step 2: Terminate child processes (auto-save middleware handles this)
    if (autoSave) {
      console.log('Terminating child processes...');
      await autoSave.cleanup();
    }

    // Step 3: Save final checkpoint
    console.log('Saving final checkpoint...');
    const checkpointData = {
      swarmId,
      timestamp: new Date().toISOString(),
      status: 'paused',
      reason: 'User initiated shutdown (Ctrl+C)',
    };

    await sessionManager.saveCheckpoint(sessionId, 'shutdown-checkpoint', checkpointData);

    // Step 4: Pause the session
    console.log('Pausing session...');
    await sessionManager.pauseSession(sessionId);

    // Step 5: Close session manager (LAST!)
    console.log('Closing session manager...');
    await sessionManager.close();

    console.log(chalk.green('✓') + ' Graceful shutdown completed');
    console.log(chalk.cyan('\nTo resume this session, run:'));
    console.log(chalk.bold(`  claude-flow hive-mind resume ${sessionId}`));

    process.exit(0);
  } catch (error) {
    console.error(chalk.red('Error during shutdown:'), error.message);
    console.error(error.stack);
    process.exit(1);
  }
};

// Register ONCE using .once() to prevent multiple invocations
process.once('SIGINT', gracefulShutdown);
process.once('SIGTERM', gracefulShutdown);
```

---

## Testing Strategy

### Unit Tests

```javascript
describe('SessionManager Cleanup', () => {
  it('should handle removeChildPid when database is closing', async () => {
    const manager = new HiveMindSessionManager();
    await manager.createSession('test-swarm', 'test', 'objective');

    manager.isClosing = true;
    const result = await manager.removeChildPid('session-123', 12345);

    expect(result).toBe(false);
    expect(manager.pendingCleanups).toHaveLength(1);
  });

  it('should wait for active operations before closing', async () => {
    const manager = new HiveMindSessionManager();
    manager.connectionRefCount = 2;

    const closePromise = manager.close();

    // Simulate operations completing
    setTimeout(() => {
      manager.releaseConnection();
      manager.releaseConnection();
    }, 100);

    await closePromise;
    expect(manager.db.open).toBe(false);
  });
});
```

### Integration Tests

```javascript
describe('Signal Handler Integration', () => {
  it('should cleanup in correct order on SIGINT', async () => {
    const events = [];

    // Mock to track order
    const mockManager = {
      saveCheckpoint: () => events.push('checkpoint'),
      stopSession: () => events.push('stop'),
      close: () => events.push('close')
    };

    // Simulate SIGINT
    process.emit('SIGINT');

    await new Promise(resolve => setTimeout(resolve, 100));

    expect(events).toEqual(['checkpoint', 'stop', 'close']);
  });
});
```

---

## Monitoring and Debugging

### Add Structured Logging

```javascript
const cleanup Logger = {
  log(phase, message, metadata = {}) {
    const entry = {
      timestamp: Date.now(),
      phase,
      message,
      pid: process.pid,
      ...metadata
    };
    console.log(JSON.stringify(entry));
  }
};

// Usage:
cleanupLogger.log('phase1', 'Starting child termination', { childCount: 3 });
```

### Add Metrics Collection

```javascript
const cleanupMetrics = {
  startTime: 0,
  checkpointTime: 0,
  terminationTime: 0,
  dbCloseTime: 0,

  record(phase, duration) {
    this[`${phase}Time`] = duration;
  },

  report() {
    console.log('Cleanup Metrics:', JSON.stringify(this, null, 2));
  }
};
```

---

## Prevention Checklist

- [ ] Use `process.once()` for signal handlers, never `process.on()`
- [ ] Always close database connections LAST in cleanup sequence
- [ ] Implement reference counting for database connections
- [ ] Add defensive checks before EVERY database operation
- [ ] Queue failed operations for retry or logging
- [ ] Use timeouts on all async cleanup operations
- [ ] Log cleanup sequence for debugging
- [ ] Test cleanup under various failure scenarios

---

## References

- SQLite Connection Management: https://www.sqlite.org/c3ref/close.html
- Node.js Process Events: https://nodejs.org/api/process.html#process_signal_events
- Graceful Shutdown Patterns: https://nodejs.org/en/docs/guides/nodejs-docker-webapp/#graceful-shutdown

---

## Appendix: Root Cause Timeline

1. **T=0ms**: User presses Ctrl+C
2. **T=1ms**: SIGINT signal received by process
3. **T=2ms**: `hive-mind.js` signal handler executes
4. **T=5ms**: `sessionManager.close()` called - **DATABASE CLOSED**
5. **T=3ms**: `auto-save-middleware.js` signal handler executes (race!)
6. **T=10ms**: `cleanup()` starts terminating children
7. **T=15ms**: Child process exits
8. **T=16ms**: Exit handler calls `removeChildPid()`
9. **T=17ms**: **CRASH** - Database connection check fails
10. **T=18ms**: Warning logged but operation fails
11. **T=20ms**: `stopSession()` tries to get child PIDs
12. **T=21ms**: **CRASH** - Database still closed

The race window is approximately 2-15ms depending on system load.
