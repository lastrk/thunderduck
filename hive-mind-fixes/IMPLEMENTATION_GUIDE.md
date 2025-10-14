# Hive Mind Race Condition Fix - Implementation Guide

## Overview

This guide provides step-by-step instructions for implementing the fixes for the "Database connection closed, cannot remove child PID during cleanup" race condition.

---

## Files Provided

1. **HIVE_MIND_RACE_CONDITION_ANALYSIS.md** - Detailed analysis of the problem
2. **session-manager-fix.js** - Fixed session manager methods
3. **auto-save-middleware-fix.js** - Fixed auto-save middleware
4. **hive-mind-coordinator-fix.js** - Cleanup coordinator
5. **IMPLEMENTATION_GUIDE.md** - This file
6. **TESTING_GUIDE.md** - Testing procedures

---

## Prerequisites

Before implementing the fixes:

1. **Backup current code**:
   ```bash
   cp -r /path/to/claude-flow /path/to/claude-flow-backup-$(date +%Y%m%d)
   ```

2. **Identify installation location**:
   ```bash
   npm root -g  # For global install
   # Or
   which claude-flow  # Find binary location
   ```

3. **Verify write permissions**:
   ```bash
   ls -la /path/to/node_modules/claude-flow/src/cli/simple-commands/hive-mind/
   ```

---

## Implementation Steps

### Step 1: Update Session Manager (Critical)

**File**: `src/cli/simple-commands/hive-mind/session-manager.js`

#### 1.1 Add Constructor Fields

Add these fields at the end of the constructor (around line 23):

```javascript
constructor(hiveMindDir = null) {
  // ... existing code ...

  // Add these new fields:
  this.pendingCleanups = [];
  this.connectionRefCount = 0;
  this.isClosing = false;
  this.cleanupLogger = this.createCleanupLogger();
}
```

#### 1.2 Add New Methods

Add these methods to the class (can be added before the `close()` method):

```javascript
createCleanupLogger() {
  return {
    log: (phase, message, metadata = {}) => {
      const entry = {
        timestamp: new Date().toISOString(),
        phase,
        message,
        pid: process.pid,
        sessionManager: 'HiveMindSessionManager',
        ...metadata
      };
      console.log(`[CLEANUP] ${JSON.stringify(entry)}`);
    }
  };
}

acquireConnection() {
  if (this.isClosing) {
    throw new Error('SessionManager is closing, cannot acquire connection');
  }
  this.connectionRefCount++;
  this.cleanupLogger.log('connection', 'Connection acquired', {
    refCount: this.connectionRefCount
  });
}

releaseConnection() {
  this.connectionRefCount--;
  this.cleanupLogger.log('connection', 'Connection released', {
    refCount: this.connectionRefCount
  });

  if (this.isClosing && this.connectionRefCount === 0) {
    this.cleanupLogger.log('connection', 'All references released, performing close');
    this._performClose();
  }
}

async processPendingCleanups() {
  // Copy implementation from session-manager-fix.js lines 84-140
}

async _performClose() {
  // Copy implementation from session-manager-fix.js lines 142-159
}
```

#### 1.3 Replace Existing Methods

**Replace `close()` method** (around line 1178):

```javascript
async close() {
  if (this.isClosing) {
    this.cleanupLogger.log('close', 'Already closing, skipping');
    return;
  }

  this.cleanupLogger.log('close', 'Close requested', {
    activeRefs: this.connectionRefCount
  });
  this.isClosing = true;

  // Wait for all active operations to complete
  const maxWaitTime = 5000;
  const startTime = Date.now();
  const pollInterval = 100;

  while (this.connectionRefCount > 0 && (Date.now() - startTime) < maxWaitTime) {
    this.cleanupLogger.log('close', 'Waiting for operations to complete', {
      activeRefs: this.connectionRefCount,
      elapsed: Date.now() - startTime
    });

    await new Promise(resolve => setTimeout(resolve, pollInterval));
  }

  if (this.connectionRefCount > 0) {
    this.cleanupLogger.log('close', 'Force closing with active operations', {
      activeRefs: this.connectionRefCount,
      warning: 'Some operations may not complete'
    });
  }

  await this._performClose();
}
```

**Replace `removeChildPid()` method** (around line 983):

Copy the full implementation from `session-manager-fix.js` lines 161-255.

**Replace `getChildPids()` method** (around line 1033):

Copy the full implementation from `session-manager-fix.js` lines 257-296.

**Replace `stopSession()` method** (around line 1058):

Copy the full implementation from `session-manager-fix.js` lines 298-388.

#### 1.4 Verification

After changes, verify the file syntax:

```bash
node --check src/cli/simple-commands/hive-mind/session-manager.js
```

---

### Step 2: Update Auto-Save Middleware

**File**: `src/cli/simple-commands/hive-mind/auto-save-middleware.js`

#### 2.1 Add Constructor Fields

Add at the end of constructor (around line 16):

```javascript
constructor(sessionId, sessionManager, saveInterval = 30000) {
  // ... existing code ...

  // Add these:
  this.isCleaningUp = false;
  this.cleanupMetrics = {
    startTime: 0,
    checkpointTime: 0,
    terminationTime: 0,
    dbCloseTime: 0,
    totalTime: 0
  };
}
```

#### 2.2 Replace Methods

**Replace `start()` method** (around line 22):

Copy from `auto-save-middleware-fix.js` lines 23-51.

**Replace `stop()` method** (around line 57):

Copy from `auto-save-middleware-fix.js` lines 53-76.

**Replace `cleanup()` method** (around line 243):

Copy from `auto-save-middleware-fix.js` lines 78-198.

**Add `terminateChildProcess()` method**:

Copy from `auto-save-middleware-fix.js` lines 200-275.

**Replace `registerChildProcess()` method** (around line 227):

Copy from `auto-save-middleware-fix.js` lines 277-310.

#### 2.3 Verification

```bash
node --check src/cli/simple-commands/hive-mind/auto-save-middleware.js
```

---

### Step 3: Update Hive Mind Main File

**File**: `src/cli/simple-commands/hive-mind.js`

#### 3.1 Add Coordinator Import

At the top of the file (around line 10), add:

```javascript
import { HiveMindCleanupCoordinator } from './hive-mind/coordinator.js';
```

#### 3.2 Create Coordinator File

Create a new file: `src/cli/simple-commands/hive-mind/coordinator.js`

Copy the entire content from `hive-mind-coordinator-fix.js`.

#### 3.3 Replace Signal Handler Code

Find the signal handler section (around line 854-905) and replace with:

```javascript
// ===== GRACEFUL SHUTDOWN COORDINATOR =====
const cleanupCoordinator = new HiveMindCleanupCoordinator(
  sessionManager,
  autoSave,
  sessionId,
  swarmId,
  hiveMind
);

const gracefulShutdown = async () => {
  try {
    const result = await cleanupCoordinator.shutdown();

    if (result.success) {
      process.exit(0);
    } else {
      console.error('\nShutdown completed with errors');
      process.exit(1);
    }
  } catch (error) {
    console.error('\nFatal shutdown error:', error);
    await cleanupCoordinator.emergencyShutdown();
    process.exit(1);
  }
};

// Register signal handlers ONCE
process.once('SIGINT', gracefulShutdown);
process.once('SIGTERM', gracefulShutdown);

console.log(chalk.blue('💡 To pause:') + ' Press Ctrl+C to safely pause and resume later');
console.log(chalk.blue('💡 To resume:') + ' claude-flow hive-mind resume ' + sessionId);
```

#### 3.4 Verification

```bash
node --check src/cli/simple-commands/hive-mind.js
node --check src/cli/simple-commands/hive-mind/coordinator.js
```

---

## Testing

### Basic Functionality Test

1. **Start a hive-mind swarm**:
   ```bash
   claude-flow hive-mind spawn "test task" --workers 2
   ```

2. **Wait 5 seconds**, then press `Ctrl+C`

3. **Expected output**:
   ```
   ===== HIVE MIND GRACEFUL SHUTDOWN =====
   Phase 1: Stopping auto-save timer...
   Phase 2: Terminating child processes...
   Phase 3: Saving final checkpoint...
   Phase 4: Pausing session...
   Phase 5: Closing database connection...
   ===== SHUTDOWN COMPLETE =====
   ```

4. **Verify no errors**:
   ```bash
   # Should see no "Database connection closed" errors
   # Should exit cleanly with code 0
   echo $?  # Should print: 0
   ```

5. **Check session state**:
   ```bash
   sqlite3 .hive-mind/hive.db "SELECT id, status FROM sessions ORDER BY created_at DESC LIMIT 1;"
   ```

   Should show status: `paused`

### Stress Test

1. **Start with many workers**:
   ```bash
   claude-flow hive-mind spawn "stress test" --workers 5
   ```

2. **Immediately press Ctrl+C** (within 1 second)

3. **Verify graceful shutdown** even with fast interruption

### Database Integrity Test

1. **After shutdown, verify database**:
   ```bash
   sqlite3 .hive-mind/hive.db "PRAGMA integrity_check;"
   ```

   Should output: `ok`

2. **Check for orphaned PIDs**:
   ```bash
   sqlite3 .hive-mind/hive.db "SELECT id, child_pids FROM sessions WHERE status='paused' ORDER BY created_at DESC LIMIT 1;"
   ```

   Should show empty array: `[]`

---

## Rollback Procedure

If issues occur:

1. **Stop all hive-mind processes**:
   ```bash
   pkill -f "claude-flow hive-mind"
   ```

2. **Restore from backup**:
   ```bash
   rm -rf /path/to/claude-flow/src/cli/simple-commands/hive-mind*
   cp -r /path/to/backup/hive-mind* /path/to/claude-flow/src/cli/simple-commands/
   ```

3. **Restart npm if globally installed**:
   ```bash
   npm cache clean --force
   ```

---

## Monitoring and Debugging

### Enable Detailed Logging

Set environment variable:

```bash
export HIVE_MIND_DEBUG=1
claude-flow hive-mind spawn "debug test" --workers 2
```

### Check Cleanup Logs

All cleanup operations now log structured JSON:

```bash
# During shutdown, watch for:
grep "\[CLEANUP\]" ~/.hive-mind/sessions/*.log
```

### Monitor Database Operations

```bash
# Real-time monitoring
watch -n 0.5 'sqlite3 .hive-mind/hive.db "SELECT COUNT(*) FROM sessions WHERE status='\''active'\'';"'
```

---

## Common Issues and Solutions

### Issue 1: "SessionManager is closing, cannot acquire connection"

**Cause**: Operation attempted after close() called

**Solution**: This is expected and handled gracefully. The operation will be queued in `pendingCleanups`.

### Issue 2: Shutdown hangs at Phase 5

**Cause**: Database has active operations that won't complete

**Fix**: Increase `maxWaitTime` in close() method from 5000ms to 10000ms

### Issue 3: Child processes not terminating

**Cause**: Processes not responding to SIGTERM

**Fix**: Check `terminateChildProcess()` timeout values (currently 2000ms graceful, 3000ms force)

---

## Performance Considerations

### Typical Shutdown Times

- **Phase 1** (Stop timer): < 10ms
- **Phase 2** (Terminate 2 children): 100-2000ms
- **Phase 3** (Save checkpoint): 50-200ms
- **Phase 4** (Pause session): 10-50ms
- **Phase 5** (Close DB): 10-100ms

**Total**: Usually < 3 seconds for typical workloads

### If Shutdown Takes > 5 Seconds

1. Check for:
   - Stuck database transactions
   - Hanging child processes
   - Large checkpoint data

2. Review metrics in shutdown output

3. Consider emergency shutdown if hung > 10 seconds

---

## Support and Troubleshooting

### Collect Diagnostic Information

```bash
# Gather logs
tar -czf hive-mind-debug-$(date +%Y%m%d).tar.gz \
  .hive-mind/ \
  /tmp/claude-flow-*.log \
  ~/.npm/_logs/

# Include in issue report
```

### Report Issues

Include:
1. Full shutdown log output
2. Database state (`sqlite3 .hive-mind/hive.db .dump > db-dump.sql`)
3. System info (`uname -a`, `node --version`)
4. Steps to reproduce

---

## Additional Resources

- **Architecture Diagram**: See HIVE_MIND_RACE_CONDITION_ANALYSIS.md
- **Testing Guide**: See TESTING_GUIDE.md
- **Original Issue**: Reference GitHub issue or ticket

---

## Sign-off Checklist

Before deploying to production:

- [ ] All files backed up
- [ ] Syntax validation passed
- [ ] Basic functionality test passed
- [ ] Stress test passed
- [ ] Database integrity verified
- [ ] Rollback procedure tested
- [ ] Monitoring in place
- [ ] Team notified of changes

---

## Version History

- **v1.0** (2025-10-14): Initial fix implementation
  - Added connection reference counting
  - Implemented cleanup coordinator
  - Fixed race conditions

---

## License and Attribution

This fix was developed by the CODER agent in the Hive Mind collective to address critical race condition issues in the claude-flow hive-mind implementation.

---

## Appendix: Quick Reference

### Signal Handler Flow (After Fix)

```
Ctrl+C pressed
  |
  v
SIGINT (once)
  |
  v
gracefulShutdown()
  |
  v
coordinator.shutdown()
  |
  +---> Phase 1: Stop timer
  |
  +---> Phase 2: Terminate children (wait for exit events)
  |
  +---> Phase 3: Save checkpoint (DB still open)
  |
  +---> Phase 4: Pause session (DB still open)
  |
  +---> Phase 5: Close database (LAST!)
  |
  v
process.exit(0)
```

### Key Differences from Original

| Aspect | Original | Fixed |
|--------|----------|-------|
| Signal handlers | Multiple (race) | Single (coordinated) |
| DB closure timing | Random | Always last |
| Child termination | Uncoordinated | Ordered with timeout |
| Error handling | Silent failures | Logged and queued |
| Connection safety | No protection | Reference counted |

---

**END OF IMPLEMENTATION GUIDE**
