/**
 * FIXED AUTO-SAVE MIDDLEWARE - Hive Mind Race Condition Fix
 *
 * This file contains fixes for the auto-save middleware to ensure
 * proper cleanup sequencing and prevent duplicate signal handlers.
 *
 * Key Fixes:
 * 1. Remove duplicate SIGINT/SIGTERM handlers
 * 2. Proper cleanup sequencing (children → checkpoint → database)
 * 3. Re-entry protection for cleanup()
 * 4. Graceful child process termination with timeouts
 * 5. Comprehensive error handling and logging
 */

class AutoSaveMiddlewareFixes {

  /**
   * CONSTRUCTOR ADDITIONS
   * Add these fields to existing constructor
   */
  constructorAdditions() {
    // Prevent cleanup re-entry
    this.isCleaningUp = false;

    // Track cleanup metrics
    this.cleanupMetrics = {
      startTime: 0,
      checkpointTime: 0,
      terminationTime: 0,
      dbCloseTime: 0,
      totalTime: 0
    };
  }

  /**
   * FIXED: start() method
   * Removes duplicate signal handlers - let parent coordinate signals
   *
   * Replace the existing start() method with this implementation
   */
  start() {
    if (this.isActive) {
      console.log('[AutoSave] Already active, skipping start');
      return;
    }

    console.log('[AutoSave] Starting auto-save monitoring...');
    this.isActive = true;

    // Set up periodic saves
    this.saveTimer = setInterval(() => {
      if (this.pendingChanges.length > 0) {
        console.log(`[AutoSave] Auto-saving ${this.pendingChanges.length} changes...`);
        this.performAutoSave();
      }
    }, this.saveInterval);

    // REMOVED: Duplicate signal handlers
    // Let the parent hive-mind.js coordinate signal handling
    // This prevents race conditions between multiple handlers

    console.log(`[AutoSave] Active - saving every ${this.saveInterval}ms`);
  }

  /**
   * FIXED: stop() method
   * Gracefully stops auto-save without closing database
   */
  stop() {
    if (!this.isActive) {
      return;
    }

    console.log('[AutoSave] Stopping auto-save monitoring...');

    if (this.saveTimer) {
      clearInterval(this.saveTimer);
      this.saveTimer = null;
    }

    this.isActive = false;

    // Final save
    if (this.pendingChanges.length > 0) {
      console.log('[AutoSave] Performing final save...');
      this.performAutoSave();
    }

    // DO NOT close session manager here
    // Let the parent coordinate the full cleanup sequence
    console.log('[AutoSave] Auto-save stopped successfully');
  }

  /**
   * FIXED: cleanup() method
   * Properly sequences cleanup operations to prevent race conditions
   *
   * Replace the existing cleanup() method with this implementation
   */
  async cleanup() {
    // Prevent re-entry
    if (this.isCleaningUp) {
      console.log('[AutoSave] Cleanup already in progress, skipping');
      return;
    }

    this.isCleaningUp = true;
    this.cleanupMetrics.startTime = Date.now();

    try {
      console.log('[AutoSave] ===== Starting graceful cleanup =====');

      // ===== PHASE 1: Stop accepting new work =====
      console.log('[AutoSave] Phase 1: Stopping auto-save timer...');
      if (this.saveTimer) {
        clearInterval(this.saveTimer);
        this.saveTimer = null;
      }
      this.isActive = false;

      // ===== PHASE 2: Save final checkpoint BEFORE terminating children =====
      console.log('[AutoSave] Phase 2: Saving final checkpoint...');
      const checkpointStart = Date.now();

      try {
        await this.performAutoSave();
        this.cleanupMetrics.checkpointTime = Date.now() - checkpointStart;
        console.log(`[AutoSave] Checkpoint saved (${this.cleanupMetrics.checkpointTime}ms)`);
      } catch (error) {
        console.error('[AutoSave] Failed to save final checkpoint:', error);
        // Continue with cleanup even if save fails
      }

      // ===== PHASE 3: Terminate all child processes =====
      const childCount = this.childProcesses.size;
      console.log(`[AutoSave] Phase 3: Terminating ${childCount} child processes...`);
      const terminationStart = Date.now();

      if (childCount > 0) {
        const terminationPromises = [];

        for (const childProcess of this.childProcesses) {
          terminationPromises.push(
            this.terminateChildProcess(childProcess)
          );
        }

        // Wait for all children to terminate with timeout
        const terminationTimeout = 5000; // 5 seconds
        try {
          await Promise.race([
            Promise.all(terminationPromises),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('Child termination timeout')), terminationTimeout)
            )
          ]);
          console.log('[AutoSave] All child processes terminated successfully');
        } catch (error) {
          console.warn('[AutoSave] Child termination timeout, forcing shutdown');
          // Force kill any remaining processes
          for (const childProcess of this.childProcesses) {
            try {
              if (childProcess.pid) {
                process.kill(childProcess.pid, 'SIGKILL');
              }
            } catch (e) {
              // Already dead, ignore
            }
          }
        }

        this.cleanupMetrics.terminationTime = Date.now() - terminationStart;
        console.log(`[AutoSave] Children terminated (${this.cleanupMetrics.terminationTime}ms)`);
      }

      // Clear the set
      this.childProcesses.clear();

      // ===== PHASE 4: Update session status =====
      console.log('[AutoSave] Phase 4: Updating session status...');

      try {
        const session = await this.sessionManager.getSession(this.sessionId);
        if (session && (session.status === 'active' || session.status === 'paused')) {
          await this.sessionManager.stopSession(this.sessionId);
          console.log('[AutoSave] Session status updated to stopped');
        }
      } catch (error) {
        console.error('[AutoSave] Failed to update session status:', error);
        // Continue - this is not critical
      }

      // ===== PHASE 5: Close database connection LAST =====
      console.log('[AutoSave] Phase 5: Closing database connection...');
      const dbCloseStart = Date.now();

      try {
        await this.sessionManager.close();
        this.cleanupMetrics.dbCloseTime = Date.now() - dbCloseStart;
        console.log(`[AutoSave] Database closed (${this.cleanupMetrics.dbCloseTime}ms)`);
      } catch (error) {
        console.error('[AutoSave] Error closing database:', error);
      }

      this.cleanupMetrics.totalTime = Date.now() - this.cleanupMetrics.startTime;

      console.log('[AutoSave] ===== Cleanup completed successfully =====');
      console.log('[AutoSave] Cleanup metrics:', JSON.stringify(this.cleanupMetrics, null, 2));

    } catch (error) {
      console.error('[AutoSave] Critical error during cleanup:', error);
      console.error(error.stack);

      // Last-ditch effort to close database
      try {
        await this.sessionManager.close();
      } catch (closeError) {
        console.error('[AutoSave] Failed to close database in error handler:', closeError);
      }
    } finally {
      this.isCleaningUp = false;
    }
  }

  /**
   * NEW: Gracefully terminate a single child process
   * Handles SIGTERM → wait → SIGKILL sequence
   */
  async terminateChildProcess(childProcess) {
    if (!childProcess || !childProcess.pid) {
      return;
    }

    const pid = childProcess.pid;
    console.log(`[AutoSave] Terminating child process ${pid}...`);

    try {
      // Check if process is still alive
      process.kill(pid, 0);
    } catch (e) {
      console.log(`[AutoSave] Process ${pid} already terminated`);
      return;
    }

    return new Promise((resolve) => {
      let resolved = false;
      const gracefulTimeout = 2000; // 2 seconds for graceful shutdown
      const forceTimeout = 3000; // 3 seconds total including force kill

      // Set up exit listener
      const onExit = (code, signal) => {
        if (resolved) return;
        resolved = true;
        clearTimeout(gracefulTimer);
        clearTimeout(forceTimer);
        console.log(`[AutoSave] Process ${pid} exited (code: ${code}, signal: ${signal})`);
        resolve();
      };

      childProcess.once('exit', onExit);

      // Graceful shutdown timer
      const gracefulTimer = setTimeout(() => {
        if (resolved) return;

        try {
          // Check if still alive
          process.kill(pid, 0);
          console.log(`[AutoSave] Process ${pid} did not respond to SIGTERM, sending SIGKILL...`);

          // Force kill
          childProcess.kill('SIGKILL');
        } catch (e) {
          // Process already dead
          if (!resolved) {
            resolved = true;
            resolve();
          }
        }
      }, gracefulTimeout);

      // Force timeout - give up and move on
      const forceTimer = setTimeout(() => {
        if (resolved) return;
        resolved = true;
        console.warn(`[AutoSave] Process ${pid} force timeout, continuing cleanup`);
        childProcess.removeListener('exit', onExit);
        resolve();
      }, forceTimeout);

      // Send SIGTERM for graceful shutdown
      try {
        childProcess.kill('SIGTERM');
        console.log(`[AutoSave] Sent SIGTERM to process ${pid}`);
      } catch (error) {
        // Failed to send signal, probably already dead
        if (!resolved) {
          resolved = true;
          clearTimeout(gracefulTimer);
          clearTimeout(forceTimer);
          resolve();
        }
      }
    });
  }

  /**
   * FIXED: registerChildProcess() method
   * Improved error handling for child process tracking
   *
   * Replace the existing registerChildProcess() method
   */
  registerChildProcess(childProcess) {
    if (!childProcess || !childProcess.pid) {
      console.warn('[AutoSave] Cannot register child process: invalid process object');
      return;
    }

    const pid = childProcess.pid;
    console.log(`[AutoSave] Registering child process ${pid}`);

    this.childProcesses.add(childProcess);

    // Add to session manager tracking
    try {
      this.sessionManager.addChildPid(this.sessionId, pid);
    } catch (error) {
      console.error(`[AutoSave] Failed to add child PID to session:`, error);
    }

    // Remove from tracking when process exits
    childProcess.once('exit', async (code, signal) => {
      console.log(`[AutoSave] Child process ${pid} exited (code: ${code}, signal: ${signal})`);

      this.childProcesses.delete(childProcess);

      // Remove from session manager tracking
      // This may fail if database is closing - that's OK
      try {
        await this.sessionManager.removeChildPid(this.sessionId, pid);
      } catch (error) {
        console.warn(`[AutoSave] Failed to remove child PID from session:`, error.message);
        // This is not critical - the session manager will handle it gracefully
      }
    });
  }

  /**
   * NEW: Get cleanup status for monitoring
   */
  getCleanupStatus() {
    return {
      isCleaningUp: this.isCleaningUp,
      activeChildProcesses: this.childProcesses.size,
      childPids: Array.from(this.childProcesses).map(p => p.pid).filter(Boolean),
      metrics: this.cleanupMetrics
    };
  }

  /**
   * NEW: Emergency cleanup
   * Use only in critical scenarios where normal cleanup hangs
   */
  async emergencyCleanup() {
    console.error('[AutoSave] ===== EMERGENCY CLEANUP =====');

    // Force kill all children immediately
    for (const childProcess of this.childProcesses) {
      try {
        if (childProcess.pid) {
          process.kill(childProcess.pid, 'SIGKILL');
          console.error(`[AutoSave] Force killed process ${childProcess.pid}`);
        }
      } catch (e) {
        // Ignore errors
      }
    }

    this.childProcesses.clear();

    // Try to close database
    try {
      if (this.sessionManager) {
        await this.sessionManager.close();
      }
    } catch (e) {
      console.error('[AutoSave] Emergency database close failed:', e);
    }

    console.error('[AutoSave] Emergency cleanup completed');
  }
}

/**
 * USAGE INSTRUCTIONS:
 *
 * 1. Add the constructor additions to AutoSaveMiddleware constructor
 * 2. Add all new methods to the AutoSaveMiddleware class
 * 3. Replace start(), stop(), cleanup(), and registerChildProcess() methods
 * 4. Remove duplicate signal handler registrations
 * 5. Update parent code to coordinate cleanup properly
 *
 * Example parent coordination (hive-mind.js):
 *
 * const gracefulShutdown = async () => {
 *   console.log('Starting coordinated shutdown...');
 *
 *   // Step 1: Stop auto-save (but don't close DB)
 *   if (autoSave) {
 *     autoSave.stop();
 *   }
 *
 *   // Step 2: Let auto-save cleanup children and save checkpoint
 *   if (autoSave) {
 *     await autoSave.cleanup();
 *   }
 *
 *   // Step 3: Done! (cleanup() already closed the database)
 *   console.log('Shutdown complete');
 *   process.exit(0);
 * };
 *
 * process.once('SIGINT', gracefulShutdown);
 * process.once('SIGTERM', gracefulShutdown);
 */

module.exports = AutoSaveMiddlewareFixes;
