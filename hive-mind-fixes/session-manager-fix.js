/**
 * FIXED SESSION MANAGER - Hive Mind Race Condition Fix
 *
 * This file contains the critical fixes for the database connection
 * race condition that causes "Database connection closed, cannot remove
 * child PID during cleanup" errors.
 *
 * Key Fixes:
 * 1. Connection reference counting to prevent premature closure
 * 2. Pending cleanup queue for operations attempted during closure
 * 3. Graceful degradation when database is unavailable
 * 4. Proper async close() with waiting for active operations
 * 5. Comprehensive error handling and logging
 */

// Add these methods to the HiveMindSessionManager class

class HiveMindSessionManagerFixes {

  /**
   * CONSTRUCTOR ADDITIONS
   * Add these fields to the existing constructor
   */
  constructorAdditions() {
    // Track pending cleanups when DB is closing/closed
    this.pendingCleanups = [];

    // Reference counting for database connections
    this.connectionRefCount = 0;

    // Flag to indicate closing state
    this.isClosing = false;

    // Cleanup logger for debugging
    this.cleanupLogger = this.createCleanupLogger();
  }

  /**
   * Create structured logger for cleanup operations
   */
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

  /**
   * Acquire a connection reference
   * Prevents premature closure while operations are active
   */
  acquireConnection() {
    if (this.isClosing) {
      throw new Error('SessionManager is closing, cannot acquire connection');
    }
    this.connectionRefCount++;
    this.cleanupLogger.log('connection', 'Connection acquired', {
      refCount: this.connectionRefCount
    });
  }

  /**
   * Release a connection reference
   * Triggers actual closure if closing and no refs remaining
   */
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

  /**
   * Process any pending cleanup operations
   * Attempts to execute queued operations before final closure
   */
  async processPendingCleanups() {
    if (this.pendingCleanups.length === 0) {
      return;
    }

    this.cleanupLogger.log('pending', `Processing ${this.pendingCleanups.length} pending cleanups`);

    // Sort by timestamp to maintain order
    this.pendingCleanups.sort((a, b) => a.timestamp - b.timestamp);

    for (const cleanup of this.pendingCleanups) {
      try {
        if (cleanup.type === 'removeChild') {
          this.cleanupLogger.log('pending', `Retrying child PID removal`, {
            pid: cleanup.pid,
            sessionId: cleanup.sessionId
          });

          // Direct database operation since we're in final cleanup
          if (this.db && this.db.open) {
            const session = this.db.prepare('SELECT child_pids FROM sessions WHERE id = ?')
              .get(cleanup.sessionId);

            if (session && session.child_pids) {
              const childPids = JSON.parse(session.child_pids);
              const index = childPids.indexOf(cleanup.pid);

              if (index > -1) {
                childPids.splice(index, 1);
                this.db.prepare(`
                  UPDATE sessions
                  SET child_pids = ?, updated_at = CURRENT_TIMESTAMP
                  WHERE id = ?
                `).run(JSON.stringify(childPids), cleanup.sessionId);

                this.cleanupLogger.log('pending', 'Successfully removed child PID', {
                  pid: cleanup.pid
                });
              }
            }
          }
        }
      } catch (err) {
        this.cleanupLogger.log('pending', `Failed to process pending cleanup`, {
          error: err.message,
          cleanup
        });
      }
    }

    this.pendingCleanups = [];
  }

  /**
   * Perform actual database closure
   * Internal method called when safe to close
   */
  async _performClose() {
    if (!this.db || this.isInMemory) {
      return;
    }

    try {
      this.cleanupLogger.log('close', 'Starting database closure');

      // Process any pending cleanups before closing
      await this.processPendingCleanups();

      // Close the database connection
      this.db.close();
      this.cleanupLogger.log('close', 'Database closed successfully');
    } catch (error) {
      this.cleanupLogger.log('close', 'Error closing database', {
        error: error.message
      });
    }
  }

  /**
   * FIXED: close() method
   * Gracefully waits for active operations before closing
   *
   * Replace the existing close() method with this implementation
   */
  async close() {
    if (this.isClosing) {
      this.cleanupLogger.log('close', 'Already closing, skipping');
      return; // Already closing
    }

    this.cleanupLogger.log('close', 'Close requested', {
      activeRefs: this.connectionRefCount
    });
    this.isClosing = true;

    // Wait for all active operations to complete
    const maxWaitTime = 5000; // 5 seconds
    const startTime = Date.now();
    const pollInterval = 100; // Check every 100ms

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

  /**
   * FIXED: removeChildPid() method
   * Properly handles closed connections and uses reference counting
   *
   * Replace the existing removeChildPid() method with this implementation
   */
  async removeChildPid(sessionId, pid) {
    await this.ensureInitialized();

    this.cleanupLogger.log('removeChild', 'Remove child PID requested', {
      sessionId,
      pid
    });

    if (this.isInMemory) {
      // Use in-memory storage
      const session = this.memoryStore.sessions.get(sessionId);
      if (!session) {
        this.cleanupLogger.log('removeChild', 'Session not found in memory', { sessionId });
        return false;
      }

      const childPids = session.child_pids ?
        JSON.parse(session.child_pids) : [];
      const index = childPids.indexOf(pid);

      if (index > -1) {
        childPids.splice(index, 1);
        session.child_pids = JSON.stringify(childPids);
        session.updated_at = new Date().toISOString();

        this.cleanupLogger.log('removeChild', 'Child PID removed from memory', { pid });
        await this.logSessionEvent(sessionId, 'info', 'Child process removed', null, { pid });
      }

      return true;
    }

    // Check if we're in the process of closing
    if (this.isClosing) {
      this.cleanupLogger.log('removeChild', 'Manager closing, queueing operation', {
        sessionId,
        pid
      });

      this.pendingCleanups.push({
        sessionId,
        pid,
        type: 'removeChild',
        timestamp: Date.now()
      });
      return false;
    }

    // Check if database connection is still open
    if (!this.db || !this.db.open) {
      this.cleanupLogger.log('removeChild', 'Database closed, queueing operation', {
        sessionId,
        pid
      });

      this.pendingCleanups.push({
        sessionId,
        pid,
        type: 'removeChild',
        timestamp: Date.now()
      });
      return false;
    }

    // Acquire connection reference to prevent closure during operation
    try {
      this.acquireConnection();

      const session = this.db.prepare('SELECT child_pids FROM sessions WHERE id = ?')
        .get(sessionId);

      if (!session) {
        this.cleanupLogger.log('removeChild', 'Session not found in database', { sessionId });
        return false;
      }

      const childPids = session.child_pids ?
        JSON.parse(session.child_pids) : [];
      const index = childPids.indexOf(pid);

      if (index > -1) {
        childPids.splice(index, 1);
      } else {
        this.cleanupLogger.log('removeChild', 'PID not found in child list', {
          pid,
          existingPids: childPids
        });
      }

      const stmt = this.db.prepare(`
        UPDATE sessions
        SET child_pids = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `);

      stmt.run(JSON.stringify(childPids), sessionId);

      this.cleanupLogger.log('removeChild', 'Child PID removed successfully', { pid });
      await this.logSessionEvent(sessionId, 'info', 'Child process removed', null, { pid });

      return true;

    } catch (error) {
      this.cleanupLogger.log('removeChild', 'Error removing child PID', {
        pid,
        error: error.message,
        stack: error.stack
      });

      // Queue for retry
      this.pendingCleanups.push({
        sessionId,
        pid,
        type: 'removeChild',
        error: error.message,
        timestamp: Date.now()
      });

      return false;
    } finally {
      this.releaseConnection();
    }
  }

  /**
   * FIXED: getChildPids() method
   * Properly handles closed connections with graceful fallback
   *
   * Replace the existing getChildPids() method with this implementation
   */
  async getChildPids(sessionId) {
    await this.ensureInitialized();

    this.cleanupLogger.log('getChildPids', 'Get child PIDs requested', { sessionId });

    if (this.isInMemory) {
      const session = this.memoryStore.sessions.get(sessionId);
      if (!session || !session.child_pids) {
        return [];
      }
      return JSON.parse(session.child_pids);
    }

    // If closing or database closed, return empty array
    // This prevents crashes during shutdown
    if (this.isClosing || !this.db || !this.db.open) {
      this.cleanupLogger.log('getChildPids', 'Database unavailable, returning empty array', {
        isClosing: this.isClosing,
        dbOpen: this.db && this.db.open
      });
      return [];
    }

    try {
      this.acquireConnection();

      const session = this.db.prepare('SELECT child_pids FROM sessions WHERE id = ?')
        .get(sessionId);

      if (!session || !session.child_pids) {
        return [];
      }

      const pids = JSON.parse(session.child_pids);
      this.cleanupLogger.log('getChildPids', 'Retrieved child PIDs', {
        count: pids.length,
        pids
      });

      return pids;

    } catch (error) {
      this.cleanupLogger.log('getChildPids', 'Error getting child PIDs', {
        error: error.message
      });
      return [];
    } finally {
      this.releaseConnection();
    }
  }

  /**
   * FIXED: stopSession() method
   * Handles cases where child PID retrieval may fail
   *
   * Replace lines 1058-1106 with this implementation
   */
  async stopSession(sessionId) {
    this.cleanupLogger.log('stopSession', 'Stop session requested', { sessionId });

    const session = await this.getSession(sessionId);
    if (!session) {
      throw new Error(`Session ${sessionId} not found`);
    }

    // Get child PIDs (may return empty if DB is closing)
    const childPids = await this.getChildPids(sessionId);

    this.cleanupLogger.log('stopSession', 'Terminating child processes', {
      count: childPids.length,
      pids: childPids
    });

    // Terminate child processes
    for (const pid of childPids) {
      try {
        // Check if process is still alive
        process.kill(pid, 0);

        // Process exists, send SIGTERM
        process.kill(pid, 'SIGTERM');

        this.cleanupLogger.log('stopSession', 'Child process terminated', { pid });
        await this.logSessionEvent(sessionId, 'info', 'Child process terminated', null, { pid });
      } catch (err) {
        // Process might already be dead or we don't have permission
        this.cleanupLogger.log('stopSession', 'Child process termination failed or not needed', {
          pid,
          error: err.message
        });

        await this.logSessionEvent(sessionId, 'warning', 'Failed to terminate child process', null, {
          pid,
          error: err.message,
        });
      }
    }

    // Update session status
    if (this.isInMemory) {
      const sessionData = this.memoryStore.sessions.get(sessionId);
      if (sessionData) {
        sessionData.status = 'stopped';
        sessionData.updated_at = new Date().toISOString();
      }
    } else if (this.db && this.db.open && !this.isClosing) {
      try {
        this.acquireConnection();

        const stmt = this.db.prepare(`
          UPDATE sessions
          SET status = 'stopped', updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `);

        stmt.run(sessionId);

        // Update swarm status
        this.db.prepare('UPDATE swarms SET status = ? WHERE id = ?')
          .run('stopped', session.swarm_id);

        this.cleanupLogger.log('stopSession', 'Session status updated to stopped');
      } catch (error) {
        this.cleanupLogger.log('stopSession', 'Error updating session status', {
          error: error.message
        });
      } finally {
        this.releaseConnection();
      }
    } else {
      this.cleanupLogger.log('stopSession', 'Database unavailable, skipping status update');
    }

    await this.logSessionEvent(sessionId, 'info', 'Session stopped');

    return true;
  }

  /**
   * Get cleanup statistics for monitoring
   */
  getCleanupStats() {
    return {
      isClosing: this.isClosing,
      activeConnections: this.connectionRefCount,
      pendingCleanups: this.pendingCleanups.length,
      pendingOperations: this.pendingCleanups.map(c => ({
        type: c.type,
        pid: c.pid,
        age: Date.now() - c.timestamp
      }))
    };
  }

  /**
   * Force flush all pending operations
   * Use only in emergency shutdown scenarios
   */
  async forceFlushPendingOperations() {
    this.cleanupLogger.log('force', 'Force flushing pending operations', {
      count: this.pendingCleanups.length
    });

    if (this.db && this.db.open) {
      await this.processPendingCleanups();
    } else {
      this.cleanupLogger.log('force', 'Database unavailable, logging pending operations', {
        operations: this.pendingCleanups
      });
    }
  }
}

/**
 * USAGE INSTRUCTIONS:
 *
 * 1. Add the constructor additions to HiveMindSessionManager constructor
 * 2. Add all the new methods to the HiveMindSessionManager class
 * 3. Replace the existing close(), removeChildPid(), getChildPids(), and stopSession() methods
 * 4. Update any code that calls sessionManager.close() to use await
 *
 * Example:
 *
 * // OLD:
 * sessionManager.close();
 *
 * // NEW:
 * await sessionManager.close();
 *
 * // With timeout:
 * await Promise.race([
 *   sessionManager.close(),
 *   new Promise((_, reject) =>
 *     setTimeout(() => reject(new Error('Close timeout')), 10000)
 *   )
 * ]);
 */

module.exports = HiveMindSessionManagerFixes;
