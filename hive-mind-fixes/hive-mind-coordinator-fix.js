/**
 * HIVE MIND COORDINATOR FIX - Central Cleanup Orchestration
 *
 * This file contains the coordinated cleanup handler for hive-mind.js
 * to replace the signal handlers around lines 854-905.
 *
 * Key Features:
 * 1. Single point of signal handler registration
 * 2. Coordinated cleanup sequence across all components
 * 3. Timeout protection for hanging operations
 * 4. Comprehensive logging and metrics
 * 5. Re-entry protection
 */

class HiveMindCleanupCoordinator {
  constructor(sessionManager, autoSave, sessionId, swarmId, hiveMind) {
    this.sessionManager = sessionManager;
    this.autoSave = autoSave;
    this.sessionId = sessionId;
    this.swarmId = swarmId;
    this.hiveMind = hiveMind;

    this.isShuttingDown = false;
    this.shutdownPromise = null;

    // Metrics for monitoring
    this.metrics = {
      startTime: 0,
      phase1Time: 0, // Stop auto-save
      phase2Time: 0, // Terminate children
      phase3Time: 0, // Save checkpoint
      phase4Time: 0, // Pause session
      phase5Time: 0, // Close database
      totalTime: 0,
      errors: []
    };
  }

  /**
   * Main shutdown orchestration method
   * Ensures all cleanup happens in the correct order
   */
  async shutdown() {
    // Prevent multiple simultaneous shutdowns
    if (this.isShuttingDown) {
      console.log('[Coordinator] Shutdown already in progress, waiting...');
      return this.shutdownPromise;
    }

    this.isShuttingDown = true;
    this.metrics.startTime = Date.now();

    // Store promise for other callers
    this.shutdownPromise = this._executeShutdown();

    return this.shutdownPromise;
  }

  /**
   * Internal shutdown execution
   */
  async _executeShutdown() {
    console.log('\n\n' + '='.repeat(60));
    console.log('          HIVE MIND GRACEFUL SHUTDOWN');
    console.log('='.repeat(60) + '\n');

    try {
      // ===== PHASE 1: Stop accepting new work =====
      console.log('🛑 Phase 1: Stopping auto-save timer...');
      const phase1Start = Date.now();

      try {
        if (this.autoSave && this.autoSave.isActive) {
          this.autoSave.stop();
          console.log('   ✓ Auto-save timer stopped');
        } else {
          console.log('   ℹ Auto-save not active');
        }
      } catch (error) {
        this.recordError('phase1', error);
        console.error('   ✗ Failed to stop auto-save:', error.message);
      }

      this.metrics.phase1Time = Date.now() - phase1Start;

      // ===== PHASE 2: Terminate child processes =====
      console.log('\n🔪 Phase 2: Terminating child processes...');
      const phase2Start = Date.now();

      try {
        if (this.autoSave && this.autoSave.childProcesses.size > 0) {
          const childCount = this.autoSave.childProcesses.size;
          console.log(`   ℹ ${childCount} child process(es) to terminate`);

          // Get child PIDs for logging
          const childPids = Array.from(this.autoSave.childProcesses)
            .map(p => p.pid)
            .filter(Boolean);
          console.log(`   PIDs: [${childPids.join(', ')}]`);

          // Terminate all children
          const terminationPromises = [];
          for (const childProcess of this.autoSave.childProcesses) {
            terminationPromises.push(
              this.autoSave.terminateChildProcess(childProcess)
            );
          }

          // Wait with timeout
          await Promise.race([
            Promise.all(terminationPromises),
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error('Child termination timeout')), 5000)
            )
          ]);

          this.autoSave.childProcesses.clear();
          console.log('   ✓ All child processes terminated');
        } else {
          console.log('   ℹ No child processes to terminate');
        }
      } catch (error) {
        this.recordError('phase2', error);
        console.error('   ✗ Child termination failed:', error.message);
        console.warn('   ⚠ Continuing with cleanup...');
      }

      this.metrics.phase2Time = Date.now() - phase2Start;

      // ===== PHASE 3: Save final checkpoint =====
      console.log('\n💾 Phase 3: Saving final checkpoint...');
      const phase3Start = Date.now();

      try {
        const checkpointData = {
          swarmId: this.swarmId,
          sessionId: this.sessionId,
          timestamp: new Date().toISOString(),
          status: 'paused',
          reason: 'User initiated graceful shutdown (Ctrl+C)',
          hiveMindConfig: this.hiveMind ? {
            name: this.hiveMind.config?.name,
            maxWorkers: this.hiveMind.config?.maxWorkers,
          } : null,
          metrics: {
            totalTime: Date.now() - this.metrics.startTime
          }
        };

        await this.sessionManager.saveCheckpoint(
          this.sessionId,
          'graceful-shutdown',
          checkpointData
        );

        console.log('   ✓ Final checkpoint saved');
      } catch (error) {
        this.recordError('phase3', error);
        console.error('   ✗ Failed to save checkpoint:', error.message);
        console.warn('   ⚠ Continuing with cleanup...');
      }

      this.metrics.phase3Time = Date.now() - phase3Start;

      // ===== PHASE 4: Pause session =====
      console.log('\n⏸️  Phase 4: Pausing session...');
      const phase4Start = Date.now();

      try {
        await this.sessionManager.pauseSession(this.sessionId);
        console.log('   ✓ Session paused');
      } catch (error) {
        this.recordError('phase4', error);
        console.error('   ✗ Failed to pause session:', error.message);
        console.warn('   ⚠ Continuing with cleanup...');
      }

      this.metrics.phase4Time = Date.now() - phase4Start;

      // ===== PHASE 5: Close database connection =====
      console.log('\n🔒 Phase 5: Closing database connection...');
      const phase5Start = Date.now();

      try {
        // This will wait for any active operations to complete
        await Promise.race([
          this.sessionManager.close(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Database close timeout')), 10000)
          )
        ]);

        console.log('   ✓ Database connection closed');
      } catch (error) {
        this.recordError('phase5', error);
        console.error('   ✗ Failed to close database:', error.message);
        console.warn('   ⚠ Database may not have closed cleanly');
      }

      this.metrics.phase5Time = Date.now() - phase5Start;
      this.metrics.totalTime = Date.now() - this.metrics.startTime;

      // ===== SHUTDOWN COMPLETE =====
      console.log('\n' + '='.repeat(60));
      console.log('          SHUTDOWN COMPLETE');
      console.log('='.repeat(60));

      this.printMetrics();
      this.printResumeInstructions();

      return { success: true, metrics: this.metrics };

    } catch (error) {
      console.error('\n' + '='.repeat(60));
      console.error('          SHUTDOWN FAILED');
      console.error('='.repeat(60));
      console.error('\nCritical error during shutdown:', error);
      console.error(error.stack);

      this.recordError('critical', error);
      this.metrics.totalTime = Date.now() - this.metrics.startTime;

      // Last-ditch cleanup attempt
      console.error('\nAttempting emergency cleanup...');
      try {
        await this.sessionManager.close();
      } catch (e) {
        console.error('Emergency close failed:', e.message);
      }

      return { success: false, metrics: this.metrics, error };
    }
  }

  /**
   * Record an error for metrics
   */
  recordError(phase, error) {
    this.metrics.errors.push({
      phase,
      message: error.message,
      stack: error.stack,
      timestamp: Date.now()
    });
  }

  /**
   * Print cleanup metrics
   */
  printMetrics() {
    console.log('\n📊 Cleanup Metrics:');
    console.log('   Phase 1 (Stop auto-save):     ' + this.metrics.phase1Time + 'ms');
    console.log('   Phase 2 (Terminate children): ' + this.metrics.phase2Time + 'ms');
    console.log('   Phase 3 (Save checkpoint):    ' + this.metrics.phase3Time + 'ms');
    console.log('   Phase 4 (Pause session):      ' + this.metrics.phase4Time + 'ms');
    console.log('   Phase 5 (Close database):     ' + this.metrics.phase5Time + 'ms');
    console.log('   ─────────────────────────────────────────────');
    console.log('   Total:                        ' + this.metrics.totalTime + 'ms');

    if (this.metrics.errors.length > 0) {
      console.log('\n⚠️  Errors encountered:');
      this.metrics.errors.forEach((err, i) => {
        console.log(`   ${i + 1}. [${err.phase}] ${err.message}`);
      });
    }
  }

  /**
   * Print resume instructions
   */
  printResumeInstructions() {
    console.log('\n💡 To resume this session, run:');
    console.log('   claude-flow hive-mind resume ' + this.sessionId);
    console.log();
  }

  /**
   * Emergency shutdown for critical scenarios
   */
  async emergencyShutdown() {
    console.error('\n' + '!'.repeat(60));
    console.error('          EMERGENCY SHUTDOWN');
    console.error('!'.repeat(60) + '\n');

    // Force kill all children
    if (this.autoSave) {
      try {
        await this.autoSave.emergencyCleanup();
      } catch (e) {
        console.error('Emergency cleanup failed:', e);
      }
    }

    // Force close database
    try {
      if (this.sessionManager && this.sessionManager.db) {
        this.sessionManager.db.close();
      }
    } catch (e) {
      console.error('Emergency database close failed:', e);
    }

    console.error('\nEmergency shutdown completed - session may be in inconsistent state');
  }
}

/**
 * USAGE IN hive-mind.js (around line 854-905):
 *
 * Replace the existing signal handler code with:
 */
function createCleanupHandlerExample(sessionManager, autoSave, sessionId, swarmId, hiveMind) {
  // Create coordinator
  const coordinator = new HiveMindCleanupCoordinator(
    sessionManager,
    autoSave,
    sessionId,
    swarmId,
    hiveMind
  );

  // Single shutdown handler
  const gracefulShutdown = async () => {
    try {
      const result = await coordinator.shutdown();

      if (result.success) {
        process.exit(0);
      } else {
        console.error('\nShutdown completed with errors');
        process.exit(1);
      }
    } catch (error) {
      console.error('\nFatal shutdown error:', error);
      await coordinator.emergencyShutdown();
      process.exit(1);
    }
  };

  // Register ONCE using .once() to prevent multiple invocations
  process.once('SIGINT', gracefulShutdown);
  process.once('SIGTERM', gracefulShutdown);

  // Optional: Unhandled rejection handler
  process.once('unhandledRejection', async (reason, promise) => {
    console.error('\nUnhandled Promise Rejection during shutdown:', reason);
    await coordinator.emergencyShutdown();
    process.exit(1);
  });

  console.log('✓ Graceful shutdown handlers registered');

  return coordinator;
}

/**
 * COMPLETE REPLACEMENT CODE FOR hive-mind.js lines 854-905:
 */
const replacementCode = `
// ===== GRACEFUL SHUTDOWN COORDINATOR =====
// Create cleanup coordinator for orderly shutdown
const cleanupCoordinator = new HiveMindCleanupCoordinator(
  sessionManager,
  autoSave,
  sessionId,
  swarmId,
  hiveMind
);

// Single graceful shutdown handler
const gracefulShutdown = async () => {
  try {
    const result = await cleanupCoordinator.shutdown();

    if (result.success) {
      process.exit(0);
    } else {
      console.error('\\nShutdown completed with errors');
      process.exit(1);
    }
  } catch (error) {
    console.error('\\nFatal shutdown error:', error);
    await cleanupCoordinator.emergencyShutdown();
    process.exit(1);
  }
};

// Register signal handlers ONCE
process.once('SIGINT', gracefulShutdown);
process.once('SIGTERM', gracefulShutdown);

console.log(chalk.blue('💡 To pause:') + ' Press Ctrl+C to safely pause and resume later');
console.log(chalk.blue('💡 To resume:') + ' claude-flow hive-mind resume ' + sessionId);
`;

module.exports = {
  HiveMindCleanupCoordinator,
  createCleanupHandlerExample,
  replacementCode
};
