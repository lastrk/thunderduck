package com.thunderduck.connect.session;

import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.runtime.ArrowStreamingExecutor;
import com.thunderduck.runtime.DuckDBRuntime;
import com.thunderduck.runtime.QueryExecutor;
import com.thunderduck.runtime.StreamingConfig;
import com.thunderduck.types.StructType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a single Spark Connect session.
 *
 * <p>Each session maintains:
 * <ul>
 *   <li>Unique session ID</li>
 *   <li>DuckDB runtime (owns the database connection)</li>
 *   <li>Session-scoped configuration</li>
 *   <li>Temporary view registry</li>
 *   <li>Creation timestamp</li>
 * </ul>
 *
 * <p>The session owns its DuckDBRuntime and is responsible for closing it
 * when the session is closed. This ensures proper resource cleanup and
 * isolation between sessions.
 */
public class Session implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private final String sessionId;
    private final DuckDBRuntime runtime;
    private final Path databasePath;  // null for in-memory databases
    private final long createdAt;
    private final Map<String, String> config;
    private final Map<String, LogicalPlan> tempViews;
    private final Map<String, StructType> viewSchemas = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    // Cached executors - created lazily, shared across all operations in this session
    private volatile BufferAllocator sharedAllocator;
    private volatile QueryExecutor cachedQueryExecutor;
    private volatile ArrowStreamingExecutor cachedStreamingExecutor;

    /**
     * Create a new session with the given ID and an in-memory DuckDB database.
     *
     * <p>Creates a new in-memory DuckDB database named after the session ID.
     * Data does not persist after session close.
     *
     * @param sessionId Unique session identifier (typically UUID from client)
     */
    public Session(String sessionId) {
        this(sessionId, DuckDBRuntime.create("jdbc:duckdb::memory:" + sanitizeSessionId(sessionId)), null);
    }

    /**
     * Create a new session with a persistent on-disk DuckDB database.
     *
     * <p>Creates a new DuckDB database file in the sessions directory.
     * Data persists across session reconnects with the same session ID.
     * The database file is deleted when {@link #cleanup()} is called.
     *
     * @param sessionId Unique session identifier
     * @param sessionsDir Directory where session database files are stored
     */
    public Session(String sessionId, Path sessionsDir) {
        this.sessionId = sessionId;
        this.databasePath = sessionsDir.resolve(sanitizeSessionId(sessionId) + ".duckdb");
        this.runtime = DuckDBRuntime.createPersistent(databasePath.toString());
        this.createdAt = System.currentTimeMillis();
        this.config = new HashMap<>();
        this.tempViews = new ConcurrentHashMap<>();

        // Set default configuration
        config.putAll(SparkDefaults.getDefaults());
        config.put("spark.app.name", "thunderduck-connect");
        config.put("spark.sql.dialect", "spark");

        logger.info("Created persistent session {} with database at {}", sessionId, databasePath);
    }

    /**
     * Create a new session with the given ID and DuckDB runtime.
     *
     * <p>This constructor allows injecting a custom runtime, useful for testing.
     *
     * @param sessionId Unique session identifier
     * @param runtime DuckDB runtime for this session
     * @param databasePath Path to database file (null for in-memory)
     */
    public Session(String sessionId, DuckDBRuntime runtime, Path databasePath) {
        this.sessionId = sessionId;
        this.runtime = runtime;
        this.databasePath = databasePath;
        this.createdAt = System.currentTimeMillis();
        this.config = new HashMap<>();
        this.tempViews = new ConcurrentHashMap<>();

        // Set default configuration
        config.putAll(SparkDefaults.getDefaults());
        config.put("spark.app.name", "thunderduck-connect");
        config.put("spark.sql.dialect", "spark");
    }

    /**
     * Sanitize session ID for use in DuckDB database name.
     *
     * <p>DuckDB database names have restrictions, so we sanitize the session ID
     * to ensure it's valid.
     *
     * @param sessionId Original session ID
     * @return Sanitized session ID safe for database naming
     */
    private static String sanitizeSessionId(String sessionId) {
        // Replace non-alphanumeric chars with underscore, limit length
        return sessionId.replaceAll("[^a-zA-Z0-9]", "_").substring(0, Math.min(sessionId.length(), 50));
    }

    /**
     * Get session ID.
     *
     * @return Session identifier
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get the DuckDB runtime for this session.
     *
     * @return DuckDB runtime
     * @throws IllegalStateException if session is closed
     */
    public DuckDBRuntime getRuntime() {
        if (closed) {
            throw new IllegalStateException("Session is closed: " + sessionId);
        }
        return runtime;
    }

    /**
     * Get a shared BufferAllocator for this session.
     *
     * <p>The allocator is created lazily and reused across all operations
     * in this session, reducing memory allocation overhead.
     *
     * @return Shared BufferAllocator for this session
     * @throws IllegalStateException if session is closed
     */
    public BufferAllocator getSharedAllocator() {
        if (closed) {
            throw new IllegalStateException("Session is closed: " + sessionId);
        }
        if (sharedAllocator == null) {
            synchronized (this) {
                if (sharedAllocator == null) {
                    sharedAllocator = new RootAllocator(Long.MAX_VALUE);
                    logger.debug("Created shared allocator for session {}", sessionId);
                }
            }
        }
        return sharedAllocator;
    }

    /**
     * Get a cached QueryExecutor for this session.
     *
     * <p>The executor is created lazily and reused across all operations
     * in this session, avoiding repeated RootAllocator creation overhead.
     *
     * @return Cached QueryExecutor for this session
     * @throws IllegalStateException if session is closed
     */
    public QueryExecutor getQueryExecutor() {
        if (closed) {
            throw new IllegalStateException("Session is closed: " + sessionId);
        }
        if (cachedQueryExecutor == null) {
            synchronized (this) {
                if (cachedQueryExecutor == null) {
                    cachedQueryExecutor = new QueryExecutor(runtime);
                    logger.debug("Created cached QueryExecutor for session {}", sessionId);
                }
            }
        }
        return cachedQueryExecutor;
    }

    /**
     * Get a cached ArrowStreamingExecutor for this session.
     *
     * <p>The executor is created lazily and reused across all operations
     * in this session. Uses the session's shared allocator, which allows
     * the executor to be reused without creating a new RootAllocator for
     * each query (reducing per-query overhead by 2-5ms).
     *
     * <p>The executor does NOT own the allocator - the session owns it and
     * will close it when the session is closed.
     *
     * @return Cached ArrowStreamingExecutor for this session
     * @throws IllegalStateException if session is closed
     */
    public ArrowStreamingExecutor getStreamingExecutor() {
        if (closed) {
            throw new IllegalStateException("Session is closed: " + sessionId);
        }
        if (cachedStreamingExecutor == null) {
            synchronized (this) {
                if (cachedStreamingExecutor == null) {
                    // Use session's shared allocator - executor does NOT own it
                    // This allows reusing the executor across multiple queries
                    cachedStreamingExecutor = new ArrowStreamingExecutor(
                        runtime,
                        getSharedAllocator(),
                        StreamingConfig.DEFAULT_BATCH_SIZE
                    );
                    logger.debug("Created cached ArrowStreamingExecutor for session {} with shared allocator",
                        sessionId);
                }
            }
        }
        return cachedStreamingExecutor;
    }

    /**
     * Get session creation timestamp.
     *
     * @return Timestamp in milliseconds since epoch
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Get configuration value.
     *
     * @param key Configuration key
     * @return Configuration value, or null if not set
     */
    public String getConfig(String key) {
        return config.get(key);
    }

    /**
     * Set configuration value.
     *
     * @param key Configuration key
     * @param value Configuration value
     */
    public void setConfig(String key, String value) {
        config.put(key, value);
    }

    /**
     * Get all configuration entries.
     *
     * @return Immutable copy of configuration map
     */
    public Map<String, String> getAllConfig() {
        return new HashMap<>(config);
    }

    /**
     * Register a temporary view.
     *
     * @param name View name
     * @param plan Logical plan representing the view
     */
    public void registerTempView(String name, LogicalPlan plan) {
        tempViews.put(name, plan);
        // Cache the plan's inferred schema so that later references to this view
        // use the correct nullable flags instead of DuckDB's DESCRIBE (all nullable).
        try {
            StructType schema = plan.schema();
            if (schema != null) {
                viewSchemas.put(name, schema);
                logger.debug("Cached view schema for '{}': {}", name, schema);
            }
        } catch (Exception e) {
            logger.debug("Could not cache schema for view '{}': {}", name, e.getMessage());
        }
    }

    /**
     * Get a temporary view by name.
     *
     * @param name View name
     * @return Logical plan if view exists, empty otherwise
     */
    public Optional<LogicalPlan> getTempView(String name) {
        return Optional.ofNullable(tempViews.get(name));
    }

    /**
     * Drop a temporary view.
     *
     * @param name View name
     * @return true if view existed and was dropped, false otherwise
     */
    public boolean dropTempView(String name) {
        viewSchemas.remove(name);
        return tempViews.remove(name) != null;
    }

    /**
     * Get all temporary view names.
     *
     * @return Set of view names
     */
    public Set<String> getTempViewNames() {
        return tempViews.keySet();
    }

    /**
     * Clear all temporary views.
     */
    public void clearTempViews() {
        tempViews.clear();
        viewSchemas.clear();
    }

    /**
     * Cache a view's schema directly (for SQL-created temp views).
     *
     * <p>Unlike {@link #registerTempView(String, LogicalPlan)} which stores the full plan,
     * this method only caches the schema. Used when a CREATE TEMP VIEW is executed via
     * SQL (not the DataFrame API), where the view already exists in DuckDB.
     *
     * @param name view name
     * @param schema the inferred schema
     */
    public void cacheViewSchema(String name, StructType schema) {
        viewSchemas.put(name, schema);
        logger.debug("Cached view schema for '{}': {}", name, schema);
    }

    /**
     * Get the view schema cache (live reference).
     *
     * @return unmodifiable view of the view schema cache
     */
    public Map<String, StructType> getViewSchemas() {
        return Collections.unmodifiableMap(viewSchemas);
    }

    /**
     * Check if this session is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Get the database path for persistent sessions.
     *
     * @return Path to database file, or null for in-memory sessions
     */
    public Path getDatabasePath() {
        return databasePath;
    }

    /**
     * Check if this session uses a persistent database.
     *
     * @return true if session uses an on-disk database
     */
    public boolean isPersistent() {
        return databasePath != null;
    }

    /**
     * Close the session and release all resources.
     *
     * <p>Closes the DuckDB runtime and clears all temp views.
     * For persistent sessions, call {@link #cleanup()} to also delete database files.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // Clear temp views and cached schemas
        tempViews.clear();
        viewSchemas.clear();

        // Close cached executors (must be closed before allocator and runtime)
        if (cachedStreamingExecutor != null) {
            try {
                cachedStreamingExecutor.close();
            } catch (Exception e) {
                logger.warn("Error closing ArrowStreamingExecutor for session {}: {}",
                    sessionId, e.getMessage());
            }
            cachedStreamingExecutor = null;
        }

        // Note: QueryExecutor doesn't implement AutoCloseable, but it holds resources
        // that are tied to the runtime. Setting to null allows GC.
        cachedQueryExecutor = null;

        // Close shared allocator
        if (sharedAllocator != null) {
            try {
                sharedAllocator.close();
            } catch (Exception e) {
                logger.warn("Error closing BufferAllocator for session {}: {}",
                    sessionId, e.getMessage());
            }
            sharedAllocator = null;
        }

        // Close DuckDB runtime
        if (runtime != null) {
            runtime.close();
        }
    }

    /**
     * Close the session and delete persistent database files.
     *
     * <p>This method should be called when the session is being permanently removed
     * (e.g., on expiry). It closes the session and deletes the database file and
     * any associated files (WAL, etc.).
     *
     * <p>For in-memory sessions, this is equivalent to {@link #close()}.
     */
    public void cleanup() {
        // First close the session and runtime
        close();

        // Then delete database files if persistent
        if (databasePath != null) {
            try {
                // Delete main database file
                Files.deleteIfExists(databasePath);
                logger.info("Deleted session database: {}", databasePath);

                // Delete WAL file if exists
                Path walPath = databasePath.resolveSibling(databasePath.getFileName() + ".wal");
                Files.deleteIfExists(walPath);

                // Delete any other DuckDB temp files
                Path tmpPath = databasePath.resolveSibling(databasePath.getFileName() + ".tmp");
                Files.deleteIfExists(tmpPath);

            } catch (IOException e) {
                logger.warn("Failed to delete session database files for {}: {}",
                    sessionId, e.getMessage());
            }
        }
    }

    @Override
    public String toString() {
        return String.format("Session[id=%s, created=%d, views=%d, closed=%s]",
            sessionId, createdAt, tempViews.size(), closed);
    }
}
