package com.thunderduck.runtime;

import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.concurrent.*;
import java.util.Objects;

/**
 * Connection manager for DuckDB with connection pooling and
 * hardware-aware optimization.
 *
 * <p>This class manages a pool of DuckDB connections configured for
 * optimal performance based on hardware capabilities. The pool is
 * thread-safe and handles connection lifecycle automatically.
 *
 * <p>Features:
 * <ul>
 *   <li>Connection pooling with configurable size</li>
 *   <li>Hardware-aware configuration (memory limit, thread count)</li>
 *   <li>Support for in-memory and persistent databases</li>
 *   <li>Automatic connection configuration</li>
 *   <li>Thread-safe connection acquisition and release</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   // In-memory database
 *   DuckDBConnectionManager manager = new DuckDBConnectionManager();
 *
 *   // Persistent database
 *   Configuration config = Configuration.persistent("/path/to/db.duckdb");
 *   DuckDBConnectionManager manager = new DuckDBConnectionManager(config);
 *
 *   // Use connection
 *   DuckDBConnection conn = manager.getConnection();
 *   try {
 *       // Execute queries...
 *   } finally {
 *       manager.releaseConnection(conn);
 *   }
 *
 *   // Cleanup
 *   manager.close();
 * </pre>
 *
 * @see HardwareProfile
 * @see QueryExecutor
 */
public class DuckDBConnectionManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DuckDBConnectionManager.class);

    private final String jdbcUrl;
    private final HardwareProfile hardware;
    private final BlockingQueue<DuckDBConnection> connectionPool;
    private final int poolSize;
    private volatile boolean closed = false;

    /**
     * Creates a connection manager with default in-memory configuration.
     */
    public DuckDBConnectionManager() {
        this(Configuration.inMemory());
    }

    /**
     * Creates a connection manager with the specified configuration.
     *
     * @param config the configuration
     * @throws SQLException if connection pool initialization fails
     */
    public DuckDBConnectionManager(Configuration config) {
        Objects.requireNonNull(config, "config must not be null");

        this.jdbcUrl = buildJdbcUrl(config);
        this.hardware = HardwareProfile.detect();
        this.poolSize = config.poolSize > 0 ? config.poolSize :
                        Math.min(Runtime.getRuntime().availableProcessors(), 8);
        this.connectionPool = new ArrayBlockingQueue<>(poolSize);

        // Initialize pool
        try {
            for (int i = 0; i < poolSize; i++) {
                connectionPool.offer(createConnection());
            }
        } catch (SQLException e) {
            // Clean up any connections that were created
            try {
                close();
            } catch (SQLException closeEx) {
                // Ignore close exception
            }
            throw new RuntimeException("Failed to initialize connection pool", e);
        }
    }

    /**
     * Acquires a connection from the pool.
     *
     * <p>This method blocks until a connection is available or the timeout
     * (30 seconds) is reached.
     *
     * @return a connection from the pool
     * @throws SQLException if the pool is exhausted or closed
     */
    public DuckDBConnection getConnection() throws SQLException {
        if (closed) {
            throw new SQLException("Connection manager is closed");
        }

        try {
            DuckDBConnection conn = connectionPool.poll(30, TimeUnit.SECONDS);
            if (conn == null) {
                throw new SQLException("Connection pool exhausted - timeout after 30 seconds");
            }

            // Verify connection is still valid
            if (!conn.isClosed()) {
                return conn;
            }

            // Connection is closed, create a new one
            return createConnection();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection", e);
        }
    }

    /**
     * Borrows a connection from the pool with automatic cleanup.
     *
     * <p>This method returns a PooledConnection that automatically releases
     * the underlying connection when closed. Use with try-with-resources:
     * <pre>
     *   try (PooledConnection pooled = manager.borrowConnection()) {
     *       DuckDBConnection conn = pooled.get();
     *       // Use connection...
     *   } // Automatically released
     * </pre>
     *
     * @return pooled connection that auto-releases on close
     * @throws SQLException if no connection available or pool is closed
     */
    public PooledConnection borrowConnection() throws SQLException {
        DuckDBConnection conn = getConnection();
        return new PooledConnection(conn, this);
    }

    /**
     * Returns a connection to the pool.
     *
     * <p>Connections should always be released back to the pool when done.
     * This method validates connection health before returning to the pool.
     * Invalid connections are discarded and replaced.
     *
     * <p>Note: Prefer using borrowConnection() with try-with-resources
     * instead of manual connection management with getConnection() and
     * releaseConnection().
     *
     * @param conn the connection to release (may be null)
     */
    public void releaseConnection(DuckDBConnection conn) {
        if (conn == null || closed) {
            return;
        }

        try {
            // Validate connection health before returning to pool
            if (!isConnectionValid(conn)) {
                logger.warn("Invalid connection detected, not returning to pool");
                try {
                    conn.close();
                } catch (SQLException ignored) {
                    // Ignore close errors for invalid connections
                }

                // Create replacement connection
                try {
                    DuckDBConnection replacement = createConnection();
                    if (!connectionPool.offer(replacement)) {
                        logger.warn("Connection pool full, closing replacement");
                        replacement.close();
                    }
                } catch (SQLException e) {
                    logger.warn("Failed to create replacement connection: " + e.getMessage());
                }
                return;
            }

            // Return valid connection to pool
            if (!connectionPool.offer(conn)) {
                logger.warn("Connection pool full, closing extra connection");
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("Failed to close connection: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.warn("Error releasing connection: " + e.getMessage());
        }
    }

    /**
     * Validates that a connection is still usable.
     *
     * <p>Checks if the connection is:
     * <ul>
     *   <li>Not null</li>
     *   <li>Not closed</li>
     *   <li>Still responds to basic queries (isValid check)</li>
     * </ul>
     *
     * @param conn the connection to validate
     * @return true if connection is valid and usable
     */
    private boolean isConnectionValid(DuckDBConnection conn) {
        try {
            return conn != null && !conn.isClosed() && conn.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Creates a new configured DuckDB connection.
     *
     * @return a new connection
     * @throws SQLException if connection creation fails
     */
    private DuckDBConnection createConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);

        // Configure connection for optimal performance
        try (Statement stmt = duckConn.createStatement()) {
            // Set memory limit based on hardware
            stmt.execute(String.format("SET memory_limit='%s'",
                                      hardware.recommendedMemoryLimit()));

            // Set thread count
            stmt.execute(String.format("SET threads=%d",
                                      hardware.recommendedThreadCount()));

            // Enable progress bar for long queries
            stmt.execute("SET enable_progress_bar=false"); // Disabled for non-interactive use

            // Enable parallel CSV/Parquet reading
            stmt.execute("SET preserve_insertion_order=false");

            // Set NULL ordering to match Spark SQL (NULLs FIRST by default)
            // Critical for TPC-DS queries using ROLLUP/CUBE which produce NULL subtotals
            stmt.execute("SET default_null_order='NULLS FIRST'");
        }

        return duckConn;
    }

    /**
     * Builds the JDBC URL from configuration.
     *
     * @param config the configuration
     * @return the JDBC URL
     */
    private String buildJdbcUrl(Configuration config) {
        if (config.inMemory) {
            return "jdbc:duckdb:";
        } else {
            return "jdbc:duckdb:" + config.databasePath;
        }
    }

    /**
     * Closes all connections in the pool and shuts down the manager.
     *
     * <p>After calling this method, the manager cannot be used anymore.
     *
     * @throws SQLException if any connection fails to close
     */
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        closed = true;

        SQLException firstException = null;

        // Close all connections in the pool
        while (!connectionPool.isEmpty()) {
            DuckDBConnection conn = connectionPool.poll();
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    if (firstException == null) {
                        firstException = e;
                    }
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Returns the hardware profile used by this manager.
     *
     * @return the hardware profile
     */
    public HardwareProfile getHardwareProfile() {
        return hardware;
    }

    /**
     * Returns the connection pool size.
     *
     * @return the pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Returns whether this manager is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Configuration for the connection manager.
     */
    public static class Configuration {
        /** Whether to use in-memory database */
        public boolean inMemory = true;

        /** Database file path (for persistent databases) */
        public String databasePath = null;

        /** Connection pool size (0 = auto-detect) */
        public int poolSize = 0;

        /**
         * Creates an in-memory database configuration.
         *
         * @return the configuration
         */
        public static Configuration inMemory() {
            Configuration config = new Configuration();
            config.inMemory = true;
            return config;
        }

        /**
         * Creates a persistent database configuration.
         *
         * @param path the database file path
         * @return the configuration
         */
        public static Configuration persistent(String path) {
            Configuration config = new Configuration();
            config.inMemory = false;
            config.databasePath = Objects.requireNonNull(path, "path must not be null");
            return config;
        }

        /**
         * Sets the connection pool size.
         *
         * @param size the pool size (0 for auto-detect)
         * @return this configuration
         */
        public Configuration withPoolSize(int size) {
            if (size < 0) {
                throw new IllegalArgumentException("poolSize must be non-negative");
            }
            this.poolSize = size;
            return this;
        }
    }
}
