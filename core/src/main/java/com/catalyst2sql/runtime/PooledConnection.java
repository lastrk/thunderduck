package com.catalyst2sql.runtime;

import org.duckdb.DuckDBConnection;
import java.util.Objects;

/**
 * Auto-closeable wrapper for DuckDB connections that ensures proper cleanup.
 *
 * <p>This class implements the loan pattern for connection management,
 * ensuring that connections are always returned to the pool even in the
 * presence of exceptions.
 *
 * <p>Usage:
 * <pre>
 *   try (PooledConnection conn = manager.borrowConnection()) {
 *       // Use conn.get() to access the underlying connection
 *       Statement stmt = conn.get().createStatement();
 *       ResultSet rs = stmt.executeQuery("SELECT * FROM table");
 *   } // Automatically released back to pool
 * </pre>
 *
 * <p>This pattern prevents common resource leak scenarios:
 * <ul>
 *   <li>Forgetting to call releaseConnection() in finally blocks</li>
 *   <li>Early returns that skip cleanup code</li>
 *   <li>Exception handling that bypasses connection release</li>
 * </ul>
 *
 * @see DuckDBConnectionManager
 */
public class PooledConnection implements AutoCloseable {

    private final DuckDBConnection connection;
    private final DuckDBConnectionManager manager;
    private boolean released = false;

    /**
     * Creates a pooled connection wrapper.
     *
     * <p>This constructor is package-private and should only be called
     * by DuckDBConnectionManager.
     *
     * @param connection the underlying DuckDB connection
     * @param manager the connection manager that owns the connection
     * @throws NullPointerException if connection or manager is null
     */
    PooledConnection(DuckDBConnection connection, DuckDBConnectionManager manager) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.manager = Objects.requireNonNull(manager, "manager must not be null");
    }

    /**
     * Returns the underlying DuckDB connection.
     *
     * <p>The connection can be used until this PooledConnection is closed.
     * After closing, any attempt to use the connection will result in
     * undefined behavior.
     *
     * @return the underlying connection
     * @throws IllegalStateException if connection already released
     */
    public DuckDBConnection get() {
        if (released) {
            throw new IllegalStateException("Connection already released to pool");
        }
        return connection;
    }

    /**
     * Closes this pooled connection and returns it to the pool.
     *
     * <p>This method is idempotent - calling it multiple times is safe
     * and will only release the connection once.
     *
     * <p>After calling this method, the connection is no longer valid
     * for use and get() will throw IllegalStateException.
     */
    @Override
    public void close() {
        if (!released) {
            released = true;
            manager.releaseConnection(connection);
        }
    }

    /**
     * Returns whether this pooled connection has been released.
     *
     * @return true if the connection has been released back to the pool
     */
    public boolean isReleased() {
        return released;
    }
}
