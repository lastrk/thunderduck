package com.thunderduck.runtime;

import com.thunderduck.exception.QueryExecutionException;
import com.thunderduck.logging.QueryLogger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.Objects;
import java.util.UUID;

/**
 * Executes SQL queries against DuckDB and returns results.
 *
 * <p>This class provides a high-level API for executing queries and updates
 * against a DuckDB database, with automatic connection management and
 * Arrow data conversion.
 *
 * <p>Features:
 * <ul>
 *   <li>Query execution with Arrow result conversion</li>
 *   <li>Update/DDL statement execution</li>
 *   <li>Automatic connection management</li>
 *   <li>Error handling and cleanup</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   DuckDBConnectionManager manager = new DuckDBConnectionManager();
 *   QueryExecutor executor = new QueryExecutor(manager);
 *
 *   // Execute query
 *   VectorSchemaRoot result = executor.executeQuery(
 *       "SELECT * FROM read_parquet('data.parquet') WHERE age > 25");
 *
 *   // Execute update
 *   int rowsAffected = executor.executeUpdate(
 *       "CREATE TABLE users (id INTEGER, name VARCHAR)");
 * </pre>
 *
 * @see DuckDBConnectionManager
 * @see ArrowInterchange
 */
public class QueryExecutor {

    private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private final DuckDBConnectionManager connectionManager;

    /**
     * Creates a query executor with the specified connection manager.
     *
     * @param connectionManager the connection manager
     */
    public QueryExecutor(DuckDBConnectionManager connectionManager) {
        this.connectionManager = Objects.requireNonNull(
            connectionManager, "connectionManager must not be null");
    }

    /**
     * Executes a query and returns results as Arrow VectorSchemaRoot.
     *
     * <p>This method executes the SQL query, converts the JDBC ResultSet
     * to Apache Arrow format, and returns the result. The connection is
     * automatically acquired from the pool and released after execution.
     *
     * <p>Supports SQL introspection via EXPLAIN statements:
     * <ul>
     *   <li>EXPLAIN &lt;query&gt; - Returns logical query plan</li>
     *   <li>EXPLAIN ANALYZE &lt;query&gt; - Executes query and returns runtime statistics</li>
     *   <li>EXPLAIN (FORMAT JSON) &lt;query&gt; - Returns plan in JSON format</li>
     * </ul>
     *
     * <p>The returned VectorSchemaRoot must be closed by the caller to
     * free memory.
     *
     * @param sql the SQL query to execute
     * @return the query results as Arrow VectorSchemaRoot
     * @throws QueryExecutionException if query execution fails
     * @throws NullPointerException if sql is null
     */
    public VectorSchemaRoot executeQuery(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        // Generate unique query ID for logging correlation
        String queryId = "q_" + UUID.randomUUID().toString().substring(0, 8);
        QueryLogger.startQuery(queryId);

        long queryStartTime = System.nanoTime();

        try {
            // Detect EXPLAIN statements for SQL introspection
            boolean isExplain = isExplainStatement(sql);

            // Use try-with-resources for automatic connection cleanup
            try (PooledConnection pooled = connectionManager.borrowConnection()) {
                DuckDBConnection conn = pooled.get();
                Statement stmt = null;
                ResultSet rs = null;

                try {
                    // Execute query
                    long execStartTime = System.nanoTime();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(sql);
                    long execTimeMs = (System.nanoTime() - execStartTime) / 1_000_000;

                    // Convert to Arrow
                    long arrowStartTime = System.nanoTime();
                    VectorSchemaRoot result = ArrowInterchange.fromResultSet(rs);
                    long arrowTimeMs = (System.nanoTime() - arrowStartTime) / 1_000_000;

                    // Get row count from result
                    long rowCount = result.getRowCount();

                    // Log execution metrics
                    QueryLogger.logExecution(execTimeMs + arrowTimeMs, rowCount);

                    // Log SQL for EXPLAIN statements (they're typically short)
                    if (isExplain) {
                        QueryLogger.logSQLGeneration(sql, 0); // No generation for direct SQL
                    }

                    // Complete query logging
                    long totalTimeMs = (System.nanoTime() - queryStartTime) / 1_000_000;
                    QueryLogger.completeQuery(totalTimeMs);

                    return result;

                } catch (SQLException e) {
                    // Log error before throwing
                    QueryLogger.logError(e);

                    // Wrap in QueryExecutionException with context
                    throw new QueryExecutionException(
                        "Failed to execute query: " + e.getMessage(), e, sql);

                } finally {
                    // Clean up JDBC resources
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            // Log but don't throw
                            logger.warn("Error closing ResultSet: " + e.getMessage());
                        }
                    }
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            // Log but don't throw
                            logger.warn("Error closing Statement: " + e.getMessage());
                        }
                    }
                }
            } catch (SQLException e) {
                // Log error
                QueryLogger.logError(e);

                // Wrap connection acquisition errors
                throw new QueryExecutionException(
                    "Failed to acquire database connection: " + e.getMessage(), e, sql);
            }
        } finally {
            // Always clear logging context to prevent memory leaks
            QueryLogger.clearContext();
        }
    }

    /**
     * Checks if a SQL statement is an EXPLAIN statement for query introspection.
     *
     * <p>Supports the following EXPLAIN variants:
     * <ul>
     *   <li>EXPLAIN &lt;query&gt;</li>
     *   <li>EXPLAIN ANALYZE &lt;query&gt;</li>
     *   <li>EXPLAIN (FORMAT JSON) &lt;query&gt;</li>
     * </ul>
     *
     * @param sql the SQL statement to check
     * @return true if the statement starts with EXPLAIN (case-insensitive)
     */
    private boolean isExplainStatement(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }

        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("EXPLAIN");
    }

    /**
     * Executes an update/DDL statement.
     *
     * <p>This method is used for INSERT, UPDATE, DELETE, CREATE, DROP,
     * and other statements that don't return result sets.
     *
     * @param sql the SQL statement to execute
     * @return the number of rows affected (for DML), or 0 (for DDL)
     * @throws QueryExecutionException if statement execution fails
     * @throws NullPointerException if sql is null
     */
    public int executeUpdate(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        // Use try-with-resources for automatic connection cleanup
        try (PooledConnection pooled = connectionManager.borrowConnection()) {
            DuckDBConnection conn = pooled.get();
            Statement stmt = null;

            try {
                // Execute update
                stmt = conn.createStatement();
                return stmt.executeUpdate(sql);

            } catch (SQLException e) {
                // Wrap in QueryExecutionException with context
                throw new QueryExecutionException(
                    "Failed to execute update: " + e.getMessage(), e, sql);

            } finally {
                // Clean up JDBC resources
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        // Log but don't throw
                        logger.warn("Error closing Statement: " + e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            // Wrap connection acquisition errors
            throw new QueryExecutionException(
                "Failed to acquire database connection: " + e.getMessage(), e, sql);
        } // Connection automatically released here
    }

    /**
     * Executes a statement (query or update).
     *
     * <p>This method can execute any SQL statement. For queries, it returns
     * true and the results can be retrieved with getResultSet(). For updates,
     * it returns false and the update count can be retrieved with getUpdateCount().
     *
     * @param sql the SQL statement to execute
     * @return true if the result is a ResultSet, false if it's an update count
     * @throws QueryExecutionException if statement execution fails
     * @throws NullPointerException if sql is null
     */
    public boolean execute(String sql) throws QueryExecutionException {
        Objects.requireNonNull(sql, "sql must not be null");

        // Use try-with-resources for automatic connection cleanup
        try (PooledConnection pooled = connectionManager.borrowConnection()) {
            DuckDBConnection conn = pooled.get();
            Statement stmt = null;

            try {
                // Execute statement
                stmt = conn.createStatement();
                return stmt.execute(sql);

            } catch (SQLException e) {
                // Wrap in QueryExecutionException with context
                throw new QueryExecutionException(
                    "Failed to execute statement: " + e.getMessage(), e, sql);

            } finally {
                // Clean up JDBC resources
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        // Log but don't throw
                        logger.warn("Error closing Statement: " + e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            // Wrap connection acquisition errors
            throw new QueryExecutionException(
                "Failed to acquire database connection: " + e.getMessage(), e, sql);
        } // Connection automatically released here
    }

    /**
     * Returns the connection manager used by this executor.
     *
     * @return the connection manager
     */
    public DuckDBConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
