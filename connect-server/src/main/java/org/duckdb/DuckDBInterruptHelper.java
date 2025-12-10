package org.duckdb;

/**
 * Helper class to expose DuckDB's package-private interrupt() method.
 *
 * The DuckDBConnection.interrupt() method calls the native duckdb_jdbc_interrupt()
 * function which cancels running queries. However, it is package-private,
 * so this helper class (in the org.duckdb package) provides public access.
 *
 * @see <a href="https://github.com/duckdb/duckdb-java">DuckDB Java JDBC</a>
 */
public final class DuckDBInterruptHelper {

    private DuckDBInterruptHelper() {
        // Utility class - no instantiation
    }

    /**
     * Interrupt a running query on the given DuckDB connection.
     *
     * This method is safe to call from a different thread than the one
     * executing the query. The query will be cancelled asynchronously.
     *
     * @param connection The DuckDB connection with a running query
     * @throws IllegalArgumentException if connection is null
     * @throws IllegalStateException if connection is closed
     */
    public static void interrupt(DuckDBConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("Connection cannot be null");
        }
        try {
            connection.interrupt();
        } catch (Exception e) {
            // Log but don't propagate - interrupt is best-effort
            // The connection may already be closed or the query finished
        }
    }

    /**
     * Safely interrupt a connection, handling the case where it may be a
     * wrapped connection or null.
     *
     * @param connection A JDBC Connection that may be a DuckDBConnection
     * @return true if interrupt was called, false if connection was not a DuckDBConnection
     */
    public static boolean interruptIfDuckDB(java.sql.Connection connection) {
        if (connection instanceof DuckDBConnection) {
            interrupt((DuckDBConnection) connection);
            return true;
        }
        return false;
    }
}
