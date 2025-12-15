package com.thunderduck.connect.server;

import com.thunderduck.connect.service.SparkConnectServiceImpl;
import com.thunderduck.connect.session.SessionManager;
import com.thunderduck.runtime.DuckDBConnectionManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Spark Connect Server bootstrap.
 *
 * Responsibilities:
 * 1. Create singleton DuckDB connection
 * 2. Initialize SessionManager with timeout
 * 3. Create and configure gRPC server
 * 4. Handle graceful shutdown
 *
 * Usage:
 * <pre>
 * SparkConnectServer server = new SparkConnectServer(15002, 300_000);
 * server.start();
 * server.blockUntilShutdown();
 * </pre>
 */
public class SparkConnectServer {
    private static final Logger logger = LoggerFactory.getLogger(SparkConnectServer.class);

    private final int port;
    private final long sessionTimeoutMs;
    private final String duckDbPath;

    private Server grpcServer;
    private DuckDBConnectionManager connectionManager;
    private SessionManager sessionManager;

    /**
     * Create server with default configuration.
     * - Port: 15002 (Spark Connect default)
     * - Session timeout: 300 seconds (5 minutes)
     * - DuckDB: in-memory
     */
    public SparkConnectServer() {
        this(15002, 300_000, null);
    }

    /**
     * Create server with custom configuration.
     *
     * @param port gRPC server port
     * @param sessionTimeoutMs Session timeout in milliseconds
     * @param duckDbPath Path to DuckDB database file (null for in-memory)
     */
    public SparkConnectServer(int port, long sessionTimeoutMs, String duckDbPath) {
        this.port = port;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.duckDbPath = duckDbPath;
    }

    /**
     * Start the server.
     *
     * @throws IOException if server fails to bind
     * @throws SQLException if DuckDB connection fails
     */
    public void start() throws IOException, SQLException {
        logger.info("Starting Spark Connect Server...");
        logger.info("Configuration: port={}, sessionTimeout={}ms, duckDbPath={}",
            port, sessionTimeoutMs, duckDbPath != null ? duckDbPath : "in-memory");

        // 1. Create singleton DuckDB connection
        initializeDuckDB();

        // 2. Create SessionManager
        sessionManager = new SessionManager(sessionTimeoutMs);

        // 3. Create gRPC service implementation
        SparkConnectServiceImpl service = new SparkConnectServiceImpl(
            sessionManager,
            connectionManager
        );

        // 4. Build and start gRPC server
        grpcServer = ServerBuilder.forPort(port)
            .addService(service)
            .build()
            .start();

        logger.info("Spark Connect Server started on port {}", port);
        logger.info("Ready to accept connections (single-session mode)");

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            try {
                SparkConnectServer.this.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
    }

    /**
     * Stop the server gracefully.
     */
    public void stop() {
        logger.info("Stopping Spark Connect Server...");

        // 1. Stop accepting new connections
        if (grpcServer != null) {
            try {
                grpcServer.shutdown();
                if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warn("Server did not terminate gracefully, forcing shutdown");
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted during server shutdown", e);
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 2. Shutdown session manager
        if (sessionManager != null) {
            sessionManager.shutdown();
        }

        // 3. Close DuckDB connection manager
        if (connectionManager != null) {
            try {
                connectionManager.close();
                logger.info("DuckDB connection manager closed");
            } catch (SQLException e) {
                logger.error("Error closing DuckDB connection manager", e);
            }
        }

        logger.info("Spark Connect Server stopped");
    }

    /**
     * Block until server is terminated.
     *
     * @throws InterruptedException if waiting is interrupted
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    /**
     * Initialize singleton DuckDB connection manager.
     *
     * Creates a connection manager with pool size 1 (singleton pattern)
     * for single-session usage.
     *
     * @throws SQLException if connection fails
     */
    private void initializeDuckDB() throws SQLException {
        logger.info("Initializing DuckDB connection manager...");

        // Create configuration with pool size 1 (singleton)
        DuckDBConnectionManager.Configuration config;
        if (duckDbPath != null) {
            config = DuckDBConnectionManager.Configuration.persistent(duckDbPath);
        } else {
            config = DuckDBConnectionManager.Configuration.inMemory();
        }
        // Use default auto-detect pool size (min of CPUs, 8) for concurrent operations
        // Pool size 1 causes connection exhaustion during concurrent schema analysis
        // See: Connection pool exhausted - timeout after 30 seconds

        // Create connection manager
        connectionManager = new DuckDBConnectionManager(config);
        logger.info("DuckDB connection manager created with pool size {}", connectionManager.getPoolSize());

        // Test connection
        try (var pooled = connectionManager.borrowConnection()) {
            var conn = pooled.get();
            var stmt = conn.createStatement();
            var rs = stmt.executeQuery("SELECT 1 as test");
            if (rs.next()) {
                logger.info("DuckDB connection test successful: {}", rs.getInt(1));
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            logger.error("DuckDB connection test failed", e);
            throw e;
        }
    }

    /**
     * Get the server port.
     *
     * @return Port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Main entry point.
     *
     * Usage:
     * <pre>
     * java -jar connect-server.jar [port] [sessionTimeoutMs] [duckDbPath]
     * </pre>
     *
     * Examples:
     * <pre>
     * # Start with defaults (port 15002, 5 min timeout, in-memory)
     * java -jar connect-server.jar
     *
     * # Start on custom port
     * java -jar connect-server.jar 50051
     *
     * # Start with persistent database
     * java -jar connect-server.jar 15002 300000 /data/my.duckdb
     * </pre>
     */
    public static void main(String[] args) {
        try {
            // Parse command-line arguments
            int port = args.length > 0 ? Integer.parseInt(args[0]) : 15002;
            long sessionTimeout = args.length > 1 ? Long.parseLong(args[1]) : 300_000;
            String duckDbPath = args.length > 2 ? args[2] : null;

            // Create and start server
            SparkConnectServer server = new SparkConnectServer(port, sessionTimeout, duckDbPath);
            server.start();

            logger.info("================================================");
            logger.info("Spark Connect Server is running");
            logger.info("Port: {}", port);
            logger.info("Session timeout: {}ms", sessionTimeout);
            logger.info("DuckDB: {}", duckDbPath != null ? duckDbPath : "in-memory");
            logger.info("================================================");
            logger.info("Connect with PySpark:");
            logger.info("  from pyspark.sql import SparkSession");
            logger.info("  spark = SparkSession.builder \\");
            logger.info("      .remote(\"sc://localhost:{}\") \\", port);
            logger.info("      .getOrCreate()");
            logger.info("  spark.sql(\"SELECT 1\").show()");
            logger.info("================================================");

            // Block until shutdown
            server.blockUntilShutdown();

        } catch (Exception e) {
            logger.error("Server failed to start", e);
            System.exit(1);
        }
    }
}
