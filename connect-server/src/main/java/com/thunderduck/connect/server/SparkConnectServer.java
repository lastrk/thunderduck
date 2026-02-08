package com.thunderduck.connect.server;

import com.thunderduck.connect.service.SparkConnectServiceImpl;
import com.thunderduck.connect.session.SessionManager;
import com.thunderduck.runtime.DuckDBRuntime;
import com.thunderduck.runtime.SparkCompatMode;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Spark Connect Server bootstrap.
 *
 * Responsibilities:
 * 1. Initialize SessionManager with timeout
 * 2. Create and configure gRPC server
 * 3. Handle graceful shutdown
 *
 * Each session creates its own DuckDB runtime, providing session isolation
 * and proper resource management.
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

    private Server grpcServer;
    private SessionManager sessionManager;

    /**
     * Create server with default configuration.
     * - Port: 15002 (Spark Connect default)
     * - Session timeout: 300 seconds (5 minutes)
     */
    public SparkConnectServer() {
        this(15002, 300_000);
    }

    /**
     * Create server with custom configuration.
     *
     * @param port gRPC server port
     * @param sessionTimeoutMs Session timeout in milliseconds
     */
    public SparkConnectServer(int port, long sessionTimeoutMs) {
        this.port = port;
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    /**
     * Start the server.
     *
     * @throws IOException if server fails to bind
     */
    public void start() throws IOException {
        logger.info("Starting Spark Connect Server...");
        logger.info("Configuration: port={}, sessionTimeout={}ms", port, sessionTimeoutMs);

        // 1. Create SessionManager (sessions create their own DuckDB runtimes)
        sessionManager = new SessionManager(sessionTimeoutMs);

        // 2. Create gRPC service implementation
        SparkConnectServiceImpl service = new SparkConnectServiceImpl(sessionManager);

        // 3. Build and start gRPC server
        grpcServer = ServerBuilder.forPort(port)
            .addService(service)
            .build()
            .start();

        logger.info("Spark Connect Server started on port {}", port);
        logger.info("Ready to accept connections (session-scoped DuckDB runtimes)");

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

        // 2. Shutdown session manager (closes all sessions and their DuckDB runtimes)
        if (sessionManager != null) {
            sessionManager.shutdown();
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
     * java -jar connect-server.jar [port] [sessionTimeoutMs]
     * </pre>
     *
     * Examples:
     * <pre>
     * # Start with defaults (port 15002, 5 min timeout)
     * java -jar connect-server.jar
     *
     * # Start on custom port
     * java -jar connect-server.jar 50051
     *
     * # Start with custom port and timeout
     * java -jar connect-server.jar 15002 600000
     * </pre>
     */
    public static void main(String[] args) {
        try {
            // Separate named flags (--strict, --relaxed) from positional args
            List<String> positionalArgs = new ArrayList<>();
            SparkCompatMode.Mode compatMode = null;

            for (String arg : args) {
                if ("--strict".equals(arg)) {
                    compatMode = SparkCompatMode.Mode.STRICT;
                } else if ("--relaxed".equals(arg)) {
                    compatMode = SparkCompatMode.Mode.RELAXED;
                } else {
                    positionalArgs.add(arg);
                }
            }

            // Fall back to system property if no CLI flag
            if (compatMode == null) {
                String sysProp = System.getProperty("thunderduck.spark.compat.mode");
                compatMode = SparkCompatMode.parse(sysProp);
            }

            // Configure mode early, before any DuckDBRuntime is created
            SparkCompatMode.configure(compatMode);

            // Parse positional args: [port] [sessionTimeoutMs]
            int port = positionalArgs.size() > 0 ? Integer.parseInt(positionalArgs.get(0)) : 15002;
            long sessionTimeout = positionalArgs.size() > 1 ? Long.parseLong(positionalArgs.get(1)) : 300_000;

            // Startup probe: create a throwaway DuckDBRuntime to trigger extension loading
            // and set SparkCompatMode.extensionLoaded before accepting connections
            logger.info("Probing extension availability (mode={})", compatMode);
            try (DuckDBRuntime probe = DuckDBRuntime.create("jdbc:duckdb::memory:__probe")) {
                // Extension loading happens in constructor; result is now in SparkCompatMode
            }

            // If strict mode and extension didn't load, fail fast
            if (compatMode == SparkCompatMode.Mode.STRICT && !SparkCompatMode.isExtensionLoaded()) {
                logger.error("Strict Spark compatibility mode requested but extension failed to load");
                System.exit(1);
            }

            // Create and start server
            SparkConnectServer server = new SparkConnectServer(port, sessionTimeout);
            server.start();

            // Determine compat label for banner
            String compatLabel = SparkCompatMode.isExtensionLoaded()
                ? "STRICT (extension loaded)"
                : "RELAXED (vanilla DuckDB functions)";

            logger.info("================================================");
            logger.info("Spark Connect Server is running");
            logger.info("Port: {}", port);
            logger.info("Session timeout: {}ms", sessionTimeout);
            logger.info("Spark compatibility: {}", compatLabel);
            logger.info("DuckDB: Session-scoped in-memory databases");
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
