package com.thunderduck.runtime;

import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * DuckDB runtime - owns a single DuckDB connection.
 *
 * <p>Each DuckDBRuntime instance manages one DuckDB connection. The runtime
 * is responsible for creating, configuring, and closing the connection.
 *
 * <p>Typical usage in session-scoped context:
 * <pre>{@code
 * // Create runtime for a session
 * DuckDBRuntime runtime = DuckDBRuntime.create("jdbc:duckdb::memory:session_123");
 *
 * // Use connection for queries
 * Connection conn = runtime.getConnection();
 * // ... execute queries ...
 *
 * // Close when session ends
 * runtime.close();
 * }</pre>
 *
 * <p>Test usage:
 * <pre>{@code
 * @BeforeEach
 * void setup() {
 *     runtime = DuckDBRuntime.create("jdbc:duckdb::memory:test_" + System.nanoTime());
 * }
 *
 * @AfterEach
 * void teardown() {
 *     runtime.close();
 * }
 * }</pre>
 */
public class DuckDBRuntime implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DuckDBRuntime.class);

    /** Default JDBC URL for named in-memory database */
    public static final String DEFAULT_JDBC_URL = "jdbc:duckdb::memory:thunderduck";

    private final String jdbcUrl;
    private final DuckDBConnection connection;
    private final HardwareProfile hardware;
    private volatile boolean closed = false;

    /**
     * Private constructor - use create() factory method.
     *
     * @param jdbcUrl JDBC URL for DuckDB connection
     * @throws SQLException if connection fails
     */
    private DuckDBRuntime(String jdbcUrl) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        this.hardware = HardwareProfile.detect();

        logger.info("Creating DuckDB runtime with URL: {}", jdbcUrl);

        // Configure connection properties for streaming results and unsigned extensions
        // This enables true streaming where results are not fully materialized
        // before iteration begins - critical for memory-efficient large result handling
        Properties props = new Properties();
        props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, "true");
        props.setProperty("allow_unsigned_extensions", "true");

        // Create and configure connection with streaming enabled
        Connection rawConn = DriverManager.getConnection(jdbcUrl, props);
        this.connection = rawConn.unwrap(DuckDBConnection.class);
        configureConnection();

        // Load extensions based on configured compatibility mode
        SparkCompatMode.Mode mode = SparkCompatMode.getConfiguredMode();
        switch (mode) {
            case RELAXED -> logger.info("Relaxed Spark compatibility mode - extension loading skipped");
            case STRICT -> {
                boolean strictLoaded = loadBundledExtensions();
                SparkCompatMode.setExtensionLoaded(strictLoaded);
                if (!strictLoaded) {
                    throw new SQLException(
                        "Strict Spark compatibility mode requires the thdck_spark_funcs extension, " +
                        "but it could not be loaded. Build with: mvn clean package -DskipTests -Pbuild-extension");
                }
                logger.info("Strict Spark compatibility mode - extension loaded");
            }
            case AUTO -> {
                boolean autoLoaded = loadBundledExtensions();
                SparkCompatMode.setExtensionLoaded(autoLoaded);
                if (autoLoaded) {
                    logger.info("Strict Spark compatibility mode - extension loaded");
                } else {
                    logger.info("Relaxed Spark compatibility mode - extension not available");
                }
            }
        }

        logger.info("DuckDB runtime initialized with streaming results enabled");
    }

    /**
     * Configure connection for optimal performance.
     *
     * <p>Configuration includes:
     * <ul>
     *   <li>Memory limit based on hardware profile</li>
     *   <li>Thread count based on available cores</li>
     *   <li>jemalloc background threads for 8+ core machines (Linux only)</li>
     *   <li>Spark-compatible NULL ordering</li>
     * </ul>
     *
     * @see <a href="https://duckdb.org/docs/stable/core_extensions/jemalloc">DuckDB jemalloc docs</a>
     */
    private void configureConnection() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Set memory limit based on hardware
            stmt.execute(String.format("SET memory_limit='%s'",
                hardware.recommendedMemoryLimit()));

            // Set thread count
            stmt.execute(String.format("SET threads=%d",
                hardware.recommendedThreadCount()));

            // Enable jemalloc background threads on 8+ core Linux machines
            // This improves allocation performance by allowing background threads
            // to handle memory purging without blocking foreground operations.
            // jemalloc is enabled by default on Linux; this setting enables its
            // background thread feature for asynchronous memory management.
            // See: https://duckdb.org/docs/stable/core_extensions/jemalloc#configuration
            if (hardware.cpuCores() >= 8 && isLinux()) {
                stmt.execute("SET allocator_background_threads=true");
                logger.debug("Enabled jemalloc background threads (8+ cores on Linux)");
            }

            // Enable progress bar for long queries
            stmt.execute("SET enable_progress_bar=false");

            // Ensure insertion order is preserved
            stmt.execute("SET preserve_insertion_order=true");

            // Set NULL ordering to match Spark SQL
            stmt.execute("SET default_null_order='NULLS FIRST'");

            logger.debug("DuckDB configured: memory={}, threads={}, cores={}",
                hardware.recommendedMemoryLimit(), hardware.recommendedThreadCount(),
                hardware.cpuCores());
        }
    }

    /**
     * Check if running on Linux (where jemalloc is available by default).
     *
     * @return true if the OS is Linux
     */
    private boolean isLinux() {
        String os = System.getProperty("os.name", "").toLowerCase();
        return os.contains("linux");
    }

    /**
     * Create a new DuckDBRuntime with the default JDBC URL.
     *
     * @return new DuckDBRuntime instance
     * @throws RuntimeException if connection fails
     */
    public static DuckDBRuntime create() {
        return create(DEFAULT_JDBC_URL);
    }

    /**
     * Create a new DuckDBRuntime with custom JDBC URL.
     *
     * @param jdbcUrl JDBC URL (e.g., "jdbc:duckdb::memory:session123")
     * @return new DuckDBRuntime instance
     * @throws RuntimeException if connection fails
     */
    public static DuckDBRuntime create(String jdbcUrl) {
        try {
            return new DuckDBRuntime(jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create DuckDB runtime: " + jdbcUrl, e);
        }
    }

    /**
     * Create a new DuckDBRuntime with a persistent on-disk database.
     *
     * <p>The database file will be created if it doesn't exist.
     * Data persists across runtime restarts.
     *
     * @param dbPath path to the DuckDB database file (e.g., "/path/to/database.duckdb")
     * @return new DuckDBRuntime instance
     * @throws RuntimeException if connection fails
     */
    public static DuckDBRuntime createPersistent(String dbPath) {
        String jdbcUrl = "jdbc:duckdb:" + dbPath;
        logger.info("Creating persistent DuckDB runtime at: {}", dbPath);
        return create(jdbcUrl);
    }

    /**
     * Get the underlying DuckDB connection.
     *
     * <p>The connection is managed by the runtime - callers should NOT close it.
     *
     * @return the DuckDB connection
     * @throws IllegalStateException if runtime is closed
     */
    public DuckDBConnection getConnection() {
        if (closed) {
            throw new IllegalStateException("DuckDB runtime is closed");
        }
        return connection;
    }

    /**
     * Get the JDBC URL used by this runtime.
     *
     * @return the JDBC URL
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    /**
     * Get the hardware profile used for configuration.
     *
     * @return the hardware profile
     */
    public HardwareProfile getHardwareProfile() {
        return hardware;
    }

    /**
     * Check if this runtime is closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Load bundled extensions from classpath resources.
     *
     * <p>This method queries the DuckDB platform (e.g., "linux_amd64") and attempts
     * to load the thdck_spark_funcs extension if bundled for this platform.
     *
     * @return true if extension was loaded successfully, false otherwise
     */
    private boolean loadBundledExtensions() {
        try {
            // Query PRAGMA platform to get OS/arch string
            String platform;
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("PRAGMA platform")) {
                if (!rs.next()) {
                    logger.warn("Could not determine platform via PRAGMA platform");
                    return false;
                }
                platform = rs.getString(1);
            }

            logger.debug("Detected platform: {}", platform);

            // Try to load thdck_spark_funcs extension
            return loadExtensionResource(platform, "thdck_spark_funcs");
        } catch (SQLException e) {
            logger.warn("Failed to load bundled extensions: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Load a specific extension from classpath resources.
     *
     * @param platform the platform identifier (e.g., "linux_amd64")
     * @param extName the extension name (without .duckdb_extension suffix)
     * @return true if extension was loaded successfully, false otherwise
     */
    private boolean loadExtensionResource(String platform, String extName) {
        String resourcePath = "/extensions/" + platform + "/" + extName + ".duckdb_extension";
        try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                logger.info("Extension '{}' not bundled for platform: {}", extName, platform);
                return false;
            }

            // Extract to temp file
            Path tempDir = Files.createTempDirectory("thunderduck-ext-");
            Path tempFile = tempDir.resolve(extName + ".duckdb_extension");
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            tempFile.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();

            // Load extension
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("LOAD '" + tempFile.toAbsolutePath() + "'");
            }

            logger.info("Extension '{}' loaded successfully", extName);
            return true;
        } catch (IOException | SQLException e) {
            logger.warn("Failed to load extension '{}': {}", extName, e.getMessage());
            return false;
        }
    }

    /**
     * Close the runtime and release resources.
     *
     * <p>After closing, the runtime cannot be used.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        logger.info("Closing DuckDB runtime: {}", jdbcUrl);
        try {
            connection.close();
            logger.info("DuckDB connection closed");
        } catch (SQLException e) {
            logger.error("Error closing DuckDB connection", e);
        }
    }
}
