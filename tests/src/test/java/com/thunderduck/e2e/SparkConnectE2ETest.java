package com.thunderduck.e2e;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.*;
import java.net.Socket;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.List;
import java.util.ArrayList;

/**
 * End-to-End test runner that executes Python-based PySpark tests
 * against the thunderduck Spark Connect server.
 *
 * This test class:
 * 1. Starts the thunderduck server
 * 2. Runs Python test suites via PySpark
 * 3. Validates the complete Spark Connect → DuckDB pipeline
 *
 * Run with: mvn verify -DskipUnitTests=true
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkConnectE2ETest {

    private Process thunderduckServer;
    private static final int SERVER_PORT = 15002; // Default port for thunderduck Spark Connect server
    private static final String PYTHON_TEST_DIR = "src/test/python";
    private static final int SERVER_STARTUP_TIMEOUT = 30; // seconds

    @BeforeAll
    void setUp() throws Exception {
        System.out.println("Starting thunderduck Spark Connect server for E2E tests...");

        // Only start server if not already running (e.g., from external start)
        if (!isPortOpen("localhost", SERVER_PORT)) {
            startThunderduckServer();
        } else {
            System.out.println("thunderduck server already running on port " + SERVER_PORT);
        }

        // Install Python dependencies
        installPythonDependencies();
    }

    @AfterAll
    void tearDown() {
        System.out.println("Cleaning up E2E test environment...");

        if (thunderduckServer != null && thunderduckServer.isAlive()) {
            System.out.println("Stopping thunderduck server...");
            thunderduckServer.destroyForcibly();
            try {
                thunderduckServer.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("DataFrame Operations Test Suite")
    void testDataFrameOperations() {
        runPythonTest("test_dataframes.py", "DataFrame operations");
    }

    @Test
    @Disabled
    @Order(2)
    @DisplayName("SQL Functionality Test Suite")
    void testSQLFunctionality() {
        runPythonTest("test_sql.py", "SQL functionality");
    }

    @Test
    @Order(3)
    @DisplayName("TPC-H Benchmark Test Suite")
    @EnabledIfSystemProperty(named = "tpch.enabled", matches = "true")
    void testTPCH() {
        runPythonTest("test_tpch.py", "TPC-H benchmarks");
    }

    @Test
    @Order(4)
    @DisplayName("TPC-DS Benchmark Test Suite")
    @EnabledIfSystemProperty(named = "tpcds.enabled", matches = "true")
    void testTPCDS() {
        runPythonTest("test_tpcds.py", "TPC-DS benchmarks");
    }

    @Test
    @Disabled
    @Order(5)
    @DisplayName("Edge Cases and Error Handling")
    void testEdgeCases() {
        runPythonTest("test_edge_cases.py", "Edge cases");
    }

    private void startThunderduckServer() throws Exception {
        // Build path to JAR file
        Path projectRoot = Paths.get(System.getProperty("user.dir"));
        // If running from tests directory, go up one level
        if (projectRoot.endsWith("tests")) {
            projectRoot = projectRoot.getParent();
        }
        Path jarPath = projectRoot.resolve("connect-server/target/thunderduck-connect-server-0.1.0-SNAPSHOT.jar");

        if (!Files.exists(jarPath)) {
            throw new FileNotFoundException(
                "thunderduck server JAR not found at: " + jarPath +
                ". Run 'mvn package' first."
            );
        }

        // Start the server
        ProcessBuilder pb = new ProcessBuilder(
            "java",
                "-Xmx2g",
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-jar",
                jarPath.toString()
        );

        pb.redirectErrorStream(true);
        thunderduckServer = pb.start();

        // Capture server output in background
        captureProcessOutput(thunderduckServer);

        // Wait for server to be ready
        waitForServer(SERVER_PORT, SERVER_STARTUP_TIMEOUT);
        System.out.println("thunderduck server started successfully on port " + SERVER_PORT);
    }

    private void installPythonDependencies() throws Exception {
        System.out.println("Installing Python dependencies...");

        Path requirementsFile = Paths.get(PYTHON_TEST_DIR, "requirements.txt");
        if (!Files.exists(requirementsFile)) {
            System.out.println("No requirements.txt found, skipping dependency installation");
            return;
        }

        ProcessBuilder pb = new ProcessBuilder(
            "pip3", "install", "-q", "-r", requirementsFile.toString()
        );

        Process process = pb.start();
        boolean finished = process.waitFor(60, TimeUnit.SECONDS);

        if (!finished) {
            process.destroyForcibly();
            throw new TimeoutException("Python dependency installation timed out");
        }

        if (process.exitValue() != 0) {
            throw new RuntimeException("Failed to install Python dependencies");
        }

        System.out.println("Python dependencies installed successfully");
    }

    private void runPythonTest(String testFile, String testName) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("Running " + testName + " tests...");
        System.out.println("=".repeat(70));

        try {
            Path testPath = Paths.get(PYTHON_TEST_DIR, "thunderduck_e2e", testFile);

            if (!Files.exists(testPath)) {
                // Test file doesn't exist yet, skip gracefully
                System.out.println("Test file not implemented yet: " + testPath);
                Assumptions.assumeTrue(false, "Test not yet implemented: " + testFile);
                return;
            }

            List<String> command = new ArrayList<>();
            command.add("python3");
            command.add("-m");
            command.add("pytest");
            command.add(testPath.toString());
            command.add("-v");
            command.add("--tb=short");
            command.add("--color=yes");

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);

            Process process = pb.start();

            // Capture and display output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    System.out.println(line);
                }
            }

            boolean finished = process.waitFor(5, TimeUnit.MINUTES);

            if (!finished) {
                process.destroyForcibly();
                throw new TimeoutException("Test execution timed out: " + testFile);
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                Assertions.fail(String.format(
                    "%s tests failed with exit code %d\nOutput:\n%s",
                    testName, exitCode, output
                ));
            }

            System.out.println("\n✓ " + testName + " tests passed");

        } catch (Exception e) {
            Assertions.fail("Failed to run " + testName + " tests: " + e.getMessage(), e);
        }
    }

    private void captureProcessOutput(Process process) {
        Thread outputThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[SERVER] " + line);
                }
            } catch (IOException e) {
                // Process terminated, expected
            }
        });
        outputThread.setDaemon(true);
        outputThread.start();
    }

    private void waitForServer(int port, int timeoutSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (System.currentTimeMillis() < deadline) {
            if (isPortOpen("localhost", port)) {
                Thread.sleep(2000); // Extra wait for full initialization
                return;
            }
            Thread.sleep(1000);
        }

        throw new TimeoutException(
            "thunderduck server did not start on port " + port +
            " within " + timeoutSeconds + " seconds"
        );
    }

    private boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
