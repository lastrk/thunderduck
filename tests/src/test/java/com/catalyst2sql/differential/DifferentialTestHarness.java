package com.catalyst2sql.differential;

import com.catalyst2sql.differential.model.ComparisonResult;
import com.catalyst2sql.differential.model.Divergence;
import com.catalyst2sql.differential.validation.DataFrameComparator;
import com.catalyst2sql.differential.validation.SchemaValidator;
import com.catalyst2sql.test.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Abstract base class for differential testing between Spark and catalyst2sql.
 *
 * <p>This harness provides:
 * <ul>
 *   <li>Spark 3.5.3 local mode session management</li>
 *   <li>DuckDB connection management for catalyst2sql</li>
 *   <li>Schema validation utilities</li>
 *   <li>Data comparison utilities</li>
 *   <li>Divergence collection and reporting</li>
 * </ul>
 *
 * <p>Tests extending this class can execute the same query on both engines
 * and automatically compare results with detailed divergence reporting.
 */
public abstract class DifferentialTestHarness extends TestBase {

    protected SparkSession spark;
    protected Connection duckdb;
    protected SchemaValidator schemaValidator;
    protected DataFrameComparator dataComparator;
    protected List<ComparisonResult> allResults;
    protected Path tempDir;

    @BeforeEach
    @Override
    public void doSetUp() {
        try {
            // Create temporary directory for test data
            tempDir = Files.createTempDirectory("differential_test_");

            // Initialize Spark 3.5.3 local mode
            spark = SparkSession.builder()
                    .master("local[*]")
                    .appName("DifferentialTest")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.shuffle.partitions", "4")
                    .config("spark.sql.adaptive.enabled", "false")
                    .config("spark.driver.host", "localhost")
                    .getOrCreate();

            // Suppress Spark logging
            spark.sparkContext().setLogLevel("ERROR");

            // Initialize DuckDB connection
            duckdb = DriverManager.getConnection("jdbc:duckdb:");

            // Initialize validators
            schemaValidator = new SchemaValidator();
            dataComparator = new DataFrameComparator();
            allResults = new ArrayList<>();

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize differential test harness", e);
        }
    }

    @AfterEach
    @Override
    public void doTearDown() {
        try {
            // Close DuckDB connection
            if (duckdb != null && !duckdb.isClosed()) {
                duckdb.close();
            }

            // Stop Spark session
            if (spark != null) {
                spark.stop();
            }

            // Clean up temporary directory
            if (tempDir != null && Files.exists(tempDir)) {
                deleteDirectory(tempDir.toFile());
            }

        } catch (Exception e) {
            logger.warning("Error during test cleanup: " + e.getMessage());
        }
    }

    /**
     * Execute a query on both Spark and catalyst2sql, then compare results.
     *
     * @param testName descriptive name for this test
     * @param sparkDF Spark DataFrame result
     * @param duckdbResultSet DuckDB ResultSet result
     * @return ComparisonResult containing any divergences found
     */
    protected ComparisonResult executeAndCompare(
            String testName,
            Dataset<Row> sparkDF,
            ResultSet duckdbResultSet) {

        ComparisonResult result = new ComparisonResult(testName);

        try {
            // Compare results
            List<Divergence> divergences = dataComparator.compare(sparkDF, duckdbResultSet);

            for (Divergence d : divergences) {
                result.addDivergence(d);
            }

        } catch (Exception e) {
            result.addDivergence(new Divergence(
                    Divergence.Type.EXECUTION_ERROR,
                    Divergence.Severity.CRITICAL,
                    "Comparison failed: " + e.getMessage(),
                    null,
                    null
            ));
        }

        allResults.add(result);
        return result;
    }

    /**
     * Execute a query on both Spark and catalyst2sql by writing/reading Parquet.
     *
     * @param testName descriptive name for this test
     * @param sparkDF Spark DataFrame to write as Parquet
     * @param duckdbQuery DuckDB SQL query to execute on the Parquet file
     * @return ComparisonResult containing any divergences found
     */
    protected ComparisonResult executeAndCompareViaParquet(
            String testName,
            Dataset<Row> sparkDF,
            String duckdbQuery) {

        ComparisonResult result = new ComparisonResult(testName);

        try {
            // Write Spark result to Parquet
            String parquetPath = tempDir.resolve(testName + "_spark.parquet").toString();
            sparkDF.write().mode("overwrite").parquet(parquetPath);

            // Read with DuckDB and execute query
            try (Statement stmt = duckdb.createStatement()) {
                String loadQuery = String.format(
                        "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet_scan('%s/**/*.parquet')",
                        testName, parquetPath
                );
                stmt.execute(loadQuery);

                try (ResultSet rs = stmt.executeQuery(duckdbQuery)) {
                    List<Divergence> divergences = dataComparator.compare(sparkDF, rs);
                    for (Divergence d : divergences) {
                        result.addDivergence(d);
                    }
                }
            }

        } catch (Exception e) {
            result.addDivergence(new Divergence(
                    Divergence.Type.EXECUTION_ERROR,
                    Divergence.Severity.CRITICAL,
                    "Execution failed: " + e.getMessage(),
                    null,
                    e.toString()
            ));
        }

        allResults.add(result);
        return result;
    }

    /**
     * Get summary of all test results.
     */
    protected String getSummary() {
        int total = allResults.size();
        int passed = (int) allResults.stream().filter(ComparisonResult::isPassed).count();
        int failed = total - passed;

        int criticalCount = 0;
        int highCount = 0;
        int mediumCount = 0;
        int lowCount = 0;

        for (ComparisonResult r : allResults) {
            for (Divergence d : r.getDivergences()) {
                switch (d.getSeverity()) {
                    case CRITICAL:
                        criticalCount++;
                        break;
                    case HIGH:
                        highCount++;
                        break;
                    case MEDIUM:
                        mediumCount++;
                        break;
                    case LOW:
                        lowCount++;
                        break;
                }
            }
        }

        return String.format(
                "Tests: %d total, %d passed, %d failed\n" +
                        "Divergences: %d CRITICAL, %d HIGH, %d MEDIUM, %d LOW",
                total, passed, failed, criticalCount, highCount, mediumCount, lowCount
        );
    }

    /**
     * Get all comparison results.
     */
    protected List<ComparisonResult> getAllResults() {
        return new ArrayList<>(allResults);
    }

    /**
     * Helper to delete directory recursively.
     */
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
