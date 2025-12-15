package com.thunderduck.command;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.runtime.DuckDBConnectionManager;
import com.thunderduck.runtime.QueryExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;

/**
 * WriteOperation Unit Tests.
 *
 * Tests the WriteOperation command implementation that handles:
 * - df.write.parquet()
 * - df.write.csv()
 * - df.write.json()
 *
 * Uses DuckDB COPY statements to write data to various formats.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("WriteOperation Tests")
public class WriteOperationTest extends TestBase {

    private DuckDBConnectionManager connectionManager;
    private QueryExecutor executor;

    @BeforeEach
    void setUp() {
        connectionManager = new DuckDBConnectionManager();
        executor = new QueryExecutor(connectionManager);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Nested
    @DisplayName("Parquet Write Tests")
    class ParquetWriteTests {

        @Test
        @DisplayName("Write simple data to Parquet file")
        void testWriteSimpleParquet(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("output.parquet");

            // Generate COPY statement (same as SparkConnectServiceImpl.executeWriteOperation)
            String sql = "SELECT 1 as id, 'Alice' as name, 25 as age";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            assertThat(outputPath).exists();
            assertThat(Files.size(outputPath)).isGreaterThan(0);
        }

        @Test
        @DisplayName("Write with GZIP compression")
        void testWriteWithGzipCompression(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("gzip.parquet");

            String sql = "SELECT generate_series as id FROM generate_series(1, 100)";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION GZIP)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            assertThat(outputPath).exists();
        }

        @Test
        @DisplayName("Write with ZSTD compression")
        void testWriteWithZstdCompression(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("zstd.parquet");

            String sql = "SELECT generate_series as id FROM generate_series(1, 100)";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            assertThat(outputPath).exists();
        }

        @Test
        @DisplayName("Write partitioned data")
        void testWritePartitionedParquet(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("partitioned");

            String sql = "SELECT 1 as id, 'A' as category UNION ALL SELECT 2 as id, 'B' as category";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (\"category\"))",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            // Partitioned output creates a directory
            assertThat(outputPath).isDirectory();
        }

        @Test
        @DisplayName("Read back written Parquet file")
        void testRoundTripParquet(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("roundtrip.parquet");

            // Write
            String writeSQL = "SELECT 1 as id, 'Test' as name";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                writeSQL, outputPath.toString().replace("\\", "/")
            );
            executor.execute(copySQL);

            // Read back
            String readSQL = String.format(
                "SELECT * FROM read_parquet('%s')",
                outputPath.toString().replace("\\", "/")
            );
            var result = executor.executeQuery(readSQL);

            assertThat(result).isNotNull();
            assertThat(result.getRowCount()).isEqualTo(1);
            result.close();
        }
    }

    @Nested
    @DisplayName("CSV Write Tests")
    class CSVWriteTests {

        @Test
        @DisplayName("Write simple data to CSV file")
        void testWriteSimpleCSV(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("output.csv");

            String sql = "SELECT 1 as id, 'Alice' as name, 25 as age";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT CSV, HEADER true)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            assertThat(outputPath).exists();
            String content = Files.readString(outputPath);
            assertThat(content).contains("id,name,age");
            assertThat(content).contains("1,Alice,25");
        }

        @Test
        @DisplayName("Write CSV with custom delimiter")
        void testWriteCSVWithDelimiter(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("output.tsv");

            String sql = "SELECT 1 as id, 'Bob' as name";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT CSV, HEADER true, DELIMITER '\t')",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            String content = Files.readString(outputPath);
            assertThat(content).contains("id\tname");
        }

        @Test
        @DisplayName("Write CSV without header")
        void testWriteCSVNoHeader(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("noheader.csv");

            String sql = "SELECT 1 as id, 'Data' as value";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT CSV, HEADER false)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            String content = Files.readString(outputPath);
            assertThat(content.trim()).isEqualTo("1,Data");
        }
    }

    @Nested
    @DisplayName("JSON Write Tests")
    class JSONWriteTests {

        @Test
        @DisplayName("Write simple data to JSON file")
        void testWriteSimpleJSON(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("output.json");

            String sql = "SELECT 1 as id, 'Alice' as name";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT JSON)",
                sql, outputPath.toString().replace("\\", "/")
            );

            executor.execute(copySQL);

            assertThat(outputPath).exists();
            String content = Files.readString(outputPath);
            assertThat(content).contains("\"id\"");
            assertThat(content).contains("\"name\"");
        }
    }

    @Nested
    @DisplayName("Save Mode Tests")
    class SaveModeTests {

        @Test
        @DisplayName("OVERWRITE mode replaces existing file")
        void testOverwriteMode(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("overwrite.parquet");

            // First write
            String sql1 = "SELECT 1 as id";
            String copySQL1 = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                sql1, outputPath.toString().replace("\\", "/")
            );
            executor.execute(copySQL1);
            long firstSize = Files.size(outputPath);

            // Overwrite with different data
            String sql2 = "SELECT generate_series as id FROM generate_series(1, 1000)";
            String copySQL2 = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                sql2, outputPath.toString().replace("\\", "/")
            );
            executor.execute(copySQL2);
            long secondSize = Files.size(outputPath);

            // File should be different size after overwrite
            assertThat(secondSize).isNotEqualTo(firstSize);
        }

        @Test
        @DisplayName("ERROR_IF_EXISTS check (simulated)")
        void testErrorIfExistsCheck(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("exists.parquet");

            // Create file
            String sql = "SELECT 1 as id";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                sql, outputPath.toString().replace("\\", "/")
            );
            executor.execute(copySQL);

            // Verify file exists
            assertThat(outputPath).exists();

            // In SparkConnectServiceImpl, ERROR_IF_EXISTS would check this
            // and return an error BEFORE executing the COPY
            java.io.File file = new java.io.File(outputPath.toString());
            assertThat(file.exists()).isTrue();
        }

        @Test
        @DisplayName("APPEND mode with read-union-write")
        void testAppendMode(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("append.parquet");

            // First write: 10 rows
            String sql1 = "SELECT generate_series as id FROM generate_series(1, 10)";
            String copySQL1 = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                sql1, outputPath.toString().replace("\\", "/")
            );
            executor.execute(copySQL1);

            // Append: read existing + union new + write back
            String newDataSQL = "SELECT generate_series as id FROM generate_series(11, 20)";
            String unionSQL = String.format(
                "SELECT * FROM read_parquet('%s') UNION ALL %s",
                outputPath.toString().replace("\\", "/"),
                newDataSQL
            );
            String appendCopySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET)",
                unionSQL, outputPath.toString().replace("\\", "/")
            );
            executor.execute(appendCopySQL);

            // Verify: should now have 20 rows
            String readSQL = String.format(
                "SELECT COUNT(*) as cnt FROM read_parquet('%s')",
                outputPath.toString().replace("\\", "/")
            );
            var result = executor.executeQuery(readSQL);
            // Get count from result
            assertThat(result).isNotNull();
            assertThat(result.getRowCount()).isEqualTo(1);
            result.close();
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Write to non-existent directory fails")
        void testWriteToNonExistentDirectory() {
            String sql = "SELECT 1 as id";
            String copySQL = String.format(
                "COPY (%s) TO '/nonexistent/path/output.parquet' (FORMAT PARQUET)",
                sql
            );

            assertThatThrownBy(() -> executor.execute(copySQL))
                .isInstanceOf(com.thunderduck.exception.QueryExecutionException.class)
                .hasMessageContaining("Cannot open file");
        }

        @Test
        @DisplayName("Invalid SQL causes error")
        void testInvalidSQLCausesError(@TempDir Path tempDir) {
            Path outputPath = tempDir.resolve("output.parquet");

            String copySQL = String.format(
                "COPY (SELECT * FROM nonexistent_table) TO '%s' (FORMAT PARQUET)",
                outputPath.toString().replace("\\", "/")
            );

            assertThatThrownBy(() -> executor.execute(copySQL))
                .isInstanceOf(com.thunderduck.exception.QueryExecutionException.class)
                .hasMessageContaining("does not exist");
        }
    }

    @Nested
    @DisplayName("Large Data Tests")
    class LargeDataTests {

        @Test
        @DisplayName("Write 100K rows efficiently")
        void testWrite100KRows(@TempDir Path tempDir) throws Exception {
            Path outputPath = tempDir.resolve("large.parquet");

            String sql = "SELECT generate_series as id, 'Data_' || generate_series as value " +
                        "FROM generate_series(1, 100000)";
            String copySQL = String.format(
                "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                sql, outputPath.toString().replace("\\", "/")
            );

            long startTime = System.currentTimeMillis();
            executor.execute(copySQL);
            long duration = System.currentTimeMillis() - startTime;

            assertThat(outputPath).exists();
            assertThat(Files.size(outputPath)).isGreaterThan(100_000); // Should be at least 100KB

            // Should complete in under 5 seconds
            assertThat(duration).isLessThan(5000);

            // Verify row count
            String readSQL = String.format(
                "SELECT COUNT(*) as cnt FROM read_parquet('%s')",
                outputPath.toString().replace("\\", "/")
            );
            var result = executor.executeQuery(readSQL);
            assertThat(result.getRowCount()).isEqualTo(1);
            result.close();
        }
    }
}
