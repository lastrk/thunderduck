package com.catalyst2sql.io;

import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.logical.TableScan;
import com.catalyst2sql.types.StructType;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Reads Parquet files and creates logical plans.
 *
 * <p>This class provides factory methods for creating TableScan nodes
 * that read from Parquet files. DuckDB's native Parquet reader is used
 * for high-performance file I/O with automatic schema detection.
 *
 * <p>Supported reading modes:
 * <ul>
 *   <li>Single file: read_parquet('file.parquet')</li>
 *   <li>Glob pattern: read_parquet('*.parquet')</li>
 *   <li>Directory: read_parquet('data/&#42;&#42;/*.parquet')</li>
 *   <li>Partitioned dataset: Hive-style partitioning</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   // Read single file
 *   LogicalPlan plan = ParquetReader.readFile("data/users.parquet");
 *
 *   // Read multiple files with glob
 *   LogicalPlan plan = ParquetReader.readGlob("data/users_*.parquet");
 *
 *   // Read partitioned dataset
 *   LogicalPlan plan = ParquetReader.readPartitioned(
 *       "data/users",
 *       List.of("year", "month")
 *   );
 * </pre>
 *
 * @see ParquetWriter
 * @see TableScan
 */
public class ParquetReader {

    /**
     * Reads a single Parquet file.
     *
     * <p>Creates a TableScan node that uses DuckDB's read_parquet function
     * to read the specified file. Schema is inferred automatically by DuckDB.
     *
     * @param path the file path (absolute or relative)
     * @return a TableScan logical plan node
     * @throws NullPointerException if path is null
     */
    public static LogicalPlan readFile(String path) {
        Objects.requireNonNull(path, "path must not be null");

        // DuckDB will infer schema when the query is executed
        // For now, we use null schema - it will be resolved at execution time
        return new TableScan(path, TableScan.TableFormat.PARQUET, null);
    }

    /**
     * Reads a single Parquet file with explicit schema.
     *
     * @param path the file path
     * @param schema the expected schema
     * @return a TableScan logical plan node
     * @throws NullPointerException if path or schema is null
     */
    public static LogicalPlan readFile(String path, StructType schema) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        return new TableScan(path, TableScan.TableFormat.PARQUET, schema);
    }

    /**
     * Reads multiple Parquet files matching a glob pattern.
     *
     * <p>DuckDB supports glob patterns natively, including:
     * <ul>
     *   <li>{@code *} - matches any string</li>
     *   <li>{@code ?} - matches single character</li>
     *   <li>{@code **} - matches any number of directories</li>
     *   <li>{@code [abc]} - matches a, b, or c</li>
     * </ul>
     *
     * <p>Examples:
     * <pre>
     *   {@code "data/*.parquet"}           - all .parquet files in data/
     *   {@code "data/year=2024/*.parquet"} - files in specific partition
     *   {@code "data/ ** /*.parquet"}      - all .parquet files recursively (remove spaces)
     * </pre>
     *
     * @param globPattern the glob pattern
     * @return a TableScan logical plan node
     * @throws NullPointerException if globPattern is null
     */
    public static LogicalPlan readGlob(String globPattern) {
        Objects.requireNonNull(globPattern, "globPattern must not be null");

        // DuckDB handles glob patterns natively in read_parquet
        return new TableScan(globPattern, TableScan.TableFormat.PARQUET, null);
    }

    /**
     * Reads a partitioned Parquet dataset.
     *
     * <p>Reads a Hive-style partitioned dataset where partition columns
     * are encoded in the directory structure. For example:
     * <pre>{@code
     *   data/year=2024/month=01/data.parquet
     *   data/year=2024/month=02/data.parquet
     * }</pre>
     *
     * <p>DuckDB automatically detects and extracts partition column values
     * from the directory structure.
     *
     * @param basePath the base directory path
     * @param partitionColumns the partition column names (for documentation)
     * @return a TableScan logical plan node
     * @throws NullPointerException if basePath or partitionColumns is null
     */
    public static LogicalPlan readPartitioned(String basePath,
                                              List<String> partitionColumns) {
        Objects.requireNonNull(basePath, "basePath must not be null");
        Objects.requireNonNull(partitionColumns, "partitionColumns must not be null");

        // DuckDB automatically detects Hive-style partitioning
        // Use glob pattern to read all parquet files recursively
        String pattern = basePath.endsWith("/") ? basePath : basePath + "/";
        pattern += "**/*.parquet";

        return new TableScan(pattern, TableScan.TableFormat.PARQUET, null);
    }

    /**
     * Reads Parquet files from a directory.
     *
     * <p>Reads all .parquet files in the specified directory (non-recursive).
     *
     * @param dirPath the directory path
     * @return a TableScan logical plan node
     * @throws NullPointerException if dirPath is null
     */
    public static LogicalPlan readDirectory(String dirPath) {
        Objects.requireNonNull(dirPath, "dirPath must not be null");

        // Add glob pattern for all parquet files in directory
        String pattern = dirPath.endsWith("/") ? dirPath : dirPath + "/";
        pattern += "*.parquet";

        return new TableScan(pattern, TableScan.TableFormat.PARQUET, null);
    }

    /**
     * Reads Parquet files from a directory recursively.
     *
     * <p>Reads all .parquet files in the specified directory and all subdirectories.
     *
     * @param dirPath the directory path
     * @return a TableScan logical plan node
     * @throws NullPointerException if dirPath is null
     */
    public static LogicalPlan readDirectoryRecursive(String dirPath) {
        Objects.requireNonNull(dirPath, "dirPath must not be null");

        // Add glob pattern for all parquet files recursively
        String pattern = dirPath.endsWith("/") ? dirPath : dirPath + "/";
        pattern += "**/*.parquet";

        return new TableScan(pattern, TableScan.TableFormat.PARQUET, null);
    }

    /**
     * Reads Parquet with a filter predicate pushed down.
     *
     * <p>Note: Filter pushdown is handled by the Filter logical plan node.
     * This method is provided for convenience but creates the same result
     * as readFile().
     *
     * @param path the file path
     * @param filterSQL the filter SQL (will be applied by Filter node)
     * @return a TableScan logical plan node
     * @throws NullPointerException if path is null
     * @deprecated Use readFile() and add a Filter node instead
     */
    @Deprecated
    public static LogicalPlan readWithFilter(String path, String filterSQL) {
        Objects.requireNonNull(path, "path must not be null");
        // Filter pushdown is handled by logical plan optimization
        return readFile(path);
    }

    /**
     * Creates a TableScan for Delta Lake format.
     *
     * <p>Reads a Delta Lake table using DuckDB's delta extension.
     *
     * @param tablePath the Delta Lake table path
     * @return a TableScan logical plan node
     * @throws NullPointerException if tablePath is null
     */
    public static LogicalPlan readDelta(String tablePath) {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        return new TableScan(tablePath, TableScan.TableFormat.DELTA, null);
    }

    /**
     * Creates a TableScan for Iceberg format.
     *
     * <p>Reads an Iceberg table using DuckDB's iceberg extension.
     *
     * @param tablePath the Iceberg table path
     * @return a TableScan logical plan node
     * @throws NullPointerException if tablePath is null
     */
    public static LogicalPlan readIceberg(String tablePath) {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        return new TableScan(tablePath, TableScan.TableFormat.ICEBERG, null);
    }
}
