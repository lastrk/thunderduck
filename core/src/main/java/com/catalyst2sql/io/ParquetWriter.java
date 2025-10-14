package com.catalyst2sql.io;

import com.catalyst2sql.generator.SQLQuoting;
import com.catalyst2sql.runtime.QueryExecutor;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * Writes query results to Parquet files.
 *
 * <p>This class provides methods for writing SQL query results to Parquet
 * files using DuckDB's native COPY statement. Features include:
 * <ul>
 *   <li>Multiple compression formats (SNAPPY, GZIP, ZSTD, UNCOMPRESSED)</li>
 *   <li>Partitioned writes (Hive-style partitioning)</li>
 *   <li>Configurable row group size</li>
 *   <li>High-performance parallel writing</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   QueryExecutor executor = new QueryExecutor(connectionManager);
 *   ParquetWriter writer = new ParquetWriter(executor);
 *
 *   // Write query results to Parquet
 *   writer.write(
 *       "SELECT * FROM users WHERE age > 25",
 *       "output/users.parquet",
 *       Compression.SNAPPY
 *   );
 *
 *   // Write partitioned output
 *   writer.writePartitioned(
 *       "SELECT * FROM sales",
 *       "output/sales",
 *       List.of("year", "month"),
 *       Compression.ZSTD
 *   );
 * </pre>
 *
 * @see ParquetReader
 * @see QueryExecutor
 */
public class ParquetWriter {

    /**
     * Compression formats supported by Parquet.
     */
    public enum Compression {
        /** Snappy compression (fast, moderate compression ratio) */
        SNAPPY,

        /** Gzip compression (slower, better compression ratio) */
        GZIP,

        /** Zstandard compression (fast, excellent compression ratio) */
        ZSTD,

        /** No compression */
        UNCOMPRESSED
    }

    private final QueryExecutor executor;

    /**
     * Creates a Parquet writer with the specified query executor.
     *
     * @param executor the query executor
     * @throws NullPointerException if executor is null
     */
    public ParquetWriter(QueryExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    /**
     * Writes query results to a Parquet file with specified compression.
     *
     * <p>This method uses DuckDB's COPY statement to write query results
     * directly to a Parquet file. The operation is performed in a single
     * pass for optimal performance.
     *
     * @param sql the SQL query
     * @param outputPath the output file path
     * @param compression the compression format
     * @throws SQLException if write operation fails
     * @throws NullPointerException if any parameter is null
     */
    public void write(String sql, String outputPath, Compression compression)
            throws SQLException {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(outputPath, "outputPath must not be null");
        Objects.requireNonNull(compression, "compression must not be null");

        String copySQL = String.format(
            "COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION %s)",
            sql,
            SQLQuoting.quoteFilePath(outputPath),
            compression.name()
        );

        executor.executeUpdate(copySQL);
    }

    /**
     * Writes query results to a Parquet file with default SNAPPY compression.
     *
     * <p>SNAPPY provides a good balance between compression speed and
     * compression ratio, making it the default choice for most use cases.
     *
     * @param sql the SQL query
     * @param outputPath the output file path
     * @throws SQLException if write operation fails
     * @throws NullPointerException if any parameter is null
     */
    public void write(String sql, String outputPath) throws SQLException {
        write(sql, outputPath, Compression.SNAPPY);
    }

    /**
     * Writes query results to a partitioned Parquet dataset.
     *
     * <p>Creates a Hive-style partitioned dataset where partition columns
     * are encoded in the directory structure. For example, partitioning by
     * [year, month] creates:
     * <pre>
     *   output/year=2024/month=01/data.parquet
     *   output/year=2024/month=02/data.parquet
     * </pre>
     *
     * @param sql the SQL query
     * @param outputPath the base output directory
     * @param partitionColumns the partition column names
     * @param compression the compression format
     * @throws SQLException if write operation fails
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if partitionColumns is empty
     */
    public void writePartitioned(String sql, String outputPath,
                                List<String> partitionColumns,
                                Compression compression) throws SQLException {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(outputPath, "outputPath must not be null");
        Objects.requireNonNull(partitionColumns, "partitionColumns must not be null");
        Objects.requireNonNull(compression, "compression must not be null");

        if (partitionColumns.isEmpty()) {
            throw new IllegalArgumentException("partitionColumns must not be empty");
        }

        // Quote partition column names to prevent SQL injection
        StringBuilder partitionSQL = new StringBuilder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (i > 0) {
                partitionSQL.append(", ");
            }
            partitionSQL.append(SQLQuoting.quoteIdentifier(partitionColumns.get(i)));
        }

        String copySQL = String.format(
            "COPY (%s) TO %s (FORMAT PARQUET, PARTITION_BY (%s), COMPRESSION %s)",
            sql,
            SQLQuoting.quoteFilePath(outputPath),
            partitionSQL.toString(),
            compression.name()
        );

        executor.executeUpdate(copySQL);
    }

    /**
     * Writes query results to a partitioned Parquet dataset with default compression.
     *
     * @param sql the SQL query
     * @param outputPath the base output directory
     * @param partitionColumns the partition column names
     * @throws SQLException if write operation fails
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if partitionColumns is empty
     */
    public void writePartitioned(String sql, String outputPath,
                                List<String> partitionColumns) throws SQLException {
        writePartitioned(sql, outputPath, partitionColumns, Compression.SNAPPY);
    }

    /**
     * Writes query results with custom Parquet options.
     *
     * <p>Allows fine-grained control over Parquet writing options such as
     * row group size, encoding, and statistics.
     *
     * @param sql the SQL query
     * @param outputPath the output file path
     * @param options the Parquet write options
     * @throws SQLException if write operation fails
     * @throws NullPointerException if any parameter is null
     */
    public void writeWithOptions(String sql, String outputPath, WriteOptions options)
            throws SQLException {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(outputPath, "outputPath must not be null");
        Objects.requireNonNull(options, "options must not be null");

        StringBuilder copySQL = new StringBuilder();
        copySQL.append("COPY (").append(sql).append(") TO ");
        copySQL.append(SQLQuoting.quoteFilePath(outputPath));
        copySQL.append(" (FORMAT PARQUET");

        // Add compression
        copySQL.append(", COMPRESSION ").append(options.compression.name());

        // Add row group size if specified
        if (options.rowGroupSize > 0) {
            copySQL.append(", ROW_GROUP_SIZE ").append(options.rowGroupSize);
        }

        // Add partitioning if specified
        if (options.partitionColumns != null && !options.partitionColumns.isEmpty()) {
            copySQL.append(", PARTITION_BY (");
            for (int i = 0; i < options.partitionColumns.size(); i++) {
                if (i > 0) {
                    copySQL.append(", ");
                }
                copySQL.append(SQLQuoting.quoteIdentifier(options.partitionColumns.get(i)));
            }
            copySQL.append(")");
        }

        copySQL.append(")");

        executor.executeUpdate(copySQL.toString());
    }

    /**
     * Appends query results to an existing Parquet file.
     *
     * <p>Note: DuckDB's COPY command overwrites by default. To append,
     * you need to read the existing data, union it with new data, and
     * write back.
     *
     * @param sql the SQL query
     * @param outputPath the output file path
     * @throws SQLException if operation fails
     * @throws NullPointerException if any parameter is null
     */
    public void append(String sql, String outputPath) throws SQLException {
        Objects.requireNonNull(sql, "sql must not be null");
        Objects.requireNonNull(outputPath, "outputPath must not be null");

        // Read existing data, union with new data, and write back
        String unionSQL = String.format(
            "SELECT * FROM read_parquet(%s) UNION ALL %s",
            SQLQuoting.quoteFilePath(outputPath),
            sql
        );

        write(unionSQL, outputPath, Compression.SNAPPY);
    }

    /**
     * Returns the query executor used by this writer.
     *
     * @return the query executor
     */
    public QueryExecutor getExecutor() {
        return executor;
    }

    /**
     * Options for Parquet writing.
     */
    public static class WriteOptions {
        private Compression compression = Compression.SNAPPY;
        private int rowGroupSize = -1; // -1 means use default
        private List<String> partitionColumns = null;

        /**
         * Sets the compression format.
         *
         * @param compression the compression
         * @return this options object
         */
        public WriteOptions withCompression(Compression compression) {
            this.compression = Objects.requireNonNull(compression);
            return this;
        }

        /**
         * Sets the row group size.
         *
         * <p>Larger row groups improve compression but use more memory.
         * Typical values are 64MB to 256MB.
         *
         * @param size the row group size in rows
         * @return this options object
         */
        public WriteOptions withRowGroupSize(int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("rowGroupSize must be positive");
            }
            this.rowGroupSize = size;
            return this;
        }

        /**
         * Sets the partition columns.
         *
         * @param columns the partition column names
         * @return this options object
         */
        public WriteOptions withPartitions(List<String> columns) {
            this.partitionColumns = columns;
            return this;
        }

        /**
         * Creates default write options.
         *
         * @return the default options
         */
        public static WriteOptions defaults() {
            return new WriteOptions();
        }
    }
}
