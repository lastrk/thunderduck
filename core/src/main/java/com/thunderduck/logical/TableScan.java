package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Logical plan node representing a table scan (reading from a data source).
 *
 * <p>This node reads data from:
 * <ul>
 *   <li>Parquet files (native DuckDB support)</li>
 *   <li>Delta Lake tables (via DuckDB delta extension)</li>
 *   <li>Iceberg tables (via DuckDB iceberg extension)</li>
 * </ul>
 *
 * <p>Example SQL generation:
 * <pre>
 *   TableScan("/data/table.parquet", PARQUET) → read_parquet('/data/table.parquet')
 *   TableScan("/data/delta-table", DELTA) → delta_scan('/data/delta-table')
 *   TableScan("/data/iceberg-table", ICEBERG) → iceberg_scan('/data/iceberg-table')
 * </pre>
 */
public final class TableScan extends LogicalPlan {

    /**
     * Supported table formats.
     */
    public enum TableFormat {
        TABLE,      // Regular DuckDB table
        PARQUET,
        DELTA,
        ICEBERG
    }

    private final String source;
    private final TableFormat format;
    private final Map<String, String> options;

    /**
     * Creates a table scan node.
     *
     * @param source the file path or table path
     * @param format the table format
     * @param options additional options (e.g., version for Delta, snapshot for Iceberg)
     * @param schema the table schema
     */
    public TableScan(String source, TableFormat format, Map<String, String> options, StructType schema) {
        super(); // No children
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.format = Objects.requireNonNull(format, "format must not be null");
        this.options = new HashMap<>(options);
        this.schema = schema;
    }

    /**
     * Creates a table scan node without options.
     *
     * @param source the file path or table path
     * @param format the table format
     * @param schema the table schema
     */
    public TableScan(String source, TableFormat format, StructType schema) {
        this(source, format, new HashMap<>(), schema);
    }

    /**
     * Returns the source path.
     *
     * @return the source path
     */
    public String source() {
        return source;
    }

    /**
     * Returns the table format.
     *
     * @return the format
     */
    public TableFormat format() {
        return format;
    }

    /**
     * Returns the options map.
     *
     * @return an unmodifiable options map
     */
    public Map<String, String> options() {
        return Map.copyOf(options);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        Objects.requireNonNull(generator, "generator must not be null");

        return switch (format) {
            case TABLE -> "SELECT * FROM %s".formatted(
                com.thunderduck.generator.SQLQuoting.quoteIdentifier(source));
            case PARQUET -> "SELECT * FROM read_parquet(%s)".formatted(
                com.thunderduck.generator.SQLQuoting.quoteFilePath(source));
            case DELTA -> "SELECT * FROM delta_scan(%s)".formatted(
                com.thunderduck.generator.SQLQuoting.quoteFilePath(source));
            case ICEBERG -> "SELECT * FROM iceberg_scan(%s)".formatted(
                com.thunderduck.generator.SQLQuoting.quoteFilePath(source));
        };
    }

    @Override
    public StructType inferSchema() {
        // Schema is provided at construction time
        return schema;
    }

    @Override
    public String toString() {
        return String.format("TableScan(%s, %s)", source, format);
    }
}
