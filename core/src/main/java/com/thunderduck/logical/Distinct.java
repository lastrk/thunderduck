package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a distinct operation (removes duplicate rows).
 *
 * <p>This node removes duplicate rows from its input based on:
 * <ul>
 *   <li>All columns if no specific columns are specified</li>
 *   <li>Specified columns if a column list is provided</li>
 * </ul>
 *
 * <p>Example SQL generation:
 * <pre>
 *   Distinct(input, null) → SELECT DISTINCT * FROM (input)
 *   Distinct(input, ["col1", "col2"]) → SELECT DISTINCT col1, col2 FROM (input)
 * </pre>
 */
public class Distinct extends LogicalPlan {

    private final List<String> columns;

    /**
     * Creates a distinct node.
     *
     * @param input the input logical plan
     * @param columns the columns to use for deduplication (null for all columns)
     */
    public Distinct(LogicalPlan input, List<String> columns) {
        super(input);
        this.columns = columns == null ? null : Collections.unmodifiableList(columns);
    }

    /**
     * Creates a distinct node for all columns.
     *
     * @param input the input logical plan
     */
    public Distinct(LogicalPlan input) {
        this(input, null);
    }

    /**
     * Returns the columns to use for deduplication.
     *
     * @return the columns list, or null for all columns
     */
    public List<String> columns() {
        return columns;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        Objects.requireNonNull(generator, "generator must not be null");

        String inputSQL = generator.generate(children().get(0));

        // If the input is a simple table scan or already has SELECT, we can optimize
        // For now, we'll use a subquery approach for safety
        if (columns == null || columns.isEmpty()) {
            // DISTINCT on all columns
            return String.format("SELECT DISTINCT * FROM (%s) AS t", inputSQL);
        } else {
            // DISTINCT on specific columns
            String columnList = String.join(", ", columns);
            return String.format("SELECT DISTINCT %s FROM (%s) AS t", columnList, inputSQL);
        }
    }

    @Override
    public StructType inferSchema() {
        // Distinct doesn't change the schema, just removes duplicates
        return children().get(0).inferSchema();
    }

    @Override
    public String toString() {
        if (columns == null || columns.isEmpty()) {
            return "Distinct(all columns)";
        } else {
            return String.format("Distinct(columns=%s)", columns);
        }
    }
}