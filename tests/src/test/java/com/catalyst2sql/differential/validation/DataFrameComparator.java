package com.catalyst2sql.differential.validation;

import com.catalyst2sql.differential.model.Divergence;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

/**
 * Compares Spark DataFrame with JDBC ResultSet row-by-row.
 *
 * <p>Orchestrates comprehensive data validation including:
 * <ul>
 *   <li>Schema validation</li>
 *   <li>Row count comparison</li>
 *   <li>Row-by-row data comparison</li>
 *   <li>Null value handling</li>
 *   <li>Numerical epsilon comparison</li>
 * </ul>
 */
public class DataFrameComparator {

    private final SchemaValidator schemaValidator;
    private final RowComparator rowComparator;

    public DataFrameComparator() {
        this.schemaValidator = new SchemaValidator();
        this.rowComparator = new RowComparator();
    }

    /**
     * Compare Spark DataFrame with JDBC ResultSet.
     *
     * @param sparkDF Spark DataFrame
     * @param resultSet JDBC ResultSet
     * @return List of divergences found
     */
    public List<Divergence> compare(Dataset<Row> sparkDF, ResultSet resultSet) {
        List<Divergence> divergences = new ArrayList<>();

        try {
            // Step 1: Validate schemas
            ResultSetMetaData metadata = resultSet.getMetaData();
            List<Divergence> schemaDivergences = schemaValidator.compare(sparkDF.schema(), metadata);
            divergences.addAll(schemaDivergences);

            // If critical schema issues, stop comparison
            boolean hasCriticalSchemaIssue = schemaDivergences.stream()
                    .anyMatch(d -> d.getSeverity() == Divergence.Severity.CRITICAL);
            if (hasCriticalSchemaIssue) {
                return divergences;
            }

            // Step 2: Collect Spark rows
            List<Row> sparkRows = sparkDF.collectAsList();

            // Step 3: Collect JDBC rows
            List<Object[]> jdbcRows = new ArrayList<>();
            int columnCount = metadata.getColumnCount();
            while (resultSet.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = resultSet.getObject(i + 1);
                }
                jdbcRows.add(row);
            }

            // Step 4: Compare row counts
            if (sparkRows.size() != jdbcRows.size()) {
                divergences.add(new Divergence(
                        Divergence.Type.ROW_COUNT_MISMATCH,
                        Divergence.Severity.CRITICAL,
                        "Row count mismatch",
                        sparkRows.size(),
                        jdbcRows.size()
                ));
                // Continue with comparison using min count
            }

            // Step 5: Compare rows
            int minRowCount = Math.min(sparkRows.size(), jdbcRows.size());
            for (int i = 0; i < minRowCount; i++) {
                Row sparkRow = sparkRows.get(i);
                Object[] jdbcRow = jdbcRows.get(i);

                List<Divergence> rowDivergences = rowComparator.compare(
                        sparkRow, jdbcRow, sparkDF.schema(), i
                );
                divergences.addAll(rowDivergences);
            }

        } catch (Exception e) {
            divergences.add(new Divergence(
                    Divergence.Type.EXECUTION_ERROR,
                    Divergence.Severity.CRITICAL,
                    "Data comparison failed: " + e.getMessage(),
                    null,
                    e.toString()
            ));
        }

        return divergences;
    }
}
