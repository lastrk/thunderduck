package com.catalyst2sql.differential.validation;

import com.catalyst2sql.differential.model.Divergence;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Compares individual rows field-by-field with proper null and type handling.
 */
public class RowComparator {

    private final NumericalValidator numericalValidator;

    public RowComparator() {
        this.numericalValidator = new NumericalValidator();
    }

    /**
     * Compare a Spark Row with a JDBC row.
     *
     * @param sparkRow Spark Row
     * @param jdbcRow JDBC row as Object array
     * @param schema Schema for field types
     * @param rowIndex Row index for error reporting
     * @return List of divergences found in this row
     */
    public List<Divergence> compare(Row sparkRow, Object[] jdbcRow, StructType schema, int rowIndex) {
        List<Divergence> divergences = new ArrayList<>();

        int fieldCount = Math.min(sparkRow.length(), jdbcRow.length);

        for (int i = 0; i < fieldCount; i++) {
            Object sparkValue = sparkRow.isNullAt(i) ? null : sparkRow.get(i);
            Object jdbcValue = jdbcRow[i];

            String fieldName = i < schema.fields().length ? schema.fields()[i].name() : "field_" + i;

            // Handle nulls
            if (sparkValue == null && jdbcValue == null) {
                continue; // Both null, equal
            }

            if (sparkValue == null || jdbcValue == null) {
                divergences.add(new Divergence(
                        Divergence.Type.NULL_HANDLING,
                        Divergence.Severity.HIGH,
                        String.format("Row %d, column '%s': NULL mismatch", rowIndex, fieldName),
                        sparkValue,
                        jdbcValue
                ));
                continue;
            }

            // Compare values based on type
            if (!areValuesEqual(sparkValue, jdbcValue, schema.fields()[i].dataType())) {
                divergences.add(new Divergence(
                        Divergence.Type.DATA_MISMATCH,
                        Divergence.Severity.HIGH,
                        String.format("Row %d, column '%s': Value mismatch", rowIndex, fieldName),
                        sparkValue,
                        jdbcValue
                ));
            }
        }

        return divergences;
    }

    /**
     * Check if two values are equal considering their types.
     */
    private boolean areValuesEqual(Object sparkValue, Object jdbcValue, org.apache.spark.sql.types.DataType dataType) {
        // For numerical types, use epsilon comparison
        if (numericalValidator.isNumericalType(dataType)) {
            return numericalValidator.areNumericallyEqual(sparkValue, jdbcValue, dataType);
        }

        // For string types, compare as strings
        if (dataType instanceof org.apache.spark.sql.types.StringType) {
            return Objects.equals(String.valueOf(sparkValue), String.valueOf(jdbcValue));
        }

        // For boolean types
        if (dataType instanceof org.apache.spark.sql.types.BooleanType) {
            return Objects.equals(sparkValue, jdbcValue);
        }

        // For other types, use Object.equals
        return Objects.equals(sparkValue, jdbcValue);
    }
}
