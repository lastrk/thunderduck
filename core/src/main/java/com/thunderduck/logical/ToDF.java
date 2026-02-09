package com.thunderduck.logical;

import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing column renaming (toDF operation).
 *
 * <p>This renames all columns in the DataFrame to the provided names.
 * The number of names must match the number of columns in the input.
 *
 * <p>Examples:
 * <pre>
 *   df.toDF("new_col1", "new_col2", "new_col3")
 * </pre>
 */
public final class ToDF extends LogicalPlan {

    private final List<String> columnNames;

    /**
     * Creates a ToDF node.
     *
     * @param child the child node
     * @param columnNames the new column names
     */
    public ToDF(LogicalPlan child, List<String> columnNames) {
        super(child);
        this.columnNames = new ArrayList<>(Objects.requireNonNull(columnNames, "columnNames must not be null"));
    }

    /**
     * Returns the new column names.
     *
     * @return an unmodifiable list of column names
     */
    public List<String> columnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        String inputSql = generator.generate(child());

        if (columnNames.isEmpty()) {
            return inputSql;
        }

        // Build column alias list
        StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                columnList.append(", ");
            }
            columnList.append(SQLQuoting.quoteIdentifier(columnNames.get(i)));
        }

        // DuckDB supports table alias with column names: AS alias(col1, col2, ...)
        return String.format("SELECT * FROM (%s) AS _todf_subquery(%s)",
            inputSql, columnList.toString());
    }

    @Override
    public StructType inferSchema() {
        if (columnNames.isEmpty()) {
            return child().schema();
        }

        // Get child schema
        StructType childSchema = child().schema();
        if (childSchema == null) {
            return null;
        }

        // Create new schema with renamed columns
        List<StructField> fields = new ArrayList<>();
        List<StructField> childFields = childSchema.fields();

        for (int i = 0; i < columnNames.size(); i++) {
            String newName = columnNames.get(i);

            // Preserve type and nullable from child if available
            if (i < childFields.size()) {
                StructField childField = childFields.get(i);
                fields.add(new StructField(newName, childField.dataType(), childField.nullable()));
            } else {
                // More names than child columns - shouldn't happen normally
                fields.add(new StructField(newName, com.thunderduck.types.StringType.get(), true));
            }
        }

        return new StructType(fields);
    }

    @Override
    public String toString() {
        return String.format("ToDF(%s)", columnNames);
    }
}
