package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Logical plan node representing adding or replacing columns.
 *
 * <p>This is used when Spark's withColumn() or select() with new columns
 * is called. It preserves all existing columns (except those being replaced)
 * and adds the new/replacement columns.
 *
 * <p>Examples:
 * <pre>
 *   df.withColumn("total", col("price") * col("quantity"))
 *   df.withColumn("row_num", row_number().over(window))
 * </pre>
 */
public final class WithColumns extends LogicalPlan {

    private final List<String> columnNames;
    private final List<Expression> columnExpressions;

    /**
     * Creates a WithColumns node.
     *
     * @param child the child node
     * @param columnNames the names of columns to add/replace
     * @param columnExpressions the expressions for each column
     */
    public WithColumns(LogicalPlan child, List<String> columnNames, List<Expression> columnExpressions) {
        super(child);
        this.columnNames = new ArrayList<>(Objects.requireNonNull(columnNames, "columnNames must not be null"));
        this.columnExpressions = new ArrayList<>(Objects.requireNonNull(columnExpressions, "columnExpressions must not be null"));

        if (columnNames.size() != columnExpressions.size()) {
            throw new IllegalArgumentException("columnNames and columnExpressions must have the same size");
        }
    }

    /**
     * Returns the column names being added/replaced.
     *
     * @return an unmodifiable list of column names
     */
    public List<String> columnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    /**
     * Returns the column expressions.
     *
     * @return an unmodifiable list of expressions
     */
    public List<Expression> columnExpressions() {
        return Collections.unmodifiableList(columnExpressions);
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

        // Build column exclusion list
        StringBuilder excludeFilter = new StringBuilder();
        excludeFilter.append("COLUMNS(c -> c NOT IN (");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                excludeFilter.append(", ");
            }
            excludeFilter.append("'").append(columnNames.get(i).replace("'", "''")).append("'");
        }
        excludeFilter.append("))");

        // Build new column expressions
        StringBuilder newColExprs = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                newColExprs.append(", ");
            }
            newColExprs.append(columnExpressions.get(i).toSQL());
            newColExprs.append(" AS ");
            newColExprs.append(com.thunderduck.generator.SQLQuoting.quoteIdentifier(columnNames.get(i)));
        }

        return String.format("SELECT %s, %s FROM (%s) AS _withcol_subquery",
            excludeFilter.toString(), newColExprs.toString(), inputSql);
    }

    @Override
    public StructType inferSchema() {
        // Get child schema
        StructType childSchema = child().schema();
        if (childSchema == null) {
            return null;
        }

        // Build map of column names being replaced to their index
        // This allows O(1) lookup and tracks which columns are replacements vs new
        Map<String, Integer> replacementIndex = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            replacementIndex.put(columnNames.get(i), i);
        }

        // Track which columns exist in child schema
        Set<String> childColumnNames = new HashSet<>();
        for (StructField field : childSchema.fields()) {
            childColumnNames.add(field.name());
        }

        List<StructField> fields = new ArrayList<>();

        // Iterate through child columns IN ORDER to preserve column positions
        for (StructField field : childSchema.fields()) {
            if (replacementIndex.containsKey(field.name())) {
                // Replace this column at its original position
                int idx = replacementIndex.get(field.name());
                Expression expr = columnExpressions.get(idx);
                DataType type = resolveExpressionType(expr, childSchema);
                boolean nullable = resolveNullable(expr, childSchema);
                fields.add(new StructField(field.name(), type, nullable));
            } else {
                // Keep original column
                fields.add(field);
            }
        }

        // Add any NEW columns (columns that don't exist in child schema) at end
        for (int i = 0; i < columnNames.size(); i++) {
            String name = columnNames.get(i);
            if (!childColumnNames.contains(name)) {
                // This is a new column, not a replacement
                Expression expr = columnExpressions.get(i);
                DataType type = resolveExpressionType(expr, childSchema);
                boolean nullable = resolveNullable(expr, childSchema);
                fields.add(new StructField(name, type, nullable));
            }
        }

        return new StructType(fields);
    }

    /**
     * Resolves the type of an expression using the centralized TypeInferenceEngine.
     */
    private DataType resolveExpressionType(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveType(expr, childSchema);
    }

    /**
     * Resolves the nullability of an expression using the centralized TypeInferenceEngine.
     */
    private boolean resolveNullable(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveNullable(expr, childSchema);
    }

    @Override
    public String toString() {
        return String.format("WithColumns(%s)", columnNames);
    }
}
