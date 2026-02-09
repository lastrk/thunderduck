package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing a struct literal.
 *
 * <p>Struct literals contain named field-value pairs. This class preserves
 * the field names and value expressions to enable proper type inference
 * by examining the types of each field value.
 *
 * <p>SQL form: {'field1': val1, 'field2': val2}
 * <p>Spark form: struct(named_struct('field1', val1, 'field2', val2))
 *
 * <p>Type inference returns StructType with fields inferred from the literal.
 */
public final class StructLiteralExpression implements Expression {

    private final List<String> fieldNames;
    private final List<Expression> fieldValues;

    /**
     * Creates a struct literal expression.
     *
     * @param fieldNames the field names (must match fieldValues size)
     * @param fieldValues the field values
     * @throws IllegalArgumentException if fieldNames and fieldValues have different sizes,
     *         or if fieldNames is empty
     */
    public StructLiteralExpression(List<String> fieldNames, List<Expression> fieldValues) {
        Objects.requireNonNull(fieldNames, "fieldNames must not be null");
        Objects.requireNonNull(fieldValues, "fieldValues must not be null");

        if (fieldNames.size() != fieldValues.size()) {
            throw new IllegalArgumentException(
                "fieldNames and fieldValues must have the same size: " +
                fieldNames.size() + " vs " + fieldValues.size());
        }

        if (fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Struct literal requires at least one field");
        }

        this.fieldNames = new ArrayList<>(fieldNames);
        this.fieldValues = new ArrayList<>(fieldValues);
    }

    /**
     * Returns the field names.
     *
     * @return an unmodifiable list of field names
     */
    public List<String> fieldNames() {
        return Collections.unmodifiableList(fieldNames);
    }

    /**
     * Returns the field values.
     *
     * @return an unmodifiable list of field value expressions
     */
    public List<Expression> fieldValues() {
        return Collections.unmodifiableList(fieldValues);
    }

    /**
     * Returns the number of fields.
     *
     * @return field count
     */
    public int size() {
        return fieldNames.size();
    }

    /**
     * Returns the data type of this struct literal.
     *
     * <p>Infers the type of each field from its value expression.
     *
     * @return StructType with inferred field types
     */
    @Override
    public DataType dataType() {
        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType type = fieldValues.get(i).dataType();
            boolean nullable = fieldValues.get(i).nullable();
            fields.add(new StructField(name, type, nullable));
        }
        return new StructType(fields);
    }

    /**
     * Returns whether this struct can be null.
     *
     * <p>Struct literals themselves are not null (they're constant values).
     *
     * @return false (struct literals are not null)
     */
    @Override
    public boolean nullable() {
        return false;
    }

    /**
     * Generates the SQL representation of this struct literal.
     *
     * @return SQL string in the form "{'field1': val1, 'field2': val2}"
     */
    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder("{");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            // Use single quotes for field names in DuckDB struct syntax
            String quotedName = "'" + fieldNames.get(i).replace("'", "''") + "'";
            sql.append(quotedName).append(": ").append(fieldValues.get(i).toSQL());
        }
        sql.append("}");
        return sql.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof StructLiteralExpression)) return false;
        StructLiteralExpression that = (StructLiteralExpression) obj;
        return Objects.equals(fieldNames, that.fieldNames) &&
               Objects.equals(fieldValues, that.fieldValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldValues);
    }

    @Override
    public String toString() {
        return "StructLiteral(" + fieldNames.size() + " fields)";
    }
}
