package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression for adding, replacing, or dropping fields in a struct.
 *
 * <p>Represents Spark's struct field manipulation operations:
 * <ul>
 *   <li>{@code col.withField("field", value)} - add/replace a field</li>
 *   <li>{@code col.dropFields("field")} - remove a field</li>
 * </ul>
 *
 * <p>DuckDB translation:
 * <ul>
 *   <li>Add/Replace: {@code struct_insert(struct, field := value)}</li>
 *   <li>Drop: Requires rebuilding the struct without the dropped field</li>
 * </ul>
 *
 * <p>Examples:
 * <pre>
 *   person.withField("age", lit(30))
 *   → struct_insert(person, age := 30)
 *
 *   person.dropFields("age")
 *   → struct_pack(name := person.name)  -- rebuild without 'age'
 * </pre>
 *
 * <p>Note: Drop operation requires knowing all struct fields, which may not
 * always be available at conversion time. In such cases, a raw SQL approach
 * or error is used.
 */
public final class UpdateFieldsExpression implements Expression {

    /**
     * The type of struct field operation.
     */
    public enum OperationType {
        /** Add or replace a field */
        ADD_OR_REPLACE,
        /** Drop/remove a field */
        DROP
    }

    private final Expression structExpr;
    private final String fieldName;
    private final Optional<Expression> valueExpr;
    private final OperationType operationType;

    /**
     * Creates an UpdateFieldsExpression for adding/replacing a field.
     *
     * @param structExpr the struct expression to modify
     * @param fieldName the field name to add/replace
     * @param valueExpr the value for the field
     */
    public UpdateFieldsExpression(Expression structExpr, String fieldName, Expression valueExpr) {
        this.structExpr = Objects.requireNonNull(structExpr, "structExpr must not be null");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.valueExpr = Optional.of(Objects.requireNonNull(valueExpr, "valueExpr must not be null"));
        this.operationType = OperationType.ADD_OR_REPLACE;
    }

    /**
     * Creates an UpdateFieldsExpression for dropping a field.
     *
     * @param structExpr the struct expression to modify
     * @param fieldName the field name to drop
     */
    public UpdateFieldsExpression(Expression structExpr, String fieldName) {
        this.structExpr = Objects.requireNonNull(structExpr, "structExpr must not be null");
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.valueExpr = Optional.empty();
        this.operationType = OperationType.DROP;
    }

    /**
     * Returns the struct expression being modified.
     *
     * @return the struct expression
     */
    public Expression structExpr() {
        return structExpr;
    }

    /**
     * Returns the field name being modified.
     *
     * @return the field name
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Returns the value expression for add/replace operations.
     *
     * @return the value expression, if present
     */
    public Optional<Expression> valueExpr() {
        return valueExpr;
    }

    /**
     * Returns the operation type.
     *
     * @return ADD_OR_REPLACE or DROP
     */
    public OperationType operationType() {
        return operationType;
    }

    @Override
    public DataType dataType() {
        // Returns a struct (same or modified)
        return structExpr.dataType();
    }

    @Override
    public boolean nullable() {
        return structExpr.nullable();
    }

    /**
     * Converts to DuckDB SQL.
     *
     * <p>Add/Replace uses struct_insert:
     * <pre>
     *   struct_insert(struct, field := value)
     * </pre>
     *
     * <p>Drop requires struct reconstruction, which is complex without schema.
     * We output a placeholder comment for drop operations.
     *
     * @return the SQL string
     */
    @Override
    public String toSQL() {
        String structSql = structExpr.toSQL();

        if (operationType == OperationType.ADD_OR_REPLACE) {
            // struct_insert(struct, field := value)
            String valueSql = valueExpr.get().toSQL();
            return String.format("struct_insert(%s, %s := %s)",
                structSql, escapeFieldName(fieldName), valueSql);
        } else {
            // DROP operation - this is complex because we need to know all fields
            // DuckDB doesn't have a direct "drop field from struct" function
            // Options:
            // 1. Use a subquery with explicit field selection
            // 2. Error/warn and let the user handle it
            // 3. Generate a placeholder that could be resolved later
            //
            // For now, generate an unsupported operation marker
            // A full implementation would need schema introspection
            return String.format("/* UNSUPPORTED: drop field '%s' from struct */ %s",
                fieldName, structSql);
        }
    }

    /**
     * Escapes a field name for use in SQL.
     *
     * @param name the field name
     * @return the escaped name
     */
    private String escapeFieldName(String name) {
        // If the name contains special characters, quote it
        if (name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            return name;  // Simple identifier, no quoting needed
        }
        // Use double quotes for identifiers with special characters
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof UpdateFieldsExpression)) return false;
        UpdateFieldsExpression that = (UpdateFieldsExpression) obj;
        return Objects.equals(structExpr, that.structExpr) &&
               Objects.equals(fieldName, that.fieldName) &&
               Objects.equals(valueExpr, that.valueExpr) &&
               operationType == that.operationType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(structExpr, fieldName, valueExpr, operationType);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates an expression to add or replace a field in a struct.
     *
     * @param struct the struct expression
     * @param fieldName the field name
     * @param value the value for the field
     * @return the update fields expression
     */
    public static UpdateFieldsExpression withField(Expression struct, String fieldName, Expression value) {
        return new UpdateFieldsExpression(struct, fieldName, value);
    }

    /**
     * Creates an expression to drop a field from a struct.
     *
     * @param struct the struct expression
     * @param fieldName the field name to drop
     * @return the update fields expression
     */
    public static UpdateFieldsExpression dropField(Expression struct, String fieldName) {
        return new UpdateFieldsExpression(struct, fieldName);
    }
}
