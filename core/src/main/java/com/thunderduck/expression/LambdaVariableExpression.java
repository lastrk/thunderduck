package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;

/**
 * Expression representing a lambda variable reference.
 *
 * <p>Lambda variables are parameters bound within lambda expressions.
 * They are referenced by name within the lambda body.
 *
 * <p>Examples:
 * <pre>
 *   x         -- single variable reference
 *   acc       -- accumulator variable in reduce/aggregate
 *   key       -- key variable in map operations
 *   value     -- value variable in map operations
 * </pre>
 *
 * <p>Lambda variables are emitted as plain identifiers in the generated SQL.
 * They are NOT quoted since they are internal to the lambda expression.
 */
public final class LambdaVariableExpression implements Expression {

    private final String variableName;
    private final DataType dataType;

    /**
     * Creates a lambda variable expression.
     *
     * @param variableName the variable name
     * @param dataType the data type of the variable (inferred from context)
     */
    public LambdaVariableExpression(String variableName, DataType dataType) {
        if (variableName == null || variableName.trim().isEmpty()) {
            throw new IllegalArgumentException("variableName must not be null or empty");
        }
        this.variableName = variableName;
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
    }

    /**
     * Creates a lambda variable expression with unknown type.
     *
     * <p>The type defaults to StringType for untyped contexts.
     *
     * @param variableName the variable name
     */
    public LambdaVariableExpression(String variableName) {
        this(variableName, StringType.get());
    }

    /**
     * Returns the variable name.
     *
     * @return the variable name
     */
    public String variableName() {
        return variableName;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        // Lambda variables can be nullable depending on the input array
        return true;
    }

    /**
     * Converts this variable reference to SQL.
     *
     * <p>Lambda variables are emitted as plain identifiers without quotes.
     *
     * @return the variable name
     */
    @Override
    public String toSQL() {
        return variableName;
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LambdaVariableExpression)) return false;
        LambdaVariableExpression that = (LambdaVariableExpression) obj;
        return Objects.equals(variableName, that.variableName) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variableName, dataType);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a lambda variable with the given name and type.
     *
     * @param name the variable name
     * @param dataType the data type
     * @return the lambda variable expression
     */
    public static LambdaVariableExpression of(String name, DataType dataType) {
        return new LambdaVariableExpression(name, dataType);
    }

    /**
     * Creates a lambda variable with the given name (untyped).
     *
     * @param name the variable name
     * @return the lambda variable expression
     */
    public static LambdaVariableExpression of(String name) {
        return new LambdaVariableExpression(name);
    }
}
