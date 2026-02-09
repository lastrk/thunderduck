package com.thunderduck.expression;

import com.thunderduck.functions.FunctionRegistry;
import com.thunderduck.types.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing a function call.
 *
 * <p>Function calls invoke built-in or user-defined functions with zero or more arguments.
 *
 * <p>Examples:
 * <pre>
 *   upper(name)                -- string function
 *   abs(value)                 -- math function
 *   year(date_col)             -- date function
 *   sum(amount)                -- aggregate function
 *   concat(first_name, ' ', last_name)  -- multi-argument function
 * </pre>
 *
 * <p>The function name is mapped from Spark SQL to DuckDB SQL using {@link FunctionRegistry}.
 */
public final class FunctionCall implements Expression {

    private final String functionName;
    private final List<Expression> arguments;
    private final DataType dataType;
    private final boolean nullable;

    /**
     * Creates a function call expression.
     *
     * @param functionName the function name (Spark SQL)
     * @param arguments the function arguments
     * @param dataType the return data type
     * @param nullable whether the result can be null
     */
    public FunctionCall(String functionName, List<Expression> arguments,
                       DataType dataType, boolean nullable) {
        this.functionName = Objects.requireNonNull(functionName, "functionName must not be null");
        if (this.functionName.trim().isEmpty()) {
            throw new IllegalArgumentException("functionName must not be empty");
        }
        this.arguments = new ArrayList<>(Objects.requireNonNull(arguments, "arguments must not be null"));
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
        this.nullable = nullable;
    }

    /**
     * Creates a function call expression with nullable result.
     *
     * @param functionName the function name
     * @param arguments the function arguments
     * @param dataType the return data type
     */
    public FunctionCall(String functionName, List<Expression> arguments, DataType dataType) {
        this(functionName, arguments, dataType, true);
    }

    /**
     * Returns the function name.
     *
     * @return the function name
     */
    public String functionName() {
        return functionName;
    }

    /**
     * Returns the function arguments.
     *
     * @return an unmodifiable list of arguments
     */
    public List<Expression> arguments() {
        return Collections.unmodifiableList(arguments);
    }

    /**
     * Returns the number of arguments.
     *
     * @return the argument count
     */
    public int argumentCount() {
        return arguments.size();
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean nullable() {
        return nullable;
    }

    /**
     * Checks if this function is supported by the function registry.
     *
     * @return true if supported, false otherwise
     */
    public boolean isSupported() {
        return FunctionRegistry.isSupported(functionName);
    }

    /**
     * Converts this function call to its SQL string representation.
     *
     * <p>The function name is translated from Spark SQL to DuckDB SQL using
     * the function registry.
     *
     * @return the SQL string
     */
    public String toSQL() {
        // Convert arguments to SQL strings
        String[] argStrings = arguments.stream()
            .map(Expression::toSQL)
            .toArray(String[]::new);

        // Translate using function registry
        try {
            return FunctionRegistry.translate(functionName, argStrings);
        } catch (UnsupportedOperationException e) {
            // Fallback to direct function call if not in registry
            String argsSQL = String.join(", ", argStrings);
            return functionName + "(" + argsSQL + ")";
        }
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof FunctionCall)) return false;
        FunctionCall that = (FunctionCall) obj;
        return nullable == that.nullable &&
               Objects.equals(functionName, that.functionName) &&
               Objects.equals(arguments, that.arguments) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, arguments, dataType, nullable);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a function call with no arguments.
     *
     * @param functionName the function name
     * @param dataType the return type
     * @return the function call expression
     */
    public static FunctionCall of(String functionName, DataType dataType) {
        return new FunctionCall(functionName, Collections.emptyList(), dataType);
    }

    /**
     * Creates a function call with one argument.
     *
     * @param functionName the function name
     * @param arg the argument
     * @param dataType the return type
     * @return the function call expression
     */
    public static FunctionCall of(String functionName, Expression arg, DataType dataType) {
        return new FunctionCall(functionName, Collections.singletonList(arg), dataType);
    }

    /**
     * Creates a function call with two arguments.
     *
     * @param functionName the function name
     * @param arg1 the first argument
     * @param arg2 the second argument
     * @param dataType the return type
     * @return the function call expression
     */
    public static FunctionCall of(String functionName, Expression arg1, Expression arg2, DataType dataType) {
        List<Expression> args = new ArrayList<>();
        args.add(arg1);
        args.add(arg2);
        return new FunctionCall(functionName, args, dataType);
    }

    /**
     * Creates a function call with multiple arguments.
     *
     * @param functionName the function name
     * @param dataType the return type
     * @param args the arguments
     * @return the function call expression
     */
    public static FunctionCall of(String functionName, DataType dataType, Expression... args) {
        return new FunctionCall(functionName, List.of(args), dataType);
    }
}
