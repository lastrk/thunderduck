package com.thunderduck.expression;

import com.thunderduck.functions.FunctionRegistry;
import com.thunderduck.types.ArrayType;
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
    private final boolean distinct;

    /**
     * Creates a function call expression.
     *
     * @param functionName the function name (Spark SQL)
     * @param arguments the function arguments
     * @param dataType the return data type
     * @param nullable whether the result can be null
     * @param distinct whether DISTINCT is applied to arguments
     */
    public FunctionCall(String functionName, List<Expression> arguments,
                       DataType dataType, boolean nullable, boolean distinct) {
        this.functionName = Objects.requireNonNull(functionName, "functionName must not be null");
        if (this.functionName.trim().isEmpty()) {
            throw new IllegalArgumentException("functionName must not be empty");
        }
        this.arguments = new ArrayList<>(Objects.requireNonNull(arguments, "arguments must not be null"));
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
        this.nullable = nullable;
        this.distinct = distinct;
    }

    /**
     * Creates a function call expression (non-distinct).
     *
     * @param functionName the function name (Spark SQL)
     * @param arguments the function arguments
     * @param dataType the return data type
     * @param nullable whether the result can be null
     */
    public FunctionCall(String functionName, List<Expression> arguments,
                       DataType dataType, boolean nullable) {
        this(functionName, arguments, dataType, nullable, false);
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

    /**
     * Returns whether DISTINCT is applied to arguments.
     *
     * @return true if DISTINCT is applied
     */
    public boolean distinct() {
        return distinct;
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
        // Handle struct() with named arguments (AliasExpression args).
        // Spark: struct(lit(99).alias("id"), lit("Carol").alias("name"))
        // DuckDB: struct_pack(id := 99, name := 'Carol')
        // The standard row() function doesn't support AS aliases.
        if (functionName.equalsIgnoreCase("struct") && !arguments.isEmpty()
                && arguments.stream().anyMatch(a -> a instanceof AliasExpression)) {
            StringBuilder sb = new StringBuilder("struct_pack(");
            for (int i = 0; i < arguments.size(); i++) {
                if (i > 0) sb.append(", ");
                Expression arg = arguments.get(i);
                if (arg instanceof AliasExpression alias) {
                    sb.append(alias.alias()).append(" := ").append(alias.expression().toSQL());
                } else {
                    sb.append(arg.toSQL());
                }
            }
            sb.append(")");
            return sb.toString();
        }

        // Convert arguments to SQL strings
        String[] argStrings = arguments.stream()
            .map(Expression::toSQL)
            .toArray(String[]::new);

        // Handle polymorphic functions that need type-based dispatch.
        // Spark's reverse() works on both strings and arrays, but DuckDB requires
        // reverse() for strings and list_reverse() for arrays.
        String effectiveName = functionName;
        if (functionName.equalsIgnoreCase("reverse") && !arguments.isEmpty()
                && arguments.get(0).dataType() instanceof ArrayType) {
            effectiveName = "list_reverse";
        }

        // Spark's size() works on both arrays and maps.
        // DuckDB: len() works on arrays, cardinality() works on maps.
        // Default mapping uses len() (for arrays). For maps, type-aware dispatch
        // would be needed but requires schema resolution (UnresolvedColumn returns StringType).
        // Map size is handled here when type info is available (e.g., typed expressions).
        if (functionName.equalsIgnoreCase("size") && !arguments.isEmpty()
                && arguments.get(0).dataType() instanceof com.thunderduck.types.MapType) {
            return "CAST(cardinality(" + argStrings[0] + ") AS INTEGER)";
        }

        // DISTINCT is handled by building the SQL directly rather than going through
        // FunctionRegistry.translate(), since DISTINCT only applies to standard aggregates
        // where we control the output format (e.g., COUNT(DISTINCT x), SUM(DISTINCT x)).
        if (distinct) {
            String duckdbName = FunctionRegistry.resolveName(effectiveName);
            String argsSQL = String.join(", ", argStrings);
            return duckdbName + "(DISTINCT " + argsSQL + ")";
        }

        // Translate using function registry
        String sql;
        try {
            sql = FunctionRegistry.translate(effectiveName, argStrings);
        } catch (UnsupportedOperationException e) {
            // Fallback to direct function call if not in registry
            String argsSQL = String.join(", ", argStrings);
            sql = effectiveName + "(" + argsSQL + ")";
        }

        // DuckDB returns BIGINT for string length functions, but Spark returns INTEGER.
        // Wrap with CAST to match Spark's return type.
        if (isStringLengthFunction(functionName)) {
            return "CAST(" + sql + " AS INTEGER)";
        }

        // DuckDB's list_position returns INTEGER, but Spark's array_position returns BIGINT.
        if (functionName.equalsIgnoreCase("array_position")) {
            return "CAST(" + sql + " AS BIGINT)";
        }

        return sql;
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
               distinct == that.distinct &&
               Objects.equals(functionName, that.functionName) &&
               Objects.equals(arguments, that.arguments) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, arguments, dataType, nullable, distinct);
    }

    /**
     * Returns whether this function is a string length function that returns BIGINT in DuckDB
     * but should return INTEGER to match Spark semantics.
     */
    private static boolean isStringLengthFunction(String funcName) {
        String lower = funcName.toLowerCase();
        return lower.equals("length") || lower.equals("char_length") || lower.equals("character_length");
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
