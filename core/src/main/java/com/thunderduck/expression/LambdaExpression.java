package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing a lambda function.
 *
 * <p>Lambda functions are anonymous functions used in higher-order functions like
 * transform, filter, aggregate, etc.
 *
 * <p>Examples:
 * <pre>
 *   x -> x + 1                 -- single parameter lambda
 *   (acc, x) -> acc + x        -- two parameter lambda (for reduce/aggregate)
 *   (x, i) -> x + i            -- element with index lambda
 * </pre>
 *
 * <p>Lambda expressions generate DuckDB Python-style syntax:
 * <pre>
 *   lambda x: x + 1
 *   lambda acc, x: acc + x
 * </pre>
 */
public final class LambdaExpression implements Expression {

    private final List<String> parameters;
    private final Expression body;
    private final DataType returnType;

    /**
     * Creates a lambda expression.
     *
     * @param parameters the parameter names (1-3 parameters)
     * @param body the lambda body expression
     * @param returnType the return data type of the lambda
     */
    public LambdaExpression(List<String> parameters, Expression body, DataType returnType) {
        if (parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Lambda must have at least one parameter");
        }
        if (parameters.size() > 3) {
            throw new IllegalArgumentException("Lambda can have at most 3 parameters");
        }
        this.parameters = new ArrayList<>(parameters);
        this.body = Objects.requireNonNull(body, "body must not be null");
        this.returnType = Objects.requireNonNull(returnType, "returnType must not be null");
    }

    /**
     * Creates a lambda expression with inferred return type from body.
     *
     * @param parameters the parameter names
     * @param body the lambda body expression
     */
    public LambdaExpression(List<String> parameters, Expression body) {
        this(parameters, body, body.dataType());
    }

    /**
     * Returns the lambda parameter names.
     *
     * @return an unmodifiable list of parameter names
     */
    public List<String> parameters() {
        return Collections.unmodifiableList(parameters);
    }

    /**
     * Returns the lambda body expression.
     *
     * @return the body expression
     */
    public Expression body() {
        return body;
    }

    /**
     * Returns the number of parameters.
     *
     * @return the parameter count
     */
    public int parameterCount() {
        return parameters.size();
    }

    @Override
    public DataType dataType() {
        // Lambda itself doesn't have a simple data type -
        // it's a function type. Return the body's type for now.
        return returnType;
    }

    @Override
    public boolean nullable() {
        return body.nullable();
    }

    /**
     * Converts this lambda to DuckDB Python-style syntax.
     *
     * <p>Examples:
     * <pre>
     *   lambda x: x + 1
     *   lambda acc, x: acc + x
     * </pre>
     *
     * @return the SQL string
     */
    @Override
    public String toSQL() {
        String paramList = String.join(", ", parameters);
        return "lambda " + paramList + ": " + body.toSQL();
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LambdaExpression)) return false;
        LambdaExpression that = (LambdaExpression) obj;
        return Objects.equals(parameters, that.parameters) &&
               Objects.equals(body, that.body) &&
               Objects.equals(returnType, that.returnType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, body, returnType);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a single-parameter lambda.
     *
     * @param param the parameter name
     * @param body the lambda body
     * @return the lambda expression
     */
    public static LambdaExpression of(String param, Expression body) {
        return new LambdaExpression(Collections.singletonList(param), body);
    }

    /**
     * Creates a two-parameter lambda.
     *
     * @param param1 the first parameter name
     * @param param2 the second parameter name
     * @param body the lambda body
     * @return the lambda expression
     */
    public static LambdaExpression of(String param1, String param2, Expression body) {
        return new LambdaExpression(List.of(param1, param2), body);
    }

    /**
     * Creates a three-parameter lambda.
     *
     * @param param1 the first parameter name
     * @param param2 the second parameter name
     * @param param3 the third parameter name
     * @param body the lambda body
     * @return the lambda expression
     */
    public static LambdaExpression of(String param1, String param2, String param3, Expression body) {
        return new LambdaExpression(List.of(param1, param2, param3), body);
    }
}
