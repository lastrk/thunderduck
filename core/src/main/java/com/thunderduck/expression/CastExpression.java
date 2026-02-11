package com.thunderduck.expression;

import com.thunderduck.types.ArrayType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DecimalType;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.FloatType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.TypeMapper;
import java.util.Objects;
import java.util.Set;

/**
 * Expression that casts another expression to a different data type.
 *
 * <p>Examples:
 * <pre>
 *   CAST(amount AS DECIMAL(10,2))
 *   CAST(date_str AS DATE)
 *   CAST(123 AS VARCHAR)
 * </pre>
 */
public final class CastExpression implements Expression {

    private final Expression expression;
    private final DataType targetType;

    /**
     * Creates a cast expression.
     *
     * @param expression the expression to cast
     * @param targetType the target data type
     */
    public CastExpression(Expression expression, DataType targetType) {
        this.expression = Objects.requireNonNull(expression, "expression must not be null");
        this.targetType = Objects.requireNonNull(targetType, "targetType must not be null");
    }

    /**
     * Returns the expression being cast.
     *
     * @return the expression
     */
    public Expression expression() {
        return expression;
    }

    /**
     * Returns the target data type.
     *
     * @return the target type
     */
    public DataType targetType() {
        return targetType;
    }

    @Override
    public DataType dataType() {
        return targetType;
    }

    @Override
    public boolean nullable() {
        // Cast preserves nullability
        return expression.nullable();
    }

    /** Date extraction functions that already return integers -- no TRUNC needed. */
    private static final Set<String> INTEGER_RETURNING_FUNCTIONS = Set.of(
        "year", "month", "day", "hour", "minute", "second",
        "quarter", "dayofweek", "dayofyear", "weekofyear"
    );

    /**
     * Converts this cast expression to its SQL string representation.
     *
     * <p>When the target type is INTEGER or INT, the inner expression is wrapped
     * with TRUNC to match Spark's truncation-toward-zero semantics (DuckDB may
     * round instead). The TRUNC wrapper is skipped when the inner expression is
     * already an integer type, a date-extraction function, or already a TRUNC call.
     *
     * @return the SQL string
     */
    public String toSQL() {
        String innerSQL = expression.toSQL();
        // Use DuckDB type syntax for complex types (ArrayType, MapType)
        // e.g., ARRAY<INT> -> INTEGER[], MAP<STRING,INT> -> MAP(VARCHAR, INTEGER)
        // For simple types, use uppercase to match Spark column naming convention.
        String sqlTypeName = resolveDuckDBTypeName(targetType);
        String targetName = targetType.typeName().toUpperCase();

        if (targetName.equals("INTEGER") || targetName.equals("INT")) {
            if (!needsTrunc(expression)) {
                return String.format("CAST(%s AS %s)", innerSQL, sqlTypeName);
            }
            return String.format("CAST(TRUNC(%s) AS %s)", innerSQL, sqlTypeName);
        }

        return String.format("CAST(%s AS %s)", innerSQL, sqlTypeName);
    }

    /**
     * Returns the type name in uppercase, preserving parameterized suffixes.
     * E.g., "decimal(15,4)" → "DECIMAL(15,4)", "varchar" → "VARCHAR".
     */
    public static String uppercaseTypeName(DataType type) {
        String name = type.typeName();
        int parenIdx = name.indexOf('(');
        if (parenIdx >= 0) {
            return name.substring(0, parenIdx).toUpperCase() + name.substring(parenIdx);
        }
        return name.toUpperCase();
    }

    /**
     * Resolves the DuckDB type name for a target type.
     * Uses TypeMapper for complex types (ArrayType, MapType) to get proper
     * DuckDB syntax (e.g., INTEGER[] instead of array&lt;integer&gt;).
     * Falls back to uppercaseTypeName() for primitive types.
     */
    private static String resolveDuckDBTypeName(DataType type) {
        if (type instanceof ArrayType || type instanceof MapType) {
            try {
                return TypeMapper.toDuckDBType(type);
            } catch (Exception e) {
                return uppercaseTypeName(type);
            }
        }
        return uppercaseTypeName(type);
    }

    /**
     * Determines whether the inner expression needs a TRUNC wrapper for an
     * INTEGER cast.
     *
     * @param expr the inner expression
     * @return true if TRUNC should be applied
     */
    private static boolean needsTrunc(Expression expr) {
        // Only apply TRUNC for floating-point numeric types where DuckDB
        // might round instead of truncating toward zero (Spark semantics).
        // TRUNC is NOT valid for VARCHAR, BOOLEAN, DATE, TIMESTAMP, etc.
        DataType srcType = expr.dataType();
        if (!(srcType instanceof DoubleType
                || srcType instanceof FloatType
                || srcType instanceof DecimalType)) {
            return false;
        }

        // If the source is a function call that already returns integers
        // (e.g., date extraction) or is already TRUNC, skip the wrapper
        if (expr instanceof FunctionCall fc) {
            String name = fc.functionName().toLowerCase();
            if (INTEGER_RETURNING_FUNCTIONS.contains(name) || name.equals("trunc")) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CastExpression)) return false;
        CastExpression that = (CastExpression) obj;
        return Objects.equals(expression, that.expression) &&
               Objects.equals(targetType, that.targetType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, targetType);
    }
}