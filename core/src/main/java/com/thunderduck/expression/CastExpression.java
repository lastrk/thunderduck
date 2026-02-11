package com.thunderduck.expression;

import com.thunderduck.types.ArrayType;
import com.thunderduck.types.BinaryType;
import com.thunderduck.types.BooleanType;
import com.thunderduck.types.ByteType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DateType;
import com.thunderduck.types.DecimalType;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.FloatType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.LongType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.ShortType;
import com.thunderduck.types.TimestampType;
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
        return generateCastSQL(innerSQL, expression, targetType);
    }

    /** How TRUNC should be applied for an integral cast. */
    private enum TruncMode {
        /** No TRUNC needed (source is already integral, boolean, date, etc.) */
        NONE,
        /** Direct TRUNC: source is a known floating-point or decimal type. */
        DIRECT,
        /** TRUNC via DOUBLE cast: source type is unknown at compile time
         *  (e.g., UnresolvedColumn returning StringType placeholder).
         *  Uses TRUNC(CAST(x AS DOUBLE)) so DuckDB can handle any source type. */
        VIA_DOUBLE
    }

    /**
     * Generates CAST SQL with TRUNC wrapping when needed for Spark compatibility.
     *
     * <p>When casting to an integral type (INTEGER, BIGINT, SMALLINT, TINYINT) from
     * a type that might have fractional values, wraps with TRUNC to match Spark's
     * truncation-toward-zero semantics (DuckDB rounds by default).
     *
     * <p>Three modes:
     * <ul>
     *   <li>Known numeric (DOUBLE/FLOAT/DECIMAL): {@code CAST(TRUNC(x) AS INTEGER)}</li>
     *   <li>Unknown type (UnresolvedColumn): {@code CAST(TRUNC(CAST(x AS DOUBLE)) AS INTEGER)}</li>
     *   <li>Known non-fractional (INT/BOOLEAN/DATE/etc.): {@code CAST(x AS INTEGER)}</li>
     * </ul>
     *
     * @param innerSQL the SQL string for the inner expression
     * @param innerExpr the inner expression (used for type checking)
     * @param targetType the target data type
     * @return the CAST SQL string
     */
    public static String generateCastSQL(String innerSQL, Expression innerExpr, DataType targetType) {
        String sqlTypeName = resolveDuckDBTypeName(targetType);
        if (isIntegralTargetType(targetType)) {
            TruncMode mode = truncMode(innerExpr);
            if (mode == TruncMode.DIRECT) {
                return String.format("CAST(TRUNC(%s) AS %s)", innerSQL, sqlTypeName);
            }
            if (mode == TruncMode.VIA_DOUBLE) {
                return String.format("CAST(TRUNC(CAST(%s AS DOUBLE)) AS %s)", innerSQL, sqlTypeName);
            }
        }
        return String.format("CAST(%s AS %s)", innerSQL, sqlTypeName);
    }

    /**
     * Checks whether the target type is an integral type (INTEGER, BIGINT, SMALLINT, TINYINT).
     */
    private static boolean isIntegralTargetType(DataType type) {
        return type instanceof IntegerType
            || type instanceof LongType
            || type instanceof ShortType
            || type instanceof ByteType;
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
     * Determines how TRUNC should be applied for an integral cast.
     *
     * <p>The logic considers both the declared data type and the expression kind:
     * <ul>
     *   <li>Known integer types: no TRUNC needed (already whole numbers).</li>
     *   <li>Known non-numeric types (BOOLEAN, DATE, TIMESTAMP, BINARY): no TRUNC
     *       (DuckDB's TRUNC doesn't accept these, and they don't have fractional parts).</li>
     *   <li>Known floating-point/decimal types: direct TRUNC ({@code TRUNC(x)}).</li>
     *   <li>Unknown types (StringType from UnresolvedColumn, or any other unresolved type):
     *       TRUNC via DOUBLE cast ({@code TRUNC(CAST(x AS DOUBLE))}) because
     *       DuckDB's TRUNC doesn't accept VARCHAR but the actual column might be
     *       a DOUBLE at runtime. The extra CAST-to-DOUBLE is safe for strings
     *       containing decimal numbers (e.g., "3.7") and a no-op for actual DOUBLEs.</li>
     *   <li>Integer-returning function calls (date extraction, TRUNC itself): no TRUNC.</li>
     * </ul>
     *
     * @param expr the inner expression
     * @return the TRUNC mode to use
     */
    private static TruncMode truncMode(Expression expr) {
        // If the source is a function call that already returns integers
        // (e.g., date extraction) or is already TRUNC, skip the wrapper
        if (expr instanceof FunctionCall fc) {
            String name = fc.functionName().toLowerCase();
            if (INTEGER_RETURNING_FUNCTIONS.contains(name) || name.equals("trunc")) {
                return TruncMode.NONE;
            }
        }

        DataType srcType = expr.dataType();

        // Known integer types: no truncation needed
        if (srcType instanceof IntegerType
                || srcType instanceof LongType
                || srcType instanceof ShortType
                || srcType instanceof ByteType) {
            return TruncMode.NONE;
        }

        // Known floating-point/decimal types: direct TRUNC
        if (srcType instanceof DoubleType
                || srcType instanceof FloatType
                || srcType instanceof DecimalType) {
            return TruncMode.DIRECT;
        }

        // Known non-numeric types: no TRUNC (DuckDB TRUNC doesn't accept these,
        // and they don't have fractional parts)
        if (srcType instanceof BooleanType
                || srcType instanceof DateType
                || srcType instanceof TimestampType
                || srcType instanceof BinaryType) {
            return TruncMode.NONE;
        }

        // Unknown type (StringType from UnresolvedColumn, or any other unresolved type):
        // The actual runtime type might be DOUBLE, so apply TRUNC via DOUBLE cast.
        // This is safe: TRUNC(CAST('123' AS DOUBLE)) = 123.0, TRUNC(CAST(3.7 AS DOUBLE)) = 3.0
        return TruncMode.VIA_DOUBLE;
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