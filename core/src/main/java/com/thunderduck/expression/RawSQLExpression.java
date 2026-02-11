package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.UnresolvedType;
import java.util.Objects;

/**
 * Expression representing a raw SQL expression string from Spark's expr() or selectExpr().
 *
 * <p>This expression contains user-written SQL text that will be passed through directly
 * to DuckDB without parsing or transformation. The SQL text is assumed to be compatible
 * with DuckDB's SQL dialect.
 *
 * <p>Examples from PySpark:
 * <pre>
 *   df.filter("id > 1")                    -- comparison expression
 *   df.filter("status = 'active'")         -- equality with string literal
 *   df.selectExpr("id * 2 as doubled")     -- arithmetic with alias
 *   df.filter("length(name) > 5")          -- function call
 * </pre>
 *
 * <p>Since the SQL text is not parsed, the data type is unknown until DuckDB evaluates it.
 * The type is represented as {@link UnresolvedType}, and nullability is conservatively
 * assumed to be true.
 *
 * <p><b>Limitations</b>: This approach works for most SQL expressions, but may fail if:
 * <ul>
 *   <li>Spark and DuckDB have different function names (e.g., size() vs length())</li>
 *   <li>Spark-specific syntax is used (e.g., Hive UDFs, RLIKE)</li>
 *   <li>Type declaration syntax differs (e.g., ARRAY&lt;INT&gt; vs INT[])</li>
 * </ul>
 *
 * <p>When failures occur, users should either:
 * <ul>
 *   <li>Use the DataFrame API instead of SQL expression strings</li>
 *   <li>Rewrite the expression in DuckDB-compatible SQL</li>
 *   <li>Report the incompatibility for future translation support</li>
 * </ul>
 */
public final class RawSQLExpression implements Expression {

    private final String sqlText;
    private final DataType explicitDataType;
    private final boolean explicitNullable;
    private final boolean hasExplicitMetadata;

    /**
     * Creates a raw SQL expression with unknown type and nullable=true.
     *
     * @param sqlText the SQL expression text
     * @throws NullPointerException if sqlText is null
     */
    public RawSQLExpression(String sqlText) {
        this.sqlText = Objects.requireNonNull(sqlText, "sqlText must not be null");
        this.explicitDataType = null;
        this.explicitNullable = true;
        this.hasExplicitMetadata = false;
    }

    /**
     * Creates a raw SQL expression with explicit type and nullable metadata.
     *
     * <p>Used when the type and nullability of the raw SQL can be determined
     * at parse time (e.g., INTERVAL literals are always non-null).
     *
     * @param sqlText the SQL expression text
     * @param dataType the known data type
     * @param nullable whether the expression can be null
     */
    public RawSQLExpression(String sqlText, DataType dataType, boolean nullable) {
        this.sqlText = Objects.requireNonNull(sqlText, "sqlText must not be null");
        this.explicitDataType = dataType;
        this.explicitNullable = nullable;
        this.hasExplicitMetadata = true;
    }

    /**
     * Returns the raw SQL expression text.
     *
     * @return the SQL text
     */
    public String sqlText() {
        return sqlText;
    }

    @Override
    public DataType dataType() {
        if (hasExplicitMetadata && explicitDataType != null) {
            return explicitDataType;
        }
        // Type is unknown until DuckDB evaluates the expression
        return UnresolvedType.expressionString();
    }

    @Override
    public boolean nullable() {
        if (hasExplicitMetadata) {
            return explicitNullable;
        }
        // Conservatively assume the expression might return null
        return true;
    }

    @Override
    public String toSQL() {
        // Translate Spark function names to DuckDB equivalents before returning
        // This enables SQL expression strings like selectExpr("collect_list(id)") to work
        // Note: Only handles DIRECT_MAPPINGS (simple 1:1 name replacements)
        // Wrapping in parentheses breaks AS clauses (e.g., "salary * 2 as doubled")
        // and is not needed since SQL expression strings are already complete expressions
        return com.thunderduck.functions.FunctionRegistry.rewriteSQL(sqlText);
    }

    @Override
    public String toString() {
        return "RawSQL(" + sqlText + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RawSQLExpression)) return false;
        RawSQLExpression that = (RawSQLExpression) obj;
        return sqlText.equals(that.sqlText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sqlText);
    }
}
