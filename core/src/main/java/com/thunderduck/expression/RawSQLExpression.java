package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;

/**
 * Expression that represents a raw SQL expression string.
 *
 * <p>This is used when expressions are provided as SQL strings that
 * should be passed through directly without parsing.
 *
 * <p>Examples:
 * <pre>
 *   CASE WHEN status = 'active' THEN 1 ELSE 0 END
 *   COALESCE(first_name, last_name, 'Unknown')
 * </pre>
 */
public class RawSQLExpression extends Expression {

    private final String sql;
    private final DataType explicitType;

    /**
     * Creates a raw SQL expression with unknown type (defaults to StringType).
     *
     * @param sql the SQL expression string
     */
    public RawSQLExpression(String sql) {
        this(sql, null);
    }

    /**
     * Creates a raw SQL expression with an explicit type.
     *
     * <p>Use this constructor when the expression's type can be inferred
     * from context (e.g., CASE WHEN branches).
     *
     * @param sql the SQL expression string
     * @param type the explicit data type (null to default to StringType)
     */
    public RawSQLExpression(String sql, DataType type) {
        this.sql = Objects.requireNonNull(sql, "sql must not be null");
        this.explicitType = type;
    }

    /**
     * Returns the SQL expression string.
     *
     * @return the SQL string
     */
    public String sql() {
        return sql;
    }

    @Override
    public DataType dataType() {
        // Use explicit type if provided, otherwise default to StringType
        if (explicitType != null) {
            return explicitType;
        }
        return StringType.get();
    }

    @Override
    public boolean nullable() {
        // We can't determine nullability without parsing
        // Assume nullable to be safe
        return true;
    }

    /**
     * Returns the raw SQL string.
     *
     * @return the SQL string
     */
    public String toSQL() {
        return sql;
    }

    @Override
    public String toString() {
        return "RawSQL(" + sql + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RawSQLExpression)) return false;
        RawSQLExpression that = (RawSQLExpression) obj;
        return Objects.equals(sql, that.sql) &&
               Objects.equals(explicitType, that.explicitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, explicitType);
    }
}