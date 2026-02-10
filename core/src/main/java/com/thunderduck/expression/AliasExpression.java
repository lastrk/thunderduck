package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import java.util.Objects;

/**
 * Expression that gives an alias (name) to another expression.
 *
 * <p>This is typically used in SELECT clauses to rename columns.
 *
 * <p>Examples:
 * <pre>
 *   price * quantity AS total
 *   SUM(amount) AS total_amount
 *   first_name AS fname
 * </pre>
 */
public final class AliasExpression implements Expression {

    private final Expression expression;
    private final String alias;

    /**
     * Creates an alias expression.
     *
     * @param expression the expression to alias
     * @param alias the alias name
     */
    public AliasExpression(Expression expression, String alias) {
        this.expression = Objects.requireNonNull(expression, "expression must not be null");
        this.alias = Objects.requireNonNull(alias, "alias must not be null");
    }

    /**
     * Returns the underlying expression.
     *
     * @return the expression
     */
    public Expression expression() {
        return expression;
    }

    /**
     * Returns the alias name.
     *
     * @return the alias
     */
    public String alias() {
        return alias;
    }

    @Override
    public DataType dataType() {
        return expression.dataType();
    }

    @Override
    public boolean nullable() {
        return expression.nullable();
    }

    /**
     * Converts this alias expression to its SQL string representation.
     *
     * <p>The alias is quoted if it contains spaces, special characters, or is a
     * SQL reserved word, ensuring compatibility with DuckDB's parser. Simple
     * identifiers are left unquoted for readability.
     *
     * @return the SQL string
     */
    public String toSQL() {
        return String.format("%s AS %s", expression.toSQL(),
            com.thunderduck.generator.SQLQuoting.quoteIdentifierIfNeeded(alias));
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof AliasExpression)) return false;
        AliasExpression that = (AliasExpression) obj;
        return Objects.equals(expression, that.expression) &&
               Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, alias);
    }
}