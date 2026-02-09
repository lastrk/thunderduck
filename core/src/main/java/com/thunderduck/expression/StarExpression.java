package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StructType;
import java.util.Objects;

/**
 * Expression representing a star (*) in SELECT statements.
 *
 * <p>This can be unqualified (*) or qualified (table.*).
 *
 * <p>Examples:
 * <pre>
 *   SELECT * FROM table
 *   SELECT table.* FROM table
 *   SELECT t1.*, t2.id FROM table1 t1, table2 t2
 * </pre>
 */
public final class StarExpression implements Expression {

    private final String qualifier;

    /**
     * Creates an unqualified star expression (*).
     */
    public StarExpression() {
        this.qualifier = null;
    }

    /**
     * Creates a qualified star expression (table.*).
     *
     * @param qualifier the table name or alias
     */
    public StarExpression(String qualifier) {
        this.qualifier = qualifier;
    }

    /**
     * Returns the qualifier (table name/alias) if any.
     *
     * @return the qualifier or null for unqualified star
     */
    public String qualifier() {
        return qualifier;
    }

    /**
     * Returns true if this is a qualified star (table.*).
     *
     * @return true if qualified
     */
    public boolean isQualified() {
        return qualifier != null;
    }

    @Override
    public DataType dataType() {
        // Star expression represents multiple columns, not a single type
        // This should be expanded during query planning
        return StructType.EMPTY;
    }

    @Override
    public boolean nullable() {
        // Star can include nullable columns
        return true;
    }

    /**
     * Converts this star expression to its SQL string representation.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (qualifier != null) {
            return qualifier + ".*";
        } else {
            return "*";
        }
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof StarExpression)) return false;
        StarExpression that = (StarExpression) obj;
        return Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier);
    }
}