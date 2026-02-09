package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.generator.SQLQuoting;
import java.util.Objects;

/**
 * Expression representing a reference to a column.
 *
 * <p>Column references appear in:
 * <ul>
 *   <li>SELECT clauses: SELECT name, age</li>
 *   <li>WHERE clauses: WHERE age > 25</li>
 *   <li>JOIN conditions: ON t1.id = t2.id</li>
 *   <li>GROUP BY clauses: GROUP BY category</li>
 *   <li>ORDER BY clauses: ORDER BY name</li>
 * </ul>
 *
 * <p>Column references can be:
 * <ul>
 *   <li>Simple: "name", "age"</li>
 *   <li>Qualified: "users.name", "orders.id"</li>
 *   <li>Nested: "address.city", "user.contact.email"</li>
 * </ul>
 */
public final class ColumnReference implements Expression {

    private final String columnName;
    private final String qualifier; // Optional table/alias qualifier
    private final DataType dataType;
    private final boolean nullable;

    /**
     * Creates a column reference with a qualifier.
     *
     * @param columnName the column name
     * @param qualifier the table or alias qualifier (may be null)
     * @param dataType the data type of the column
     * @param nullable whether the column is nullable
     */
    public ColumnReference(String columnName, String qualifier, DataType dataType, boolean nullable) {
        this.columnName = Objects.requireNonNull(columnName, "columnName must not be null");
        this.qualifier = qualifier;
        this.dataType = Objects.requireNonNull(dataType, "dataType must not be null");
        this.nullable = nullable;
    }

    /**
     * Creates a simple column reference without a qualifier.
     *
     * @param columnName the column name
     * @param dataType the data type of the column
     * @param nullable whether the column is nullable
     */
    public ColumnReference(String columnName, DataType dataType, boolean nullable) {
        this(columnName, null, dataType, nullable);
    }

    /**
     * Creates a nullable column reference without a qualifier.
     *
     * @param columnName the column name
     * @param dataType the data type of the column
     */
    public ColumnReference(String columnName, DataType dataType) {
        this(columnName, null, dataType, true);
    }

    /**
     * Returns the column name.
     *
     * @return the column name
     */
    public String columnName() {
        return columnName;
    }

    /**
     * Returns the qualifier (table or alias).
     *
     * @return the qualifier, or null if not qualified
     */
    public String qualifier() {
        return qualifier;
    }

    /**
     * Returns whether this column reference is qualified.
     *
     * @return true if qualified, false otherwise
     */
    public boolean isQualified() {
        return qualifier != null;
    }

    /**
     * Returns the fully qualified column name.
     *
     * @return the qualified name (e.g., "table.column" or just "column")
     */
    public String qualifiedName() {
        if (qualifier != null) {
            return qualifier + "." + columnName;
        }
        return columnName;
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
     * Converts this column reference to its SQL string representation.
     *
     * <p>Column names are properly quoted to prevent SQL injection and
     * handle reserved words, special characters, etc. Quoting is only
     * applied when necessary to keep SQL readable.
     *
     * @return the SQL string
     */
    public String toSQL() {
        if (qualifier != null) {
            return SQLQuoting.quoteIdentifierIfNeeded(qualifier) + "." +
                   SQLQuoting.quoteIdentifierIfNeeded(columnName);
        }
        return SQLQuoting.quoteIdentifierIfNeeded(columnName);
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ColumnReference)) return false;
        ColumnReference that = (ColumnReference) obj;
        return nullable == that.nullable &&
               Objects.equals(columnName, that.columnName) &&
               Objects.equals(qualifier, that.qualifier) &&
               Objects.equals(dataType, that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, qualifier, dataType, nullable);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a simple column reference.
     *
     * @param columnName the column name
     * @param dataType the data type
     * @return the column reference
     */
    public static ColumnReference of(String columnName, DataType dataType) {
        return new ColumnReference(columnName, dataType);
    }

    /**
     * Creates a qualified column reference.
     *
     * @param qualifier the table or alias name
     * @param columnName the column name
     * @param dataType the data type
     * @return the column reference
     */
    public static ColumnReference qualified(String qualifier, String columnName, DataType dataType) {
        return new ColumnReference(columnName, qualifier, dataType, true);
    }
}
