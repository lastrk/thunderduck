package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import com.thunderduck.generator.SQLQuoting;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Expression representing an unresolved column reference.
 *
 * <p>This is used during plan conversion when the data type is not yet known.
 * The column will be resolved during query planning/optimization.
 *
 * <p>The optional {@code planId} field tracks which DataFrame this column
 * reference belongs to, enabling proper qualification in join conditions
 * where both sides have columns with the same name.
 */
public final class UnresolvedColumn implements Expression {

    private final String columnName;
    private final String qualifier; // Optional table/alias qualifier
    private final OptionalLong planId; // Optional plan_id for DataFrame lineage tracking

    /**
     * Creates an unresolved column reference with a qualifier and plan_id.
     *
     * @param columnName the column name
     * @param qualifier the table or alias qualifier (may be null)
     * @param planId the plan_id for DataFrame lineage (may be empty)
     */
    public UnresolvedColumn(String columnName, String qualifier, OptionalLong planId) {
        this.columnName = Objects.requireNonNull(columnName, "columnName must not be null");
        this.qualifier = qualifier;
        this.planId = planId != null ? planId : OptionalLong.empty();
    }

    /**
     * Creates an unresolved column reference with a qualifier (no plan_id).
     *
     * @param columnName the column name
     * @param qualifier the table or alias qualifier (may be null)
     */
    public UnresolvedColumn(String columnName, String qualifier) {
        this(columnName, qualifier, OptionalLong.empty());
    }

    /**
     * Creates a simple unresolved column reference without a qualifier or plan_id.
     *
     * @param columnName the column name
     */
    public UnresolvedColumn(String columnName) {
        this(columnName, null, OptionalLong.empty());
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
     * Returns the plan_id for this column reference.
     *
     * <p>The plan_id identifies which DataFrame (logical plan) this column
     * belongs to, enabling resolution of ambiguous column names in joins.
     *
     * @return the plan_id, or empty if not set
     */
    public OptionalLong planId() {
        return planId;
    }

    /**
     * Returns whether this column reference has a plan_id.
     *
     * @return true if plan_id is present, false otherwise
     */
    public boolean hasPlanId() {
        return planId.isPresent();
    }

    @Override
    public DataType dataType() {
        // Type is unknown until resolution
        return StringType.get(); // Use a default type
    }

    @Override
    public boolean nullable() {
        // Assume nullable until resolved
        return true;
    }

    /**
     * Converts this column reference to its SQL string representation.
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
        if (!(obj instanceof UnresolvedColumn)) return false;
        UnresolvedColumn that = (UnresolvedColumn) obj;
        return Objects.equals(columnName, that.columnName) &&
               Objects.equals(qualifier, that.qualifier) &&
               Objects.equals(planId, that.planId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, qualifier, planId);
    }
}