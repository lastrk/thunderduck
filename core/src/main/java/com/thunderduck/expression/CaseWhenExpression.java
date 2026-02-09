package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Expression representing a CASE WHEN conditional expression.
 *
 * <p>CASE WHEN expressions evaluate conditions in order and return the
 * corresponding THEN value for the first matching condition. If no condition
 * matches, the ELSE value is returned (or NULL if no ELSE is specified).
 *
 * <p>SQL form:
 * <pre>
 *   CASE
 *     WHEN condition1 THEN result1
 *     WHEN condition2 THEN result2
 *     ...
 *     ELSE default_result
 *   END
 * </pre>
 *
 * <p>PySpark form:
 * <pre>
 *   F.when(condition1, result1)
 *    .when(condition2, result2)
 *    .otherwise(default_result)
 * </pre>
 *
 * <p>This class preserves the structure of the CASE WHEN expression to allow
 * schema-aware type resolution in TypeInferenceEngine. By retaining all
 * condition and result expressions, proper type inference can be performed
 * based on the types of all branches.
 */
public final class CaseWhenExpression implements Expression {

    private final List<Expression> conditions;
    private final List<Expression> thenBranches;
    private final Expression elseBranch;

    /**
     * Creates a CASE WHEN expression.
     *
     * @param conditions the WHEN conditions (must match thenBranches size)
     * @param thenBranches the THEN result expressions
     * @param elseBranch the ELSE result expression (may be null)
     * @throws IllegalArgumentException if conditions and thenBranches have different sizes
     */
    public CaseWhenExpression(List<Expression> conditions, List<Expression> thenBranches, Expression elseBranch) {
        Objects.requireNonNull(conditions, "conditions must not be null");
        Objects.requireNonNull(thenBranches, "thenBranches must not be null");

        if (conditions.size() != thenBranches.size()) {
            throw new IllegalArgumentException(
                "conditions and thenBranches must have the same size: " +
                conditions.size() + " vs " + thenBranches.size());
        }

        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("CASE WHEN requires at least one condition");
        }

        this.conditions = new ArrayList<>(conditions);
        this.thenBranches = new ArrayList<>(thenBranches);
        this.elseBranch = elseBranch;
    }

    /**
     * Returns the WHEN conditions.
     *
     * @return an unmodifiable list of condition expressions
     */
    public List<Expression> conditions() {
        return Collections.unmodifiableList(conditions);
    }

    /**
     * Returns the THEN branch expressions.
     *
     * @return an unmodifiable list of result expressions
     */
    public List<Expression> thenBranches() {
        return Collections.unmodifiableList(thenBranches);
    }

    /**
     * Returns the ELSE branch expression.
     *
     * @return the ELSE expression, or null if not specified
     */
    public Expression elseBranch() {
        return elseBranch;
    }

    /**
     * Returns the data type of this CASE WHEN expression.
     *
     * <p>This method provides a best-effort type inference without schema context.
     * It checks THEN branches and ELSE branch for a non-StringType type.
     * For proper schema-aware type resolution, use TypeInferenceEngine.resolveType().
     *
     * @return the inferred data type, or StringType as fallback
     */
    @Override
    public DataType dataType() {
        // Try to find a non-StringType from branches
        // Real resolution with schema happens in TypeInferenceEngine

        // Check THEN branches
        for (Expression branch : thenBranches) {
            DataType type = branch.dataType();
            if (type != null && !(type instanceof StringType)) {
                return type;
            }
        }

        // Check ELSE branch
        if (elseBranch != null) {
            DataType type = elseBranch.dataType();
            if (type != null && !(type instanceof StringType)) {
                return type;
            }
        }

        // Fallback to StringType (will be resolved properly with schema)
        return StringType.get();
    }

    /**
     * Returns whether this expression can produce null values.
     *
     * <p>CASE WHEN is nullable if:
     * <ul>
     *   <li>No ELSE branch is specified (implicit NULL)</li>
     *   <li>Any THEN or ELSE branch is nullable</li>
     * </ul>
     *
     * @return true if nullable
     */
    @Override
    public boolean nullable() {
        // Nullable if no ELSE (implicit NULL when no condition matches)
        if (elseBranch == null) {
            return true;
        }

        // Nullable if ELSE is nullable
        if (elseBranch.nullable()) {
            return true;
        }

        // Nullable if any THEN branch is nullable
        for (Expression branch : thenBranches) {
            if (branch.nullable()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Generates the SQL representation of this CASE WHEN expression.
     *
     * @return SQL string in the form "CASE WHEN c1 THEN r1 ... ELSE e END"
     */
    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder("CASE ");

        for (int i = 0; i < conditions.size(); i++) {
            sql.append("WHEN ").append(conditions.get(i).toSQL());
            sql.append(" THEN ").append(thenBranches.get(i).toSQL()).append(" ");
        }

        if (elseBranch != null) {
            sql.append("ELSE ").append(elseBranch.toSQL()).append(" ");
        }

        sql.append("END");
        return sql.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof CaseWhenExpression)) return false;
        CaseWhenExpression that = (CaseWhenExpression) obj;
        return Objects.equals(conditions, that.conditions) &&
               Objects.equals(thenBranches, that.thenBranches) &&
               Objects.equals(elseBranch, that.elseBranch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conditions, thenBranches, elseBranch);
    }

    @Override
    public String toString() {
        return "CaseWhen(" + conditions.size() + " branches" +
               (elseBranch != null ? ", with ELSE" : "") + ")";
    }
}
