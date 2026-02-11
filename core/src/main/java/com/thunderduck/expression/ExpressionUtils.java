package com.thunderduck.expression;

import java.util.Set;

/**
 * Utility methods for classifying and inspecting expressions.
 */
public final class ExpressionUtils {

    private ExpressionUtils() {}

    /**
     * Known aggregate function names (lowercase). Made package-accessible
     * for use by RelationConverter to distinguish scalar-wrapping-aggregate
     * expressions (e.g., size(collect_list(x))) from pure aggregates.
     */
    public static final Set<String> AGGREGATE_FUNCTIONS = Set.of(
        "count", "sum", "avg", "min", "max",
        "stddev", "stddev_samp", "stddev_pop",
        "variance", "var_samp", "var_pop",
        "first", "first_value", "last", "last_value", "any_value",
        "collect_list", "collect_set",
        "corr", "covar_samp", "covar_pop",
        "approx_count_distinct", "approx_percentile",
        "percentile", "percentile_approx",
        "count_if", "count_min_sketch",
        "bit_and", "bit_or", "bit_xor",
        "bool_and", "bool_or", "every", "some",
        "skewness", "kurtosis",
        "regr_count", "regr_r2", "regr_avgx", "regr_avgy",
        "regr_sxx", "regr_syy", "regr_sxy",
        "grouping", "grouping_id"
    );

    /**
     * Returns true if the expression tree contains an aggregate function call
     * or a window function. Used by the parser to split SELECT items into
     * grouping expressions vs. aggregate expressions.
     *
     * @param expr the expression to check
     * @return true if the expression contains an aggregate function
     */
    public static boolean containsAggregateFunction(Expression expr) {
        if (expr instanceof FunctionCall func) {
            if (AGGREGATE_FUNCTIONS.contains(func.functionName().toLowerCase())) {
                return true;
            }
            // Check arguments recursively (e.g., coalesce(sum(x), 0))
            for (Expression arg : func.arguments()) {
                if (containsAggregateFunction(arg)) return true;
            }
            return false;
        }
        if (expr instanceof WindowFunction) {
            return true;
        }
        if (expr instanceof BinaryExpression bin) {
            return containsAggregateFunction(bin.left()) ||
                   containsAggregateFunction(bin.right());
        }
        if (expr instanceof UnaryExpression unary) {
            return containsAggregateFunction(unary.operand());
        }
        if (expr instanceof CastExpression cast) {
            return containsAggregateFunction(cast.expression());
        }
        if (expr instanceof AliasExpression alias) {
            return containsAggregateFunction(alias.expression());
        }
        if (expr instanceof CaseWhenExpression cw) {
            for (Expression cond : cw.conditions()) {
                if (containsAggregateFunction(cond)) return true;
            }
            for (Expression then : cw.thenBranches()) {
                if (containsAggregateFunction(then)) return true;
            }
            if (cw.elseBranch() != null && containsAggregateFunction(cw.elseBranch())) {
                return true;
            }
            return false;
        }
        // Literals, columns, star, raw SQL, etc. — not aggregates
        return false;
    }
}
