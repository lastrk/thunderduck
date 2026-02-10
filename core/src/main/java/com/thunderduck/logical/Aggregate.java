package com.thunderduck.logical;

import com.thunderduck.expression.AliasExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing an aggregation (GROUP BY clause with aggregates).
 *
 * <p>This node groups rows from its child and computes aggregate functions.
 * Optionally supports HAVING clause to filter aggregated results.
 *
 * <p>Examples:
 * <pre>
 *   df.groupBy("category").agg(sum("amount"), avg("price"))
 *   df.groupBy("year", "month").count()
 *   df.groupBy("customer_id").agg(count("*")).having(col("count") > 5)
 * </pre>
 *
 * <p>SQL generation:
 * <pre>
 * SELECT groupingExpr1, groupingExpr2, aggFunc1, aggFunc2
 * FROM (child)
 * GROUP BY groupingExpr1, groupingExpr2
 * HAVING condition
 * </pre>
 */
public final class Aggregate extends LogicalPlan {

    private final List<Expression> groupingExpressions;
    private final List<AggregateExpression> aggregateExpressions;
    private final Expression havingCondition;
    private final GroupingSets groupingSets;

    /**
     * Creates an aggregate node with optional HAVING clause and grouping sets.
     *
     * @param child the child node
     * @param groupingExpressions the grouping expressions (empty for global aggregation)
     * @param aggregateExpressions the aggregate expressions (sum, avg, count, etc.)
     * @param havingCondition the HAVING condition (can be null)
     * @param groupingSets the grouping sets specification (ROLLUP, CUBE, etc.), or null for simple GROUP BY
     */
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions,
                    Expression havingCondition,
                    GroupingSets groupingSets) {
        super(child);
        this.groupingExpressions = new ArrayList<>(
            Objects.requireNonNull(groupingExpressions, "groupingExpressions must not be null"));
        this.aggregateExpressions = new ArrayList<>(
            Objects.requireNonNull(aggregateExpressions, "aggregateExpressions must not be null"));
        this.havingCondition = havingCondition;  // Can be null
        this.groupingSets = groupingSets;  // Can be null for simple GROUP BY
    }

    /**
     * Creates an aggregate node with optional HAVING clause (backward compatibility).
     *
     * @param child the child node
     * @param groupingExpressions the grouping expressions (empty for global aggregation)
     * @param aggregateExpressions the aggregate expressions (sum, avg, count, etc.)
     * @param havingCondition the HAVING condition (can be null)
     */
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions,
                    Expression havingCondition) {
        this(child, groupingExpressions, aggregateExpressions, havingCondition, null);
    }

    /**
     * Creates an aggregate node without HAVING clause (backward compatibility).
     *
     * @param child the child node
     * @param groupingExpressions the grouping expressions (empty for global aggregation)
     * @param aggregateExpressions the aggregate expressions (sum, avg, count, etc.)
     */
    public Aggregate(LogicalPlan child,
                    List<Expression> groupingExpressions,
                    List<AggregateExpression> aggregateExpressions) {
        this(child, groupingExpressions, aggregateExpressions, null, null);
    }

    /**
     * Returns the grouping expressions.
     *
     * @return an unmodifiable list of grouping expressions
     */
    public List<Expression> groupingExpressions() {
        return Collections.unmodifiableList(groupingExpressions);
    }

    /**
     * Returns the aggregate expressions.
     *
     * @return an unmodifiable list of aggregate expressions
     */
    public List<AggregateExpression> aggregateExpressions() {
        return Collections.unmodifiableList(aggregateExpressions);
    }

    /**
     * Returns the HAVING condition.
     *
     * @return the HAVING condition expression, or null if no HAVING clause
     */
    public Expression havingCondition() {
        return havingCondition;
    }

    /**
     * Returns the grouping sets specification (ROLLUP, CUBE, GROUPING SETS).
     *
     * @return the grouping sets, or null for simple GROUP BY
     */
    public GroupingSets groupingSets() {
        return groupingSets;
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        if (aggregateExpressions.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate SQL for aggregation with no aggregate expressions");
        }

        StringBuilder sql = new StringBuilder();

        // Resolve child schema once for type-aware SQL generation
        StructType childSchema = null;
        try {
            childSchema = child().schema();
        } catch (Exception e) {
            // Child schema resolution failed — proceed without type-aware corrections
        }

        // SELECT clause with grouping expressions and aggregates
        sql.append("SELECT ");

        List<String> selectExprs = new ArrayList<>();

        // Add grouping columns
        for (Expression expr : groupingExpressions) {
            selectExprs.add(expr.toSQL());
        }

        // Add aggregate expressions
        for (AggregateExpression aggExpr : aggregateExpressions) {
            String aggSQL;
            String originalSQL = null; // Captures pre-transformation SQL for auto-aliasing

            if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && aggExpr.isComposite()) {
                // Capture original SQL before strict mode transformation
                originalSQL = aggExpr.rawExpression().toSQL();
                // Transform aggregate function names in the AST, then render
                Expression transformed = com.thunderduck.generator.SQLGenerator.transformAggregateExpression(aggExpr.rawExpression());
                aggSQL = transformed.toSQL();
            } else if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && !aggExpr.isComposite()) {
                aggSQL = aggExpr.toSQL();
                // Normalize function name: strip _DISTINCT suffix for matching
                String baseFuncName = aggExpr.function().toUpperCase();
                if (baseFuncName.endsWith("_DISTINCT")) {
                    baseFuncName = baseFuncName.substring(0, baseFuncName.length() - "_DISTINCT".length());
                }

                if (baseFuncName.equals("SUM") || baseFuncName.equals("AVG")) {
                    // Capture original SQL before replacing function name
                    originalSQL = aggSQL;
                    if (baseFuncName.equals("SUM")) {
                        aggSQL = aggSQL.replaceFirst("(?i)\\bSUM\\b", "spark_sum");
                    } else {
                        aggSQL = aggSQL.replaceFirst("(?i)\\bAVG\\b", "spark_avg");
                    }
                }
            } else {
                aggSQL = aggExpr.toSQL();
            }

            // Wrap with CAST(... AS DOUBLE) if Spark expects DOUBLE but DuckDB returns DECIMAL
            aggSQL = wrapWithTypeCastIfNeeded(aggSQL, aggExpr, childSchema);

            // Add alias if provided, or auto-alias unaliased count(*) as "count(1)"
            // to match Spark's column naming convention.
            // In strict mode, also alias transformed functions (spark_sum -> SUM) so
            // DuckDB returns the original Spark-expected column name.
            if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
                aggSQL += " AS " + com.thunderduck.generator.SQLQuoting.quoteIdentifier(aggExpr.alias());
            } else if (aggExpr.isUnaliasedCountStar()) {
                aggSQL += " AS \"count(1)\"";
            } else if (originalSQL != null) {
                // Strict mode transformed the function name — alias back to original
                aggSQL += " AS " + com.thunderduck.generator.SQLQuoting.quoteIdentifier(originalSQL);
            }
            selectExprs.add(aggSQL);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause
        sql.append(" FROM (");
        sql.append(generator.generate(child()));
        sql.append(") AS ").append(generator.generateSubqueryAlias());

        // GROUP BY clause
        // Note: GROUP BY cannot have aliases, so we unwrap AliasExpressions
        if (!groupingExpressions.isEmpty()) {
            sql.append(" GROUP BY ");

            // Emit ROLLUP/CUBE/GROUPING SETS wrapper if present
            if (groupingSets != null) {
                sql.append(groupingSets.toSQL());
            } else {
                List<String> groupExprs = new ArrayList<>();
                for (Expression expr : groupingExpressions) {
                    // Unwrap AliasExpression for GROUP BY - aliases not allowed here
                    if (expr instanceof AliasExpression) {
                        groupExprs.add(((AliasExpression) expr).expression().toSQL());
                    } else {
                        groupExprs.add(expr.toSQL());
                    }
                }
                sql.append(String.join(", ", groupExprs));
            }
        }

        // HAVING clause
        if (havingCondition != null) {
            sql.append(" HAVING ");
            sql.append(havingCondition.toSQL());
        }

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        // Get child schema for resolving types (may be null for some plan types)
        StructType childSchema = null;
        try {
            childSchema = child().schema();
        } catch (Exception e) {
            // Child schema resolution failed - continue with null childSchema
            // We can still infer column names from expressions and types from aggregate functions
        }

        // Note: childSchema may be null, but we can still produce a schema
        // using expression names and default/aggregate types.
        // This ensures column names are preserved even when child schema is unavailable.

        List<StructField> fields = new ArrayList<>();

        // Add grouping fields with proper names and types
        // When CUBE/ROLLUP/GROUPING_SETS is used, grouping columns must be nullable
        // because subtotal and grand-total rows produce NULL for excluded dimensions.
        boolean forceNullable = (groupingSets != null);
        for (Expression expr : groupingExpressions) {
            String name = extractExpressionName(expr);
            DataType type = resolveExpressionType(expr, childSchema);
            boolean nullable = forceNullable || resolveExpressionNullable(expr, childSchema);
            fields.add(new StructField(name, type, nullable));
        }

        // Add aggregate fields
        for (AggregateExpression aggExpr : aggregateExpressions) {
            String name;
            if (aggExpr.alias() != null) {
                name = aggExpr.alias();
            } else if (aggExpr.isUnaliasedCountStar()) {
                // Match Spark's column naming: unaliased count(*) is named "count(1)"
                name = "count(1)";
            } else {
                name = aggExpr.toSQL();
            }
            DataType type;
            boolean nullable;

            if (aggExpr.isComposite()) {
                type = TypeInferenceEngine.resolveType(aggExpr.rawExpression(), childSchema);
                nullable = true; // composite aggregates can be null
            } else {
                type = inferAggregateReturnType(aggExpr, childSchema);
                nullable = TypeInferenceEngine.resolveAggregateNullable(
                    aggExpr.function(), aggExpr.argument(), childSchema);
            }
            fields.add(new StructField(name, type, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Wraps an aggregate expression SQL with a CAST if DuckDB's return type would differ from
     * Spark's expected type. Delegates to SQLGenerator's shared implementation.
     */
    private static String wrapWithTypeCastIfNeeded(String aggSQL, AggregateExpression aggExpr,
                                                      StructType childSchema) {
        return com.thunderduck.generator.SQLGenerator.wrapWithTypeCastIfNeeded(aggSQL, aggExpr, childSchema);
    }

    /**
     * Extracts a meaningful name from an expression for schema inference.
     * Follows Spark semantics for groupBy output naming.
     *
     * @param expr the expression
     * @return the extracted name
     */
    private String extractExpressionName(Expression expr) {
        // Priority 1: Explicit alias
        if (expr instanceof AliasExpression) {
            return ((AliasExpression) expr).alias();
        }

        // Priority 2: Column reference name
        if (expr instanceof UnresolvedColumn) {
            return ((UnresolvedColumn) expr).columnName();
        }

        // Priority 3: SQL representation (for complex expressions)
        return expr.toSQL();
    }

    /**
     * Resolves the data type of an expression using the centralized TypeInferenceEngine.
     */
    private DataType resolveExpressionType(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveType(expr, childSchema);
    }

    /**
     * Resolves the nullability of an expression using the centralized TypeInferenceEngine.
     */
    private boolean resolveExpressionNullable(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveNullable(expr, childSchema);
    }

    /**
     * Infers the return type for an aggregate function using the centralized TypeInferenceEngine.
     */
    private DataType inferAggregateReturnType(AggregateExpression aggExpr, StructType childSchema) {
        Expression arg = aggExpr.argument();
        DataType argType = arg != null ? resolveExpressionType(arg, childSchema) : null;
        return TypeInferenceEngine.resolveAggregateReturnType(aggExpr.function(), argType);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Aggregate(groupBy=");
        sb.append(groupingExpressions);
        if (groupingSets != null) {
            sb.append(", groupingSets=").append(groupingSets);
        }
        sb.append(", agg=").append(aggregateExpressions);
        if (havingCondition != null) {
            sb.append(", having=").append(havingCondition);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Represents an aggregate expression (e.g., SUM(amount), AVG(price)).
     *
     * <p>Supports DISTINCT keyword for unique value aggregation:
     * <pre>
     *   COUNT(DISTINCT customer_id)
     *   SUM(DISTINCT price)
     *   AVG(DISTINCT amount)
     * </pre>
     */
    public static final class AggregateExpression implements Expression {
        private final String function;
        private final Expression argument;
        private final String alias;
        private final boolean distinct;
        private final Expression rawExpression;

        /**
         * Creates an aggregate expression with optional DISTINCT modifier.
         *
         * @param function the aggregate function name (COUNT, SUM, AVG, MIN, MAX, etc.)
         * @param argument the expression to aggregate (can be null for COUNT(*))
         * @param alias the result column alias (can be null)
         * @param distinct whether to aggregate only distinct values
         */
        public AggregateExpression(String function, Expression argument, String alias, boolean distinct) {
            this.function = Objects.requireNonNull(function, "function must not be null");
            this.argument = argument;
            this.alias = alias;
            this.distinct = distinct;
            this.rawExpression = null;
        }

        /**
         * Creates an aggregate expression without DISTINCT (backward compatibility).
         *
         * @param function the aggregate function name
         * @param argument the expression to aggregate (can be null for COUNT(*))
         * @param alias the result column alias (can be null)
         */
        public AggregateExpression(String function, Expression argument, String alias) {
            this(function, argument, alias, false);
        }

        /**
         * Creates a composite aggregate expression (e.g., SUM(a) / SUM(b), SUM(a) * 0.5).
         * The raw expression tree already has correct toSQL() rendering.
         *
         * @param rawExpression the composite expression tree
         * @param alias the result column alias (can be null)
         */
        public AggregateExpression(Expression rawExpression, String alias) {
            this.function = null;
            this.argument = null;
            this.alias = alias;
            this.distinct = false;
            this.rawExpression = Objects.requireNonNull(rawExpression, "rawExpression must not be null");
        }

        /**
         * Returns the aggregate function name.
         *
         * @return the function name (e.g., "COUNT", "SUM", "AVG")
         */
        public String function() {
            return function;
        }

        /**
         * Returns the expression being aggregated.
         *
         * @return the argument expression, or null for COUNT(*)
         */
        public Expression argument() {
            return argument;
        }

        /**
         * Returns the result column alias.
         *
         * @return the alias, or null if not specified
         */
        public String alias() {
            return alias;
        }

        /**
         * Returns whether this aggregate uses DISTINCT.
         *
         * @return true if aggregating distinct values only
         */
        public boolean isDistinct() {
            return distinct;
        }

        /**
         * Returns whether this is a composite aggregate expression (e.g., SUM(a) / SUM(b)).
         *
         * @return true if this wraps a raw expression tree
         */
        public boolean isComposite() {
            return rawExpression != null;
        }

        /**
         * Returns whether this is an unaliased COUNT(*) expression.
         *
         * <p>Spark names the output column of an unaliased {@code count(*)} as {@code "count(1)"}.
         * This method detects the pattern so that SQL generation layers can add the appropriate
         * alias ({@code AS "count(1)"}) to match Spark's column naming convention.
         *
         * <p>COUNT(*) is detected when the function is "count" (case-insensitive) and either:
         * <ul>
         *   <li>The argument is null (no-argument count)</li>
         *   <li>The argument is a Literal with value "*"</li>
         * </ul>
         *
         * @return true if this is an unaliased count(*) expression
         */
        public boolean isUnaliasedCountStar() {
            if (alias != null && !alias.isEmpty()) {
                return false;
            }
            if (function == null || !function.equalsIgnoreCase("count")) {
                return false;
            }
            // count(*) when argument is null
            if (argument == null) {
                return true;
            }
            // count(*) when argument is Literal("*")
            if (argument instanceof com.thunderduck.expression.Literal &&
                "*".equals(((com.thunderduck.expression.Literal) argument).value())) {
                return true;
            }
            return false;
        }

        /**
         * Returns the raw expression for composite aggregates.
         *
         * @return the raw expression tree, or null for simple aggregates
         */
        public Expression rawExpression() {
            return rawExpression;
        }

        @Override
        public com.thunderduck.types.DataType dataType() {
            if (rawExpression != null) {
                return rawExpression.dataType();
            }
            // Type inference will depend on the function
            // For now, return the argument type (will be improved later)
            return argument != null ? argument.dataType() : null;
        }

        @Override
        public boolean nullable() {
            if (rawExpression != null) {
                return true; // composite aggregates can be null
            }
            // COUNT always returns non-null (0 for empty groups)
            String funcUpper = function.toUpperCase();
            if (funcUpper.equals("COUNT")) {
                return false;
            }
            // All other aggregates can return null (empty groups, null inputs)
            return true;
        }

        @Override
        public String toSQL() {
            // Composite aggregate: delegate to raw expression tree
            if (rawExpression != null) {
                return rawExpression.toSQL();
            }

            // Build argument SQL first
            String argSQL;
            if (argument != null) {
                // Special case: COUNT(*) where * is passed as a Literal
                if (function.equalsIgnoreCase("COUNT") &&
                    argument instanceof com.thunderduck.expression.Literal &&
                    "*".equals(((com.thunderduck.expression.Literal) argument).value())) {
                    argSQL = "*";
                } else {
                    argSQL = argument.toSQL();
                }
            } else {
                // COUNT(*) case - DISTINCT not allowed with *
                argSQL = distinct ? null : "*";
            }

            // Add DISTINCT prefix if specified
            String finalArgSQL = argSQL;
            if (distinct && argSQL != null) {
                finalArgSQL = "DISTINCT " + argSQL;
            }

            // SUM type casting is handled by Aggregate.toSQL() which has schema access.
            // AggregateExpression.toSQL() does NOT have schema access, so argument.dataType()
            // returns incorrect types (StringType for all column references via UnresolvedColumn).
            // Just return plain SUM(...) here and let Aggregate.toSQL() add the correct cast.
            // Also handle "sum_DISTINCT" variant (ExpressionConverter appends _DISTINCT suffix).
            if (isSumFunction(function) && argument != null) {
                boolean isDistinctSuffix = function.toUpperCase().endsWith("_DISTINCT");
                if (distinct || isDistinctSuffix) {
                    return "SUM(DISTINCT " + argSQL + ")";
                }
                return "SUM(" + finalArgSQL + ")";
            }

            // Translate function name using registry (handles sort_array -> list_sort, etc.)
            try {
                if (finalArgSQL != null) {
                    return com.thunderduck.functions.FunctionRegistry.translate(function, finalArgSQL);
                } else {
                    // No argument case - translate with empty args
                    return com.thunderduck.functions.FunctionRegistry.translate(function);
                }
            } catch (UnsupportedOperationException e) {
                // Fallback to uppercase function name if not in registry
                StringBuilder sql = new StringBuilder();
                sql.append(function.toUpperCase());
                sql.append("(");
                if (finalArgSQL != null) {
                    sql.append(finalArgSQL);
                }
                sql.append(")");
                return sql.toString();
            }
        }

        /**
         * Checks if a function name is a SUM variant (including sum_DISTINCT suffix).
         */
        private static boolean isSumFunction(String funcName) {
            String upper = funcName.toUpperCase();
            return upper.equals("SUM") || upper.equals("SUM_DISTINCT");
        }

        @Override
        public String toString() {
            return toSQL();
        }
    }
}
