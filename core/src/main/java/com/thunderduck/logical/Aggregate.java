package com.thunderduck.logical;

import com.thunderduck.expression.AliasExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DecimalType;
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
public class Aggregate extends LogicalPlan {

    private final List<Expression> groupingExpressions;
    private final List<AggregateExpression> aggregateExpressions;
    private final Expression havingCondition;

    /**
     * Creates an aggregate node with optional HAVING clause.
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
        super(child);
        this.groupingExpressions = new ArrayList<>(
            Objects.requireNonNull(groupingExpressions, "groupingExpressions must not be null"));
        this.aggregateExpressions = new ArrayList<>(
            Objects.requireNonNull(aggregateExpressions, "aggregateExpressions must not be null"));
        this.havingCondition = havingCondition;  // Can be null
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
        this(child, groupingExpressions, aggregateExpressions, null);
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

        // SELECT clause with grouping expressions and aggregates
        sql.append("SELECT ");

        List<String> selectExprs = new ArrayList<>();

        // Add grouping columns
        for (Expression expr : groupingExpressions) {
            selectExprs.add(expr.toSQL());
        }

        // Add aggregate expressions
        // Get child schema to determine if AVG needs CAST for decimal columns
        StructType childSchema = null;
        try {
            childSchema = child().schema();
        } catch (Exception e) {
            // Child schema unavailable - proceed without CAST
        }

        for (AggregateExpression aggExpr : aggregateExpressions) {
            String aggSQL = aggExpr.toSQL();

            // For AVG of decimal columns, wrap with CAST to preserve decimal type
            // (DuckDB returns DOUBLE for AVG, but Spark preserves DECIMAL)
            // Spark AVG of DECIMAL(p,s) returns DECIMAL(p+4, s+4)
            if (childSchema != null &&
                aggExpr.function().equalsIgnoreCase("AVG") &&
                aggExpr.argument() != null) {

                DataType argType = resolveExpressionType(aggExpr.argument(), childSchema);
                if (argType instanceof DecimalType) {
                    DecimalType decType = (DecimalType) argType;
                    // Spark: AVG(DECIMAL(p,s)) -> DECIMAL(p+4, s+4), capped at max precision 38
                    int newPrecision = Math.min(decType.precision() + 4, 38);
                    int newScale = Math.min(decType.scale() + 4, newPrecision);
                    aggSQL = String.format("CAST(%s AS DECIMAL(%d,%d))",
                        aggSQL, newPrecision, newScale);
                }
            }

            // For SUM of decimal columns, wrap with CAST to match Spark precision rules
            // Spark: SUM(DECIMAL(p,s)) -> DECIMAL(p+10, s), capped at max precision 38
            if (childSchema != null &&
                aggExpr.function().equalsIgnoreCase("SUM") &&
                aggExpr.argument() != null) {

                DataType argType = resolveExpressionType(aggExpr.argument(), childSchema);
                if (argType instanceof DecimalType) {
                    DecimalType decType = (DecimalType) argType;
                    int newPrecision = Math.min(decType.precision() + 10, 38);
                    aggSQL = String.format("CAST(%s AS DECIMAL(%d,%d))",
                        aggSQL, newPrecision, decType.scale());
                }
            }

            // Add alias if provided
            if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
                aggSQL += " AS " + com.thunderduck.generator.SQLQuoting.quoteIdentifier(aggExpr.alias());
            }
            selectExprs.add(aggSQL);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause
        sql.append(" FROM (");
        sql.append(generator.generate(child()));
        sql.append(") AS ").append(generator.generateSubqueryAlias());

        // GROUP BY clause
        if (!groupingExpressions.isEmpty()) {
            sql.append(" GROUP BY ");
            List<String> groupExprs = new ArrayList<>();
            for (Expression expr : groupingExpressions) {
                groupExprs.add(expr.toSQL());
            }
            sql.append(String.join(", ", groupExprs));
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
        for (Expression expr : groupingExpressions) {
            String name = extractExpressionName(expr);
            DataType type = resolveExpressionType(expr, childSchema);
            boolean nullable = resolveExpressionNullable(expr, childSchema);
            fields.add(new StructField(name, type, nullable));
        }

        // Add aggregate fields
        for (AggregateExpression aggExpr : aggregateExpressions) {
            String name = aggExpr.alias() != null ? aggExpr.alias() : aggExpr.toSQL();
            DataType type = inferAggregateReturnType(aggExpr, childSchema);
            fields.add(new StructField(name, type, aggExpr.nullable()));
        }

        return new StructType(fields);
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
        if (havingCondition != null) {
            return String.format("Aggregate(groupBy=%s, agg=%s, having=%s)",
                               groupingExpressions, aggregateExpressions, havingCondition);
        }
        return String.format("Aggregate(groupBy=%s, agg=%s)",
                           groupingExpressions, aggregateExpressions);
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
    public static class AggregateExpression extends Expression {
        private final String function;
        private final Expression argument;
        private final String alias;
        private final boolean distinct;

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

        @Override
        public com.thunderduck.types.DataType dataType() {
            // Type inference will depend on the function
            // For now, return the argument type (will be improved later)
            return argument != null ? argument.dataType() : null;
        }

        @Override
        public boolean nullable() {
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
            StringBuilder sql = new StringBuilder();
            sql.append(function.toUpperCase());
            sql.append("(");

            // Add DISTINCT keyword if specified
            if (distinct) {
                sql.append("DISTINCT ");
            }

            // Add argument or * for COUNT(*)
            if (argument != null) {
                // Special case: COUNT(*) where * is passed as a Literal
                if (function.equalsIgnoreCase("COUNT") &&
                    argument instanceof com.thunderduck.expression.Literal &&
                    "*".equals(((com.thunderduck.expression.Literal) argument).value())) {
                    sql.append("*");
                } else {
                    sql.append(argument.toSQL());
                }
            } else {
                // COUNT(*) case - DISTINCT not allowed with *
                if (!distinct) {
                    sql.append("*");
                }
                // If distinct is true and argument is null, we don't add *
                // This should be validated elsewhere as an error case
            }

            sql.append(")");
            return sql.toString();
        }

        @Override
        public String toString() {
            return toSQL();
        }
    }
}
