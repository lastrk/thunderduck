package com.thunderduck.generator;

import com.thunderduck.exception.SQLGenerationException;
import com.thunderduck.logical.*;
import com.thunderduck.expression.*;
import com.thunderduck.runtime.SparkCompatMode;
import com.thunderduck.types.ArrayType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;
import com.thunderduck.types.TypeMapper;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thunderduck.generator.SQLQuoting.*;

/**
 * SQL generator that converts Spark logical plans to DuckDB SQL.
 *
 * <p>Implements visitor pattern for traversing logical plan trees and
 * generating optimized DuckDB SQL with proper quoting, escaping, and
 * operator precedence handling.
 *
 * <p>This class provides a clean separation between the logical plan
 * representation and SQL generation, allowing for future optimizations
 * and SQL dialect variations.
 *
 * <p>Example usage:
 * <pre>
 *   LogicalPlan plan = ...;
 *   SQLGenerator generator = new SQLGenerator();
 *   String sql = generator.generate(plan);
 * </pre>
 *
 * @see LogicalPlan
 */
public class SQLGenerator implements com.thunderduck.logical.SQLGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SQLGenerator.class);

    private final StringBuilder sql;
    private int aliasCounter;
    private int subqueryDepth;

    /**
     * Creates a new SQL generator.
     */
    public SQLGenerator() {
        this.sql = new StringBuilder();
        this.aliasCounter = 0;
        this.subqueryDepth = 0;
    }

    /**
     * Generates SQL for a logical plan node.
     *
     * <p>This is the main entry point for SQL generation. The method
     * resets the internal state and traverses the plan tree to generate
     * complete SQL.
     *
     * <p>If SQL generation fails, the internal state is rolled back to
     * ensure consistency for subsequent calls.
     *
     * @param plan the logical plan to translate
     * @return the generated DuckDB SQL string
     * @throws NullPointerException if plan is null
     * @throws SQLGenerationException if SQL generation fails
     */
    public String generate(LogicalPlan plan) {
        Objects.requireNonNull(plan, "plan must not be null");

        // Check if this is a recursive call (buffer not empty or subquery depth > 0)
        boolean isRecursive = sql.length() > 0 || subqueryDepth > 0;

        // Save state for recursive calls or rollback
        int savedLength = sql.length();
        
        if (!isRecursive) {
            // Top-level call: reset all state
            sql.setLength(0);

            aliasCounter = 0;
            subqueryDepth = 0;
        }

        try {
            // Generate SQL
            visit(plan);
            String result = sql.toString().substring(savedLength);  // Get only the new part

            // Clear the buffer and reset counters after generating SQL for non-recursive calls
            // This ensures the generator is stateless between top-level calls
            if (!isRecursive) {
                sql.setLength(0);
                aliasCounter = 0;
                subqueryDepth = 0;
            }

            return result;

        } catch (UnsupportedOperationException e) {
            // Rollback state
            sql.setLength(savedLength);


            // Re-throw UnsupportedOperationException without wrapping
            // so callers can catch it directly if needed
            throw e;

        } catch (IllegalArgumentException e) {
            // Rollback state
            sql.setLength(savedLength);


            // Re-throw IllegalArgumentException without wrapping
            // This includes validation errors from quoting/escaping
            throw e;

        } catch (Exception e) {
            // Rollback state
            sql.setLength(savedLength);


            // Wrap in SQLGenerationException with context
            throw new SQLGenerationException(
                "Unexpected error during SQL generation", e, plan);
        }
    }

    /**
     * Visitor dispatch method for type-safe plan traversal.
     *
     * @param plan the plan node to visit
     */
    private void visit(LogicalPlan plan) {
        Objects.requireNonNull(plan, "plan must not be null");

        switch (plan) {
            case Project p          -> visitProject(p);
            case Filter f           -> visitFilter(f);
            case TableScan t        -> visitTableScan(t);
            case Sort s             -> visitSort(s);
            case Limit l            -> visitLimit(l);
            case Aggregate a        -> visitAggregate(a);
            case Join j             -> visitJoin(j);
            case Union u            -> visitUnion(u);
            case Intersect i        -> visitIntersect(i);
            case Except e           -> visitExcept(e);
            case InMemoryRelation m -> visitInMemoryRelation(m);
            case LocalRelation lr   -> visitLocalRelation(lr);
            case LocalDataRelation d -> visitLocalDataRelation(d);
            case SQLRelation sq     -> visitSQLRelation(sq);
            case AliasedRelation ar -> visitAliasedRelation(ar);
            case Distinct di        -> visitDistinct(di);
            case RangeRelation r    -> visitRangeRelation(r);
            case Tail ta            -> visitTail(ta);
            case Sample sa          -> visitSample(sa);
            case WithColumns w      -> visitWithColumns(w);
            case ToDF td            -> visitToDF(td);
            case WithCTE w          -> visitWithCTE(w);
            case RawDDLStatement ddl -> sql.append(ddl.sql());
            case SingleRowRelation sr -> {} // No FROM clause needed
        }
    }

    /**
     * Visits an SQLRelation node.
     */
    private void visitSQLRelation(SQLRelation plan) {
        sql.append(plan.toSQL(this));
    }

    /**
     * Visits an AliasedRelation node.
     *
     * <p>Generates SQL with the user-provided alias. When used in joins,
     * the alias allows join conditions to reference it directly.
     *
     * <p>Note: We must NOT call plan.toSQL(this) because that would call
     * generator.generate(child) which resets the StringBuilder, losing
     * all previously appended content.
     */
    private void visitAliasedRelation(AliasedRelation plan) {
        String childSource = getDirectlyAliasableSource(plan.child());
        if (childSource != null) {
            // Direct table reference with user alias - no subquery needed
            sql.append("SELECT * FROM ");
            sql.append(childSource);
            sql.append(" AS ");
            sql.append(SQLQuoting.quoteIdentifier(plan.alias()));
        } else {
            // Complex child needs wrapping
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            visit(plan.child());
            subqueryDepth--;
            sql.append(") AS ");
            sql.append(SQLQuoting.quoteIdentifier(plan.alias()));
        }
        // Append column aliases if present (e.g., AS alias (col1, col2))
        if (!plan.columnAliases().isEmpty()) {
            sql.append(" (");
            for (int i = 0; i < plan.columnAliases().size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(SQLQuoting.quoteIdentifier(plan.columnAliases().get(i)));
            }
            sql.append(")");
        }
    }

    /**
     * Visits a Project node (SELECT clause).
     */
    private void visitProject(Project plan) {
        LogicalPlan child = plan.child();

        // Special case: Project on Filter(s) on Join
        // Generate flat SQL to preserve alias visibility and properly qualify columns
        // This is needed for ALL joins (not just those with explicit user aliases) because
        // the column references in the Project have plan_ids that map to the join's aliases
        // Walk through stacked Filters to find the Join underneath (e.g., Filter→Filter→Join)
        // Skip flat generation for SEMI/ANTI joins (they need EXISTS/NOT EXISTS via visitJoin)
        if (child instanceof Filter) {
            List<Filter> filters = new java.util.ArrayList<>();
            LogicalPlan current = child;
            while (current instanceof Filter f) {
                filters.add(f);
                current = f.child();
            }
            if (current instanceof Join currentJoin && !containsSemiOrAntiJoin(currentJoin)) {
                generateProjectOnFilterJoin(plan, filters, currentJoin);
                return;
            }
            // Special case: Project on Filter(s) on AliasedRelation
            // Inline the FROM + alias + WHERE to preserve alias visibility in SELECT.
            // Without this, the alias (e.g., "web" or "j") is hidden inside a subquery.
            if (current instanceof AliasedRelation aliased) {
                generateProjectOnFilterAliased(plan, filters, aliased);
                return;
            }
        }

        // Special case: Project directly on Join
        // Generate flat SQL to preserve alias visibility and properly qualify columns
        // This handles cases like: df1.join(df2, cond, "right").select(df1["col"], df2["col"])
        // Without flat SQL, duplicate column names become ambiguous in the wrapped subquery
        // Skip flat generation for SEMI/ANTI joins (they need EXISTS/NOT EXISTS via visitJoin)
        if (child instanceof Join childJoin && !containsSemiOrAntiJoin(childJoin)) {
            generateProjectOnJoin(plan, childJoin);
            return;
        }

        sql.append("SELECT ");

        List<Expression> projections = plan.projections();
        List<String> aliases = plan.aliases();

        // Get child schema for resolving polymorphic functions
        StructType childSchema = null;
        try {
            childSchema = plan.child().schema();
        } catch (Exception e) {
            LOG.trace("Schema resolution failed for Project child; proceeding without it", e);
        }

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            // Resolve struct types for dropFields operations (needs schema context)
            if (childSchema != null) {
                resolveDropFieldsStructType(expr, childSchema);
            }
            // Resolve polymorphic functions (e.g., reverse -> list_reverse for arrays)
            expr = resolvePolymorphicFunctions(expr, childSchema);
            // In strict mode, transform division and aggregates for correct type dispatch
            Expression transformedExpr = expr;
            if (SparkCompatMode.isStrictMode() && childSchema != null) {
                transformedExpr = transformExpressionForStrictMode(expr, childSchema);
            }
            String exprSQL = transformedExpr.toSQL();
            sql.append(exprSQL);

            // Add alias if provided and expression doesn't already have one
            // (AliasExpression.toSQL() already includes AS alias)
            // Note: use original expr for auto-alias to preserve Spark column naming
            String alias = aliases.get(i);
            maybeAppendAutoAlias(expr, exprSQL, alias);
        }

        // Skip FROM clause for SingleRowRelation (e.g., SELECT ARRAY(1,2,3))
        if (!(plan.child() instanceof SingleRowRelation)) {
            // Add FROM clause from child - check for direct source optimization
            String childSource = getDirectlyAliasableSource(plan.child());
            if (childSource != null) {
                sql.append(" FROM ").append(childSource);
                if (subqueryDepth > 0) {
                    sql.append(" AS ").append(generateSubqueryAlias());
                }
            } else {
                sql.append(" FROM (");
                subqueryDepth++;
                visit(plan.child());
                subqueryDepth--;
                sql.append(")");
                // Add subquery alias if needed
                if (subqueryDepth > 0) {
                    sql.append(" AS ").append(generateSubqueryAlias());
                }
            }
        }
    }

    /**
     * Generates flat SQL for Project directly on Join.
     * This preserves alias visibility by qualifying column references, needed for
     * correct column resolution when both join sides have columns with the same name.
     */
    private void generateProjectOnJoin(Project project, Join join) {
        // Two-pass approach: generate FROM first to build actual alias mapping,
        // then insert SELECT before it. This avoids alias prediction errors
        // when complex join sources increment the alias counter during visit().
        Map<Long, String> planIdToAlias = new HashMap<>();
        int insertPos = sql.length();
        sql.append(" FROM ");
        generateFlatJoinChainWithMapping(join, planIdToAlias);
        String fromClause = sql.substring(insertPos);
        sql.setLength(insertPos);

        // Resolve child schema for strict mode type-aware rendering
        StructType childSchema = null;
        if (SparkCompatMode.isStrictMode()) {
            try { childSchema = project.child().schema(); }
            catch (Exception e) { LOG.trace("Schema resolution failed for Project child", e); }
        }

        // Now generate SELECT with the correct mapping
        sql.append("SELECT ");

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            // Use qualifyCondition to properly resolve column references
            // Pass schema for strict mode type-aware division/aggregate dispatch
            String qualifiedExpr = qualifyCondition(expr, planIdToAlias, childSchema);
            sql.append(qualifiedExpr);

            String alias = aliases.get(i);
            maybeAppendAutoAlias(expr, qualifiedExpr, alias);
        }

        sql.append(fromClause);
    }

    /**
     * Generates flat SQL for Project on Filter(s) on Join.
     * This preserves alias visibility by not wrapping intermediate results in subqueries,
     * needed for correct column resolution when both join sides have columns with the same name.
     * Handles stacked filters (e.g., Filter→Filter→Join) by combining all conditions with AND.
     */
    private void generateProjectOnFilterJoin(Project project, List<Filter> filters, Join join) {
        // Two-pass approach: generate FROM first to build actual alias mapping,
        // then insert SELECT before it.
        Map<Long, String> planIdToAlias = new HashMap<>();
        int insertPos = sql.length();
        sql.append(" FROM ");
        generateFlatJoinChainWithMapping(join, planIdToAlias);
        String fromClause = sql.substring(insertPos);
        sql.setLength(insertPos);

        // Resolve child schema for strict mode type-aware rendering
        StructType childSchema = null;
        if (SparkCompatMode.isStrictMode()) {
            try {
                childSchema = project.child().schema();
                if (childSchema != null) {
                    LOG.debug("Strict mode: resolved child schema for Project-on-Filter-Join: {}", childSchema);
                }
            }
            catch (Exception e) { LOG.debug("Schema resolution failed for Project child: {}", e.getMessage()); }
        }

        // Now generate SELECT with the correct mapping
        sql.append("SELECT ");

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            // Use qualifyCondition to properly resolve column references
            // Pass schema for strict mode type-aware division/aggregate dispatch
            String qualifiedExpr = qualifyCondition(expr, planIdToAlias, childSchema);
            sql.append(qualifiedExpr);

            String alias = aliases.get(i);
            maybeAppendAutoAlias(expr, qualifiedExpr, alias);
        }

        sql.append(fromClause);

        // Add WHERE clause combining all filter conditions with AND
        sql.append(" WHERE ");
        for (int i = 0; i < filters.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            String qualifiedCond = qualifyCondition(filters.get(i).condition(), planIdToAlias);
            // Wrap each filter condition in parentheses to preserve precedence
            sql.append("(").append(qualifiedCond).append(")");
        }
    }

    /**
     * Generates a Project on Filter(s) on AliasedRelation with inlined FROM + alias.
     * Preserves the alias (e.g., "web", "j") so it can be referenced in SELECT.
     */
    private void generateProjectOnFilterAliased(Project project, List<Filter> filters, AliasedRelation aliased) {
        sql.append("SELECT ");

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        StructType childSchema = null;
        try { childSchema = project.child().schema(); }
        catch (Exception e) { LOG.trace("Schema resolution failed for Project child; proceeding without it", e); }

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            expr = resolvePolymorphicFunctions(expr, childSchema);
            // In strict mode, transform division and aggregates for correct type dispatch
            Expression transformedExpr = expr;
            if (SparkCompatMode.isStrictMode() && childSchema != null) {
                transformedExpr = transformExpressionForStrictMode(expr, childSchema);
            }
            String exprSQL = transformedExpr.toSQL();
            sql.append(exprSQL);

            String alias = aliases.get(i);
            maybeAppendAutoAlias(expr, exprSQL, alias);
        }

        // Generate FROM with alias preserved
        String childSource = getDirectlyAliasableSource(aliased.child());
        if (childSource != null) {
            sql.append(" FROM ").append(childSource)
               .append(" AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
        } else {
            sql.append(" FROM (");
            subqueryDepth++;
            visit(aliased.child());
            subqueryDepth--;
            sql.append(") AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
        }

        // Add WHERE clause combining all filter conditions with AND
        sql.append(" WHERE ");
        for (int i = 0; i < filters.size(); i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append("(").append(filters.get(i).condition().toSQL()).append(")");
        }
    }

    /**
     * Generates a flat join chain without nested subqueries.
     * This preserves table aliases so they can be referenced in WHERE clauses.
     */
    private void generateFlatJoinChain(Join join) {
        generateFlatJoinChainWithMapping(join, null);
    }

    /**
     * Generates a flat join chain and optionally populates a planIdToAlias mapping.
     * This allows callers to get the mapping for qualifying expressions elsewhere.
     *
     * @param join the join to generate
     * @param planIdToAliasOut optional map to populate with plan_id → alias mappings (may be null)
     */
    private void generateFlatJoinChainWithMapping(Join join, Map<Long, String> planIdToAliasOut) {
        // Collect all joins in the chain
        List<JoinPart> joinParts = new java.util.ArrayList<>();
        LogicalPlan leftmost = collectJoinParts(join, joinParts);

        // Build plan_id → alias mapping incrementally as sources are generated.
        // We CANNOT predict aliases upfront because complex join sources (subqueries)
        // may increment the alias counter internally during visit(), causing predicted
        // aliases to diverge from actual generated aliases.
        Map<Long, String> planIdToAlias = new HashMap<>();

        // Generate leftmost table/subquery and track its actual alias
        String leftAlias = generateJoinSourceAndGetAlias(leftmost);
        collectPlanIds(leftmost, leftAlias, planIdToAlias);

        // Generate each join in sequence, tracking actual aliases
        for (JoinPart part : joinParts) {
            sql.append(" ");
            sql.append(getJoinKeyword(part.joinType));
            sql.append(" ");
            String rightAlias = generateJoinSourceAndGetAlias(part.right);
            collectPlanIds(part.right, rightAlias, planIdToAlias);
            if (part.condition != null) {
                sql.append(" ON ");
                // Use qualified condition to properly resolve column references
                sql.append(qualifyCondition(part.condition, planIdToAlias));
            }
        }

        // If caller wants the mapping, populate it
        if (planIdToAliasOut != null) {
            planIdToAliasOut.putAll(planIdToAlias);
        }
    }

    /**
     * Collects join parts from a join chain, returning the leftmost non-join plan.
     * Stops recursion at SEMI/ANTI joins since they cannot be flattened into
     * a flat join chain (they require EXISTS/NOT EXISTS subquery syntax).
     */
    private LogicalPlan collectJoinParts(Join join, List<JoinPart> parts) {
        LogicalPlan left = join.left();
        if (left instanceof Join leftJoin
                && leftJoin.joinType() != Join.JoinType.LEFT_SEMI
                && leftJoin.joinType() != Join.JoinType.LEFT_ANTI) {
            // Recursively collect from nested join (only for flattenable join types)
            LogicalPlan leftmost = collectJoinParts(leftJoin, parts);
            // Add this join's right side
            parts.add(new JoinPart(join.right(), join.joinType(), join.condition()));
            return leftmost;
        } else {
            // Left is not a join (or is a SEMI/ANTI join that can't be flattened),
            // this is the leftmost table/subquery
            // Add this join's right side as the first join
            parts.add(new JoinPart(join.right(), join.joinType(), join.condition()));
            return left;
        }
    }

    /**
     * Generates SQL for a join source (table, aliased relation, or subquery).
     */
    private void generateJoinSource(LogicalPlan plan) {
        generateJoinSourceAndGetAlias(plan);
    }

    /**
     * Generates SQL for a join source and returns the alias that was assigned.
     * This is needed because complex sources (subqueries) may increment the alias counter
     * internally, so the only reliable way to know the actual alias is to capture it
     * at the time it's generated.
     *
     * @param plan the source plan to generate
     * @return the alias assigned to this source (quoted identifier or subquery_N)
     */
    private String generateJoinSourceAndGetAlias(LogicalPlan plan) {
        if (plan instanceof AliasedRelation aliased) {
            String childSource = getDirectlyAliasableSource(aliased.child());
            if (childSource != null) {
                sql.append(childSource);
            } else {
                sql.append("(");
                visit(aliased.child());
                sql.append(")");
            }
            String alias = SQLQuoting.quoteIdentifier(aliased.alias());
            sql.append(" AS ");
            sql.append(alias);
            return alias;
        } else {
            String source = getDirectlyAliasableSource(plan);
            if (source != null) {
                sql.append(source);
                sql.append(" AS ");
                String alias = generateSubqueryAlias();
                sql.append(alias);
                return alias;
            } else {
                sql.append("(");
                visit(plan);
                sql.append(") AS ");
                String alias = generateSubqueryAlias();
                sql.append(alias);
                return alias;
            }
        }
    }

    /**
     * Helper class to hold join information.
     */
    private static class JoinPart {
        final LogicalPlan right;
        final Join.JoinType joinType;
        final Expression condition;

        JoinPart(LogicalPlan right, Join.JoinType joinType, Expression condition) {
            this.right = right;
            this.joinType = joinType;
            this.condition = condition;
        }
    }

    /**
     * Gets the SQL keyword for a join type.
     */
    private String getJoinKeyword(Join.JoinType joinType) {
        return switch (joinType) {
            case INNER     -> "INNER JOIN";
            case LEFT      -> "LEFT OUTER JOIN";
            case RIGHT     -> "RIGHT OUTER JOIN";
            case FULL      -> "FULL OUTER JOIN";
            case CROSS     -> "CROSS JOIN";
            case LEFT_SEMI -> "SEMI JOIN";
            case LEFT_ANTI -> "ANTI JOIN";
        };
    }

    /**
     * Resolves the filter condition's SQL, applying polymorphic function resolution
     * if a child schema is available. This handles cases like size(map_col) needing
     * cardinality() instead of len() for MAP columns.
     */
    private String resolveFilterConditionSQL(Filter plan) {
        StructType childSchema = null;
        try {
            childSchema = plan.child().schema();
        } catch (Exception e) {
            LOG.trace("Schema resolution failed for Filter child; using direct toSQL()", e);
        }

        if (childSchema != null) {
            Expression resolvedCondition = resolvePolymorphicFunctions(plan.condition(), childSchema);
            // In strict mode, transform division and aggregates for correct type dispatch
            if (SparkCompatMode.isStrictMode()) {
                resolvedCondition = transformExpressionForStrictMode(resolvedCondition, childSchema);
            }
            return resolvedCondition.toSQL();
        }
        return plan.condition().toSQL();
    }

    /**
     * Visits a Filter node (WHERE clause).
     */
    private void visitFilter(Filter plan) {
        LogicalPlan child = plan.child();

        // For Joins with user-defined aliases (AliasedRelation), use flat join chain
        // to preserve alias visibility. Walk through stacked filters to find the Join.
        // Skip flat generation for SEMI/ANTI joins (they need EXISTS/NOT EXISTS via visitJoin)
        {
            List<Filter> filters = new java.util.ArrayList<>();
            filters.add(plan);
            LogicalPlan current = child;
            while (current instanceof Filter f) {
                filters.add(f);
                current = f.child();
            }
            if (current instanceof Join join && hasAliasedChildren(join) && !containsSemiOrAntiJoin(join)) {
                Map<Long, String> planIdToAlias = new HashMap<>();
                String selectPrefix = "SELECT *";
                if (hasOverlappingColumns(join)) {
                    String explicitList = buildExplicitSelectForJoinChain(join);
                    if (explicitList != null) selectPrefix = "SELECT " + explicitList;
                }
                sql.append(selectPrefix).append(" FROM ");
                generateFlatJoinChainWithMapping(join, planIdToAlias);

                // In strict mode, resolve the join's child schema for proper
                // DECIMAL division/aggregate dispatch in WHERE conditions
                StructType joinSchema = null;
                if (SparkCompatMode.isStrictMode()) {
                    try {
                        joinSchema = join.schema();
                    } catch (Exception e) {
                        LOG.debug("Schema resolution failed for Filter-on-Join: {}", e.getMessage());
                    }
                }

                sql.append(" WHERE ");
                for (int i = 0; i < filters.size(); i++) {
                    if (i > 0) {
                        sql.append(" AND ");
                    }
                    String qualifiedCond = qualifyCondition(filters.get(i).condition(), planIdToAlias, joinSchema);
                    sql.append("(").append(qualifiedCond).append(")");
                }
                return;
            }
        }

        {
            // Standard approach - check for direct table reference
            String childSource = getDirectlyAliasableSource(child);
            if (childSource != null) {
                // Direct table - no subquery needed
                sql.append("SELECT * FROM ").append(childSource);
                sql.append(" WHERE ");
                sql.append(resolveFilterConditionSQL(plan));
            } else if (child instanceof AliasedRelation aliased) {
                // Preserve user-provided alias (e.g., lineitem l2)
                // so WHERE can reference l2.column
                String aliasedChildSource = getDirectlyAliasableSource(aliased.child());
                if (aliasedChildSource != null) {
                    sql.append("SELECT * FROM ").append(aliasedChildSource)
                       .append(" AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
                } else {
                    sql.append("SELECT * FROM (");
                    subqueryDepth++;
                    visit(aliased.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
                }
                sql.append(" WHERE ");
                sql.append(resolveFilterConditionSQL(plan));
            } else {
                // Complex child - wrap in subquery
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(child);
                subqueryDepth--;
                sql.append(") AS ").append(generateSubqueryAlias());
                sql.append(" WHERE ");
                sql.append(resolveFilterConditionSQL(plan));
            }
        }
    }

    /**
     * Collects plan_id → alias mappings from an entire join tree.
     *
     * @param join the join to traverse
     * @param mapping the map to populate with plan_id → alias entries
     */
    /**
     * Builds plan_id → alias mapping for a join, correctly predicting the aliases
     * that will be generated. This must match the alias generation in generateFlatJoinChainWithMapping.
     *
     * @param join the join to build mapping for
     * @param mapping the map to populate
     */
    private void buildJoinPlanIdMapping(Join join, Map<Long, String> mapping) {
        // Collect all joins in the chain (same as in generateFlatJoinChainWithMapping)
        List<JoinPart> joinParts = new java.util.ArrayList<>();
        LogicalPlan leftmost = collectJoinParts(join, joinParts);

        // Collect all sources in order: leftmost, then each join's right side
        List<LogicalPlan> sources = new java.util.ArrayList<>();
        sources.add(leftmost);
        for (JoinPart part : joinParts) {
            sources.add(part.right);
        }

        // Predict the aliases for each source, starting from current aliasCounter
        int predictedCounter = aliasCounter;
        for (LogicalPlan source : sources) {
            if (source instanceof AliasedRelation aliased) {
                // User-provided alias
                String alias = SQLQuoting.quoteIdentifier(aliased.alias());
                if (aliased.planId().isPresent()) {
                    mapping.putIfAbsent(aliased.planId().getAsLong(), alias);
                }
                collectPlanIds(aliased.child(), alias, mapping);
            } else {
                // Will generate a subquery alias
                predictedCounter++;
                String alias = "subquery_" + predictedCounter;
                collectPlanIds(source, alias, mapping);
            }
        }
    }

    private void collectJoinPlanIds(Join join, Map<Long, String> mapping) {
        collectJoinSidePlanIds(join.left(), mapping);
        collectJoinSidePlanIds(join.right(), mapping);
    }

    /**
     * Collects plan_ids from one side of a join.
     *
     * @param plan the join side plan
     * @param mapping the map to populate with plan_id → alias entries
     */
    private void collectJoinSidePlanIds(LogicalPlan plan, Map<Long, String> mapping) {
        if (plan instanceof AliasedRelation aliased) {
            String alias = SQLQuoting.quoteIdentifier(aliased.alias());
            // Map the AliasedRelation's own plan_id
            if (aliased.planId().isPresent()) {
                mapping.putIfAbsent(aliased.planId().getAsLong(), alias);
            }
            // Also map child plan_ids to this alias
            collectPlanIds(aliased.child(), alias, mapping);
        } else if (plan instanceof Join j) {
            // Recursively handle nested joins
            collectJoinPlanIds(j, mapping);
        } else {
            // For non-aliased, non-join plans, generate an alias and map their plan_ids
            // Note: This alias should match what generateSubqueryAlias() will produce later
            String alias = "subquery_" + (aliasCounter + 1);
            collectPlanIds(plan, alias, mapping);
        }
    }

    /**
     * Checks if a Join has any AliasedRelation children (directly or nested).
     * Used to determine if filter should preserve alias visibility.
     */
    private boolean hasAliasedChildren(Join join) {
        return hasAliasedRelation(join.left()) || hasAliasedRelation(join.right());
    }

    /**
     * Recursively checks if a plan contains AliasedRelation.
     */
    private boolean hasAliasedRelation(LogicalPlan plan) {
        if (plan instanceof AliasedRelation) {
            return true;
        }
        if (plan instanceof Join j) {
            return hasAliasedRelation(j.left()) || hasAliasedRelation(j.right());
        }
        if (plan instanceof Filter f) {
            return hasAliasedRelation(f.child());
        }
        return false;
    }

    /**
     * Checks if a join or any join in its left chain is a SEMI or ANTI join.
     * Semi/anti joins cannot be flattened into a flat join chain because they
     * require EXISTS/NOT EXISTS subquery syntax (handled by visitJoin).
     * The flat join chain optimizer must not include these join types.
     */
    private boolean containsSemiOrAntiJoin(Join join) {
        if (join.joinType() == Join.JoinType.LEFT_SEMI ||
            join.joinType() == Join.JoinType.LEFT_ANTI) {
            return true;
        }
        // Check left side of the chain (join chains are left-deep)
        LogicalPlan left = join.left();
        // Walk through filters to find nested joins
        while (left instanceof Filter f) {
            left = f.child();
        }
        if (left instanceof Join leftJoin) {
            return containsSemiOrAntiJoin(leftJoin);
        }
        return false;
    }

    /**
     * Checks if a join chain has overlapping column names across different sources.
     * When sources share column names, DuckDB's SELECT * auto-deduplicates with :N suffixes,
     * which differs from the SQL standard (and Spark) behavior. Returns false if any
     * source schema is unavailable, so the caller can safely fall back to SELECT *.
     */
    private boolean hasOverlappingColumns(Join join) {
        List<JoinPart> joinParts = new java.util.ArrayList<>();
        LogicalPlan leftmost = collectJoinParts(join, joinParts);

        List<LogicalPlan> sources = new java.util.ArrayList<>();
        sources.add(leftmost);
        for (JoinPart part : joinParts) sources.add(part.right);

        java.util.Set<String> seen = new java.util.HashSet<>();
        for (LogicalPlan source : sources) {
            StructType schema = source.schema();
            if (schema == null || schema.fields().isEmpty()) return false;
            for (StructField field : schema.fields()) {
                if (!seen.add(field.name())) return true;
            }
        }
        return false;
    }

    /**
     * Builds an explicit SELECT column list for a join chain, qualifying each column
     * with its source alias. This prevents DuckDB from auto-deduplicating column names
     * with :N suffixes when SELECT * encounters duplicate names across sources.
     * Returns null if any source is not an AliasedRelation or has no schema, so the
     * caller can fall back to SELECT *.
     */
    private String buildExplicitSelectForJoinChain(Join join) {
        List<JoinPart> joinParts = new java.util.ArrayList<>();
        LogicalPlan leftmost = collectJoinParts(join, joinParts);

        List<LogicalPlan> sources = new java.util.ArrayList<>();
        sources.add(leftmost);
        for (JoinPart part : joinParts) sources.add(part.right);

        List<String> selectItems = new java.util.ArrayList<>();
        for (LogicalPlan source : sources) {
            if (!(source instanceof AliasedRelation ar)) return null;
            String alias = SQLQuoting.quoteIdentifier(ar.alias());
            StructType schema = source.schema();
            if (schema == null || schema.fields().isEmpty()) return null;
            for (StructField field : schema.fields()) {
                selectItems.add(alias + "." + SQLQuoting.quoteIdentifier(field.name()));
            }
        }
        return selectItems.isEmpty() ? null : String.join(", ", selectItems);
    }

    /**
     * Visits a Distinct node (DISTINCT operation).
     */
    private void visitDistinct(Distinct plan) {
        List<String> columns = plan.columns();

        if (columns == null || columns.isEmpty()) {
            // DISTINCT on all columns
            sql.append("SELECT DISTINCT * FROM ");
        } else {
            // DISTINCT on specific columns
            sql.append("SELECT DISTINCT ");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(columns.get(i)));
            }
            sql.append(" FROM ");
        }

        // Check for direct source optimization
        String childSource = getDirectlyAliasableSource(plan.children().get(0));
        if (childSource != null) {
            sql.append(childSource);
        } else {
            sql.append("(");
            subqueryDepth++;
            visit(plan.children().get(0));
            subqueryDepth--;
            sql.append(") AS ").append(generateSubqueryAlias());
        }
    }

    /**
     * Visits a RangeRelation node.
     *
     * <p>Generates SQL using DuckDB's native range() table function.
     * The range() function returns a column named "range", which we alias to "id"
     * to match Spark's convention.
     *
     * <p>Example output:
     * <pre>
     *   SELECT range AS id FROM range(0, 10, 1)
     * </pre>
     */
    private void visitRangeRelation(RangeRelation plan) {
        // DuckDB's range(start, end, step) generates values from start to end-1
        // which matches Spark's semantics exactly (end is exclusive)
        sql.append("SELECT range AS \"id\" FROM range(")
           .append(plan.start())
           .append(", ")
           .append(plan.end())
           .append(", ")
           .append(plan.step())
           .append(")");
    }

    /**
     * Visits a TableScan node (FROM clause).
     */
    private void visitTableScan(TableScan plan) {
        String source = plan.source();
        TableScan.TableFormat format = plan.format();

        sql.append(switch (format) {
            case TABLE   -> "SELECT * FROM " + quoteIdentifier(source);
            case PARQUET -> "SELECT * FROM read_parquet(" + quoteFilePath(source) + ")";
            case DELTA   -> "SELECT * FROM delta_scan(" + quoteFilePath(source) + ")";
            case ICEBERG -> "SELECT * FROM iceberg_scan(" + quoteFilePath(source) + ")";
        });
    }

    /**
     * Visits a Sort node (ORDER BY clause).
     * Builds SQL directly in buffer to avoid corruption.
     */
    private void visitSort(Sort plan) {
        // Check if child generates a single SELECT statement that can have ORDER BY appended.
        // This avoids unnecessary subquery wrapping that breaks column/table alias scoping.
        if (canAppendClause(plan.child())) {
            visit(plan.child());
        } else {
            // Check for direct source optimization
            String childSource = getDirectlyAliasableSource(plan.child());
            if (childSource != null) {
                sql.append("SELECT * FROM ").append(childSource);
            } else {
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(plan.child());  // Use visit(), not generate()
                subqueryDepth--;
                sql.append(") AS ").append(generateSubqueryAlias());
            }
        }
        sql.append(" ORDER BY ");

        // Check if child aggregate uses ROLLUP/CUBE/GROUPING SETS.
        // These produce NULL values for subtotal/grand-total rows, which must sort first
        // to match Spark's behavior. Force NULLS FIRST on all sort columns.
        boolean forceNullsFirst = hasRollupDescendant(plan.child());

        java.util.List<Sort.SortOrder> sortOrders = plan.sortOrders();
        for (int i = 0; i < sortOrders.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Sort.SortOrder order = sortOrders.get(i);
            sql.append(order.expression().toSQL());

            // Add direction
            if (order.direction() == Sort.SortDirection.DESCENDING) {
                sql.append(" DESC");
            } else {
                sql.append(" ASC");
            }

            // Add null ordering: force NULLS FIRST when ROLLUP is detected
            if (forceNullsFirst || order.nullOrdering() == Sort.NullOrdering.NULLS_FIRST) {
                sql.append(" NULLS FIRST");
            } else {
                sql.append(" NULLS LAST");
            }
        }
    }

    /**
     * Checks if a descendant logical plan node contains an Aggregate with ROLLUP,
     * CUBE, or GROUPING SETS grouping type.
     *
     * <p>Walks through transparent intermediate nodes (Project, Filter, Limit)
     * that can appear between a Sort and its underlying Aggregate. This is needed
     * because a Sort typically wraps Project(Aggregate(...)), not Aggregate directly.
     *
     * <p>When ROLLUP/CUBE/GROUPING SETS is detected, the Sort must force NULLS FIRST
     * on all ORDER BY columns. These grouping operations produce NULL values for
     * subtotal and grand-total rows, and Spark sorts these first by default.
     *
     * @param plan the child plan node to inspect
     * @return true if a ROLLUP/CUBE/GROUPING SETS aggregate is found
     */
    private boolean hasRollupDescendant(LogicalPlan plan) {
        if (plan instanceof Aggregate agg) {
            return agg.groupingSets() != null;
        }
        // Walk through transparent nodes that can appear between Sort and Aggregate
        if (plan instanceof Project p) return hasRollupDescendant(p.child());
        if (plan instanceof Filter f) return hasRollupDescendant(f.child());
        if (plan instanceof Limit l) return hasRollupDescendant(l.child());
        return false;
    }

    /**
     * Visits a Limit node (LIMIT clause).
     */
    private void visitLimit(Limit plan) {
        // Check if child generates a single SELECT statement that can have LIMIT appended.
        if (canAppendClause(plan.child())) {
            visit(plan.child());
        } else {
            // Check for direct source optimization
            String childSource = getDirectlyAliasableSource(plan.child());
            if (childSource != null) {
                sql.append("SELECT * FROM ").append(childSource);
            } else {
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(plan.child());
                subqueryDepth--;
                sql.append(") AS ").append(generateSubqueryAlias());
            }
        }
        sql.append(" LIMIT ");
        sql.append(plan.limit());

        if (plan.offset() > 0) {
            sql.append(" OFFSET ");
            sql.append(plan.offset());
        }
    }

    /**
     * Visits a Tail node.
     *
     * <p>The Tail operation is handled in memory by TailBatchIterator for
     * memory-efficient streaming (O(N) instead of O(total_rows)). When
     * generating SQL for a Tail plan, we simply generate SQL for the child
     * plan - the tail limit is applied during streaming via the iterator wrapper.
     *
     * <p>This method is kept for compatibility with unit tests that
     * call SQLGenerator directly, but the actual execution path uses
     * TailBatchIterator instead.
     */
    private void visitTail(Tail plan) {
        // Simply generate SQL for the child plan
        // The tail operation is handled in memory by TailBatchIterator
        visit(plan.child());
    }

    /**
     * Visits a Sample node.
     *
     * <p>Generates SQL using DuckDB's USING SAMPLE clause with Bernoulli sampling
     * to match Spark's per-row probability algorithm. The Bernoulli method
     * evaluates each row independently with the specified probability.
     *
     * <p>Example outputs:
     * <pre>
     *   SELECT * FROM (child) AS subquery_1 USING SAMPLE 10% (bernoulli)
     *   SELECT * FROM (child) AS subquery_1 USING SAMPLE 10% (bernoulli, 42)
     * </pre>
     */
    private void visitSample(Sample plan) {
        sql.append("SELECT * FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());

        // Convert fraction to percentage for DuckDB SAMPLE clause
        double percentage = plan.fraction() * 100.0;

        // Use Bernoulli sampling to match Spark's per-row probability algorithm
        if (plan.seed().isPresent()) {
            sql.append(String.format(" USING SAMPLE %.6f%% (bernoulli, %d)",
                percentage, plan.seed().getAsLong()));
        } else {
            sql.append(String.format(" USING SAMPLE %.6f%% (bernoulli)",
                percentage));
        }
    }

    /**
     * Visits a WithColumns node (add/replace columns).
     * Generates SQL that preserves column order when replacing existing columns.
     */
    private void visitWithColumns(WithColumns plan) {
        // Get child schema for column ordering and type-aware SQL generation
        StructType childSchema = null;
        try {
            childSchema = plan.child().schema();
        } catch (Exception e) {
            // Child schema unavailable - fall back to legacy approach
        }

        if (childSchema == null) {
            // Fall back to legacy COLUMNS() approach when schema unavailable
            visitWithColumnsLegacy(plan, null);
            return;
        }

        // Build map of columns being modified for O(1) lookup
        Map<String, Integer> modifiedCols = new HashMap<>();
        for (int i = 0; i < plan.columnNames().size(); i++) {
            modifiedCols.put(plan.columnNames().get(i), i);
        }

        // Track which columns exist in child schema (for identifying new columns)
        Set<String> childColumnNames = new HashSet<>();
        for (StructField field : childSchema.fields()) {
            childColumnNames.add(field.name());
        }

        sql.append("SELECT ");
        boolean first = true;

        // Iterate through child columns IN ORDER to preserve column positions
        for (StructField field : childSchema.fields()) {
            if (!first) sql.append(", ");
            first = false;

            if (modifiedCols.containsKey(field.name())) {
                // Output modified expression at this position
                int idx = modifiedCols.get(field.name());
                Expression expr = plan.columnExpressions().get(idx);
                String exprSQL = generateExpressionWithCast(expr, childSchema);
                sql.append(exprSQL).append(" AS ").append(SQLQuoting.quoteIdentifier(field.name()));
            } else {
                // Keep original column
                sql.append(SQLQuoting.quoteIdentifier(field.name()));
            }
        }

        // Add any NEW columns (columns not in child schema) at end
        for (int i = 0; i < plan.columnNames().size(); i++) {
            String name = plan.columnNames().get(i);
            if (!childColumnNames.contains(name)) {
                // This is a new column, not a replacement
                if (!first) sql.append(", ");
                first = false;
                Expression expr = plan.columnExpressions().get(i);
                String exprSQL = generateExpressionWithCast(expr, childSchema);
                sql.append(exprSQL).append(" AS ").append(SQLQuoting.quoteIdentifier(name));
            }
        }

        sql.append(" FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS _withcol_subquery");
    }

    /**
     * Generates expression SQL for a WithColumns expression with type-cast wrapping.
     *
     * <p>In strict mode, {@code BinaryExpression.toSQL()} uses expression-level
     * {@code dataType()} to decide whether to emit {@code spark_decimal_div()}.
     * For DataFrame-path expressions, operand types are often unresolved
     * (StringType from UnresolvedColumn), so the division falls back to native
     * DuckDB {@code /} which always returns DOUBLE — even for DECIMAL operands.
     *
     * <p>When TypeInferenceEngine (which uses the schema) says the result should
     * be DECIMAL but the expression's declared type disagrees, the generated SQL
     * will produce DOUBLE. Wrapping with CAST corrects this at the top-level
     * SELECT projection, consistent with the architecture's strict-mode strategy.
     */
    private String generateExpressionWithCast(Expression expr, StructType childSchema) {
        String exprSQL = expr.toSQL();

        if (SparkCompatMode.isStrictMode() && childSchema != null) {
            com.thunderduck.types.DataType resolvedType =
                com.thunderduck.types.TypeInferenceEngine.resolveType(expr, childSchema);
            if (resolvedType instanceof com.thunderduck.types.DecimalType decType
                    && !(expr.dataType() instanceof com.thunderduck.types.DecimalType)) {
                return "CAST(" + exprSQL + " AS DECIMAL("
                    + decType.precision() + ", " + decType.scale() + "))";
            }
        }

        return exprSQL;
    }

    /**
     * Legacy COLUMNS()-based approach for WithColumns when schema is unavailable.
     * This places modified columns at the end but works without schema information.
     */
    private void visitWithColumnsLegacy(WithColumns plan, StructType childSchema) {
        sql.append("SELECT COLUMNS(c -> c NOT IN (");
        for (int i = 0; i < plan.columnNames().size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("'").append(plan.columnNames().get(i).replace("'", "''")).append("'");
        }
        sql.append(")), ");

        for (int i = 0; i < plan.columnNames().size(); i++) {
            if (i > 0) sql.append(", ");
            Expression expr = plan.columnExpressions().get(i);
            String exprSQL = generateExpressionWithCast(expr, childSchema);
            sql.append(exprSQL).append(" AS ").append(SQLQuoting.quoteIdentifier(plan.columnNames().get(i)));
        }

        sql.append(" FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS _withcol_subquery");
    }

    /**
     * Visits a ToDF node (column renaming operation).
     * Generates SQL that renames columns using DuckDB's table alias with column names syntax.
     */
    private void visitToDF(ToDF plan) {
        List<String> columnNames = plan.columnNames();

        if (columnNames.isEmpty()) {
            // No renaming needed, just visit child
            visit(plan.child());
            return;
        }

        // Build column alias list
        StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                columnList.append(", ");
            }
            columnList.append(quoteIdentifier(columnNames.get(i)));
        }

        // Generate: SELECT * FROM (child) AS _todf_subquery(col1, col2, ...)
        sql.append("SELECT * FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS _todf_subquery(");
        sql.append(columnList);
        sql.append(")");
    }

    /**
     * Visits an Aggregate node (GROUP BY clause).
     * Builds SQL directly in buffer to avoid corruption from generate() calls.
     */
    private void visitAggregate(Aggregate plan) {
        if (plan.aggregateExpressions().isEmpty()) {
            throw new IllegalArgumentException("Cannot generate SQL for aggregation with no aggregate expressions");
        }

        // Special case: Aggregate on Filter(s) on Join
        // Generate flat SQL to preserve table alias visibility in GROUP BY/HAVING
        // Skip flat generation for SEMI/ANTI joins (they need EXISTS/NOT EXISTS via visitJoin)
        LogicalPlan child = plan.child();
        if (child instanceof Filter) {
            List<Filter> filters = new java.util.ArrayList<>();
            LogicalPlan current = child;
            while (current instanceof Filter f) {
                filters.add(f);
                current = f.child();
            }
            if (current instanceof Join currentJoin && !containsSemiOrAntiJoin(currentJoin)) {
                generateAggregateOnJoinBase(plan, filters, currentJoin);
                return;
            }
        }

        // Special case: Aggregate directly on Join
        // Skip flat generation for SEMI/ANTI joins (they need EXISTS/NOT EXISTS via visitJoin)
        if (child instanceof Join childJoin && !containsSemiOrAntiJoin(childJoin)) {
            generateAggregateOnJoinBase(plan, Collections.emptyList(), childJoin);
            return;
        }

        // Resolve child schema once for type-aware SQL generation
        com.thunderduck.types.StructType childSchema = null;
        try {
            childSchema = plan.child().schema();
        } catch (Exception e) {
            // Child schema resolution failed — proceed without type-aware corrections
        }

        // SELECT clause with grouping expressions and aggregates
        sql.append("SELECT ");

        // Render grouping expressions
        java.util.List<String> groupingRendered = new java.util.ArrayList<>();
        for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
            groupingRendered.add(expr.toSQL());
        }

        // Render aggregate expressions
        java.util.List<String> aggregateRendered = new java.util.ArrayList<>();
        for (Aggregate.AggregateExpression aggExpr : plan.aggregateExpressions()) {
            aggregateRendered.add(renderAggregateExpression(aggExpr, childSchema));
        }

        // Combine in correct order: use selectOrder if present, else grouping-first
        java.util.List<String> selectExprs = new java.util.ArrayList<>();
        if (plan.selectOrder() != null) {
            for (Aggregate.SelectEntry entry : plan.selectOrder()) {
                if (entry.isAggregate()) {
                    selectExprs.add(aggregateRendered.get(entry.index()));
                } else {
                    selectExprs.add(groupingRendered.get(entry.index()));
                }
            }
        } else {
            selectExprs.addAll(groupingRendered);
            selectExprs.addAll(aggregateRendered);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause - check for direct source optimization
        String childSource = getDirectlyAliasableSource(plan.child());
        if (childSource != null) {
            sql.append(" FROM ").append(childSource);
        } else if (plan.child() instanceof AliasedRelation aliased) {
            // Preserve user-provided alias and column aliases
            String aliasedChildSource = getDirectlyAliasableSource(aliased.child());
            if (aliasedChildSource != null) {
                sql.append(" FROM ").append(aliasedChildSource)
                   .append(" AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
            } else {
                sql.append(" FROM (");
                subqueryDepth++;
                visit(aliased.child());
                subqueryDepth--;
                sql.append(") AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
            }
            // Append column aliases if present (e.g., AS alias (col1, col2))
            if (!aliased.columnAliases().isEmpty()) {
                sql.append(" (");
                for (int i = 0; i < aliased.columnAliases().size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(SQLQuoting.quoteIdentifier(aliased.columnAliases().get(i)));
                }
                sql.append(")");
            }
        } else {
            sql.append(" FROM (");
            subqueryDepth++;
            visit(plan.child());  // Use visit(), not generate()
            subqueryDepth--;
            sql.append(") AS ").append(generateSubqueryAlias());
        }

        // GROUP BY clause
        appendGroupByClause(plan);

        // HAVING clause
        if (plan.havingCondition() != null) {
            sql.append(" HAVING ");
            Expression havingExpr = plan.havingCondition();
            if (SparkCompatMode.isStrictMode() && childSchema != null) {
                havingExpr = transformExpressionForStrictMode(havingExpr, childSchema);
            }
            sql.append(havingExpr.toSQL());
        }
    }

    /**
     * Generates flat SQL for Aggregate on Join, with optional intermediate filters.
     * This preserves table alias visibility so GROUP BY/HAVING can reference
     * correct table-qualified columns when both join sides have same-named columns.
     *
     * @param plan the aggregate plan
     * @param filters optional filter conditions between aggregate and join (empty list = no WHERE)
     * @param join the join to generate FROM clause for
     */
    private void generateAggregateOnJoinBase(Aggregate plan, List<Filter> filters, Join join) {
        // Resolve child schema for type-aware SQL generation
        com.thunderduck.types.StructType childSchema = null;
        try {
            childSchema = plan.child().schema();
        } catch (Exception e) {
            LOG.trace("Schema resolution failed for Aggregate child; proceeding without it", e);
        }

        // Two-pass approach: generate FROM first to build actual alias mapping,
        // then insert SELECT before it. This is necessary because complex join sources
        // (subqueries) may increment the alias counter internally during visit(),
        // causing predicted aliases to diverge from actual generated aliases.
        Map<Long, String> planIdToAlias = new HashMap<>();
        int insertPos = sql.length();
        sql.append(" FROM ");
        generateFlatJoinChainWithMapping(join, planIdToAlias);
        String fromClause = sql.substring(insertPos);
        sql.setLength(insertPos);

        // Now generate SELECT with the correct mapping
        sql.append("SELECT ");
        sql.append(String.join(", ", buildAggregateSelectListQualified(plan, childSchema, planIdToAlias)));
        sql.append(fromClause);

        // WHERE clause from filters (qualified)
        if (!filters.isEmpty()) {
            sql.append(" WHERE ");
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                String qualifiedCond = qualifyCondition(filters.get(i).condition(), planIdToAlias);
                sql.append("(").append(qualifiedCond).append(")");
            }
        }

        // GROUP BY clause (qualified)
        appendGroupByClauseQualified(plan, planIdToAlias);

        // HAVING clause (qualified)
        if (plan.havingCondition() != null) {
            sql.append(" HAVING ");
            sql.append(qualifyCondition(plan.havingCondition(), planIdToAlias));
        }
    }

    /**
     * Builds the SELECT list for an Aggregate node, handling expression ordering and aliases.
     */
    private java.util.List<String> buildAggregateSelectList(Aggregate plan,
                                                             com.thunderduck.types.StructType childSchema) {
        java.util.List<String> groupingRendered = new java.util.ArrayList<>();
        for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
            groupingRendered.add(expr.toSQL());
        }

        java.util.List<String> aggregateRendered = new java.util.ArrayList<>();
        for (Aggregate.AggregateExpression aggExpr : plan.aggregateExpressions()) {
            aggregateRendered.add(renderAggregateExpression(aggExpr, childSchema));
        }

        java.util.List<String> selectExprs = new java.util.ArrayList<>();
        if (plan.selectOrder() != null) {
            for (Aggregate.SelectEntry entry : plan.selectOrder()) {
                if (entry.isAggregate()) {
                    selectExprs.add(aggregateRendered.get(entry.index()));
                } else {
                    selectExprs.add(groupingRendered.get(entry.index()));
                }
            }
        } else {
            selectExprs.addAll(groupingRendered);
            selectExprs.addAll(aggregateRendered);
        }
        return selectExprs;
    }

    /**
     * Builds the SELECT list for an Aggregate on Join, qualifying column references
     * with table aliases from the planIdToAlias mapping.
     */
    private java.util.List<String> buildAggregateSelectListQualified(Aggregate plan,
                                                                       com.thunderduck.types.StructType childSchema,
                                                                       Map<Long, String> planIdToAlias) {
        java.util.List<String> groupingRendered = new java.util.ArrayList<>();
        for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
            groupingRendered.add(qualifyCondition(expr, planIdToAlias));
        }

        java.util.List<String> aggregateRendered = new java.util.ArrayList<>();
        for (Aggregate.AggregateExpression aggExpr : plan.aggregateExpressions()) {
            aggregateRendered.add(renderAggregateExpressionQualified(aggExpr, childSchema, planIdToAlias));
        }

        java.util.List<String> selectExprs = new java.util.ArrayList<>();
        if (plan.selectOrder() != null) {
            for (Aggregate.SelectEntry entry : plan.selectOrder()) {
                if (entry.isAggregate()) {
                    selectExprs.add(aggregateRendered.get(entry.index()));
                } else {
                    selectExprs.add(groupingRendered.get(entry.index()));
                }
            }
        } else {
            selectExprs.addAll(groupingRendered);
            selectExprs.addAll(aggregateRendered);
        }
        return selectExprs;
    }

    /**
     * Appends the GROUP BY clause for an Aggregate node.
     */
    private void appendGroupByClause(Aggregate plan) {
        if (!plan.groupingExpressions().isEmpty()) {
            sql.append(" GROUP BY ");

            if (plan.groupingSets() != null) {
                sql.append(plan.groupingSets().toSQL());
            } else {
                java.util.List<String> groupExprs = new java.util.ArrayList<>();
                for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
                    if (expr instanceof com.thunderduck.expression.AliasExpression aliasExpr) {
                        groupExprs.add(aliasExpr.expression().toSQL());
                    } else {
                        groupExprs.add(expr.toSQL());
                    }
                }
                sql.append(String.join(", ", groupExprs));
            }
        }
    }

    /**
     * Appends the GROUP BY clause with qualified column references for Aggregate on Join.
     */
    private void appendGroupByClauseQualified(Aggregate plan, Map<Long, String> planIdToAlias) {
        if (!plan.groupingExpressions().isEmpty()) {
            sql.append(" GROUP BY ");

            if (plan.groupingSets() != null) {
                sql.append(plan.groupingSets().toSQL());
            } else {
                java.util.List<String> groupExprs = new java.util.ArrayList<>();
                for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
                    if (expr instanceof com.thunderduck.expression.AliasExpression aliasExpr) {
                        groupExprs.add(qualifyCondition(aliasExpr.expression(), planIdToAlias));
                    } else {
                        groupExprs.add(qualifyCondition(expr, planIdToAlias));
                    }
                }
                sql.append(String.join(", ", groupExprs));
            }
        }
    }


    /**
     * Checks whether the argument of an aggregate expression resolves to a DECIMAL type.
     * Extension functions (spark_sum, spark_avg) only have DECIMAL overloads, so they
     * should only be emitted when the input is confirmed DECIMAL. For all other types
     * (DOUBLE, INTEGER, BIGINT, etc.), vanilla DuckDB functions are used.
     *
     * @param aggExpr the aggregate expression to check
     * @param childSchema the child schema for resolving column types (may be null)
     * @return true if the argument is confirmed to be a DECIMAL type
     */
    private static boolean isDecimalAggregateArg(Aggregate.AggregateExpression aggExpr,
                                                   com.thunderduck.types.StructType childSchema) {
        if (aggExpr.argument() == null || childSchema == null) {
            return false;
        }
        com.thunderduck.types.DataType argType =
            com.thunderduck.types.TypeInferenceEngine.resolveType(aggExpr.argument(), childSchema);
        return argType instanceof com.thunderduck.types.DecimalType;
    }

    /**
     * Renders a single aggregate expression to SQL, handling strict mode transformations,
     * type casts, and aliasing.
     */
    private String renderAggregateExpression(Aggregate.AggregateExpression aggExpr,
                                              com.thunderduck.types.StructType childSchema) {
        String aggSQL;
        String originalSQL = null;

        if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && aggExpr.isComposite()) {
            originalSQL = aggExpr.rawExpression().toSQL();
            com.thunderduck.expression.Expression transformed = transformAggregateExpression(aggExpr.rawExpression(), childSchema);
            aggSQL = transformed.toSQL();
        } else if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && !aggExpr.isComposite()) {
            // First, transform argument expressions for strict mode (e.g., divisions inside
            // SUM/AVG arguments need spark_decimal_div to produce DECIMAL instead of DOUBLE).
            Expression transformedArg = aggExpr.argument() != null
                    ? transformExpressionForStrictMode(aggExpr.argument(), childSchema)
                    : null;

            // If the argument was transformed, rebuild the aggregate SQL manually
            if (transformedArg != null && transformedArg != aggExpr.argument()) {
                String argSQL = transformedArg.toSQL();
                boolean isDistinct = aggExpr.isDistinct()
                        || aggExpr.function().toUpperCase().endsWith("_DISTINCT");
                String funcName = aggExpr.function().toLowerCase();
                if (funcName.endsWith("_distinct")) {
                    funcName = funcName.substring(0, funcName.length() - "_distinct".length());
                }
                if (isDistinct) {
                    aggSQL = funcName + "(DISTINCT " + argSQL + ")";
                } else {
                    aggSQL = funcName + "(" + argSQL + ")";
                }
            } else {
                aggSQL = aggExpr.toSQL();
            }

            String baseFuncName = aggExpr.function().toUpperCase();
            if (baseFuncName.endsWith("_DISTINCT")) {
                baseFuncName = baseFuncName.substring(0, baseFuncName.length() - "_DISTINCT".length());
            }
            // For DECIMAL SUM/AVG, wrap with CAST to match Spark's return type.
            // We use CAST(sum(...) AS DECIMAL(p,s)) instead of spark_sum because
            // spark_sum triggers a DuckDB optimizer crash in UNION ALL CTEs
            // (CompressedMaterialization::CompressAggregate fails with extension aggregates).
            // Native sum() preserves DECIMAL precision, and the CAST ensures the exact
            // return type matches Spark's formula: SUM -> DECIMAL(min(38, p+10), s),
            // AVG -> DECIMAL(min(38, p+4), min(38, s+4)).
            if ((baseFuncName.equals("SUM") || baseFuncName.equals("AVG"))
                    && isDecimalAggregateArg(aggExpr, childSchema)) {
                originalSQL = aggExpr.toSQL(); // Use original (untransformed) SQL for alias
                com.thunderduck.types.DataType argType =
                    TypeInferenceEngine.resolveType(aggExpr.argument(), childSchema);
                if (argType instanceof com.thunderduck.types.DecimalType argDec) {
                    int p = argDec.precision();
                    int s = argDec.scale();
                    int resultP, resultS;
                    if (baseFuncName.equals("SUM")) {
                        resultP = Math.min(38, p + 10);
                        resultS = s;
                    } else { // AVG
                        resultP = Math.min(38, p + 4);
                        resultS = Math.min(38, s + 4);
                    }
                    aggSQL = "CAST(" + aggSQL + " AS DECIMAL(" + resultP + ", " + resultS + "))";
                }
            }
            // Spark: grouping() returns TINYINT (ByteType), grouping_id() returns BIGINT (LongType).
            // DuckDB returns INTEGER for both. Wrap with CAST to match Spark's types exactly.
            if (baseFuncName.equals("GROUPING")) {
                originalSQL = aggExpr.toSQL();
                aggSQL = "CAST(" + aggSQL + " AS TINYINT)";
            } else if (baseFuncName.equals("GROUPING_ID")) {
                originalSQL = aggExpr.toSQL();
                aggSQL = "CAST(" + aggSQL + " AS BIGINT)";
            }
        } else {
            aggSQL = aggExpr.toSQL();
        }

        aggSQL = wrapWithTypeCastIfNeeded(aggSQL, aggExpr, childSchema);

        // In relaxed mode, DuckDB's SUM returns HUGEINT for BIGINT inputs,
        // which means it never overflows (Spark's SUM returns BIGINT and throws
        // ArithmeticException on overflow). Wrap SUM(BIGINT) with CAST(... AS BIGINT)
        // to match Spark's overflow behavior.
        aggSQL = wrapSumWithBigintCastIfNeeded(aggSQL, aggExpr, childSchema);

        if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
            aggSQL += " AS " + SQLQuoting.quoteIdentifier(aggExpr.alias());
        } else if (aggExpr.isUnaliasedCountStar()) {
            aggSQL += " AS \"count(1)\"";
        } else if (originalSQL != null) {
            aggSQL += " AS " + SQLQuoting.quoteIdentifier(originalSQL);
        } else if (aggExpr.function() != null) {
            // Alias unaliased aggregates to match Spark's column naming convention
            // (e.g., sum(l_quantity) not SUM(l_quantity))
            String sparkName = buildSparkAggregateColumnName(aggExpr);
            if (sparkName != null) {
                aggSQL += " AS " + SQLQuoting.quoteIdentifier(sparkName);
            }
        }
        return aggSQL;
    }

    /**
     * Renders a single aggregate expression to SQL with qualified column references,
     * for use in Aggregate-on-Join paths where column disambiguation is needed.
     */
    private String renderAggregateExpressionQualified(Aggregate.AggregateExpression aggExpr,
                                                        com.thunderduck.types.StructType childSchema,
                                                        Map<Long, String> planIdToAlias) {
        String aggSQL;
        String originalSQL = null;

        if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && aggExpr.isComposite()) {
            originalSQL = aggExpr.rawExpression().toSQL();
            com.thunderduck.expression.Expression transformed = transformAggregateExpression(aggExpr.rawExpression(), childSchema);
            aggSQL = qualifyCondition(transformed, planIdToAlias);
        } else if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && !aggExpr.isComposite()) {
            // First, transform argument expressions for strict mode (e.g., divisions inside
            // SUM/AVG arguments need spark_decimal_div to produce DECIMAL instead of DOUBLE).
            Expression transformedArg = aggExpr.argument() != null
                    ? transformExpressionForStrictMode(aggExpr.argument(), childSchema)
                    : null;

            // If the argument was transformed, qualify and rebuild manually
            if (transformedArg != null && transformedArg != aggExpr.argument()) {
                String qualifiedArg = qualifyCondition(transformedArg, planIdToAlias);
                boolean isDistinct = aggExpr.isDistinct()
                        || aggExpr.function().toUpperCase().endsWith("_DISTINCT");
                String funcName = aggExpr.function().toLowerCase();
                if (funcName.endsWith("_distinct")) {
                    funcName = funcName.substring(0, funcName.length() - "_distinct".length());
                }
                if (isDistinct) {
                    aggSQL = funcName + "(DISTINCT " + qualifiedArg + ")";
                } else {
                    aggSQL = funcName + "(" + qualifiedArg + ")";
                }
            } else {
                aggSQL = qualifyAggregateExprSQL(aggExpr, planIdToAlias);
            }

            String baseFuncName = aggExpr.function().toUpperCase();
            if (baseFuncName.endsWith("_DISTINCT")) {
                baseFuncName = baseFuncName.substring(0, baseFuncName.length() - "_DISTINCT".length());
            }
            // For DECIMAL SUM/AVG, wrap with CAST to match Spark's return type.
            // See renderAggregateExpression for rationale (DuckDB UNION ALL + extension agg bug).
            if ((baseFuncName.equals("SUM") || baseFuncName.equals("AVG"))
                    && isDecimalAggregateArg(aggExpr, childSchema)) {
                originalSQL = aggExpr.toSQL(); // Use unqualified for the alias name
                com.thunderduck.types.DataType argType =
                    TypeInferenceEngine.resolveType(aggExpr.argument(), childSchema);
                if (argType instanceof com.thunderduck.types.DecimalType argDec) {
                    int p = argDec.precision();
                    int s = argDec.scale();
                    int resultP, resultS;
                    if (baseFuncName.equals("SUM")) {
                        resultP = Math.min(38, p + 10);
                        resultS = s;
                    } else { // AVG
                        resultP = Math.min(38, p + 4);
                        resultS = Math.min(38, s + 4);
                    }
                    aggSQL = "CAST(" + aggSQL + " AS DECIMAL(" + resultP + ", " + resultS + "))";
                }
            }
            // Spark: grouping() returns TINYINT (ByteType), grouping_id() returns BIGINT (LongType).
            // DuckDB returns INTEGER for both. Wrap with CAST to match Spark's types exactly.
            if (baseFuncName.equals("GROUPING")) {
                originalSQL = aggExpr.toSQL();
                aggSQL = "CAST(" + aggSQL + " AS TINYINT)";
            } else if (baseFuncName.equals("GROUPING_ID")) {
                originalSQL = aggExpr.toSQL();
                aggSQL = "CAST(" + aggSQL + " AS BIGINT)";
            }
        } else if (aggExpr.isComposite()) {
            aggSQL = qualifyCondition(aggExpr.rawExpression(), planIdToAlias);
        } else {
            aggSQL = qualifyAggregateExprSQL(aggExpr, planIdToAlias);
        }

        aggSQL = wrapWithTypeCastIfNeeded(aggSQL, aggExpr, childSchema);

        if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
            aggSQL += " AS " + SQLQuoting.quoteIdentifier(aggExpr.alias());
        } else if (aggExpr.isUnaliasedCountStar()) {
            aggSQL += " AS \"count(1)\"";
        } else if (originalSQL != null) {
            aggSQL += " AS " + SQLQuoting.quoteIdentifier(originalSQL);
        } else if (aggExpr.function() != null) {
            String sparkName = buildSparkAggregateColumnName(aggExpr);
            if (sparkName != null) {
                aggSQL += " AS " + SQLQuoting.quoteIdentifier(sparkName);
            }
        }
        return aggSQL;
    }

    /**
     * Qualifies column references inside a simple (non-composite) AggregateExpression.
     * Produces SQL like "SUM(t1.col)" instead of "SUM(col)".
     */
    private String qualifyAggregateExprSQL(Aggregate.AggregateExpression aggExpr,
                                            Map<Long, String> planIdToAlias) {
        if (aggExpr.argument() == null) {
            // COUNT(*) - no column to qualify
            return aggExpr.toSQL();
        }
        // Qualify the argument, then reconstruct the function call
        String qualifiedArg = qualifyCondition(aggExpr.argument(), planIdToAlias);

        // Handle distinct
        String funcName = aggExpr.function();
        boolean isDistinctSuffix = funcName.toUpperCase().endsWith("_DISTINCT");
        boolean isDistinct = aggExpr.isDistinct() || isDistinctSuffix;

        // Use lowercase for SUM to match Spark convention
        if (funcName.equalsIgnoreCase("sum") || funcName.equalsIgnoreCase("sum_distinct")) {
            if (isDistinct) {
                return "sum(DISTINCT " + qualifiedArg + ")";
            }
            return "sum(" + qualifiedArg + ")";
        }

        // For other functions, try FunctionRegistry translation with qualified arg
        try {
            if (isDistinct) {
                return com.thunderduck.functions.FunctionRegistry.translate(
                    funcName.replace("_DISTINCT", "").replace("_distinct", ""),
                    "DISTINCT " + qualifiedArg);
            }
            return com.thunderduck.functions.FunctionRegistry.translate(funcName, qualifiedArg);
        } catch (UnsupportedOperationException e) {
            // Fallback
            String displayName = funcName.toUpperCase();
            if (isDistinctSuffix) {
                displayName = displayName.substring(0, displayName.length() - "_DISTINCT".length());
            }
            if (isDistinct) {
                return displayName + "(DISTINCT " + qualifiedArg + ")";
            }
            return displayName + "(" + qualifiedArg + ")";
        }
    }

    /**
     * Builds Spark's expected column name for an unaliased aggregate.
     * Spark uses lowercase function name: sum(col), avg(col), min(col), etc.
     */
    private static String buildSparkAggregateColumnName(Aggregate.AggregateExpression aggExpr) {
        String func = aggExpr.function().toLowerCase();
        if (aggExpr.argument() != null) {
            String arg = aggExpr.argument().toSQL();
            return func + "(" + arg + ")";
        }
        return func + "(*)";
    }

    /**
     * Visits a WithCTE node (WITH ... AS (...) SELECT ...).
     */
    private void visitWithCTE(WithCTE plan) {
        sql.append(plan.toSQL(this));
    }

    /**
     * Transforms aggregate expressions in a composite expression tree for strict mode.
     * Recursively walks the AST and transforms arguments of SUM/AVG on DECIMAL
     * (e.g., divisions inside SUM need spark_decimal_div). Does NOT rename functions
     * to spark_sum/spark_avg because that triggers a DuckDB optimizer crash in
     * UNION ALL CTEs (CompressedMaterialization::CompressAggregate).
     * Instead, type correction is handled via CAST wrapping at render time.
     *
     * @param expr the expression to transform
     * @param childSchema the child schema for resolving argument types (may be null)
     * @return the transformed expression (or original if no changes needed)
     */
    public static Expression transformAggregateExpression(Expression expr,
                                                           com.thunderduck.types.StructType childSchema) {
        if (expr instanceof FunctionCall func) {
            String name = func.functionName().toLowerCase();

            // Spark: grouping() returns TINYINT (ByteType), grouping_id() returns BIGINT (LongType).
            // DuckDB returns INTEGER for both. Wrap with CAST via RawSQLExpression so that
            // composite expressions (e.g., grouping(a)+grouping(b)) emit correct CASTs.
            // We use RawSQLExpression instead of CastExpression to avoid the TRUNC wrapping
            // that CastExpression applies for integral target types (grouping already returns int).
            if (name.equals("grouping")) {
                return new com.thunderduck.expression.RawSQLExpression(
                    "CAST(" + expr.toSQL() + " AS TINYINT)",
                    com.thunderduck.types.ByteType.get(), false);
            }
            if (name.equals("grouping_id")) {
                return new com.thunderduck.expression.RawSQLExpression(
                    "CAST(" + expr.toSQL() + " AS BIGINT)",
                    com.thunderduck.types.LongType.get(), false);
            }

            if (name.equals("sum") || name.equals("sum_distinct") ||
                name.equals("avg") || name.equals("avg_distinct")) {
                // Only transform arguments if first argument is DECIMAL
                boolean isDecimalArg = false;
                if (!func.arguments().isEmpty() && childSchema != null) {
                    com.thunderduck.types.DataType argType =
                        com.thunderduck.types.TypeInferenceEngine.resolveType(
                            func.arguments().get(0), childSchema);
                    isDecimalArg = argType instanceof com.thunderduck.types.DecimalType;
                }
                if (isDecimalArg) {
                    // Transform arguments (e.g., divisions inside SUM/AVG need
                    // spark_decimal_div to produce DECIMAL instead of DOUBLE).
                    // Keep the native function name; CAST wrapping for type parity
                    // is done at render time.
                    List<Expression> transformedArgs = new java.util.ArrayList<>();
                    boolean changed = false;
                    for (Expression arg : func.arguments()) {
                        Expression transformed = transformExpressionForStrictMode(arg, childSchema);
                        transformedArgs.add(transformed);
                        if (transformed != arg) changed = true;
                    }
                    if (changed) {
                        return new FunctionCall(func.functionName(), transformedArgs, func.dataType(), func.nullable());
                    }
                }
            }
            return expr;
        }
        if (expr instanceof BinaryExpression bin) {
            Expression newLeft = transformAggregateExpression(bin.left(), childSchema);
            Expression newRight = transformAggregateExpression(bin.right(), childSchema);
            if (newLeft != bin.left() || newRight != bin.right()) {
                return new BinaryExpression(newLeft, bin.operator(), newRight);
            }
            return expr;
        }
        if (expr instanceof CastExpression cast) {
            Expression newInner = transformAggregateExpression(cast.expression(), childSchema);
            if (newInner != cast.expression()) {
                return new CastExpression(newInner, cast.targetType());
            }
            return expr;
        }
        if (expr instanceof UnaryExpression unary) {
            Expression newOperand = transformAggregateExpression(unary.operand(), childSchema);
            if (newOperand != unary.operand()) {
                return new UnaryExpression(unary.operator(), newOperand);
            }
            return expr;
        }
        if (expr instanceof AliasExpression alias) {
            Expression newInner = transformAggregateExpression(alias.expression(), childSchema);
            if (newInner != alias.expression()) {
                return new AliasExpression(newInner, alias.alias());
            }
            return expr;
        }
        // Literals, columns, etc. -- no transformation needed
        return expr;
    }

    /**
     * Transforms an expression tree for strict mode SQL generation by resolving
     * unresolved types from the child schema. Handles two key cases:
     *
     * <ol>
     *   <li><b>DECIMAL division</b>: {@code BinaryExpression(DIVIDE)} where operands
     *       resolve to DECIMAL via schema lookup are rewritten to use
     *       {@code spark_decimal_div()} (or CAST to DOUBLE for integer/integer).</li>
     *   <li><b>DECIMAL aggregates</b>: {@code FunctionCall("sum"/"avg")} where the
     *       argument resolves to DECIMAL are rewritten to {@code spark_sum/spark_avg}.</li>
     * </ol>
     *
     * <p>This method is recursive: it walks the entire expression tree so that
     * nested cases like {@code round(a / b, 2)} or {@code SUM(x) / SUM(y)} are
     * handled correctly.
     *
     * <p>Returns the original expression unchanged if no rewriting is needed,
     * or a new expression tree with affected nodes replaced.
     *
     * @param expr the expression to transform
     * @param schema the schema for resolving column types (may be null)
     * @return the transformed expression (or original if no changes needed)
     */
    public static Expression transformExpressionForStrictMode(Expression expr, StructType schema) {
        if (expr == null || schema == null) {
            return expr;
        }

        // --- BinaryExpression: handle DIVIDE with schema-resolved types ---
        if (expr instanceof BinaryExpression bin) {
            // Recursively transform children first
            Expression newLeft = transformExpressionForStrictMode(bin.left(), schema);
            Expression newRight = transformExpressionForStrictMode(bin.right(), schema);

            if (bin.operator() == BinaryExpression.Operator.DIVIDE) {
                // Resolve operand types from schema
                DataType leftType = TypeInferenceEngine.resolveType(bin.left(), schema);
                DataType rightType = TypeInferenceEngine.resolveType(bin.right(), schema);

                // Only rewrite if at least one operand resolves to DECIMAL or integral
                // (i.e., not the StringType default from UnresolvedColumn)
                boolean needsRewrite = (leftType instanceof com.thunderduck.types.DecimalType
                        || rightType instanceof com.thunderduck.types.DecimalType
                        || isIntegralForDivision(leftType) || isIntegralForDivision(rightType))
                        && !(leftType instanceof com.thunderduck.types.StringType
                             && rightType instanceof com.thunderduck.types.StringType);

                if (needsRewrite) {
                    String leftSQL = newLeft.toSQL();
                    String rightSQL = newRight.toSQL();
                    String divSQL = BinaryExpression.generateStrictDivisionSQL(
                            leftSQL, rightSQL, leftType, rightType);
                    return new RawSQLExpression(divSQL);
                }
            }

            // For non-DIVIDE operators or no rewrite needed, propagate child changes
            if (newLeft != bin.left() || newRight != bin.right()) {
                return new BinaryExpression(newLeft, bin.operator(), newRight);
            }
            return expr;
        }

        // --- FunctionCall: transform arguments (e.g., divisions inside SUM/AVG) ---
        if (expr instanceof FunctionCall func) {
            // Recursively transform arguments
            List<Expression> newArgs = new java.util.ArrayList<>();
            boolean argsChanged = false;
            for (Expression arg : func.arguments()) {
                Expression newArg = transformExpressionForStrictMode(arg, schema);
                newArgs.add(newArg);
                if (newArg != arg) argsChanged = true;
            }

            Expression result;
            // Return with transformed arguments if any changed
            if (argsChanged) {
                result = new FunctionCall(func.functionName(), newArgs,
                        func.dataType(), func.nullable(), func.distinct());
            } else {
                result = expr;
            }

            // For SUM/AVG on DECIMAL arguments, wrap with CAST to match Spark's return type.
            // DuckDB's native avg() returns DOUBLE for DECIMAL input; Spark returns DECIMAL.
            // This handles implicit aggregation (SELECT avg(col) FROM t -- no GROUP BY),
            // where the FunctionCall appears inside a Project, not an Aggregate node.
            String baseFuncName = func.functionName().toUpperCase();
            if (baseFuncName.endsWith("_DISTINCT")) {
                baseFuncName = baseFuncName.substring(0, baseFuncName.length() - "_DISTINCT".length());
            }
            if ((baseFuncName.equals("SUM") || baseFuncName.equals("AVG"))
                    && func.argumentCount() == 1) {
                DataType argType = TypeInferenceEngine.resolveType(func.arguments().get(0), schema);
                if (argType instanceof com.thunderduck.types.DecimalType argDec) {
                    int p = argDec.precision();
                    int s = argDec.scale();
                    int resultP, resultS;
                    if (baseFuncName.equals("SUM")) {
                        resultP = Math.min(38, p + 10);
                        resultS = s;
                    } else { // AVG
                        resultP = Math.min(38, p + 4);
                        resultS = Math.min(38, s + 4);
                    }
                    return new RawSQLExpression(
                            "CAST(" + result.toSQL() + " AS DECIMAL(" + resultP + ", " + resultS + "))",
                            new com.thunderduck.types.DecimalType(resultP, resultS),
                            true);
                }
            }

            return result;
        }

        // --- AliasExpression: recurse into inner expression ---
        if (expr instanceof AliasExpression alias) {
            Expression newInner = transformExpressionForStrictMode(alias.expression(), schema);
            if (newInner != alias.expression()) {
                return new AliasExpression(newInner, alias.alias());
            }
            return expr;
        }

        // --- CastExpression: recurse into inner expression ---
        if (expr instanceof CastExpression cast) {
            Expression newInner = transformExpressionForStrictMode(cast.expression(), schema);
            if (newInner != cast.expression()) {
                return new CastExpression(newInner, cast.targetType());
            }
            return expr;
        }

        // --- UnaryExpression: recurse into operand ---
        if (expr instanceof UnaryExpression unary) {
            Expression newOperand = transformExpressionForStrictMode(unary.operand(), schema);
            if (newOperand != unary.operand()) {
                return new UnaryExpression(unary.operator(), newOperand);
            }
            return expr;
        }

        // --- CaseWhenExpression: recurse into all branches ---
        if (expr instanceof CaseWhenExpression caseExpr) {
            List<Expression> newConditions = new java.util.ArrayList<>();
            List<Expression> newThenBranches = new java.util.ArrayList<>();
            boolean changed = false;
            for (int i = 0; i < caseExpr.conditions().size(); i++) {
                Expression newCond = transformExpressionForStrictMode(
                        caseExpr.conditions().get(i), schema);
                Expression newThen = transformExpressionForStrictMode(
                        caseExpr.thenBranches().get(i), schema);
                newConditions.add(newCond);
                newThenBranches.add(newThen);
                if (newCond != caseExpr.conditions().get(i)
                        || newThen != caseExpr.thenBranches().get(i)) {
                    changed = true;
                }
            }
            Expression newElse = caseExpr.elseBranch() != null
                    ? transformExpressionForStrictMode(caseExpr.elseBranch(), schema)
                    : null;
            if (newElse != caseExpr.elseBranch()) changed = true;

            if (changed) {
                return new CaseWhenExpression(newConditions, newThenBranches, newElse);
            }
            return expr;
        }

        // --- WindowFunction: skip for now (complex constructor, rare case) ---
        // Window function SUM/AVG on DECIMAL handled separately if needed.

        // --- InExpression: recurse into test expression and values ---
        if (expr instanceof InExpression inExpr) {
            Expression newTest = transformExpressionForStrictMode(inExpr.testExpr(), schema);
            List<Expression> newValues = new java.util.ArrayList<>();
            boolean changed = newTest != inExpr.testExpr();
            for (Expression val : inExpr.values()) {
                Expression newVal = transformExpressionForStrictMode(val, schema);
                newValues.add(newVal);
                if (newVal != val) changed = true;
            }
            if (changed) {
                return new InExpression(newTest, newValues, inExpr.isNegated());
            }
            return expr;
        }

        // Leaf expressions (Literal, UnresolvedColumn, StarExpression, RawSQLExpression, etc.)
        // and complex expressions we don't need to recurse into
        return expr;
    }

    /**
     * Helper for strict mode division: checks if a type is integral (for division rewriting).
     */
    private static boolean isIntegralForDivision(DataType type) {
        return type instanceof com.thunderduck.types.ByteType
                || type instanceof com.thunderduck.types.ShortType
                || type instanceof com.thunderduck.types.IntegerType
                || type instanceof com.thunderduck.types.LongType;
    }

    /**
     * Wraps an aggregate expression SQL with a CAST if DuckDB's return type would differ from
     * Spark's expected type. Handles two cases:
     * <ul>
     *   <li>DOUBLE: composite aggregates like {@code SUM(x) / 7.0} where Spark promotes to DOUBLE
     *       but DuckDB treats 7.0 as DECIMAL</li>
     *   <li>DECIMAL precision: composite aggregates like {@code SUM(a * b)} where DuckDB's
     *       intermediate precision differs from Spark's DECIMAL(38,s)</li>
     * </ul>
     *
     * @param aggSQL the aggregate expression SQL
     * @param aggExpr the aggregate expression
     * @param childSchema the child schema for resolving argument types (may be null)
     * @return the SQL, possibly wrapped with CAST
     */
    public static String wrapWithTypeCastIfNeeded(String aggSQL, Aggregate.AggregateExpression aggExpr,
                                                      com.thunderduck.types.StructType childSchema) {
        // Only composite aggregates need type correction CASTs.
        // Simple aggregates (SUM, AVG, COUNT, etc.) return the correct type natively
        // in DuckDB, or are handled by extension functions (spark_sum, spark_avg).
        // Composite aggregates (e.g., SUM(a) / 7.0, SUM(a * b)) can have type
        // mismatches because DuckDB's type promotion differs from Spark's.
        if (!aggExpr.isComposite() || childSchema == null) return aggSQL;

        com.thunderduck.types.DataType sparkType =
            com.thunderduck.types.TypeInferenceEngine.resolveType(aggExpr.rawExpression(), childSchema);

        if (sparkType instanceof com.thunderduck.types.DoubleType) {
            return "CAST(" + aggSQL + " AS DOUBLE)";
        }

        // For composite aggregates with DECIMAL type, ensure correct precision.
        // DuckDB's intermediate precision may differ from Spark's (e.g., DECIMAL(28,4) vs DECIMAL(38,4)).
        if (sparkType instanceof com.thunderduck.types.DecimalType decType) {
            return "CAST(" + aggSQL + " AS DECIMAL(" + decType.precision() + ", " + decType.scale() + "))";
        }

        return aggSQL;
    }

    /**
     * Wraps SUM(BIGINT) with CAST(... AS BIGINT) in relaxed mode to match Spark's
     * overflow behavior. DuckDB's SUM promotes BIGINT to HUGEINT (no overflow),
     * but Spark's SUM(BIGINT) returns BIGINT and throws on overflow.
     *
     * <p>Only applies to simple (non-composite) SUM aggregates where the input
     * argument resolves to LongType (BIGINT) or IntegerType.
     */
    private String wrapSumWithBigintCastIfNeeded(String aggSQL, Aggregate.AggregateExpression aggExpr,
                                                  com.thunderduck.types.StructType childSchema) {
        // Only for non-composite SUM in relaxed mode
        if (aggExpr.isComposite() || aggExpr.function() == null || childSchema == null) {
            return aggSQL;
        }
        // In strict mode, spark_sum already handles this
        if (com.thunderduck.runtime.SparkCompatMode.isStrictMode()) {
            return aggSQL;
        }

        String funcName = aggExpr.function().toUpperCase();
        if (funcName.endsWith("_DISTINCT")) {
            funcName = funcName.substring(0, funcName.length() - "_DISTINCT".length());
        }
        if (!funcName.equals("SUM")) {
            return aggSQL;
        }

        // Resolve the input argument type
        if (aggExpr.argument() != null) {
            com.thunderduck.types.DataType argType =
                com.thunderduck.types.TypeInferenceEngine.resolveType(aggExpr.argument(), childSchema);
            if (argType instanceof com.thunderduck.types.LongType
                    || argType instanceof com.thunderduck.types.IntegerType
                    || argType instanceof com.thunderduck.types.ShortType
                    || argType instanceof com.thunderduck.types.ByteType) {
                return "CAST(" + aggSQL + " AS BIGINT)";
            }
        }
        return aggSQL;
    }

    /**
     * Resolves the data type of an expression from the child schema.
     *
     * @param expr the expression
     * @param childSchema the child schema for resolving column types
     * @return the resolved data type
     */
    private com.thunderduck.types.DataType resolveExpressionType(
            com.thunderduck.expression.Expression expr,
            com.thunderduck.types.StructType childSchema) {
        // Get underlying expression if aliased
        com.thunderduck.expression.Expression baseExpr = expr;
        if (expr instanceof com.thunderduck.expression.AliasExpression ae) {
            baseExpr = ae.expression();
        }

        // Resolve column reference from child schema
        if (baseExpr instanceof UnresolvedColumn col && childSchema != null) {
            String colName = col.columnName();
            com.thunderduck.types.StructField field = childSchema.fieldByName(colName);
            if (field != null) {
                return field.dataType();
            }
        }

        // For other expressions, use their declared type
        return baseExpr.dataType();
    }

    /**
     * Attempts to generate a flat SQL statement for nested semi/anti join chains.
     *
     * <p>When semi/anti joins are stacked (e.g., Q21: anti(semi(filter(join chain)))),
     * the standard approach wraps each left side in a subquery, hiding user-defined
     * aliases like "l1" from the EXISTS/NOT EXISTS conditions. This method detects
     * such chains and generates flat SQL where the base join chain's aliases remain
     * visible to all EXISTS/NOT EXISTS clauses.
     *
     * @param plan the semi or anti join to attempt flat generation for
     * @return true if flat generation was used, false to fall back to standard approach
     */
    private boolean tryGenerateFlatSemiAntiJoin(Join plan) {
        // Collect the chain of semi/anti joins walking down the left side
        List<Join> semiAntiChain = new java.util.ArrayList<>();
        semiAntiChain.add(plan);

        LogicalPlan current = plan.left();
        while (current instanceof Join j) {
            if (j.joinType() == Join.JoinType.LEFT_SEMI || j.joinType() == Join.JoinType.LEFT_ANTI) {
                semiAntiChain.add(j);
                current = j.left();
            } else {
                break;
            }
        }

        // Need at least 2 semi/anti joins in the chain for this optimization to matter
        if (semiAntiChain.size() < 2) {
            return false;
        }

        // Walk through stacked filters to collect filter conditions
        List<Expression> filterConditions = new java.util.ArrayList<>();
        while (current instanceof Filter f) {
            filterConditions.add(f.condition());
            current = f.child();
        }

        // The base must be a regular Join with aliased children (looking through filters)
        if (!(current instanceof Join currentJoin) || !hasAliasedChildren(currentJoin)) {
            return false;
        }

        Join baseJoin = currentJoin;

        // Collect any additional filter conditions that are interleaved within the join chain.
        // The join chain may have Filters on the left side of joins (e.g., Join(Filter(...Join...), table)).
        // We need to extract these filters and flatten the entire chain.
        List<Expression> innerFilterConditions = new java.util.ArrayList<>();
        baseJoin = flattenJoinFilters(baseJoin, innerFilterConditions);

        // Build the plan_id → alias mapping from the base join chain
        Map<Long, String> planIdToAlias = new HashMap<>();

        // Generate: SELECT * FROM <flat join chain> WHERE <filters> AND EXISTS/NOT EXISTS ...
        sql.append("SELECT * FROM ");
        generateFlatJoinChainWithMapping(baseJoin, planIdToAlias);

        // Also collect plan_ids from each semi/anti join's right side
        // so that qualifyCondition can resolve columns in the EXISTS conditions
        for (Join semiAnti : semiAntiChain) {
            LogicalPlan rightPlan = semiAnti.right();
            String rightAlias;
            if (rightPlan instanceof AliasedRelation aliasedRight) {
                rightAlias = SQLQuoting.quoteIdentifier(aliasedRight.alias());
            } else {
                // Predict what alias will be generated (don't consume it yet)
                rightAlias = "subquery_" + (aliasCounter + 1 + semiAntiChain.indexOf(semiAnti));
            }
            collectPlanIds(rightPlan, rightAlias, planIdToAlias);
        }

        // Build WHERE clause: inner filters + outer filters + EXISTS/NOT EXISTS
        boolean hasWhere = false;

        // Add inner filter conditions (from within the join chain) first
        for (Expression filterCond : innerFilterConditions) {
            if (!hasWhere) {
                sql.append(" WHERE ");
                hasWhere = true;
            } else {
                sql.append(" AND ");
            }
            sql.append("(").append(qualifyCondition(filterCond, planIdToAlias)).append(")");
        }

        // Add outer filter conditions (between semi/anti chain and base join)
        for (Expression filterCond : filterConditions) {
            if (!hasWhere) {
                sql.append(" WHERE ");
                hasWhere = true;
            } else {
                sql.append(" AND ");
            }
            sql.append("(").append(qualifyCondition(filterCond, planIdToAlias)).append(")");
        }

        // Add EXISTS/NOT EXISTS for each semi/anti join
        for (Join semiAnti : semiAntiChain) {
            if (!hasWhere) {
                sql.append(" WHERE ");
                hasWhere = true;
            } else {
                sql.append(" AND ");
            }

            if (semiAnti.joinType() == Join.JoinType.LEFT_ANTI) {
                sql.append("NOT ");
            }

            // Generate EXISTS (SELECT 1 FROM <right> WHERE <condition>)
            LogicalPlan rightPlan = semiAnti.right();
            sql.append("EXISTS (SELECT 1 FROM ");

            if (rightPlan instanceof AliasedRelation aliased) {
                String childSource = getDirectlyAliasableSource(aliased.child());
                if (childSource != null) {
                    sql.append(childSource).append(" AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
                } else {
                    sql.append("(");
                    subqueryDepth++;
                    visit(aliased.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(SQLQuoting.quoteIdentifier(aliased.alias()));
                }
            } else {
                String rightSource = getDirectlyAliasableSource(rightPlan);
                if (rightSource != null) {
                    sql.append(rightSource).append(" AS ").append(generateSubqueryAlias());
                } else {
                    sql.append("(");
                    subqueryDepth++;
                    visit(rightPlan);
                    subqueryDepth--;
                    sql.append(") AS ").append(generateSubqueryAlias());
                }
            }

            if (semiAnti.condition() != null) {
                sql.append(" WHERE ");
                sql.append(qualifyCondition(semiAnti.condition(), planIdToAlias));
            }

            sql.append(")");
        }

        return true;
    }

    /**
     * Flattens a join tree by extracting Filter nodes from the left side of joins.
     *
     * <p>When a join chain has Filters interleaved (e.g., Join(Filter(Join(...)), table)),
     * this method reconstructs the join tree with the Filters removed and their conditions
     * collected separately. This allows generateFlatJoinChainWithMapping to produce a
     * completely flat FROM clause.
     *
     * @param join the join to flatten
     * @param filterConditions list to collect extracted filter conditions into
     * @return the reconstructed join tree without interleaved Filters
     */
    private Join flattenJoinFilters(Join join, List<Expression> filterConditions) {
        LogicalPlan left = join.left();

        // Peel off any Filters on the left side
        while (left instanceof Filter f) {
            filterConditions.add(f.condition());
            left = f.child();
        }

        // If the left is itself a join, recursively flatten it too
        if (left instanceof Join leftJoin) {
            left = flattenJoinFilters(leftJoin, filterConditions);
        }

        // If we peeled any filters, reconstruct the join with the new left
        if (left != join.left()) {
            return new Join(left, join.right(), join.joinType(), join.condition(), join.usingColumns());
        }
        return join;
    }

    /**
     * Visits a Join node.
     * Builds SQL directly in buffer to avoid corruption.
     *
     * <p>For columns with plan_id, this method qualifies column references
     * based on which side of the join they belong to. This enables proper
     * handling of joins where both tables have columns with the same name.
     *
     * For semi-joins and anti-joins, uses EXISTS/NOT EXISTS patterns
     * using EXISTS/NOT EXISTS for proper column alias scoping.
     */
    private void visitJoin(Join plan) {
        // Handle SEMI and ANTI joins differently (using EXISTS/NOT EXISTS)
        if (plan.joinType() == Join.JoinType.LEFT_SEMI ||
            plan.joinType() == Join.JoinType.LEFT_ANTI) {

            // Try flat generation for nested semi/anti join chains
            if (tryGenerateFlatSemiAntiJoin(plan)) {
                return;
            }

            // For semi/anti joins, we need to use WHERE EXISTS/NOT EXISTS
            // SELECT * FROM left WHERE [NOT] EXISTS (SELECT 1 FROM right WHERE condition)

            LogicalPlan leftPlan = plan.left();
            LogicalPlan rightPlan = plan.right();

            // Determine aliases - use user-provided aliases from AliasedRelation when available
            String leftAlias;
            String rightAlias;

            if (leftPlan instanceof AliasedRelation leftAliased) {
                leftAlias = SQLQuoting.quoteIdentifier(leftAliased.alias());
            } else {
                leftAlias = generateSubqueryAlias();
            }

            if (rightPlan instanceof AliasedRelation rightAliased) {
                rightAlias = SQLQuoting.quoteIdentifier(rightAliased.alias());
            } else {
                rightAlias = generateSubqueryAlias();
            }

            // Build plan_id → alias mapping for column qualification
            Map<Long, String> planIdToAlias = new HashMap<>();
            collectPlanIds(plan.left(), leftAlias, planIdToAlias);
            collectPlanIds(plan.right(), rightAlias, planIdToAlias);

            // LEFT SIDE - handle AliasedRelation, use direct aliasing for TableScan, wrap others
            if (leftPlan instanceof AliasedRelation aliased) {
                String childSource = getDirectlyAliasableSource(aliased.child());
                if (childSource != null) {
                    sql.append("SELECT * FROM ").append(childSource).append(" AS ").append(leftAlias);
                } else {
                    sql.append("SELECT * FROM (");
                    subqueryDepth++;
                    visit(aliased.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(leftAlias);
                }
            } else {
                String leftSource = getDirectlyAliasableSource(leftPlan);
                if (leftSource != null) {
                    sql.append("SELECT * FROM ").append(leftSource).append(" AS ").append(leftAlias);
                } else {
                    sql.append("SELECT * FROM (");
                    subqueryDepth++;
                    visit(leftPlan);
                    subqueryDepth--;
                    sql.append(") AS ").append(leftAlias);
                }
            }

            // Add WHERE [NOT] EXISTS
            sql.append(" WHERE ");
            if (plan.joinType() == Join.JoinType.LEFT_ANTI) {
                sql.append("NOT ");
            }

            // RIGHT SIDE - handle AliasedRelation, use direct aliasing for TableScan, wrap others
            if (rightPlan instanceof AliasedRelation aliased2) {
                String childSource = getDirectlyAliasableSource(aliased2.child());
                if (childSource != null) {
                    sql.append("EXISTS (SELECT 1 FROM ").append(childSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("EXISTS (SELECT 1 FROM (");
                    subqueryDepth++;
                    visit(aliased2.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(rightAlias);
                }
            } else {
                String rightSource = getDirectlyAliasableSource(rightPlan);
                if (rightSource != null) {
                    sql.append("EXISTS (SELECT 1 FROM ").append(rightSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("EXISTS (SELECT 1 FROM (");
                    subqueryDepth++;
                    visit(rightPlan);
                    subqueryDepth--;
                    sql.append(") AS ").append(rightAlias);
                }
            }

            // Add WHERE clause for the correlation
            if (plan.condition() != null) {
                sql.append(" WHERE ");
                // Qualify columns using plan_id mapping
                sql.append(qualifyCondition(plan.condition(), planIdToAlias));
            }

            sql.append(")");

        } else {
            // Regular joins (INNER, LEFT, RIGHT, FULL, CROSS)

            // Determine aliases - use user-provided aliases from AliasedRelation when available
            String leftAlias;
            String rightAlias;
            LogicalPlan leftPlan = plan.left();
            LogicalPlan rightPlan = plan.right();

            if (leftPlan instanceof AliasedRelation leftAliased) {
                leftAlias = SQLQuoting.quoteIdentifier(leftAliased.alias());
            } else {
                leftAlias = generateSubqueryAlias();
            }

            if (rightPlan instanceof AliasedRelation rightAliased) {
                rightAlias = SQLQuoting.quoteIdentifier(rightAliased.alias());
            } else {
                rightAlias = generateSubqueryAlias();
            }

            // Build plan_id → alias mapping for column qualification
            Map<Long, String> planIdToAlias = new HashMap<>();
            collectPlanIds(plan.left(), leftAlias, planIdToAlias);
            collectPlanIds(plan.right(), rightAlias, planIdToAlias);

            // Generate SELECT clause - use explicit column list for USING joins to deduplicate
            String selectClause = generateJoinSelectClause(plan, leftAlias, rightAlias);

            // LEFT SIDE - use direct aliasing for TableScan, handle AliasedRelation, wrap others
            if (leftPlan instanceof AliasedRelation aliased) {
                // User provided an alias - generate SQL using that alias directly
                String childSource = getDirectlyAliasableSource(aliased.child());
                if (childSource != null) {
                    sql.append(selectClause).append(" FROM ").append(childSource).append(" AS ").append(leftAlias);
                } else {
                    sql.append(selectClause).append(" FROM (");
                    subqueryDepth++;
                    visit(aliased.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(leftAlias);
                }
            } else {
                String leftSource = getDirectlyAliasableSource(leftPlan);
                if (leftSource != null) {
                    sql.append(selectClause).append(" FROM ").append(leftSource).append(" AS ").append(leftAlias);
                } else {
                    sql.append(selectClause).append(" FROM (");
                    subqueryDepth++;
                    visit(leftPlan);
                    subqueryDepth--;
                    sql.append(") AS ").append(leftAlias);
                }
            }

            // JOIN type
            sql.append(switch (plan.joinType()) {
                case INNER     -> " INNER JOIN ";
                case LEFT      -> " LEFT OUTER JOIN ";
                case RIGHT     -> " RIGHT OUTER JOIN ";
                case FULL      -> " FULL OUTER JOIN ";
                case CROSS     -> " CROSS JOIN ";
                case LEFT_SEMI -> " SEMI JOIN ";
                case LEFT_ANTI -> " ANTI JOIN ";
            });

            // RIGHT SIDE - use direct aliasing for TableScan, handle AliasedRelation, wrap others
            if (rightPlan instanceof AliasedRelation aliased2) {
                // User provided an alias - generate SQL using that alias directly
                String childSource = getDirectlyAliasableSource(aliased2.child());
                if (childSource != null) {
                    sql.append(childSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("(");
                    subqueryDepth++;
                    visit(aliased2.child());
                    subqueryDepth--;
                    sql.append(") AS ").append(rightAlias);
                }
            } else {
                String rightSource = getDirectlyAliasableSource(rightPlan);
                if (rightSource != null) {
                    sql.append(rightSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("(");
                    subqueryDepth++;
                    visit(rightPlan);
                    subqueryDepth--;
                    sql.append(") AS ").append(rightAlias);
                }
            }

            // ON clause (except for CROSS join)
            if (plan.joinType() != Join.JoinType.CROSS && plan.condition() != null) {
                sql.append(" ON ");
                // Qualify columns using plan_id mapping
                sql.append(qualifyCondition(plan.condition(), planIdToAlias));
            }
        }
    }

    /**
     * Collects plan_id → alias mappings from a logical plan tree.
     *
     * <p>Traverses the plan tree and maps each plan_id to the given alias,
     * but only if that plan_id is not already mapped. This ensures that
     * in self-join scenarios (where the same underlying data appears on
     * both sides), each side's plan_ids map to the correct alias.
     *
     * <p>For example, in {@code data.join(data.filter(...), ...)}, both sides
     * may share some plan_ids from the underlying data. We want:
     * <ul>
     *   <li>Left side's plan_ids → left alias</li>
     *   <li>Right side's plan_ids → right alias</li>
     *   <li>If a plan_id appears on both sides, the first mapping wins</li>
     * </ul>
     *
     * @param plan the logical plan to traverse
     * @param alias the alias to map plan_ids to
     * @param mapping the map to populate with plan_id → alias entries
     */
    private void collectPlanIds(LogicalPlan plan, String alias, Map<Long, String> mapping) {
        if (plan.planId().isPresent()) {
            long id = plan.planId().getAsLong();
            // Only add if not already mapped (preserves first mapping)
            mapping.putIfAbsent(id, alias);
        }
        // Recursively collect from children
        for (LogicalPlan child : plan.children()) {
            collectPlanIds(child, alias, mapping);
        }
    }

    /**
     * Qualifies column references in an expression using plan_id mapping.
     *
     * <p>Traverses the expression tree and qualifies UnresolvedColumn references
     * that have a plan_id with the appropriate table alias from the mapping.
     *
     * <p>Rules:
     * <ul>
     *   <li>Already qualified columns (with explicit qualifier) are preserved</li>
     *   <li>Columns with plan_id are qualified using the mapped alias</li>
     *   <li>Columns without plan_id are left unqualified (may cause ambiguity)</li>
     * </ul>
     *
     * @param expr the expression to qualify
     * @param planIdToAlias map from plan_id to table alias
     * @return the SQL string with qualified column references
     */
    public String qualifyCondition(Expression expr, Map<Long, String> planIdToAlias) {
        return qualifyCondition(expr, planIdToAlias, null);
    }

    /**
     * Qualifies column references in an expression tree using plan_id to alias mapping,
     * with optional schema-aware type resolution for strict mode.
     *
     * @param expr the expression to qualify
     * @param planIdToAlias mapping from plan_id to table alias
     * @param schema optional schema for resolving column types (for strict mode division/aggregates)
     * @return the qualified SQL string
     */
    public String qualifyCondition(Expression expr, Map<Long, String> planIdToAlias, StructType schema) {
        if (expr instanceof UnresolvedColumn col) {
            // Already qualified - return as-is
            if (col.isQualified()) {
                return col.toSQL();
            }
            // Use plan_id to determine alias
            if (col.hasPlanId()) {
                String alias = planIdToAlias.get(col.planId().getAsLong());
                if (alias != null) {
                    return alias + "." + SQLQuoting.quoteIdentifier(col.columnName());
                }
            }
            // No plan_id or no mapping - return unqualified (may cause ambiguity)
            return col.toSQL();
        }

        if (expr instanceof BinaryExpression binExpr) {
            String leftSQL = qualifyCondition(binExpr.left(), planIdToAlias, schema);
            String rightSQL = qualifyCondition(binExpr.right(), planIdToAlias, schema);
            if (binExpr.operator() == BinaryExpression.Operator.DIVIDE
                    && SparkCompatMode.isStrictMode()) {
                // Resolve operand types from schema if available, fall back to declared types
                DataType leftType = schema != null
                        ? TypeInferenceEngine.resolveType(binExpr.left(), schema)
                        : binExpr.left().dataType();
                DataType rightType = schema != null
                        ? TypeInferenceEngine.resolveType(binExpr.right(), schema)
                        : binExpr.right().dataType();
                LOG.debug("qualifyCondition DIVIDE: schema={}, leftType={}, rightType={}, left={}, right={}",
                        schema != null ? "present" : "null", leftType, rightType,
                        binExpr.left().toSQL(), binExpr.right().toSQL());
                return BinaryExpression.generateStrictDivisionSQL(
                        leftSQL, rightSQL, leftType, rightType);
            }
            return "(" + leftSQL + " " + binExpr.operator().symbol() + " " + rightSQL + ")";
        }

        if (expr instanceof UnaryExpression unaryExpr) {
            String operandSQL = qualifyCondition(unaryExpr.operand(), planIdToAlias, schema);
            if (unaryExpr.operator().isPrefix()) {
                if (unaryExpr.operator() == UnaryExpression.Operator.NEGATE) {
                    return "(" + unaryExpr.operator().symbol() + operandSQL + ")";
                }
                return "(" + unaryExpr.operator().symbol() + " " + operandSQL + ")";
            } else {
                return "(" + operandSQL + " " + unaryExpr.operator().symbol() + ")";
            }
        }

        if (expr instanceof AliasExpression aliasExpr) {
            String innerSQL = qualifyCondition(aliasExpr.expression(), planIdToAlias, schema);
            return innerSQL + " AS " + SQLQuoting.quoteIdentifierIfNeeded(aliasExpr.alias());
        }

        if (expr instanceof CastExpression castExpr) {
            String innerSQL = qualifyCondition(castExpr.expression(), planIdToAlias, schema);
            return CastExpression.generateCastSQL(innerSQL, castExpr.expression(), castExpr.targetType());
        }

        if (expr instanceof FunctionCall funcExpr) {
            List<String> qualifiedArgs = new ArrayList<>();
            for (Expression arg : funcExpr.arguments()) {
                qualifiedArgs.add(qualifyCondition(arg, planIdToAlias, schema));
            }

            String[] argArray = qualifiedArgs.toArray(new String[0]);
            try {
                return com.thunderduck.functions.FunctionRegistry.translate(
                    funcExpr.functionName(), argArray);
            } catch (UnsupportedOperationException e) {
                return funcExpr.functionName() + "(" + String.join(", ", qualifiedArgs) + ")";
            }
        }

        if (expr instanceof InExpression inExpr) {
            String testSQL = qualifyCondition(inExpr.testExpr(), planIdToAlias, schema);
            List<String> valuesSQLs = new ArrayList<>();
            for (Expression val : inExpr.values()) {
                valuesSQLs.add(qualifyCondition(val, planIdToAlias, schema));
            }
            String op = inExpr.isNegated() ? " NOT IN (" : " IN (";
            return testSQL + op + String.join(", ", valuesSQLs) + ")";
        }

        if (expr instanceof CaseWhenExpression caseExpr) {
            StringBuilder caseSql = new StringBuilder("CASE");
            for (int i = 0; i < caseExpr.conditions().size(); i++) {
                caseSql.append(" WHEN ")
                       .append(qualifyCondition(caseExpr.conditions().get(i), planIdToAlias, schema))
                       .append(" THEN ")
                       .append(qualifyCondition(caseExpr.thenBranches().get(i), planIdToAlias, schema));
            }
            if (caseExpr.elseBranch() != null) {
                caseSql.append(" ELSE ")
                       .append(qualifyCondition(caseExpr.elseBranch(), planIdToAlias, schema));
            }
            caseSql.append(" END");
            return caseSql.toString();
        }

        // For other expression types (Literal, etc.), use their default SQL representation
        return expr.toSQL();
    }

    /**
     * Generates a source reference that can be directly aliased in FROM clause.
     *
     * <p>Only TableScan nodes can be directly aliased because they represent
     * simple table/file references. Other leaf nodes (RangeRelation, LocalDataRelation)
     * produce SELECT statements that need subquery wrapping.
     *
     * <p>This optimization reduces verbose SQL like:
     * <pre>SELECT * FROM (SELECT * FROM "table") AS alias</pre>
     * to the cleaner:
     * <pre>SELECT * FROM "table" AS alias</pre>
     *
     * @param plan the logical plan
     * @return the source SQL if directly aliasable, or null if wrapping is needed
     */
    private String getDirectlyAliasableSource(LogicalPlan plan) {
        if (!(plan instanceof TableScan scan)) {
            return null;
        }
        String source = scan.source();
        return switch (scan.format()) {
            case TABLE   -> quoteIdentifier(source);
            case PARQUET -> "read_parquet(" + quoteFilePath(source) + ")";
            case DELTA   -> "delta_scan(" + quoteFilePath(source) + ")";
            case ICEBERG -> "iceberg_scan(" + quoteFilePath(source) + ")";
        };
    }

    /**
     * Checks if a plan node generates a single SELECT statement that can have
     * additional clauses (ORDER BY, LIMIT) appended without subquery wrapping.
     *
     * <p>This avoids generating unnecessary subqueries like
     * {@code SELECT * FROM (SELECT ... GROUP BY ...) AS subquery_N ORDER BY ...}
     * which break column/table alias scoping. Instead, we inline:
     * {@code SELECT ... GROUP BY ... ORDER BY ...}
     *
     * <p>We must be conservative: only inline when we know the child generates a
     * single SELECT statement AND inlining won't shift the aliasCounter (which would
     * break join alias prediction for DataFrame paths).
     */
    /**
     * Appends an AS alias to the SQL buffer when the Spark-convention column name
     * differs from the DuckDB expression text. This ensures that DuckDB returns
     * column names matching Spark's auto-generated naming convention.
     *
     * <p>Examples:
     * <ul>
     *   <li>count(*) needs AS "count(1)" — DuckDB internally names it "count_star()"</li>
     *   <li>CAST(x AS decimal(15,4)) needs AS "CAST(x AS DECIMAL(15,4))" — Spark uppercases type names</li>
     * </ul>
     *
     * @param expr the expression (for Spark column name computation)
     * @param exprSQL the SQL text already appended to the buffer (to compare against)
     */
    private void appendAutoAlias(Expression expr, String exprSQL) {
        // Don't add auto-alias for RawSQLExpression that already contains an embedded alias.
        // e.g., "string_split(name, ' ') as name_parts" — DuckDB will use 'name_parts'.
        if (expr instanceof com.thunderduck.expression.RawSQLExpression) {
            return;
        }
        String sparkName = Project.buildSparkColumnName(expr);
        if (sparkName != null && !sparkName.equals(exprSQL)) {
            sql.append(" AS ");
            sql.append(SQLQuoting.quoteIdentifier(sparkName));
        }
    }

    /**
     * Appends an alias for a projection expression. Handles three cases:
     * <ol>
     *   <li>Explicit alias provided → always use it</li>
     *   <li>No alias, but expression is AliasExpression/UnresolvedColumn/StarExpression → skip</li>
     *   <li>No alias, computed expression → add auto-alias if Spark name differs from SQL</li>
     * </ol>
     */
    private void maybeAppendAutoAlias(Expression expr, String exprSQL, String alias) {
        if (alias != null && !alias.isEmpty() && !(expr instanceof AliasExpression)) {
            sql.append(" AS ");
            sql.append(SQLQuoting.quoteIdentifier(alias));
        } else if (alias == null && !(expr instanceof AliasExpression)
                && !(expr instanceof com.thunderduck.expression.UnresolvedColumn)
                && !(expr instanceof com.thunderduck.expression.StarExpression)) {
            appendAutoAlias(expr, exprSQL);
        }
    }

    private boolean canAppendClause(LogicalPlan plan) {
        // Aggregate: always safe -- generates SELECT ... FROM ... GROUP BY ...
        if (plan instanceof Aggregate) return true;

        // Sort: safe if its child is appendable. This allows Limit(Sort(Filter(Join)))
        // to produce "SELECT ... ORDER BY ... LIMIT N" without wrapping in
        // "SELECT * FROM (...) AS alias LIMIT N", which would trigger DuckDB's
        // column deduplication for duplicate column names in cross joins.
        if (plan instanceof Sort s) {
            return canAppendClause(s.child());
        }

        // Project on Filter/Join or direct Join: generates flat SQL with table aliases
        // that need to stay visible for ORDER BY/LIMIT.
        if (plan instanceof Project p) {
            LogicalPlan child = p.child();
            // Walk through stacked Filters to find a Join underneath
            if (child instanceof Filter) {
                LogicalPlan current = child;
                while (current instanceof Filter f) {
                    current = f.child();
                }
                if (current instanceof Join) return true;
            }
            if (child instanceof Join) return true;
        }

        // Filter on Join: generates flat SQL (SELECT * FROM ... WHERE ...)
        // with table aliases that must stay visible for ORDER BY/LIMIT.
        // Handles the case where SELECT * is optimized away by the parser,
        // leaving Filter(Join) without a wrapping Project (e.g., CTE body).
        if (plan instanceof Filter) {
            LogicalPlan current = plan;
            while (current instanceof Filter f) {
                current = f.child();
            }
            if (current instanceof Join) return true;
        }

        return false;
    }

    /**
     * Visits a Union node.
     * Builds SQL directly in buffer.
     * Wraps children in parentheses for correct precedence in chained operations.
     *
     * <p>When left and right sides have different but compatible column types
     * (e.g., INT vs BIGINT), wraps each side in a SELECT with CASTs to the
     * widened type. This matches Spark's UNION type coercion behavior.
     */
    private void visitUnion(Union plan) {
        if (plan.byName()) {
            visitUnionByName(plan);
            return;
        }

        StructType leftSchema = plan.left().schema();
        StructType rightSchema = plan.right().schema();
        StructType widenedSchema = plan.schema();

        // Check if type widening is needed
        boolean needsCasts = false;
        if (leftSchema != null && rightSchema != null && widenedSchema != null
                && leftSchema.size() == rightSchema.size()) {
            for (int i = 0; i < leftSchema.size(); i++) {
                DataType leftType = leftSchema.fieldAt(i).dataType();
                DataType rightType = rightSchema.fieldAt(i).dataType();
                if (!leftType.equals(rightType)) {
                    needsCasts = true;
                    break;
                }
            }
        }

        if (needsCasts) {
            // Left side with CAST wrapping
            sql.append("(");
            appendUnionSideWithCasts(plan.left(), leftSchema, widenedSchema);
            sql.append(")");
        } else {
            // Left side (no casts needed)
            sql.append("(");
            visit(plan.left());
            sql.append(")");
        }

        // UNION operator
        if (plan.all()) {
            sql.append(" UNION ALL ");
        } else {
            sql.append(" UNION ");
        }

        if (needsCasts) {
            // Right side with CAST wrapping
            sql.append("(");
            appendUnionSideWithCasts(plan.right(), rightSchema, widenedSchema);
            sql.append(")");
        } else {
            // Right side (no casts needed)
            sql.append("(");
            visit(plan.right());
            sql.append(")");
        }
    }

    /**
     * Appends a UNION side wrapped in a SELECT with CASTs to widened types.
     *
     * <p>Generates: {@code SELECT CAST(col1 AS BIGINT) AS col1, col2, ... FROM (child)}
     * Only adds CAST for columns whose type differs from the widened type.
     *
     * @param child the child plan to wrap
     * @param childSchema the child's original schema
     * @param widenedSchema the target widened schema
     */
    private void appendUnionSideWithCasts(LogicalPlan child, StructType childSchema,
                                           StructType widenedSchema) {
        sql.append("SELECT ");
        for (int i = 0; i < childSchema.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            StructField childField = childSchema.fieldAt(i);
            StructField widenedField = widenedSchema.fieldAt(i);
            String quotedName = quoteIdentifier(childField.name());

            if (!childField.dataType().equals(widenedField.dataType())) {
                // Cast to widened type
                String duckDBType = TypeMapper.toDuckDBType(widenedField.dataType());
                sql.append("CAST(").append(quotedName).append(" AS ").append(duckDBType)
                   .append(") AS ").append(quotedName);
            } else {
                sql.append(quotedName);
            }
        }
        sql.append(" FROM (");
        visit(child);
        sql.append(")");
    }

    /**
     * Visits a Union node with byName=true.
     * Reorders right side columns to match left side column order by name.
     */
    private void visitUnionByName(Union plan) {
        StructType leftSchema = plan.left().schema();
        StructType rightSchema = plan.right().schema();

        if (leftSchema == null || rightSchema == null) {
            throw new UnsupportedOperationException(
                "unionByName requires schema information. Ensure both DataFrames have known schemas.");
        }

        // Left side (wrapped for precedence)
        sql.append("(");
        visit(plan.left());
        sql.append(")");

        // UNION operator
        if (plan.all()) {
            sql.append(" UNION ALL ");
        } else {
            sql.append(" UNION ");
        }

        // Right side: SELECT columns in left schema order FROM (right)
        sql.append("(SELECT ");

        // Extract field names from schemas
        List<String> leftColNames = new ArrayList<>();
        for (int i = 0; i < leftSchema.size(); i++) {
            leftColNames.add(leftSchema.fieldAt(i).name());
        }
        List<String> rightColNames = new ArrayList<>();
        for (int i = 0; i < rightSchema.size(); i++) {
            rightColNames.add(rightSchema.fieldAt(i).name());
        }

        for (int i = 0; i < leftColNames.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String leftColName = leftColNames.get(i);

            // Check if right side has this column (case-insensitive match)
            String matchedRightCol = findMatchingColumn(leftColName, rightColNames);

            if (matchedRightCol != null) {
                // Column exists in right side, select it
                sql.append(SQLQuoting.quoteIdentifier(matchedRightCol));
                // Alias to left column name if case differs
                if (!matchedRightCol.equals(leftColName)) {
                    sql.append(" AS ");
                    sql.append(SQLQuoting.quoteIdentifier(leftColName));
                }
            } else if (plan.allowMissingColumns()) {
                // Column missing in right side, fill with NULL
                sql.append("NULL AS ");
                sql.append(SQLQuoting.quoteIdentifier(leftColName));
            } else {
                throw new IllegalArgumentException(
                    "Column '" + leftColName + "' not found in right DataFrame. " +
                    "Use allowMissingColumns=true to fill missing columns with NULL.");
            }
        }

        sql.append(" FROM (");
        visit(plan.right());
        sql.append(") AS _union_by_name)");
    }

    /**
     * Finds a matching column name in the list (case-insensitive).
     *
     * @param target the column name to find
     * @param candidates the list of candidate column names
     * @return the matching column name, or null if not found
     */
    private String findMatchingColumn(String target, List<String> candidates) {
        // First try exact match
        for (String candidate : candidates) {
            if (candidate.equals(target)) {
                return candidate;
            }
        }
        // Then try case-insensitive match
        for (String candidate : candidates) {
            if (candidate.equalsIgnoreCase(target)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Visits an Intersect node.
     * Builds SQL directly in buffer.
     * Wraps children in parentheses for correct precedence in chained operations.
     */
    private void visitIntersect(Intersect plan) {
        // Left side (wrapped for precedence)
        sql.append("(");
        visit(plan.left());
        sql.append(")");

        // INTERSECT operator
        if (plan.distinct()) {
            sql.append(" INTERSECT ");
        } else {
            sql.append(" INTERSECT ALL ");
        }

        // Right side (wrapped for precedence)
        sql.append("(");
        visit(plan.right());
        sql.append(")");
    }

    /**
     * Visits an Except node.
     * Builds SQL directly in buffer.
     * Wraps children in parentheses for correct precedence in chained operations.
     */
    private void visitExcept(Except plan) {
        // Left side (wrapped for precedence)
        sql.append("(");
        visit(plan.left());
        sql.append(")");

        // EXCEPT operator
        if (plan.distinct()) {
            sql.append(" EXCEPT ");
        } else {
            sql.append(" EXCEPT ALL ");
        }

        // Right side (wrapped for precedence)
        sql.append("(");
        visit(plan.right());
        sql.append(")");
    }

    /**
     * Visits an InMemoryRelation node.
     */
    private void visitInMemoryRelation(InMemoryRelation plan) {
        // InMemoryRelation support - will be fully implemented in Week 3
        throw new UnsupportedOperationException(
            "InMemoryRelation SQL generation will be implemented in Week 3");
    }

    /**
     * Visits a LocalRelation node.
     */
    private void visitLocalRelation(LocalRelation plan) {
        // LocalRelation support - will be fully implemented in Week 3
        throw new UnsupportedOperationException(
            "LocalRelation SQL generation will be implemented in Week 3");
    }

    /**
     * Visits a LocalDataRelation node (Arrow IPC data).
     * Deserializes Arrow data and generates VALUES clause or temp table.
     */
    private void visitLocalDataRelation(LocalDataRelation plan) {
        // Deserialize Arrow data
        org.apache.arrow.vector.VectorSchemaRoot root = plan.deserializeArrowData();

        if (root == null || root.getRowCount() == 0) {
            // Empty relation - generate a VALUES clause that returns no rows
            // Use schema string if available to preserve column names
            sql.append(generateEmptyValues(plan.getSchemaString()));
        } else if (root.getRowCount() <= 100) {
            // Small dataset - use VALUES clause
            sql.append(generateValuesClause(root));
        } else {
            // Larger dataset - for now, fall back to VALUES (can be optimized with temp tables later)
            sql.append(generateValuesClause(root));
        }
    }

    /**
     * Generates an empty VALUES clause that returns no rows, preserving schema.
     * Example with schema: SELECT CAST(NULL AS INT) AS "id", CAST(NULL AS STRING) AS "name" WHERE FALSE
     * Example without schema: SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE
     *
     * @param schemaStr optional DDL schema string (e.g., "id INT, name STRING")
     * @return SQL for empty values with proper column names
     */
    public String generateEmptyValues(String schemaStr) {
        if (schemaStr == null || schemaStr.isEmpty()) {
            return "SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE";
        }

        // Try JSON format first (used by PySpark 4.0+)
        List<String[]> columns = null;
        if (schemaStr.trim().startsWith("{")) {
            columns = parseJSONSchema(schemaStr);
        }

        // Fall back to DDL format like "id INT, name STRING"
        if (columns == null || columns.isEmpty()) {
            columns = parseDDLSchema(schemaStr);
        }

        if (columns == null || columns.isEmpty()) {
            return "SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE";
        }

        StringBuilder result = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                result.append(", ");
            }
            String colName = columns.get(i)[0];
            String colType = columns.get(i)[1];
            // Generate CAST(NULL AS type) AS "colName" for each column
            result.append("CAST(NULL AS ").append(mapSparkTypeToDuckDB(colType))
                  .append(") AS ").append(quoteIdentifier(colName));
        }
        result.append(" WHERE FALSE");
        return result.toString();
    }

    /**
     * Parses a JSON schema string into column name/type pairs.
     * Handles format like: {"fields":[{"name":"id","type":"integer"},{"name":"name","type":"string"}],"type":"struct"}
     *
     * @param schemaStr the JSON schema string
     * @return list of [name, type] arrays, or null if parsing fails
     */
    private List<String[]> parseJSONSchema(String schemaStr) {
        List<String[]> columns = new ArrayList<>();
        try {
            // Simple JSON parsing without external library
            // Look for "fields" array
            int fieldsIdx = schemaStr.indexOf("\"fields\"");
            if (fieldsIdx < 0) {
                return null;
            }

            // Find the fields array start
            int arrayStart = schemaStr.indexOf('[', fieldsIdx);
            if (arrayStart < 0) {
                return null;
            }

            // Find matching end bracket
            int depth = 1;
            int arrayEnd = arrayStart + 1;
            while (arrayEnd < schemaStr.length() && depth > 0) {
                char c = schemaStr.charAt(arrayEnd);
                if (c == '[') depth++;
                else if (c == ']') depth--;
                arrayEnd++;
            }

            String fieldsArray = schemaStr.substring(arrayStart + 1, arrayEnd - 1);

            // Parse each field object - handle nested braces
            int objStart = 0;
            while ((objStart = fieldsArray.indexOf('{', objStart)) >= 0) {
                // Find matching close brace (accounting for nested objects)
                int braceDepth = 1;
                int objEnd = objStart + 1;
                while (objEnd < fieldsArray.length() && braceDepth > 0) {
                    char c = fieldsArray.charAt(objEnd);
                    if (c == '{') braceDepth++;
                    else if (c == '}') braceDepth--;
                    objEnd++;
                }
                if (braceDepth != 0) break; // Unbalanced braces

                String fieldObj = fieldsArray.substring(objStart, objEnd);

                // Extract name
                String name = extractJSONValue(fieldObj, "name");
                // Extract type
                String type = extractJSONValue(fieldObj, "type");

                if (name != null && type != null) {
                    columns.add(new String[]{name, type});
                }

                objStart = objEnd;
            }

            return columns.isEmpty() ? null : columns;
        } catch (Exception e) {
            // Parsing failed, return null to fall back to DDL parsing
            return null;
        }
    }

    /**
     * Extracts a string value from a JSON object.
     */
    private String extractJSONValue(String json, String key) {
        String searchKey = "\"" + key + "\"";
        int keyIdx = json.indexOf(searchKey);
        if (keyIdx < 0) return null;

        int colonIdx = json.indexOf(':', keyIdx);
        if (colonIdx < 0) return null;

        // Skip whitespace
        int valueStart = colonIdx + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }

        if (valueStart >= json.length()) return null;

        char startChar = json.charAt(valueStart);
        if (startChar == '"') {
            // String value
            int valueEnd = json.indexOf('"', valueStart + 1);
            if (valueEnd < 0) return null;
            return json.substring(valueStart + 1, valueEnd);
        } else {
            // Non-string value (shouldn't happen for name/type)
            return null;
        }
    }

    /**
     * Parses a DDL schema string into column name/type pairs.
     * Handles formats like "id INT, name STRING" or "id INT NOT NULL, name STRING"
     *
     * @param schemaStr the DDL schema string
     * @return list of [name, type] arrays
     */
    private List<String[]> parseDDLSchema(String schemaStr) {
        List<String[]> columns = new ArrayList<>();
        if (schemaStr == null || schemaStr.isEmpty()) {
            return columns;
        }

        // Split by comma, but handle nested types like STRUCT<...> or ARRAY<...>
        int depth = 0;
        int start = 0;
        for (int i = 0; i <= schemaStr.length(); i++) {
            char c = (i < schemaStr.length()) ? schemaStr.charAt(i) : ',';
            if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                String part = schemaStr.substring(start, i).trim();
                String[] parsed = parseColumnDef(part);
                if (parsed != null) {
                    columns.add(parsed);
                }
                start = i + 1;
            }
        }
        return columns;
    }

    /**
     * Parses a single column definition like "id INT" or "name STRING NOT NULL"
     *
     * @param colDef the column definition
     * @return [name, type] array or null if unparseable
     */
    private String[] parseColumnDef(String colDef) {
        if (colDef == null || colDef.isEmpty()) {
            return null;
        }

        // Handle backtick-quoted names like `my column`
        String name;
        String rest;
        if (colDef.startsWith("`")) {
            int endQuote = colDef.indexOf('`', 1);
            if (endQuote < 0) {
                return null;
            }
            name = colDef.substring(1, endQuote);
            rest = colDef.substring(endQuote + 1).trim();
        } else {
            // Split on first whitespace
            int spaceIdx = colDef.indexOf(' ');
            if (spaceIdx < 0) {
                return null;
            }
            name = colDef.substring(0, spaceIdx);
            rest = colDef.substring(spaceIdx + 1).trim();
        }

        // Extract type (remove NOT NULL if present)
        String type = rest.replaceAll("(?i)\\s+NOT\\s+NULL\\s*$", "").trim();
        if (type.isEmpty()) {
            return null;
        }

        return new String[]{name, type};
    }

    /**
     * Maps Spark SQL types to DuckDB types.
     *
     * @param sparkType the Spark type string
     * @return DuckDB-compatible type string
     */
    private String mapSparkTypeToDuckDB(String sparkType) {
        if (sparkType == null) {
            return "VARCHAR";
        }
        String upper = sparkType.toUpperCase().trim();
        switch (upper) {
            case "STRING":
                return "VARCHAR";
            case "INTEGER":
            case "INT":
                return "INTEGER";
            case "LONG":
            case "BIGINT":
                return "BIGINT";
            case "SHORT":
            case "SMALLINT":
                return "SMALLINT";
            case "BYTE":
            case "TINYINT":
                return "TINYINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "BOOLEAN":
                return "BOOLEAN";
            case "DATE":
                return "DATE";
            case "TIMESTAMP":
                return "TIMESTAMP";
            case "BINARY":
                return "BLOB";
            default:
                // For complex types or unknown types, return as-is
                return sparkType;
        }
    }

    /**
     * Generates a VALUES clause from Arrow VectorSchemaRoot.
     * Example: VALUES (1, 'a'), (2, 'b'), (3, 'c')
     *
     * @param root the Arrow VectorSchemaRoot containing data
     * @return SQL VALUES clause
     */
    public String generateValuesClause(org.apache.arrow.vector.VectorSchemaRoot root) {
        if (root == null || root.getRowCount() == 0) {
            return generateEmptyValues(null);
        }

        StringBuilder values = new StringBuilder("SELECT * FROM (VALUES ");

        int rowCount = root.getRowCount();
        int columnCount = root.getFieldVectors().size();

        for (int row = 0; row < rowCount; row++) {
            if (row > 0) {
                values.append(", ");
            }
            values.append("(");

            for (int col = 0; col < columnCount; col++) {
                if (col > 0) {
                    values.append(", ");
                }

                org.apache.arrow.vector.FieldVector vector = root.getVector(col);
                org.apache.arrow.vector.types.pojo.ArrowType arrowType =
                    root.getSchema().getFields().get(col).getType();
                Object value = getArrowValue(vector, row);
                values.append(formatTypedSQLValue(value, arrowType));
            }

            values.append(")");
        }

        values.append(") AS t");

        // Add column aliases if available
        if (columnCount > 0) {
            values.append("(");
            for (int col = 0; col < columnCount; col++) {
                if (col > 0) {
                    values.append(", ");
                }
                String columnName = root.getSchema().getFields().get(col).getName();
                values.append(quoteIdentifier(columnName));
            }
            values.append(")");
        }

        return values.toString();
    }

    /**
     * Gets a value from an Arrow vector at the specified row index.
     *
     * @param vector the Arrow field vector
     * @param index the row index
     * @return the value (may be null)
     */
    private Object getArrowValue(org.apache.arrow.vector.FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }

        if (vector instanceof org.apache.arrow.vector.BitVector v) {
            return v.get(index) != 0;
        } else if (vector instanceof org.apache.arrow.vector.TinyIntVector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.SmallIntVector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.IntVector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.BigIntVector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.Float4Vector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.Float8Vector v) {
            return v.get(index);
        } else if (vector instanceof org.apache.arrow.vector.VarCharVector v) {
            byte[] bytes = v.get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        } else if (vector instanceof org.apache.arrow.vector.DateDayVector v) {
            int days = v.get(index);
            return java.time.LocalDate.ofEpochDay(days);
        } else if (vector instanceof org.apache.arrow.vector.DateMilliVector v) {
            // Date stored as milliseconds since epoch
            long millis = v.get(index);
            return java.time.LocalDate.ofEpochDay(millis / (24 * 60 * 60 * 1000L));
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroTZVector v) {
            // Timestamp with timezone in microseconds
            long micros = v.get(index);
            return java.time.Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroVector v) {
            // Timestamp without timezone in microseconds
            long micros = v.get(index);
            return java.time.Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMilliTZVector v) {
            // Timestamp with timezone in milliseconds
            long millis = v.get(index);
            return java.time.Instant.ofEpochMilli(millis);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMilliVector v) {
            // Timestamp without timezone in milliseconds
            long millis = v.get(index);
            return java.time.Instant.ofEpochMilli(millis);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoTZVector v) {
            // Timestamp with timezone in nanoseconds
            long nanos = v.get(index);
            return java.time.Instant.ofEpochSecond(nanos / 1_000_000_000, nanos % 1_000_000_000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoVector v) {
            // Timestamp without timezone in nanoseconds
            long nanos = v.get(index);
            return java.time.Instant.ofEpochSecond(nanos / 1_000_000_000, nanos % 1_000_000_000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampSecTZVector v) {
            // Timestamp with timezone in seconds
            long secs = v.get(index);
            return java.time.Instant.ofEpochSecond(secs);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampSecVector v) {
            // Timestamp without timezone in seconds
            long secs = v.get(index);
            return java.time.Instant.ofEpochSecond(secs);
        } else if (vector instanceof org.apache.arrow.vector.complex.MapVector mapVector) {
            // IMPORTANT: MapVector extends ListVector, so this check MUST come before
            // the ListVector check. MapVector.getObject() returns List<JsonStringHashMap>
            // where each entry has "key" and "value" fields. Convert to a proper
            // java.util.Map so formatSQLValue produces MAP([k1,k2], [v1,v2]) syntax
            // instead of [{...}, ...] array-of-struct syntax (which DuckDB interprets
            // as MAP(K,V)[] rather than MAP(K,V)).
            Object raw = mapVector.getObject(index);
            if (raw instanceof java.util.List<?> entries) {
                java.util.LinkedHashMap<Object, Object> result = new java.util.LinkedHashMap<>();
                for (Object entry : entries) {
                    if (entry instanceof java.util.Map<?, ?> entryMap) {
                        result.put(entryMap.get("key"), entryMap.get("value"));
                    }
                }
                return result;
            }
            return raw;
        } else if (vector instanceof org.apache.arrow.vector.complex.ListVector listVector) {
            // Handle array/list types - return as List to preserve structure
            return listVector.getObject(index);  // Returns java.util.List
        }

        // Fallback: try to get object representation
        return vector.getObject(index);
    }

    /**
     * Formats a value for SQL (with proper quoting/escaping).
     *
     * @param value the value to format
     * @return SQL representation of the value
     */
    private String formatSQLValue(Object value) {
        if (value == null) {
            return "NULL";
        }

        if (value instanceof String str) {
            // Escape single quotes by doubling them
            String escaped = str.replace("'", "''");
            return "'" + escaped + "'";
        } else if (value instanceof Number num) {
            // Handle special float/double values (NaN, Infinity)
            if (value instanceof Double d) {
                if (d.isNaN()) return "'NaN'::DOUBLE";
                if (d.isInfinite()) return d > 0 ? "'Infinity'::DOUBLE" : "'-Infinity'::DOUBLE";
            } else if (value instanceof Float f) {
                if (f.isNaN()) return "'NaN'::FLOAT";
                if (f.isInfinite()) return f > 0 ? "'Infinity'::FLOAT" : "'-Infinity'::FLOAT";
            }
            return num.toString();
        } else if (value instanceof Boolean bool) {
            return bool ? "TRUE" : "FALSE";
        } else if (value instanceof java.time.LocalDate) {
            return "DATE '" + value.toString() + "'";
        } else if (value instanceof java.sql.Date) {
            return "DATE '" + value.toString() + "'";
        } else if (value instanceof java.sql.Timestamp) {
            return "TIMESTAMP '" + value.toString() + "'";
        } else if (value instanceof java.time.Instant instant) {
            // Format Instant as TIMESTAMP literal
            java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
            return "TIMESTAMP '" + ldt.toString().replace("T", " ") + "'";
        } else if (value instanceof java.time.LocalDateTime) {
            // Format LocalDateTime as TIMESTAMP literal
            return "TIMESTAMP '" + value.toString().replace("T", " ") + "'";
        } else if (value instanceof java.util.List<?> list) {
            // Format as DuckDB array literal: [elem1, elem2, ...]
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(formatSQLValue(list.get(i)));
            }
            sb.append("]");
            return sb.toString();
        } else if (value instanceof java.util.Map<?, ?> map) {
            // Format as DuckDB map literal: MAP([keys], [values])
            StringBuilder keys = new StringBuilder("[");
            StringBuilder values = new StringBuilder("[");
            boolean first = true;
            for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    keys.append(", ");
                    values.append(", ");
                }
                keys.append(formatSQLValue(entry.getKey()));
                values.append(formatSQLValue(entry.getValue()));
                first = false;
            }
            keys.append("]");
            values.append("]");
            return "MAP(" + keys + ", " + values + ")";
        } else if (value instanceof org.apache.arrow.vector.util.JsonStringHashMap<?, ?> arrowMap) {
            // Arrow returns maps as JsonStringHashMap (key-value struct entries)
            // Format as DuckDB map literal
            Object key = arrowMap.get("key");
            Object val = arrowMap.get("value");
            // This is a single entry from a map - should be handled by the List case above
            // Just format as a struct-like value
            return "{'key': " + formatSQLValue(key) + ", 'value': " + formatSQLValue(val) + "}";
        } else {
            // Fallback: convert to string and quote
            String escaped = value.toString().replace("'", "''");
            return "'" + escaped + "'";
        }
    }

    /**
     * Formats a value with explicit CAST to preserve Arrow schema type.
     * This ensures DuckDB uses the correct type instead of inferring from literals.
     *
     * @param value the value to format
     * @param type the Arrow type to cast to
     * @return SQL representation with CAST wrapper for numeric types
     */
    private String formatTypedSQLValue(Object value, org.apache.arrow.vector.types.pojo.ArrowType type) {
        if (value == null) {
            return "NULL"; // NULL doesn't need CAST
        }

        // Handle Map type: Arrow MapVector returns List<JsonStringHashMap> which must
        // be formatted as MAP(['key1', 'key2'], [val1, val2]) instead of an array literal.
        if (type.getTypeID() == org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Map
                && value instanceof java.util.List<?> entries) {
            return formatMapFromEntries(entries);
        }

        String formatted = formatSQLValue(value);

        // Only wrap numeric types that DuckDB might infer incorrectly
        switch (type.getTypeID()) {
            case Int:
            case FloatingPoint:
            case Decimal:
                String sqlType = com.thunderduck.runtime.ArrowInterchange.arrowTypeToSQLType(type);
                return "CAST(" + formatted + " AS " + sqlType + ")";
            default:
                return formatted; // Other types (String, Date, etc.) are fine as-is
        }
    }

    /**
     * Formats a list of map entries (from Arrow MapVector) as a DuckDB MAP literal.
     *
     * <p>Arrow MapVector returns each row as a List of JsonStringHashMap entries,
     * each with "key" and "value" fields. This formats them as MAP([keys], [values]).
     *
     * @param entries the list of map entries from Arrow
     * @return SQL MAP literal
     */
    private String formatMapFromEntries(java.util.List<?> entries) {
        if (entries.isEmpty()) {
            return "MAP([], [])";
        }

        StringBuilder keys = new StringBuilder("[");
        StringBuilder vals = new StringBuilder("[");
        boolean first = true;

        for (Object entry : entries) {
            if (!first) {
                keys.append(", ");
                vals.append(", ");
            }
            first = false;

            if (entry instanceof java.util.Map<?, ?> map) {
                keys.append(formatSQLValue(map.get("key")));
                vals.append(formatSQLValue(map.get("value")));
            } else {
                // Fallback: shouldn't happen for well-formed Arrow data
                keys.append("NULL");
                vals.append("NULL");
            }
        }

        keys.append("]");
        vals.append("]");
        return "MAP(" + keys + ", " + vals + ")";
    }

    /**
     * Generates a unique subquery alias.
     *
     * @return a unique alias like "subquery_1", "subquery_2", etc.
     */
    public String generateSubqueryAlias() {
        return "subquery_" + (++aliasCounter);
    }

    /**
     * Generates the SELECT clause for a join.
     *
     * <p>For USING joins, generates an explicit column list to deduplicate
     * the join columns. For regular joins (ON clause), returns "SELECT *".
     *
     * <p>For RIGHT and FULL outer joins with USING columns, we use COALESCE
     * to ensure the join column value comes from whichever side has a non-NULL
     * value, since the left side may be NULL.
     *
     * @param plan the Join plan
     * @param leftAlias the alias for the left side
     * @param rightAlias the alias for the right side
     * @return the SELECT clause (without trailing space)
     */
    private String generateJoinSelectClause(Join plan, String leftAlias, String rightAlias) {
        List<String> usingColumns = plan.usingColumns();

        // For non-USING joins, just return SELECT *
        if (usingColumns == null || usingColumns.isEmpty()) {
            return "SELECT *";
        }

        // Get schemas for building explicit column list
        com.thunderduck.types.StructType leftSchema = plan.left().schema();
        com.thunderduck.types.StructType rightSchema = plan.right().schema();

        // If schemas aren't available or empty, fall back to SELECT *
        if (leftSchema == null || rightSchema == null ||
            leftSchema.fields().isEmpty() || rightSchema.fields().isEmpty()) {
            return "SELECT *";
        }

        // Build explicit column list for USING joins
        // Spark column order: USING columns first, then non-USING left, then non-USING right
        Set<String> usingSet = new HashSet<>(usingColumns);
        List<String> selectItems = new ArrayList<>();

        // Determine if we need COALESCE for USING columns (RIGHT/FULL outer joins)
        boolean needsCoalesce = (plan.joinType() == Join.JoinType.RIGHT ||
                                 plan.joinType() == Join.JoinType.FULL);

        // 1. First, add USING columns (in the order they appear in usingColumns list)
        for (String usingCol : usingColumns) {
            String quotedName = SQLQuoting.quoteIdentifier(usingCol);
            if (needsCoalesce) {
                // For RIGHT/FULL joins, use COALESCE to get value from whichever side has it
                selectItems.add(String.format("COALESCE(%s.%s, %s.%s) AS %s",
                    leftAlias, quotedName, rightAlias, quotedName, quotedName));
            } else {
                selectItems.add(leftAlias + "." + quotedName);
            }
        }

        // 2. Add non-USING columns from left side
        for (com.thunderduck.types.StructField field : leftSchema.fields()) {
            if (!usingSet.contains(field.name())) {
                selectItems.add(leftAlias + "." + SQLQuoting.quoteIdentifier(field.name()));
            }
        }

        // 3. Add non-USING columns from right side
        for (com.thunderduck.types.StructField field : rightSchema.fields()) {
            if (!usingSet.contains(field.name())) {
                selectItems.add(rightAlias + "." +
                    SQLQuoting.quoteIdentifier(field.name()));
            }
        }

        return "SELECT " + String.join(", ", selectItems);
    }

    /**
     * Resolves struct types for UpdateFieldsExpression(DROP) operations.
     * The expression may be directly an UpdateFieldsExpression or wrapped in AliasExpression.
     */
    private void resolveDropFieldsStructType(Expression expr, StructType schema) {
        if (expr instanceof UpdateFieldsExpression update) {
            update.resolveStructType(schema);
        } else if (expr instanceof AliasExpression alias) {
            resolveDropFieldsStructType(alias.expression(), schema);
        }
    }

    /**
     * Resolves polymorphic function names based on argument types from the child schema.
     *
     * <p>Some Spark functions (like {@code reverse}) work on both strings and arrays,
     * but DuckDB requires different function names: {@code reverse()} for strings
     * and {@code list_reverse()} for arrays. This method resolves the correct function
     * name by checking the argument type against the child schema.
     *
     * @param expr the expression to resolve
     * @param childSchema the child schema for type resolution (may be null)
     * @return the expression with resolved function names, or the original if no resolution needed
     */
    private Expression resolvePolymorphicFunctions(Expression expr, StructType childSchema) {
        if (childSchema == null) {
            return expr;
        }

        // Recursively process wrapper expressions to resolve polymorphic functions
        // inside complex expression trees (e.g., filter conditions like size(map_col) > 0)
        if (expr instanceof AliasExpression ae) {
            Expression resolved = resolvePolymorphicFunctions(ae.expression(), childSchema);
            if (resolved != ae.expression()) {
                return new AliasExpression(resolved, ae.alias());
            }
            return expr;
        }

        if (expr instanceof com.thunderduck.expression.BinaryExpression be) {
            Expression resolvedLeft = resolvePolymorphicFunctions(be.left(), childSchema);
            Expression resolvedRight = resolvePolymorphicFunctions(be.right(), childSchema);
            if (resolvedLeft != be.left() || resolvedRight != be.right()) {
                return new com.thunderduck.expression.BinaryExpression(
                    resolvedLeft, be.operator(), resolvedRight);
            }
            return expr;
        }

        if (expr instanceof com.thunderduck.expression.CastExpression ce) {
            Expression resolvedExpr = resolvePolymorphicFunctions(ce.expression(), childSchema);
            if (resolvedExpr != ce.expression()) {
                return new com.thunderduck.expression.CastExpression(resolvedExpr, ce.targetType());
            }
            return expr;
        }

        if (!(expr instanceof FunctionCall fc)) {
            return expr;
        }

        String funcName = fc.functionName().toLowerCase();

        // Dispatch reverse -> list_reverse for array arguments
        if (funcName.equals("reverse") && fc.argumentCount() == 1) {
            Expression arg = fc.arguments().get(0);
            DataType argType = TypeInferenceEngine.resolveType(arg, childSchema);
            if (argType instanceof ArrayType) {
                return new FunctionCall("list_reverse", fc.arguments(), fc.dataType(), fc.nullable());
            }
        }

        // element_at: keep as element_at for MAP arguments (DuckDB native),
        // list_extract only works for arrays.
        // Note: functionName() returns the original Spark name "element_at",
        // not the translated name "list_extract" -- translation happens inside toSQL().
        if (funcName.equals("element_at") && fc.argumentCount() == 2) {
            Expression arg = fc.arguments().get(0);
            DataType argType = TypeInferenceEngine.resolveType(arg, childSchema);
            if (argType instanceof MapType) {
                // DuckDB's element_at(map, key) returns an array (list of matching values).
                // Spark's element_at(map, key) returns a scalar value.
                // Use bracket notation map[key] which returns a scalar in DuckDB.
                String mapElementSql = arg.toSQL() + "[" + fc.arguments().get(1).toSQL() + "]";
                return createVerbatimSQLExpression(mapElementSql, fc.dataType(), fc.nullable());
            }
        }

        // explode on MAP: Spark produces two columns (key, value),
        // DuckDB's unnest on MAP doesn't produce the same structure.
        // Use unnest(map_keys(...)), unnest(map_values(...)) to match Spark behavior.
        if (funcName.equals("explode") && fc.argumentCount() == 1) {
            Expression arg = fc.arguments().get(0);
            DataType argType = TypeInferenceEngine.resolveType(arg, childSchema);
            if (argType instanceof MapType) {
                String argSQL = arg.toSQL();
                String mapExplodeSql = "unnest(map_keys(" + argSQL + ")) AS \"key\", " +
                    "unnest(map_values(" + argSQL + ")) AS \"value\"";
                return createVerbatimSQLExpression(mapExplodeSql, fc.dataType(), fc.nullable());
            }
        }

        // size: use cardinality() for MAP arguments (len() is array-only in DuckDB)
        if (funcName.equals("size") && fc.argumentCount() == 1) {
            Expression arg = fc.arguments().get(0);
            DataType argType = TypeInferenceEngine.resolveType(arg, childSchema);
            if (argType instanceof MapType) {
                return createVerbatimSQLExpression(
                    "CAST(cardinality(" + arg.toSQL() + ") AS INTEGER)",
                    fc.dataType(), fc.nullable());
            }
        }

        return expr;
    }

    /**
     * Creates an Expression that returns the given SQL verbatim from toSQL().
     * Unlike RawSQLExpression, this does NOT pass through FunctionRegistry.rewriteSQL(),
     * so DuckDB-native function names (like element_at for MAPs) are preserved.
     *
     * <p>Extends RawSQLExpression behavior (appendAutoAlias skips it) by implementing
     * the same interface but with direct SQL output.
     */
    private static Expression createVerbatimSQLExpression(String sql, DataType dataType, boolean nullable) {
        // Use an anonymous Expression that returns SQL verbatim.
        // instanceof RawSQLExpression checks in appendAutoAlias won't match,
        // but explicit aliases from the Project handle aliasing correctly.
        return new Expression() {
            @Override
            public String toSQL() {
                return sql;
            }

            @Override
            public DataType dataType() {
                return dataType;
            }

            @Override
            public boolean nullable() {
                return nullable;
            }

            @Override
            public String toString() {
                return "VerbatimSQL(" + sql + ")";
            }
        };
    }
}
