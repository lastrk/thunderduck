package com.thunderduck.generator;

import com.thunderduck.exception.SQLGenerationException;
import com.thunderduck.logical.*;
import com.thunderduck.expression.*;
import com.thunderduck.runtime.SparkCompatMode;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import java.util.*;

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

    private final StringBuilder sql;
    //private final Stack<GenerationContext> contextStack;
    private int aliasCounter;
    private int subqueryDepth;

    /**
     * Creates a new SQL generator.
     */
    public SQLGenerator() {
        this.sql = new StringBuilder();
      //  this.contextStack = new Stack<>();
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

        // State tracking for debugging (can be enabled if needed)
        // System.err.println("DEBUG: generate() called - sql.length()=" + sql.length() +
        //                   ", aliasCounter=" + aliasCounter +
        //                   ", subqueryDepth=" + subqueryDepth +
        //                   ", isRecursive=" + isRecursive);

        // Save state for recursive calls or rollback
        int savedLength = sql.length();
        
        if (!isRecursive) {
            // Top-level call: reset all state
            sql.setLength(0);
            //contextStack.clear();
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
            //contextStack.clear();

            // Re-throw UnsupportedOperationException without wrapping
            // so callers can catch it directly if needed
            throw e;

        } catch (IllegalArgumentException e) {
            // Rollback state
            sql.setLength(savedLength);
            //contextStack.clear();

            // Re-throw IllegalArgumentException without wrapping
            // This includes validation errors from quoting/escaping
            throw e;

        } catch (Exception e) {
            // Rollback state
            sql.setLength(savedLength);
            //contextStack.clear();

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
            case SingleRowRelation sr -> throw new UnsupportedOperationException(
                "SQL generation not implemented for: SingleRowRelation");
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
        if (child instanceof Filter) {
            List<Filter> filters = new java.util.ArrayList<>();
            LogicalPlan current = child;
            while (current instanceof Filter) {
                filters.add((Filter) current);
                current = ((Filter) current).child();
            }
            if (current instanceof Join) {
                generateProjectOnFilterJoin(plan, filters, (Join) current);
                return;
            }
        }

        // Special case: Project directly on Join
        // Generate flat SQL to preserve alias visibility and properly qualify columns
        // This handles cases like: df1.join(df2, cond, "right").select(df1["col"], df2["col"])
        // Without flat SQL, duplicate column names become ambiguous in the wrapped subquery
        if (child instanceof Join) {
            generateProjectOnJoin(plan, (Join) child);
            return;
        }

        sql.append("SELECT ");

        List<Expression> projections = plan.projections();
        List<String> aliases = plan.aliases();

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            sql.append(expr.toSQL());

            // Add alias if provided
            String alias = aliases.get(i);
            if (alias != null && !alias.isEmpty()) {
                sql.append(" AS ");
                // Always quote aliases for security and consistency
                sql.append(SQLQuoting.quoteIdentifier(alias));
            }
        }

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

    /**
     * Generates flat SQL for Project directly on Join.
     * This preserves alias visibility by qualifying column references, needed for
     * correct column resolution when both join sides have columns with the same name.
     */
    private void generateProjectOnJoin(Project project, Join join) {
        // Build plan_id → alias mapping using the correct aliases that will be generated
        Map<Long, String> planIdToAlias = new HashMap<>();
        buildJoinPlanIdMapping(join, planIdToAlias);

        sql.append("SELECT ");

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            // Use qualifyCondition to properly resolve column references
            String qualifiedExpr = qualifyCondition(expr, planIdToAlias);
            sql.append(qualifiedExpr);

            String alias = aliases.get(i);
            if (alias != null && !alias.isEmpty()) {
                // Project.projections() are already unwrapped (AliasExpressions removed),
                // so always add the alias from the separate aliases list
                sql.append(" AS ");
                sql.append(SQLQuoting.quoteIdentifier(alias));
            }
        }

        sql.append(" FROM ");

        // Generate flat join chain (conditions will also be qualified using internal mapping)
        generateFlatJoinChainWithMapping(join, null);
    }

    /**
     * Generates flat SQL for Project on Filter(s) on Join.
     * This preserves alias visibility by not wrapping intermediate results in subqueries,
     * needed for correct column resolution when both join sides have columns with the same name.
     * Handles stacked filters (e.g., Filter→Filter→Join) by combining all conditions with AND.
     */
    private void generateProjectOnFilterJoin(Project project, List<Filter> filters, Join join) {
        // Build plan_id → alias mapping using the correct aliases that will be generated
        Map<Long, String> planIdToAlias = new HashMap<>();
        buildJoinPlanIdMapping(join, planIdToAlias);

        sql.append("SELECT ");

        List<Expression> projections = project.projections();
        List<String> aliases = project.aliases();

        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            // Use qualifyCondition to properly resolve column references
            String qualifiedExpr = qualifyCondition(expr, planIdToAlias);
            sql.append(qualifiedExpr);

            String alias = aliases.get(i);
            if (alias != null && !alias.isEmpty()) {
                // Project.projections() are already unwrapped (AliasExpressions removed),
                // so always add the alias from the separate aliases list
                sql.append(" AS ");
                sql.append(SQLQuoting.quoteIdentifier(alias));
            }
        }

        sql.append(" FROM ");

        // Generate flat join chain (conditions will also be qualified using internal mapping)
        generateFlatJoinChainWithMapping(join, null);

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

        // Collect all sources in order: leftmost, then each join's right side
        List<LogicalPlan> sources = new java.util.ArrayList<>();
        sources.add(leftmost);
        for (JoinPart part : joinParts) {
            sources.add(part.right);
        }

        // Build plan_id → alias mapping by predicting aliases for each source
        // We need to track which subquery number will be used for non-aliased sources
        Map<Long, String> planIdToAlias = new HashMap<>();
        int predictedCounter = aliasCounter;
        for (LogicalPlan source : sources) {
            if (source instanceof AliasedRelation) {
                AliasedRelation aliased = (AliasedRelation) source;
                String alias = SQLQuoting.quoteIdentifier(aliased.alias());
                if (aliased.planId().isPresent()) {
                    planIdToAlias.putIfAbsent(aliased.planId().getAsLong(), alias);
                }
                collectPlanIds(aliased.child(), alias, planIdToAlias);
            } else {
                // Non-aliased source will get the next subquery number
                predictedCounter++;
                String alias = "subquery_" + predictedCounter;
                collectPlanIds(source, alias, planIdToAlias);
            }
        }

        // If caller wants the mapping, populate it
        if (planIdToAliasOut != null) {
            planIdToAliasOut.putAll(planIdToAlias);
        }

        // Generate leftmost table/subquery
        generateJoinSource(leftmost);

        // Generate each join in sequence
        for (JoinPart part : joinParts) {
            sql.append(" ");
            sql.append(getJoinKeyword(part.joinType));
            sql.append(" ");
            generateJoinSource(part.right);
            if (part.condition != null) {
                sql.append(" ON ");
                // Use qualified condition to properly resolve column references
                sql.append(qualifyCondition(part.condition, planIdToAlias));
            }
        }
    }

    /**
     * Collects join parts from a join chain, returning the leftmost non-join plan.
     */
    private LogicalPlan collectJoinParts(Join join, List<JoinPart> parts) {
        LogicalPlan left = join.left();
        if (left instanceof Join) {
            // Recursively collect from nested join
            LogicalPlan leftmost = collectJoinParts((Join) left, parts);
            // Add this join's right side
            parts.add(new JoinPart(join.right(), join.joinType(), join.condition()));
            return leftmost;
        } else {
            // Left is not a join, this is the leftmost table
            // Add this join's right side as the first join
            parts.add(new JoinPart(join.right(), join.joinType(), join.condition()));
            return left;
        }
    }

    /**
     * Generates SQL for a join source (table, aliased relation, or subquery).
     */
    private void generateJoinSource(LogicalPlan plan) {
        if (plan instanceof AliasedRelation) {
            AliasedRelation aliased = (AliasedRelation) plan;
            String childSource = getDirectlyAliasableSource(aliased.child());
            if (childSource != null) {
                sql.append(childSource);
            } else {
                sql.append("(");
                visit(aliased.child());
                sql.append(")");
            }
            sql.append(" AS ");
            sql.append(SQLQuoting.quoteIdentifier(aliased.alias()));
        } else {
            String source = getDirectlyAliasableSource(plan);
            if (source != null) {
                sql.append(source);
                sql.append(" AS ");
                sql.append(generateSubqueryAlias());
            } else {
                sql.append("(");
                visit(plan);
                sql.append(") AS ");
                sql.append(generateSubqueryAlias());
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
     * Visits a Filter node (WHERE clause).
     */
    private void visitFilter(Filter plan) {
        LogicalPlan child = plan.child();

        // For Joins with user-defined aliases (AliasedRelation), use flat join chain
        // to preserve alias visibility. Walk through stacked filters to find the Join.
        {
            List<Filter> filters = new java.util.ArrayList<>();
            filters.add(plan);
            LogicalPlan current = child;
            while (current instanceof Filter) {
                filters.add((Filter) current);
                current = ((Filter) current).child();
            }
            if (current instanceof Join && hasAliasedChildren((Join) current)) {
                Join join = (Join) current;
                Map<Long, String> planIdToAlias = new HashMap<>();
                sql.append("SELECT * FROM ");
                generateFlatJoinChainWithMapping(join, planIdToAlias);
                sql.append(" WHERE ");
                for (int i = 0; i < filters.size(); i++) {
                    if (i > 0) {
                        sql.append(" AND ");
                    }
                    String qualifiedCond = qualifyCondition(filters.get(i).condition(), planIdToAlias);
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
                sql.append(plan.condition().toSQL());
            } else {
                // Complex child - wrap in subquery
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(child);
                subqueryDepth--;
                sql.append(") AS ").append(generateSubqueryAlias());
                sql.append(" WHERE ");
                sql.append(plan.condition().toSQL());
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
            if (source instanceof AliasedRelation) {
                // User-provided alias
                AliasedRelation aliased = (AliasedRelation) source;
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
        if (plan instanceof AliasedRelation) {
            AliasedRelation aliased = (AliasedRelation) plan;
            String alias = SQLQuoting.quoteIdentifier(aliased.alias());
            // Map the AliasedRelation's own plan_id
            if (aliased.planId().isPresent()) {
                mapping.putIfAbsent(aliased.planId().getAsLong(), alias);
            }
            // Also map child plan_ids to this alias
            collectPlanIds(aliased.child(), alias, mapping);
        } else if (plan instanceof Join) {
            // Recursively handle nested joins
            collectJoinPlanIds((Join) plan, mapping);
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
        if (plan instanceof Join) {
            Join j = (Join) plan;
            return hasAliasedRelation(j.left()) || hasAliasedRelation(j.right());
        }
        if (plan instanceof Filter) {
            return hasAliasedRelation(((Filter) plan).child());
        }
        return false;
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
        sql.append(" ORDER BY ");

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

            // Add null ordering
            if (order.nullOrdering() == Sort.NullOrdering.NULLS_FIRST) {
                sql.append(" NULLS FIRST");
            } else {
                sql.append(" NULLS LAST");
            }
        }
    }

    /**
     * Visits a Limit node (LIMIT clause).
     */
    private void visitLimit(Limit plan) {
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
     * Generates expression SQL for a WithColumns expression.
     *
     * <p>In strict mode, {@code BinaryExpression.toSQL()} emits
     * {@code spark_decimal_div()} which returns the correct Spark type natively.
     * In relaxed mode, DuckDB's native types are acceptable. Either way, no
     * CAST wrapper is needed.
     */
    private String generateExpressionWithCast(Expression expr, StructType childSchema) {
        return expr.toSQL();
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

        // SELECT clause with grouping expressions and aggregates
        sql.append("SELECT ");

        java.util.List<String> selectExprs = new java.util.ArrayList<>();

        // Add grouping columns
        for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
            selectExprs.add(expr.toSQL());
        }

        // Add aggregate expressions
        for (Aggregate.AggregateExpression aggExpr : plan.aggregateExpressions()) {
            String aggSQL;

            if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && aggExpr.isComposite()) {
                // Transform aggregate function names in the AST, then render
                com.thunderduck.expression.Expression transformed = transformAggregateExpression(aggExpr.rawExpression());
                aggSQL = transformed.toSQL();
            } else if (com.thunderduck.runtime.SparkCompatMode.isStrictMode() && !aggExpr.isComposite()) {
                aggSQL = aggExpr.toSQL();
                // Normalize function name: strip _DISTINCT suffix for matching
                String baseFuncName = aggExpr.function().toUpperCase();
                if (baseFuncName.endsWith("_DISTINCT")) {
                    baseFuncName = baseFuncName.substring(0, baseFuncName.length() - "_DISTINCT".length());
                }

                if (baseFuncName.equals("SUM")) {
                    aggSQL = aggSQL.replaceFirst("(?i)\\bSUM\\b", "spark_sum");
                } else if (baseFuncName.equals("AVG")) {
                    aggSQL = aggSQL.replaceFirst("(?i)\\bAVG\\b", "spark_avg");
                }
            } else {
                aggSQL = aggExpr.toSQL();
            }

            // Add alias if provided
            if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
                aggSQL += " AS " + SQLQuoting.quoteIdentifier(aggExpr.alias());
            }
            selectExprs.add(aggSQL);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause - check for direct source optimization
        String childSource = getDirectlyAliasableSource(plan.child());
        if (childSource != null) {
            sql.append(" FROM ").append(childSource);
        } else {
            sql.append(" FROM (");
            subqueryDepth++;
            visit(plan.child());  // Use visit(), not generate()
            subqueryDepth--;
            sql.append(") AS ").append(generateSubqueryAlias());
        }

        // GROUP BY clause
        // Note: GROUP BY cannot have aliases, so we unwrap AliasExpressions
        if (!plan.groupingExpressions().isEmpty()) {
            sql.append(" GROUP BY ");
            java.util.List<String> groupExprs = new java.util.ArrayList<>();
            for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
                // Unwrap AliasExpression for GROUP BY - aliases not allowed here
                if (expr instanceof com.thunderduck.expression.AliasExpression) {
                    groupExprs.add(((com.thunderduck.expression.AliasExpression) expr).expression().toSQL());
                } else {
                    groupExprs.add(expr.toSQL());
                }
            }
            sql.append(String.join(", ", groupExprs));
        }

        // HAVING clause
        if (plan.havingCondition() != null) {
            sql.append(" HAVING ");
            sql.append(plan.havingCondition().toSQL());
        }
    }

    /**
     * Transforms aggregate function names in an expression tree for strict mode.
     * Recursively walks the AST and replaces SUM/AVG with spark_sum/spark_avg.
     * This is type-safe: only transforms actual FunctionCall nodes, never column names.
     *
     * @param expr the expression to transform
     * @return the transformed expression (or original if no changes needed)
     */
    public static Expression transformAggregateExpression(Expression expr) {
        if (expr instanceof FunctionCall func) {
            String name = func.functionName().toLowerCase();
            if (name.equals("sum") || name.equals("sum_distinct")) {
                return new FunctionCall("spark_sum", func.arguments(), func.dataType(), func.nullable());
            }
            if (name.equals("avg") || name.equals("avg_distinct")) {
                return new FunctionCall("spark_avg", func.arguments(), func.dataType(), func.nullable());
            }
            return expr;
        }
        if (expr instanceof BinaryExpression bin) {
            Expression newLeft = transformAggregateExpression(bin.left());
            Expression newRight = transformAggregateExpression(bin.right());
            if (newLeft != bin.left() || newRight != bin.right()) {
                return new BinaryExpression(newLeft, bin.operator(), newRight);
            }
            return expr;
        }
        if (expr instanceof CastExpression cast) {
            Expression newInner = transformAggregateExpression(cast.expression());
            if (newInner != cast.expression()) {
                return new CastExpression(newInner, cast.targetType());
            }
            return expr;
        }
        if (expr instanceof UnaryExpression unary) {
            Expression newOperand = transformAggregateExpression(unary.operand());
            if (newOperand != unary.operand()) {
                return new UnaryExpression(unary.operator(), newOperand);
            }
            return expr;
        }
        // Literals, columns, etc. -- no transformation needed
        return expr;
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
        if (expr instanceof com.thunderduck.expression.AliasExpression) {
            baseExpr = ((com.thunderduck.expression.AliasExpression) expr).expression();
        }

        // Resolve column reference from child schema
        if (baseExpr instanceof UnresolvedColumn && childSchema != null) {
            String colName = ((UnresolvedColumn) baseExpr).columnName();
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
        while (current instanceof Join) {
            Join j = (Join) current;
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
        while (current instanceof Filter) {
            filterConditions.add(((Filter) current).condition());
            current = ((Filter) current).child();
        }

        // The base must be a regular Join with aliased children (looking through filters)
        if (!(current instanceof Join) || !hasAliasedChildren((Join) current)) {
            return false;
        }

        Join baseJoin = (Join) current;

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
            if (rightPlan instanceof AliasedRelation) {
                rightAlias = SQLQuoting.quoteIdentifier(((AliasedRelation) rightPlan).alias());
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

            if (rightPlan instanceof AliasedRelation) {
                AliasedRelation aliased = (AliasedRelation) rightPlan;
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
        while (left instanceof Filter) {
            filterConditions.add(((Filter) left).condition());
            left = ((Filter) left).child();
        }

        // If the left is itself a join, recursively flatten it too
        if (left instanceof Join) {
            left = flattenJoinFilters((Join) left, filterConditions);
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

            if (leftPlan instanceof AliasedRelation) {
                leftAlias = SQLQuoting.quoteIdentifier(((AliasedRelation) leftPlan).alias());
            } else {
                leftAlias = generateSubqueryAlias();
            }

            if (rightPlan instanceof AliasedRelation) {
                rightAlias = SQLQuoting.quoteIdentifier(((AliasedRelation) rightPlan).alias());
            } else {
                rightAlias = generateSubqueryAlias();
            }

            // Build plan_id → alias mapping for column qualification
            Map<Long, String> planIdToAlias = new HashMap<>();
            collectPlanIds(plan.left(), leftAlias, planIdToAlias);
            collectPlanIds(plan.right(), rightAlias, planIdToAlias);

            // LEFT SIDE - handle AliasedRelation, use direct aliasing for TableScan, wrap others
            if (leftPlan instanceof AliasedRelation) {
                AliasedRelation aliased = (AliasedRelation) leftPlan;
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
            if (rightPlan instanceof AliasedRelation) {
                AliasedRelation aliased = (AliasedRelation) rightPlan;
                String childSource = getDirectlyAliasableSource(aliased.child());
                if (childSource != null) {
                    sql.append("EXISTS (SELECT 1 FROM ").append(childSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("EXISTS (SELECT 1 FROM (");
                    subqueryDepth++;
                    visit(aliased.child());
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

            if (leftPlan instanceof AliasedRelation) {
                leftAlias = SQLQuoting.quoteIdentifier(((AliasedRelation) leftPlan).alias());
            } else {
                leftAlias = generateSubqueryAlias();
            }

            if (rightPlan instanceof AliasedRelation) {
                rightAlias = SQLQuoting.quoteIdentifier(((AliasedRelation) rightPlan).alias());
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
            if (leftPlan instanceof AliasedRelation) {
                // User provided an alias - generate SQL using that alias directly
                AliasedRelation aliased = (AliasedRelation) leftPlan;
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
            if (rightPlan instanceof AliasedRelation) {
                // User provided an alias - generate SQL using that alias directly
                AliasedRelation aliased = (AliasedRelation) rightPlan;
                String childSource = getDirectlyAliasableSource(aliased.child());
                if (childSource != null) {
                    sql.append(childSource).append(" AS ").append(rightAlias);
                } else {
                    sql.append("(");
                    subqueryDepth++;
                    visit(aliased.child());
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
            String leftSQL = qualifyCondition(binExpr.left(), planIdToAlias);
            String rightSQL = qualifyCondition(binExpr.right(), planIdToAlias);
            if (binExpr.operator() == BinaryExpression.Operator.DIVIDE
                    && SparkCompatMode.isStrictMode()) {
                return String.format("spark_decimal_div(%s, %s)", leftSQL, rightSQL);
            }
            return "(" + leftSQL + " " + binExpr.operator().symbol() + " " + rightSQL + ")";
        }

        if (expr instanceof UnaryExpression unaryExpr) {
            String operandSQL = qualifyCondition(unaryExpr.operand(), planIdToAlias);
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
            String innerSQL = qualifyCondition(aliasExpr.expression(), planIdToAlias);
            return innerSQL + " AS " + aliasExpr.alias();
        }

        if (expr instanceof CastExpression castExpr) {
            String innerSQL = qualifyCondition(castExpr.expression(), planIdToAlias);
            return "CAST(" + innerSQL + " AS " + castExpr.targetType().typeName() + ")";
        }

        if (expr instanceof FunctionCall funcExpr) {
            List<String> qualifiedArgs = new ArrayList<>();
            for (Expression arg : funcExpr.arguments()) {
                qualifiedArgs.add(qualifyCondition(arg, planIdToAlias));
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
            String testSQL = qualifyCondition(inExpr.testExpr(), planIdToAlias);
            List<String> valuesSQLs = new ArrayList<>();
            for (Expression val : inExpr.values()) {
                valuesSQLs.add(qualifyCondition(val, planIdToAlias));
            }
            String op = inExpr.isNegated() ? " NOT IN (" : " IN (";
            return testSQL + op + String.join(", ", valuesSQLs) + ")";
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
     * Visits a Union node.
     * Builds SQL directly in buffer.
     * Wraps children in parentheses for correct precedence in chained operations.
     */
    private void visitUnion(Union plan) {
        if (plan.byName()) {
            visitUnionByName(plan);
            return;
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

        // Right side (wrapped for precedence)
        sql.append("(");
        visit(plan.right());
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

        if (vector instanceof org.apache.arrow.vector.BitVector) {
            return ((org.apache.arrow.vector.BitVector) vector).get(index) != 0;
        } else if (vector instanceof org.apache.arrow.vector.TinyIntVector) {
            return ((org.apache.arrow.vector.TinyIntVector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.SmallIntVector) {
            return ((org.apache.arrow.vector.SmallIntVector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.IntVector) {
            return ((org.apache.arrow.vector.IntVector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.BigIntVector) {
            return ((org.apache.arrow.vector.BigIntVector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.Float4Vector) {
            return ((org.apache.arrow.vector.Float4Vector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.Float8Vector) {
            return ((org.apache.arrow.vector.Float8Vector) vector).get(index);
        } else if (vector instanceof org.apache.arrow.vector.VarCharVector) {
            byte[] bytes = ((org.apache.arrow.vector.VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        } else if (vector instanceof org.apache.arrow.vector.DateDayVector) {
            int days = ((org.apache.arrow.vector.DateDayVector) vector).get(index);
            return java.time.LocalDate.ofEpochDay(days);
        } else if (vector instanceof org.apache.arrow.vector.DateMilliVector) {
            // Date stored as milliseconds since epoch
            long millis = ((org.apache.arrow.vector.DateMilliVector) vector).get(index);
            return java.time.LocalDate.ofEpochDay(millis / (24 * 60 * 60 * 1000L));
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroTZVector) {
            // Timestamp with timezone in microseconds
            long micros = ((org.apache.arrow.vector.TimeStampMicroTZVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroVector) {
            // Timestamp without timezone in microseconds
            long micros = ((org.apache.arrow.vector.TimeStampMicroVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(micros / 1_000_000, (micros % 1_000_000) * 1000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMilliTZVector) {
            // Timestamp with timezone in milliseconds
            long millis = ((org.apache.arrow.vector.TimeStampMilliTZVector) vector).get(index);
            return java.time.Instant.ofEpochMilli(millis);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMilliVector) {
            // Timestamp without timezone in milliseconds
            long millis = ((org.apache.arrow.vector.TimeStampMilliVector) vector).get(index);
            return java.time.Instant.ofEpochMilli(millis);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoTZVector) {
            // Timestamp with timezone in nanoseconds
            long nanos = ((org.apache.arrow.vector.TimeStampNanoTZVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(nanos / 1_000_000_000, nanos % 1_000_000_000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampNanoVector) {
            // Timestamp without timezone in nanoseconds
            long nanos = ((org.apache.arrow.vector.TimeStampNanoVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(nanos / 1_000_000_000, nanos % 1_000_000_000);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampSecTZVector) {
            // Timestamp with timezone in seconds
            long secs = ((org.apache.arrow.vector.TimeStampSecTZVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(secs);
        } else if (vector instanceof org.apache.arrow.vector.TimeStampSecVector) {
            // Timestamp without timezone in seconds
            long secs = ((org.apache.arrow.vector.TimeStampSecVector) vector).get(index);
            return java.time.Instant.ofEpochSecond(secs);
        } else if (vector instanceof org.apache.arrow.vector.complex.ListVector) {
            // Handle array/list types - return as List to preserve structure
            org.apache.arrow.vector.complex.ListVector listVector = (org.apache.arrow.vector.complex.ListVector) vector;
            return listVector.getObject(index);  // Returns java.util.List
        } else if (vector instanceof org.apache.arrow.vector.complex.MapVector) {
            // Handle map types - return as List of Map.Entry-like structures
            org.apache.arrow.vector.complex.MapVector mapVector = (org.apache.arrow.vector.complex.MapVector) vector;
            return mapVector.getObject(index);  // Returns List of structs (key, value)
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

        if (value instanceof String) {
            // Escape single quotes by doubling them
            String escaped = ((String) value).replace("'", "''");
            return "'" + escaped + "'";
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        } else if (value instanceof java.time.LocalDate) {
            return "DATE '" + value.toString() + "'";
        } else if (value instanceof java.sql.Date) {
            return "DATE '" + value.toString() + "'";
        } else if (value instanceof java.sql.Timestamp) {
            return "TIMESTAMP '" + value.toString() + "'";
        } else if (value instanceof java.time.Instant) {
            // Format Instant as TIMESTAMP literal
            java.time.Instant instant = (java.time.Instant) value;
            java.time.LocalDateTime ldt = java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
            return "TIMESTAMP '" + ldt.toString().replace("T", " ") + "'";
        } else if (value instanceof java.time.LocalDateTime) {
            // Format LocalDateTime as TIMESTAMP literal
            return "TIMESTAMP '" + value.toString().replace("T", " ") + "'";
        } else if (value instanceof java.util.List) {
            // Format as DuckDB array literal: [elem1, elem2, ...]
            java.util.List<?> list = (java.util.List<?>) value;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(formatSQLValue(list.get(i)));
            }
            sb.append("]");
            return sb.toString();
        } else if (value instanceof java.util.Map) {
            // Format as DuckDB map literal: MAP([keys], [values])
            java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
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
        } else if (value instanceof org.apache.arrow.vector.util.JsonStringHashMap) {
            // Arrow returns maps as JsonStringHashMap (key-value struct entries)
            // Format as DuckDB map literal
            @SuppressWarnings("unchecked")
            org.apache.arrow.vector.util.JsonStringHashMap<String, ?> arrowMap =
                (org.apache.arrow.vector.util.JsonStringHashMap<String, ?>) value;
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
        String formatted = formatSQLValue(value);
        if (value == null) {
            return formatted; // NULL doesn't need CAST
        }

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

    // Removed local quoteIdentifier() methods - using SQLQuoting.quoteIdentifier() everywhere
    // which always quotes identifiers for consistency and security (see Week 13 Phase 1 fixes)

    /**
     * Escapes single quotes in a string literal.
     *
     * @param str the string to escape
     * @return the escaped string
     */
    /*private String escapeSingleQuotes(String str) {
        return str.replace("'", "''");
    }*/

    /**
     * Generates a unique subquery alias.
     *
     * @return a unique alias like "subquery_1", "subquery_2", etc.
     */
    public String generateSubqueryAlias() {
        return "subquery_" + (++aliasCounter);
    }

    /**
     * Context for SQL generation.
     *
     * <p>Tracks state during traversal, such as current aliases,
     * available columns, and generation options.
     */
    //private static class GenerationContext {
        //private final Map<String, String> aliases;
        // private final Set<String> availableColumns;

        /*public GenerationContext() {
            //this.aliases = new HashMap<>();
           // this.availableColumns = new HashSet<>();
        }*/
    //}

    /**
     * Returns the current subquery depth.
     *
     * @return the subquery depth
     */
    public int getSubqueryDepth() {
        return subqueryDepth;
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
}
