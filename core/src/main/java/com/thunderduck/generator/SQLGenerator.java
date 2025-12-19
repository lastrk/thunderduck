package com.thunderduck.generator;

import com.thunderduck.exception.SQLGenerationException;
import com.thunderduck.logical.*;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.UnresolvedColumn;
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

        if (plan instanceof Project) {
            visitProject((Project) plan);
        } else if (plan instanceof Filter) {
            visitFilter((Filter) plan);
        } else if (plan instanceof TableScan) {
            visitTableScan((TableScan) plan);
        } else if (plan instanceof Sort) {
            visitSort((Sort) plan);
        } else if (plan instanceof Limit) {
            visitLimit((Limit) plan);
        } else if (plan instanceof Aggregate) {
            visitAggregate((Aggregate) plan);
        } else if (plan instanceof Join) {
            visitJoin((Join) plan);
        } else if (plan instanceof Union) {
            visitUnion((Union) plan);
        } else if (plan instanceof InMemoryRelation) {
            visitInMemoryRelation((InMemoryRelation) plan);
        } else if (plan instanceof LocalRelation) {
            visitLocalRelation((LocalRelation) plan);
        } else if (plan instanceof LocalDataRelation) {
            visitLocalDataRelation((LocalDataRelation) plan);
        } else if (plan instanceof SQLRelation) {
            visitSQLRelation((SQLRelation) plan);
        } else if (plan instanceof Distinct) {
            visitDistinct((Distinct) plan);
        } else if (plan instanceof RangeRelation) {
            visitRangeRelation((RangeRelation) plan);
        } else if (plan instanceof Tail) {
            visitTail((Tail) plan);
        } else if (plan instanceof Sample) {
            visitSample((Sample) plan);
        } else {
            throw new UnsupportedOperationException(
                "SQL generation not implemented for: " + plan.getClass().getSimpleName());
        }
    }

    /**
     * Visits an SQLRelation node.
     */
    private void visitSQLRelation(SQLRelation plan) {
        sql.append(plan.toSQL(this));
    }

    /**
     * Visits a Project node (SELECT clause).
     */
    private void visitProject(Project plan) {
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

        // Add FROM clause from child
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

    /**
     * Visits a Filter node (WHERE clause).
     */
    private void visitFilter(Filter plan) {
        sql.append("SELECT * FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());
        sql.append(" WHERE ");
        sql.append(plan.condition().toSQL());
    }

    /**
     * Visits a Distinct node (DISTINCT operation).
     */
    private void visitDistinct(Distinct plan) {
        List<String> columns = plan.columns();

        if (columns == null || columns.isEmpty()) {
            // DISTINCT on all columns
            sql.append("SELECT DISTINCT * FROM (");
        } else {
            // DISTINCT on specific columns
            sql.append("SELECT DISTINCT ");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(columns.get(i)));
            }
            sql.append(" FROM (");
        }

        subqueryDepth++;
        visit(plan.children().get(0));
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());
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

        switch (format) {
            case TABLE:
                // Regular DuckDB table - use table name directly with proper quoting
                sql.append("SELECT * FROM ");
                sql.append(quoteIdentifier(source));
                break;

            case PARQUET:
                // Use DuckDB's read_parquet function with safe quoting
                sql.append("SELECT * FROM read_parquet(");
                sql.append(quoteFilePath(source));
                sql.append(")");
                break;

            case DELTA:
                // Use DuckDB's delta_scan function with safe quoting
                sql.append("SELECT * FROM delta_scan(");
                sql.append(quoteFilePath(source));
                sql.append(")");
                break;

            case ICEBERG:
                // Use DuckDB's iceberg_scan function with safe quoting
                sql.append("SELECT * FROM iceberg_scan(");
                sql.append(quoteFilePath(source));
                sql.append(")");
                break;

            default:
                throw new UnsupportedOperationException(
                    "Unsupported table format: " + format);
        }
    }

    /**
     * Visits a Sort node (ORDER BY clause).
     * Builds SQL directly in buffer to avoid corruption.
     */
    private void visitSort(Sort plan) {
        sql.append("SELECT * FROM (");
        subqueryDepth++;
        visit(plan.child());  // Use visit(), not generate()
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());
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
        sql.append("SELECT * FROM (");
        subqueryDepth++;
        visit(plan.child());
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());
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
            String aggSQL = aggExpr.toSQL();
            // Add alias if provided
            if (aggExpr.alias() != null && !aggExpr.alias().isEmpty()) {
                aggSQL += " AS " + SQLQuoting.quoteIdentifier(aggExpr.alias());
            }
            selectExprs.add(aggSQL);
        }

        sql.append(String.join(", ", selectExprs));

        // FROM clause
        sql.append(" FROM (");
        subqueryDepth++;
        visit(plan.child());  // Use visit(), not generate()
        subqueryDepth--;
        sql.append(") AS ").append(generateSubqueryAlias());

        // GROUP BY clause
        if (!plan.groupingExpressions().isEmpty()) {
            sql.append(" GROUP BY ");
            java.util.List<String> groupExprs = new java.util.ArrayList<>();
            for (com.thunderduck.expression.Expression expr : plan.groupingExpressions()) {
                groupExprs.add(expr.toSQL());
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
     * Visits a Join node.
     * Builds SQL directly in buffer to avoid corruption.
     *
     * <p>For columns with plan_id, this method qualifies column references
     * based on which side of the join they belong to. This enables proper
     * handling of joins where both tables have columns with the same name.
     *
     * For semi-joins and anti-joins, uses EXISTS/NOT EXISTS patterns
     * since DuckDB doesn't support LEFT SEMI JOIN syntax directly.
     */
    private void visitJoin(Join plan) {
        // Handle SEMI and ANTI joins differently (using EXISTS/NOT EXISTS)
        if (plan.joinType() == Join.JoinType.LEFT_SEMI ||
            plan.joinType() == Join.JoinType.LEFT_ANTI) {

            // For semi/anti joins, we need to use WHERE EXISTS/NOT EXISTS
            // SELECT * FROM left WHERE [NOT] EXISTS (SELECT 1 FROM right WHERE condition)

            String leftAlias = generateSubqueryAlias();
            String rightAlias = generateSubqueryAlias();

            // Build plan_id → alias mapping for column qualification
            Map<Long, String> planIdToAlias = new HashMap<>();
            collectPlanIds(plan.left(), leftAlias, planIdToAlias);
            collectPlanIds(plan.right(), rightAlias, planIdToAlias);

            // LEFT SIDE - use direct aliasing for TableScan, wrap others
            String leftSource = getDirectlyAliasableSource(plan.left());
            if (leftSource != null) {
                sql.append("SELECT * FROM ").append(leftSource).append(" AS ").append(leftAlias);
            } else {
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(plan.left());
                subqueryDepth--;
                sql.append(") AS ").append(leftAlias);
            }

            // Add WHERE [NOT] EXISTS
            sql.append(" WHERE ");
            if (plan.joinType() == Join.JoinType.LEFT_ANTI) {
                sql.append("NOT ");
            }

            // RIGHT SIDE - use direct aliasing for TableScan, wrap others
            String rightSource = getDirectlyAliasableSource(plan.right());
            if (rightSource != null) {
                sql.append("EXISTS (SELECT 1 FROM ").append(rightSource).append(" AS ").append(rightAlias);
            } else {
                sql.append("EXISTS (SELECT 1 FROM (");
                subqueryDepth++;
                visit(plan.right());
                subqueryDepth--;
                sql.append(") AS ").append(rightAlias);
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

            // Generate aliases before visiting children
            String leftAlias = generateSubqueryAlias();
            String rightAlias = generateSubqueryAlias();

            // Build plan_id → alias mapping for column qualification
            Map<Long, String> planIdToAlias = new HashMap<>();
            collectPlanIds(plan.left(), leftAlias, planIdToAlias);
            collectPlanIds(plan.right(), rightAlias, planIdToAlias);

            // LEFT SIDE - use direct aliasing for TableScan, wrap others
            String leftSource = getDirectlyAliasableSource(plan.left());
            if (leftSource != null) {
                sql.append("SELECT * FROM ").append(leftSource).append(" AS ").append(leftAlias);
            } else {
                sql.append("SELECT * FROM (");
                subqueryDepth++;
                visit(plan.left());
                subqueryDepth--;
                sql.append(") AS ").append(leftAlias);
            }

            // JOIN type
            switch (plan.joinType()) {
                case INNER:
                    sql.append(" INNER JOIN ");
                    break;
                case LEFT:
                    sql.append(" LEFT OUTER JOIN ");
                    break;
                case RIGHT:
                    sql.append(" RIGHT OUTER JOIN ");
                    break;
                case FULL:
                    sql.append(" FULL OUTER JOIN ");
                    break;
                case CROSS:
                    sql.append(" CROSS JOIN ");
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "Unexpected join type: " + plan.joinType());
            }

            // RIGHT SIDE - use direct aliasing for TableScan, wrap others
            String rightSource = getDirectlyAliasableSource(plan.right());
            if (rightSource != null) {
                sql.append(rightSource).append(" AS ").append(rightAlias);
            } else {
                sql.append("(");
                subqueryDepth++;
                visit(plan.right());
                subqueryDepth--;
                sql.append(") AS ").append(rightAlias);
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
        if (expr instanceof UnresolvedColumn) {
            UnresolvedColumn col = (UnresolvedColumn) expr;

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

        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            String leftSQL = qualifyCondition(binExpr.left(), planIdToAlias);
            String rightSQL = qualifyCondition(binExpr.right(), planIdToAlias);
            return "(" + leftSQL + " " + binExpr.operator().symbol() + " " + rightSQL + ")";
        }

        // For other expression types, use their default SQL representation
        // This handles literals, function calls, etc.
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
        if (!(plan instanceof TableScan)) {
            return null;
        }

        TableScan scan = (TableScan) plan;
        String source = scan.source();

        switch (scan.format()) {
            case TABLE:
                return quoteIdentifier(source);
            case PARQUET:
                return "read_parquet(" + quoteFilePath(source) + ")";
            case DELTA:
                return "delta_scan(" + quoteFilePath(source) + ")";
            case ICEBERG:
                return "iceberg_scan(" + quoteFilePath(source) + ")";
            default:
                return null;
        }
    }

    /**
     * Visits a Union node.
     * Builds SQL directly in buffer.
     */
    private void visitUnion(Union plan) {
        // Left side
        visit(plan.left());

        // UNION operator
        if (plan.all()) {
            sql.append(" UNION ALL ");
        } else {
            sql.append(" UNION ");
        }

        // Right side
        visit(plan.right());
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
                Object value = getArrowValue(vector, row);
                values.append(formatSQLValue(value));
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
        } else if (vector instanceof org.apache.arrow.vector.TimeStampMicroVector) {
            long micros = ((org.apache.arrow.vector.TimeStampMicroVector) vector).get(index);
            return new java.sql.Timestamp(micros / 1000);
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
        } else {
            // Fallback: convert to string and quote
            String escaped = value.toString().replace("'", "''");
            return "'" + escaped + "'";
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
}
