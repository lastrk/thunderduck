package com.thunderduck.generator;

import com.thunderduck.exception.SQLGenerationException;
import com.thunderduck.logical.*;
import com.thunderduck.expression.Expression;
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
     * Visits a TableScan node (FROM clause).
     */
    private void visitTableScan(TableScan plan) {
        String source = plan.source();
        TableScan.TableFormat format = plan.format();

        switch (format) {
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

            // Start with SELECT * FROM left
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            visit(plan.left());
            subqueryDepth--;
            sql.append(") AS ").append(leftAlias);

            // Add WHERE [NOT] EXISTS
            sql.append(" WHERE ");
            if (plan.joinType() == Join.JoinType.LEFT_ANTI) {
                sql.append("NOT ");
            }
            sql.append("EXISTS (SELECT 1 FROM (");

            // Add right side
            subqueryDepth++;
            visit(plan.right());
            subqueryDepth--;
            sql.append(") AS ").append(rightAlias);

            // Add WHERE clause for the correlation
            if (plan.condition() != null) {
                sql.append(" WHERE ");
                // Need to properly handle the join condition here
                // For now, using the condition as-is (may need column qualification)
                sql.append(plan.condition().toSQL());
            }

            sql.append(")");

        } else {
            // Regular joins (INNER, LEFT, RIGHT, FULL, CROSS)

            // SELECT * FROM left
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            visit(plan.left());
            subqueryDepth--;
            sql.append(") AS ").append(generateSubqueryAlias());

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

            // Right side
            sql.append("(");
            subqueryDepth++;
            visit(plan.right());
            subqueryDepth--;
            sql.append(") AS ").append(generateSubqueryAlias());

            // ON clause (except for CROSS join)
            if (plan.joinType() != Join.JoinType.CROSS && plan.condition() != null) {
                sql.append(" ON ");
                sql.append(plan.condition().toSQL());
            }
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
            sql.append(generateEmptyValues());
        } else if (root.getRowCount() <= 100) {
            // Small dataset - use VALUES clause
            sql.append(generateValuesClause(root));
        } else {
            // Larger dataset - for now, fall back to VALUES (can be optimized with temp tables later)
            sql.append(generateValuesClause(root));
        }
    }

    /**
     * Generates an empty VALUES clause that returns no rows.
     * Example: SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE
     *
     * @return SQL for empty values
     */
    public String generateEmptyValues() {
        return "SELECT * FROM (VALUES (NULL)) AS t WHERE FALSE";
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
            return generateEmptyValues();
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
