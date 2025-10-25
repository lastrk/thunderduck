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
    private final Stack<GenerationContext> contextStack;
    private int aliasCounter;
    private int subqueryDepth;

    /**
     * Creates a new SQL generator.
     */
    public SQLGenerator() {
        this.sql = new StringBuilder();
        this.contextStack = new Stack<>();
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
        int savedAliasCounter = aliasCounter;
        int savedSubqueryDepth = subqueryDepth;

        if (!isRecursive) {
            // Top-level call: reset all state
            sql.setLength(0);
            contextStack.clear();
            aliasCounter = 0;
            subqueryDepth = 0;
        }

        try {
            // Generate SQL
            visit(plan);
            String result = sql.toString().substring(savedLength);  // Get only the new part
            return result;

        } catch (UnsupportedOperationException e) {
            // Rollback state
            sql.setLength(savedLength);
            contextStack.clear();

            // Re-throw UnsupportedOperationException without wrapping
            // so callers can catch it directly if needed
            throw e;

        } catch (IllegalArgumentException e) {
            // Rollback state
            sql.setLength(savedLength);
            contextStack.clear();

            // Re-throw IllegalArgumentException without wrapping
            // This includes validation errors from quoting/escaping
            throw e;

        } catch (Exception e) {
            // Rollback state
            sql.setLength(savedLength);
            contextStack.clear();

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
        } else if (plan instanceof SQLRelation) {
            visitSQLRelation((SQLRelation) plan);
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
                aggSQL += " AS " + quoteIdentifier(aggExpr.alias());
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
     */
    private void visitJoin(Join plan) {
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
            case LEFT_SEMI:
                sql.append(" LEFT SEMI JOIN ");
                break;
            case LEFT_ANTI:
                sql.append(" LEFT ANTI JOIN ");
                break;
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
     * Quotes an identifier for use in SQL.
     *
     * <p>DuckDB uses double quotes for identifiers that need quoting
     * (e.g., reserved words, special characters, case-sensitive names).
     *
     * @param identifier the identifier to quote
     * @return the quoted identifier
     */
    public static String quoteIdentifier(String identifier) {
        Objects.requireNonNull(identifier, "identifier must not be null");

        // Check if identifier needs quoting
        if (needsQuoting(identifier)) {
            // Escape any double quotes in the identifier
            String escaped = identifier.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }

        return identifier;
    }

    /**
     * Checks if an identifier needs quoting.
     *
     * @param identifier the identifier to check
     * @return true if quoting is needed
     */
    private static boolean needsQuoting(String identifier) {
        if (identifier.isEmpty()) {
            return true;
        }

        // Check if it starts with a letter or underscore
        char first = identifier.charAt(0);
        if (!Character.isLetter(first) && first != '_') {
            return true;
        }

        // Check if it contains only letters, digits, and underscores
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return true;
            }
        }

        // Check if it's a reserved word (simplified check)
        String upper = identifier.toUpperCase();
        if (isReservedWord(upper)) {
            return true;
        }

        return false;
    }

    /**
     * Checks if a word is a SQL reserved word.
     *
     * @param word the word to check (uppercase)
     * @return true if reserved
     */
    private static boolean isReservedWord(String word) {
        // Common SQL reserved words
        Set<String> reserved = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
            "ON", "USING", "AS", "AND", "OR", "NOT", "IN", "EXISTS",
            "CASE", "WHEN", "THEN", "ELSE", "END", "NULL", "TRUE", "FALSE",
            "UNION", "INTERSECT", "EXCEPT", "LIMIT", "OFFSET"
        );
        return reserved.contains(word);
    }

    /**
     * Escapes single quotes in a string literal.
     *
     * @param str the string to escape
     * @return the escaped string
     */
    private String escapeSingleQuotes(String str) {
        return str.replace("'", "''");
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
     * Context for SQL generation.
     *
     * <p>Tracks state during traversal, such as current aliases,
     * available columns, and generation options.
     */
    private static class GenerationContext {
        private final Map<String, String> aliases;
        private final Set<String> availableColumns;

        public GenerationContext() {
            this.aliases = new HashMap<>();
            this.availableColumns = new HashSet<>();
        }
    }

    /**
     * Returns the current subquery depth.
     *
     * @return the subquery depth
     */
    public int getSubqueryDepth() {
        return subqueryDepth;
    }
}
