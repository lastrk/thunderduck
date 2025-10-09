package com.spark2sql.translator;

import com.spark2sql.plan.Expression;
import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.plan.PlanVisitor;
import com.spark2sql.plan.nodes.*;
import com.spark2sql.plan.expressions.*;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Simplified translator that converts LogicalPlan to SQL.
 * This is a placeholder implementation for Phase 1.
 */
public class PlanToSQLTranslator {
    private static final Logger LOG = LoggerFactory.getLogger(PlanToSQLTranslator.class);

    public String translate(LogicalPlan plan) {
        // For Phase 1, we'll use a simple direct SQL generation
        // In later phases, this will use Calcite for optimization

        SQLBuilder builder = new SQLBuilder();
        plan.accept(builder);
        String sql = builder.build();

        LOG.debug("Translated plan to SQL: {}", sql);
        return sql;
    }

    /**
     * Simple SQL builder for Phase 1.
     */
    private static class SQLBuilder implements PlanVisitor<Void> {
        private StringBuilder sql = new StringBuilder();
        private int subqueryDepth = 0;

        public String build() {
            return sql.toString();
        }

        @Override
        public Void visitProject(Project project) {
            sql.append("SELECT ");

            List<?> columns = project.getProjectList();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) sql.append(", ");
                // Project list contains Column objects, we need to extract their expressions
                Object col = columns.get(i);
                if (col.getClass().getName().equals("org.apache.spark.sql.Column")) {
                    // Use reflection to get the expression from Column
                    try {
                        java.lang.reflect.Method exprMethod = col.getClass().getDeclaredMethod("expr");
                        exprMethod.setAccessible(true);
                        Expression expr = (Expression) exprMethod.invoke(col);
                        sql.append(expressionToSQL(expr));
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to extract expression from Column", e);
                    }
                } else {
                    sql.append(expressionToSQL((Expression) col));
                }
            }

            sql.append(" FROM (");
            subqueryDepth++;
            project.children().get(0).accept(this);
            subqueryDepth--;
            sql.append(")");

            if (subqueryDepth == 0) {
                sql.append(" AS t");
            }

            return null;
        }

        @Override
        public Void visitFilter(Filter filter) {
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            filter.children().get(0).accept(this);
            subqueryDepth--;
            sql.append(") WHERE ");
            sql.append(expressionToSQL(filter.getCondition()));

            return null;
        }

        @Override
        public Void visitLimit(Limit limit) {
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            limit.children().get(0).accept(this);
            subqueryDepth--;
            sql.append(") LIMIT ").append(limit.getN());

            return null;
        }

        @Override
        public Void visitTableScan(TableScan scan) {
            sql.append("SELECT * FROM \"").append(scan.getTableName()).append("\"");
            return null;
        }

        @Override
        public Void visitLocalRelation(LocalRelation relation) {
            // For local relations, we need to generate VALUES clause
            sql.append("VALUES ");
            List<Row> rows = relation.getRows();

            if (rows.isEmpty()) {
                sql.append("(NULL)");
            } else {
                for (int i = 0; i < rows.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append("(");
                    Row row = rows.get(i);
                    for (int j = 0; j < row.length(); j++) {
                        if (j > 0) sql.append(", ");
                        sql.append(literalToSQL(row.get(j)));
                    }
                    sql.append(")");
                }
            }

            return null;
        }

        @Override
        public Void visitDistinct(Distinct distinct) {
            sql.append("SELECT DISTINCT * FROM (");
            subqueryDepth++;
            distinct.children().get(0).accept(this);
            subqueryDepth--;
            sql.append(")");

            return null;
        }

        @Override
        public Void visitSort(Sort sort) {
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            sort.children().get(0).accept(this);
            subqueryDepth--;
            sql.append(") ORDER BY ");

            List<?> sortExprs = sort.getSortExpressions();
            for (int i = 0; i < sortExprs.size(); i++) {
                if (i > 0) sql.append(", ");
                Object sortExpr = sortExprs.get(i);
                Expression expr;

                if (sortExpr.getClass().getName().equals("org.apache.spark.sql.Column")) {
                    // Extract expression from Column using reflection
                    try {
                        java.lang.reflect.Method exprMethod = sortExpr.getClass().getDeclaredMethod("expr");
                        exprMethod.setAccessible(true);
                        expr = (Expression) exprMethod.invoke(sortExpr);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to extract expression from Column", e);
                    }
                } else {
                    expr = (Expression) sortExpr;
                }

                if (expr instanceof SortOrder) {
                    SortOrder so = (SortOrder) expr;
                    sql.append(expressionToSQL(so.getChild()));
                    sql.append(so.isAscending() ? " ASC" : " DESC");
                    sql.append(so.isNullsFirst() ? " NULLS FIRST" : " NULLS LAST");
                } else {
                    sql.append(expressionToSQL(expr));
                }
            }

            return null;
        }

        @Override
        public Void visitAggregate(Aggregate aggregate) {
            // TODO: Implement aggregate translation in Phase 3
            sql.append("SELECT * FROM (");
            if (!aggregate.children().isEmpty()) {
                aggregate.children().get(0).accept(this);
            }
            sql.append(")");
            return null;
        }

        @Override
        public Void visitJoin(Join join) {
            sql.append("SELECT * FROM (");
            subqueryDepth++;
            join.getLeft().accept(this);
            subqueryDepth--;
            sql.append(") AS left_tbl ");

            // Map Spark join types to SQL join types
            String joinType = join.getJoinType().toLowerCase();
            switch (joinType) {
                case "inner":
                    sql.append("INNER JOIN");
                    break;
                case "left":
                case "left_outer":
                    sql.append("LEFT OUTER JOIN");
                    break;
                case "right":
                case "right_outer":
                    sql.append("RIGHT OUTER JOIN");
                    break;
                case "outer":
                case "full":
                case "full_outer":
                    sql.append("FULL OUTER JOIN");
                    break;
                case "cross":
                    sql.append("CROSS JOIN");
                    break;
                case "left_semi":
                    // Left semi join - returns only left rows that have a match
                    sql.append("INNER JOIN");
                    break;
                case "left_anti":
                    // Left anti join - returns only left rows that don't have a match
                    sql.append("LEFT OUTER JOIN");
                    break;
                default:
                    throw new UnsupportedOperationException("Join type not supported: " + joinType);
            }

            sql.append(" (");
            subqueryDepth++;
            join.getRight().accept(this);
            subqueryDepth--;
            sql.append(") AS right_tbl");

            // Add join condition if not cross join
            if (!"cross".equals(joinType) && join.getCondition() != null) {
                sql.append(" ON ");
                sql.append(expressionToSQL(join.getCondition()));

                // Special handling for semi and anti joins
                if ("left_semi".equals(joinType)) {
                    // For semi join, we need to select only from left table
                    sql.insert(0, "SELECT left_tbl.* FROM (");
                    sql.append(")");
                } else if ("left_anti".equals(joinType)) {
                    // For anti join, add WHERE right_tbl.col IS NULL
                    // This requires knowing the join key, which is complex
                    // For now, we'll require explicit WHERE clause
                    sql.insert(0, "SELECT left_tbl.* FROM (");
                    sql.append(") WHERE right_tbl.* IS NULL");
                }
            }

            return null;
        }

        private String expressionToSQL(Expression expr) {
            // Simplified expression translation
            if (expr instanceof ColumnReference) {
                return "\"" + ((ColumnReference) expr).getName() + "\"";
            } else if (expr instanceof Literal) {
                return literalToSQL(((Literal) expr).getValue());
            } else if (expr instanceof BinaryExpression) {
                BinaryExpression be = (BinaryExpression) expr;
                String left = expressionToSQL(be.getLeft());
                String right = expressionToSQL(be.getRight());
                String op = getOperator(be);
                return "(" + left + " " + op + " " + right + ")";
            } else if (expr instanceof IsNull) {
                return "(" + expressionToSQL(((IsNull) expr).getChild()) + " IS NULL)";
            } else if (expr instanceof IsNotNull) {
                return "(" + expressionToSQL(((IsNotNull) expr).getChild()) + " IS NOT NULL)";
            } else if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                return expressionToSQL(alias.getChild()) + " AS \"" + alias.getName() + "\"";
            } else {
                // Default: try to use toString
                return expr.toString();
            }
        }

        private String getOperator(BinaryExpression expr) {
            if (expr instanceof Add) return "+";
            if (expr instanceof Subtract) return "-";
            if (expr instanceof Multiply) return "*";
            if (expr instanceof Divide) {
                // Use our Spark-compatible integer division UDF when needed
                return "/";
            }
            if (expr instanceof Remainder) return "%";
            if (expr instanceof EqualTo) return "=";
            if (expr instanceof NotEqualTo) return "!=";
            if (expr instanceof GreaterThan) return ">";
            if (expr instanceof GreaterThanOrEqual) return ">=";
            if (expr instanceof LessThan) return "<";
            if (expr instanceof LessThanOrEqual) return "<=";
            if (expr instanceof And) return "AND";
            if (expr instanceof Or) return "OR";

            return expr.getClass().getSimpleName().toUpperCase();
        }

        private String literalToSQL(Object value) {
            if (value == null) {
                return "NULL";
            } else if (value instanceof String) {
                return "'" + value.toString().replace("'", "''") + "'";
            } else if (value instanceof Boolean) {
                return value.toString().toUpperCase();
            } else {
                return value.toString();
            }
        }
    }
}