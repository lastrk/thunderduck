package com.thunderduck.parser;

import com.thunderduck.expression.*;
import com.thunderduck.expression.window.FrameBoundary;
import com.thunderduck.expression.window.WindowFrame;
import com.thunderduck.logical.*;
import com.thunderduck.types.*;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.apache.spark.sql.catalyst.parser.SqlBaseParserBaseVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ANTLR Visitor that builds Thunderduck LogicalPlan and Expression AST nodes
 * from the SparkSQL parse tree.
 *
 * <p>This visitor converts SparkSQL parse tree nodes into the same LogicalPlan/Expression
 * AST that the DataFrame API path uses, enabling both paths to share the SQLGenerator,
 * type inference, and schema correction infrastructure.
 *
 * <p>Supported SQL features:
 * <ul>
 *   <li>SELECT with expressions, aliases, DISTINCT, *</li>
 *   <li>FROM with table references, subqueries, CTEs (WITH)</li>
 *   <li>WHERE with predicates</li>
 *   <li>JOIN (INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI)</li>
 *   <li>GROUP BY with HAVING, ROLLUP, CUBE, GROUPING SETS</li>
 *   <li>ORDER BY with ASC/DESC/NULLS FIRST/LAST</li>
 *   <li>LIMIT/OFFSET</li>
 *   <li>UNION/INTERSECT/EXCEPT</li>
 *   <li>CASE WHEN, CAST, IN, EXISTS, BETWEEN, LIKE</li>
 *   <li>Window functions (OVER, PARTITION BY, ORDER BY, frame specs)</li>
 *   <li>Aggregate functions, arithmetic, comparison, logical operators</li>
 *   <li>Typed literals (DATE, TIMESTAMP), interval arithmetic</li>
 * </ul>
 */
public class SparkSQLAstBuilder extends SqlBaseParserBaseVisitor<Object> {

    private static final Logger logger = LoggerFactory.getLogger(SparkSQLAstBuilder.class);

    // ==================== Entry Points ====================

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return (LogicalPlan) visit(ctx.statement());
    }

    @Override
    public LogicalPlan visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        return visitQuery(ctx.query());
    }

    // ==================== Query / CTE ====================

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx) {
        LogicalPlan plan;

        // Handle CTEs (WITH clause)
        if (ctx.ctes() != null) {
            // CTEs are handled by wrapping the query in a SQLRelation
            // that includes the WITH clause in raw SQL form.
            // This is a pragmatic approach: DuckDB supports CTEs natively,
            // so we pass them through rather than trying to decompose them.
            return buildCTEQuery(ctx);
        }

        // Process query term (SELECT, UNION, etc.)
        plan = (LogicalPlan) visit(ctx.queryTerm());

        // Apply ORDER BY, LIMIT, OFFSET from queryOrganization
        plan = applyQueryOrganization(plan, ctx.queryOrganization());

        return plan;
    }

    private LogicalPlan buildCTEQuery(SqlBaseParser.QueryContext ctx) {
        // Reconstruct the full SQL including WITH clause and pass through as SQLRelation.
        // This is correct because DuckDB supports CTE syntax directly.
        // In the future, we could decompose CTEs into separate LogicalPlan nodes.
        String fullSql = ctx.start.getInputStream().getText(
            new org.antlr.v4.runtime.misc.Interval(
                ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        return new SQLRelation(fullSql);
    }

    // ==================== Query Term (SET operations) ====================

    @Override
    public LogicalPlan visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return (LogicalPlan) visit(ctx.queryPrimary());
    }

    @Override
    public LogicalPlan visitSetOperation(SqlBaseParser.SetOperationContext ctx) {
        LogicalPlan left = (LogicalPlan) visit(ctx.left);
        LogicalPlan right = (LogicalPlan) visit(ctx.right);

        boolean isAll = ctx.setQuantifier() != null &&
            ctx.setQuantifier().ALL() != null;

        String operator = ctx.operator.getText().toUpperCase();
        return switch (operator) {
            case "UNION" -> new Union(left, right, isAll);
            case "INTERSECT" -> new Intersect(left, right, !isAll);
            case "EXCEPT", "MINUS" -> new Except(left, right, !isAll);
            default -> throw new UnsupportedOperationException(
                "Unsupported set operation: " + operator);
        };
    }

    // ==================== Query Primary ====================

    @Override
    public LogicalPlan visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {
        return (LogicalPlan) visit(ctx.querySpecification());
    }

    @Override
    public LogicalPlan visitSubquery(SqlBaseParser.SubqueryContext ctx) {
        return visitQuery(ctx.query());
    }

    // ==================== SELECT Statement ====================

    @Override
    public LogicalPlan visitRegularQuerySpecification(
            SqlBaseParser.RegularQuerySpecificationContext ctx) {

        // For GROUP BY queries, pass the entire SELECT...FROM...GROUP BY as raw SQL.
        // This preserves all subquery aliases, column aliases, and complex aggregation
        // patterns that DuckDB handles natively. The GROUP BY clause references columns
        // by name, so regenerating the FROM clause would break column alias references.
        if (ctx.aggregationClause() != null) {
            String fullSql = getOriginalText(ctx);
            return new SQLRelation(fullSql);
        }

        // 1. Build FROM clause (source relation)
        LogicalPlan source;
        if (ctx.fromClause() != null) {
            source = visitFromClause(ctx.fromClause());
        } else {
            // No FROM: e.g., SELECT 1, SELECT CAST(NULL AS INT)
            source = new SingleRowRelation();
        }

        // 2. Apply WHERE filter
        if (ctx.whereClause() != null) {
            Expression condition = visitBooleanExpr(ctx.whereClause().booleanExpression());
            source = new Filter(source, condition);
        }

        // 3. Build SELECT projections
        source = buildProjection(source, ctx.selectClause());

        // 4. Apply HAVING (only valid with GROUP BY, but handle gracefully)
        if (ctx.havingClause() != null) {
            Expression havingCond = visitBooleanExpr(ctx.havingClause().booleanExpression());
            source = new Filter(source, havingCond);
        }

        return source;
    }

    // ==================== FROM Clause ====================

    @Override
    public LogicalPlan visitFromClause(SqlBaseParser.FromClauseContext ctx) {
        List<SqlBaseParser.RelationContext> relations = ctx.relation();
        LogicalPlan plan = visitRelation(relations.get(0));

        // Handle comma-separated tables (implicit cross join)
        for (int i = 1; i < relations.size(); i++) {
            LogicalPlan right = visitRelation(relations.get(i));
            plan = new Join(plan, right, Join.JoinType.CROSS, Literal.of(true));
        }

        return plan;
    }

    @Override
    public LogicalPlan visitRelation(SqlBaseParser.RelationContext ctx) {
        LogicalPlan plan = (LogicalPlan) visit(ctx.relationPrimary());

        // Apply join extensions
        for (SqlBaseParser.RelationExtensionContext ext : ctx.relationExtension()) {
            if (ext.joinRelation() != null) {
                plan = buildJoin(plan, ext.joinRelation());
            }
        }

        return plan;
    }

    // ==================== Table References ====================

    @Override
    public LogicalPlan visitTableName(SqlBaseParser.TableNameContext ctx) {
        String tableName = resolveIdentifierReference(ctx.identifierReference());

        // Wrap in SQLRelation that generates: SELECT * FROM tableName
        String quotedName = quoteIdentifierIfNeeded(tableName);
        LogicalPlan plan = new SQLRelation("SELECT * FROM " + quotedName);

        // Apply table alias
        plan = applyTableAlias(plan, ctx.tableAlias());

        return plan;
    }

    @Override
    public LogicalPlan visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {
        LogicalPlan plan = visitQuery(ctx.query());
        plan = applyTableAlias(plan, ctx.tableAlias());
        return plan;
    }

    @Override
    public LogicalPlan visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {
        LogicalPlan plan = visitRelation(ctx.relation());
        plan = applyTableAlias(plan, ctx.tableAlias());
        return plan;
    }

    /**
     * Applies table alias with optional column aliases from a tableAlias context.
     */
    private LogicalPlan applyTableAlias(LogicalPlan plan,
                                         SqlBaseParser.TableAliasContext aliasCtx) {
        if (aliasCtx != null && aliasCtx.strictIdentifier() != null) {
            String alias = getIdentifierText(aliasCtx.strictIdentifier());
            List<String> columnAliases = java.util.Collections.emptyList();
            if (aliasCtx.identifierList() != null) {
                columnAliases = resolveIdentifierSeq(
                    aliasCtx.identifierList().identifierSeq());
            }
            plan = new AliasedRelation(plan, alias, columnAliases);
        }
        return plan;
    }

    // ==================== JOIN ====================

    private LogicalPlan buildJoin(LogicalPlan left, SqlBaseParser.JoinRelationContext ctx) {
        LogicalPlan right = (LogicalPlan) visit(ctx.right);

        // Determine join type
        Join.JoinType joinType = resolveJoinType(ctx.joinType());

        // Handle NATURAL joins
        if (ctx.NATURAL() != null) {
            // Natural joins use USING with all common columns.
            // We pass through as raw SQL since the column names aren't known at parse time.
            String rightSql = generateNodeSql(right);
            String joinKeyword = joinTypeToSQL(joinType);
            return new SQLRelation("(" + generateNodeSql(left) + ") NATURAL " +
                joinKeyword + " (" + rightSql + ")");
        }

        // Join condition
        Expression condition;
        List<String> usingColumns = Collections.emptyList();
        if (ctx.joinCriteria() != null) {
            if (ctx.joinCriteria().booleanExpression() != null) {
                condition = visitBooleanExpr(ctx.joinCriteria().booleanExpression());
            } else if (ctx.joinCriteria().identifierList() != null) {
                // USING clause
                usingColumns = resolveIdentifierSeq(
                    ctx.joinCriteria().identifierList().identifierSeq());
                // Build equi-join condition from USING columns
                condition = buildUsingCondition(usingColumns);
            } else {
                condition = Literal.of(true);
            }
        } else if (joinType == Join.JoinType.CROSS) {
            condition = Literal.of(true);
        } else {
            condition = Literal.of(true);
        }

        return new Join(left, right, joinType, condition, usingColumns);
    }

    private Join.JoinType resolveJoinType(SqlBaseParser.JoinTypeContext ctx) {
        if (ctx.CROSS() != null) return Join.JoinType.CROSS;
        if (ctx.FULL() != null) return Join.JoinType.FULL;
        if (ctx.SEMI() != null) return Join.JoinType.LEFT_SEMI;
        if (ctx.ANTI() != null) return Join.JoinType.LEFT_ANTI;
        if (ctx.LEFT() != null) {
            if (ctx.SEMI() != null) return Join.JoinType.LEFT_SEMI;
            if (ctx.ANTI() != null) return Join.JoinType.LEFT_ANTI;
            return Join.JoinType.LEFT;
        }
        if (ctx.RIGHT() != null) return Join.JoinType.RIGHT;
        return Join.JoinType.INNER;
    }

    private String joinTypeToSQL(Join.JoinType joinType) {
        return switch (joinType) {
            case INNER -> "INNER JOIN";
            case LEFT -> "LEFT OUTER JOIN";
            case RIGHT -> "RIGHT OUTER JOIN";
            case FULL -> "FULL OUTER JOIN";
            case CROSS -> "CROSS JOIN";
            case LEFT_SEMI -> "SEMI JOIN";
            case LEFT_ANTI -> "ANTI JOIN";
        };
    }

    private Expression buildUsingCondition(List<String> columns) {
        Expression condition = null;
        for (String col : columns) {
            Expression eq = BinaryExpression.equal(
                new UnresolvedColumn(col),
                new UnresolvedColumn(col));
            condition = condition == null ? eq : BinaryExpression.and(condition, eq);
        }
        return condition != null ? condition : Literal.of(true);
    }

    // ==================== Projection (SELECT) ====================

    private LogicalPlan buildProjection(LogicalPlan source,
                                         SqlBaseParser.SelectClauseContext selectCtx) {
        boolean isDistinct = selectCtx.setQuantifier() != null &&
            selectCtx.setQuantifier().DISTINCT() != null;

        List<Expression> projections = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for (SqlBaseParser.NamedExpressionContext named :
                selectCtx.namedExpressionSeq().namedExpression()) {
            Expression expr = visitExpr(named.expression());

            // Handle alias
            String alias = null;
            if (named.name != null) {
                alias = getErrorCapturingIdentifierText(named.name);
            } else if (named.identifierList() != null) {
                // Multiple aliases - take first
                alias = getErrorCapturingIdentifierText(
                    named.identifierList().identifierSeq().ident.get(0));
            }

            // Handle AliasExpression wrapping
            if (alias != null) {
                expr = new AliasExpression(expr, alias);
            }

            projections.add(expr);
            aliases.add(alias);
        }

        // Check if this is just SELECT * (passthrough)
        if (projections.size() == 1 && projections.get(0) instanceof StarExpression) {
            if (isDistinct) {
                return new Distinct(source);
            }
            return source;
        }

        LogicalPlan result = new Project(source, projections, aliases);

        if (isDistinct) {
            result = new Distinct(result);
        }

        return result;
    }

    // ==================== Aggregation (GROUP BY) ====================

    private LogicalPlan buildAggregation(LogicalPlan source,
                                          SqlBaseParser.AggregationClauseContext aggCtx,
                                          SqlBaseParser.SelectClauseContext selectCtx,
                                          SqlBaseParser.HavingClauseContext havingCtx) {
        // For complex aggregation with GROUP BY, we build the entire query
        // as a SQLRelation using the original SQL text, because the Aggregate
        // LogicalPlan node has a specific structure that doesn't map 1:1 to
        // arbitrary SQL GROUP BY clauses.
        //
        // This is a pragmatic choice: the GROUP BY SQL passes through to DuckDB
        // which handles it natively. As we mature the parser, we can decompose
        // this into proper Aggregate nodes.

        // Reconstruct from SELECT through GROUP BY/HAVING using raw SQL
        StringBuilder sql = new StringBuilder();

        // SELECT clause
        sql.append(getOriginalText(selectCtx));

        // FROM clause is embedded in the source plan
        String sourceSql = generateNodeSql(source);
        sql.append(" FROM (").append(sourceSql).append(") AS _agg_source");

        // GROUP BY clause
        sql.append(" ").append(getOriginalText(aggCtx));

        // HAVING clause
        if (havingCtx != null) {
            sql.append(" ").append(getOriginalText(havingCtx));
        }

        return new SQLRelation(sql.toString());
    }

    // ==================== ORDER BY / LIMIT / OFFSET ====================

    private LogicalPlan applyQueryOrganization(LogicalPlan plan,
                                                SqlBaseParser.QueryOrganizationContext ctx) {
        if (ctx == null) return plan;

        // Apply ORDER BY
        if (!ctx.order.isEmpty()) {
            List<Sort.SortOrder> sortOrders = new ArrayList<>();
            for (SqlBaseParser.SortItemContext item : ctx.order) {
                Expression expr = visitExpr(item.expression());
                Sort.SortDirection direction = Sort.SortDirection.ASCENDING;
                Sort.NullOrdering nullOrdering;

                if (item.ordering != null && item.ordering.getType() ==
                        org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DESC) {
                    direction = Sort.SortDirection.DESCENDING;
                }

                // Determine null ordering
                if (item.nullOrder != null) {
                    if (item.nullOrder.getType() ==
                            org.apache.spark.sql.catalyst.parser.SqlBaseLexer.FIRST) {
                        nullOrdering = Sort.NullOrdering.NULLS_FIRST;
                    } else {
                        nullOrdering = Sort.NullOrdering.NULLS_LAST;
                    }
                } else {
                    // Spark default: NULLS FIRST for ASC, NULLS LAST for DESC
                    nullOrdering = direction == Sort.SortDirection.ASCENDING ?
                        Sort.NullOrdering.NULLS_FIRST : Sort.NullOrdering.NULLS_LAST;
                }

                sortOrders.add(new Sort.SortOrder(expr, direction, nullOrdering));
            }
            plan = new Sort(plan, sortOrders);
        }

        // Apply LIMIT
        if (ctx.limit != null) {
            long limitVal = evaluateLimitExpression(ctx.limit);
            long offsetVal = 0;
            if (ctx.offset != null) {
                offsetVal = evaluateLimitExpression(ctx.offset);
            }
            plan = new Limit(plan, limitVal, offsetVal);
        } else if (ctx.offset != null) {
            // OFFSET without LIMIT (unusual but valid)
            long offsetVal = evaluateLimitExpression(ctx.offset);
            plan = new Limit(plan, Long.MAX_VALUE, offsetVal);
        }

        return plan;
    }

    private long evaluateLimitExpression(SqlBaseParser.ExpressionContext ctx) {
        Expression expr = visitExpr(ctx);
        if (expr instanceof Literal lit && lit.value() instanceof Number num) {
            return num.longValue();
        }
        // Non-literal LIMIT: use a large default
        return Long.MAX_VALUE;
    }

    // ==================== Expression Visitors ====================

    /**
     * Top-level expression visitor dispatcher.
     */
    private Expression visitExpr(SqlBaseParser.ExpressionContext ctx) {
        return visitBooleanExpr(ctx.booleanExpression());
    }

    private Expression visitBooleanExpr(SqlBaseParser.BooleanExpressionContext ctx) {
        if (ctx instanceof SqlBaseParser.LogicalNotContext not) {
            Expression child = visitBooleanExpr(not.booleanExpression());
            return new UnaryExpression(UnaryExpression.Operator.NOT, child);
        }

        if (ctx instanceof SqlBaseParser.LogicalBinaryContext bin) {
            Expression left = visitBooleanExpr(bin.left);
            Expression right = visitBooleanExpr(bin.right);
            if (bin.operator.getType() ==
                    org.apache.spark.sql.catalyst.parser.SqlBaseLexer.AND) {
                return BinaryExpression.and(left, right);
            } else {
                return BinaryExpression.or(left, right);
            }
        }

        if (ctx instanceof SqlBaseParser.ExistsContext exists) {
            LogicalPlan subquery = visitQuery(exists.query());
            return new ExistsSubquery(subquery);
        }

        if (ctx instanceof SqlBaseParser.PredicatedContext pred) {
            Expression value = visitValueExpr(pred.valueExpression());

            // Apply predicate (BETWEEN, IN, LIKE, IS NULL, etc.)
            if (pred.predicate() != null) {
                return applyPredicate(value, pred.predicate());
            }

            return value;
        }

        throw new UnsupportedOperationException(
            "Unsupported boolean expression: " + ctx.getClass().getSimpleName());
    }

    private Expression applyPredicate(Expression value,
                                       SqlBaseParser.PredicateContext pred) {
        boolean negated = pred.errorCapturingNot() != null;

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.BETWEEN) {
            // BETWEEN lower AND upper
            Expression lower = visitValueExpr(pred.lower);
            Expression upper = visitValueExpr(pred.upper);
            Expression result = new RawSQLExpression(
                value.toSQL() + " BETWEEN " + lower.toSQL() + " AND " + upper.toSQL());
            if (negated) {
                return new RawSQLExpression(
                    value.toSQL() + " NOT BETWEEN " + lower.toSQL() + " AND " + upper.toSQL());
            }
            return result;
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.IN) {
            // IN (values) or IN (subquery)
            if (pred.query() != null) {
                LogicalPlan subquery = visitQuery(pred.query());
                Expression result = new InSubquery(value, subquery);
                if (negated) {
                    return new UnaryExpression(UnaryExpression.Operator.NOT, result);
                }
                return result;
            } else {
                List<Expression> values = new ArrayList<>();
                for (SqlBaseParser.ExpressionContext expr : pred.expression()) {
                    values.add(visitExpr(expr));
                }
                Expression result = new InExpression(value, values);
                if (negated) {
                    return new UnaryExpression(UnaryExpression.Operator.NOT, result);
                }
                return result;
            }
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.LIKE ||
            pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.ILIKE) {
            String op = pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.ILIKE ? "ILIKE" : "LIKE";
            Expression pattern = visitValueExpr(pred.pattern);
            String notStr = negated ? "NOT " : "";
            return new RawSQLExpression(
                value.toSQL() + " " + notStr + op + " " + pattern.toSQL());
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.RLIKE) {
            Expression pattern = visitValueExpr(pred.pattern);
            // rlike → regexp_matches via FunctionRegistry
            Expression rlike = new FunctionCall("rlike", List.of(value, pattern),
                com.thunderduck.types.BooleanType.get(), true);
            return negated ? UnaryExpression.not(rlike) : rlike;
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.NULL) {
            // IS [NOT] NULL — proper AST nodes with BooleanType and nullable=false
            return negated ? UnaryExpression.isNotNull(value) : UnaryExpression.isNull(value);
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.TRUE ||
            pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.FALSE ||
            pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.UNKNOWN) {
            String keyword = pred.kind.getText().toUpperCase();
            String notStr = negated ? "NOT " : "";
            return new RawSQLExpression(
                value.toSQL() + " IS " + notStr + keyword);
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DISTINCT) {
            // IS [NOT] DISTINCT FROM
            Expression right = visitValueExpr(pred.right);
            String notStr = negated ? "NOT " : "";
            return new RawSQLExpression(
                value.toSQL() + " IS " + notStr + "DISTINCT FROM " + right.toSQL());
        }

        throw new UnsupportedOperationException(
            "Unsupported predicate: " + pred.kind.getText());
    }

    // ==================== Value Expressions ====================

    private Expression visitValueExpr(SqlBaseParser.ValueExpressionContext ctx) {
        if (ctx instanceof SqlBaseParser.ValueExpressionDefaultContext def) {
            return visitPrimaryExpr(def.primaryExpression());
        }

        if (ctx instanceof SqlBaseParser.ArithmeticUnaryContext unary) {
            Expression child = visitValueExpr(unary.valueExpression());
            if (unary.operator.getType() ==
                    org.apache.spark.sql.catalyst.parser.SqlBaseLexer.MINUS) {
                return new UnaryExpression(UnaryExpression.Operator.NEGATE, child);
            }
            if (unary.operator.getType() ==
                    org.apache.spark.sql.catalyst.parser.SqlBaseLexer.TILDE) {
                return new UnaryExpression(UnaryExpression.Operator.BITWISE_NOT, child);
            }
            // PLUS is identity
            return child;
        }

        if (ctx instanceof SqlBaseParser.ArithmeticBinaryContext bin) {
            Expression left = visitValueExpr(bin.left);
            Expression right = visitValueExpr(bin.right);
            BinaryExpression.Operator op = resolveArithmeticOp(bin.operator);
            return new BinaryExpression(left, op, right);
        }

        if (ctx instanceof SqlBaseParser.ComparisonContext cmp) {
            Expression left = visitValueExpr(cmp.left);
            Expression right = visitValueExpr(cmp.right);
            BinaryExpression.Operator op = resolveComparisonOp(cmp.comparisonOperator());
            return new BinaryExpression(left, op, right);
        }

        throw new UnsupportedOperationException(
            "Unsupported value expression: " + ctx.getClass().getSimpleName());
    }

    private BinaryExpression.Operator resolveArithmeticOp(Token op) {
        return switch (op.getType()) {
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.PLUS ->
                BinaryExpression.Operator.ADD;
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.MINUS ->
                BinaryExpression.Operator.SUBTRACT;
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.ASTERISK ->
                BinaryExpression.Operator.MULTIPLY;
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.SLASH ->
                BinaryExpression.Operator.DIVIDE;
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.PERCENT ->
                BinaryExpression.Operator.MODULO;
            case org.apache.spark.sql.catalyst.parser.SqlBaseLexer.CONCAT_PIPE ->
                BinaryExpression.Operator.CONCAT;
            default -> throw new UnsupportedOperationException(
                "Unsupported arithmetic operator: " + op.getText());
        };
    }

    private BinaryExpression.Operator resolveComparisonOp(
            SqlBaseParser.ComparisonOperatorContext ctx) {
        if (ctx.EQ() != null) return BinaryExpression.Operator.EQUAL;
        if (ctx.NEQ() != null || ctx.NEQJ() != null) return BinaryExpression.Operator.NOT_EQUAL;
        if (ctx.LT() != null) return BinaryExpression.Operator.LESS_THAN;
        if (ctx.LTE() != null) return BinaryExpression.Operator.LESS_THAN_OR_EQUAL;
        if (ctx.GT() != null) return BinaryExpression.Operator.GREATER_THAN;
        if (ctx.GTE() != null) return BinaryExpression.Operator.GREATER_THAN_OR_EQUAL;
        throw new UnsupportedOperationException(
            "Unsupported comparison operator: " + ctx.getText());
    }

    // ==================== Primary Expressions ====================

    private Expression visitPrimaryExpr(SqlBaseParser.PrimaryExpressionContext ctx) {
        // CAST / TRY_CAST
        if (ctx instanceof SqlBaseParser.CastContext cast) {
            Expression expr = visitExpr(cast.expression());
            DataType targetType = resolveDataType(cast.dataType());
            return new CastExpression(expr, targetType);
        }

        // CAST by :: operator
        if (ctx instanceof SqlBaseParser.CastByColonContext castByColon) {
            Expression expr = visitPrimaryExpr(castByColon.primaryExpression());
            DataType targetType = resolveDataType(castByColon.dataType());
            return new CastExpression(expr, targetType);
        }

        // Constants (NULL, numbers, strings, booleans, intervals, typed literals)
        if (ctx instanceof SqlBaseParser.ConstantDefaultContext constCtx) {
            return visitConstant(constCtx.constant());
        }

        // Column reference
        if (ctx instanceof SqlBaseParser.ColumnReferenceContext colRef) {
            String name = getIdentifierText(colRef.identifier());
            return new UnresolvedColumn(name);
        }

        // Qualified column (table.column)
        if (ctx instanceof SqlBaseParser.DereferenceContext deref) {
            Expression base = visitPrimaryExpr(deref.primaryExpression());
            String field = getIdentifierText(deref.fieldName);
            if (base instanceof UnresolvedColumn col) {
                return new UnresolvedColumn(field, col.columnName());
            }
            // Complex dereference - use raw SQL
            return new RawSQLExpression(base.toSQL() + "." + quoteIdentifierIfNeeded(field));
        }

        // Star expression: * or table.*
        if (ctx instanceof SqlBaseParser.StarContext star) {
            if (star.qualifiedName() != null) {
                String qualifier = resolveQualifiedName(star.qualifiedName());
                return new StarExpression(qualifier);
            }
            return new StarExpression();
        }

        // Function call
        if (ctx instanceof SqlBaseParser.FunctionCallContext funcCtx) {
            return visitFunctionCallExpr(funcCtx);
        }

        // CASE WHEN
        if (ctx instanceof SqlBaseParser.SearchedCaseContext searched) {
            return buildSearchedCase(searched);
        }

        if (ctx instanceof SqlBaseParser.SimpleCaseContext simple) {
            return buildSimpleCase(simple);
        }

        // Subquery expression
        if (ctx instanceof SqlBaseParser.SubqueryExpressionContext sub) {
            LogicalPlan subquery = visitQuery(sub.query());
            return new ScalarSubquery(subquery);
        }

        // Parenthesized expression
        if (ctx instanceof SqlBaseParser.ParenthesizedExpressionContext paren) {
            return visitExpr(paren.expression());
        }

        // Row constructor: (a, b, c)
        if (ctx instanceof SqlBaseParser.RowConstructorContext row) {
            List<String> parts = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext named : row.namedExpression()) {
                Expression e = visitExpr(named.expression());
                parts.add(e.toSQL());
            }
            return new RawSQLExpression("(" + String.join(", ", parts) + ")");
        }

        // EXTRACT(field FROM source)
        if (ctx instanceof SqlBaseParser.ExtractContext extract) {
            String field = getIdentifierText(extract.field);
            Expression source = visitValueExpr(extract.source);
            return new RawSQLExpression(
                "EXTRACT(" + field + " FROM " + source.toSQL() + ")");
        }

        // SUBSTRING
        if (ctx instanceof SqlBaseParser.SubstringContext substr) {
            Expression str = visitValueExpr(substr.str);
            Expression pos = visitValueExpr(substr.pos);
            if (substr.len != null) {
                Expression len = visitValueExpr(substr.len);
                return new FunctionCall("substring",
                    List.of(str, pos, len), StringType.get());
            }
            return new FunctionCall("substring",
                List.of(str, pos), StringType.get());
        }

        // TRIM
        if (ctx instanceof SqlBaseParser.TrimContext trim) {
            Expression source = visitValueExpr(trim.srcStr);
            if (trim.trimStr != null) {
                Expression trimStr = visitValueExpr(trim.trimStr);
                String option = "BOTH";
                if (trim.trimOption != null) {
                    option = trim.trimOption.getText().toUpperCase();
                }
                return new RawSQLExpression(
                    "TRIM(" + option + " " + trimStr.toSQL() +
                    " FROM " + source.toSQL() + ")");
            }
            return new FunctionCall("trim", List.of(source), StringType.get());
        }

        // POSITION(substr IN str)
        if (ctx instanceof SqlBaseParser.PositionContext pos) {
            Expression substr = visitValueExpr(pos.substr);
            Expression str = visitValueExpr(pos.str);
            return new FunctionCall("position",
                List.of(substr, str), IntegerType.get());
        }

        // OVERLAY
        if (ctx instanceof SqlBaseParser.OverlayContext overlay) {
            Expression input = visitValueExpr(overlay.input);
            Expression replace = visitValueExpr(overlay.replace);
            Expression position = visitValueExpr(overlay.position);
            if (overlay.length != null) {
                Expression length = visitValueExpr(overlay.length);
                return new RawSQLExpression(
                    "OVERLAY(" + input.toSQL() + " PLACING " + replace.toSQL() +
                    " FROM " + position.toSQL() + " FOR " + length.toSQL() + ")");
            }
            return new RawSQLExpression(
                "OVERLAY(" + input.toSQL() + " PLACING " + replace.toSQL() +
                " FROM " + position.toSQL() + ")");
        }

        // STRUCT constructor
        if (ctx instanceof SqlBaseParser.StructContext structCtx) {
            List<String> parts = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext named : structCtx.argument) {
                Expression e = visitExpr(named.expression());
                String alias = null;
                if (named.name != null) {
                    alias = getErrorCapturingIdentifierText(named.name);
                }
                if (alias != null) {
                    parts.add(alias + " := " + e.toSQL());
                } else {
                    parts.add(e.toSQL());
                }
            }
            return new RawSQLExpression("struct_pack(" + String.join(", ", parts) + ")");
        }

        // CURRENT_DATE, CURRENT_TIMESTAMP, etc.
        if (ctx instanceof SqlBaseParser.CurrentLikeContext current) {
            return new RawSQLExpression(current.name.getText());
        }

        // Array subscript: expr[index]
        if (ctx instanceof SqlBaseParser.SubscriptContext sub) {
            Expression base = visitPrimaryExpr(sub.value);
            Expression index = visitValueExpr(sub.index);
            return ExtractValueExpression.arrayElement(base, index);
        }

        // Lambda expressions
        if (ctx instanceof SqlBaseParser.LambdaContext) {
            // Pass through as raw SQL for now
            return new RawSQLExpression(getOriginalText(ctx));
        }

        // FIRST/LAST/ANY_VALUE
        if (ctx instanceof SqlBaseParser.FirstContext first) {
            Expression arg = visitExpr(first.expression());
            return new FunctionCall("first", List.of(arg), arg.dataType());
        }
        if (ctx instanceof SqlBaseParser.LastContext last) {
            Expression arg = visitExpr(last.expression());
            return new FunctionCall("last", List.of(arg), arg.dataType());
        }
        if (ctx instanceof SqlBaseParser.Any_valueContext anyVal) {
            Expression arg = visitExpr(anyVal.expression());
            return new FunctionCall("any_value", List.of(arg), arg.dataType());
        }

        // TIMESTAMPADD / TIMESTAMPDIFF
        if (ctx instanceof SqlBaseParser.TimestampaddContext ||
            ctx instanceof SqlBaseParser.TimestampdiffContext) {
            return new RawSQLExpression(getOriginalText(ctx));
        }

        // Fallback: use raw SQL text
        logger.debug("Falling back to raw SQL for primary expression: {}",
            ctx.getClass().getSimpleName());
        return new RawSQLExpression(getOriginalText(ctx));
    }

    // ==================== Function Calls ====================

    private Expression visitFunctionCallExpr(SqlBaseParser.FunctionCallContext ctx) {
        String funcName = resolveFunctionName(ctx.functionName());

        // Collect arguments
        List<Expression> args = new ArrayList<>();
        boolean isDistinct = ctx.setQuantifier() != null &&
            ctx.setQuantifier().DISTINCT() != null;

        for (SqlBaseParser.FunctionArgumentContext argCtx : ctx.argument) {
            if (argCtx.expression() != null) {
                args.add(visitExpr(argCtx.expression()));
            } else if (argCtx.namedArgumentExpression() != null) {
                // Named argument: key => value
                Expression val = visitExpr(argCtx.namedArgumentExpression().value);
                args.add(val);
            }
        }

        // Handle COUNT(*)
        if (funcName.equalsIgnoreCase("count") && args.isEmpty() &&
            ctx.argument.isEmpty() && ctx.getText().contains("*")) {
            // count(*) - use Literal("*") as the argument for proper AST representation
            args = List.of(new RawSQLExpression("*"));
        }

        boolean hasFilter = ctx.where != null;
        boolean hasWithinGroup = ctx.sortItem() != null && !ctx.sortItem().isEmpty();
        boolean hasWindow = ctx.windowSpec() != null;
        boolean hasNullsOption = ctx.nullsOption != null;
        boolean hasModifiers = hasFilter || hasWithinGroup || hasWindow || hasNullsOption;

        // Simple case: no modifiers — return a proper FunctionCall AST node.
        // This enables proper name translation via FunctionRegistry.translate() and
        // preserves type information for downstream consumers.
        if (!hasModifiers) {
            return new FunctionCall(funcName, args,
                UnresolvedType.expressionString(), true, isDistinct);
        }

        // Complex case: function has SQL modifiers (FILTER, OVER, WITHIN GROUP, NULLS).
        // Build the base function call via FunctionCall.toSQL() for proper name translation,
        // then append modifiers and wrap in RawSQLExpression.
        FunctionCall baseCall = new FunctionCall(funcName, args,
            UnresolvedType.expressionString(), true, isDistinct);
        StringBuilder sql = new StringBuilder(baseCall.toSQL());

        // Handle FILTER clause
        if (hasFilter) {
            Expression filterExpr = visitBooleanExpr(ctx.where);
            sql.append(" FILTER (WHERE ").append(filterExpr.toSQL()).append(")");
        }

        // Handle WITHIN GROUP
        if (hasWithinGroup) {
            sql.append(" WITHIN GROUP (ORDER BY ");
            for (int i = 0; i < ctx.sortItem().size(); i++) {
                if (i > 0) sql.append(", ");
                SqlBaseParser.SortItemContext si = ctx.sortItem(i);
                sql.append(visitExpr(si.expression()).toSQL());
                if (si.ordering != null) {
                    sql.append(si.ordering.getType() ==
                        org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DESC ?
                        " DESC" : " ASC");
                }
            }
            sql.append(")");
        }

        // Handle OVER (window function)
        if (hasWindow) {
            sql.append(" OVER ");
            sql.append(buildWindowSpec(ctx.windowSpec()));
        }

        // Handle IGNORE NULLS / RESPECT NULLS
        if (hasNullsOption) {
            sql.append(" ");
            sql.append(ctx.nullsOption.getText().toUpperCase());
            sql.append(" NULLS");
        }

        return new RawSQLExpression(sql.toString());
    }

    private String buildWindowSpec(SqlBaseParser.WindowSpecContext ctx) {
        if (ctx instanceof SqlBaseParser.WindowRefContext ref) {
            return getErrorCapturingIdentifierText(ref.name);
        }

        if (ctx instanceof SqlBaseParser.WindowDefContext def) {
            StringBuilder sql = new StringBuilder("(");

            // PARTITION BY
            if (!def.partition.isEmpty()) {
                sql.append("PARTITION BY ");
                for (int i = 0; i < def.partition.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(visitExpr(def.partition.get(i)).toSQL());
                }
            }

            // ORDER BY
            if (def.sortItem() != null && !def.sortItem().isEmpty()) {
                if (!def.partition.isEmpty()) sql.append(" ");
                sql.append("ORDER BY ");
                for (int i = 0; i < def.sortItem().size(); i++) {
                    if (i > 0) sql.append(", ");
                    SqlBaseParser.SortItemContext si = def.sortItem(i);
                    sql.append(visitExpr(si.expression()).toSQL());
                    if (si.ordering != null) {
                        sql.append(si.ordering.getType() ==
                            org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DESC ?
                            " DESC" : " ASC");
                    }
                    if (si.nullOrder != null) {
                        sql.append(si.nullOrder.getType() ==
                            org.apache.spark.sql.catalyst.parser.SqlBaseLexer.FIRST ?
                            " NULLS FIRST" : " NULLS LAST");
                    }
                }
            }

            // Window frame
            if (def.windowFrame() != null) {
                sql.append(" ").append(buildWindowFrame(def.windowFrame()));
            }

            sql.append(")");
            return sql.toString();
        }

        return getOriginalText(ctx);
    }

    private String buildWindowFrame(SqlBaseParser.WindowFrameContext ctx) {
        StringBuilder sql = new StringBuilder();
        String frameType = ctx.frameType.getText().toUpperCase();

        if (ctx.BETWEEN() != null) {
            sql.append(frameType).append(" BETWEEN ");
            sql.append(buildFrameBound(ctx.start)).append(" AND ");
            sql.append(buildFrameBound(ctx.end));
        } else {
            sql.append(frameType).append(" ");
            sql.append(buildFrameBound(ctx.start));
        }

        return sql.toString();
    }

    private String buildFrameBound(SqlBaseParser.FrameBoundContext ctx) {
        if (ctx.UNBOUNDED() != null) {
            return "UNBOUNDED " + ctx.boundType.getText().toUpperCase();
        }
        if (ctx.CURRENT() != null) {
            return "CURRENT ROW";
        }
        // Expression-based bound
        Expression expr = visitExpr(ctx.expression());
        return expr.toSQL() + " " + ctx.boundType.getText().toUpperCase();
    }

    // ==================== CASE WHEN ====================

    private Expression buildSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {
        List<Expression> conditions = new ArrayList<>();
        List<Expression> thenBranches = new ArrayList<>();

        for (SqlBaseParser.WhenClauseContext whenCtx : ctx.whenClause()) {
            conditions.add(visitExpr(whenCtx.condition));
            thenBranches.add(visitExpr(whenCtx.result));
        }

        Expression elseBranch = null;
        if (ctx.elseExpression != null) {
            elseBranch = visitExpr(ctx.elseExpression);
        }

        return new CaseWhenExpression(conditions, thenBranches, elseBranch);
    }

    private Expression buildSimpleCase(SqlBaseParser.SimpleCaseContext ctx) {
        Expression caseValue = visitExpr(ctx.value);

        List<Expression> conditions = new ArrayList<>();
        List<Expression> thenBranches = new ArrayList<>();

        for (SqlBaseParser.WhenClauseContext whenCtx : ctx.whenClause()) {
            // Simple CASE: CASE x WHEN val THEN ... becomes CASE WHEN x = val THEN ...
            Expression whenVal = visitExpr(whenCtx.condition);
            conditions.add(BinaryExpression.equal(caseValue, whenVal));
            thenBranches.add(visitExpr(whenCtx.result));
        }

        Expression elseBranch = null;
        if (ctx.elseExpression != null) {
            elseBranch = visitExpr(ctx.elseExpression);
        }

        return new CaseWhenExpression(conditions, thenBranches, elseBranch);
    }

    // ==================== Constants / Literals ====================

    private Expression visitConstant(SqlBaseParser.ConstantContext ctx) {
        if (ctx instanceof SqlBaseParser.NullLiteralContext) {
            return Literal.nullValue(UnresolvedType.expressionString());
        }

        if (ctx instanceof SqlBaseParser.BooleanLiteralContext bool) {
            boolean val = bool.booleanValue().TRUE() != null;
            return Literal.of(val);
        }

        if (ctx instanceof SqlBaseParser.StringLiteralContext str) {
            // Concatenate all string parts
            StringBuilder sb = new StringBuilder();
            for (SqlBaseParser.StringLitContext lit : str.stringLit()) {
                sb.append(getStringLitValue(lit));
            }
            return Literal.of(sb.toString());
        }

        if (ctx instanceof SqlBaseParser.NumericLiteralContext num) {
            return visitNumber(num.number());
        }

        if (ctx instanceof SqlBaseParser.TypeConstructorContext typeCon) {
            // Typed literal: DATE '2025-01-15', TIMESTAMP '...', etc.
            String litType = typeCon.literalType().getText().toUpperCase();
            String value = getStringLitValue(typeCon.stringLit());

            return switch (litType) {
                case "DATE" -> new Literal(value, DateType.get());
                case "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ" ->
                    new Literal(value, TimestampType.get());
                default ->
                    // Other typed literals - pass through as raw SQL
                    new RawSQLExpression(litType + " '" + value + "'");
            };
        }

        if (ctx instanceof SqlBaseParser.IntervalLiteralContext interval) {
            // Pass interval through as raw SQL - DuckDB supports INTERVAL syntax
            return new RawSQLExpression(getOriginalText(interval));
        }

        // Fallback
        return new RawSQLExpression(getOriginalText(ctx));
    }

    private Expression visitNumber(SqlBaseParser.NumberContext ctx) {
        String text = ctx.getText();

        if (ctx instanceof SqlBaseParser.IntegerLiteralContext) {
            try {
                long val = Long.parseLong(text);
                if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                    return Literal.of((int) val);
                }
                return Literal.of(val);
            } catch (NumberFormatException e) {
                return new RawSQLExpression(text);
            }
        }

        if (ctx instanceof SqlBaseParser.DecimalLiteralContext) {
            return new RawSQLExpression(text);
        }

        if (ctx instanceof SqlBaseParser.DoubleLiteralContext) {
            try {
                return Literal.of(Double.parseDouble(text.replace("D", "").replace("d", "")));
            } catch (NumberFormatException e) {
                return new RawSQLExpression(text);
            }
        }

        if (ctx instanceof SqlBaseParser.FloatLiteralContext) {
            try {
                return Literal.of(Float.parseFloat(text.replace("F", "").replace("f", "")));
            } catch (NumberFormatException e) {
                return new RawSQLExpression(text);
            }
        }

        if (ctx instanceof SqlBaseParser.BigIntLiteralContext) {
            try {
                return Literal.of(Long.parseLong(text.replace("L", "").replace("l", "")));
            } catch (NumberFormatException e) {
                return new RawSQLExpression(text);
            }
        }

        if (ctx instanceof SqlBaseParser.ExponentLiteralContext) {
            try {
                return Literal.of(Double.parseDouble(text));
            } catch (NumberFormatException e) {
                return new RawSQLExpression(text);
            }
        }

        // Fallback for other numeric types
        return new RawSQLExpression(text);
    }

    // ==================== Data Type Resolution ====================

    /**
     * Converts SparkSQL data type syntax to Thunderduck DataType.
     */
    private DataType resolveDataType(SqlBaseParser.DataTypeContext ctx) {
        if (ctx instanceof SqlBaseParser.PrimitiveDataTypeContext prim) {
            String typeName = prim.type().getText().toUpperCase();

            // Handle precision/scale for DECIMAL
            List<TerminalNode> intValues = prim.INTEGER_VALUE();

            return switch (typeName) {
                case "BOOLEAN" -> BooleanType.get();
                case "TINYINT", "BYTE" -> ByteType.get();
                case "SMALLINT", "SHORT" -> ShortType.get();
                case "INT", "INTEGER" -> IntegerType.get();
                case "BIGINT", "LONG" -> LongType.get();
                case "FLOAT", "REAL" -> FloatType.get();
                case "DOUBLE" -> DoubleType.get();
                case "STRING" -> StringType.get();
                case "VARCHAR", "CHAR", "CHARACTER" -> {
                    // VARCHAR(n) or CHAR(n) - map to STRING in Spark
                    yield StringType.get();
                }
                case "DATE" -> DateType.get();
                case "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ" -> TimestampType.get();
                case "BINARY" -> BinaryType.get();
                case "DECIMAL", "DEC", "NUMERIC" -> {
                    int precision = 10; // Spark default
                    int scale = 0;      // Spark default
                    if (intValues != null && !intValues.isEmpty()) {
                        precision = Integer.parseInt(intValues.get(0).getText());
                        if (intValues.size() > 1) {
                            scale = Integer.parseInt(intValues.get(1).getText());
                        }
                    }
                    yield new DecimalType(precision, scale);
                }
                case "VOID" -> UnresolvedType.expressionString();
                default -> {
                    logger.debug("Unknown type: {}, using UnresolvedType", typeName);
                    yield UnresolvedType.expressionString();
                }
            };
        }

        // Complex types: ARRAY<T>, MAP<K,V>, STRUCT<...>
        // Pass through as unresolved for now
        return UnresolvedType.expressionString();
    }

    // ==================== Identifier Helpers ====================

    private String resolveIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx) {
        if (ctx.multipartIdentifier() != null) {
            return resolveMultipartIdentifier(ctx.multipartIdentifier());
        }
        // IDENTIFIER(expr) - dynamic identifier
        return getOriginalText(ctx);
    }

    private String resolveMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ctx.parts.size(); i++) {
            if (i > 0) sb.append(".");
            sb.append(getErrorCapturingIdentifierText(ctx.parts.get(i)));
        }
        return sb.toString();
    }

    private String resolveQualifiedName(SqlBaseParser.QualifiedNameContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ctx.identifier().size(); i++) {
            if (i > 0) sb.append(".");
            sb.append(getIdentifierText(ctx.identifier(i)));
        }
        return sb.toString();
    }

    private String resolveFunctionName(SqlBaseParser.FunctionNameContext ctx) {
        if (ctx.qualifiedName() != null) {
            return resolveQualifiedName(ctx.qualifiedName());
        }
        if (ctx.FILTER() != null) return "filter";
        if (ctx.LEFT() != null) return "left";
        if (ctx.RIGHT() != null) return "right";
        if (ctx.identFunc != null) return "IDENTIFIER";
        return ctx.getText();
    }

    private List<String> resolveIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx) {
        List<String> result = new ArrayList<>();
        for (SqlBaseParser.ErrorCapturingIdentifierContext id : ctx.ident) {
            result.add(getErrorCapturingIdentifierText(id));
        }
        return result;
    }

    private String getIdentifierText(SqlBaseParser.IdentifierContext ctx) {
        if (ctx.strictIdentifier() != null) {
            return getIdentifierText(ctx.strictIdentifier());
        }
        if (ctx.strictNonReserved() != null) {
            return ctx.strictNonReserved().getText();
        }
        return ctx.getText();
    }

    private String getIdentifierText(SqlBaseParser.StrictIdentifierContext ctx) {
        if (ctx instanceof SqlBaseParser.UnquotedIdentifierContext unquoted) {
            return unquoted.getText();
        }
        if (ctx instanceof SqlBaseParser.QuotedIdentifierAlternativeContext quoted) {
            return getQuotedIdentifierText(quoted.quotedIdentifier());
        }
        return ctx.getText();
    }

    private String getErrorCapturingIdentifierText(
            SqlBaseParser.ErrorCapturingIdentifierContext ctx) {
        return getIdentifierText(ctx.identifier());
    }

    private String getQuotedIdentifierText(SqlBaseParser.QuotedIdentifierContext ctx) {
        if (ctx.BACKQUOTED_IDENTIFIER() != null) {
            String text = ctx.BACKQUOTED_IDENTIFIER().getText();
            // Remove surrounding backticks
            return text.substring(1, text.length() - 1).replace("``", "`");
        }
        if (ctx.DOUBLEQUOTED_STRING() != null) {
            String text = ctx.DOUBLEQUOTED_STRING().getText();
            return text.substring(1, text.length() - 1).replace("\\\"", "\"");
        }
        return ctx.getText();
    }

    private String getStringLitValue(SqlBaseParser.StringLitContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            String text = ctx.STRING_LITERAL().getText();
            // Remove surrounding quotes and unescape
            return text.substring(1, text.length() - 1).replace("''", "'");
        }
        if (ctx.DOUBLEQUOTED_STRING() != null) {
            String text = ctx.DOUBLEQUOTED_STRING().getText();
            return text.substring(1, text.length() - 1).replace("\\\"", "\"");
        }
        return ctx.getText();
    }

    // ==================== Utility Methods ====================

    /**
     * Gets the original SQL text for a parse tree node.
     */
    private String getOriginalText(ParseTree ctx) {
        if (ctx instanceof org.antlr.v4.runtime.ParserRuleContext prc) {
            if (prc.start != null && prc.stop != null) {
                return prc.start.getInputStream().getText(
                    new org.antlr.v4.runtime.misc.Interval(
                        prc.start.getStartIndex(), prc.stop.getStopIndex()));
            }
        }
        return ctx.getText();
    }

    /**
     * Quotes an identifier if it needs quoting (contains special chars, is a keyword, etc.).
     */
    private String quoteIdentifierIfNeeded(String identifier) {
        // Use the existing SQLQuoting utility
        return com.thunderduck.generator.SQLQuoting.quoteIdentifierIfNeeded(identifier);
    }

    /**
     * Generates SQL text from a LogicalPlan node.
     */
    private String generateNodeSql(LogicalPlan plan) {
        com.thunderduck.generator.SQLGenerator generator =
            new com.thunderduck.generator.SQLGenerator();
        return generator.generate(plan);
    }
}
