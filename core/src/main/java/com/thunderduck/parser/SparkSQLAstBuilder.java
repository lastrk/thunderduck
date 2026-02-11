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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
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

    /**
     * Optional DuckDB connection for resolving table schemas.
     * When non-null, table references are resolved to their actual column schemas
     * via PRAGMA table_info, enabling inferSchema() for all plan shapes.
     */
    private final Connection connection;

    /**
     * Creates an AST builder without schema resolution (backward compatible).
     */
    public SparkSQLAstBuilder() {
        this(null);
    }

    /**
     * Creates an AST builder with optional schema resolution.
     *
     * @param connection DuckDB connection for table schema resolution (can be null)
     */
    public SparkSQLAstBuilder(Connection connection) {
        this.connection = connection;
    }

    // ==================== Entry Points ====================

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return (LogicalPlan) visit(ctx.statement());
    }

    @Override
    public LogicalPlan visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        return visitQuery(ctx.query());
    }

    // ==================== DDL/DML Statements ====================

    @Override
    public LogicalPlan visitCreateTable(SqlBaseParser.CreateTableContext ctx) {
        StringBuilder sql = new StringBuilder();

        // CREATE TABLE [IF NOT EXISTS] tableName
        SqlBaseParser.CreateTableHeaderContext header = ctx.createTableHeader();
        sql.append("CREATE TABLE ");
        if (header.IF() != null && header.EXISTS() != null) {
            sql.append("IF NOT EXISTS ");
        }
        String tableName = resolveIdentifierReference(header.identifierReference());
        sql.append(quoteIdentifierIfNeeded(tableName));

        // Column definitions: (col1 type1, col2 type2, ...)
        if (ctx.colDefinitionList() != null) {
            sql.append(" (");
            List<SqlBaseParser.ColDefinitionContext> colDefs = ctx.colDefinitionList().colDefinition();
            for (int i = 0; i < colDefs.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(buildColumnDefinition(colDefs.get(i)));
            }
            sql.append(")");
        }

        // Handle CTAS: CREATE TABLE ... AS SELECT ...
        if (ctx.query() != null) {
            LogicalPlan queryPlan = visitQuery(ctx.query());
            String querySql = generateNodeSql(queryPlan);
            sql.append(" AS ").append(querySql);
        }

        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitDropTable(SqlBaseParser.DropTableContext ctx) {
        StringBuilder sql = new StringBuilder("DROP TABLE ");
        if (ctx.IF() != null && ctx.EXISTS() != null) {
            sql.append("IF EXISTS ");
        }
        String tableName = resolveIdentifierReference(ctx.identifierReference());
        sql.append(quoteIdentifierIfNeeded(tableName));
        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitTruncateTable(SqlBaseParser.TruncateTableContext ctx) {
        StringBuilder sql = new StringBuilder("TRUNCATE TABLE ");
        String tableName = resolveIdentifierReference(ctx.identifierReference());
        sql.append(quoteIdentifierIfNeeded(tableName));
        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitCreateView(SqlBaseParser.CreateViewContext ctx) {
        StringBuilder sql = new StringBuilder("CREATE ");
        if (ctx.OR() != null && ctx.REPLACE() != null) {
            sql.append("OR REPLACE ");
        }
        if (ctx.TEMPORARY() != null) {
            sql.append("TEMPORARY ");
        }
        if (ctx.GLOBAL() != null) {
            sql.append("GLOBAL ");
        }
        sql.append("VIEW ");
        if (ctx.IF() != null && ctx.EXISTS() != null) {
            sql.append("IF NOT EXISTS ");
        }
        String viewName = resolveIdentifierReference(ctx.identifierReference());
        sql.append(quoteIdentifierIfNeeded(viewName));

        // Column aliases for the view
        if (ctx.identifierCommentList() != null) {
            sql.append(" ").append(getOriginalText(ctx.identifierCommentList()));
        }

        // The inner query -- use full AST parsing
        if (ctx.query() != null) {
            LogicalPlan queryPlan = visitQuery(ctx.query());
            String querySql = generateNodeSql(queryPlan);
            sql.append(" AS ").append(querySql);
        }

        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitDropView(SqlBaseParser.DropViewContext ctx) {
        StringBuilder sql = new StringBuilder("DROP VIEW ");
        if (ctx.IF() != null && ctx.EXISTS() != null) {
            sql.append("IF EXISTS ");
        }
        String viewName = resolveIdentifierReference(ctx.identifierReference());
        sql.append(quoteIdentifierIfNeeded(viewName));
        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitDmlStatement(SqlBaseParser.DmlStatementContext ctx) {
        // DML statement wraps: optional WITH clause + dmlStatementNoWith
        // dmlStatementNoWith has alternatives: SingleInsertQuery, MultiInsertQuery, etc.
        SqlBaseParser.DmlStatementNoWithContext noWith = ctx.dmlStatementNoWith();

        if (noWith instanceof SqlBaseParser.SingleInsertQueryContext singleInsert) {
            return buildSingleInsert(singleInsert, ctx.ctes());
        }

        // Fallback: use the original SQL text for complex DML patterns
        return new RawDDLStatement(getOriginalText(ctx));
    }

    /**
     * Builds a single INSERT statement from parsed components.
     *
     * <p>Handles:
     * <ul>
     *   <li>INSERT INTO tableName query (INSERT INTO ... SELECT ...)</li>
     *   <li>INSERT INTO tableName VALUES (...) (parsed as inline table query)</li>
     * </ul>
     */
    private LogicalPlan buildSingleInsert(SqlBaseParser.SingleInsertQueryContext ctx,
                                           SqlBaseParser.CtesContext ctes) {
        StringBuilder sql = new StringBuilder();

        // Optional WITH clause
        if (ctes != null) {
            sql.append(getOriginalText(ctes)).append(" ");
        }

        // INSERT INTO clause
        SqlBaseParser.InsertIntoContext insertInto = ctx.insertInto();
        if (insertInto instanceof SqlBaseParser.InsertIntoTableContext insert) {
            sql.append("INSERT INTO ");
            String tableName = resolveIdentifierReference(insert.identifierReference());
            sql.append(quoteIdentifierIfNeeded(tableName));

            // Optional column list
            if (insert.identifierList() != null) {
                sql.append(" (");
                sql.append(getOriginalText(insert.identifierList()));
                sql.append(")");
            }
        } else if (insertInto instanceof SqlBaseParser.InsertOverwriteTableContext overwrite) {
            sql.append("INSERT OR REPLACE INTO ");
            String tableName = resolveIdentifierReference(overwrite.identifierReference());
            sql.append(quoteIdentifierIfNeeded(tableName));
        } else {
            // Fallback for other insert types (INSERT OVERWRITE DIR, etc.)
            sql.append(getOriginalText(insertInto));
        }

        // Query part (VALUES or SELECT)
        if (ctx.query() != null) {
            sql.append(" ");
            // Try to parse the query through the AST for proper transformation
            try {
                LogicalPlan queryPlan = visitQuery(ctx.query());
                String querySql = generateNodeSql(queryPlan);
                sql.append(querySql);
            } catch (Exception e) {
                // Fallback to original text if AST parsing fails
                logger.debug("INSERT query AST parsing failed, using original text: {}", e.getMessage());
                sql.append(getOriginalText(ctx.query()));
            }
        }

        return new RawDDLStatement(sql.toString());
    }

    @Override
    public LogicalPlan visitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        String tableName = resolveIdentifierReference(ctx.identifierReference());
        sql.append(quoteIdentifierIfNeeded(tableName));
        sql.append(" ADD COLUMN ");

        // Use original text for the column definitions since the grammar for
        // QualifiedColTypeWithPositionList is complex and rarely needs type mapping
        if (ctx.columns != null) {
            sql.append(getOriginalText(ctx.columns));
        }

        return new RawDDLStatement(sql.toString());
    }

    // ==================== DDL Helper Methods ====================

    /**
     * Builds a column definition string with Spark-to-DuckDB type mapping.
     *
     * <p>Converts: "name STRING" -> "name VARCHAR", "count LONG" -> "count BIGINT", etc.
     */
    private String buildColumnDefinition(SqlBaseParser.ColDefinitionContext colDef) {
        String colName = getErrorCapturingIdentifierText(colDef.colName);
        String quotedName = quoteIdentifierIfNeeded(colName);

        // Map the Spark data type to DuckDB type
        SqlBaseParser.DataTypeContext dataTypeCtx = colDef.dataType();
        String duckdbType = resolveDuckDBTypeString(dataTypeCtx);

        // Handle column options (NOT NULL, DEFAULT, etc.)
        StringBuilder result = new StringBuilder();
        result.append(quotedName).append(" ").append(duckdbType);

        if (colDef.colDefinitionOption() != null) {
            for (SqlBaseParser.ColDefinitionOptionContext opt : colDef.colDefinitionOption()) {
                result.append(" ").append(getOriginalText(opt));
            }
        }

        return result.toString();
    }

    /**
     * Resolves a SparkSQL DataType parse tree node to a DuckDB type string.
     *
     * <p>Uses the existing {@link #resolveDataType(SqlBaseParser.DataTypeContext)} to parse
     * into a Thunderduck DataType, then maps to DuckDB via {@link TypeMapper#toDuckDBType(DataType)}.
     * Falls back to the original text for complex types that don't need mapping.
     */
    private String resolveDuckDBTypeString(SqlBaseParser.DataTypeContext ctx) {
        try {
            DataType dataType = resolveDataType(ctx);
            return com.thunderduck.types.TypeMapper.toDuckDBType(dataType);
        } catch (Exception e) {
            // Fallback: use original text (e.g., for complex types like ARRAY<STRING>)
            return getOriginalText(ctx);
        }
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
        // Parse each CTE definition into a WithCTE.CTEDefinition
        List<WithCTE.CTEDefinition> definitions = new ArrayList<>();
        for (SqlBaseParser.NamedQueryContext nq : ctx.ctes().namedQuery()) {
            String name = getErrorCapturingIdentifierText(nq.name);
            LogicalPlan ctePlan = visitQuery(nq.query());

            List<String> columnAliases = Collections.emptyList();
            if (nq.columnAliases != null) {
                columnAliases = resolveIdentifierSeq(nq.columnAliases.identifierSeq());
            }

            definitions.add(new WithCTE.CTEDefinition(name, ctePlan, columnAliases));
        }

        // Parse the main query body (without the WITH prefix)
        LogicalPlan body = (LogicalPlan) visit(ctx.queryTerm());
        body = applyQueryOrganization(body, ctx.queryOrganization());

        return new WithCTE(definitions, body);
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

    // ==================== VALUES (InlineTable) ====================

    /**
     * Handles VALUES as a queryPrimary (e.g., VALUES (1, 'a'), (2, 'b')).
     * DuckDB supports VALUES natively, so we pass through as SQLRelation.
     */
    @Override
    public LogicalPlan visitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx) {
        return buildInlineTable(ctx.inlineTable());
    }

    /**
     * Handles VALUES in FROM clause (e.g., FROM VALUES (1, 'a') AS t(id, name)).
     * DuckDB supports VALUES natively, so we pass through as SQLRelation.
     */
    @Override
    public LogicalPlan visitInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx) {
        return buildInlineTable(ctx.inlineTable());
    }

    /**
     * Builds a SQLRelation from an InlineTable (VALUES) context.
     * Generates: SELECT * FROM (VALUES (expr1), (expr2), ...) with optional table alias.
     */
    private LogicalPlan buildInlineTable(SqlBaseParser.InlineTableContext ctx) {
        // Build VALUES clause from expressions
        StringBuilder valuesSql = new StringBuilder("VALUES ");
        List<SqlBaseParser.ExpressionContext> exprs = ctx.expression();
        for (int i = 0; i < exprs.size(); i++) {
            if (i > 0) valuesSql.append(", ");
            valuesSql.append(getOriginalText(exprs.get(i)));
        }

        LogicalPlan plan = new SQLRelation("SELECT * FROM (" + valuesSql + ")");

        // Apply table alias if present
        plan = applyTableAlias(plan, ctx.tableAlias());

        return plan;
    }

    // ==================== SELECT Statement ====================

    @Override
    public LogicalPlan visitRegularQuerySpecification(
            SqlBaseParser.RegularQuerySpecificationContext ctx) {

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

        // 3. GROUP BY: decompose into proper Aggregate node
        if (ctx.aggregationClause() != null) {
            return buildAggregateQuery(source, ctx.selectClause(),
                ctx.aggregationClause(), ctx.havingClause());
        }

        // 4. Build SELECT projections (non-aggregate path)
        source = buildProjection(source, ctx.selectClause());

        // 5. Apply HAVING (only valid with GROUP BY, but handle gracefully)
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
            plan = new Join(plan, right, Join.JoinType.CROSS, null);
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

        // Resolve table schema when connection is available
        StructType tableSchema = null;
        if (connection != null) {
            tableSchema = resolveTableSchema(tableName);
        }

        LogicalPlan plan;
        if (tableSchema != null && tableSchema.size() > 0) {
            plan = new SQLRelation("SELECT * FROM " + quotedName, tableSchema);
        } else {
            plan = new SQLRelation("SELECT * FROM " + quotedName);
        }

        // Apply table alias (explicit alias from SQL)
        plan = applyTableAlias(plan, ctx.tableAlias());

        // If no explicit alias was applied, use the table name as an implicit alias.
        // This preserves table name visibility in cross-join WHERE clauses, e.g.:
        //   FROM web_sales, time_dim WHERE ws_sold_time_sk = time_dim.t_time_sk
        // Without this, the SQLGenerator would assign "subquery_N" aliases and
        // "time_dim.t_time_sk" would fail to resolve.
        if (!(plan instanceof AliasedRelation)) {
            plan = new AliasedRelation(plan, tableName, java.util.Collections.emptyList());
        }

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
            condition = null;
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

    /**
     * Builds an Aggregate logical plan node from parsed SQL components.
     *
     * <p>Decomposes GROUP BY queries into proper AST:
     * <ol>
     *   <li>Parse GROUP BY expressions</li>
     *   <li>Detect ROLLUP/CUBE/GROUPING SETS</li>
     *   <li>Parse SELECT items and classify as grouping vs. aggregate</li>
     *   <li>Parse HAVING condition</li>
     *   <li>Build Aggregate node</li>
     * </ol>
     */
    private LogicalPlan buildAggregateQuery(LogicalPlan source,
                                             SqlBaseParser.SelectClauseContext selectCtx,
                                             SqlBaseParser.AggregationClauseContext aggCtx,
                                             SqlBaseParser.HavingClauseContext havingCtx) {

        // 1. Parse GROUP BY expressions and detect grouping sets
        List<Expression> groupByExprs = new ArrayList<>();
        GroupingSets groupingSets = parseAggregationClause(aggCtx, groupByExprs);

        // 2. Parse SELECT items
        boolean isDistinct = selectCtx.setQuantifier() != null &&
            selectCtx.setQuantifier().DISTINCT() != null;

        List<Expression> selectExprs = new ArrayList<>();
        List<String> selectAliases = new ArrayList<>();
        for (SqlBaseParser.NamedExpressionContext named :
                selectCtx.namedExpressionSeq().namedExpression()) {
            Expression expr = visitExpr(named.expression());

            String alias = null;
            if (named.name != null) {
                alias = getErrorCapturingIdentifierText(named.name);
            } else if (named.identifierList() != null) {
                alias = getErrorCapturingIdentifierText(
                    named.identifierList().identifierSeq().ident.get(0));
            }

            if (alias != null) {
                expr = new AliasExpression(expr, alias);
            }
            selectExprs.add(expr);
            selectAliases.add(alias);
        }

        // 3. Classify SELECT items into grouping expressions and aggregate expressions
        //    Track original ordering so interleaved grouping/aggregate columns are preserved.
        List<Expression> groupingExprs = new ArrayList<>();
        List<Aggregate.AggregateExpression> aggregateExprs = new ArrayList<>();
        List<Aggregate.SelectEntry> selectOrder = new ArrayList<>();

        for (Expression expr : selectExprs) {
            Expression inner = (expr instanceof AliasExpression ae) ? ae.expression() : expr;
            String alias = (expr instanceof AliasExpression ae) ? ae.alias() : null;

            if (inner instanceof StarExpression) {
                // SELECT * with GROUP BY — pass through as grouping
                selectOrder.add(new Aggregate.SelectEntry(false, groupingExprs.size()));
                groupingExprs.add(expr);
            } else if (com.thunderduck.expression.ExpressionUtils.containsAggregateFunction(inner)) {
                // Contains aggregate function → AggregateExpression
                selectOrder.add(new Aggregate.SelectEntry(true, aggregateExprs.size()));
                aggregateExprs.add(buildAggregateExpression(inner, alias));
            } else {
                // No aggregate function → grouping expression
                selectOrder.add(new Aggregate.SelectEntry(false, groupingExprs.size()));
                groupingExprs.add(expr);
            }
        }

        // Ensure all actual GROUP BY expressions are included in groupingExprs.
        // SELECT-derived groupingExprs may miss GROUP BY columns not in SELECT
        // (e.g., SELECT i_item_id, SUM(x) FROM t GROUP BY i_item_id, d_qoy).
        // Extra entries here only affect the GROUP BY clause, not SELECT
        // (which is controlled by selectOrder).
        for (Expression gbExpr : groupByExprs) {
            String gbSql = gbExpr.toSQL();
            boolean found = false;
            for (Expression ge : groupingExprs) {
                Expression geInner = (ge instanceof AliasExpression ae) ? ae.expression() : ge;
                if (geInner.toSQL().equals(gbSql)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                groupingExprs.add(gbExpr);
            }
        }

        // If no aggregate functions found (e.g., SELECT a, b FROM t GROUP BY a, b),
        // we still need at least one aggregate expression for the Aggregate node.
        // This can happen with GROUP BY without aggregates (deduplication pattern).
        // In this case, use a special marker — emit as a Project over Aggregate with
        // a dummy count to preserve GROUP BY semantics.
        if (aggregateExprs.isEmpty()) {
            // All items are grouping; add a hidden COUNT(*) that won't be in output
            // Actually, Aggregate node requires aggregate expressions. But this pattern
            // is valid SQL. Use SQLRelation fallback for this edge case.
            return buildAggregationFallback(source, selectCtx, aggCtx, havingCtx);
        }

        // 4. Parse HAVING
        Expression havingCondition = null;
        if (havingCtx != null) {
            havingCondition = visitBooleanExpr(havingCtx.booleanExpression());
        }

        // 5. Build Aggregate node with original select ordering
        Aggregate aggregate = new Aggregate(source, groupingExprs,
            aggregateExprs, havingCondition, groupingSets, selectOrder);

        // 6. Apply DISTINCT if specified
        if (isDistinct) {
            return new Distinct(aggregate);
        }

        return aggregate;
    }

    /**
     * Fallback for edge cases that resist proper decomposition (e.g., GROUP BY without aggregates).
     */
    private LogicalPlan buildAggregationFallback(LogicalPlan source,
                                                   SqlBaseParser.SelectClauseContext selectCtx,
                                                   SqlBaseParser.AggregationClauseContext aggCtx,
                                                   SqlBaseParser.HavingClauseContext havingCtx) {
        StringBuilder sql = new StringBuilder();
        sql.append(getOriginalText(selectCtx));
        String sourceSql = generateNodeSql(source);
        sql.append(" FROM (").append(sourceSql).append(") AS _agg_source");
        sql.append(" ").append(getOriginalText(aggCtx));
        if (havingCtx != null) {
            sql.append(" ").append(getOriginalText(havingCtx));
        }
        return new SQLRelation(sql.toString());
    }

    /**
     * Parses the aggregation clause into GROUP BY expressions and optional GroupingSets.
     *
     * <p>ANTLR grammar has two alternatives:
     * <ul>
     *   <li>Alt 1: GROUP BY groupByClause, groupByClause (inline ROLLUP/CUBE/GROUPING SETS)</li>
     *   <li>Alt 2: GROUP BY namedExpression, ... [WITH ROLLUP | WITH CUBE | GROUPING SETS(...)]</li>
     * </ul>
     *
     * @param aggCtx the aggregation clause context
     * @param groupByExprs output list for GROUP BY expressions
     * @return GroupingSets if ROLLUP/CUBE/GROUPING SETS detected, null otherwise
     */
    private GroupingSets parseAggregationClause(SqlBaseParser.AggregationClauseContext aggCtx,
                                                 List<Expression> groupByExprs) {
        // Alternative 1: GROUP BY with groupByClause (may contain groupingAnalytics)
        if (!aggCtx.groupingExpressionsWithGroupingAnalytics.isEmpty()) {
            return parseGroupByClauses(aggCtx.groupingExpressionsWithGroupingAnalytics, groupByExprs);
        }

        // Alternative 2: GROUP BY namedExpression, ... [WITH ROLLUP | WITH CUBE | GROUPING SETS]
        for (SqlBaseParser.NamedExpressionContext named : aggCtx.groupingExpressions) {
            groupByExprs.add(visitExpr(named.expression()));
        }

        // Check for WITH ROLLUP / WITH CUBE / GROUPING SETS suffix
        if (aggCtx.kind != null) {
            int kindType = aggCtx.kind.getType();
            if (kindType == org.apache.spark.sql.catalyst.parser.SqlBaseLexer.ROLLUP) {
                return GroupingSets.rollup(new ArrayList<>(groupByExprs));
            } else if (kindType == org.apache.spark.sql.catalyst.parser.SqlBaseLexer.CUBE) {
                return GroupingSets.cube(new ArrayList<>(groupByExprs));
            } else if (kindType == org.apache.spark.sql.catalyst.parser.SqlBaseLexer.GROUPING) {
                // GROUPING SETS(...)
                List<List<Expression>> sets = new ArrayList<>();
                for (SqlBaseParser.GroupingSetContext gs : aggCtx.groupingSet()) {
                    sets.add(parseGroupingSet(gs));
                }
                return GroupingSets.groupingSets(sets);
            }
        }

        return null;
    }

    /**
     * Parses Alt 1 groupByClause list, which may include inline ROLLUP/CUBE/GROUPING SETS.
     */
    private GroupingSets parseGroupByClauses(List<SqlBaseParser.GroupByClauseContext> clauses,
                                              List<Expression> groupByExprs) {
        GroupingSets result = null;

        for (SqlBaseParser.GroupByClauseContext clause : clauses) {
            if (clause.groupingAnalytics() != null) {
                // This clause is ROLLUP(...), CUBE(...), or GROUPING SETS(...)
                result = parseGroupingAnalytics(clause.groupingAnalytics(), groupByExprs);
            } else {
                // Plain expression
                groupByExprs.add(visitExpr(clause.expression()));
            }
        }

        return result;
    }

    /**
     * Parses a groupingAnalytics node: ROLLUP(...) | CUBE(...) | GROUPING SETS(...)
     */
    private GroupingSets parseGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx,
                                                 List<Expression> groupByExprs) {
        if (ctx.ROLLUP() != null) {
            List<Expression> columns = new ArrayList<>();
            for (SqlBaseParser.GroupingSetContext gs : ctx.groupingSet()) {
                columns.addAll(parseGroupingSet(gs));
            }
            groupByExprs.addAll(columns);
            return GroupingSets.rollup(columns);
        }

        if (ctx.CUBE() != null) {
            List<Expression> columns = new ArrayList<>();
            for (SqlBaseParser.GroupingSetContext gs : ctx.groupingSet()) {
                columns.addAll(parseGroupingSet(gs));
            }
            groupByExprs.addAll(columns);
            return GroupingSets.cube(columns);
        }

        // GROUPING SETS
        List<List<Expression>> sets = new ArrayList<>();
        for (SqlBaseParser.GroupingElementContext elem : ctx.groupingElement()) {
            if (elem.groupingAnalytics() != null) {
                // Nested groupingAnalytics — expand
                List<Expression> nested = new ArrayList<>();
                parseGroupingAnalytics(elem.groupingAnalytics(), nested);
                sets.add(nested);
            } else if (elem.groupingSet() != null) {
                sets.add(parseGroupingSet(elem.groupingSet()));
            }
        }
        // Add all columns from all sets to groupByExprs
        for (List<Expression> set : sets) {
            for (Expression e : set) {
                if (!groupByExprs.contains(e)) {
                    groupByExprs.add(e);
                }
            }
        }
        return GroupingSets.groupingSets(sets);
    }

    /**
     * Parses a single grouping set: either a parenthesized list of expressions or a single expression.
     */
    private List<Expression> parseGroupingSet(SqlBaseParser.GroupingSetContext ctx) {
        List<Expression> exprs = new ArrayList<>();
        for (SqlBaseParser.ExpressionContext exprCtx : ctx.expression()) {
            exprs.add(visitExpr(exprCtx));
        }
        return exprs;
    }

    /**
     * Builds an AggregateExpression from a parsed expression.
     * Handles simple function calls (SUM, COUNT, etc.) and composite expressions
     * (SUM(a) / SUM(b), CASE WHEN ... THEN SUM(...), etc.)
     */
    private Aggregate.AggregateExpression buildAggregateExpression(Expression expr, String alias) {
        // Simple case: direct aggregate function call
        if (expr instanceof FunctionCall func) {
            String funcName = func.functionName();
            List<Expression> args = func.arguments();

            // Handle DISTINCT
            boolean isDistinct = func.distinct();

            // Extract single argument (or null for COUNT(*))
            Expression argument = null;
            if (!args.isEmpty()) {
                if (args.size() == 1) {
                    argument = args.get(0);
                } else {
                    // Multi-arg aggregate (e.g., covar_samp(x, y)) — use composite
                    return new Aggregate.AggregateExpression(expr, alias);
                }
            }

            return new Aggregate.AggregateExpression(funcName, argument, alias, isDistinct);
        }

        // Composite expression containing aggregate functions
        // (e.g., SUM(a) / SUM(b), CAST(COUNT(*) AS DOUBLE), etc.)
        return new Aggregate.AggregateExpression(expr, alias);
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
            Expression lower = visitValueExpr(pred.lower);
            Expression upper = visitValueExpr(pred.upper);
            return new BetweenExpression(value, lower, upper, negated);
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
            boolean ilike = pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.ILIKE;
            Expression pattern = visitValueExpr(pred.pattern);
            return new LikeExpression(value, pattern, negated, ilike);
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
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.TRUE) {
            return negated ? UnaryExpression.isNotTrue(value) : UnaryExpression.isTrue(value);
        }
        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.FALSE) {
            return negated ? UnaryExpression.isNotFalse(value) : UnaryExpression.isFalse(value);
        }
        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.UNKNOWN) {
            return negated ? UnaryExpression.isNotUnknown(value) : UnaryExpression.isUnknown(value);
        }

        if (pred.kind.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DISTINCT) {
            // IS [NOT] DISTINCT FROM — null-safe equality
            Expression right = visitValueExpr(pred.right);
            return new IsDistinctFromExpression(value, right, negated);
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
            Expression result = new BinaryExpression(left, op, right);
            // In Spark, DATE + INTERVAL preserves DATE type.
            // In DuckDB, DATE + INTERVAL promotes to TIMESTAMP.
            // Wrap with CAST(... AS DATE) when a DATE operand is combined with an interval.
            if ((op == BinaryExpression.Operator.ADD || op == BinaryExpression.Operator.SUBTRACT)
                    && isDateType(left) && isIntervalExpression(right)) {
                result = new CastExpression(result, DateType.get());
            }
            return result;
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

    /**
     * Checks if an expression has DateType (e.g., DATE '2025-01-15' literal).
     */
    private boolean isDateType(Expression expr) {
        return expr.dataType() instanceof DateType;
    }

    /**
     * Checks if an expression represents an interval value.
     * Handles both the parsed RawSQLExpression path (INTERVAL '2' YEAR)
     * and the typed IntervalExpression path.
     */
    private boolean isIntervalExpression(Expression expr) {
        if (expr instanceof IntervalExpression) {
            return true;
        }
        if (expr instanceof RawSQLExpression raw) {
            return raw.toSQL().trim().toUpperCase().startsWith("INTERVAL");
        }
        return false;
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
            // Complex dereference — proper AST node
            return new FieldAccessExpression(base, field);
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
            List<Expression> elements = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext named : row.namedExpression()) {
                elements.add(visitExpr(named.expression()));
            }
            return new RowConstructorExpression(elements);
        }

        // EXTRACT(field FROM source)
        // Map to Spark function equivalents (year, month, etc.) which go through
        // FunctionRegistry CUSTOM_TRANSLATORS for proper CAST(... AS INTEGER) wrapping.
        // Unmapped fields fall back to raw EXTRACT(...) syntax.
        if (ctx instanceof SqlBaseParser.ExtractContext extract) {
            String field = getIdentifierText(extract.field);
            Expression source = visitValueExpr(extract.source);
            String sparkFunc = switch (field.toUpperCase()) {
                case "YEAR" -> "year";
                case "MONTH" -> "month";
                case "DAY" -> "day";
                case "HOUR" -> "hour";
                case "MINUTE" -> "minute";
                case "SECOND" -> "second";
                case "QUARTER" -> "quarter";
                case "DOY" -> "dayofyear";
                case "DOW" -> "dayofweek";
                case "WEEK" -> "weekofyear";
                default -> null;
            };
            if (sparkFunc != null) {
                return new FunctionCall(sparkFunc, List.of(source), IntegerType.get());
            }
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
            List<String> fieldNames = new ArrayList<>();
            List<Expression> fieldValues = new ArrayList<>();
            boolean allNamed = true;
            for (SqlBaseParser.NamedExpressionContext named : structCtx.argument) {
                Expression e = visitExpr(named.expression());
                fieldValues.add(e);
                if (named.name != null) {
                    fieldNames.add(getErrorCapturingIdentifierText(named.name));
                } else {
                    allNamed = false;
                }
            }
            if (allNamed && !fieldNames.isEmpty()) {
                return new StructLiteralExpression(fieldNames, fieldValues);
            }
            // Positional fields — use struct_pack function call
            return new FunctionCall("struct_pack", fieldValues,
                UnresolvedType.expressionString(), false);
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
            // count(*) — use StarExpression for proper AST representation
            args = List.of(new StarExpression());
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

        // Window-only case (no FILTER/WITHIN GROUP): construct WindowFunction AST
        if (hasWindow && !hasFilter && !hasWithinGroup) {
            return buildWindowFunctionAST(funcName, args, isDistinct, hasNullsOption,
                hasNullsOption ? ctx.nullsOption : null, ctx.windowSpec());
        }

        // Complex case: function has FILTER, WITHIN GROUP, or mixed modifiers.
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

        // Handle OVER (window function) — only reached when FILTER/WITHIN GROUP also present
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

    /**
     * Builds a WindowFunction AST node from parsed components.
     * Handles IGNORE/RESPECT NULLS by encoding them in the argument list
     * for first/last/nth_value, or falling back to RawSQLExpression for others.
     */
    private Expression buildWindowFunctionAST(String funcName, List<Expression> args,
            boolean isDistinct, boolean hasNullsOption, Token nullsOption,
            SqlBaseParser.WindowSpecContext windowSpec) {

        // Handle DISTINCT: WindowFunction doesn't support DISTINCT directly.
        // Fall back to RawSQLExpression for DISTINCT window functions.
        if (isDistinct) {
            FunctionCall baseCall = new FunctionCall(funcName, args,
                UnresolvedType.expressionString(), true, true);
            StringBuilder sql = new StringBuilder(baseCall.toSQL());
            sql.append(" OVER ").append(buildWindowSpec(windowSpec));
            if (hasNullsOption) {
                sql.append(" ").append(nullsOption.getText().toUpperCase()).append(" NULLS");
            }
            return new RawSQLExpression(sql.toString());
        }

        // Handle nullsOption for functions that WindowFunction doesn't manage internally
        String funcUpper = funcName.toUpperCase();
        boolean isFirstLast = funcUpper.equals("FIRST") || funcUpper.equals("FIRST_VALUE") ||
                              funcUpper.equals("LAST") || funcUpper.equals("LAST_VALUE");
        boolean isNthValue = funcUpper.equals("NTH_VALUE");

        if (hasNullsOption && !isFirstLast && !isNthValue) {
            // WindowFunction only handles IGNORE NULLS for first/last/nth_value.
            // For other functions with NULLS option, fall back to RawSQLExpression.
            FunctionCall baseCall = new FunctionCall(funcName, args,
                UnresolvedType.expressionString(), true, false);
            StringBuilder sql = new StringBuilder(baseCall.toSQL());
            sql.append(" OVER ").append(buildWindowSpec(windowSpec));
            sql.append(" ").append(nullsOption.getText().toUpperCase()).append(" NULLS");
            return new RawSQLExpression(sql.toString());
        }

        // Parse window specification into AST components
        if (windowSpec instanceof SqlBaseParser.WindowRefContext ref) {
            String windowName = getErrorCapturingIdentifierText(ref.name);
            return new WindowFunction(funcName, args, windowName);
        }

        if (windowSpec instanceof SqlBaseParser.WindowDefContext def) {
            List<Expression> partitionBy = new ArrayList<>();
            for (SqlBaseParser.ExpressionContext partExpr : def.partition) {
                partitionBy.add(visitExpr(partExpr));
            }

            List<Sort.SortOrder> orderBy = new ArrayList<>();
            if (def.sortItem() != null) {
                for (SqlBaseParser.SortItemContext si : def.sortItem()) {
                    orderBy.add(parseSortItem(si));
                }
            }

            WindowFrame frame = null;
            if (def.windowFrame() != null) {
                frame = parseWindowFrame(def.windowFrame());
            }

            return new WindowFunction(funcName, args, partitionBy, orderBy, frame);
        }

        // Fallback: unrecognized window spec type
        FunctionCall baseCall = new FunctionCall(funcName, args,
            UnresolvedType.expressionString(), true, false);
        return new RawSQLExpression(baseCall.toSQL() + " OVER " + buildWindowSpec(windowSpec));
    }

    private Sort.SortOrder parseSortItem(SqlBaseParser.SortItemContext si) {
        Expression expr = visitExpr(si.expression());

        Sort.SortDirection direction = Sort.SortDirection.ASCENDING;
        if (si.ordering != null && si.ordering.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.DESC) {
            direction = Sort.SortDirection.DESCENDING;
        }

        Sort.NullOrdering nullOrdering;
        if (si.nullOrder != null) {
            nullOrdering = si.nullOrder.getType() ==
                org.apache.spark.sql.catalyst.parser.SqlBaseLexer.FIRST ?
                Sort.NullOrdering.NULLS_FIRST : Sort.NullOrdering.NULLS_LAST;
        } else {
            // Default: ASC → NULLS FIRST, DESC → NULLS LAST (matches Spark convention)
            nullOrdering = direction == Sort.SortDirection.ASCENDING ?
                Sort.NullOrdering.NULLS_FIRST : Sort.NullOrdering.NULLS_LAST;
        }

        return new Sort.SortOrder(expr, direction, nullOrdering);
    }

    private WindowFrame parseWindowFrame(SqlBaseParser.WindowFrameContext ctx) {
        String frameTypeStr = ctx.frameType.getText().toUpperCase();
        WindowFrame.FrameType frameType = switch (frameTypeStr) {
            case "ROWS" -> WindowFrame.FrameType.ROWS;
            case "RANGE" -> WindowFrame.FrameType.RANGE;
            case "GROUPS" -> WindowFrame.FrameType.GROUPS;
            default -> throw new UnsupportedOperationException(
                "Unsupported frame type: " + frameTypeStr);
        };

        FrameBoundary start = parseFrameBound(ctx.start);
        FrameBoundary end;
        if (ctx.BETWEEN() != null) {
            end = parseFrameBound(ctx.end);
        } else {
            // Without BETWEEN, the frame is from `start` to CURRENT ROW
            end = FrameBoundary.CurrentRow.getInstance();
        }

        return new WindowFrame(frameType, start, end);
    }

    private FrameBoundary parseFrameBound(SqlBaseParser.FrameBoundContext ctx) {
        if (ctx.UNBOUNDED() != null) {
            String boundType = ctx.boundType.getText().toUpperCase();
            return boundType.equals("PRECEDING") ?
                FrameBoundary.UnboundedPreceding.getInstance() :
                FrameBoundary.UnboundedFollowing.getInstance();
        }
        if (ctx.CURRENT() != null) {
            return FrameBoundary.CurrentRow.getInstance();
        }
        // Expression-based bound (e.g., "3 PRECEDING" or "5 FOLLOWING")
        Expression expr = visitExpr(ctx.expression());
        String boundType = ctx.boundType.getText().toUpperCase();
        // The expression should be a numeric literal
        long offset = 1;
        if (expr instanceof Literal) {
            Object val = ((Literal) expr).value();
            if (val instanceof Number) {
                offset = ((Number) val).longValue();
            }
        }
        return boundType.equals("PRECEDING") ?
            new FrameBoundary.Preceding(offset) :
            new FrameBoundary.Following(offset);
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
        if (ctx instanceof SqlBaseParser.ComplexDataTypeContext complex) {
            String typeName = complex.complex.getText().toUpperCase();
            List<SqlBaseParser.DataTypeContext> typeArgs = complex.dataType();
            switch (typeName) {
                case "ARRAY":
                    if (!typeArgs.isEmpty()) {
                        DataType elementType = resolveDataType(typeArgs.get(0));
                        return new ArrayType(elementType, true);
                    }
                    return new ArrayType(StringType.get(), true);
                case "MAP":
                    if (typeArgs.size() >= 2) {
                        DataType keyType = resolveDataType(typeArgs.get(0));
                        DataType valueType = resolveDataType(typeArgs.get(1));
                        return new MapType(keyType, valueType, true);
                    }
                    return new MapType(StringType.get(), StringType.get(), true);
                default:
                    // STRUCT and others -- fall through to unresolved
                    break;
            }
        }

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

    // ==================== Table Schema Resolution ====================

    /**
     * Resolves the schema of a table by querying DuckDB.
     *
     * <p>Uses {@code PRAGMA table_info('tableName')} to retrieve column names and types,
     * then converts them to a Thunderduck StructType. If the query fails (e.g., table
     * doesn't exist, view, or CTE reference), returns null to fall back to empty schema.
     *
     * @param tableName the table name to resolve
     * @return the resolved schema, or null if resolution fails
     */
    private StructType resolveTableSchema(String tableName) {
        try {
            // Use SELECT * FROM table LIMIT 0 to get schema for both tables and views
            // This is more robust than PRAGMA table_info which only works for base tables
            String schemaQuery = "SELECT * FROM " + quoteIdentifierIfNeeded(tableName) + " LIMIT 0";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(schemaQuery)) {

                java.sql.ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                if (columnCount == 0) return null;

                List<StructField> fields = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    String colName = meta.getColumnName(i);
                    String typeName = meta.getColumnTypeName(i).toUpperCase();
                    int nullable = meta.isNullable(i);

                    DataType dataType = mapDuckDBTypeToThunderduck(typeName, meta, i);
                    boolean isNullable = (nullable != java.sql.ResultSetMetaData.columnNoNulls);

                    fields.add(new StructField(colName, dataType, isNullable));
                }

                logger.debug("Resolved schema for table '{}': {} columns", tableName, fields.size());
                return new StructType(fields);
            }
        } catch (Exception e) {
            // Table doesn't exist, is a CTE reference, or other error — fall back gracefully
            logger.debug("Could not resolve schema for table '{}': {}", tableName, e.getMessage());
            return null;
        }
    }

    /**
     * Maps a DuckDB SQL type name to a Thunderduck DataType.
     *
     * @param typeName the DuckDB type name (e.g., "INTEGER", "VARCHAR", "DECIMAL(18,2)")
     * @param meta the result set metadata for precision/scale info
     * @param columnIndex 1-based column index
     * @return the corresponding Thunderduck DataType
     */
    private DataType mapDuckDBTypeToThunderduck(String typeName, java.sql.ResultSetMetaData meta, int columnIndex) {
        try {
            return switch (typeName) {
                case "BOOLEAN" -> BooleanType.get();
                case "TINYINT" -> ByteType.get();
                case "SMALLINT" -> ShortType.get();
                case "INTEGER", "INT" -> IntegerType.get();
                case "BIGINT", "LONG", "INT8", "HUGEINT" -> LongType.get();
                case "FLOAT", "REAL", "FLOAT4" -> FloatType.get();
                case "DOUBLE", "FLOAT8" -> DoubleType.get();
                case "VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR" -> StringType.get();
                case "DATE" -> DateType.get();
                case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
                     "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S" -> TimestampType.get();
                case "BLOB", "BYTEA", "BINARY", "VARBINARY" -> BinaryType.get();
                case "DECIMAL", "NUMERIC" -> {
                    int precision = meta.getPrecision(columnIndex);
                    int scale = meta.getScale(columnIndex);
                    if (precision <= 0) precision = 10;
                    yield new DecimalType(precision, scale);
                }
                default -> {
                    // Handle parameterized types like DECIMAL(18,2)
                    if (typeName.startsWith("DECIMAL") || typeName.startsWith("NUMERIC")) {
                        int precision = meta.getPrecision(columnIndex);
                        int scale = meta.getScale(columnIndex);
                        if (precision <= 0) precision = 10;
                        yield new DecimalType(precision, scale);
                    }
                    logger.debug("Unknown DuckDB type '{}', mapping to StringType", typeName);
                    yield StringType.get();
                }
            };
        } catch (Exception e) {
            logger.debug("Error mapping DuckDB type '{}': {}", typeName, e.getMessage());
            return StringType.get();
        }
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
