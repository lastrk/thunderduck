package com.thunderduck.parser;

import com.thunderduck.logical.LogicalPlan;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Entry point for parsing SparkSQL into Thunderduck LogicalPlan AST.
 *
 * <p>Wraps the ANTLR4-generated parser with:
 * <ul>
 *   <li>SLL-first, LL-fallback two-phase parsing for performance</li>
 *   <li>User-friendly error messages via {@link SparkSQLErrorListener}</li>
 *   <li>Thread-safe via ThreadLocal parser pool</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   SparkSQLParser parser = SparkSQLParser.getInstance();
 *   LogicalPlan plan = parser.parse("SELECT * FROM t WHERE id > 5");
 *   String duckdbSQL = sqlGenerator.generate(plan);
 * </pre>
 */
public class SparkSQLParser {

    private static final Logger logger = LoggerFactory.getLogger(SparkSQLParser.class);

    /**
     * ThreadLocal parser pool: each thread gets its own parser instance.
     * This avoids lock contention while keeping DFA caches per-thread.
     */
    private static final ThreadLocal<SparkSQLParser> PARSER_POOL =
        ThreadLocal.withInitial(SparkSQLParser::new);

    private final SqlBaseLexer lexer;
    private final CommonTokenStream tokens;
    private final SqlBaseParser parser;

    /**
     * Creates a new parser instance. Typically accessed via {@link #getInstance()}.
     */
    public SparkSQLParser() {
        this.lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString("")));
        this.tokens = new CommonTokenStream(lexer);
        this.parser = new SqlBaseParser(tokens);

        // Configure parser
        parser.removeErrorListeners();
        parser.addErrorListener(new SparkSQLErrorListener());
    }

    /**
     * Returns the thread-local parser instance.
     *
     * @return parser for the current thread
     */
    public static SparkSQLParser getInstance() {
        return PARSER_POOL.get();
    }

    /**
     * Parses a SparkSQL query string into a Thunderduck LogicalPlan.
     *
     * <p>Uses SLL prediction mode first (fast path for 95%+ of queries),
     * then falls back to LL mode on SLL failure (handles ambiguous grammars).
     *
     * @param sql the SparkSQL query string
     * @return the logical plan AST
     * @throws SparkSQLParseException if the SQL is syntactically invalid
     * @throws UnsupportedOperationException if the SQL uses unsupported features
     */
    public LogicalPlan parse(String sql) {
        return parse(sql, null);
    }

    /**
     * Parses a SparkSQL query string into a Thunderduck LogicalPlan with schema resolution.
     *
     * <p>When a DuckDB connection is provided, table references are resolved to their
     * actual schemas via {@code DESCRIBE tableName}. This enables {@code inferSchema()}
     * to succeed for all plan shapes, eliminating the need for regex-based schema fixups.
     *
     * <p>When connection is null, behaves identically to {@link #parse(String)}.
     *
     * @param sql the SparkSQL query string
     * @param connection optional DuckDB connection for table schema resolution (can be null)
     * @return the logical plan AST
     * @throws SparkSQLParseException if the SQL is syntactically invalid
     * @throws UnsupportedOperationException if the SQL uses unsupported features
     */
    public LogicalPlan parse(String sql, Connection connection) {
        if (sql == null || sql.isBlank()) {
            throw new SparkSQLParseException(0, 0, "", "SQL query must not be null or empty");
        }

        logger.debug("Parsing SparkSQL: {}", sql);

        // Reset lexer and parser state
        lexer.setInputStream(new UpperCaseCharStream(CharStreams.fromString(sql)));
        tokens.setTokenSource(lexer);
        parser.setTokenStream(tokens);

        // Phase 1: Try SLL mode (fast)
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        parser.removeErrorListeners();
        parser.setErrorHandler(new BailErrorStrategy());

        SqlBaseParser.SingleStatementContext tree;
        try {
            tree = parser.singleStatement();
        } catch (Exception e) {
            // Phase 2: Fall back to LL mode (correct for all grammars)
            logger.debug("SLL parse failed, falling back to LL mode: {}", e.getMessage());
            tokens.seek(0);
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            parser.removeErrorListeners();
            parser.addErrorListener(new SparkSQLErrorListener());
            parser.setErrorHandler(new DefaultErrorStrategy());

            tree = parser.singleStatement();
        }

        // Build AST from parse tree
        SparkSQLAstBuilder astBuilder = new SparkSQLAstBuilder(connection);
        LogicalPlan plan = (LogicalPlan) astBuilder.visit(tree);

        logger.debug("Successfully parsed SparkSQL into LogicalPlan: {}", plan);
        return plan;
    }

    /**
     * Tests whether the given SQL can be parsed without errors.
     *
     * @param sql the SQL to test
     * @return true if the SQL parses successfully, false otherwise
     */
    public boolean canParse(String sql) {
        try {
            parse(sql);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
