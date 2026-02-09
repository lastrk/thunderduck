package com.thunderduck.connect.service;

import com.thunderduck.connect.converter.PlanConverter;
import com.thunderduck.connect.converter.ExpressionConverter;
import com.thunderduck.connect.session.Session;
import com.thunderduck.connect.session.SessionManager;
import com.thunderduck.connect.sql.SQLParameterSubstitution;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.runtime.ArrowBatchIterator;
import com.thunderduck.runtime.ArrowStreamingExecutor;
import com.thunderduck.runtime.QueryExecutor;
import com.thunderduck.runtime.TailBatchIterator;
import com.thunderduck.runtime.SchemaCorrectedBatchIterator;
import com.thunderduck.runtime.SparkCompatMode;
import com.thunderduck.logical.Tail;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.schema.SchemaInferrer;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.spark.connect.proto.*;
import org.apache.spark.connect.proto.Relation;
import org.apache.spark.connect.proto.SQL;
import org.apache.spark.connect.proto.SqlCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.thunderduck.generator.SQLQuoting.quoteIdentifier;
import static com.thunderduck.generator.SQLQuoting.quoteFilePath;

/**
 * Implementation of Spark Connect gRPC service.
 *
 * This service bridges Spark Connect protocol to DuckDB via thunderduck.
 * Key features:
 * - Session management with per-session DuckDB runtime
 * - Zero-copy Arrow streaming via DuckDB's arrowExportStream
 * - SQL query execution and DataFrame plan deserialization
 */
public class SparkConnectServiceImpl extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SparkConnectServiceImpl.class);

    private final SessionManager sessionManager;
    private final SQLGenerator sqlGenerator;
    private final CatalogOperationHandler catalogHandler;

    /**
     * Create Spark Connect service with session manager.
     *
     * <p>Each session maintains its own DuckDBRuntime for query execution.
     *
     * @param sessionManager Manager for session lifecycle
     */
    public SparkConnectServiceImpl(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        this.sqlGenerator = new SQLGenerator();
        this.catalogHandler = new CatalogOperationHandler();

        logger.info("SparkConnectServiceImpl initialized with session-scoped DuckDB runtimes");
    }

    /**
     * Create a StatisticsOperationHandler with schema inference for the session.
     *
     * @param session the session providing the DuckDB connection
     * @return a StatisticsOperationHandler instance
     */
    private StatisticsOperationHandler createStatisticsHandler(Session session) {
        SchemaInferrer schemaInferrer = new SchemaInferrer(session.getRuntime().getConnection());
        return new StatisticsOperationHandler(schemaInferrer);
    }

    /**
     * Creates a PlanConverter without schema inference.
     * Use createPlanConverter(Session) when schema inference is needed (e.g., NA functions).
     *
     * @return a PlanConverter instance without schema inference capability
     */
    private PlanConverter createPlanConverter() {
        return new PlanConverter();
    }

    /**
     * Creates a PlanConverter with schema inference capability using the session's DuckDB connection.
     * This enables NA functions (dropna, fillna, replace) that need to infer column types.
     *
     * @param session the session providing the DuckDB connection
     * @return a PlanConverter instance with schema inference capability
     */
    private PlanConverter createPlanConverter(Session session) {
        return new PlanConverter(session.getRuntime().getConnection());
    }

    /**
     * Execute a Spark plan and stream results back to client.
     *
     * This is the main RPC for query execution:
     * 1. Acquire execution slot (may wait in queue if busy)
     * 2. Extract session ID from request
     * 3. Deserialize plan to SQL
     * 4. Execute via QueryExecutor
     * 5. Stream results as Arrow batches
     * 6. Release execution slot in finally block
     *
     * @param request ExecutePlanRequest containing the plan
     * @param responseObserver Stream observer for responses
     */
    @Override
    public void executePlan(ExecutePlanRequest request,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("executePlan called for session: {}", sessionId);

        Session session = null;

        try {
            // Acquire execution slot (may wait in queue if busy)
            Context grpcContext = Context.current();
            session = sessionManager.startExecution(sessionId, grpcContext);

            // Extract the plan
            if (!request.hasPlan()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must contain a plan")
                    .asRuntimeException());
                return;
            }

            Plan plan = request.getPlan();
            logger.debug("Plan type: {}", plan.getOpTypeCase());

            // Handle SQL queries (both direct and command-wrapped)
            String sql = null;
            boolean isShowString = false;
            int showStringNumRows = 20;  // default
            int showStringTruncate = 20; // default
            boolean showStringVertical = false;

            if (plan.hasRoot()) {
                Relation root = plan.getRoot();
                logger.info("Root relation type: {} (hasCatalog={})", root.getRelTypeCase(), root.hasCatalog());

                // Check for ShowString wrapping any relation
                if (root.hasShowString() && root.getShowString().hasInput()) {
                    var showString = root.getShowString();
                    Relation input = showString.getInput();
                    isShowString = true;
                    showStringNumRows = showString.getNumRows();
                    showStringTruncate = showString.getTruncate();
                    showStringVertical = showString.getVertical();

                    if (input.hasSql()) {
                        // SparkSQL not yet supported - pending SQL parser integration
                        throw new UnsupportedOperationException(
                            "spark.sql() is not yet supported. Please use DataFrame API instead. " +
                            "SQL support will be added in a future release.");
                    } else {
                        // Deserialize non-SQL relation and generate SQL
                        try {
                            LogicalPlan innerPlan = createPlanConverter(session).convertRelation(input);
                            sql = sqlGenerator.generate(innerPlan);
                            logger.debug("Generated SQL from ShowString.input: {}", sql);
                        } catch (Exception e) {
                            logger.error("Failed to deserialize ShowString inner relation", e);
                            responseObserver.onError(Status.INTERNAL
                                .withDescription("ShowString deserialization failed: " + e.getMessage())
                                .asRuntimeException());
                            return;
                        }
                    }
                } else if (root.hasSql()) {
                    // Handle spark.sql() queries with parameter substitution
                    SQL sqlRelation = root.getSql();
                    String query = sqlRelation.getQuery();

                    logger.info("Received spark.sql() query: {}", query);

                    // Substitute parameters if present
                    if (sqlRelation.getNamedArgumentsCount() > 0 ||
                        sqlRelation.getPosArgumentsCount() > 0 ||
                        sqlRelation.getArgsCount() > 0 ||
                        sqlRelation.getPosArgsCount() > 0) {

                        ExpressionConverter expressionConverter = new ExpressionConverter();
                        SQLParameterSubstitution paramSubst = new SQLParameterSubstitution(expressionConverter);
                        query = paramSubst.substituteParameters(sqlRelation);
                        logger.info("After parameter substitution: {}", query);
                    }

                    // Pass through to DuckDB with SQL preprocessing
                    sql = query;
                } else if (root.hasCatalog()) {
                    // Handle catalog operations (dropTempView, etc.)
                    executeCatalogOperation(root.getCatalog(), session, responseObserver);
                    return;
                } else if (isStatisticsRelation(root)) {
                    // Handle statistics operations (cov, corr, describe, etc.)
                    executeStatisticsOperation(root, session, responseObserver);
                    return;
                }
            } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                // Handle SQL commands (alternative path for spark.sql())
                SqlCommand sqlCommand = plan.getCommand().getSqlCommand();
                String query = null;

                // SqlCommand.sql is deprecated. The SQL query comes in the input relation.
                if (sqlCommand.hasInput() && sqlCommand.getInput().hasSql()) {
                    SQL sqlRelation = sqlCommand.getInput().getSql();
                    query = sqlRelation.getQuery();

                    logger.info("Received spark.sql() via SqlCommand: {}", query);

                    // Handle parameter substitution
                    if (sqlRelation.getNamedArgumentsCount() > 0 ||
                        sqlRelation.getPosArgumentsCount() > 0 ||
                        sqlRelation.getArgsCount() > 0 ||
                        sqlRelation.getPosArgsCount() > 0) {

                        ExpressionConverter expressionConverter = new ExpressionConverter();
                        SQLParameterSubstitution paramSubst = new SQLParameterSubstitution(expressionConverter);
                        query = paramSubst.substituteParameters(sqlRelation);
                        logger.info("After parameter substitution: {}", query);
                    }
                } else if (!sqlCommand.getSql().isEmpty()) {
                    // Fallback to deprecated field
                    query = sqlCommand.getSql();
                    logger.info("Received SQL via deprecated SqlCommand.sql field: {}", query);
                }

                if (query != null && !query.isEmpty()) {
                    sql = query;
                } else {
                    logger.error("SqlCommand has no query");
                    throw new IllegalArgumentException("SqlCommand has no query");
                }
            } else if (plan.hasCommand()) {
                // Handle non-SQL commands (CreateTempView, DropTempView, etc.)
                Command command = plan.getCommand();
                logger.debug("Handling COMMAND: {}", command.getCommandTypeCase());

                executeCommand(command, session, responseObserver);
                return; // Command handling is complete
            }

            if (sql != null) {
                // Apply all SQL preprocessing for Spark compatibility
                String originalSql = sql;
                sql = preprocessSQL(sql);

                if (!originalSql.equals(sql)) {
                    logger.debug("Applied SQL preprocessing");
                }

                logger.info("Executing SQL: {}", sql);

                if (isShowString) {
                    // For ShowString, format results as text and return in 'show_string' column
                    executeShowString(sql, session, showStringNumRows, showStringTruncate,
                        showStringVertical, responseObserver);
                } else {
                    // Regular SQL execution
                    executeSQL(sql, session, responseObserver);
                }
            } else if (plan.hasRoot()) {
                // Non-SQL plan - use plan deserialization
                logger.info("Deserializing DataFrame plan: {}", plan.getRoot().getRelTypeCase());

                try {
                    // Start timing instrumentation
                    QueryTimingStats timing = new QueryTimingStats();
                    timing.startTotal();

                    // Convert Protobuf plan to LogicalPlan
                    timing.startPlanConvert();
                    LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);
                    timing.stopPlanConvert();

                    // Generate SQL from LogicalPlan
                    timing.startSqlGenerate();
                    String generatedSQL = sqlGenerator.generate(logicalPlan);

                    // Get logical schema for correct nullable flags
                    // DuckDB returns all columns as nullable, but Spark has specific rules
                    StructType logicalSchema = logicalPlan.schema();

                    // Apply SQL preprocessing for Spark compatibility (spark_sum, spark_avg, etc.)
                    generatedSQL = preprocessSQL(generatedSQL);
                    timing.stopSqlGenerate();
                    logger.info("Generated SQL from plan: {}", generatedSQL);

                    // In strict mode, fix decimal precision for complex aggregate expressions
                    // The logical schema from Java type inference may have DuckDB's intermediate
                    // precision (e.g., DECIMAL(28,4)) instead of Spark's expected precision (DECIMAL(38,4)).
                    // This correction ensures SchemaCorrectedBatchIterator promotes Arrow output correctly.
                    if (SparkCompatMode.isStrictMode() && logicalSchema != null) {
                        logicalSchema = fixCountNullable(generatedSQL, logicalSchema);
                        logicalSchema = fixDecimalPrecisionForComplexAggregates(generatedSQL, logicalSchema);

                        // Fix DECIMAL->DOUBLE mismatch at the SQL level:
                        // When PySpark uses F.lit(100.00) or / 7.0, the logical schema says DOUBLE,
                        // but DuckDB treats 100.0 as DECIMAL and returns DECIMAL.
                        // Instead of converting Arrow vectors row-by-row, we add CAST(... AS DOUBLE)
                        // in the SQL so DuckDB does the conversion natively.
                        generatedSQL = fixDecimalToDoubleCasts(generatedSQL, logicalSchema, session.getSessionId());
                    }

                    // Check if this is a Tail plan - use TailBatchIterator wrapper
                    if (logicalPlan instanceof Tail) {
                        Tail tailPlan = (Tail) logicalPlan;
                        executeSQLStreaming(generatedSQL, logicalSchema, session, responseObserver, (int) tailPlan.limit(), timing);
                    } else {
                        // Execute the generated SQL with schema correction
                        executeSQLStreaming(generatedSQL, logicalSchema, session, responseObserver, -1, timing);
                    }

                } catch (Exception e) {
                    logger.error("Plan deserialization failed", e);
                    responseObserver.onError(Status.INTERNAL
                        .withDescription("Plan deserialization failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
                }
            } else {
                // Unsupported plan type
                String details = String.format("Plan type: %s", plan.getOpTypeCase());
                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("Unsupported plan type. " + details)
                    .asRuntimeException());
            }

        } catch (StatusRuntimeException e) {
            // gRPC status errors (queue full, cancelled, etc.) - pass through
            logger.warn("Plan execution failed with gRPC error for session {}: {}", sessionId, e.getMessage());
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Error executing plan for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Execution failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        } finally {
            // Always release execution slot
            if (session != null) {
                sessionManager.completeExecution(sessionId);
            }
        }
    }

    /**
     * Execute a Command (non-query operation).
     *
     * Handles:
     * - CreateDataFrameViewCommand (createOrReplaceTempView)
     * - DropTempViewCommand (dropTempView)
     * - Other commands as needed
     *
     * @param command The command to execute
     * @param session Session object
     * @param responseObserver Stream observer for responses
     */
    private void executeCommand(Command command, Session session,
                               StreamObserver<ExecutePlanResponse> responseObserver) {

        try {
            if (command.hasCreateDataframeView()) {
                // Handle createOrReplaceTempView
                CreateDataFrameViewCommand viewCmd = command.getCreateDataframeView();
                String viewName = viewCmd.getName();
                Relation input = viewCmd.getInput();
                boolean replace = viewCmd.getReplace();

                logger.info("Creating temp view: '{}' (replace={})", viewName, replace);

                // Convert input relation to LogicalPlan
                LogicalPlan logicalPlan = createPlanConverter(session).convertRelation(input);

                // Check if view already exists and replace=false
                if (!replace && session.getTempView(viewName).isPresent()) {
                    responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("Temp view already exists: " + viewName)
                        .asRuntimeException());
                    return;
                }

                session.registerTempView(viewName, logicalPlan);

                // Generate SQL from the plan and create DuckDB view
                // NOTE: We use non-temp views because DuckDB temp views are connection-scoped,
                // and our connection pool means different requests may use different connections.
                // Non-temp views are visible across all connections to the same database instance.
                String viewSQL = sqlGenerator.generate(logicalPlan);
                String createViewSQL = String.format("CREATE OR REPLACE VIEW %s AS %s",
                    quoteIdentifier(viewName), viewSQL);

                logger.debug("Creating DuckDB view: {}", createViewSQL);

                // Execute the CREATE VIEW statement in DuckDB
                QueryExecutor executor = new QueryExecutor(session.getRuntime());
                try {
                    executor.execute(createViewSQL);
                } catch (Exception e) {
                    logger.error("Failed to create DuckDB view", e);
                    responseObserver.onError(Status.INTERNAL
                        .withDescription("Failed to create DuckDB view: " + e.getMessage())
                        .asRuntimeException());
                    return;
                }

                // Return success response (empty result with operation ID)
                String operationId = java.util.UUID.randomUUID().toString();
                ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                    .setSessionId(session.getSessionId())
                    .setOperationId(operationId)
                    .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                logger.info("✓ View created in DuckDB: '{}' (session: {})", viewName, session.getSessionId());

            } else if (command.hasWriteOperation()) {
                // Handle df.write.parquet(), df.write.csv(), etc.
                executeWriteOperation(command.getWriteOperation(), session, responseObserver);

            } else {
                // Unsupported command type
                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("Unsupported command type: " + command.getCommandTypeCase())
                    .asRuntimeException());
            }

        } catch (Exception e) {
            logger.error("Command execution failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Command execution failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Execute a Catalog operation (dropTempView, tableExists, listTables, etc.).
     *
     * Delegates to CatalogOperationHandler which handles:
     * - DropTempView (spark.catalog.dropTempView)
     * - TableExists (spark.catalog.tableExists)
     * - ListTables (spark.catalog.listTables)
     * - ListColumns (spark.catalog.listColumns)
     * - ListDatabases (spark.catalog.listDatabases)
     * - DatabaseExists (spark.catalog.databaseExists)
     * - CurrentDatabase (spark.catalog.currentDatabase)
     * - SetCurrentDatabase (spark.catalog.setCurrentDatabase)
     *
     * @param catalog The catalog operation to execute
     * @param session Session object
     * @param responseObserver Stream observer for responses
     */
    private void executeCatalogOperation(Catalog catalog, Session session,
                                        StreamObserver<ExecutePlanResponse> responseObserver) {
        catalogHandler.execute(catalog, session, responseObserver);
    }

    /**
     * Check if a relation is a statistics operation.
     *
     * @param relation the relation to check
     * @return true if it's a statistics operation
     */
    private boolean isStatisticsRelation(Relation relation) {
        switch (relation.getRelTypeCase()) {
            case COV:
            case CORR:
            case APPROX_QUANTILE:
            case DESCRIBE:
            case SUMMARY:
            case CROSSTAB:
            case FREQ_ITEMS:
            case SAMPLE_BY:
                return true;
            default:
                return false;
        }
    }

    /**
     * Execute a Statistics operation (cov, corr, describe, etc.).
     *
     * Delegates to StatisticsOperationHandler which handles:
     * - StatCov (df.stat.cov)
     * - StatCorr (df.stat.corr)
     * - StatApproxQuantile (df.stat.approxQuantile)
     * - StatDescribe (df.describe)
     * - StatSummary (df.summary)
     * - StatCrosstab (df.stat.crosstab)
     * - StatFreqItems (df.stat.freqItems)
     * - StatSampleBy (df.stat.sampleBy)
     *
     * @param relation The statistics relation to execute
     * @param session Session object
     * @param responseObserver Stream observer for responses
     */
    private void executeStatisticsOperation(Relation relation, Session session,
                                           StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            StatisticsOperationHandler statsHandler = createStatisticsHandler(session);

            // Get the input SQL for the operation
            String inputSql = null;
            Relation input = null;

            switch (relation.getRelTypeCase()) {
                case COV:
                    input = relation.getCov().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleCov(relation.getCov(), inputSql, session, responseObserver);
                    break;

                case CORR:
                    input = relation.getCorr().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleCorr(relation.getCorr(), inputSql, session, responseObserver);
                    break;

                case APPROX_QUANTILE:
                    input = relation.getApproxQuantile().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleApproxQuantile(relation.getApproxQuantile(), inputSql, session, responseObserver);
                    break;

                case DESCRIBE:
                    input = relation.getDescribe().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleDescribe(relation.getDescribe(), inputSql, session, responseObserver);
                    break;

                case SUMMARY:
                    input = relation.getSummary().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleSummary(relation.getSummary(), inputSql, session, responseObserver);
                    break;

                case CROSSTAB:
                    input = relation.getCrosstab().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleCrosstab(relation.getCrosstab(), inputSql, session, responseObserver);
                    break;

                case FREQ_ITEMS:
                    input = relation.getFreqItems().getInput();
                    inputSql = generateInputSql(input, session);
                    statsHandler.handleFreqItems(relation.getFreqItems(), inputSql, session, responseObserver);
                    break;

                case SAMPLE_BY:
                    input = relation.getSampleBy().getInput();
                    inputSql = generateInputSql(input, session);
                    // Get the column expression SQL
                    String colExpr = convertExpressionToSql(relation.getSampleBy().getCol(), session);
                    statsHandler.handleSampleBy(relation.getSampleBy(), inputSql, colExpr, session, responseObserver);
                    break;

                default:
                    responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("Unsupported statistics operation: " + relation.getRelTypeCase())
                        .asRuntimeException());
            }

        } catch (Exception e) {
            logger.error("Statistics operation failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Statistics operation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Generate SQL for an input relation.
     */
    private String generateInputSql(Relation input, Session session) {
        LogicalPlan logicalPlan = createPlanConverter(session).convertRelation(input);
        return sqlGenerator.generate(logicalPlan);
    }

    /**
     * Convert a proto Expression to SQL string.
     */
    private String convertExpressionToSql(org.apache.spark.connect.proto.Expression expr, Session session) {
        com.thunderduck.expression.Expression converted = createPlanConverter(session).convertExpression(expr);
        return converted.toSQL();
    }

    /**
     * Generate SQL for a statistics relation (for schema analysis).
     * This delegates to StatisticsOperationHandler's SQL generation.
     */
    private String generateStatisticsSql(Relation relation, Session session) {
        StatisticsOperationHandler statsHandler = createStatisticsHandler(session);
        String inputSql;
        Relation input;

        switch (relation.getRelTypeCase()) {
            case COV:
                input = relation.getCov().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateCovSql(relation.getCov(), inputSql);

            case CORR:
                input = relation.getCorr().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateCorrSql(relation.getCorr(), inputSql);

            case APPROX_QUANTILE:
                input = relation.getApproxQuantile().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateApproxQuantileSql(relation.getApproxQuantile(), inputSql);

            case DESCRIBE:
                input = relation.getDescribe().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateDescribeSql(relation.getDescribe(), inputSql);

            case SUMMARY:
                input = relation.getSummary().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateSummarySql(relation.getSummary(), inputSql);

            case CROSSTAB:
                input = relation.getCrosstab().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateCrosstabSql(relation.getCrosstab(), inputSql);

            case FREQ_ITEMS:
                input = relation.getFreqItems().getInput();
                inputSql = generateInputSql(input, session);
                return statsHandler.generateFreqItemsSql(relation.getFreqItems(), inputSql);

            case SAMPLE_BY:
                input = relation.getSampleBy().getInput();
                inputSql = generateInputSql(input, session);
                String colExpr = convertExpressionToSql(relation.getSampleBy().getCol(), session);
                return statsHandler.generateSampleBySql(relation.getSampleBy(), inputSql, colExpr);

            default:
                throw new IllegalArgumentException("Unsupported statistics relation type: " + relation.getRelTypeCase());
        }
    }

    /**
     * Analyze a plan and return metadata (schema, execution plan, etc.).
     *
     * @param request AnalyzePlanRequest
     * @param responseObserver Response observer
     */
    @SuppressWarnings("deprecation") // getSql() deprecated in Spark 4.0+, kept for backward compatibility
    @Override
    public void analyzePlan(AnalyzePlanRequest request,
                           StreamObserver<AnalyzePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("analyzePlan called for session: {}", sessionId);

        try {
            // Get or create session for metadata operation (doesn't acquire execution slot)
            Session session = sessionManager.getOrCreateSessionForMetadata(sessionId);

            // Handle different analysis types
            if (request.hasSchema()) {
                // Schema analysis - extract schema from plan
                Plan plan = request.getSchema().getPlan();
                logger.debug("Schema analysis for plan type: {}", plan.getOpTypeCase());

                try {
                    com.thunderduck.types.StructType schema = null;
                    String sql = null;

                    // Check if this is a SQL query (special handling needed)
                    if (plan.hasRoot() && plan.getRoot().hasSql()) {
                        // Direct SQL query - infer schema using LIMIT 0
                        sql = plan.getRoot().getSql().getQuery();
                        // Apply SQL preprocessing for Spark compatibility
                        sql = preprocessSQL(sql);
                        logger.debug("Analyzing SQL query schema: {}", sql.substring(0, Math.min(100, sql.length())));
                        schema = inferSchemaFromDuckDB(sql, sessionId);
                        // In strict mode, fix nullable flags and decimal precision for SQL queries
                        if (SparkCompatMode.isStrictMode() && schema != null) {
                            schema = fixCountNullable(sql, schema);
                            schema = fixDecimalPrecisionForComplexAggregates(sql, schema);
                        }

                    } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                        // SQL command - infer schema
                        SqlCommand sqlCommand = plan.getCommand().getSqlCommand();

                        // In Spark 4.0.1, the 'sql' field is deprecated and replaced with 'input' relation
                        if (sqlCommand.hasInput() && sqlCommand.getInput().hasSql()) {
                            sql = sqlCommand.getInput().getSql().getQuery();
                        } else if (!sqlCommand.getSql().isEmpty()) {
                            // Fallback for older clients or backward compatibility
                            sql = sqlCommand.getSql();
                        }

                        // Apply SQL preprocessing for Spark compatibility
                        sql = preprocessSQL(sql);
                        logger.debug("Analyzing SQL command schema: {}", sql.substring(0, Math.min(100, sql.length())));
                        schema = inferSchemaFromDuckDB(sql, sessionId);
                        // In strict mode, fix nullable flags and decimal precision for SQL queries
                        if (SparkCompatMode.isStrictMode() && schema != null) {
                            schema = fixCountNullable(sql, schema);
                            schema = fixDecimalPrecisionForComplexAggregates(sql, schema);
                        }

                    } else if (plan.hasRoot() && isStatisticsRelation(plan.getRoot())) {
                        // Statistics relation - generate SQL and infer schema
                        sql = generateStatisticsSql(plan.getRoot(), session);
                        logger.debug("Analyzing statistics relation schema: {}", sql.substring(0, Math.min(100, sql.length())));
                        schema = inferSchemaFromDuckDB(sql, sessionId);

                    } else {
                        // Regular plan - convert to SQL and infer schema from DuckDB
                        // This ensures correct types for aggregate functions (collect_list, countDistinct, etc.)
                        // whose return types depend on DuckDB execution, not static type inference.
                        LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);
                        sql = sqlGenerator.generate(logicalPlan);
                        com.thunderduck.types.StructType logicalSchema = logicalPlan.schema();

                        // Apply SQL preprocessing for Spark compatibility (spark_sum, spark_avg, casts)
                        sql = preprocessSQL(sql);
                        com.thunderduck.types.StructType duckDBSchema = inferSchemaFromDuckDB(sql, sessionId);

                        if (SparkCompatMode.isRelaxedMode()) {
                            // Relaxed mode: use DuckDB's actual types as-is
                            schema = duckDBSchema;
                        } else {
                            // Strict mode: merge nullable from logical plan, fix decimal precision
                            schema = mergeNullableOnly(duckDBSchema, logicalSchema);
                            schema = fixCountNullable(sql, schema);
                            schema = fixDecimalPrecisionForComplexAggregates(sql, schema);
                        }
                    }

                    // Convert to Spark Connect proto DataType
                    org.apache.spark.connect.proto.DataType protoDataType = convertSchemaToProto(schema);

                    // Build response with schema
                    AnalyzePlanResponse response = AnalyzePlanResponse.newBuilder()
                        .setSessionId(sessionId)
                        .setSchema(AnalyzePlanResponse.Schema.newBuilder()
                            .setSchema(protoDataType)
                            .build())
                        .build();

                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                    logger.debug("Schema analysis complete: {} fields", schema.size());

                } catch (Exception e) {
                    logger.error("Schema extraction failed", e);
                    responseObserver.onError(Status.INTERNAL
                        .withDescription("Schema analysis failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
                }
            } else if (request.hasIsLocal()) {
                // IS_LOCAL analysis - ThunderDuck is always local (single-node)
                AnalyzePlanResponse response = AnalyzePlanResponse.newBuilder()
                    .setSessionId(sessionId)
                    .setIsLocal(AnalyzePlanResponse.IsLocal.newBuilder()
                        .setIsLocal(true)
                        .build())
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                // Other analysis types not yet implemented
                logger.warn("Unsupported analyze type: {}", request.getAnalyzeCase());
                AnalyzePlanResponse response = AnalyzePlanResponse.newBuilder()
                    .setSessionId(sessionId)
                    .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }

        } catch (Exception e) {
            logger.error("Error analyzing plan for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Analysis failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Preprocess SQL query to make Spark SQL compatible with DuckDB.
     *
     * Applies various transformations:
     * - Convert backticks to double quotes for identifiers
     * - Fix string concatenation (+ to ||)
     * - Fix COUNT(*) aliasing
     * - Fix "returns" keyword usage
     * - Fix Q90 "at cross join" syntax
     *
     * Note: NULL ordering is handled at the DuckDB configuration level
     * (default_null_order='NULLS FIRST' is set in DuckDBRuntime)
     *
     * @param sql Original SQL query
     * @return Preprocessed SQL query
     */
    private String preprocessSQL(String sql) {
        // Translate Spark SQL backticks to DuckDB double quotes for identifier quoting
        sql = sql.replace('`', '"');

        // Fix count(*) column naming to match Spark (only if not already aliased)
        String sqlLower = sql.toLowerCase();
        if (!sqlLower.contains("order by")) {
            // Simple case: no ORDER BY in the query
            sql = sql.replaceAll("(?i)count\\s*\\(\\s*\\*\\s*\\)(?!\\s+as\\s+|\\s*[><=!]|\\s+(?!(?i)from|where|group|order|having|limit|union|intersect|except|join|on|and|or|into|by|desc|asc|with|select)(?-i)[a-z_][a-z0-9_]*)", "count(*) AS \"count(1)\"");
        } else {
            // Complex case: need to avoid ORDER BY context
            int orderByIndex = sqlLower.lastIndexOf("order by");
            String beforeOrderBy = sql.substring(0, orderByIndex);
            String orderByClause = sql.substring(orderByIndex);

            beforeOrderBy = beforeOrderBy.replaceAll("(?i)count\\s*\\(\\s*\\*\\s*\\)(?!\\s+as\\s+|\\s*[><=!]|\\s+(?!(?i)from|where|group|order|having|limit|union|intersect|except|join|on|and|or|into|by|desc|asc|with|select)(?-i)[a-z_][a-z0-9_]*)", "count(*) AS \"count(1)\"");
            sql = beforeOrderBy + orderByClause;
        }

        // Fix "returns" keyword used as alias without AS
        sql = sql.replaceAll("(\\)\\s+)(?i)returns(?=\\s*,|\\s*\\)|\\s+from|\\s*$)", "$1AS returns");

        // Fix string concatenation operator: Spark uses + but DuckDB uses ||
        sql = sql.replaceAll("(\\))\\s*\\+\\s*(')", "$1 || $2");
        sql = sql.replaceAll("(')\\s*\\+\\s*(coalesce|')", "$1 || $2");

        // Fix Q90: Remove "at" before "cross join"
        sql = sql.replaceAll("\\)\\s+at\\s+cross\\s+join", ") cross join");

        // Fix date extraction functions: DuckDB returns BIGINT, Spark returns INTEGER
        // Wrap year(...) with CAST(... AS INTEGER) for Spark compatibility
        // Must match function calls only (not 'year' in INTERVAL '1' year)
        // Skip if already wrapped in CAST(year(...) AS INTEGER) (e.g., generated SQL from DataFrame path)
        // Done before the integer division fix since that fix targets explicit CAST(x AS INTEGER)
        if (!sql.toUpperCase().contains("CAST(YEAR(")) {
            sql = sql.replaceAll("(?i)\\byear\\s*\\(([^)]+)\\)", "CAST(year($1) AS INTEGER)");
        }

        // Fix integer division/casting for Spark compatibility
        // Spark's cast(x/y as integer) truncates, DuckDB might round
        // Replace cast(expr as integer) with CAST(TRUNC(expr) AS INTEGER)
        // Skip CAST(year(...) AS INTEGER) since those are already correctly typed
        sql = sql.replaceAll("(?i)cast\\s*\\((?!year\\s*\\()(.*?)\\s+as\\s+integer\\s*\\)",
                            "CAST(TRUNC($1) AS INTEGER)");

        // In strict mode, use Spark-compatible aggregate functions from the DuckDB extension
        // spark_sum returns DECIMAL(min(p+10,38), s) for decimal input, BIGINT for integer input
        // spark_avg returns DECIMAL(min(p+4,38), min(s+4,18)) for decimal input
        // This matches Spark's type semantics exactly
        // Column naming (spark_sum -> sum) is handled by normalizeAggregateColumnName in schema layer
        if (SparkCompatMode.isStrictMode()) {
            sql = sql.replaceAll("(?i)\\bSUM\\s*\\(", "spark_sum(");
            sql = sql.replaceAll("(?i)\\bAVG\\s*\\(", "spark_avg(");

            // Fix decimal division precision: when the outermost SELECT has division
            // of aggregate results over multiplication expressions, the intermediate
            // DuckDB precision differs from Spark, causing wrong division scale.
            // Wrap these expressions in CAST(... AS DECIMAL(38,6)) to match Spark output.
            sql = fixDivisionDecimalCasts(sql);
        }

        // Fix case of DESC/ASC keywords - DuckDB requires uppercase
        // Using a simple replacement approach
        sql = sql.replaceAll("(?i)\\bdesc\\b", "DESC");
        sql = sql.replaceAll("(?i)\\basc\\b", "ASC");

        // Additional NULL ordering for ROLLUP queries
        // While DuckDB is configured with default_null_order='NULLS FIRST',
        // ROLLUP queries need explicit NULLS FIRST in ORDER BY for all columns
        if (sql.toLowerCase().contains("group by rollup") && sql.toLowerCase().contains("order by")) {
            // Find ORDER BY clause and add NULLS FIRST to ALL columns
            // This is tricky because we need to handle:
            // 1. Simple columns: col1, col2
            // 2. Columns with DESC/ASC: col1 DESC, col2 ASC
            // 3. CASE expressions: CASE WHEN ... END
            // 4. Function calls: func(col)

            // First, find the ORDER BY clause
            String sqlLowerCase = sql.toLowerCase();
            int orderByIndex = sqlLowerCase.lastIndexOf("order by");
            int limitIndex = sqlLowerCase.indexOf("limit", orderByIndex);

            String beforeOrderBy = sql.substring(0, orderByIndex);
            String orderByClause;
            String afterOrderBy = "";

            if (limitIndex > 0) {
                // Skip the "order by" prefix (8 chars)
                orderByClause = sql.substring(orderByIndex + 8, limitIndex).trim();
                afterOrderBy = sql.substring(limitIndex);
            } else {
                // Skip the "order by" prefix (8 chars)
                orderByClause = sql.substring(orderByIndex + 8).trim();
            }

            // Split the ORDER BY clause by commas (being careful with CASE expressions)
            // For simplicity, let's just add NULLS FIRST after DESC/ASC or at the end of each expression
            StringBuilder newOrderBy = new StringBuilder();
            int i = 0;
            int parenLevel = 0;
            int caseLevel = 0;
            StringBuilder currentExpr = new StringBuilder();

            while (i < orderByClause.length()) {
                char c = orderByClause.charAt(i);

                // Track parentheses
                if (c == '(') parenLevel++;
                else if (c == ')') parenLevel--;

                // Track CASE expressions
                String upcoming = orderByClause.substring(i).toLowerCase();
                if (upcoming.startsWith("case ")) caseLevel++;
                else if (upcoming.startsWith("end") && caseLevel > 0) {
                    currentExpr.append("END");
                    i += 3;
                    caseLevel--;
                    continue;
                }

                // Check for comma separator (only at top level)
                if (c == ',' && parenLevel == 0 && caseLevel == 0) {
                    // Process the current expression
                    String expr = currentExpr.toString().trim();
                    if (!expr.isEmpty()) {
                        // Check if it already has NULLS FIRST/LAST
                        if (!expr.toUpperCase().contains("NULLS ")) {
                            // Check if it ends with DESC or ASC
                            if (expr.toUpperCase().endsWith(" DESC") || expr.toUpperCase().endsWith(" ASC")) {
                                expr += " NULLS FIRST";
                            } else {
                                expr += " NULLS FIRST";
                            }
                        }
                        if (newOrderBy.length() > 0) {
                            newOrderBy.append(", ");
                        }
                        newOrderBy.append(expr);
                    }
                    currentExpr = new StringBuilder();
                } else {
                    currentExpr.append(c);
                }
                i++;
            }

            // Don't forget the last expression
            String expr = currentExpr.toString().trim();
            if (!expr.isEmpty()) {
                if (!expr.toUpperCase().contains("NULLS ")) {
                    expr += " NULLS FIRST";
                }
                if (newOrderBy.length() > 0) {
                    newOrderBy.append(", ");
                }
                newOrderBy.append(expr);
            }

            // Reconstruct the query
            sql = beforeOrderBy + "ORDER BY " + newOrderBy.toString() + " " + afterOrderBy;
        }

        // Translate Spark-specific SQL functions to DuckDB equivalents
        sql = translateSparkFunctions(sql);

        return sql;
    }

    /**
     * Translates Spark-specific SQL functions to DuckDB equivalents.
     *
     * <p>Handles functions like:
     * <ul>
     *   <li>NAMED_STRUCT('field', value, ...) → struct_pack(field := value, ...)</li>
     *   <li>MAP('k1', v1, 'k2', v2) → MAP(['k1', 'k2'], [v1, v2])</li>
     *   <li>STRUCT(v1, v2) → row(v1, v2)</li>
     *   <li>ARRAY(v1, v2) → list_value(v1, v2)</li>
     * </ul>
     *
     * @param sql the SQL string to translate
     * @return the translated SQL string
     */
    private String translateSparkFunctions(String sql) {
        // Defense-in-depth: Rewrite Spark function names to DuckDB equivalents
        // This handles simple 1:1 name mappings (DIRECT_MAPPINGS) in SQL strings
        // Note: RawSQLExpression.toSQL() also calls this, but we do it here as well
        // to catch any SQL strings that bypass RawSQLExpression
        sql = com.thunderduck.functions.FunctionRegistry.rewriteSQL(sql);

        // Translate NAMED_STRUCT to struct_pack
        sql = translateNamedStruct(sql);

        // Translate MAP function to DuckDB MAP syntax
        sql = translateMapFunction(sql);

        // Translate STRUCT to row
        sql = translateStructFunction(sql);

        // Translate ARRAY to list_value
        sql = translateArrayFunction(sql);

        return sql;
    }

    /**
     * Translates NAMED_STRUCT('field', value, ...) to struct_pack(field := value, ...)
     */
    private String translateNamedStruct(String sql) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        String sqlLower = sql.toLowerCase();

        while (i < sql.length()) {
            int funcStart = sqlLower.indexOf("named_struct(", i);
            if (funcStart == -1) {
                result.append(sql.substring(i));
                break;
            }

            // Check this isn't part of a larger identifier
            if (funcStart > 0 && Character.isLetterOrDigit(sql.charAt(funcStart - 1))) {
                result.append(sql.substring(i, funcStart + 13));
                i = funcStart + 13;
                continue;
            }

            // Append everything before the function
            result.append(sql.substring(i, funcStart));

            // Find the matching closing parenthesis
            int parenStart = funcStart + 12; // Position of '('
            int parenEnd = findMatchingParen(sql, parenStart);

            if (parenEnd == -1) {
                // No matching paren found, just append and continue
                result.append(sql.substring(funcStart));
                break;
            }

            // Extract arguments
            String argsStr = sql.substring(parenStart + 1, parenEnd);
            java.util.List<String> args = parseArguments(argsStr);

            // Build struct_pack syntax
            result.append("struct_pack(");
            for (int j = 0; j < args.size(); j += 2) {
                if (j > 0) {
                    result.append(", ");
                }
                // Extract field name from quoted string
                String fieldName = extractFieldNameFromSQL(args.get(j));
                String value = (j + 1 < args.size()) ? args.get(j + 1) : "NULL";
                result.append(fieldName).append(" := ").append(value);
            }
            result.append(")");

            i = parenEnd + 1;
            sqlLower = sql.toLowerCase(); // Refresh for next iteration
        }

        return result.toString();
    }

    /**
     * Translates MAP('k1', v1, 'k2', v2) to MAP(['k1', 'k2'], [v1, v2])
     */
    private String translateMapFunction(String sql) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        String sqlLower = sql.toLowerCase();

        while (i < sql.length()) {
            // Look for standalone "map(" - not "map_keys", "map_values", etc.
            int funcStart = sqlLower.indexOf("map(", i);
            if (funcStart == -1) {
                result.append(sql.substring(i));
                break;
            }

            // Check this isn't part of a larger identifier (e.g., map_keys, bitmap, etc.)
            if (funcStart > 0 && (Character.isLetterOrDigit(sql.charAt(funcStart - 1)) || sql.charAt(funcStart - 1) == '_')) {
                result.append(sql.substring(i, funcStart + 4));
                i = funcStart + 4;
                continue;
            }

            // Append everything before the function
            result.append(sql.substring(i, funcStart));

            // Find the matching closing parenthesis
            int parenStart = funcStart + 3; // Position of '('
            int parenEnd = findMatchingParen(sql, parenStart);

            if (parenEnd == -1) {
                result.append(sql.substring(funcStart));
                break;
            }

            // Extract arguments
            String argsStr = sql.substring(parenStart + 1, parenEnd);
            java.util.List<String> args = parseArguments(argsStr);

            // Build DuckDB MAP syntax: MAP([keys], [values])
            if (args.isEmpty()) {
                result.append("MAP([], [])");
            } else {
                StringBuilder keys = new StringBuilder("[");
                StringBuilder values = new StringBuilder("[");

                for (int j = 0; j < args.size(); j += 2) {
                    if (j > 0) {
                        keys.append(", ");
                        values.append(", ");
                    }
                    keys.append(args.get(j));
                    values.append((j + 1 < args.size()) ? args.get(j + 1) : "NULL");
                }

                keys.append("]");
                values.append("]");

                result.append("MAP(").append(keys).append(", ").append(values).append(")");
            }

            i = parenEnd + 1;
            sqlLower = sql.toLowerCase();
        }

        return result.toString();
    }

    /**
     * Translates STRUCT(v1, v2) to row(v1, v2)
     */
    private String translateStructFunction(String sql) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        String sqlLower = sql.toLowerCase();

        while (i < sql.length()) {
            int funcStart = sqlLower.indexOf("struct(", i);
            if (funcStart == -1) {
                result.append(sql.substring(i));
                break;
            }

            // Check this isn't part of a larger identifier (e.g., named_struct)
            if (funcStart > 0 && (Character.isLetterOrDigit(sql.charAt(funcStart - 1)) || sql.charAt(funcStart - 1) == '_')) {
                result.append(sql.substring(i, funcStart + 7));
                i = funcStart + 7;
                continue;
            }

            // Append everything before, then replace "struct(" with "row("
            result.append(sql.substring(i, funcStart));
            result.append("row(");

            i = funcStart + 7; // Skip past "struct("
            sqlLower = sql.toLowerCase();
        }

        return result.toString();
    }

    /**
     * Translates ARRAY(v1, v2) to list_value(v1, v2)
     */
    private String translateArrayFunction(String sql) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        String sqlLower = sql.toLowerCase();

        while (i < sql.length()) {
            int funcStart = sqlLower.indexOf("array(", i);
            if (funcStart == -1) {
                result.append(sql.substring(i));
                break;
            }

            // Check this isn't part of a larger identifier (e.g., array_contains)
            if (funcStart > 0 && (Character.isLetterOrDigit(sql.charAt(funcStart - 1)) || sql.charAt(funcStart - 1) == '_')) {
                result.append(sql.substring(i, funcStart + 6));
                i = funcStart + 6;
                continue;
            }

            // Also check it's not followed by a letter/underscore (e.g., ARRAY<INT>)
            int afterParen = funcStart + 5;
            if (afterParen < sql.length() && sql.charAt(afterParen) == '<') {
                // This is ARRAY<type> cast syntax, not ARRAY() constructor
                result.append(sql.substring(i, funcStart + 6));
                i = funcStart + 6;
                continue;
            }

            // Append everything before, then replace "array(" with "list_value("
            result.append(sql.substring(i, funcStart));
            result.append("list_value(");

            i = funcStart + 6; // Skip past "array("
            sqlLower = sql.toLowerCase();
        }

        return result.toString();
    }

    /**
     * Finds the position of the matching closing parenthesis.
     * @param sql the SQL string
     * @param openParenPos the position of the opening parenthesis
     * @return the position of the matching closing paren, or -1 if not found
     */
    private int findMatchingParen(String sql, int openParenPos) {
        if (openParenPos >= sql.length() || sql.charAt(openParenPos) != '(') {
            return -1;
        }

        int level = 1;
        boolean inString = false;
        char stringChar = 0;

        for (int i = openParenPos + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (inString) {
                if (c == stringChar) {
                    // Check for escaped quote
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        i++; // Skip escaped quote
                    } else {
                        inString = false;
                    }
                }
            } else {
                if (c == '\'' || c == '"') {
                    inString = true;
                    stringChar = c;
                } else if (c == '(') {
                    level++;
                } else if (c == ')') {
                    level--;
                    if (level == 0) {
                        return i;
                    }
                }
            }
        }

        return -1;
    }

    /**
     * Parses a comma-separated argument list, respecting nested parens and strings.
     */
    private java.util.List<String> parseArguments(String argsStr) {
        java.util.List<String> args = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parenLevel = 0;
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < argsStr.length(); i++) {
            char c = argsStr.charAt(i);

            if (inString) {
                current.append(c);
                if (c == stringChar) {
                    if (i + 1 < argsStr.length() && argsStr.charAt(i + 1) == stringChar) {
                        current.append(argsStr.charAt(i + 1));
                        i++; // Skip escaped quote
                    } else {
                        inString = false;
                    }
                }
            } else {
                if (c == '\'' || c == '"') {
                    inString = true;
                    stringChar = c;
                    current.append(c);
                } else if (c == '(') {
                    parenLevel++;
                    current.append(c);
                } else if (c == ')') {
                    parenLevel--;
                    current.append(c);
                } else if (c == ',' && parenLevel == 0) {
                    args.add(current.toString().trim());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }

        if (current.length() > 0) {
            args.add(current.toString().trim());
        }

        return args;
    }

    /**
     * Extracts a field name from a SQL string literal.
     * Removes surrounding quotes and handles escaped quotes.
     */
    private String extractFieldNameFromSQL(String quotedName) {
        if (quotedName == null) {
            return "field";
        }
        String trimmed = quotedName.trim();
        // Remove surrounding single quotes
        if (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length() >= 2) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            // Handle escaped quotes
            return inner.replace("''", "'");
        }
        // Remove surrounding double quotes
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            return inner.replace("\"\"", "\"");
        }
        return trimmed;
    }

    /**
     * Get or set configuration values.
     *
     * Handles GetWithDefault and Get operations for PySpark client compatibility.
     *
     * @param request ConfigRequest
     * @param responseObserver Response observer
     */
    @Override
    public void config(ConfigRequest request,
                      StreamObserver<ConfigResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("config called for session: {}", sessionId);

        try {
            // Get or create session for metadata operation (doesn't acquire execution slot)
            Session session = sessionManager.getOrCreateSessionForMetadata(sessionId);

            ConfigResponse.Builder responseBuilder = ConfigResponse.newBuilder()
                .setSessionId(sessionId);

            // Handle different operation types
            if (request.hasOperation()) {
                ConfigRequest.Operation op = request.getOperation();

                switch (op.getOpTypeCase()) {
                    case GET_WITH_DEFAULT:
                        // Return the default values provided by the client
                        ConfigRequest.GetWithDefault getWithDefault = op.getGetWithDefault();
                        for (KeyValue kv : getWithDefault.getPairsList()) {
                            responseBuilder.addPairs(KeyValue.newBuilder()
                                .setKey(kv.getKey())
                                .setValue(kv.getValue())
                                .build());
                        }
                        logger.debug("Returning default configs: {} pairs", getWithDefault.getPairsCount());
                        break;

                    case GET:
                        // Return actual configuration values from session
                        ConfigRequest.Get get = op.getGet();
                        // session is already available from getOrCreateSessionForMetadata above

                        // If no specific keys requested, return all configs
                        if (get.getKeysCount() == 0) {
                            Map<String, String> allConfig = session.getAllConfig();
                            for (Map.Entry<String, String> entry : allConfig.entrySet()) {
                                responseBuilder.addPairs(KeyValue.newBuilder()
                                    .setKey(entry.getKey())
                                    .setValue(entry.getValue())
                                    .build());
                            }
                            logger.debug("Returning all configs: {} pairs", allConfig.size());
                        } else {
                            // Return requested keys only
                            for (String key : get.getKeysList()) {
                                String value = session.getConfig(key);
                                if (value != null) {
                                    responseBuilder.addPairs(KeyValue.newBuilder()
                                        .setKey(key)
                                        .setValue(value)
                                        .build());
                                }
                            }
                            logger.debug("Returning {} configs out of {} requested keys",
                                responseBuilder.getPairsCount(), get.getKeysList());
                        }
                        break;

                    default:
                        logger.debug("Config operation type {} not implemented", op.getOpTypeCase());
                        break;
                }
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling config for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Config operation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Add artifacts (JARs, files) to session.
     * Not implemented - DuckDB doesn't support dynamic artifact loading.
     */
    @Override
    public StreamObserver<AddArtifactsRequest> addArtifacts(
            StreamObserver<AddArtifactsResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact management not supported")
            .asRuntimeException());
        return null;
    }

    /**
     * Check artifact status.
     * Not implemented - DuckDB doesn't support dynamic artifact loading.
     */
    @Override
    public void artifactStatus(ArtifactStatusesRequest request,
                              StreamObserver<ArtifactStatusesResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact status not supported")
            .asRuntimeException());
    }

    /**
     * Interrupt running execution.
     * Not implemented - requires async execution tracking.
     */
    @Override
    public void interrupt(InterruptRequest request,
                         StreamObserver<InterruptResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Query interruption not supported")
            .asRuntimeException());
    }

    /**
     * Reattach to existing execution.
     *
     * Since queries complete synchronously, this returns an empty stream with
     * ResultComplete to indicate the execution has already finished.
     * This allows PySpark clients with reattachable execution to work properly.
     */
    @Override
    public void reattachExecute(ReattachExecuteRequest request,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("reattachExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // Queries complete synchronously, so return ResultComplete to indicate done.

        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.debug("Reattach completed (operation already finished): {}", operationId);
    }

    /**
     * Release reattachable execution.
     *
     * Since queries complete synchronously, this is a no-op that returns
     * success to satisfy the PySpark client cleanup flow.
     */
    @Override
    public void releaseExecute(ReleaseExecuteRequest request,
                              StreamObserver<ReleaseExecuteResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("releaseExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // Return success response (no-op since queries complete synchronously)
        ReleaseExecuteResponse response = ReleaseExecuteResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.debug("Released operation: {}", operationId);
    }

    // ========== Helper Methods ==========

    /**
     * Execute SQL query and format results as ShowString (text table).
     *
     * @param sql SQL query string
     * @param session Session object
     * @param numRows Maximum rows to show
     * @param truncate Maximum column width (0 = no truncation)
     * @param vertical Show vertically if true
     * @param responseObserver Response stream
     */
    private void executeShowString(String sql, Session session, int numRows, int truncate,
                                   boolean vertical, StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            logger.info("[{}] Executing ShowString SQL for session {}: {}", operationId, session.getSessionId(), sql);

            // Create QueryExecutor with connection manager
            QueryExecutor executor = new QueryExecutor(session.getRuntime());

            // Execute query and get Arrow results
            org.apache.arrow.vector.VectorSchemaRoot results = executor.executeQuery(sql);

            // Format results as text table
            String formattedText = formatAsTextTable(results, numRows, truncate, vertical);

            // Create a new VectorSchemaRoot with single column 'show_string'
            org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator();
            org.apache.arrow.vector.VarCharVector showStringVector = new org.apache.arrow.vector.VarCharVector(
                "show_string", allocator);

            java.util.List<org.apache.arrow.vector.FieldVector> vectors = new java.util.ArrayList<>();
            vectors.add(showStringVector);
            java.util.List<org.apache.arrow.vector.types.pojo.Field> fields = new java.util.ArrayList<>();
            fields.add(org.apache.arrow.vector.types.pojo.Field.nullable("show_string",
                new org.apache.arrow.vector.types.pojo.ArrowType.Utf8()));

            org.apache.arrow.vector.VectorSchemaRoot showStringRoot = new org.apache.arrow.vector.VectorSchemaRoot(
                fields, vectors);

            // Set the formatted text in the vector
            showStringRoot.setRowCount(1);
            showStringVector.allocateNew();
            showStringVector.set(0, formattedText.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            showStringVector.setValueCount(1);

            // Stream the ShowString result
            streamArrowResults(showStringRoot, session.getSessionId(), operationId, responseObserver);

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[{}] ShowString completed in {}ms", operationId, durationMs);

            // Clean up resources
            showStringRoot.close();
            showStringVector.close();
            allocator.close();
            results.close();

        } catch (Exception e) {
            logger.error("[{}] ShowString execution failed", operationId, e);

            Status status;
            if (e instanceof java.sql.SQLException) {
                status = Status.INVALID_ARGUMENT.withDescription("SQL error: " + e.getMessage());
            } else if (e instanceof IllegalArgumentException) {
                status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            } else {
                status = Status.INTERNAL.withDescription("Execution failed: " + e.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }
    }

    /**
     * Format Arrow results as a text table (similar to Spark's showString).
     */
    private String formatAsTextTable(org.apache.arrow.vector.VectorSchemaRoot root,
                                      int maxRows, int truncate, boolean vertical) {
        StringBuilder sb = new StringBuilder();

        int rowCount = Math.min(root.getRowCount(), maxRows);
        java.util.List<org.apache.arrow.vector.FieldVector> vectors = root.getFieldVectors();

        if (vertical) {
            // Vertical format: one column per line
            for (int row = 0; row < rowCount; row++) {
                sb.append("-RECORD ").append(row).append("-\n");
                for (org.apache.arrow.vector.FieldVector vector : vectors) {
                    String colName = vector.getField().getName();
                    String value = getValueAsString(vector, row, truncate);
                    sb.append(" ").append(colName).append(" : ").append(value).append("\n");
                }
            }
        } else {
            // Horizontal format: traditional table
            // Calculate column widths
            java.util.List<Integer> columnWidths = new java.util.ArrayList<>();
            java.util.List<String> columnNames = new java.util.ArrayList<>();

            for (org.apache.arrow.vector.FieldVector vector : vectors) {
                String colName = vector.getField().getName();
                columnNames.add(colName);
                int maxWidth = colName.length();

                // Check data widths
                for (int row = 0; row < rowCount; row++) {
                    String value = getValueAsString(vector, row, truncate);
                    maxWidth = Math.max(maxWidth, value.length());
                }
                columnWidths.add(maxWidth);
            }

            // Print separator
            sb.append("+");
            for (int width : columnWidths) {
                for (int i = 0; i < width + 2; i++) sb.append("-");
                sb.append("+");
            }
            sb.append("\n");

            // Print header
            sb.append("|");
            for (int i = 0; i < columnNames.size(); i++) {
                sb.append(" ");
                sb.append(padRight(columnNames.get(i), columnWidths.get(i)));
                sb.append(" |");
            }
            sb.append("\n");

            // Print separator
            sb.append("+");
            for (int width : columnWidths) {
                for (int i = 0; i < width + 2; i++) sb.append("-");
                sb.append("+");
            }
            sb.append("\n");

            // Print data rows
            for (int row = 0; row < rowCount; row++) {
                sb.append("|");
                for (int col = 0; col < vectors.size(); col++) {
                    String value = getValueAsString(vectors.get(col), row, truncate);
                    sb.append(" ");
                    sb.append(padRight(value, columnWidths.get(col)));
                    sb.append(" |");
                }
                sb.append("\n");
            }

            // Print final separator
            sb.append("+");
            for (int width : columnWidths) {
                for (int i = 0; i < width + 2; i++) sb.append("-");
                sb.append("+");
            }
            sb.append("\n");
        }

        // Add row count info if truncated
        if (root.getRowCount() > maxRows) {
            sb.append("only showing top ").append(maxRows).append(" rows\n");
        }

        return sb.toString();
    }

    private String getValueAsString(org.apache.arrow.vector.FieldVector vector, int index, int truncate) {
        if (vector.isNull(index)) {
            return "null";
        }

        String value = vector.getObject(index).toString();
        if (truncate > 0 && value.length() > truncate) {
            value = value.substring(0, truncate - 3) + "...";
        }
        return value;
    }

    private String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }

    /**
     * Execute SQL query and stream results as Arrow batches.
     *
     * <p>Handles both queries (SELECT) and DDL statements (CREATE, DROP, ALTER, etc.).
     * DDL statements return an empty result set with success status.
     *
     * <p>If streaming is enabled (via -Dthunderduck.streaming.enabled=true), uses
     * the new ArrowStreamingExecutor for batch-by-batch streaming. Otherwise, uses
     * the legacy full-materialization path.
     *
     * @param sql SQL query string
     * @param session Session object
     * @param responseObserver Response stream
     */
    private void executeSQL(String sql, Session session,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        // DDL statements don't return results - handle separately
        if (isDDLStatement(sql)) {
            executeDDL(sql, session, responseObserver);
            return;
        }

        // In strict mode, build nullable schema from SQL analysis for COUNT columns
        if (SparkCompatMode.isStrictMode()) {
            try {
                StructType nullableSchema = inferNullableSchemaFromSQL(sql, session.getSessionId());
                if (nullableSchema != null) {
                    executeSQLStreaming(sql, nullableSchema, session, responseObserver);
                    return;
                }
            } catch (Exception e) {
                logger.debug("Could not infer nullable schema from SQL, falling back to default: {}", e.getMessage());
            }
        }

        // All queries use streaming (zero-copy Arrow batch iteration)
        executeSQLStreaming(sql, session, responseObserver);
    }

    /**
     * Execute SQL query using streaming Arrow batch iteration.
     *
     * <p>Uses DuckDB's native arrowExportStream() for zero-copy batch streaming.
     * Each batch is serialized to Arrow IPC format and sent immediately to the client.
     *
     * @param sql SQL query string
     * @param session Session object
     * @param responseObserver Response stream
     */
    private void executeSQLStreaming(String sql, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver) {
        executeSQLStreaming(sql, null, session, responseObserver, -1);
    }

    /**
     * Execute SQL query with schema correction for correct nullable flags.
     *
     * <p>When a logical schema is provided, the Arrow output from DuckDB is wrapped
     * to correct nullable flags to match Spark semantics.
     *
     * @param sql SQL query string
     * @param logicalSchema the logical plan schema with correct nullable flags (can be null)
     * @param session Session object
     * @param responseObserver Response stream
     */
    private void executeSQLStreaming(String sql, StructType logicalSchema, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver) {
        executeSQLStreaming(sql, logicalSchema, session, responseObserver, -1);
    }

    /**
     * Execute SQL query with streaming Arrow batches, optionally with tail collection.
     *
     * <p>When tailLimit > 0, the results are wrapped in a {@link TailBatchIterator}
     * that collects all batches and returns only the last N rows. This is memory-efficient
     * with O(N) memory where N is the tail limit.
     *
     * <p>When logicalSchema is provided, the results are wrapped in a
     * {@link SchemaCorrectedBatchIterator} that corrects nullable flags to match Spark semantics.
     *
     * @param sql SQL query string
     * @param logicalSchema the logical plan schema with correct nullable flags (can be null)
     * @param session Session object
     * @param responseObserver Response stream
     * @param tailLimit if >= 0, return only the last tailLimit rows; -1 for all rows
     */
    private void executeSQLStreaming(String sql, StructType logicalSchema, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver,
                                     int tailLimit) {
        executeSQLStreaming(sql, logicalSchema, session, responseObserver, tailLimit, null);
    }

    /**
     * Execute SQL query with streaming Arrow batches and timing instrumentation.
     *
     * <p>This is the main entry point for DataFrame plan execution with full timing
     * instrumentation across all phases.
     *
     * @param sql SQL query string
     * @param logicalSchema the logical plan schema with correct nullable flags (can be null)
     * @param session Session object
     * @param responseObserver Response stream
     * @param tailLimit if >= 0, return only the last tailLimit rows; -1 for all rows
     * @param timing timing stats collector (can be null for direct SQL execution)
     */
    private void executeSQLStreaming(String sql, StructType logicalSchema, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver,
                                     int tailLimit, QueryTimingStats timing) {
        String operationId = java.util.UUID.randomUUID().toString();

        try {
            String opDesc = tailLimit > 0 ? "streaming tail(" + tailLimit + ")" : "streaming";
            logger.info("[{}] Executing {} SQL for session {}: {}",
                operationId, opDesc, session.getSessionId(),
                sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);

            // Get cached executor from session (reuses shared allocator)
            // This avoids creating a new RootAllocator for each query, reducing overhead by 2-5ms
            ArrowStreamingExecutor executor = session.getStreamingExecutor();

            // Time DuckDB execution (to first batch)
            if (timing != null) timing.startDuckdbExecute();
            try (ArrowBatchIterator baseIterator = executor.executeStreaming(sql)) {
                if (timing != null) timing.stopDuckdbExecute();

                ArrowBatchIterator iterator = baseIterator;

                if (SparkCompatMode.isRelaxedMode() || logicalSchema == null) {
                    // Relaxed mode: no schema correction — return DuckDB's Arrow batches as-is.
                    // Also skip correction when there's no logical schema (direct SQL queries).
                    logger.debug("[{}] Skipping schema correction (relaxed={}, logicalSchema={})",
                        operationId, SparkCompatMode.isRelaxedMode(), logicalSchema != null);
                } else {
                    // Strict mode: correct nullable flags only (DuckDB returns all nullable=true,
                    // but Spark has correct nullable semantics from the logical plan)
                    iterator = new SchemaCorrectedBatchIterator(iterator, logicalSchema, executor.getAllocator());
                }

                // Wrap with TailBatchIterator if tail operation requested
                if (tailLimit >= 0) {
                    // TailBatchIterator needs the allocator from executor to share root
                    // tailLimit >= 0 because tail(0) should return empty result, not all rows
                    iterator = new TailBatchIterator(iterator, executor.getAllocator(), tailLimit);
                }

                // Time result streaming
                if (timing != null) timing.startResultStream();
                try (ArrowBatchIterator effectiveIterator = iterator) {
                    StreamingResultHandler handler = new StreamingResultHandler(
                        responseObserver, session.getSessionId(), operationId);
                    handler.streamResults(effectiveIterator);

                    if (timing != null) {
                        timing.stopResultStream();
                        timing.stopTotal();
                        timing.setBatchCount(handler.getBatchCount());
                        timing.setRowCount(handler.getTotalRows());
                        logger.info("[{}] Query timing: {}", operationId, timing.toLogString());
                    } else {
                        // Fallback logging when no timing stats provided
                        logger.info("[{}] {} query completed, {} batches, {} rows",
                            operationId, opDesc, handler.getBatchCount(), handler.getTotalRows());
                    }
                }
            }

        } catch (Exception e) {
            logger.error("[{}] Streaming SQL execution failed", operationId, e);

            // Determine appropriate gRPC status code based on exception type
            Status status;
            if (e instanceof java.sql.SQLException) {
                status = Status.INVALID_ARGUMENT.withDescription("SQL error: " + e.getMessage());
            } else if (e instanceof IllegalArgumentException) {
                status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            } else {
                status = Status.INTERNAL.withDescription("Execution failed: " + e.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }
    }

    /**
     * Execute DDL statement (CREATE, DROP, ALTER, etc.).
     *
     * <p>DDL statements don't return results - they return an empty success response.
     *
     * @param sql DDL statement
     * @param session Session object
     * @param responseObserver Response stream
     */
    private void executeDDL(String sql, Session session,
                            StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            logger.info("[{}] Executing DDL for session {}: {}", operationId, session.getSessionId(), sql);

            // Create QueryExecutor with connection manager
            QueryExecutor executor = new QueryExecutor(session.getRuntime());

            // Execute DDL using executeUpdate (no ResultSet)
            executor.executeUpdate(sql);

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[{}] DDL completed in {}ms", operationId, durationMs);

            // Return empty success response for DDL
            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(session.getSessionId())
                .setOperationId(operationId)
                .setResponseId(java.util.UUID.randomUUID().toString())
                .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("[{}] DDL execution failed", operationId, e);

            // Determine appropriate gRPC status code based on exception type
            Status status;
            if (e instanceof java.sql.SQLException) {
                status = Status.INVALID_ARGUMENT.withDescription("SQL error: " + e.getMessage());
            } else if (e instanceof IllegalArgumentException) {
                status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
            } else {
                status = Status.INTERNAL.withDescription("Execution failed: " + e.getMessage());
            }

            responseObserver.onError(status.asRuntimeException());
        }
    }

    /**
     * Checks if a SQL statement is a DDL/DML statement that doesn't return a ResultSet.
     *
     * <p>Non-query statements include:
     * <ul>
     *   <li>DDL: CREATE, DROP, ALTER, TRUNCATE, RENAME, GRANT, REVOKE</li>
     *   <li>DML: INSERT, UPDATE, DELETE, MERGE</li>
     *   <li>Session: SET, RESET, USE, PRAGMA</li>
     *   <li>Admin: INSTALL, LOAD, ATTACH, DETACH, CHECKPOINT, VACUUM</li>
     * </ul>
     * These need to be executed with executeUpdate() instead of executeQuery().
     *
     * @param sql the SQL statement to check
     * @return true if the statement is DDL/DML (non-query)
     */
    private boolean isDDLStatement(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }

        String trimmed = sql.trim().toUpperCase();

        // Check for DDL keywords
        return trimmed.startsWith("CREATE ") ||
               trimmed.startsWith("DROP ") ||
               trimmed.startsWith("ALTER ") ||
               trimmed.startsWith("TRUNCATE ") ||
               trimmed.startsWith("RENAME ") ||
               trimmed.startsWith("GRANT ") ||
               trimmed.startsWith("REVOKE ") ||
               // DML statements (don't return ResultSet)
               trimmed.startsWith("INSERT ") ||
               // UPDATE and DELETE removed - not supported on V1 tables in Spark
               trimmed.startsWith("MERGE ") ||
               // Session/admin commands
               trimmed.startsWith("SET ") ||
               trimmed.startsWith("RESET ") ||
               trimmed.startsWith("PRAGMA ") ||
               trimmed.startsWith("INSTALL ") ||
               trimmed.startsWith("LOAD ") ||
               trimmed.startsWith("ATTACH ") ||
               trimmed.startsWith("DETACH ") ||
               trimmed.startsWith("USE ") ||
               trimmed.startsWith("CHECKPOINT ") ||
               trimmed.startsWith("VACUUM ");
    }

    /**
     * Stream a single Arrow batch to client.
     *
     * Used for small result sets like ShowString or Tail operations.
     * For large result sets, use StreamingResultHandler with ArrowBatchIterator.
     *
     * @param root Arrow VectorSchemaRoot with results
     * @param sessionId Session ID
     * @param operationId Operation ID for logging
     * @param responseObserver Response stream
     */
    private void streamArrowResults(org.apache.arrow.vector.VectorSchemaRoot root,
                                    String sessionId,
                                    String operationId,
                                    StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            // Serialize Arrow data to bytes using Arrow IPC format
            java.io.ByteArrayOutputStream dataOut = new java.io.ByteArrayOutputStream();
            org.apache.arrow.vector.ipc.ArrowStreamWriter writer =
                new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                    root,
                    null,  // DictionaryProvider
                    java.nio.channels.Channels.newChannel(dataOut)
                );

            // CRITICAL: Must call start() to write schema header before data!
            // Without this, PySpark cannot properly read DATE columns and other types
            writer.start();
            writer.writeBatch();
            writer.end();
            writer.close();

            byte[] arrowData = dataOut.toByteArray();

            // Build gRPC response with Arrow data
            ExecutePlanResponse.Builder arrowResponseBuilder = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(java.util.UUID.randomUUID().toString())
                .setArrowBatch(org.apache.spark.connect.proto.ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(root.getRowCount())
                    .setData(com.google.protobuf.ByteString.copyFrom(arrowData))
                    .build());

            // Add schema information for client compatibility
            // Note: Schema is embedded in Arrow IPC format, but some clients expect it separately
            // For now, we'll send without explicit schema field as it's optional

            ExecutePlanResponse arrowResponse = arrowResponseBuilder.build();

            responseObserver.onNext(arrowResponse);

            // Send ResultComplete to indicate execution is finished (required for reattachable execution)
            ExecutePlanResponse completeResponse = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(java.util.UUID.randomUUID().toString())
                .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
                .build();

            responseObserver.onNext(completeResponse);
            responseObserver.onCompleted();

            logger.debug("[{}] Streamed {} rows ({} bytes)",
                operationId, root.getRowCount(), arrowData.length);

        } catch (java.io.IOException e) {
            logger.error("[{}] Arrow serialization failed", operationId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Result serialization failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    // ==================== Helper Methods for Schema Conversion ====================

    /**
     * Converts thunderduck StructType to Spark Connect DataType.
     */
    private org.apache.spark.connect.proto.DataType convertSchemaToProto(com.thunderduck.types.StructType schema) {
        org.apache.spark.connect.proto.DataType.Struct.Builder structBuilder =
            org.apache.spark.connect.proto.DataType.Struct.newBuilder();

        for (com.thunderduck.types.StructField field : schema.fields()) {
            org.apache.spark.connect.proto.DataType.StructField.Builder fieldBuilder =
                org.apache.spark.connect.proto.DataType.StructField.newBuilder()
                    .setName(field.name())
                    .setNullable(field.nullable())
                    .setDataType(convertDataTypeToProto(field.dataType()));
            structBuilder.addFields(fieldBuilder);
        }

        return org.apache.spark.connect.proto.DataType.newBuilder()
            .setStruct(structBuilder)
            .build();
    }

    /**
     * Converts thunderduck DataType to Spark Connect DataType.
     */
    private org.apache.spark.connect.proto.DataType convertDataTypeToProto(com.thunderduck.types.DataType dataType) {
        if (dataType instanceof com.thunderduck.types.IntegerType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setInteger(org.apache.spark.connect.proto.DataType.Integer.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.LongType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setLong(org.apache.spark.connect.proto.DataType.Long.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.DoubleType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setDouble(org.apache.spark.connect.proto.DataType.Double.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.FloatType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setFloat(org.apache.spark.connect.proto.DataType.Float.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.StringType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setString(org.apache.spark.connect.proto.DataType.String.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.BooleanType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setBoolean(org.apache.spark.connect.proto.DataType.Boolean.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.DateType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setDate(org.apache.spark.connect.proto.DataType.Date.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.TimestampType) {
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setTimestamp(org.apache.spark.connect.proto.DataType.Timestamp.newBuilder().build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.DecimalType) {
            com.thunderduck.types.DecimalType decimalType = (com.thunderduck.types.DecimalType) dataType;
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setDecimal(org.apache.spark.connect.proto.DataType.Decimal.newBuilder()
                    .setPrecision(decimalType.precision())
                    .setScale(decimalType.scale())
                    .build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.ArrayType) {
            com.thunderduck.types.ArrayType arrayType = (com.thunderduck.types.ArrayType) dataType;
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setArray(org.apache.spark.connect.proto.DataType.Array.newBuilder()
                    .setElementType(convertDataTypeToProto(arrayType.elementType()))
                    .setContainsNull(arrayType.containsNull())  // Preserve actual containsNull flag
                    .build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.MapType) {
            com.thunderduck.types.MapType mapType = (com.thunderduck.types.MapType) dataType;
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setMap(org.apache.spark.connect.proto.DataType.Map.newBuilder()
                    .setKeyType(convertDataTypeToProto(mapType.keyType()))
                    .setValueType(convertDataTypeToProto(mapType.valueType()))
                    .setValueContainsNull(mapType.valueContainsNull())  // Preserve actual valueContainsNull flag
                    .build())
                .build();
        } else if (dataType instanceof com.thunderduck.types.StructType) {
            com.thunderduck.types.StructType structType = (com.thunderduck.types.StructType) dataType;
            org.apache.spark.connect.proto.DataType.Struct.Builder structBuilder =
                org.apache.spark.connect.proto.DataType.Struct.newBuilder();
            for (com.thunderduck.types.StructField field : structType.fields()) {
                org.apache.spark.connect.proto.DataType.StructField.Builder fieldBuilder =
                    org.apache.spark.connect.proto.DataType.StructField.newBuilder()
                        .setName(field.name())
                        .setNullable(field.nullable())
                        .setDataType(convertDataTypeToProto(field.dataType()));
                structBuilder.addFields(fieldBuilder);
            }
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setStruct(structBuilder)
                .build();
        } else {
            // Default to string for unsupported types
            logger.warn("Unsupported data type for schema conversion: {}, defaulting to STRING", dataType.getClass().getSimpleName());
            return org.apache.spark.connect.proto.DataType.newBuilder()
                .setString(org.apache.spark.connect.proto.DataType.String.newBuilder().build())
                .build();
        }
    }

    /**
     * Infers schema from DuckDB by executing LIMIT 0 query.
     *
     * @param sql The SQL query to infer schema from
     * @param sessionId The session ID to use for accessing DuckDB runtime with temp views
     */
    private com.thunderduck.types.StructType inferSchemaFromDuckDB(String sql, String sessionId) throws Exception {
        // Use the session's DuckDB runtime to access temp views registered in that session
        QueryExecutor executor = new QueryExecutor(sessionManager.getOrCreateSessionForMetadata(sessionId).getRuntime());
        String schemaQuery = "SELECT * FROM (" + sql + ") AS schema_infer LIMIT 0";

        logger.debug("Inferring schema from SQL: {}", schemaQuery);

        try (org.apache.arrow.vector.VectorSchemaRoot result = executor.executeQuery(schemaQuery)) {
            // Extract schema from Arrow VectorSchemaRoot
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = result.getSchema();

            // Convert Arrow schema to thunderduck StructType
            java.util.List<com.thunderduck.types.StructField> fields = new java.util.ArrayList<>();
            for (org.apache.arrow.vector.types.pojo.Field arrowField : arrowSchema.getFields()) {
                com.thunderduck.types.DataType fieldType = convertArrowFieldToDataType(arrowField);
                // In strict mode, normalize column names: spark_sum -> sum, spark_avg -> avg
                String colName = normalizeAggregateColumnName(arrowField.getName());
                fields.add(new com.thunderduck.types.StructField(
                    colName,
                    fieldType,
                    arrowField.isNullable()
                ));
            }

            logger.debug("Inferred schema with {} fields", fields.size());
            return new com.thunderduck.types.StructType(fields);
        }
    }

    /**
     * Merges nullable flags from logical schema onto DuckDB's schema.
     *
     * <p>DuckDB returns all columns as nullable=true. The logical plan has correct nullable
     * info from input schemas (e.g., COUNT is non-nullable, column references inherit from source).
     * This method keeps DuckDB's data types unchanged and only corrects nullable flags.
     *
     * @param duckDBSchema Schema from DuckDB execution (correct types, all nullable)
     * @param logicalSchema Schema from logical plan (correct nullable flags)
     * @return Schema with DuckDB types and logical plan's nullable flags
     */
    private com.thunderduck.types.StructType mergeNullableOnly(
            com.thunderduck.types.StructType duckDBSchema,
            com.thunderduck.types.StructType logicalSchema) {

        if (logicalSchema == null || logicalSchema.size() != duckDBSchema.size()) {
            return duckDBSchema;
        }

        java.util.List<com.thunderduck.types.StructField> fields = new java.util.ArrayList<>();
        for (int i = 0; i < duckDBSchema.size(); i++) {
            com.thunderduck.types.StructField duckField = duckDBSchema.fields().get(i);
            com.thunderduck.types.StructField logicalField = logicalSchema.fields().get(i);

            com.thunderduck.types.DataType effectiveType = duckField.dataType();

            // When Spark's logical plan says DoubleType but DuckDB returns DecimalType,
            // use Spark's DoubleType. This happens when PySpark uses F.lit(100.00) or / 7.0
            // which creates DoubleType in Spark, but DuckDB treats 100.0 as DECIMAL.
            // Spark's type coercion: DOUBLE op DECIMAL -> DOUBLE.
            if (logicalField.dataType() instanceof com.thunderduck.types.DoubleType
                && duckField.dataType() instanceof com.thunderduck.types.DecimalType) {
                effectiveType = com.thunderduck.types.DoubleType.get();
                logger.debug("Column '{}': logical DoubleType overrides DuckDB {}", duckField.name(), duckField.dataType());
            }

            fields.add(new com.thunderduck.types.StructField(
                duckField.name(),
                effectiveType,
                logicalField.nullable()
            ));
        }

        return new com.thunderduck.types.StructType(fields);
    }

    /**
     * Normalizes column names that contain extension function names back to standard SQL names.
     *
     * <p>When strict mode rewrites SUM/AVG to spark_sum/spark_avg, DuckDB names columns
     * using the extension function name (e.g., "spark_sum(l_quantity)"). This normalizes
     * them back to the original names (e.g., "sum(l_quantity)") for Spark compatibility.
     */
    private String normalizeAggregateColumnName(String name) {
        if (name == null) return name;
        return name.replace("spark_sum(", "sum(").replace("spark_avg(", "avg(");
    }

    /**
     * Fixes decimal precision for SUM/AVG results of complex expressions in SQL queries.
     *
     * <p>DuckDB computes intermediate expression types (multiplications) with different
     * precision rules than Spark. When spark_sum operates on a multiplication result,
     * DuckDB's lower intermediate precision propagates into spark_sum's return type.
     *
     * <p>Example: SUM(col1 * (1 - col2))
     * <ul>
     *   <li>Spark: DECIMAL(15,2) * DECIMAL(16,2) = DECIMAL(32,4) -> SUM = DECIMAL(38,4)</li>
     *   <li>DuckDB: DECIMAL(15,2) * DECIMAL(16,2) = DECIMAL(18,4) -> spark_sum = DECIMAL(28,4)</li>
     * </ul>
     *
     * <p>This method detects SUM/AVG columns with complex expressions (containing * or /)
     * and promotes their precision to DECIMAL(38, s) to match Spark behavior.
     *
     * @param sql the original SQL query
     * @param schema the DuckDB schema
     * @return schema with promoted decimal precision where needed
     */
    // Decimal promotion strategies for different expression patterns
    private static final int DECIMAL_NO_CHANGE = 0;           // Extension handles correctly
    private static final int DECIMAL_PROMOTE_PRECISION = 1;    // Promote precision to 38, keep scale
    private static final int DECIMAL_FIX_DIVISION_TYPE = 2;    // Fix both precision and scale for division

    private StructType fixDecimalPrecisionForComplexAggregates(String sql, StructType schema) {
        if (schema == null || schema.size() == 0) return schema;

        int[] promotionStrategy = detectComplexAggregateColumns(sql, schema.size());

        boolean hasChanges = false;
        java.util.List<StructField> fields = new java.util.ArrayList<>();

        for (int i = 0; i < schema.size(); i++) {
            StructField field = schema.fields().get(i);

            if (promotionStrategy[i] != DECIMAL_NO_CHANGE && field.dataType() instanceof com.thunderduck.types.DecimalType) {
                com.thunderduck.types.DecimalType decType = (com.thunderduck.types.DecimalType) field.dataType();

                if (promotionStrategy[i] == DECIMAL_FIX_DIVISION_TYPE) {
                    int targetScale = 6;
                    int targetPrecision = 38;
                    if (decType.precision() != targetPrecision || decType.scale() != targetScale) {
                        StructField promoted = new StructField(
                            field.name(),
                            new com.thunderduck.types.DecimalType(targetPrecision, targetScale),
                            field.nullable()
                        );
                        fields.add(promoted);
                        hasChanges = true;
                        logger.debug("Fixed decimal type for division aggregate '{}': DECIMAL({},{}) -> DECIMAL({},{})",
                            field.name(), decType.precision(), decType.scale(), targetPrecision, targetScale);
                        continue;
                    }
                } else if (promotionStrategy[i] == DECIMAL_PROMOTE_PRECISION && decType.precision() < 38) {
                    StructField promoted = new StructField(
                        field.name(),
                        new com.thunderduck.types.DecimalType(38, decType.scale()),
                        field.nullable()
                    );
                    fields.add(promoted);
                    hasChanges = true;
                    logger.debug("Promoted decimal precision for complex aggregate '{}': DECIMAL({},{}) -> DECIMAL(38,{})",
                        field.name(), decType.precision(), decType.scale(), decType.scale());
                    continue;
                }
            }

            fields.add(field);
        }

        return hasChanges ? new StructType(fields) : schema;
    }

    /**
     * Detects which output columns in a SQL query come from expressions involving decimal
     * arithmetic (SUM/AVG combined with multiplication or division).
     */
    private int[] detectComplexAggregateColumns(String sql, int columnCount) {
        int[] result = new int[columnCount];

        try {
            String effectiveSql = unwrapSelectStar(sql);
            String upperSql = effectiveSql.toUpperCase().trim();

            int selectIdx = findOutermostKeyword(upperSql, "SELECT");
            if (selectIdx < 0) return result;

            int fromIdx = findOutermostKeyword(upperSql.substring(selectIdx + 6), "FROM");
            if (fromIdx < 0) return result;
            fromIdx += selectIdx + 6;

            String selectList = effectiveSql.substring(selectIdx + 6, fromIdx).trim();

            if (selectList.toUpperCase().startsWith("DISTINCT")) {
                selectList = selectList.substring(8).trim();
            }

            java.util.List<String> selectItems = splitSelectList(selectList);
            java.util.Set<String> arithmeticAliases = collectArithmeticAliases(sql);

            for (int i = 0; i < Math.min(selectItems.size(), columnCount); i++) {
                String item = selectItems.get(i).trim();
                String itemUpper = item.toUpperCase();

                boolean hasDivisionAroundAggregate = containsArithmeticWithAggregate(itemUpper);
                boolean hasSumOfMultiplication = containsAggregateWithArithmeticInside(item);
                boolean hasAggregateOfArithmeticAlias = containsAggregateOfArithmeticAlias(item, arithmeticAliases);

                if (hasSumOfMultiplication && hasDivisionAroundAggregate) {
                    result[i] = DECIMAL_FIX_DIVISION_TYPE;
                    continue;
                }

                if (hasSumOfMultiplication) {
                    result[i] = DECIMAL_PROMOTE_PRECISION;
                    continue;
                }

                if (hasDivisionAroundAggregate) {
                    if (hasAggregateOfArithmeticAlias) {
                        result[i] = DECIMAL_FIX_DIVISION_TYPE;
                    } else {
                        result[i] = DECIMAL_NO_CHANGE;
                    }
                    continue;
                }

                if (hasAggregateOfArithmeticAlias) {
                    result[i] = DECIMAL_PROMOTE_PRECISION;
                    continue;
                }

                String alias = extractAliasOrReference(item);
                if (alias != null && arithmeticAliases.contains(alias.toLowerCase())) {
                    result[i] = DECIMAL_PROMOTE_PRECISION;
                    continue;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not detect complex aggregate columns in SQL: {}", e.getMessage());
        }

        return result;
    }

    private boolean containsArithmeticWithAggregate(String itemUpper) {
        boolean hasAggregate = itemUpper.matches("(?s).*\\b(?:SPARK_SUM|SPARK_AVG|SUM|AVG)\\s*\\(.*");
        if (!hasAggregate) return false;

        String stripped = stripAggregateBodies(itemUpper);
        return stripped.contains("*") || stripped.contains("/")
            || stripped.toUpperCase().contains("SPARK_DECIMAL_DIV(");
    }

    private boolean containsAggregateWithArithmeticInside(String item) {
        String upper = item.toUpperCase();
        for (String prefix : new String[]{"SPARK_SUM(", "SPARK_AVG(", "SUM(", "AVG("}) {
            int idx = upper.indexOf(prefix);
            while (idx >= 0) {
                if (idx > 0) {
                    char prev = item.charAt(idx - 1);
                    if (Character.isLetterOrDigit(prev) || prev == '_') {
                        idx = upper.indexOf(prefix, idx + 1);
                        continue;
                    }
                }
                int parenStart = idx + prefix.length() - 1;
                int parenEnd = findMatchingCloseParen(item, parenStart);
                if (parenEnd > 0) {
                    String body = item.substring(parenStart + 1, parenEnd);
                    if (body.contains("*") || body.contains("/")) {
                        return true;
                    }
                }
                idx = upper.indexOf(prefix, idx + 1);
            }
        }
        return false;
    }

    private String stripAggregateBodies(String expr) {
        StringBuilder sb = new StringBuilder();
        String upper = expr.toUpperCase();
        int i = 0;
        while (i < expr.length()) {
            boolean isAgg = false;
            int funcEnd = -1;
            for (String agg : new String[]{"SPARK_SUM(", "SPARK_AVG(", "SUM(", "AVG("}) {
                if (upper.startsWith(agg, i)) {
                    isAgg = true;
                    funcEnd = i + agg.length() - 1;
                    break;
                }
            }
            if (isAgg && funcEnd >= 0) {
                int closeIdx = findMatchingCloseParen(expr, funcEnd);
                if (closeIdx > 0) {
                    sb.append("AGG_RESULT");
                    i = closeIdx + 1;
                    continue;
                }
            }
            sb.append(expr.charAt(i));
            i++;
        }
        return sb.toString();
    }

    private boolean containsAggregateOfArithmeticAlias(String item, java.util.Set<String> arithmeticAliases) {
        if (arithmeticAliases.isEmpty()) return false;

        String upper = item.toUpperCase();
        for (String prefix : new String[]{"SPARK_SUM(", "SPARK_AVG(", "SUM(", "AVG("}) {
            int idx = upper.indexOf(prefix);
            while (idx >= 0) {
                int parenStart = idx + prefix.length() - 1;
                int parenEnd = findMatchingCloseParen(item, parenStart);
                if (parenEnd > 0) {
                    String argStr = item.substring(parenStart + 1, parenEnd).trim();
                    for (String alias : arithmeticAliases) {
                        if (argStr.toLowerCase().contains(alias)) {
                            return true;
                        }
                    }
                }
                idx = upper.indexOf(prefix, idx + 1);
            }
        }
        return false;
    }

    private String extractAliasOrReference(String selectItem) {
        String item = selectItem.trim();
        String upper = item.toUpperCase();

        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int lastAsPos = -1;
        for (int i = 0; i < item.length() - 2; i++) {
            char c = item.charAt(i);
            if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; continue; }
            if (c == '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; continue; }
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && upper.startsWith("AS ", i)
                         && (i == 0 || !Character.isLetterOrDigit(item.charAt(i - 1)))) {
                    lastAsPos = i;
                }
            }
        }

        if (lastAsPos >= 0) {
            String alias = item.substring(lastAsPos + 3).trim();
            if (alias.startsWith("\"") && alias.endsWith("\"")) {
                alias = alias.substring(1, alias.length() - 1);
            }
            return alias;
        }

        if (item.matches("(?i)[a-z_][a-z0-9_]*")) {
            return item;
        }

        return null;
    }

    private java.util.Set<String> collectArithmeticAliases(String sql) {
        java.util.Set<String> aliases = new java.util.HashSet<>();

        java.util.regex.Pattern aliasPattern = java.util.regex.Pattern.compile(
            "(?i)\\s+AS\\s+(?:\"([^\"]+)\"|([a-z_][a-z0-9_]*))",
            java.util.regex.Pattern.CASE_INSENSITIVE
        );

        java.util.regex.Matcher aliasMatcher = aliasPattern.matcher(sql);
        while (aliasMatcher.find()) {
            int asStart = aliasMatcher.start();
            String beforeAs = sql.substring(0, asStart).trim();

            int exprStart = findExpressionStart(beforeAs);
            if (exprStart < 0) continue;

            String expr = beforeAs.substring(exprStart).trim();

            if (expr.matches("(?s).*[\\w)]\\s*[*/]\\s*[\\w(].*")) {
                String alias = aliasMatcher.group(1) != null ? aliasMatcher.group(1).toLowerCase()
                    : aliasMatcher.group(2).toLowerCase();
                aliases.add(alias);
                logger.trace("Found arithmetic alias '{}' from expression: {}", alias, expr);
            }
        }

        return aliases;
    }

    private int findExpressionStart(String sql) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = sql.length() - 1; i >= 0; i--) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; continue; }
            if (c == '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; continue; }
            if (inSingleQuote || inDoubleQuote) continue;

            if (c == ')') depth++;
            else if (c == '(') depth--;

            if (depth == 0 && c == ',') {
                return i + 1;
            }

            if (depth == 0 && i >= 6) {
                String possibleKeyword = sql.substring(i - 6, i + 1).toUpperCase();
                if (possibleKeyword.endsWith("SELECT") &&
                    (i - 6 == 0 || !Character.isLetterOrDigit(sql.charAt(i - 7)))) {
                    return i + 1;
                }
            }
        }

        return 0;
    }

    /**
     * Rewrites SQL to add explicit DECIMAL(38,6) CASTs around division expressions
     * involving aggregates of multiplication.
     */
    private String fixDivisionDecimalCasts(String sql) {
        try {
            String wrapperPrefix = findSelectStarWrapperPrefix(sql);
            if (wrapperPrefix != null) {
                return fixDivisionDecimalCastsInWrapped(sql, wrapperPrefix);
            }

            return fixDivisionDecimalCastsCore(sql);
        } catch (Exception e) {
            logger.debug("Could not apply division decimal casts: {}", e.getMessage());
            return sql;
        }
    }

    private String findSelectStarWrapperPrefix(String sql) {
        String trimmed = sql.trim();
        String upper = trimmed.toUpperCase();

        if (!upper.startsWith("SELECT")) return null;
        int idx = 6;
        while (idx < upper.length() && Character.isWhitespace(upper.charAt(idx))) idx++;

        if (upper.startsWith("DISTINCT", idx)) {
            idx += 8;
            while (idx < upper.length() && Character.isWhitespace(upper.charAt(idx))) idx++;
        }

        if (idx >= upper.length() || upper.charAt(idx) != '*') return null;
        int afterStar = idx + 1;
        while (afterStar < upper.length() && Character.isWhitespace(upper.charAt(afterStar))) afterStar++;

        if (!upper.startsWith("FROM", afterStar)) return null;
        int afterFrom = afterStar + 4;
        while (afterFrom < upper.length() && Character.isWhitespace(upper.charAt(afterFrom))) afterFrom++;

        if (afterFrom >= trimmed.length() || trimmed.charAt(afterFrom) != '(') return null;

        return trimmed.substring(0, afterFrom + 1);
    }

    private String fixDivisionDecimalCastsInWrapped(String sql, String prefix) {
        String trimmed = sql.trim();
        int openParen = prefix.length() - 1;
        int closeParen = findMatchingCloseParen(trimmed, openParen);
        if (closeParen < 0) return sql;

        String inner = trimmed.substring(openParen + 1, closeParen);
        String suffix = trimmed.substring(closeParen);

        String fixedInner = fixDivisionDecimalCasts(inner);

        if (fixedInner.equals(inner)) return sql;
        return prefix.substring(0, prefix.length() - 1) + "(" + fixedInner + suffix;
    }

    private String fixDivisionDecimalCastsCore(String sql) {
        String upperSql = sql.toUpperCase().trim();

        int selectIdx = findOutermostKeyword(upperSql, "SELECT");
        if (selectIdx < 0) return sql;

        int fromIdx = findOutermostKeyword(upperSql.substring(selectIdx + 6), "FROM");
        if (fromIdx < 0) return sql;
        fromIdx += selectIdx + 6;

        String selectList = sql.substring(selectIdx + 6, fromIdx).trim();
        String beforeSelect = sql.substring(0, selectIdx + 6);
        String afterFrom = sql.substring(fromIdx);

        String distinctPrefix = "";
        if (selectList.toUpperCase().startsWith("DISTINCT")) {
            distinctPrefix = selectList.substring(0, 8) + " ";
            selectList = selectList.substring(8).trim();
        }

        java.util.List<String> selectItems = splitSelectList(selectList);
        java.util.Set<String> arithmeticAliases = collectArithmeticAliases(sql);

        boolean modified = false;
        java.util.List<String> newItems = new java.util.ArrayList<>();

        for (String item : selectItems) {
            String trimmed = item.trim();
            String trimmedUpper = trimmed.toUpperCase();

            boolean hasDivision = containsArithmeticWithAggregate(trimmedUpper);
            if (!hasDivision) {
                newItems.add(item);
                continue;
            }

            boolean hasMultiplicationInput = containsAggregateOfArithmeticAlias(trimmed, arithmeticAliases)
                || containsAggregateWithArithmeticInside(trimmed);

            if (!hasMultiplicationInput) {
                newItems.add(item);
                continue;
            }

            String expr = trimmed;
            String aliasSuffix = "";

            int asPos = findLastTopLevelAs(trimmed);
            if (asPos >= 0) {
                expr = trimmed.substring(0, asPos).trim();
                aliasSuffix = " " + trimmed.substring(asPos);
            }

            String castExpr = "CAST(" + expr + " AS DECIMAL(38,6))" + aliasSuffix;
            newItems.add(castExpr);
            modified = true;
            logger.debug("Added DECIMAL(38,6) cast for division expression: {}", trimmed);
        }

        if (!modified) return sql;

        StringBuilder sb = new StringBuilder();
        sb.append(beforeSelect).append(" ").append(distinctPrefix);
        for (int i = 0; i < newItems.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(newItems.get(i));
        }
        sb.append(" ").append(afterFrom);

        return sb.toString();
    }

    private String unwrapSelectStar(String sql) {
        String current = sql.trim();
        int maxIterations = 20;

        for (int iter = 0; iter < maxIterations; iter++) {
            String upper = current.toUpperCase().trim();

            if (!upper.startsWith("SELECT")) return current;

            int idx = 6;
            while (idx < upper.length() && Character.isWhitespace(upper.charAt(idx))) idx++;

            if (upper.startsWith("DISTINCT", idx)) {
                idx += 8;
                while (idx < upper.length() && Character.isWhitespace(upper.charAt(idx))) idx++;
            }

            if (idx >= upper.length() || upper.charAt(idx) != '*') {
                return current;
            }

            int afterStar = idx + 1;
            while (afterStar < upper.length() && Character.isWhitespace(upper.charAt(afterStar))) afterStar++;

            if (afterStar >= upper.length() || !upper.startsWith("FROM", afterStar)) {
                return current;
            }

            int afterFrom = afterStar + 4;
            while (afterFrom < upper.length() && Character.isWhitespace(upper.charAt(afterFrom))) afterFrom++;

            if (afterFrom >= current.length() || current.charAt(afterFrom) != '(') {
                return current;
            }

            int closeParen = findMatchingCloseParen(current, afterFrom);
            if (closeParen < 0) return current;

            String inner = current.substring(afterFrom + 1, closeParen).trim();
            current = inner;
        }

        return current;
    }

    private int findLastTopLevelAs(String item) {
        String upper = item.toUpperCase();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int lastAsPos = -1;

        for (int i = 0; i < item.length() - 2; i++) {
            char c = item.charAt(i);
            if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; continue; }
            if (c == '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; continue; }
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && upper.startsWith("AS ", i)
                         && (i == 0 || !Character.isLetterOrDigit(item.charAt(i - 1)))) {
                    lastAsPos = i;
                }
            }
        }
        return lastAsPos;
    }

    /**
     * Fixes DECIMAL-to-DOUBLE type mismatches at the SQL level.
     *
     * <p>When PySpark uses F.lit(100.00) or divides by 7.0, the logical schema expects
     * DOUBLE because Spark's type coercion says DOUBLE op DECIMAL -> DOUBLE. But DuckDB
     * treats numeric literals like 100.0 as DECIMAL, so the SQL result is DECIMAL.
     *
     * <p>This method detects such mismatches by comparing the logical schema against what
     * DuckDB would return, and wraps the affected SELECT expressions with CAST(... AS DOUBLE).
     * This moves the conversion into DuckDB's SQL engine where it's efficient, instead of
     * doing row-by-row Arrow vector conversion in Java.
     *
     * @param sql the SQL query (after preprocessing)
     * @param logicalSchema the logical plan schema with expected types
     * @param sessionId the session ID for DuckDB schema inference
     * @return modified SQL with DOUBLE casts where needed, or original SQL if no changes needed
     */
    private String fixDecimalToDoubleCasts(String sql, StructType logicalSchema, String sessionId) {
        try {
            // Infer what DuckDB would actually return
            StructType duckDBSchema = inferSchemaFromDuckDB(sql, sessionId);
            if (duckDBSchema == null || duckDBSchema.size() == 0) return sql;
            if (logicalSchema.size() != duckDBSchema.size()) return sql;

            // Find columns where logical schema says DOUBLE but DuckDB returns DECIMAL
            java.util.List<Integer> doubleCastColumns = new java.util.ArrayList<>();
            for (int i = 0; i < logicalSchema.size(); i++) {
                StructField logicalField = logicalSchema.fields().get(i);
                StructField duckField = duckDBSchema.fields().get(i);
                if (logicalField.dataType() instanceof com.thunderduck.types.DoubleType
                    && duckField.dataType() instanceof com.thunderduck.types.DecimalType) {
                    doubleCastColumns.add(i);
                    logger.debug("Column {} '{}' needs DECIMAL->DOUBLE SQL cast (logical=DoubleType, DuckDB={})",
                        i, logicalField.name(), duckField.dataType());
                }
            }

            if (doubleCastColumns.isEmpty()) return sql;

            // Apply CAST(... AS DOUBLE) to the affected columns
            return applyDoubleCastsToSelectItems(sql, doubleCastColumns);
        } catch (Exception e) {
            logger.debug("Could not apply DECIMAL->DOUBLE SQL casts: {}", e.getMessage());
            return sql;
        }
    }

    /**
     * Wraps specific SELECT items with CAST(... AS DOUBLE) based on column indices.
     * Handles wrapped SELECT * FROM (...) patterns by operating on the inner query.
     */
    private String applyDoubleCastsToSelectItems(String sql, java.util.List<Integer> columnIndices) {
        // Handle SELECT * FROM (...) wrappers
        String wrapperPrefix = findSelectStarWrapperPrefix(sql);
        if (wrapperPrefix != null) {
            String trimmed = sql.trim();
            int openParen = wrapperPrefix.length() - 1;
            int closeParen = findMatchingCloseParen(trimmed, openParen);
            if (closeParen < 0) return sql;

            String inner = trimmed.substring(openParen + 1, closeParen);
            String suffix = trimmed.substring(closeParen);

            String fixedInner = applyDoubleCastsToSelectItems(inner, columnIndices);
            if (fixedInner.equals(inner)) return sql;
            return wrapperPrefix.substring(0, wrapperPrefix.length() - 1) + "(" + fixedInner + suffix;
        }

        // Find the outermost SELECT list
        String upperSql = sql.toUpperCase().trim();
        int selectIdx = findOutermostKeyword(upperSql, "SELECT");
        if (selectIdx < 0) return sql;

        int fromIdx = findOutermostKeyword(upperSql.substring(selectIdx + 6), "FROM");
        if (fromIdx < 0) return sql;
        fromIdx += selectIdx + 6;

        String selectList = sql.substring(selectIdx + 6, fromIdx).trim();
        String beforeSelect = sql.substring(0, selectIdx + 6);
        String afterFrom = sql.substring(fromIdx);

        String distinctPrefix = "";
        if (selectList.toUpperCase().startsWith("DISTINCT")) {
            distinctPrefix = selectList.substring(0, 8) + " ";
            selectList = selectList.substring(8).trim();
        }

        java.util.List<String> selectItems = splitSelectList(selectList);

        boolean modified = false;
        java.util.List<String> newItems = new java.util.ArrayList<>();

        for (int i = 0; i < selectItems.size(); i++) {
            if (columnIndices.contains(i)) {
                String item = selectItems.get(i).trim();

                // Separate expression from alias
                String expr = item;
                String aliasSuffix = "";
                int asPos = findLastTopLevelAs(item);
                if (asPos >= 0) {
                    expr = item.substring(0, asPos).trim();
                    aliasSuffix = " " + item.substring(asPos);
                }

                String castExpr = "CAST(" + expr + " AS DOUBLE)" + aliasSuffix;
                newItems.add(castExpr);
                modified = true;
                logger.debug("Added DOUBLE cast for column {}: {}", i, item);
            } else {
                newItems.add(selectItems.get(i));
            }
        }

        if (!modified) return sql;

        StringBuilder sb = new StringBuilder();
        sb.append(beforeSelect).append(" ").append(distinctPrefix);
        for (int i = 0; i < newItems.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(newItems.get(i));
        }
        sb.append(" ").append(afterFrom);

        return sb.toString();
    }

    /**
     * Infers a schema with corrected nullable flags for raw SQL queries.
     */
    private StructType inferNullableSchemaFromSQL(String sql, String sessionId) throws Exception {
        StructType duckDBSchema = inferSchemaFromDuckDB(sql, sessionId);
        if (duckDBSchema == null || duckDBSchema.size() == 0) {
            return null;
        }

        StructType result = fixCountNullable(sql, duckDBSchema);
        result = fixDecimalPrecisionForComplexAggregates(sql, result);
        return result;
    }

    /**
     * Fixes nullable flags for COUNT columns in a schema based on SQL analysis.
     */
    private StructType fixCountNullable(String sql, StructType schema) {
        boolean[] isCountColumn = detectCountColumns(sql, schema.size());

        boolean hasCorrections = false;
        java.util.List<StructField> fields = new java.util.ArrayList<>();
        for (int i = 0; i < schema.size(); i++) {
            StructField field = schema.fields().get(i);
            if (isCountColumn[i] && field.nullable()) {
                fields.add(new StructField(field.name(), field.dataType(), false));
                hasCorrections = true;
            } else {
                fields.add(field);
            }
        }

        return hasCorrections ? new StructType(fields) : schema;
    }

    private boolean[] detectCountColumns(String sql, int columnCount) {
        boolean[] result = new boolean[columnCount];

        try {
            java.util.Set<String> countAliases = collectCountAliases(sql);

            String upperSql = sql.toUpperCase().trim();

            int selectIdx = findOutermostKeyword(upperSql, "SELECT");
            if (selectIdx < 0) return result;

            int fromIdx = findOutermostKeyword(upperSql.substring(selectIdx + 6), "FROM");
            if (fromIdx < 0) return result;
            fromIdx += selectIdx + 6;

            String selectList = sql.substring(selectIdx + 6, fromIdx).trim();

            if (selectList.toUpperCase().startsWith("DISTINCT")) {
                selectList = selectList.substring(8).trim();
            }

            java.util.List<String> selectItems = splitSelectList(selectList);

            for (int i = 0; i < Math.min(selectItems.size(), columnCount); i++) {
                String item = selectItems.get(i).trim();
                String itemUpper = item.toUpperCase();

                if (itemUpper.matches(".*\\bCOUNT\\s*\\(.*")) {
                    result[i] = true;
                    continue;
                }

                String colName = extractColumnName(item);
                if (colName != null && countAliases.contains(colName.toUpperCase())) {
                    result[i] = true;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not detect COUNT columns in SQL: {}", e.getMessage());
        }

        return result;
    }

    private java.util.Set<String> collectCountAliases(String sql) {
        java.util.Set<String> aliases = new java.util.HashSet<>();

        java.util.regex.Pattern countAsPattern = java.util.regex.Pattern.compile(
            "(?i)\\bcount\\s*\\([^)]*\\)\\s+(?:as\\s+)?([\"']?\\w+[\"']?)");
        java.util.regex.Matcher matcher = countAsPattern.matcher(sql);
        while (matcher.find()) {
            String alias = matcher.group(1).replaceAll("[\"']", "").trim();
            if (!alias.equalsIgnoreCase("FROM") && !alias.equalsIgnoreCase("WHERE") &&
                !alias.equalsIgnoreCase("GROUP") && !alias.equalsIgnoreCase("ORDER") &&
                !alias.equalsIgnoreCase("HAVING") && !alias.equalsIgnoreCase("LIMIT") &&
                !alias.equalsIgnoreCase("AND") && !alias.equalsIgnoreCase("OR")) {
                aliases.add(alias.toUpperCase());
            }
        }

        detectRenamedCountColumns(sql, aliases);

        return aliases;
    }

    private void detectRenamedCountColumns(String sql, java.util.Set<String> aliases) {
        java.util.regex.Pattern renamePattern = java.util.regex.Pattern.compile(
            "(?i)\\)\\s+AS\\s+\\w+\\s*\\(([^)]+)\\)");
        java.util.regex.Matcher renameMatcher = renamePattern.matcher(sql);

        while (renameMatcher.find()) {
            String columnList = renameMatcher.group(1).trim();
            String[] columns = columnList.split("\\s*,\\s*");

            int subqueryEnd = renameMatcher.start();

            int subqueryStart = findMatchingOpenParen(sql, subqueryEnd);
            if (subqueryStart < 0) continue;

            String subquery = sql.substring(subqueryStart + 1, subqueryEnd);

            String subqueryUpper = subquery.toUpperCase().trim();
            int selectIdx = findOutermostKeyword(subqueryUpper, "SELECT");
            if (selectIdx < 0) continue;

            int fromIdx = findOutermostKeyword(subqueryUpper.substring(selectIdx + 6), "FROM");
            if (fromIdx < 0) continue;
            fromIdx += selectIdx + 6;

            String selectList = subquery.substring(selectIdx + 6, fromIdx).trim();
            java.util.List<String> selectItems = splitSelectList(selectList);

            for (int i = 0; i < Math.min(selectItems.size(), columns.length); i++) {
                String item = selectItems.get(i).trim().toUpperCase();
                if (item.matches(".*\\bCOUNT\\s*\\(.*")) {
                    String renamedCol = columns[i].trim().replaceAll("[\"']", "");
                    aliases.add(renamedCol.toUpperCase());
                }
            }
        }
    }

    private int findMatchingOpenParen(String sql, int closeParenPos) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = closeParenPos; i >= 0; i--) {
            char c = sql.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (!inSingleQuote && !inDoubleQuote) {
                if (c == ')') depth++;
                else if (c == '(') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
        }
        return -1;
    }

    private int findMatchingCloseParen(String sql, int openParenPos) {
        int depth = 1;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = openParenPos + 1; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
        }
        return -1;
    }

    private String extractColumnName(String selectItem) {
        String item = selectItem.trim();

        if (item.matches("(?i)[a-z_][a-z0-9_]*")) {
            return item;
        }

        return null;
    }

    private int findOutermostKeyword(String sql, String keyword) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        String upperSql = sql.toUpperCase();

        for (int i = 0; i <= upperSql.length() - keyword.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                continue;
            }
            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && upperSql.startsWith(keyword, i)) {
                    if (i > 0) {
                        char prev = sql.charAt(i - 1);
                        if (Character.isLetterOrDigit(prev) || prev == '_') continue;
                    }
                    int end = i + keyword.length();
                    if (end < sql.length()) {
                        char next = sql.charAt(end);
                        if (Character.isLetterOrDigit(next) || next == '_') continue;
                    }
                    return i;
                }
            }
        }
        return -1;
    }

    private java.util.List<String> splitSelectList(String selectList) {
        java.util.List<String> items = new java.util.ArrayList<>();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int start = 0;

        for (int i = 0; i < selectList.length(); i++) {
            char c = selectList.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (c == ',' && depth == 0) {
                    items.add(selectList.substring(start, i));
                    start = i + 1;
                }
            }
        }
        items.add(selectList.substring(start));

        return items;
    }

    /**
     * Converts Arrow Field to thunderduck DataType.
     * Handles complex types (List, Map, Struct) by recursively converting child fields.
     */
    private com.thunderduck.types.DataType convertArrowFieldToDataType(org.apache.arrow.vector.types.pojo.Field arrowField) {
        org.apache.arrow.vector.types.pojo.ArrowType arrowType = arrowField.getType();
        java.util.List<org.apache.arrow.vector.types.pojo.Field> children = arrowField.getChildren();

        // Handle complex types first (they need access to children)
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.List) {
            // Arrow List has one child field (the element type)
            if (children != null && !children.isEmpty()) {
                org.apache.arrow.vector.types.pojo.Field elementField = children.get(0);
                com.thunderduck.types.DataType elementType = convertArrowFieldToDataType(elementField);
                // Use containsNull=false to match Spark's collect_list/collect_set behavior
                // DuckDB marks list elements as nullable, but Spark uses non-nullable
                boolean containsNull = false;
                return new com.thunderduck.types.ArrayType(elementType, containsNull);
            }
            // Fallback for empty list - default to string element
            return new com.thunderduck.types.ArrayType(com.thunderduck.types.StringType.get(), false);
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Map) {
            // Arrow Map has one child field (entries struct with key and value fields)
            if (children != null && !children.isEmpty()) {
                org.apache.arrow.vector.types.pojo.Field entriesField = children.get(0);
                java.util.List<org.apache.arrow.vector.types.pojo.Field> entryChildren = entriesField.getChildren();
                if (entryChildren != null && entryChildren.size() >= 2) {
                    com.thunderduck.types.DataType keyType = convertArrowFieldToDataType(entryChildren.get(0));
                    com.thunderduck.types.DataType valueType = convertArrowFieldToDataType(entryChildren.get(1));
                    boolean valueContainsNull = entryChildren.get(1).isNullable();
                    return new com.thunderduck.types.MapType(keyType, valueType, valueContainsNull);
                }
            }
            // Fallback
            return new com.thunderduck.types.MapType(
                com.thunderduck.types.StringType.get(),
                com.thunderduck.types.StringType.get(),
                true);
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
            // Arrow Struct has child fields for each struct field
            java.util.List<com.thunderduck.types.StructField> structFields = new java.util.ArrayList<>();
            if (children != null) {
                for (org.apache.arrow.vector.types.pojo.Field child : children) {
                    com.thunderduck.types.DataType childType = convertArrowFieldToDataType(child);
                    structFields.add(new com.thunderduck.types.StructField(
                        child.getName(),
                        childType,
                        child.isNullable()
                    ));
                }
            }
            return new com.thunderduck.types.StructType(structFields);
        }

        // Handle primitive types (no children needed)
        if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
            org.apache.arrow.vector.types.pojo.ArrowType.Int intType =
                (org.apache.arrow.vector.types.pojo.ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 32) {
                return com.thunderduck.types.IntegerType.get();
            } else if (intType.getBitWidth() == 64) {
                return com.thunderduck.types.LongType.get();
            } else if (intType.getBitWidth() == 16) {
                return com.thunderduck.types.ShortType.get();
            } else if (intType.getBitWidth() == 8) {
                return com.thunderduck.types.ByteType.get();
            }
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
            org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fpType =
                (org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) arrowType;
            if (fpType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE) {
                return com.thunderduck.types.DoubleType.get();
            } else if (fpType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                return com.thunderduck.types.FloatType.get();
            }
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8 ||
                   arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.LargeUtf8) {
            return com.thunderduck.types.StringType.get();
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) {
            return com.thunderduck.types.BooleanType.get();
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
            return com.thunderduck.types.DateType.get();
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            return com.thunderduck.types.TimestampType.get();
        } else if (arrowType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Decimal) {
            org.apache.arrow.vector.types.pojo.ArrowType.Decimal decimalType =
                (org.apache.arrow.vector.types.pojo.ArrowType.Decimal) arrowType;
            return new com.thunderduck.types.DecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        // Default to string for unknown types
        logger.warn("Unknown Arrow type: {}, defaulting to STRING", arrowType.getClass().getSimpleName());
        return com.thunderduck.types.StringType.get();
    }

    /**
     * Execute a WriteOperation command (df.write.parquet(), df.write.csv(), etc.).
     *
     * Generates a DuckDB COPY statement based on the WriteOperation parameters.
     * Supports:
     * - Formats: parquet, csv, json (via source parameter)
     * - Modes: OVERWRITE, ERROR_IF_EXISTS, IGNORE, APPEND
     * - Partitioning: via partitioning_columns
     * - Compression and other options via options map
     *
     * @param writeOp The WriteOperation proto message
     * @param session Session object
     * @param responseObserver Stream observer for responses
     */
    private void executeWriteOperation(WriteOperation writeOp, Session session,
                                       StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            // 1. Validate required fields
            if (!writeOp.hasInput()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("WriteOperation requires an input relation")
                    .asRuntimeException());
                return;
            }

            // 2. Get output path (only path writes supported for now)
            String outputPath;
            if (writeOp.hasPath()) {
                outputPath = writeOp.getPath();
            } else if (writeOp.hasTable()) {
                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("WriteOperation to table is not yet supported. Use df.write.parquet('/path') instead.")
                    .asRuntimeException());
                return;
            } else {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("WriteOperation requires a path or table destination")
                    .asRuntimeException());
                return;
            }

            // 3. Determine format (default to parquet)
            String format = writeOp.hasSource() ? writeOp.getSource().toLowerCase() : "parquet";
            if (!format.equals("parquet") && !format.equals("csv") && !format.equals("json")) {
                responseObserver.onError(Status.UNIMPLEMENTED
                    .withDescription("Unsupported write format: " + format + ". Supported formats: parquet, csv, json")
                    .asRuntimeException());
                return;
            }

            // 4. Handle SaveMode
            WriteOperation.SaveMode mode = writeOp.getMode();
            boolean fileExists = checkFileExists(outputPath);

            switch (mode) {
                case SAVE_MODE_ERROR_IF_EXISTS:
                    if (fileExists) {
                        responseObserver.onError(Status.ALREADY_EXISTS
                            .withDescription("Path already exists: " + outputPath)
                            .asRuntimeException());
                        return;
                    }
                    break;
                case SAVE_MODE_IGNORE:
                    if (fileExists) {
                        // Return success without writing
                        logger.info("WriteOperation IGNORE mode: path exists, skipping write: {}", outputPath);
                        sendWriteSuccessResponse(session.getSessionId(), responseObserver);
                        return;
                    }
                    break;
                case SAVE_MODE_APPEND:
                    // APPEND requires special handling - read existing + union + write
                    if (fileExists && format.equals("parquet")) {
                        executeAppendWrite(writeOp, outputPath, session, responseObserver);
                        return;
                    }
                    // If file doesn't exist, fall through to normal write
                    break;
                case SAVE_MODE_OVERWRITE:
                case SAVE_MODE_UNSPECIFIED:
                default:
                    // OVERWRITE is DuckDB's default behavior - just write
                    break;
            }

            // 5. Convert input relation to SQL
            LogicalPlan inputPlan = createPlanConverter(session).convertRelation(writeOp.getInput());
            String inputSQL = sqlGenerator.generate(inputPlan);
            logger.debug("WriteOperation input SQL: {}", inputSQL);

            // 6. Build COPY statement
            StringBuilder copySQL = new StringBuilder();
            copySQL.append("COPY (").append(inputSQL).append(") TO ");
            copySQL.append(quoteFilePath(outputPath));
            copySQL.append(" (FORMAT ").append(format.toUpperCase());

            // Add compression if specified in options
            Map<String, String> options = writeOp.getOptionsMap();
            if (options.containsKey("compression")) {
                copySQL.append(", COMPRESSION ").append(options.get("compression").toUpperCase());
            } else if (format.equals("parquet")) {
                copySQL.append(", COMPRESSION SNAPPY"); // Default for parquet
            }

            // Add partitioning if specified
            if (writeOp.getPartitioningColumnsCount() > 0) {
                copySQL.append(", PARTITION_BY (");
                for (int i = 0; i < writeOp.getPartitioningColumnsCount(); i++) {
                    if (i > 0) copySQL.append(", ");
                    copySQL.append(quoteIdentifier(writeOp.getPartitioningColumns(i)));
                }
                copySQL.append(")");
            }

            // Add CSV-specific options
            if (format.equals("csv")) {
                if (options.containsKey("header")) {
                    copySQL.append(", HEADER ").append(options.get("header").toLowerCase().equals("true"));
                } else {
                    copySQL.append(", HEADER true"); // Default to header for CSV
                }
                if (options.containsKey("delimiter") || options.containsKey("sep")) {
                    String delimiter = options.getOrDefault("delimiter", options.get("sep"));
                    copySQL.append(", DELIMITER '").append(delimiter).append("'");
                }
            }

            copySQL.append(")");

            logger.info("Executing WriteOperation: {}", copySQL);

            // 7. Execute the COPY statement
            QueryExecutor executor = new QueryExecutor(session.getRuntime());
            executor.execute(copySQL.toString());

            // 8. Return success response
            sendWriteSuccessResponse(session.getSessionId(), responseObserver);
            logger.info("✓ WriteOperation completed: {} rows written to {}", "unknown", outputPath);

        } catch (Exception e) {
            logger.error("WriteOperation failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("WriteOperation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Check if a file or directory exists.
     * Supports local paths and potentially cloud paths if extensions loaded.
     */
    private boolean checkFileExists(String path) {
        try {
            java.io.File file = new java.io.File(path);
            return file.exists();
        } catch (Exception e) {
            // For cloud paths, assume doesn't exist (let DuckDB handle errors)
            return false;
        }
    }

    /**
     * Execute an APPEND write by reading existing data, unioning with new data, and writing back.
     */
    private void executeAppendWrite(WriteOperation writeOp, String outputPath, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            // Convert input relation to SQL
            LogicalPlan inputPlan = createPlanConverter(session).convertRelation(writeOp.getInput());
            String newDataSQL = sqlGenerator.generate(inputPlan);

            // Build union query: existing + new
            String unionSQL = String.format(
                "SELECT * FROM read_parquet(%s) UNION ALL %s",
                quoteFilePath(outputPath),
                newDataSQL
            );

            // Build COPY statement
            StringBuilder copySQL = new StringBuilder();
            copySQL.append("COPY (").append(unionSQL).append(") TO ");
            copySQL.append(quoteFilePath(outputPath));
            copySQL.append(" (FORMAT PARQUET, COMPRESSION SNAPPY)");

            logger.info("Executing WriteOperation APPEND: {}", copySQL);

            // Execute
            QueryExecutor executor = new QueryExecutor(session.getRuntime());
            executor.execute(copySQL.toString());

            sendWriteSuccessResponse(session.getSessionId(), responseObserver);
            logger.info("✓ WriteOperation APPEND completed to {}", outputPath);

        } catch (Exception e) {
            logger.error("WriteOperation APPEND failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("WriteOperation APPEND failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Send a success response for WriteOperation.
     */
    private void sendWriteSuccessResponse(String sessionId,
                                          StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
