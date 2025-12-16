package com.thunderduck.connect.service;

import com.thunderduck.connect.converter.PlanConverter;
import com.thunderduck.connect.session.Session;
import com.thunderduck.connect.session.SessionManager;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.runtime.ArrowBatchIterator;
import com.thunderduck.runtime.ArrowStreamingExecutor;
import com.thunderduck.runtime.QueryExecutor;
import com.thunderduck.runtime.TailBatchIterator;
import com.thunderduck.logical.Tail;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.spark.connect.proto.*;
import org.apache.spark.connect.proto.Relation;
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

        logger.info("SparkConnectServiceImpl initialized with session-scoped DuckDB runtimes");
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
                        sql = input.getSql().getQuery();
                        logger.debug("Found SQL in ShowString.input: {}", sql);
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
                    // Direct SQL relation
                    sql = root.getSql().getQuery();
                    logger.debug("Found direct SQL relation: {}", sql);
                } else if (root.hasCatalog()) {
                    // Handle catalog operations (dropTempView, etc.)
                    executeCatalogOperation(root.getCatalog(), session, responseObserver);
                    return;
                }
            } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                // SQL command (e.g., spark.sql(...))
                SqlCommand sqlCommand = plan.getCommand().getSqlCommand();

                // In Spark 4.0.1, the 'sql' field is deprecated and replaced with 'input' relation
                if (sqlCommand.hasInput() && sqlCommand.getInput().hasSql()) {
                    sql = sqlCommand.getInput().getSql().getQuery();
                    logger.debug("Found SQL command (from input relation): {}", sql);
                } else if (!sqlCommand.getSql().isEmpty()) {
                    // Fallback for older clients or backward compatibility
                    sql = sqlCommand.getSql();
                    logger.debug("Found SQL command (legacy): {}", sql);
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
                    // Convert Protobuf plan to LogicalPlan
                    LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);

                    // Generate SQL from LogicalPlan
                    String generatedSQL = sqlGenerator.generate(logicalPlan);
                    logger.info("Generated SQL from plan: {}", generatedSQL);

                    // Check if this is a Tail plan - use TailBatchIterator wrapper
                    if (logicalPlan instanceof Tail) {
                        Tail tailPlan = (Tail) logicalPlan;
                        executeSQLStreaming(generatedSQL, session, responseObserver, (int) tailPlan.limit());
                    } else {
                        // Execute the generated SQL
                        executeSQL(generatedSQL, session, responseObserver);
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
     * Execute a Catalog operation (dropTempView, tableExists, etc.).
     *
     * Currently handles:
     * - DropTempView (spark.catalog.dropTempView)
     *
     * @param catalog The catalog operation to execute
     * @param session Session object
     * @param responseObserver Stream observer for responses
     */
    private void executeCatalogOperation(Catalog catalog, Session session,
                                        StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            logger.debug("Handling CATALOG operation: {}", catalog.getCatTypeCase());

            switch (catalog.getCatTypeCase()) {
                case DROP_TEMP_VIEW:
                    DropTempView dropTempView = catalog.getDropTempView();
                    String viewName = dropTempView.getViewName();
                    logger.info("Dropping temp view: '{}'", viewName);

                    // Remove from session's temp view registry
                    boolean existed = session.dropTempView(viewName);

                    // Drop the view from DuckDB (we use non-temp views for cross-connection visibility)
                    String dropViewSQL = String.format("DROP VIEW IF EXISTS %s", quoteIdentifier(viewName));
                    logger.debug("Executing DuckDB: {}", dropViewSQL);

                    QueryExecutor executor = new QueryExecutor(session.getRuntime());
                    try {
                        executor.execute(dropViewSQL);
                    } catch (Exception e) {
                        logger.error("Failed to drop DuckDB view", e);
                        responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to drop DuckDB view: " + e.getMessage())
                            .asRuntimeException());
                        return;
                    }

                    // Return success response with the result (true if existed, false otherwise)
                    // The response for dropTempView should be an Arrow batch with boolean
                    String operationId = java.util.UUID.randomUUID().toString();
                    StreamingResultHandler resultHandler = new StreamingResultHandler(
                        responseObserver, session.getSessionId(), operationId);

                    try {
                        resultHandler.streamBooleanResult(existed);
                        logger.info("✓ View dropped from DuckDB: '{}' (existed={}, session: {})",
                            viewName, existed, session.getSessionId());
                    } catch (java.io.IOException e) {
                        logger.error("Failed to send dropTempView result", e);
                        responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to send result: " + e.getMessage())
                            .asRuntimeException());
                    }
                    break;

                default:
                    responseObserver.onError(Status.UNIMPLEMENTED
                        .withDescription("Unsupported catalog operation: " + catalog.getCatTypeCase())
                        .asRuntimeException());
            }

        } catch (Exception e) {
            logger.error("Catalog operation failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Catalog operation failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
        }
    }

    /**
     * Analyze a plan and return metadata (schema, execution plan, etc.).
     *
     * @param request AnalyzePlanRequest
     * @param responseObserver Response observer
     */
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

                    } else {
                        // Regular plan - deserialize and extract schema
                        LogicalPlan logicalPlan = createPlanConverter(session).convert(plan);
                        schema = logicalPlan.schema();

                        if (schema == null || schema.size() == 0) {
                            // Infer schema from generated SQL when plan doesn't provide schema
                            // (e.g., SQLRelation returns empty schema since it can't infer without DuckDB)
                            sql = sqlGenerator.generate(logicalPlan);
                            schema = inferSchemaFromDuckDB(sql, sessionId);
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

        // Fix integer division/casting for Spark compatibility
        // Spark's cast(x/y as integer) truncates, DuckDB might round
        // Replace cast(expr as integer) with CAST(TRUNC(expr) AS INTEGER)
        sql = sql.replaceAll("(?i)cast\\s*\\((.*?)\\s+as\\s+integer\\s*\\)",
                            "CAST(TRUNC($1) AS INTEGER)");

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

        return sql;
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
        executeSQLStreaming(sql, session, responseObserver, -1);
    }

    /**
     * Execute SQL query with streaming Arrow batches, optionally with tail collection.
     *
     * <p>When tailLimit > 0, the results are wrapped in a {@link TailBatchIterator}
     * that collects all batches and returns only the last N rows. This is memory-efficient
     * with O(N) memory where N is the tail limit.
     *
     * @param sql SQL query string
     * @param session Session object
     * @param responseObserver Response stream
     * @param tailLimit if > 0, return only the last tailLimit rows
     */
    private void executeSQLStreaming(String sql, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver,
                                     int tailLimit) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            String opDesc = tailLimit > 0 ? "streaming tail(" + tailLimit + ")" : "streaming";
            logger.info("[{}] Executing {} SQL for session {}: {}",
                operationId, opDesc, session.getSessionId(),
                sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);

            // Create executor and get base iterator
            ArrowStreamingExecutor executor = new ArrowStreamingExecutor(session.getRuntime());
            try (ArrowBatchIterator baseIterator = executor.executeStreaming(sql)) {
                // Wrap with TailBatchIterator if tail operation requested
                ArrowBatchIterator iterator = baseIterator;
                if (tailLimit >= 0) {
                    // TailBatchIterator needs the allocator from executor to share root
                    // tailLimit >= 0 because tail(0) should return empty result, not all rows
                    iterator = new TailBatchIterator(baseIterator, executor.getAllocator(), tailLimit);
                }

                try (ArrowBatchIterator effectiveIterator = iterator) {
                    StreamingResultHandler handler = new StreamingResultHandler(
                        responseObserver, session.getSessionId(), operationId);
                    handler.streamResults(effectiveIterator);

                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                    logger.info("[{}] {} query completed in {}ms, {} batches, {} rows",
                        operationId, opDesc, durationMs, handler.getBatchCount(), handler.getTotalRows());
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
     * Checks if a SQL statement is a DDL statement that doesn't return a ResultSet.
     *
     * <p>DDL statements include: CREATE, DROP, ALTER, TRUNCATE, RENAME, GRANT, REVOKE.
     * These need to be executed with executeUpdate() instead of executeQuery().
     *
     * @param sql the SQL statement to check
     * @return true if the statement is DDL
     */
    private boolean isDDLStatement(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }

        String trimmed = sql.trim().toUpperCase();

        // Check for common DDL keywords
        return trimmed.startsWith("CREATE ") ||
               trimmed.startsWith("DROP ") ||
               trimmed.startsWith("ALTER ") ||
               trimmed.startsWith("TRUNCATE ") ||
               trimmed.startsWith("RENAME ") ||
               trimmed.startsWith("GRANT ") ||
               trimmed.startsWith("REVOKE ") ||
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
                com.thunderduck.types.DataType fieldType = convertArrowTypeToDataType(arrowField.getType());
                fields.add(new com.thunderduck.types.StructField(
                    arrowField.getName(),
                    fieldType,
                    arrowField.isNullable()
                ));
            }

            logger.debug("Inferred schema with {} fields", fields.size());
            return new com.thunderduck.types.StructType(fields);
        }
    }

    /**
     * Converts Arrow type to thunderduck DataType.
     */
    private com.thunderduck.types.DataType convertArrowTypeToDataType(org.apache.arrow.vector.types.pojo.ArrowType arrowType) {
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
