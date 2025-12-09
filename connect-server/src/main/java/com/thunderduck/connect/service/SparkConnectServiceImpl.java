package com.thunderduck.connect.service;

import com.thunderduck.connect.converter.PlanConverter;
import com.thunderduck.connect.session.Session;
import com.thunderduck.connect.session.SessionManager;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.runtime.DuckDBConnectionManager;
import com.thunderduck.runtime.QueryExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.spark.connect.proto.*;
import org.apache.spark.connect.proto.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.thunderduck.generator.SQLQuoting.quoteIdentifier;

/**
 * Implementation of Spark Connect gRPC service.
 *
 * This service bridges Spark Connect protocol to DuckDB via thunderduck.
 * Key features:
 * - Single-session management (DuckDB is single-user)
 * - SQL query execution via QueryExecutor
 * - Minimal viable implementation for MVP
 */
public class SparkConnectServiceImpl extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(SparkConnectServiceImpl.class);

    private final SessionManager sessionManager;
    private final DuckDBConnectionManager connectionManager;
    private final PlanConverter planConverter;
    private final SQLGenerator sqlGenerator;

    /**
     * Create Spark Connect service with session manager and DuckDB connection manager.
     *
     * @param sessionManager Manager for single-session lifecycle
     * @param connectionManager DuckDB connection manager (pool size 1)
     */
    public SparkConnectServiceImpl(SessionManager sessionManager,
                                  DuckDBConnectionManager connectionManager) {
        this.sessionManager = sessionManager;
        this.connectionManager = connectionManager;
        this.planConverter = new PlanConverter();
        this.sqlGenerator = new SQLGenerator();
        logger.info("SparkConnectServiceImpl initialized with plan deserialization support");
    }

    /**
     * Execute a Spark plan and stream results back to client.
     *
     * This is the main RPC for query execution. For MVP:
     * 1. Extract session ID from request
     * 2. Validate/create session
     * 3. Deserialize plan to SQL
     * 4. Execute via QueryExecutor
     * 5. Stream results as Arrow batches
     *
     * @param request ExecutePlanRequest containing the plan
     * @param responseObserver Stream observer for responses
     */
    @Override
    public void executePlan(ExecutePlanRequest request,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        logger.info("executePlan called for session: {}", sessionId);

        try {
            // Update activity or create session
            updateOrCreateSession(sessionId);

            // Extract the plan
            if (!request.hasPlan()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must contain a plan")
                    .asRuntimeException());
                return;
            }

            Plan plan = request.getPlan();
            logger.debug("Plan type: {}", plan.getOpTypeCase());

            // For MVP, handle SQL queries (both direct and command-wrapped)
            String sql = null;
            boolean isShowString = false;
            int showStringNumRows = 20;  // default
            int showStringTruncate = 20; // default
            boolean showStringVertical = false;

            if (plan.hasRoot()) {
                Relation root = plan.getRoot();
                logger.debug("Root relation type: {}", root.getRelTypeCase());

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
                            LogicalPlan innerPlan = planConverter.convertRelation(input);
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
                }
            } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                // SQL command (e.g., spark.sql(...))
                sql = plan.getCommand().getSqlCommand().getSql();
                logger.debug("Found SQL command: {}", sql);
            } else if (plan.hasCommand()) {
                // Handle non-SQL commands (CreateTempView, DropTempView, etc.)
                Command command = plan.getCommand();
                logger.debug("Handling COMMAND: {}", command.getCommandTypeCase());

                executeCommand(command, sessionId, responseObserver);
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
                    executeShowString(sql, sessionId, showStringNumRows, showStringTruncate,
                        showStringVertical, responseObserver);
                } else {
                    // Regular SQL execution
                    executeSQL(sql, sessionId, responseObserver);
                }
            } else if (plan.hasRoot()) {
                // Non-SQL plan - use plan deserialization
                logger.info("Deserializing DataFrame plan: {}", plan.getRoot().getRelTypeCase());

                try {
                    // Convert Protobuf plan to LogicalPlan
                    LogicalPlan logicalPlan = planConverter.convert(plan);

                    // Generate SQL from LogicalPlan
                    String generatedSQL = sqlGenerator.generate(logicalPlan);
                    logger.info("Generated SQL from plan: {}", generatedSQL);

                    // Execute the generated SQL
                    executeSQL(generatedSQL, sessionId, responseObserver);

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

        } catch (Exception e) {
            logger.error("Error executing plan for session " + sessionId, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Execution failed: " + e.getMessage())
                .withCause(e)
                .asRuntimeException());
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
     * @param sessionId Session identifier
     * @param responseObserver Stream observer for responses
     */
    private void executeCommand(Command command, String sessionId,
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
                LogicalPlan logicalPlan = planConverter.convertRelation(input);

                // Get session and register view
                Session session = sessionManager.getSession(sessionId);
                if (session == null) {
                    responseObserver.onError(Status.INTERNAL
                        .withDescription("Session not found: " + sessionId)
                        .asRuntimeException());
                    return;
                }

                // Check if view already exists and replace=false
                if (!replace && session.getTempView(viewName).isPresent()) {
                    responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("Temp view already exists: " + viewName)
                        .asRuntimeException());
                    return;
                }

                session.registerTempView(viewName, logicalPlan);

                // Generate SQL from the plan and create DuckDB temp view
                String viewSQL = sqlGenerator.generate(logicalPlan);
                String createViewSQL = String.format("CREATE OR REPLACE TEMP VIEW %s AS %s",
                    quoteIdentifier(viewName), viewSQL);

                logger.debug("Creating DuckDB temp view: {}", createViewSQL);

                // Execute the CREATE VIEW statement in DuckDB
                QueryExecutor executor = new QueryExecutor(connectionManager);
                try {
                    executor.execute(createViewSQL);
                } catch (Exception e) {
                    logger.error("Failed to create DuckDB temp view", e);
                    responseObserver.onError(Status.INTERNAL
                        .withDescription("Failed to create DuckDB temp view: " + e.getMessage())
                        .asRuntimeException());
                    return;
                }

                // Return success response (empty result with operation ID)
                String operationId = java.util.UUID.randomUUID().toString();
                ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                    .setSessionId(sessionId)
                    .setOperationId(operationId)
                    .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                logger.info("✓ Temp view created in DuckDB: '{}' (session: {})", viewName, sessionId);

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
     * Analyze a plan and return metadata (schema, execution plan, etc.).
     *
     * For MVP, return minimal response.
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
            updateOrCreateSession(sessionId);

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
                        schema = inferSchemaFromDuckDB(sql);

                    } else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
                        // SQL command - infer schema
                        sql = plan.getCommand().getSqlCommand().getSql();
                        // Apply SQL preprocessing for Spark compatibility
                        sql = preprocessSQL(sql);
                        logger.debug("Analyzing SQL command schema: {}", sql.substring(0, Math.min(100, sql.length())));
                        schema = inferSchemaFromDuckDB(sql);

                    } else {
                        // Regular plan - deserialize and extract schema
                        LogicalPlan logicalPlan = planConverter.convert(plan);
                        schema = logicalPlan.schema();

                        if (schema == null) {
                            // Infer schema from generated SQL
                            sql = sqlGenerator.generate(logicalPlan);
                            schema = inferSchemaFromDuckDB(sql);
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
     * (default_null_order='NULLS FIRST' is set in DuckDBConnectionManager)
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
     * For MVP: Handle GetWithDefault operations by returning the default values.
     * This is required for PySpark client compatibility.
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
            updateOrCreateSession(sessionId);

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
                        Session session = sessionManager.getSession(sessionId);

                        if (session == null) {
                            logger.error("Session not found: {}", sessionId);
                            responseObserver.onError(Status.NOT_FOUND
                                .withDescription("Session not found: " + sessionId)
                                .asRuntimeException());
                            return;
                        }

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
     * NOT IMPLEMENTED in MVP.
     */
    @Override
    public StreamObserver<AddArtifactsRequest> addArtifacts(
            StreamObserver<AddArtifactsResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact management not implemented in MVP")
            .asRuntimeException());
        return null;
    }

    /**
     * Check artifact status.
     * NOT IMPLEMENTED in MVP.
     */
    @Override
    public void artifactStatus(ArtifactStatusesRequest request,
                              StreamObserver<ArtifactStatusesResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Artifact status not implemented in MVP")
            .asRuntimeException());
    }

    /**
     * Interrupt running execution.
     * NOT IMPLEMENTED in MVP (single-threaded execution for now).
     */
    @Override
    public void interrupt(InterruptRequest request,
                         StreamObserver<InterruptResponse> responseObserver) {
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Query interruption not implemented in MVP")
            .asRuntimeException());
    }

    /**
     * Reattach to existing execution.
     *
     * For MVP: Since we execute queries synchronously and don't maintain execution state,
     * we return NOT_FOUND to indicate the execution has already completed.
     * This allows clients with reattachable execution enabled to work properly.
     */
    @Override
    public void reattachExecute(ReattachExecuteRequest request,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("reattachExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // For MVP, we don't maintain execution state. Queries complete immediately.
        // Return empty stream with ResultComplete to indicate execution is done.
        // This allows PySpark clients with reattachable execution to work properly.

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
     * For MVP: Since we don't maintain execution state (queries complete immediately),
     * this is a no-op that returns success to satisfy the client cleanup flow.
     */
    @Override
    public void releaseExecute(ReleaseExecuteRequest request,
                              StreamObserver<ReleaseExecuteResponse> responseObserver) {
        String sessionId = request.getSessionId();
        String operationId = request.getOperationId();

        logger.info("releaseExecute called for session: {}, operation: {}",
            sessionId, operationId);

        // Return success response (no-op, we don't maintain execution state)
        ReleaseExecuteResponse response = ReleaseExecuteResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.debug("Released operation: {} (no-op in MVP)", operationId);
    }

    // ========== Helper Methods ==========

    /**
     * Update activity for existing session or create new session.
     *
     * @param sessionId Session ID from request
     * @throws Exception if session creation fails (server busy)
     */
    private void updateOrCreateSession(String sessionId) throws Exception {
        SessionManager.SessionInfo info = sessionManager.getSessionInfo();

        if (info.hasActiveSession) {
            // Update activity for existing session
            sessionManager.updateActivity(sessionId);
        } else {
            // Create new session (will throw if server is busy)
            Session session = sessionManager.createSession(sessionId);
            logger.info("New session created: {}", session);
        }
    }

    /**
     * Execute SQL query and format results as ShowString (text table).
     *
     * @param sql SQL query string
     * @param sessionId Session ID
     * @param numRows Maximum rows to show
     * @param truncate Maximum column width (0 = no truncation)
     * @param vertical Show vertically if true
     * @param responseObserver Response stream
     */
    private void executeShowString(String sql, String sessionId, int numRows, int truncate,
                                   boolean vertical, StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            logger.info("[{}] Executing ShowString SQL for session {}: {}", operationId, sessionId, sql);

            // Create QueryExecutor with connection manager
            QueryExecutor executor = new QueryExecutor(connectionManager);

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
            streamArrowResults(showStringRoot, sessionId, operationId, responseObserver);

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
     * @param sql SQL query string
     * @param sessionId Session ID
     * @param responseObserver Response stream
     */
    private void executeSQL(String sql, String sessionId,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        String operationId = java.util.UUID.randomUUID().toString();
        long startTime = System.nanoTime();

        try {
            logger.info("[{}] Executing SQL for session {}: {}", operationId, sessionId, sql);

            // Create QueryExecutor with connection manager
            QueryExecutor executor = new QueryExecutor(connectionManager);

            // Execute query and get Arrow results
            org.apache.arrow.vector.VectorSchemaRoot results = executor.executeQuery(sql);

            // Stream Arrow results
            streamArrowResults(results, sessionId, operationId, responseObserver);

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            logger.info("[{}] Query completed in {}ms, {} rows",
                operationId, durationMs, results.getRowCount());

            // Clean up Arrow resources
            results.close();

        } catch (Exception e) {
            logger.error("[{}] SQL execution failed", operationId, e);

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
     * Stream Arrow results to client.
     *
     * For MVP, we stream all results in a single batch.
     * Multi-batch streaming will be added in Week 12.
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
     */
    private com.thunderduck.types.StructType inferSchemaFromDuckDB(String sql) throws Exception {
        QueryExecutor executor = new QueryExecutor(connectionManager);
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
}
