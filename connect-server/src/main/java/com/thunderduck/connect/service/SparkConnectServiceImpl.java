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
            }

            if (sql != null) {
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
                    // Deserialize plan
                    LogicalPlan logicalPlan = planConverter.convert(plan);

                    // Get or infer schema
                    com.thunderduck.types.StructType schema = logicalPlan.schema();
                    if (schema == null) {
                        // Infer schema from DuckDB
                        String sql = sqlGenerator.generate(logicalPlan);
                        schema = inferSchemaFromDuckDB(sql);
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
                        // For MVP, return empty values for GET requests
                        ConfigRequest.Get get = op.getGet();
                        logger.debug("Config GET requested for {} keys (returning empty)", get.getKeysCount());
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
        // Return NOT_FOUND to indicate the execution is already complete.
        responseObserver.onError(Status.NOT_FOUND
            .withDescription("Operation " + operationId + " not found. " +
                "Queries complete immediately in MVP and cannot be reattached.")
            .asRuntimeException());
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
            org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(fields);

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
