package com.thunderduck.connect.service;

import com.thunderduck.connect.session.Session;
import com.thunderduck.runtime.QueryExecutor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.connect.proto.Catalog;
import org.apache.spark.connect.proto.CurrentDatabase;
import org.apache.spark.connect.proto.DatabaseExists;
import org.apache.spark.connect.proto.DropTempView;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.apache.spark.connect.proto.ListColumns;
import org.apache.spark.connect.proto.ListDatabases;
import org.apache.spark.connect.proto.ListTables;
import org.apache.spark.connect.proto.SetCurrentDatabase;
import org.apache.spark.connect.proto.TableExists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.thunderduck.generator.SQLQuoting.quoteIdentifier;

/**
 * Handles Spark Connect Catalog operations by delegating to DuckDB.
 *
 * <p>This handler maps Spark Catalog operations to DuckDB's information_schema
 * and system tables. See docs/architect/CATALOG_OPERATIONS.md for details.
 *
 * <p>Implemented operations:
 * <ul>
 *   <li>DROP_TEMP_VIEW - Drop a temporary view</li>
 *   <li>TABLE_EXISTS - Check if table/view exists</li>
 *   <li>LIST_TABLES - List all tables/views</li>
 *   <li>LIST_COLUMNS - List columns for a table</li>
 *   <li>LIST_DATABASES - List all schemas</li>
 *   <li>DATABASE_EXISTS - Check if schema exists</li>
 * </ul>
 */
public class CatalogOperationHandler {

    private static final Logger logger = LoggerFactory.getLogger(CatalogOperationHandler.class);

    // Schema filter to exclude system schemas
    private static final String SCHEMA_FILTER =
        "table_schema NOT IN ('information_schema', 'pg_catalog')";

    /**
     * Execute a catalog operation.
     *
     * @param catalog the catalog operation to execute
     * @param session the session
     * @param responseObserver the gRPC response observer
     */
    public void execute(Catalog catalog, Session session,
                       StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            logger.debug("Handling CATALOG operation: {}", catalog.getCatTypeCase());

            switch (catalog.getCatTypeCase()) {
                case DROP_TEMP_VIEW:
                    handleDropTempView(catalog.getDropTempView(), session, responseObserver);
                    break;

                case TABLE_EXISTS:
                    handleTableExists(catalog.getTableExists(), session, responseObserver);
                    break;

                case LIST_TABLES:
                    handleListTables(catalog.getListTables(), session, responseObserver);
                    break;

                case LIST_COLUMNS:
                    handleListColumns(catalog.getListColumns(), session, responseObserver);
                    break;

                case LIST_DATABASES:
                    handleListDatabases(catalog.getListDatabases(), session, responseObserver);
                    break;

                case DATABASE_EXISTS:
                    handleDatabaseExists(catalog.getDatabaseExists(), session, responseObserver);
                    break;

                case CURRENT_DATABASE:
                    handleCurrentDatabase(session, responseObserver);
                    break;

                case SET_CURRENT_DATABASE:
                    handleSetCurrentDatabase(catalog.getSetCurrentDatabase(), session, responseObserver);
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
     * Handle DROP_TEMP_VIEW operation.
     */
    private void handleDropTempView(DropTempView dropTempView, Session session,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String viewName = dropTempView.getViewName();
        logger.info("Dropping temp view: '{}'", viewName);

        // Remove from session's temp view registry
        boolean existed = session.dropTempView(viewName);

        // Drop the view from DuckDB
        String dropViewSQL = String.format("DROP VIEW IF EXISTS %s", quoteIdentifier(viewName));
        logger.debug("Executing DuckDB: {}", dropViewSQL);

        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        executor.execute(dropViewSQL);

        // Return boolean result
        String operationId = UUID.randomUUID().toString();
        StreamingResultHandler resultHandler = new StreamingResultHandler(
            responseObserver, session.getSessionId(), operationId);
        resultHandler.streamBooleanResult(existed);

        logger.info("✓ View dropped from DuckDB: '{}' (existed={}, session: {})",
            viewName, existed, session.getSessionId());
    }

    /**
     * Handle TABLE_EXISTS operation.
     */
    private void handleTableExists(TableExists tableExists, Session session,
                                   StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = tableExists.getTableName();
        String dbName = tableExists.hasDbName() ? tableExists.getDbName() : null;

        logger.info("Checking table exists: '{}' (db={})", tableName, dbName);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '");
        sql.append(escapeSql(tableName)).append("'");
        sql.append(" AND ").append(SCHEMA_FILTER);
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        sql.append(") AS result");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql.toString())) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                // DuckDB returns boolean as BitVector
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            // Return boolean result
            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamBooleanResult(exists);

            logger.info("✓ tableExists('{}') = {}", tableName, exists);
        }
    }

    /**
     * Handle LIST_TABLES operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - catalog: string (always "default")
     * - namespace: array<string> (schema path)
     * - description: string (empty)
     * - tableType: string (MANAGED, VIEW, EXTERNAL)
     * - isTemporary: boolean
     */
    private void handleListTables(ListTables listTables, Session session,
                                  StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = listTables.hasDbName() ? listTables.getDbName() : null;
        String pattern = listTables.hasPattern() ? listTables.getPattern() : null;

        logger.info("Listing tables (db={}, pattern={})", dbName, pattern);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE ");
        sql.append(SCHEMA_FILTER);
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        if (pattern != null) {
            sql.append(" AND table_name LIKE '").append(escapeSql(pattern)).append("'");
        }
        sql.append(" ORDER BY table_schema, table_name");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListTablesSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector catalogVec = (VarCharVector) result.getVector("catalog");
                VarCharVector namespaceVec = (VarCharVector) result.getVector("namespace");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector typeVec = (VarCharVector) result.getVector("tableType");
                BitVector tempVec = (BitVector) result.getVector("isTemporary");

                nameVec.allocateNew(rowCount);
                catalogVec.allocateNew(rowCount);
                namespaceVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                typeVec.allocateNew(rowCount);
                tempVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("table_name");
                VarCharVector srcSchema = (VarCharVector) queryResult.getVector("table_schema");
                VarCharVector srcType = (VarCharVector) queryResult.getVector("table_type");

                for (int i = 0; i < rowCount; i++) {
                    String tableName = new String(srcName.get(i), StandardCharsets.UTF_8);
                    String schemaName = new String(srcSchema.get(i), StandardCharsets.UTF_8);
                    String tableType = new String(srcType.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, tableName.getBytes(StandardCharsets.UTF_8));
                    catalogVec.set(i, "default".getBytes(StandardCharsets.UTF_8));
                    namespaceVec.set(i, ("[\"" + schemaName + "\"]").getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));

                    // Map table type
                    String sparkType = mapTableType(tableType);
                    typeVec.set(i, sparkType.getBytes(StandardCharsets.UTF_8));

                    // Check if temporary (in session registry)
                    boolean isTemp = session.getTempView(tableName).isPresent();
                    tempVec.set(i, isTemp ? 1 : 0);
                }

                nameVec.setValueCount(rowCount);
                catalogVec.setValueCount(rowCount);
                namespaceVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                typeVec.setValueCount(rowCount);
                tempVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listTables returned {} tables", rowCount);
            }
        }
    }

    /**
     * Handle LIST_COLUMNS operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - description: string (empty)
     * - dataType: string
     * - nullable: boolean
     * - isPartition: boolean (always false)
     * - isBucket: boolean (always false)
     */
    private void handleListColumns(ListColumns listColumns, Session session,
                                   StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String tableName = listColumns.getTableName();
        String dbName = listColumns.hasDbName() ? listColumns.getDbName() : null;

        logger.info("Listing columns for table: '{}' (db={})", tableName, dbName);

        // Build query
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT column_name, data_type, is_nullable FROM information_schema.columns ");
        sql.append("WHERE table_name = '").append(escapeSql(tableName)).append("'");
        if (dbName != null) {
            sql.append(" AND table_schema = '").append(escapeSql(dbName)).append("'");
        }
        sql.append(" ORDER BY ordinal_position");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            if (queryResult.getRowCount() == 0) {
                // Table not found
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Table not found: " + tableName)
                    .asRuntimeException());
                return;
            }

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListColumnsSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector dataTypeVec = (VarCharVector) result.getVector("dataType");
                BitVector nullableVec = (BitVector) result.getVector("nullable");
                BitVector partitionVec = (BitVector) result.getVector("isPartition");
                BitVector bucketVec = (BitVector) result.getVector("isBucket");
                BitVector clusterVec = (BitVector) result.getVector("isCluster");

                nameVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                dataTypeVec.allocateNew(rowCount);
                nullableVec.allocateNew(rowCount);
                partitionVec.allocateNew(rowCount);
                bucketVec.allocateNew(rowCount);
                clusterVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("column_name");
                VarCharVector srcType = (VarCharVector) queryResult.getVector("data_type");
                VarCharVector srcNullable = (VarCharVector) queryResult.getVector("is_nullable");

                for (int i = 0; i < rowCount; i++) {
                    String colName = new String(srcName.get(i), StandardCharsets.UTF_8);
                    String dataType = new String(srcType.get(i), StandardCharsets.UTF_8);
                    String nullable = new String(srcNullable.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, colName.getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                    dataTypeVec.set(i, dataType.getBytes(StandardCharsets.UTF_8));
                    nullableVec.set(i, "YES".equals(nullable) ? 1 : 0);
                    partitionVec.set(i, 0);  // Always false
                    bucketVec.set(i, 0);     // Always false
                    clusterVec.set(i, 0);    // Always false
                }

                nameVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                dataTypeVec.setValueCount(rowCount);
                nullableVec.setValueCount(rowCount);
                partitionVec.setValueCount(rowCount);
                bucketVec.setValueCount(rowCount);
                clusterVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listColumns('{}') returned {} columns", tableName, rowCount);
            }
        }
    }

    /**
     * Handle LIST_DATABASES operation.
     *
     * Returns Arrow table with columns:
     * - name: string
     * - catalog: string (always "default")
     * - description: string (empty)
     * - locationUri: string (empty)
     */
    private void handleListDatabases(ListDatabases listDatabases, Session session,
                                     StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String pattern = listDatabases.hasPattern() ? listDatabases.getPattern() : null;

        logger.info("Listing databases (pattern={})", pattern);

        // Build query (DISTINCT to avoid duplicates in DuckDB's information_schema)
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE ");
        sql.append("schema_name NOT IN ('information_schema', 'pg_catalog')");
        if (pattern != null) {
            sql.append(" AND schema_name LIKE '").append(escapeSql(pattern)).append("'");
        }
        sql.append(" ORDER BY schema_name");

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot queryResult = executor.executeQuery(sql.toString())) {

            // Build result Arrow table with Spark catalog schema
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                Schema schema = createListDatabasesSchema();
                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

                int rowCount = queryResult.getRowCount();
                result.setRowCount(rowCount);

                // Get vectors
                VarCharVector nameVec = (VarCharVector) result.getVector("name");
                VarCharVector catalogVec = (VarCharVector) result.getVector("catalog");
                VarCharVector descVec = (VarCharVector) result.getVector("description");
                VarCharVector locVec = (VarCharVector) result.getVector("locationUri");

                nameVec.allocateNew(rowCount);
                catalogVec.allocateNew(rowCount);
                descVec.allocateNew(rowCount);
                locVec.allocateNew(rowCount);

                // Get source vectors
                VarCharVector srcName = (VarCharVector) queryResult.getVector("schema_name");

                for (int i = 0; i < rowCount; i++) {
                    String schemaName = new String(srcName.get(i), StandardCharsets.UTF_8);

                    nameVec.set(i, schemaName.getBytes(StandardCharsets.UTF_8));
                    catalogVec.set(i, "default".getBytes(StandardCharsets.UTF_8));
                    descVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                    locVec.set(i, "".getBytes(StandardCharsets.UTF_8));
                }

                nameVec.setValueCount(rowCount);
                catalogVec.setValueCount(rowCount);
                descVec.setValueCount(rowCount);
                locVec.setValueCount(rowCount);

                // Stream result
                String operationId = UUID.randomUUID().toString();
                StreamingResultHandler resultHandler = new StreamingResultHandler(
                    responseObserver, session.getSessionId(), operationId);
                resultHandler.streamArrowResult(result);

                logger.info("✓ listDatabases returned {} databases", rowCount);
            }
        }
    }

    /**
     * Handle DATABASE_EXISTS operation.
     */
    private void handleDatabaseExists(DatabaseExists databaseExists, Session session,
                                      StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = databaseExists.getDbName();

        logger.info("Checking database exists: '{}'", dbName);

        // Build query
        String sql = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata " +
                    "WHERE schema_name = '" + escapeSql(dbName) + "' " +
                    "AND schema_name NOT IN ('information_schema', 'pg_catalog')) AS result";

        logger.debug("Executing: {}", sql);

        // Execute query
        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            // Return boolean result
            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamBooleanResult(exists);

            logger.info("✓ databaseExists('{}') = {}", dbName, exists);
        }
    }

    /**
     * Handle CURRENT_DATABASE operation.
     */
    private void handleCurrentDatabase(Session session,
                                       StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        // Get current database from session config, default to "main"
        String currentDb = session.getConfig("spark.catalog.currentDatabase");
        if (currentDb == null) {
            currentDb = "main";
        }

        logger.info("Current database: '{}'", currentDb);

        // Return as single-row Arrow table with "value" column
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = new Schema(Arrays.asList(
                Field.nullable("value", ArrowType.Utf8.INSTANCE)
            ));
            VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
            result.setRowCount(1);

            VarCharVector vec = (VarCharVector) result.getVector("value");
            vec.allocateNew(1);
            vec.set(0, currentDb.getBytes(StandardCharsets.UTF_8));
            vec.setValueCount(1);

            String operationId = UUID.randomUUID().toString();
            StreamingResultHandler resultHandler = new StreamingResultHandler(
                responseObserver, session.getSessionId(), operationId);
            resultHandler.streamArrowResult(result);

            logger.info("✓ currentDatabase() = '{}'", currentDb);
        }
    }

    /**
     * Handle SET_CURRENT_DATABASE operation.
     */
    private void handleSetCurrentDatabase(SetCurrentDatabase setCurrentDb, Session session,
                                          StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        String dbName = setCurrentDb.getDbName();

        logger.info("Setting current database to: '{}'", dbName);

        // Validate database exists
        String sql = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata " +
                    "WHERE schema_name = '" + escapeSql(dbName) + "') AS result";

        QueryExecutor executor = new QueryExecutor(session.getRuntime());
        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            boolean exists = false;
            if (result.getRowCount() > 0) {
                BitVector vec = (BitVector) result.getVector(0);
                exists = vec.get(0) == 1;
            }

            if (!exists) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Database not found: " + dbName)
                    .asRuntimeException());
                return;
            }
        }

        // Set in session config
        session.setConfig("spark.catalog.currentDatabase", dbName);

        // Set search path in DuckDB
        executor.execute("SET search_path TO " + quoteIdentifier(dbName));

        // Return empty success response
        String operationId = UUID.randomUUID().toString();
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(session.getSessionId())
            .setOperationId(operationId)
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        logger.info("✓ setCurrentDatabase('{}') completed", dbName);
    }

    // ==================== Helper Methods ====================

    /**
     * Create Arrow schema for listTables result.
     */
    private Schema createListTablesSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("catalog", ArrowType.Utf8.INSTANCE),
            Field.nullable("namespace", ArrowType.Utf8.INSTANCE),  // JSON array string
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("tableType", ArrowType.Utf8.INSTANCE),
            Field.nullable("isTemporary", ArrowType.Bool.INSTANCE)
        ));
    }

    /**
     * Create Arrow schema for listColumns result.
     */
    private Schema createListColumnsSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("dataType", ArrowType.Utf8.INSTANCE),
            Field.nullable("nullable", ArrowType.Bool.INSTANCE),
            Field.nullable("isPartition", ArrowType.Bool.INSTANCE),
            Field.nullable("isBucket", ArrowType.Bool.INSTANCE),
            Field.nullable("isCluster", ArrowType.Bool.INSTANCE)
        ));
    }

    /**
     * Create Arrow schema for listDatabases result.
     */
    private Schema createListDatabasesSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            Field.nullable("catalog", ArrowType.Utf8.INSTANCE),
            Field.nullable("description", ArrowType.Utf8.INSTANCE),
            Field.nullable("locationUri", ArrowType.Utf8.INSTANCE)
        ));
    }

    /**
     * Map DuckDB table type to Spark table type.
     */
    private String mapTableType(String duckdbType) {
        if (duckdbType == null) return "MANAGED";
        switch (duckdbType.toUpperCase()) {
            case "VIEW":
                return "VIEW";
            case "BASE TABLE":
                return "MANAGED";
            case "EXTERNAL TABLE":
                return "EXTERNAL";
            default:
                return "MANAGED";
        }
    }

    /**
     * Escape single quotes in SQL strings.
     */
    private String escapeSql(String value) {
        if (value == null) return null;
        return value.replace("'", "''");
    }
}
