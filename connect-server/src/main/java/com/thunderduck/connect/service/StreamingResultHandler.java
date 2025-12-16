package com.thunderduck.connect.service;

import com.google.protobuf.ByteString;
import com.thunderduck.runtime.ArrowBatchIterator;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.connect.proto.ExecutePlanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.UUID;

/**
 * Handles streaming Arrow batches to gRPC clients.
 *
 * <p>This class streams Arrow batches from an {@link ArrowBatchIterator} to a gRPC
 * {@link StreamObserver}, serializing each batch to Arrow IPC format for transmission
 * via the Spark Connect protocol.
 *
 * <p>Features:
 * <ul>
 *   <li>Batch-by-batch streaming with configurable batch size</li>
 *   <li>Client cancellation detection via gRPC Context</li>
 *   <li>Proper error handling with partial result notification</li>
 *   <li>Schema sent with first batch only (Arrow IPC format)</li>
 * </ul>
 */
public class StreamingResultHandler {

    private static final Logger logger = LoggerFactory.getLogger(StreamingResultHandler.class);

    private final StreamObserver<ExecutePlanResponse> responseObserver;
    private final String sessionId;
    private final String operationId;
    private final Context grpcContext;

    private int batchIndex = 0;
    private long totalRows = 0;

    /**
     * Create a streaming result handler.
     *
     * @param responseObserver gRPC stream observer for sending responses
     * @param sessionId session identifier
     * @param operationId operation identifier for logging and tracking
     */
    public StreamingResultHandler(StreamObserver<ExecutePlanResponse> responseObserver,
                                  String sessionId,
                                  String operationId) {
        this.responseObserver = responseObserver;
        this.sessionId = sessionId;
        this.operationId = operationId;
        this.grpcContext = Context.current();
    }

    /**
     * Stream all batches from iterator to gRPC client.
     *
     * <p>Iterates through all batches, serializing each to Arrow IPC format
     * and sending via gRPC. Handles client cancellation and iteration errors.
     *
     * <p>If there are no batches (empty result), sends an empty batch with just
     * the schema so that PySpark can construct an empty DataFrame.
     *
     * @param iterator Arrow batch iterator
     * @throws IOException if streaming fails
     */
    public void streamResults(ArrowBatchIterator iterator) throws IOException {
        try {
            while (iterator.hasNext()) {
                // Check for client cancellation between batches
                if (grpcContext.isCancelled()) {
                    logger.info("[{}] Query cancelled by client after {} batches, {} rows",
                        operationId, batchIndex, totalRows);
                    return;
                }

                VectorSchemaRoot batch = iterator.next();
                streamBatch(batch, batchIndex == 0);
                totalRows += batch.getRowCount();
                batchIndex++;
            }

            // Check for iteration errors
            if (iterator.hasError()) {
                throw new IOException("Batch iteration failed", iterator.getError());
            }

            // If no batches were streamed, send an empty batch with schema
            // This is required for PySpark to construct an empty DataFrame
            if (batchIndex == 0) {
                sendEmptyBatchWithSchema(iterator.getSchema());
            }

            // Send completion marker
            sendResultComplete();

            logger.info("[{}] Streamed {} batches, {} total rows",
                operationId, batchIndex, totalRows);

        } catch (Exception e) {
            logger.error("[{}] Streaming failed after {} batches, {} rows",
                operationId, batchIndex, totalRows, e);

            responseObserver.onError(Status.INTERNAL
                .withDescription("Query streaming failed after " + batchIndex +
                               " batches: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Serialize and stream a single batch to the client.
     *
     * @param batch the Arrow batch to stream
     * @param includeSchema whether to include schema header (first batch only)
     * @throws IOException if serialization fails
     */
    private void streamBatch(VectorSchemaRoot batch, boolean includeSchema) throws IOException {
        // Serialize batch to Arrow IPC format
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (ArrowStreamWriter writer = new ArrowStreamWriter(
                batch, null, Channels.newChannel(out))) {

            // CRITICAL: Must call start() to write schema header!
            // Without this, PySpark cannot properly read DATE columns and other types
            writer.start();
            writer.writeBatch();
            writer.end();
        }

        byte[] arrowData = out.toByteArray();

        // Build gRPC response
        ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setResponseId(UUID.randomUUID().toString())
            .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder()
                .setRowCount(batch.getRowCount())
                .setData(ByteString.copyFrom(arrowData))
                .build())
            .build();

        responseObserver.onNext(response);

        logger.debug("[{}] Sent batch {}: {} rows, {} bytes",
            operationId, batchIndex, batch.getRowCount(), arrowData.length);
    }

    /**
     * Send an empty batch with just the schema.
     *
     * <p>This is required when there are no results (e.g., tail(0) or a query that
     * returns zero rows). PySpark needs at least one Arrow batch with the schema
     * to construct an empty DataFrame.
     *
     * @param schema the Arrow schema
     * @throws IOException if serialization fails
     */
    private void sendEmptyBatchWithSchema(Schema schema) throws IOException {
        // Create empty VectorSchemaRoot with the schema
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(schema, allocator)) {

            emptyRoot.setRowCount(0);

            // Serialize to Arrow IPC format
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    emptyRoot, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            byte[] arrowData = out.toByteArray();

            // Build gRPC response
            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(UUID.randomUUID().toString())
                .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(0)
                    .setData(ByteString.copyFrom(arrowData))
                    .build())
                .build();

            responseObserver.onNext(response);
            batchIndex++;

            logger.debug("[{}] Sent empty batch with schema: {} bytes", operationId, arrowData.length);
        }
    }

    /**
     * Send completion marker to indicate all results have been sent.
     */
    private void sendResultComplete() {
        ExecutePlanResponse complete = ExecutePlanResponse.newBuilder()
            .setSessionId(sessionId)
            .setOperationId(operationId)
            .setResponseId(UUID.randomUUID().toString())
            .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
            .build();

        responseObserver.onNext(complete);
        responseObserver.onCompleted();
    }

    /**
     * Get the number of batches streamed so far.
     */
    public int getBatchCount() {
        return batchIndex;
    }

    /**
     * Get the total number of rows streamed so far.
     */
    public long getTotalRows() {
        return totalRows;
    }

    /**
     * Stream a single boolean result to the client.
     *
     * <p>This is used for catalog operations like dropTempView that return a boolean
     * indicating success or whether an entity existed.
     *
     * @param value the boolean value to return
     * @throws IOException if serialization fails
     */
    public void streamBooleanResult(boolean value) throws IOException {
        // Create Arrow schema with single boolean field
        org.apache.arrow.vector.types.pojo.Field field =
            new org.apache.arrow.vector.types.pojo.Field(
                "value",
                org.apache.arrow.vector.types.pojo.FieldType.nullable(
                    org.apache.arrow.vector.types.pojo.ArrowType.Bool.INSTANCE),
                null);
        Schema schema = new Schema(java.util.Collections.singletonList(field));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            // Set the boolean value
            org.apache.arrow.vector.BitVector vector =
                (org.apache.arrow.vector.BitVector) root.getVector("value");
            vector.allocateNew(1);
            vector.set(0, value ? 1 : 0);
            vector.setValueCount(1);
            root.setRowCount(1);

            // Serialize to Arrow IPC format
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(
                    root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            byte[] arrowData = out.toByteArray();

            // Build gRPC response with Arrow batch
            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(operationId)
                .setResponseId(UUID.randomUUID().toString())
                .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(1)
                    .setData(ByteString.copyFrom(arrowData))
                    .build())
                .build();

            responseObserver.onNext(response);
            batchIndex++;
            totalRows++;

            logger.debug("[{}] Sent boolean result: {}", operationId, value);
        }

        // Send completion marker
        sendResultComplete();
    }
}
