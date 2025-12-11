package com.thunderduck.runtime;

/**
 * Configuration constants for Arrow streaming.
 */
public final class StreamingConfig {

    private StreamingConfig() {} // Utility class

    /** Default batch size in rows - aligned with DuckDB row group */
    public static final int DEFAULT_BATCH_SIZE = 8192;

    /** Maximum batch size to prevent excessive memory per batch */
    public static final int MAX_BATCH_SIZE = 65536;

    /** Minimum batch size to prevent too many small batches */
    public static final int MIN_BATCH_SIZE = 1024;

    /**
     * Validate and normalize batch size to be within allowed bounds.
     *
     * @param requested the requested batch size
     * @return normalized batch size within [MIN_BATCH_SIZE, MAX_BATCH_SIZE]
     */
    public static int normalizeBatchSize(int requested) {
        if (requested <= 0) return DEFAULT_BATCH_SIZE;
        if (requested < MIN_BATCH_SIZE) return MIN_BATCH_SIZE;
        if (requested > MAX_BATCH_SIZE) return MAX_BATCH_SIZE;
        return requested;
    }
}
