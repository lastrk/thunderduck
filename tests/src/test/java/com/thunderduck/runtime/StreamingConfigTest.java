package com.thunderduck.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StreamingConfig Tests")
class StreamingConfigTest {

    @Test
    @DisplayName("Constants have expected values")
    void constantsHaveExpectedValues() {
        assertEquals(8192, StreamingConfig.DEFAULT_BATCH_SIZE);
        assertEquals(65536, StreamingConfig.MAX_BATCH_SIZE);
        assertEquals(1024, StreamingConfig.MIN_BATCH_SIZE);
    }

    @Test
    @DisplayName("normalizeBatchSize returns default for zero")
    void normalizeBatchSizeReturnsDefaultForZero() {
        assertEquals(StreamingConfig.DEFAULT_BATCH_SIZE, StreamingConfig.normalizeBatchSize(0));
    }

    @Test
    @DisplayName("normalizeBatchSize returns default for negative")
    void normalizeBatchSizeReturnsDefaultForNegative() {
        assertEquals(StreamingConfig.DEFAULT_BATCH_SIZE, StreamingConfig.normalizeBatchSize(-1));
        assertEquals(StreamingConfig.DEFAULT_BATCH_SIZE, StreamingConfig.normalizeBatchSize(-100));
        assertEquals(StreamingConfig.DEFAULT_BATCH_SIZE, StreamingConfig.normalizeBatchSize(Integer.MIN_VALUE));
    }

    @Test
    @DisplayName("normalizeBatchSize clamps to minimum")
    void normalizeBatchSizeClampsToMinimum() {
        assertEquals(StreamingConfig.MIN_BATCH_SIZE, StreamingConfig.normalizeBatchSize(1));
        assertEquals(StreamingConfig.MIN_BATCH_SIZE, StreamingConfig.normalizeBatchSize(100));
        assertEquals(StreamingConfig.MIN_BATCH_SIZE, StreamingConfig.normalizeBatchSize(1023));
    }

    @Test
    @DisplayName("normalizeBatchSize clamps to maximum")
    void normalizeBatchSizeClampsToMaximum() {
        assertEquals(StreamingConfig.MAX_BATCH_SIZE, StreamingConfig.normalizeBatchSize(65537));
        assertEquals(StreamingConfig.MAX_BATCH_SIZE, StreamingConfig.normalizeBatchSize(100000));
        assertEquals(StreamingConfig.MAX_BATCH_SIZE, StreamingConfig.normalizeBatchSize(Integer.MAX_VALUE));
    }

    @ParameterizedTest
    @CsvSource({
        "1024, 1024",   // At minimum
        "2048, 2048",   // Normal value
        "4096, 4096",   // Normal value
        "8192, 8192",   // Default
        "16384, 16384", // Larger
        "32768, 32768", // Larger
        "65536, 65536"  // At maximum
    })
    @DisplayName("normalizeBatchSize passes through valid values")
    void normalizeBatchSizePassesThroughValidValues(int input, int expected) {
        assertEquals(expected, StreamingConfig.normalizeBatchSize(input));
    }

    @Test
    @DisplayName("normalizeBatchSize boundary conditions")
    void normalizeBatchSizeBoundaryConditions() {
        // Exactly at min boundary
        assertEquals(StreamingConfig.MIN_BATCH_SIZE, StreamingConfig.normalizeBatchSize(StreamingConfig.MIN_BATCH_SIZE));
        // One below min
        assertEquals(StreamingConfig.MIN_BATCH_SIZE, StreamingConfig.normalizeBatchSize(StreamingConfig.MIN_BATCH_SIZE - 1));
        // One above min
        assertEquals(StreamingConfig.MIN_BATCH_SIZE + 1, StreamingConfig.normalizeBatchSize(StreamingConfig.MIN_BATCH_SIZE + 1));

        // Exactly at max boundary
        assertEquals(StreamingConfig.MAX_BATCH_SIZE, StreamingConfig.normalizeBatchSize(StreamingConfig.MAX_BATCH_SIZE));
        // One below max
        assertEquals(StreamingConfig.MAX_BATCH_SIZE - 1, StreamingConfig.normalizeBatchSize(StreamingConfig.MAX_BATCH_SIZE - 1));
        // One above max
        assertEquals(StreamingConfig.MAX_BATCH_SIZE, StreamingConfig.normalizeBatchSize(StreamingConfig.MAX_BATCH_SIZE + 1));
    }
}
