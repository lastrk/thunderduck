package com.thunderduck.schema;

/**
 * Exception thrown when schema inference fails.
 */
public class SchemaInferenceException extends RuntimeException {

    public SchemaInferenceException(String message) {
        super(message);
    }

    public SchemaInferenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
