package com.catalyst2sql.exception;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Exception thrown when query execution fails.
 *
 * <p>This exception wraps SQLException with query context and provides
 * user-friendly error messages for common database errors.
 *
 * <p>Common causes:
 * <ul>
 *   <li>Column not found errors</li>
 *   <li>Data type conversion errors</li>
 *   <li>Memory limit exceeded</li>
 *   <li>Invalid SQL syntax</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   try {
 *       VectorSchemaRoot result = executor.executeQuery(sql);
 *   } catch (QueryExecutionException e) {
 *       System.err.println(e.getUserMessage());
 *       System.err.println("Failed SQL: " + e.getFailedSQL());
 *   }
 * </pre>
 *
 * @see com.catalyst2sql.runtime.QueryExecutor
 */
public class QueryExecutionException extends RuntimeException {

    private final String failedSQL;

    /**
     * Creates a query execution exception.
     *
     * @param message the error message
     * @param sql the SQL that failed to execute
     */
    public QueryExecutionException(String message, String sql) {
        super(message);
        this.failedSQL = sql;
    }

    /**
     * Creates a query execution exception with a cause.
     *
     * @param message the error message
     * @param cause the underlying cause (typically SQLException)
     * @param sql the SQL that failed to execute
     */
    public QueryExecutionException(String message, Throwable cause, String sql) {
        super(message, cause);
        this.failedSQL = sql;
    }

    /**
     * Returns the SQL statement that failed to execute.
     *
     * @return the failed SQL, or null if not available
     */
    public String getFailedSQL() {
        return failedSQL;
    }

    /**
     * Returns a user-friendly error message.
     *
     * <p>Translates technical database error messages into actionable
     * guidance for users. Detects common error patterns and provides
     * specific suggestions.
     *
     * @return user-friendly error message
     */
    public String getUserMessage() {
        String message = getMessage();

        if (message == null) {
            return "Query execution failed. Check SQL syntax and data types.";
        }

        // Translate common DuckDB error messages
        if (message.contains("Binder Error") && message.contains("not found")) {
            return translateColumnNotFound(message);
        }

        if (message.contains("Conversion Error")) {
            return translateConversionError(message);
        }

        if (message.contains("Out of Memory Error")) {
            return "Query requires more memory than available. " +
                   "Try reducing data size, adding filters, or increasing memory limit.";
        }

        if (message.contains("Syntax Error") || message.contains("Parser Error")) {
            return translateSyntaxError(message);
        }

        if (message.contains("Catalog Error")) {
            return translateCatalogError(message);
        }

        if (message.contains("IO Error")) {
            return translateIOError(message);
        }

        // Default message
        return "Query execution failed: " + message;
    }

    /**
     * Translates column not found errors.
     */
    private String translateColumnNotFound(String message) {
        // Extract column name
        Pattern pattern = Pattern.compile("column \"([^\"]+)\" not found");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            String missingColumn = matcher.group(1);

            // Extract candidates if available
            pattern = Pattern.compile("Candidate Bindings: (.+)");
            matcher = pattern.matcher(message);
            if (matcher.find()) {
                String candidates = matcher.group(1);
                return "Column '" + missingColumn + "' not found. Available columns: " + candidates;
            }

            return "Column '" + missingColumn + "' not found in table. " +
                   "Check column name spelling and case sensitivity.";
        }

        return "Column not found: " + message;
    }

    /**
     * Translates data type conversion errors.
     */
    private String translateConversionError(String message) {
        if (message.contains("Could not convert string")) {
            Pattern pattern = Pattern.compile("Could not convert string \"([^\"]+)\" to '([^']+)'");
            Matcher matcher = pattern.matcher(message);
            if (matcher.find()) {
                String value = matcher.group(1);
                String targetType = matcher.group(2);
                return "Cannot convert value '" + value + "' to type " + targetType + ". " +
                       "Check data types match expected types.";
            }
        }

        return "Data type mismatch in query. " +
               "Check that column types match expected types in operations.";
    }

    /**
     * Translates SQL syntax errors.
     */
    private String translateSyntaxError(String message) {
        Pattern pattern = Pattern.compile("syntax error at or near \"([^\"]+)\"");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            String token = matcher.group(1);
            return "SQL syntax error near '" + token + "'. " +
                   "Check SQL statement for typos and proper syntax.";
        }

        return "SQL syntax error. Check statement for typos and proper syntax.";
    }

    /**
     * Translates catalog (table/file) errors.
     */
    private String translateCatalogError(String message) {
        if (message.contains("Table") && message.contains("does not exist")) {
            return "Table or file not found. " +
                   "Check that file path is correct and file exists.";
        }

        return "Table or file error: " + message;
    }

    /**
     * Translates I/O errors.
     */
    private String translateIOError(String message) {
        if (message.contains("No such file")) {
            return "File not found. Check that file path is correct and file exists.";
        }

        if (message.contains("Permission denied")) {
            return "Permission denied accessing file. " +
                   "Check file permissions and access rights.";
        }

        return "I/O error reading file: " + message;
    }

    /**
     * Returns a detailed technical message for debugging.
     *
     * @return technical error message with full context
     */
    public String getTechnicalMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Query Execution Failed\n");
        sb.append("Error: ").append(getMessage()).append("\n");

        if (failedSQL != null) {
            sb.append("Failed SQL:\n").append(failedSQL).append("\n");
        }

        if (getCause() != null) {
            sb.append("Cause: ").append(getCause().getClass().getName()).append("\n");
            sb.append("Cause Message: ").append(getCause().getMessage()).append("\n");
        }

        return sb.toString();
    }
}
