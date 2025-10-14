package com.catalyst2sql.exception;

import com.catalyst2sql.logical.LogicalPlan;

/**
 * Exception thrown when SQL generation fails.
 *
 * <p>This exception provides context about the failed logical plan for debugging
 * and user-friendly error messages that help users understand what went wrong.
 *
 * <p>Common causes:
 * <ul>
 *   <li>Unsupported logical plan operators</li>
 *   <li>Invalid expressions or data types</li>
 *   <li>Incompatible operation combinations</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   try {
 *       String sql = generator.generate(plan);
 *   } catch (SQLGenerationException e) {
 *       System.err.println(e.getUserMessage());
 *       System.err.println("Failed plan: " + e.getFailedPlan());
 *   }
 * </pre>
 *
 * @see com.catalyst2sql.generator.SQLGenerator
 */
public class SQLGenerationException extends RuntimeException {

    private final LogicalPlan failedPlan;

    /**
     * Creates a SQL generation exception.
     *
     * @param message the error message
     * @param plan the logical plan that failed to generate SQL
     */
    public SQLGenerationException(String message, LogicalPlan plan) {
        super(message + " (plan type: " + (plan != null ? plan.getClass().getSimpleName() : "null") + ")");
        this.failedPlan = plan;
    }

    /**
     * Creates a SQL generation exception with a cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     * @param plan the logical plan that failed to generate SQL
     */
    public SQLGenerationException(String message, Throwable cause, LogicalPlan plan) {
        super(message + " (plan type: " + (plan != null ? plan.getClass().getSimpleName() : "null") + ")", cause);
        this.failedPlan = plan;
    }

    /**
     * Returns the logical plan that failed to generate SQL.
     *
     * @return the failed plan, or null if not available
     */
    public LogicalPlan getFailedPlan() {
        return failedPlan;
    }

    /**
     * Returns a user-friendly error message.
     *
     * <p>Translates technical error messages into actionable guidance for users.
     *
     * @return user-friendly error message
     */
    public String getUserMessage() {
        String planType = failedPlan != null ? failedPlan.getClass().getSimpleName() : "unknown";

        // Provide specific guidance based on plan type
        switch (planType) {
            case "Aggregate":
                return "Failed to generate SQL for aggregation query. " +
                       "Aggregation support is currently limited. " +
                       "Please simplify your query or contact support.";

            case "Join":
                return "Failed to generate SQL for join query. " +
                       "Join support is currently limited. " +
                       "Please simplify your query or contact support.";

            case "Union":
                return "Failed to generate SQL for union query. " +
                       "Union support is currently limited. " +
                       "Please simplify your query or contact support.";

            case "InMemoryRelation":
            case "LocalRelation":
                return "Failed to generate SQL for in-memory data. " +
                       "Direct translation of in-memory datasets is not supported. " +
                       "Please write data to Parquet first.";

            default:
                return "Failed to generate SQL for query. " +
                       "Unsupported operation: " + planType + ". " +
                       "Please simplify your query or contact support.";
        }
    }

    /**
     * Returns a detailed technical message for debugging.
     *
     * @return technical error message with full context
     */
    public String getTechnicalMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("SQL Generation Failed\n");
        sb.append("Error: ").append(getMessage()).append("\n");

        if (failedPlan != null) {
            sb.append("Failed Plan Type: ").append(failedPlan.getClass().getName()).append("\n");
            sb.append("Plan String: ").append(failedPlan.toString()).append("\n");
        }

        if (getCause() != null) {
            sb.append("Cause: ").append(getCause().getMessage()).append("\n");
        }

        return sb.toString();
    }
}
