package com.catalyst2sql.generator;

/**
 * Utilities for safely quoting SQL identifiers and literals.
 *
 * <p>This class provides methods to properly escape and quote SQL identifiers,
 * string literals, and file paths to prevent SQL injection vulnerabilities.
 *
 * <p>SQL injection can occur when user-provided strings are concatenated
 * directly into SQL without proper escaping. This class ensures all special
 * characters are properly escaped according to SQL standards.
 *
 * <p>Example usage:
 * <pre>
 *   String tableName = SQLQuoting.quoteIdentifier("users");
 *   // Result: "users"
 *
 *   String userInput = SQLQuoting.quoteLiteral("O'Reilly");
 *   // Result: 'O''Reilly'
 *
 *   String path = SQLQuoting.quoteFilePath("/data/file.parquet");
 *   // Result: '/data/file.parquet'
 * </pre>
 *
 * @see SQLGenerator
 */
public class SQLQuoting {

    /**
     * Quotes an identifier (table name, column name, alias).
     *
     * <p>Uses double quotes and escapes internal quotes according to SQL standard.
     * Identifiers that need quoting include:
     * <ul>
     *   <li>Reserved words (SELECT, FROM, WHERE, etc.)</li>
     *   <li>Names with special characters</li>
     *   <li>Case-sensitive names</li>
     *   <li>Names starting with digits</li>
     * </ul>
     *
     * @param identifier the identifier to quote
     * @return quoted identifier safe for SQL
     * @throws IllegalArgumentException if identifier is null or empty
     */
    public static String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }

        // Escape double quotes by doubling them (SQL standard)
        String escaped = identifier.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    /**
     * Quotes a string literal value.
     *
     * <p>Uses single quotes and escapes internal quotes according to SQL standard.
     * Returns NULL (without quotes) if the value is null.
     *
     * @param value the string value to quote
     * @return quoted literal safe for SQL, or NULL if value is null
     */
    public static String quoteLiteral(String value) {
        if (value == null) {
            return "NULL";
        }

        // Escape single quotes by doubling them (SQL standard)
        String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }

    /**
     * Quotes a file path for COPY/read_parquet statements.
     *
     * <p>Validates that the path doesn't contain SQL injection attempts
     * such as semicolons or SQL comment markers.
     *
     * @param path the file path to quote
     * @return quoted path safe for SQL
     * @throws IllegalArgumentException if path is null, empty, or contains
     *         suspicious characters that could indicate SQL injection
     */
    public static String quoteFilePath(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        // Detect SQL injection attempts
        if (path.contains(";") || path.contains("--") ||
            path.contains("/*") || path.contains("*/")) {
            throw new IllegalArgumentException(
                "Invalid characters in file path (possible SQL injection): " + path);
        }

        // Escape single quotes
        String escaped = path.replace("'", "''");
        return "'" + escaped + "'";
    }

    /**
     * Quotes a table name for use in SQL.
     *
     * <p>Validates that the table name doesn't contain dangerous patterns
     * like semicolons or comment markers that could indicate SQL injection.
     *
     * @param tableName the table name to quote
     * @return quoted table name safe for SQL
     * @throws IllegalArgumentException if table name is null or contains invalid characters
     */
    public static String quoteTableName(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }

        // Check for suspicious patterns in table names
        if (tableName.contains(";") || tableName.contains("--")) {
            throw new IllegalArgumentException(
                "Table name contains invalid characters: " + tableName);
        }

        return quoteIdentifier(tableName);
    }

    /**
     * Checks if a string contains potential SQL injection patterns.
     *
     * <p>This is a utility method for additional validation beyond quoting.
     * It's exposed for use by other components that need to validate input.
     *
     * @param input the string to check
     * @return true if the string appears safe, false if it contains injection patterns
     */
    public static boolean isSafe(String input) {
        if (input == null) {
            return true; // null is safe (will be quoted as NULL)
        }

        // Check for common SQL injection patterns
        String lower = input.toLowerCase();
        return !input.contains(";") &&
               !input.contains("--") &&
               !input.contains("/*") &&
               !input.contains("*/") &&
               !lower.contains(" drop ") &&
               !lower.contains(" delete ") &&
               !lower.contains(" insert ") &&
               !lower.contains(" update ") &&
               !lower.contains(" create ") &&
               !lower.contains(" alter ");
    }

    /**
     * Validates that a string is safe to use as an identifier.
     *
     * <p>Only allows identifiers that:
     * <ul>
     *   <li>Start with a letter or underscore</li>
     *   <li>Contain only letters, digits, and underscores</li>
     * </ul>
     *
     * <p>This validation is stricter than SQL standard but provides
     * better protection against injection attacks.
     *
     * @param identifier the identifier to validate
     * @throws IllegalArgumentException if identifier is null, empty, or
     *         contains invalid characters
     */
    public static void validateIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }

        if (!identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException(
                "Invalid identifier (must start with letter/underscore, " +
                "contain only alphanumeric/underscore): " + identifier);
        }
    }

    /**
     * Quotes an identifier only if needed.
     *
     * <p>Checks if the identifier needs quoting based on SQL naming rules.
     * Simple identifiers that follow standard naming conventions don't need
     * quotes, making the generated SQL more readable.
     *
     * @param identifier the identifier to conditionally quote
     * @return the identifier, quoted if necessary
     * @throws IllegalArgumentException if identifier is null or empty
     */
    public static String quoteIdentifierIfNeeded(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }

        // Check if identifier needs quoting
        if (needsQuoting(identifier)) {
            return quoteIdentifier(identifier);
        }

        return identifier;
    }

    /**
     * Checks if an identifier needs quoting.
     *
     * @param identifier the identifier to check
     * @return true if quoting is needed
     */
    private static boolean needsQuoting(String identifier) {
        if (identifier.isEmpty()) {
            return true;
        }

        // Check if it starts with a letter or underscore
        char first = identifier.charAt(0);
        if (!Character.isLetter(first) && first != '_') {
            return true;
        }

        // Check if it contains only letters, digits, and underscores
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') {
                return true;
            }
        }

        // Check if it's a reserved word
        if (isReservedWord(identifier.toUpperCase())) {
            return true;
        }

        return false;
    }

    /**
     * Checks if a word is a SQL reserved word.
     *
     * @param word the word to check (should be uppercase)
     * @return true if reserved
     */
    private static boolean isReservedWord(String word) {
        // Common SQL reserved words
        switch (word) {
            case "SELECT":
            case "FROM":
            case "WHERE":
            case "GROUP":
            case "BY":
            case "ORDER":
            case "HAVING":
            case "JOIN":
            case "LEFT":
            case "RIGHT":
            case "INNER":
            case "OUTER":
            case "FULL":
            case "CROSS":
            case "ON":
            case "USING":
            case "AS":
            case "AND":
            case "OR":
            case "NOT":
            case "IN":
            case "EXISTS":
            case "CASE":
            case "WHEN":
            case "THEN":
            case "ELSE":
            case "END":
            case "NULL":
            case "TRUE":
            case "FALSE":
            case "UNION":
            case "INTERSECT":
            case "EXCEPT":
            case "LIMIT":
            case "OFFSET":
            case "ALL":
            case "DISTINCT":
            case "IS":
            case "BETWEEN":
            case "LIKE":
            case "ILIKE":
            case "ASC":
            case "DESC":
            case "NULLS":
            case "FIRST":
            case "LAST":
                return true;
            default:
                return false;
        }
    }
}
