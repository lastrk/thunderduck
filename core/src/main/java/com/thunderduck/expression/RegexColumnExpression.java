package com.thunderduck.expression;

import com.thunderduck.types.DataType;
import com.thunderduck.types.StringType;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression for regex-based column selection.
 *
 * <p>Represents Spark's {@code df.colRegex()} which selects columns matching a regex pattern.
 * This is translated to DuckDB's {@code COLUMNS('pattern')} expression.
 *
 * <p>Examples:
 * <pre>
 *   df.colRegex("`col_.*`")      -- Select columns starting with "col_"
 *   df.colRegex("`^test_\\d+`")  -- Select columns like test_1, test_2, etc.
 * </pre>
 *
 * <p>DuckDB translation:
 * <pre>
 *   COLUMNS('col_.*')       -- Matches columns by regex
 *   t.COLUMNS('pattern')    -- With table qualifier
 * </pre>
 *
 * <p>Note: Spark colRegex uses backticks around the pattern, which are stripped
 * during conversion. DuckDB COLUMNS() uses RE2 regex syntax.
 */
public final class RegexColumnExpression implements Expression {

    private final String pattern;
    private final Optional<String> tableAlias;

    /**
     * Creates a RegexColumnExpression with a pattern.
     *
     * @param pattern the regex pattern (without backticks)
     */
    public RegexColumnExpression(String pattern) {
        this(pattern, null);
    }

    /**
     * Creates a RegexColumnExpression with a pattern and optional table qualifier.
     *
     * @param pattern the regex pattern (without backticks)
     * @param tableAlias the optional table alias/qualifier
     */
    public RegexColumnExpression(String pattern, String tableAlias) {
        this.pattern = Objects.requireNonNull(pattern, "pattern must not be null");
        this.tableAlias = Optional.ofNullable(tableAlias);
    }

    /**
     * Returns the regex pattern.
     *
     * @return the pattern
     */
    public String pattern() {
        return pattern;
    }

    /**
     * Returns the optional table alias.
     *
     * @return the table alias, if specified
     */
    public Optional<String> tableAlias() {
        return tableAlias;
    }

    @Override
    public DataType dataType() {
        // COLUMNS() returns multiple columns, but for type purposes we use StringType
        return StringType.get();
    }

    @Override
    public boolean nullable() {
        // Columns themselves may be nullable
        return true;
    }

    /**
     * Converts to DuckDB COLUMNS() syntax.
     *
     * <p>Examples:
     * <pre>
     *   COLUMNS('col_.*')
     *   t.COLUMNS('^test_')
     * </pre>
     *
     * @return the SQL string
     */
    @Override
    public String toSQL() {
        // Escape single quotes in pattern for SQL string
        String escapedPattern = pattern.replace("'", "''");

        if (tableAlias.isPresent()) {
            return String.format("%s.COLUMNS('%s')", tableAlias.get(), escapedPattern);
        } else {
            return String.format("COLUMNS('%s')", escapedPattern);
        }
    }

    @Override
    public String toString() {
        return toSQL();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof RegexColumnExpression)) return false;
        RegexColumnExpression that = (RegexColumnExpression) obj;
        return Objects.equals(pattern, that.pattern) &&
               Objects.equals(tableAlias, that.tableAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, tableAlias);
    }

    // ==================== Factory Methods ====================

    /**
     * Creates a RegexColumnExpression from a pattern.
     *
     * @param pattern the regex pattern
     * @return the expression
     */
    public static RegexColumnExpression of(String pattern) {
        return new RegexColumnExpression(pattern);
    }

    /**
     * Creates a RegexColumnExpression with a table qualifier.
     *
     * @param pattern the regex pattern
     * @param tableAlias the table alias
     * @return the expression
     */
    public static RegexColumnExpression of(String pattern, String tableAlias) {
        return new RegexColumnExpression(pattern, tableAlias);
    }

    /**
     * Strips backticks from a Spark regex column pattern.
     *
     * <p>Spark's colRegex uses backticks: {@code `pattern`}
     * DuckDB's COLUMNS uses just the pattern: {@code 'pattern'}
     *
     * @param sparkPattern the Spark pattern (with or without backticks)
     * @return the pattern without backticks
     */
    public static String stripBackticks(String sparkPattern) {
        if (sparkPattern == null) {
            return null;
        }
        String p = sparkPattern;
        if (p.startsWith("`")) {
            p = p.substring(1);
        }
        if (p.endsWith("`")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }
}
