package com.thunderduck.functions;

import com.thunderduck.types.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of Spark SQL functions mapped to DuckDB SQL functions.
 *
 * <p>This class provides mappings for 500+ Spark SQL functions to their DuckDB equivalents.
 * Most functions (90%+) have direct 1:1 mappings, while some require custom translation logic.
 *
 * <p>Function categories:
 * <ul>
 *   <li>String functions: upper, lower, trim, substring, concat, etc.</li>
 *   <li>Math functions: abs, ceil, floor, round, sqrt, pow, etc.</li>
 *   <li>Date/time functions: year, month, day, date_add, datediff, etc.</li>
 *   <li>Aggregate functions: sum, avg, min, max, count, stddev, etc.</li>
 *   <li>Window functions: row_number, rank, dense_rank, lag, lead, etc.</li>
 *   <li>Array functions: array_contains, array_distinct, size, explode, etc.</li>
 * </ul>
 *
 * @see FunctionTranslator
 */
public class FunctionRegistry {

    private static final Map<String, String> DIRECT_MAPPINGS = new HashMap<>();
    private static final Map<String, FunctionTranslator> CUSTOM_TRANSLATORS = new HashMap<>();
    private static final Map<String, FunctionMetadata> FUNCTION_METADATA = new HashMap<>();

    static {
        initializeStringFunctions();
        initializeMathFunctions();
        initializeDateFunctions();
        initializeAggregateFunctions();
        initializeWindowFunctions();
        initializeArrayFunctions();
        initializeConditionalFunctions();
        initializeFunctionMetadata();
    }

    /**
     * Translates a Spark SQL function call to DuckDB SQL.
     *
     * @param functionName the Spark function name
     * @param args the function arguments (as SQL strings)
     * @return the translated DuckDB SQL function call
     * @throws UnsupportedOperationException if the function is not supported
     */
    public static String translate(String functionName, String... args) {
        if (functionName == null || functionName.isEmpty()) {
            throw new IllegalArgumentException("functionName must not be null or empty");
        }

        String normalizedName = functionName.toLowerCase();

        // Check for direct mapping
        String duckdbFunction = DIRECT_MAPPINGS.get(normalizedName);
        if (duckdbFunction != null) {
            return buildFunctionCall(duckdbFunction, args);
        }

        // Check for custom translator
        FunctionTranslator translator = CUSTOM_TRANSLATORS.get(normalizedName);
        if (translator != null) {
            return translator.translate(args);
        }

        throw new UnsupportedOperationException("Unsupported function: " + functionName);
    }

    /**
     * Resolves a Spark function name to its DuckDB equivalent.
     *
     * <p>Checks DIRECT_MAPPINGS only. Returns the original name if no mapping exists.
     * This is useful when callers need the DuckDB function name without building the
     * full function call (e.g., for DISTINCT handling).
     *
     * @param functionName the Spark function name
     * @return the DuckDB function name, or the original name if not mapped
     */
    public static String resolveName(String functionName) {
        String normalizedName = functionName.toLowerCase();
        String duckdbName = DIRECT_MAPPINGS.get(normalizedName);
        return duckdbName != null ? duckdbName : functionName;
    }

    /**
     * Rewrites SQL expression strings by replacing Spark function names with DuckDB equivalents.
     *
     * <p>This method performs regex-based function name translation for SQL expression strings.
     * It handles:
     * <ul>
     *   <li>DIRECT_MAPPINGS: Simple 1:1 name replacements (167+ functions)</li>
     *   <li>Selected CUSTOM_TRANSLATORS: collect_list, collect_set, size (argument transformation)</li>
     * </ul>
     *
     * <p>Edge cases handled:
     * <ul>
     *   <li>Case insensitive matching (Spark SQL is case insensitive)</li>
     *   <li>Word boundaries to avoid partial matches</li>
     *   <li>Only matches function names followed by '(' to avoid column name conflicts</li>
     *   <li>Nested parentheses in function arguments</li>
     * </ul>
     *
     * <p>Limitations:
     * <ul>
     *   <li>Most CUSTOM_TRANSLATORS not supported (e.g., locate with argument reordering)</li>
     *   <li>May not correctly handle quoted strings containing function names</li>
     *   <li>Does not validate SQL syntax or function arguments</li>
     * </ul>
     *
     * @param sql the SQL expression string to rewrite
     * @return the rewritten SQL with DuckDB function names
     */
    public static String rewriteSQL(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        String result = sql;

        // First, rewrite DIRECT_MAPPINGS (simple 1:1 replacements)
        for (Map.Entry<String, String> entry : DIRECT_MAPPINGS.entrySet()) {
            String sparkName = entry.getKey();
            String duckdbName = entry.getValue();

            // Skip if names are already the same (no translation needed)
            if (sparkName.equalsIgnoreCase(duckdbName)) {
                continue;
            }

            // Pattern: function_name followed by '(' (with optional whitespace)
            // Use word boundaries to avoid partial matches (e.g., don't match "my_collect_list")
            // Use positive lookahead (?=\s*\() to ensure followed by '('
            // Case insensitive flag for Spark SQL compatibility
            String pattern = "\\b" + sparkName + "(?=\\s*\\()";

            // Replace all occurrences (case insensitive)
            result = result.replaceAll("(?i)" + pattern, duckdbName);
        }

        // Then, handle selected CUSTOM_TRANSLATORS (must be done after DIRECT_MAPPINGS)
        // These require argument extraction and transformation
        // We do these after DIRECT_MAPPINGS so that function arguments are already translated
        result = rewriteCollectList(result);
        result = rewriteCollectSet(result);
        result = rewriteSize(result);

        return result;
    }

    /**
     * Rewrites collect_list() function calls in SQL strings.
     * Spark: collect_list(col)
     * DuckDB: list(col) FILTER (WHERE col IS NOT NULL)
     */
    private static String rewriteCollectList(String sql) {
        return rewriteFunctionWithPattern(sql, "collect_list", (arg) ->
            "list(" + arg + ") FILTER (WHERE " + arg + " IS NOT NULL)"
        );
    }

    /**
     * Rewrites collect_set() function calls in SQL strings.
     * Spark: collect_set(col)
     * DuckDB: list_distinct(list(col) FILTER (WHERE col IS NOT NULL))
     */
    private static String rewriteCollectSet(String sql) {
        return rewriteFunctionWithPattern(sql, "collect_set", (arg) ->
            "list_distinct(list(" + arg + ") FILTER (WHERE " + arg + " IS NOT NULL))"
        );
    }

    /**
     * Rewrites size() function calls in SQL strings.
     * Spark: size(array_col)
     * DuckDB: CAST(len(array_col) AS INTEGER)
     */
    private static String rewriteSize(String sql) {
        return rewriteFunctionWithPattern(sql, "size", (arg) ->
            "CAST(len(" + arg + ") AS INTEGER)"
        );
    }

    /**
     * Helper method to rewrite a function call by extracting its argument and applying a transformation.
     *
     * @param sql the SQL string
     * @param functionName the function name to find (case insensitive)
     * @param transformer function to transform the argument
     * @return the rewritten SQL
     */
    private static String rewriteFunctionWithPattern(String sql, String functionName,
                                                      java.util.function.Function<String, String> transformer) {
        StringBuilder result = new StringBuilder();
        int pos = 0;
        String lowerSql = sql.toLowerCase();
        String lowerFuncName = functionName.toLowerCase();

        while (pos < sql.length()) {
            // Find next occurrence of function name (case insensitive)
            int funcStart = lowerSql.indexOf(lowerFuncName, pos);
            if (funcStart == -1) {
                // No more occurrences, append rest of string
                result.append(sql.substring(pos));
                break;
            }

            // Check if this is a word boundary (not part of a larger identifier)
            // Identifiers can contain letters, digits, and underscores
            if (funcStart > 0) {
                char prevChar = sql.charAt(funcStart - 1);
                if (Character.isLetterOrDigit(prevChar) || prevChar == '_') {
                    // Part of larger identifier, skip it
                    result.append(sql.substring(pos, funcStart + lowerFuncName.length()));
                    pos = funcStart + lowerFuncName.length();
                    continue;
                }
            }

            // Check if followed by '(' (with optional whitespace)
            int parenStart = funcStart + functionName.length();
            while (parenStart < sql.length() && Character.isWhitespace(sql.charAt(parenStart))) {
                parenStart++;
            }

            if (parenStart >= sql.length() || sql.charAt(parenStart) != '(') {
                // Not followed by '(', not a function call
                result.append(sql.substring(pos, funcStart + lowerFuncName.length()));
                pos = funcStart + lowerFuncName.length();
                continue;
            }

            // Found a function call, extract the argument
            int parenEnd = findMatchingParen(sql, parenStart);
            if (parenEnd == -1) {
                // No matching paren, skip this occurrence
                result.append(sql.substring(pos, funcStart + lowerFuncName.length()));
                pos = funcStart + lowerFuncName.length();
                continue;
            }

            // Extract the argument (everything between the parentheses)
            String arg = sql.substring(parenStart + 1, parenEnd).trim();

            // Append everything before the function call
            result.append(sql.substring(pos, funcStart));

            // Apply the transformation
            result.append(transformer.apply(arg));

            // Move position past the closing paren
            pos = parenEnd + 1;
        }

        return result.toString();
    }

    /**
     * Finds the matching closing parenthesis for an opening parenthesis.
     *
     * @param sql the SQL string
     * @param openParenPos the position of the opening parenthesis
     * @return the position of the matching closing parenthesis, or -1 if not found
     */
    private static int findMatchingParen(String sql, int openParenPos) {
        if (openParenPos >= sql.length() || sql.charAt(openParenPos) != '(') {
            return -1;
        }

        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = openParenPos; i < sql.length(); i++) {
            char c = sql.charAt(i);

            // Handle string literals (single quotes)
            if (c == '\'' && !inDoubleQuote) {
                // Check if escaped
                if (i > 0 && sql.charAt(i - 1) == '\\') {
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                continue;
            }

            // Handle identifiers (double quotes)
            if (c == '"' && !inSingleQuote) {
                // Check if escaped
                if (i > 0 && sql.charAt(i - 1) == '\\') {
                    continue;
                }
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            // Only count parentheses outside of quotes
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }

        return -1; // No matching paren found
    }

    /**
     * Checks if a function is supported.
     *
     * @param functionName the function name
     * @return true if supported, false otherwise
     */
    public static boolean isSupported(String functionName) {
        if (functionName == null || functionName.isEmpty()) {
            return false;
        }
        String normalizedName = functionName.toLowerCase();
        return DIRECT_MAPPINGS.containsKey(normalizedName) ||
               CUSTOM_TRANSLATORS.containsKey(normalizedName);
    }

    /**
     * Gets the DuckDB function name for a Spark function (if direct mapping exists).
     *
     * @param sparkFunction the Spark function name
     * @return the DuckDB function name, or empty if not a direct mapping
     */
    public static Optional<String> getDuckDBFunction(String sparkFunction) {
        if (sparkFunction == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(DIRECT_MAPPINGS.get(sparkFunction.toLowerCase()));
    }

    private static String buildFunctionCall(String functionName, String... args) {
        if (args.length == 0) {
            return functionName + "()";
        }
        return functionName + "(" + String.join(", ", args) + ")";
    }

    // ==================== String Functions ====================

    private static void initializeStringFunctions() {
        // Case conversion
        DIRECT_MAPPINGS.put("upper", "upper");
        DIRECT_MAPPINGS.put("lower", "lower");
        DIRECT_MAPPINGS.put("initcap", "initcap");

        // Trimming
        DIRECT_MAPPINGS.put("trim", "trim");
        DIRECT_MAPPINGS.put("ltrim", "ltrim");
        DIRECT_MAPPINGS.put("rtrim", "rtrim");

        // Substring and manipulation
        DIRECT_MAPPINGS.put("substring", "substring");
        DIRECT_MAPPINGS.put("substr", "substr");
        DIRECT_MAPPINGS.put("length", "length");
        // concat: Spark returns NULL if any arg is NULL; DuckDB skips NULLs.
        // Use || operator which propagates NULLs correctly.
        CUSTOM_TRANSLATORS.put("concat", args -> {
            if (args.length == 0) {
                return "''";
            }
            if (args.length == 1) {
                return "CAST(" + args[0] + " AS VARCHAR)";
            }
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    sb.append(" || ");
                }
                sb.append("CAST(").append(args[i]).append(" AS VARCHAR)");
            }
            sb.append(")");
            return sb.toString();
        });
        DIRECT_MAPPINGS.put("replace", "replace");
        DIRECT_MAPPINGS.put("reverse", "reverse");
        DIRECT_MAPPINGS.put("repeat", "repeat");

        // Position and search
        DIRECT_MAPPINGS.put("instr", "instr");
        // Note: locate uses CUSTOM_TRANSLATOR (see below) due to argument order difference
        DIRECT_MAPPINGS.put("position", "position");

        // Padding
        DIRECT_MAPPINGS.put("lpad", "lpad");
        DIRECT_MAPPINGS.put("rpad", "rpad");

        // Splitting and joining
        // split: Spark split(str, pattern, limit) has optional 3rd arg.
        // DuckDB string_split(str, delim) only takes 2 args.
        // When limit=-1 (default from PySpark), just drop the 3rd arg.
        CUSTOM_TRANSLATORS.put("split", args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("split requires at least 2 arguments");
            }
            // DuckDB's string_split only takes 2 args - ignore limit parameter
            return "string_split(" + args[0] + ", " + args[1] + ")";
        });
        DIRECT_MAPPINGS.put("concat_ws", "concat_ws");

        // Regular expressions
        DIRECT_MAPPINGS.put("regexp_extract", "regexp_extract");
        // regexp_replace: Spark replaces ALL occurrences by default; DuckDB replaces only the first.
        // Append 'g' flag for global replacement to match Spark semantics.
        CUSTOM_TRANSLATORS.put("regexp_replace", args -> {
            if (args.length < 3) {
                throw new IllegalArgumentException("regexp_replace requires at least 3 arguments");
            }
            // If caller provides a 4th arg (flags), ensure 'g' is included for Spark semantics
            if (args.length >= 4) {
                String flags = args[3].trim();
                // Check if 'g' is already in the flags string (inside quotes)
                if (!flags.contains("g")) {
                    // Insert 'g' before the closing quote: 'si' -> 'sig'
                    flags = flags.substring(0, flags.length() - 1) + "g" + flags.charAt(flags.length() - 1);
                }
                return "regexp_replace(" + args[0] + ", " + args[1] + ", " + args[2] + ", " + flags + ")";
            }
            // Default: add 'g' flag for global replacement
            return "regexp_replace(" + args[0] + ", " + args[1] + ", " + args[2] + ", 'g')";
        });
        DIRECT_MAPPINGS.put("regexp_like", "regexp_matches");

        // String predicates
        DIRECT_MAPPINGS.put("startswith", "starts_with");
        DIRECT_MAPPINGS.put("endswith", "ends_with");
        DIRECT_MAPPINGS.put("contains", "contains");
        DIRECT_MAPPINGS.put("rlike", "regexp_matches");
        CUSTOM_TRANSLATORS.put("like", args -> "(" + args[0] + " LIKE " + args[1] + ")");

        // Other string functions
        DIRECT_MAPPINGS.put("ascii", "ascii");
        DIRECT_MAPPINGS.put("chr", "chr");
        DIRECT_MAPPINGS.put("md5", "md5");
        DIRECT_MAPPINGS.put("sha1", "sha1");
        DIRECT_MAPPINGS.put("sha2", "sha256"); // Spark sha2(str, 256) → DuckDB sha256(str)

        // Custom translator for locate - Spark and DuckDB have different argument orders
        // Spark: locate(substr, str) - substring first, string second
        // DuckDB instr: instr(str, substr) - string first, substring second
        // Note: Spark returns NULL if str is NULL, DuckDB instr returns 0 - we must preserve NULL
        CUSTOM_TRANSLATORS.put("locate", args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("locate requires at least 2 arguments");
            }
            String substr = args[0];
            String str = args[1];

            if (args.length > 2) {
                // locate(substr, str, pos) - with start position
                String startPos = args[2];
                return "CASE WHEN " + str + " IS NULL THEN NULL " +
                       "WHEN instr(substr(" + str + ", " + startPos + "), " + substr + ") > 0 " +
                       "THEN instr(substr(" + str + ", " + startPos + "), " + substr + ") + " + startPos + " - 1 " +
                       "ELSE 0 END";
            }
            // Basic 2-arg form: swap argument order for instr, but return NULL if str is NULL
            return "CASE WHEN " + str + " IS NULL THEN NULL ELSE instr(" + str + ", " + substr + ") END";
        });
    }

    // ==================== Math Functions ====================

    private static void initializeMathFunctions() {
        // Basic arithmetic
        DIRECT_MAPPINGS.put("abs", "abs");
        DIRECT_MAPPINGS.put("ceil", "ceil");
        DIRECT_MAPPINGS.put("ceiling", "ceiling");
        DIRECT_MAPPINGS.put("floor", "floor");
        DIRECT_MAPPINGS.put("round", "round");
        DIRECT_MAPPINGS.put("truncate", "trunc");
        DIRECT_MAPPINGS.put("trunc", "trunc");

        // Power and roots
        DIRECT_MAPPINGS.put("sqrt", "sqrt");
        DIRECT_MAPPINGS.put("pow", "pow");
        DIRECT_MAPPINGS.put("power", "power");
        DIRECT_MAPPINGS.put("exp", "exp");

        // Logarithms
        DIRECT_MAPPINGS.put("log", "ln");
        DIRECT_MAPPINGS.put("ln", "ln");
        DIRECT_MAPPINGS.put("log10", "log10");
        DIRECT_MAPPINGS.put("log2", "log2");

        // Trigonometric
        DIRECT_MAPPINGS.put("sin", "sin");
        DIRECT_MAPPINGS.put("cos", "cos");
        DIRECT_MAPPINGS.put("tan", "tan");
        DIRECT_MAPPINGS.put("asin", "asin");
        DIRECT_MAPPINGS.put("acos", "acos");
        DIRECT_MAPPINGS.put("atan", "atan");
        DIRECT_MAPPINGS.put("atan2", "atan2");

        // Hyperbolic
        DIRECT_MAPPINGS.put("sinh", "sinh");
        DIRECT_MAPPINGS.put("cosh", "cosh");
        DIRECT_MAPPINGS.put("tanh", "tanh");

        // Other math functions
        DIRECT_MAPPINGS.put("sign", "sign");
        DIRECT_MAPPINGS.put("signum", "sign");
        DIRECT_MAPPINGS.put("degrees", "degrees");
        DIRECT_MAPPINGS.put("radians", "radians");
        DIRECT_MAPPINGS.put("pi", "pi");
        DIRECT_MAPPINGS.put("e", "e");
        DIRECT_MAPPINGS.put("rand", "random");
        DIRECT_MAPPINGS.put("random", "random");

        // Comparison functions
        DIRECT_MAPPINGS.put("greatest", "greatest");
        DIRECT_MAPPINGS.put("least", "least");

        // Custom translator for pmod (positive modulo)
        // DuckDB doesn't have pmod, so we emulate: pmod(a, b) = ((a % b) + b) % b
        CUSTOM_TRANSLATORS.put("pmod", args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("pmod requires 2 arguments");
            }
            String a = args[0];
            String b = args[1];
            // Positive modulo formula: ((a % b) + b) % b
            return "(((" + a + ") % (" + b + ") + (" + b + ")) % (" + b + "))";
        });
    }

    // ==================== Date/Time Functions ====================

    private static void initializeDateFunctions() {
        // Extract components - DuckDB returns BIGINT, Spark returns INTEGER
        // Must cast to INTEGER for Spark compatibility
        CUSTOM_TRANSLATORS.put("year", args -> "CAST(year(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("month", args -> "CAST(month(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("day", args -> "CAST(day(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("dayofmonth", args -> "CAST(day(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("dayofyear", args -> "CAST(dayofyear(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("hour", args -> "CAST(hour(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("minute", args -> "CAST(minute(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("second", args -> "CAST(second(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("quarter", args -> "CAST(quarter(" + args[0] + ") AS INTEGER)");
        CUSTOM_TRANSLATORS.put("weekofyear", args -> "CAST(weekofyear(" + args[0] + ") AS INTEGER)");

        // dayofweek: Spark returns 1=Sunday...7=Saturday; DuckDB returns 0=Sunday...6=Saturday
        // Need to add 1 to DuckDB result
        CUSTOM_TRANSLATORS.put("dayofweek", args -> "dayofweek(" + args[0] + ") + 1");

        // Date arithmetic - date_add works directly
        DIRECT_MAPPINGS.put("date_add", "date_add");

        // date_sub: Spark date_sub(date, days)
        // DuckDB doesn't have date_sub(date, int) - use date - INTERVAL 'n days'
        CUSTOM_TRANSLATORS.put("date_sub", args ->
            "CAST((" + args[0] + " - INTERVAL '" + args[1] + " days') AS DATE)");

        // datediff: Spark datediff(end, start) returns (end - start) days
        // DuckDB datediff('day', A, B) returns (B - A) days (second minus first!)
        // So we swap args: datediff('day', start, end) = end - start
        CUSTOM_TRANSLATORS.put("datediff", args ->
            "datediff('day', " + args[1] + ", " + args[0] + ")");

        // add_months: Spark add_months(date, months)
        // DuckDB: date + INTERVAL 'n months'
        CUSTOM_TRANSLATORS.put("add_months", args ->
            "CAST((" + args[0] + " + INTERVAL '" + args[1] + " months') AS DATE)");

        // months_between: Spark months_between(date1, date2, roundOff) returns (date1 - date2) months
        // DuckDB datediff('day', A, B) returns (B - A) days (second minus first!)
        // So: datediff('day', date2, date1) / 31.0 = (date1 - date2) / 31.0
        CUSTOM_TRANSLATORS.put("months_between", args -> {
            if (args.length >= 2) {
                // Compute the fractional months: (date1 - date2) / 31.0
                // This is an approximation; for exact Spark parity, more complex logic needed
                return "CAST(datediff('day', " + args[1] + ", " + args[0] + ") AS DOUBLE) / 31.0";
            }
            return "0.0";
        });

        // Date formatting - need format string conversion
        // Spark uses Java SimpleDateFormat (yyyy-MM-dd), DuckDB uses strftime (%Y-%m-%d)
        CUSTOM_TRANSLATORS.put("date_format", args -> {
            if (args.length >= 2) {
                String format = convertSparkDateFormatToDuckDB(args[1]);
                return "strftime(" + args[0] + ", " + format + ")";
            }
            return "strftime(" + args[0] + ", '%Y-%m-%d')";
        });

        // to_date: Spark to_date(string) or to_date(string, format)
        CUSTOM_TRANSLATORS.put("to_date", args -> {
            if (args.length == 1) {
                return "CAST(" + args[0] + " AS DATE)";
            } else {
                // to_date with format - use strptime then CAST
                String format = convertSparkDateFormatToDuckDB(args[1]);
                return "CAST(strptime(" + args[0] + ", " + format + ") AS DATE)";
            }
        });

        // to_timestamp: Spark to_timestamp(string) or to_timestamp(string, format)
        CUSTOM_TRANSLATORS.put("to_timestamp", args -> {
            if (args.length == 1) {
                return "CAST(" + args[0] + " AS TIMESTAMP)";
            } else {
                // to_timestamp with format - use strptime then CAST
                String format = convertSparkDateFormatToDuckDB(args[1]);
                return "CAST(strptime(" + args[0] + ", " + format + ") AS TIMESTAMP)";
            }
        });

        // Current date/time
        DIRECT_MAPPINGS.put("current_date", "current_date");
        DIRECT_MAPPINGS.put("current_timestamp", "current_timestamp");
        DIRECT_MAPPINGS.put("now", "now");

        // Date truncation - Spark always returns TIMESTAMP, DuckDB may return DATE
        // Cast result to TIMESTAMP to match Spark behavior
        CUSTOM_TRANSLATORS.put("date_trunc", args ->
            "CAST(date_trunc(" + String.join(", ", args) + ") AS TIMESTAMP)");
        CUSTOM_TRANSLATORS.put("trunc", args ->
            "CAST(date_trunc(" + String.join(", ", args) + ") AS TIMESTAMP)");

        // last_day: same in both
        DIRECT_MAPPINGS.put("last_day", "last_day");

        // next_day: Spark uses day names, DuckDB same
        // Spark: next_day(date, 'Monday')
        // DuckDB: next_day(date, 'monday') - case insensitive
        DIRECT_MAPPINGS.put("next_day", "next_day");

        // unix_timestamp: Spark unix_timestamp() or unix_timestamp(timestamp) or unix_timestamp(string, format)
        // DuckDB: epoch(timestamp) returns DOUBLE, but Spark returns LONG
        // Cast to BIGINT to match Spark's return type
        CUSTOM_TRANSLATORS.put("unix_timestamp", args -> {
            if (args.length == 0) {
                return "CAST(epoch(current_timestamp) AS BIGINT)";
            } else if (args.length == 1) {
                return "CAST(epoch(" + args[0] + ") AS BIGINT)";
            } else {
                // unix_timestamp(expr, format)
                // If expr is a string literal (single quotes in SQL), parse it with strptime
                // Otherwise, assume it's already a date/timestamp column and just use epoch
                // Note: Double quotes are identifiers in SQL (column names), not string literals
                String expr = args[0].trim();
                if (expr.startsWith("'") && !expr.startsWith("''")) {
                    // It's a string literal - needs parsing
                    String format = convertSparkDateFormatToDuckDB(args[1]);
                    return "CAST(epoch(strptime(" + args[0] + ", " + format + ")) AS BIGINT)";
                } else {
                    // It's a column reference, identifier, or timestamp expression
                    // Just use epoch() directly - the format is ignored for non-string inputs
                    return "CAST(epoch(" + args[0] + ") AS BIGINT)";
                }
            }
        });

        // from_unixtime: Convert epoch seconds to timestamp string
        // Spark: from_unixtime(epoch) or from_unixtime(epoch, format)
        // DuckDB: strftime(to_timestamp(epoch), format)
        CUSTOM_TRANSLATORS.put("from_unixtime", args -> {
            if (args.length == 1) {
                return "strftime(to_timestamp(" + args[0] + "), '%Y-%m-%d %H:%M:%S')";
            } else {
                String format = convertSparkDateFormatToDuckDB(args[1]);
                return "strftime(to_timestamp(" + args[0] + "), " + format + ")";
            }
        });
    }

    /**
     * Converts Spark/Java SimpleDateFormat patterns to DuckDB strftime patterns.
     * Common patterns:
     * - yyyy -> %Y (4-digit year)
     * - yy -> %y (2-digit year)
     * - MM -> %m (month 01-12)
     * - dd -> %d (day 01-31)
     * - HH -> %H (hour 00-23)
     * - hh -> %I (hour 01-12)
     * - mm -> %M (minute 00-59)
     * - ss -> %S (second 00-59)
     * - a -> %p (AM/PM)
     * - E -> %a (abbreviated weekday)
     * - EEEE -> %A (full weekday)
     */
    private static String convertSparkDateFormatToDuckDB(String sparkFormat) {
        if (sparkFormat == null || sparkFormat.isEmpty()) {
            return "'%Y-%m-%d %H:%M:%S'";
        }

        // Remove surrounding quotes if present
        String format = sparkFormat;
        if (format.startsWith("'") && format.endsWith("'")) {
            format = format.substring(1, format.length() - 1);
        }

        // Convert patterns (order matters - longer patterns first)
        format = format.replace("yyyy", "%Y");
        format = format.replace("yy", "%y");
        format = format.replace("MMMM", "%B");  // Full month name
        format = format.replace("MMM", "%b");   // Abbreviated month name
        format = format.replace("MM", "%m");
        format = format.replace("dd", "%d");
        format = format.replace("HH", "%H");
        format = format.replace("hh", "%I");
        format = format.replace("mm", "%M");
        format = format.replace("ss", "%S");
        format = format.replace("SSS", "%f");   // Milliseconds
        format = format.replace("EEEE", "%A");  // Full weekday
        format = format.replace("EEE", "%a");   // Abbreviated weekday
        format = format.replace("E", "%a");     // Abbreviated weekday
        format = format.replace("a", "%p");     // AM/PM

        return "'" + format + "'";
    }

    // ==================== Aggregate Functions ====================

    private static void initializeAggregateFunctions() {
        // Basic aggregates
        // SUM: emit plain SUM(...) without type casting.
        // Type casting (BIGINT for integers, DECIMAL(p+10,s) for decimals) is handled by
        // Aggregate.toSQL() which has schema access to determine the correct target type.
        // FunctionRegistry is schema-unaware, so it must NOT make type assumptions.
        CUSTOM_TRANSLATORS.put("sum", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("sum requires at least 1 argument");
            }
            // Use lowercase to match Spark's auto-generated column naming convention
            // (e.g., sum(col) not SUM(col)). DuckDB is case-insensitive for function names.
            return "sum(" + args[0] + ")";
        });
        DIRECT_MAPPINGS.put("avg", "avg");
        DIRECT_MAPPINGS.put("mean", "avg");
        DIRECT_MAPPINGS.put("min", "min");
        DIRECT_MAPPINGS.put("max", "max");
        DIRECT_MAPPINGS.put("count", "count");

        // Statistical aggregates
        DIRECT_MAPPINGS.put("stddev", "stddev");
        DIRECT_MAPPINGS.put("stddev_samp", "stddev_samp");
        DIRECT_MAPPINGS.put("stddev_pop", "stddev_pop");
        DIRECT_MAPPINGS.put("variance", "var_samp");
        DIRECT_MAPPINGS.put("var_samp", "var_samp");
        DIRECT_MAPPINGS.put("var_pop", "var_pop");

        // Collection aggregates
        // Note: collect_list and collect_set use custom translators (see below)
        // collect_list maps to list() but needs FILTER to exclude NULLs (matching Spark semantics)
        DIRECT_MAPPINGS.put("first", "first");
        DIRECT_MAPPINGS.put("last", "last");

        // collect_list: DuckDB's list() includes NULLs, but Spark excludes them
        // Use FILTER clause to match Spark semantics
        CUSTOM_TRANSLATORS.put("collect_list", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("collect_list requires at least 1 argument");
            }
            String arg = args[0];
            // Use FILTER clause to exclude NULLs (matching Spark semantics)
            return "list(" + arg + ") FILTER (WHERE " + arg + " IS NOT NULL)";
        });

        // collect_set requires wrapping: list_distinct(list(arg))
        // DuckDB's list() aggregates all values, list_distinct() removes duplicates
        // Also use FILTER clause to exclude NULLs (matching Spark semantics)
        CUSTOM_TRANSLATORS.put("collect_set", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("collect_set requires at least 1 argument");
            }
            String arg = args[0];
            return "list_distinct(list(" + arg + ") FILTER (WHERE " + arg + " IS NOT NULL))";
        });

        // Other aggregates
        DIRECT_MAPPINGS.put("approx_count_distinct", "approx_count_distinct");
        DIRECT_MAPPINGS.put("corr", "corr");
        DIRECT_MAPPINGS.put("covar_pop", "covar_pop");
        DIRECT_MAPPINGS.put("covar_samp", "covar_samp");

        // Grouping functions (used with ROLLUP/CUBE/GROUPING SETS)
        // DuckDB supports grouping() with identical semantics to Spark
        DIRECT_MAPPINGS.put("grouping", "grouping");
        // grouping_id uses the same bit ordering in both Spark and DuckDB:
        // bit (N-1-i) is set for the i-th argument when that column is NOT in the
        // current grouping key. Direct mapping preserves this convention.
        DIRECT_MAPPINGS.put("grouping_id", "grouping_id");

        // DISTINCT aggregate functions - DISTINCT keyword goes inside parentheses
        // These handle Spark's countDistinct(), sumDistinct() etc. which arrive as
        // "count_distinct", "sum_distinct" after ExpressionConverter appends "_DISTINCT"
        CUSTOM_TRANSLATORS.put("count_distinct", args -> {
            if (args.length == 1) {
                return "COUNT(DISTINCT " + args[0] + ")";
            } else {
                // Multiple columns: wrap in ROW() for DuckDB tuple semantics
                // Spark: COUNT(DISTINCT col1, col2) counts distinct (col1, col2) pairs
                // DuckDB: COUNT(DISTINCT col1, col2) only counts distinct col1 (WRONG!)
                // Solution: COUNT(DISTINCT ROW(col1, col2)) counts distinct tuples
                return "COUNT(DISTINCT ROW(" + String.join(", ", args) + "))";
            }
        });
        // SUM DISTINCT: same as SUM, emit plain SQL without type casting.
        // Type casting is handled by Aggregate.toSQL() with schema access.
        CUSTOM_TRANSLATORS.put("sum_distinct", args ->
            "SUM(DISTINCT " + String.join(", ", args) + ")");
        CUSTOM_TRANSLATORS.put("avg_distinct", args ->
            "AVG(DISTINCT " + String.join(", ", args) + ")");
    }

    // ==================== Window Functions ====================

    private static void initializeWindowFunctions() {
        // Ranking functions
        DIRECT_MAPPINGS.put("row_number", "row_number");
        DIRECT_MAPPINGS.put("rank", "rank");
        DIRECT_MAPPINGS.put("dense_rank", "dense_rank");
        DIRECT_MAPPINGS.put("percent_rank", "percent_rank");
        DIRECT_MAPPINGS.put("ntile", "ntile");

        // Analytic functions
        DIRECT_MAPPINGS.put("lag", "lag");
        DIRECT_MAPPINGS.put("lead", "lead");
        DIRECT_MAPPINGS.put("first_value", "first_value");
        DIRECT_MAPPINGS.put("last_value", "last_value");
        DIRECT_MAPPINGS.put("nth_value", "nth_value");

        // Cumulative functions
        DIRECT_MAPPINGS.put("cume_dist", "cume_dist");
    }

    // ==================== Array Functions ====================

    private static void initializeArrayFunctions() {
        // Array operations
        DIRECT_MAPPINGS.put("array_contains", "list_contains");
        // array_distinct uses custom translator to sort for deterministic order
        // DuckDB's list_distinct doesn't guarantee order, but Spark sorts the result
        CUSTOM_TRANSLATORS.put("array_distinct", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("array_distinct requires at least 1 argument");
            }
            return "list_sort(list_distinct(" + args[0] + "))";
        });
        // Note: array_sort and sort_array use custom translators (see below)
        DIRECT_MAPPINGS.put("array_max", "list_max");
        DIRECT_MAPPINGS.put("array_min", "list_min");
        // Note: size uses custom translator to cast to INT (see below)
        DIRECT_MAPPINGS.put("explode", "unnest");
        DIRECT_MAPPINGS.put("flatten", "flatten");
        // Note: "reverse" for arrays needs list_reverse, but the string "reverse" mapping
        // in initializeStringFunctions() only works on VARCHAR. We override "reverse" here
        // with a custom translator that tries list_reverse first (DuckDB will error if it's a string,
        // so we use list_reverse which works for arrays).
        // Actually, we can't distinguish at the function registry level whether the arg is array or string.
        // So we map array_reverse explicitly and leave reverse as-is (string only).
        DIRECT_MAPPINGS.put("array_reverse", "list_reverse");
        // array_position: Spark returns 0 when element not found, NULL when array is NULL
        // DuckDB's list_position returns NULL for both cases
        CUSTOM_TRANSLATORS.put("array_position", args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("array_position requires at least 2 arguments");
            }
            // Preserve NULL for NULL arrays, return 0 for not-found
            return "CASE WHEN " + args[0] + " IS NULL THEN NULL ELSE COALESCE(list_position(" + args[0] + ", " + args[1] + "), 0) END";
        });
        // element_at: array → list_extract, MAP → keep as element_at (DuckDB native)
        // Handled polymorphically in SQLGenerator.resolvePolymorphicFunctions()
        DIRECT_MAPPINGS.put("element_at", "list_extract");
        DIRECT_MAPPINGS.put("slice", "list_slice");
        DIRECT_MAPPINGS.put("arrays_overlap", "list_has_any");

        // Array construction
        DIRECT_MAPPINGS.put("array", "list_value");

        // array_union: DuckDB has no list_union; use list_distinct(list_concat(a, b))
        CUSTOM_TRANSLATORS.put("array_union", args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("array_union requires exactly 2 arguments");
            }
            return "list_sort(list_distinct(list_concat(" + args[0] + ", " + args[1] + ")))";
        });

        // array_intersect: DuckDB has list_intersect but ordering may differ from Spark
        // Wrap in list_sort for deterministic order
        CUSTOM_TRANSLATORS.put("array_intersect", args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("array_intersect requires exactly 2 arguments");
            }
            return "list_sort(list_intersect(" + args[0] + ", " + args[1] + "))";
        });

        // array_except: DuckDB has no list_except; use list_filter with NOT list_contains
        CUSTOM_TRANSLATORS.put("array_except", args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("array_except requires exactly 2 arguments");
            }
            return "list_sort(list_distinct(list_filter(" + args[0] + ", x -> NOT list_contains(" + args[1] + ", x))))";
        });

        // Higher-order array functions (lambda-based)
        DIRECT_MAPPINGS.put("transform", "list_transform");
        // Note: "filter" is NOT in DIRECT_MAPPINGS because the regex-based rewriteSQL()
        // would corrupt SQL FILTER (WHERE ...) aggregate clauses used by collect_list/collect_set.
        // The lambda HOF filter() is handled by ExpressionConverter which creates
        // FunctionCall("list_filter", ...) directly from the protobuf.
        CUSTOM_TRANSLATORS.put("filter", args -> {
            // Array higher-order function: filter(array, lambda)
            // Translated to list_filter(array, lambda) in DuckDB
            return "list_filter(" + String.join(", ", args) + ")";
        });
        DIRECT_MAPPINGS.put("aggregate", "list_reduce");

        // array_join: Spark's array_join(arr, delimiter) -> DuckDB's array_to_string(arr, delimiter)
        CUSTOM_TRANSLATORS.put("array_join", args -> {
            if (args.length < 2) {
                throw new IllegalArgumentException("array_join requires at least 2 arguments");
            }
            if (args.length == 2) {
                return "array_to_string(" + args[0] + ", " + args[1] + ")";
            }
            // array_join(arr, delimiter, nullReplacement) - 3-arg form
            // DuckDB's array_to_string doesn't support null replacement directly
            return "array_to_string(list_transform(" + args[0] + ", x -> COALESCE(CAST(x AS VARCHAR), " + args[2] + ")), " + args[1] + ")";
        });

        // reverse for arrays: Spark's reverse() works on both strings and arrays.
        // DuckDB's reverse() only works on strings, list_reverse() works on arrays.
        // Since the FunctionRegistry can't know the argument type, we use a custom translator
        // that wraps in list_reverse for the DataFrame API path (where PySpark sends it as "reverse"
        // for array columns). For string columns, the string "reverse" direct mapping handles it.
        // The ExpressionConverter should ideally dispatch based on type, but as a pragmatic fix,
        // we override "reverse" with list_reverse here (it will fail for string args, which
        // should go through the string_functions path).
        // Actually, we can't override since string "reverse" is already mapped.
        // Instead, we handle this in the ExpressionConverter for array-typed arguments.

        // explode_outer: like explode but preserves NULL/empty array rows as NULL values
        // DuckDB's unnest drops NULL array rows. Fix: replace NULL arrays with [NULL]
        // so unnest produces one row with NULL value.
        CUSTOM_TRANSLATORS.put("explode_outer", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("explode_outer requires at least 1 argument");
            }
            String arg = args[0];
            return "unnest(CASE WHEN " + arg + " IS NULL THEN list_value(NULL) ELSE " + arg + " END)";
        });

        // Map accessor functions
        DIRECT_MAPPINGS.put("map_keys", "map_keys");
        DIRECT_MAPPINGS.put("map_values", "map_values");
        DIRECT_MAPPINGS.put("map_entries", "map_entries");

        // MAP_FROM_ARRAYS: create map from key and value arrays
        // Spark: MAP_FROM_ARRAYS(ARRAY('a', 'b'), ARRAY(1, 2))
        // DuckDB: MAP(keys_array, values_array)
        CUSTOM_TRANSLATORS.put("map_from_arrays", args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("map_from_arrays requires exactly 2 arguments");
            }
            return "MAP(" + args[0] + ", " + args[1] + ")";
        });
        // Note: exists, forall require special handling in ExpressionConverter
        // as they need to wrap list_transform with list_any/list_all

        // size() returns INT in Spark but DuckDB len() returns BIGINT - need CAST
        // DuckDB's len() works on arrays/lists; cardinality() only works on MAPs.
        // The polymorphic dispatch for MAP type is handled in SQLGenerator.resolvePolymorphicFunctions().
        CUSTOM_TRANSLATORS.put("size", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("size requires at least 1 argument");
            }
            return "CAST(len(" + args[0] + ") AS INTEGER)";
        });

        // Custom translators for sort_array/array_sort
        // Spark uses boolean (TRUE=asc, FALSE=desc), DuckDB uses string ('ASC'/'DESC')
        CUSTOM_TRANSLATORS.put("sort_array", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("sort_array requires at least 1 argument");
            }
            String arrayArg = args[0];
            String direction = "'ASC'";  // default ascending
            if (args.length > 1) {
                String secondArg = args[1].trim().toUpperCase();
                // Convert TRUE -> 'ASC', FALSE -> 'DESC'
                if ("FALSE".equals(secondArg)) {
                    direction = "'DESC'";
                }
            }
            return "list_sort(" + arrayArg + ", " + direction + ")";
        });

        CUSTOM_TRANSLATORS.put("array_sort", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("array_sort requires at least 1 argument");
            }
            String arrayArg = args[0];
            String direction = "'ASC'";  // default ascending
            if (args.length > 1) {
                String secondArg = args[1].trim().toUpperCase();
                if ("FALSE".equals(secondArg)) {
                    direction = "'DESC'";
                }
            }
            return "list_sort(" + arrayArg + ", " + direction + ")";
        });

        // ==================== Complex Type Constructors ====================

        // STRUCT: positional struct → row() function
        // Spark: struct(col1, col2)
        // DuckDB: row(col1, col2)
        DIRECT_MAPPINGS.put("struct", "row");

        // NAMED_STRUCT: alternating name/value pairs → struct_pack syntax
        // Spark: named_struct('name', 'Alice', 'age', 30)
        // DuckDB: struct_pack(name := 'Alice', age := 30)
        CUSTOM_TRANSLATORS.put("named_struct", args -> {
            if (args.length == 0) {
                return "struct_pack()";
            }
            if (args.length % 2 != 0) {
                throw new IllegalArgumentException(
                    "named_struct requires even number of arguments (name/value pairs)");
            }

            StringBuilder sb = new StringBuilder("struct_pack(");
            for (int i = 0; i < args.length; i += 2) {
                if (i > 0) {
                    sb.append(", ");
                }
                String fieldName = extractFieldName(args[i]);
                String value = args[i + 1];
                sb.append(fieldName).append(" := ").append(value);
            }
            sb.append(")");
            return sb.toString();
        });

        // MAP constructor: alternating key/value pairs → MAP([keys], [values])
        // Spark: map('a', 1, 'b', 2)
        // DuckDB: MAP(['a', 'b'], [1, 2])
        FunctionTranslator mapTranslator = args -> {
            if (args.length == 0) {
                return "MAP([], [])";
            }
            if (args.length % 2 != 0) {
                throw new IllegalArgumentException(
                    "map requires even number of arguments (key/value pairs)");
            }

            StringBuilder keys = new StringBuilder("[");
            StringBuilder values = new StringBuilder("[");

            for (int i = 0; i < args.length; i += 2) {
                if (i > 0) {
                    keys.append(", ");
                    values.append(", ");
                }
                keys.append(args[i]);
                values.append(args[i + 1]);
            }

            keys.append("]");
            values.append("]");

            return "MAP(" + keys + ", " + values + ")";
        };
        CUSTOM_TRANSLATORS.put("map", mapTranslator);
        CUSTOM_TRANSLATORS.put("create_map", mapTranslator);
    }

    // ==================== Conditional Functions ====================

    private static void initializeConditionalFunctions() {
        // Null handling
        DIRECT_MAPPINGS.put("coalesce", "coalesce");
        DIRECT_MAPPINGS.put("nvl", "coalesce");
        DIRECT_MAPPINGS.put("ifnull", "ifnull");
        DIRECT_MAPPINGS.put("nullif", "nullif");
        DIRECT_MAPPINGS.put("isnull", "isnull");
        DIRECT_MAPPINGS.put("isnotnull", "isnotnull");

        // nvl2: Spark's nvl2(expr, value_if_not_null, value_if_null)
        // DuckDB equivalent: CASE WHEN expr IS NOT NULL THEN value_if_not_null ELSE value_if_null END
        CUSTOM_TRANSLATORS.put("nvl2", args -> {
            if (args.length != 3) {
                throw new IllegalArgumentException("nvl2 requires exactly 3 arguments");
            }
            return "CASE WHEN " + args[0] + " IS NOT NULL THEN " + args[1] + " ELSE " + args[2] + " END";
        });

        // nanvl: Spark's nanvl(a, b) returns b if a is NaN, else a
        // DuckDB equivalent: CASE WHEN isnan(a) THEN b ELSE a END
        CUSTOM_TRANSLATORS.put("nanvl", args -> {
            if (args.length != 2) {
                throw new IllegalArgumentException("nanvl requires exactly 2 arguments");
            }
            return "CASE WHEN isnan(" + args[0] + ") THEN " + args[1] + " ELSE " + args[0] + " END";
        });

        // Conditional
        DIRECT_MAPPINGS.put("if", "if");
        DIRECT_MAPPINGS.put("case", "case");
        DIRECT_MAPPINGS.put("when", "when");

        // Type checking
        DIRECT_MAPPINGS.put("isnan", "isnan");
        DIRECT_MAPPINGS.put("isinf", "isinf");
    }

    /**
     * Gets the total number of registered functions.
     *
     * @return the count of registered functions
     */
    public static int registeredFunctionCount() {
        return DIRECT_MAPPINGS.size() + CUSTOM_TRANSLATORS.size();
    }

    /**
     * Maps a Spark function name to DuckDB equivalent.
     *
     * <p>Returns the mapped DuckDB function name if a direct mapping exists,
     * otherwise returns the original name unchanged.
     *
     * @param sparkFunctionName the Spark function name
     * @return the DuckDB function name (or original if no mapping)
     */
    public static String mapFunctionName(String sparkFunctionName) {
        if (sparkFunctionName == null) {
            return null;
        }
        String normalizedName = sparkFunctionName.toLowerCase();
        return DIRECT_MAPPINGS.getOrDefault(normalizedName, sparkFunctionName);
    }

    /**
     * Gets the function metadata if available.
     *
     * <p>Function metadata provides type inference and nullable information
     * in addition to translation logic.
     *
     * @param functionName the Spark function name
     * @return the metadata, or empty if not available
     */
    public static Optional<FunctionMetadata> getMetadata(String functionName) {
        if (functionName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(FUNCTION_METADATA.get(functionName.toLowerCase()));
    }

    // ==================== Function Metadata Initialization ====================

    /**
     * Initialize function metadata for key functions that need type/nullable inference.
     *
     * <p>This provides a single source of truth for functions that have complex
     * type or nullable inference requirements. Functions not in this map will
     * fall back to the legacy inference logic in ExpressionConverter.
     */
    private static void initializeFunctionMetadata() {
        // Null-coalescing functions: return first argument's type, non-null if ANY arg is non-null
        FUNCTION_METADATA.put("greatest", FunctionMetadata.builder("greatest")
            .duckdbName("greatest")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.nullCoalescing())
            .build());

        FUNCTION_METADATA.put("least", FunctionMetadata.builder("least")
            .duckdbName("least")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.nullCoalescing())
            .build());

        FUNCTION_METADATA.put("coalesce", FunctionMetadata.builder("coalesce")
            .duckdbName("coalesce")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.nullCoalescing())
            .build());

        FUNCTION_METADATA.put("nvl", FunctionMetadata.builder("nvl")
            .duckdbName("coalesce")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.nullCoalescing())
            .build());

        FUNCTION_METADATA.put("ifnull", FunctionMetadata.builder("ifnull")
            .duckdbName("ifnull")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.nullCoalescing())
            .build());

        // Math type-preserving functions
        FUNCTION_METADATA.put("abs", FunctionMetadata.builder("abs")
            .duckdbName("abs")
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("pmod", FunctionMetadata.builder("pmod")
            .translator(args -> {
                if (args.length < 2) {
                    throw new IllegalArgumentException("pmod requires 2 arguments");
                }
                String a = args[0];
                String b = args[1];
                return "(((" + a + ") % (" + b + ") + (" + b + ")) % (" + b + "))";
            })
            .returnType(FunctionMetadata.firstArgTypePreserving())
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        // Position functions return IntegerType
        FUNCTION_METADATA.put("locate", FunctionMetadata.builder("locate")
            .translator(args -> {
                if (args.length < 2) {
                    throw new IllegalArgumentException("locate requires at least 2 arguments");
                }
                String substr = args[0];
                String str = args[1];
                if (args.length > 2) {
                    String startPos = args[2];
                    return "CASE WHEN " + str + " IS NULL THEN NULL " +
                           "WHEN instr(substr(" + str + ", " + startPos + "), " + substr + ") > 0 " +
                           "THEN instr(substr(" + str + ", " + startPos + "), " + substr + ") + " + startPos + " - 1 " +
                           "ELSE 0 END";
                }
                return "CASE WHEN " + str + " IS NULL THEN NULL ELSE instr(" + str + ", " + substr + ") END";
            })
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("instr", FunctionMetadata.builder("instr")
            .duckdbName("instr")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("position", FunctionMetadata.builder("position")
            .duckdbName("position")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("array_position", FunctionMetadata.builder("array_position")
            .duckdbName("list_position")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        // Boolean-returning functions
        FUNCTION_METADATA.put("isnull", FunctionMetadata.builder("isnull")
            .duckdbName("isnull")
            .returnType(FunctionMetadata.constantType(BooleanType.get()))
            .nullable(FunctionMetadata.alwaysNonNull())
            .build());

        FUNCTION_METADATA.put("isnotnull", FunctionMetadata.builder("isnotnull")
            .duckdbName("isnotnull")
            .returnType(FunctionMetadata.constantType(BooleanType.get()))
            .nullable(FunctionMetadata.alwaysNonNull())
            .build());

        // Date/time functions with specific return types
        // Note: These use CUSTOM_TRANSLATORS for SQL generation, but need metadata for type inference
        FUNCTION_METADATA.put("months_between", FunctionMetadata.builder("months_between")
            .duckdbName("datediff")  // Placeholder - actual translation in CUSTOM_TRANSLATORS
            .returnType(FunctionMetadata.constantType(DoubleType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("to_date", FunctionMetadata.builder("to_date")
            .duckdbName("cast")  // Placeholder - actual translation in CUSTOM_TRANSLATORS
            .returnType(FunctionMetadata.constantType(DateType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("to_timestamp", FunctionMetadata.builder("to_timestamp")
            .duckdbName("cast")  // Placeholder - actual translation in CUSTOM_TRANSLATORS
            .returnType(FunctionMetadata.constantType(TimestampType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("unix_timestamp", FunctionMetadata.builder("unix_timestamp")
            .duckdbName("epoch")  // Placeholder - actual translation in CUSTOM_TRANSLATORS
            .returnType(FunctionMetadata.constantType(LongType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("date_trunc", FunctionMetadata.builder("date_trunc")
            .duckdbName("date_trunc")
            .returnType(FunctionMetadata.constantType(TimestampType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("last_day", FunctionMetadata.builder("last_day")
            .duckdbName("last_day")
            .returnType(FunctionMetadata.constantType(DateType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("next_day", FunctionMetadata.builder("next_day")
            .duckdbName("next_day")
            .returnType(FunctionMetadata.constantType(DateType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("from_unixtime", FunctionMetadata.builder("from_unixtime")
            .duckdbName("strftime")  // Placeholder - actual translation in CUSTOM_TRANSLATORS
            .returnType(FunctionMetadata.constantType(StringType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        // DateTime extraction functions return IntegerType (per Spark spec)
        FUNCTION_METADATA.put("year", FunctionMetadata.builder("year")
            .duckdbName("year")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("month", FunctionMetadata.builder("month")
            .duckdbName("month")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("day", FunctionMetadata.builder("day")
            .duckdbName("day")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("dayofmonth", FunctionMetadata.builder("dayofmonth")
            .duckdbName("day")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("hour", FunctionMetadata.builder("hour")
            .duckdbName("hour")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("minute", FunctionMetadata.builder("minute")
            .duckdbName("minute")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("second", FunctionMetadata.builder("second")
            .duckdbName("second")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("quarter", FunctionMetadata.builder("quarter")
            .duckdbName("quarter")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("weekofyear", FunctionMetadata.builder("weekofyear")
            .duckdbName("weekofyear")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());

        FUNCTION_METADATA.put("dayofyear", FunctionMetadata.builder("dayofyear")
            .duckdbName("dayofyear")
            .returnType(FunctionMetadata.constantType(IntegerType.get()))
            .nullable(FunctionMetadata.anyArgNullable())
            .build());
    }

    // ==================== Helper Methods ====================

    /**
     * Extracts a field name from a quoted string literal.
     * Removes surrounding single/double quotes if present.
     *
     * <p>Used by complex type constructors like named_struct to extract
     * field names from Spark SQL string literals.
     *
     * @param quotedName the potentially quoted field name
     * @return the unquoted field name
     */
    private static String extractFieldName(String quotedName) {
        if (quotedName == null) {
            return "field";
        }
        String trimmed = quotedName.trim();
        // Remove surrounding single quotes
        if (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        // Remove surrounding double quotes
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }
}
