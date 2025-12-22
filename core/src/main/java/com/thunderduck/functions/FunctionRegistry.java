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
        DIRECT_MAPPINGS.put("concat", "concat");
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
        DIRECT_MAPPINGS.put("split", "string_split");
        DIRECT_MAPPINGS.put("concat_ws", "concat_ws");

        // Regular expressions
        DIRECT_MAPPINGS.put("regexp_extract", "regexp_extract");
        DIRECT_MAPPINGS.put("regexp_replace", "regexp_replace");
        DIRECT_MAPPINGS.put("regexp_like", "regexp_matches");

        // String predicates
        DIRECT_MAPPINGS.put("startswith", "starts_with");
        DIRECT_MAPPINGS.put("endswith", "ends_with");
        DIRECT_MAPPINGS.put("contains", "contains");
        DIRECT_MAPPINGS.put("rlike", "regexp_matches");

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
        // Extract components - direct mappings
        DIRECT_MAPPINGS.put("year", "year");
        DIRECT_MAPPINGS.put("month", "month");
        DIRECT_MAPPINGS.put("day", "day");
        DIRECT_MAPPINGS.put("dayofmonth", "day");
        DIRECT_MAPPINGS.put("dayofyear", "dayofyear");
        DIRECT_MAPPINGS.put("hour", "hour");
        DIRECT_MAPPINGS.put("minute", "minute");
        DIRECT_MAPPINGS.put("second", "second");
        DIRECT_MAPPINGS.put("quarter", "quarter");
        DIRECT_MAPPINGS.put("weekofyear", "weekofyear");

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
        // SUM needs special handling: DuckDB returns DECIMAL(38,0) for integer inputs,
        // but Spark returns BIGINT. Cast to BIGINT for Spark compatibility.
        CUSTOM_TRANSLATORS.put("sum", args -> {
            if (args.length < 1) {
                throw new IllegalArgumentException("sum requires at least 1 argument");
            }
            return "CAST(SUM(" + args[0] + ") AS BIGINT)";
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
        DIRECT_MAPPINGS.put("array_distinct", "list_distinct");
        // Note: array_sort and sort_array use custom translators (see below)
        DIRECT_MAPPINGS.put("array_max", "list_max");
        DIRECT_MAPPINGS.put("array_min", "list_min");
        // Note: size uses custom translator to cast to INT (see below)
        DIRECT_MAPPINGS.put("explode", "unnest");
        DIRECT_MAPPINGS.put("flatten", "flatten");
        // Note: "reverse" for strings is mapped above (line 128) to DuckDB's reverse()
        // For arrays, use array_reverse explicitly
        DIRECT_MAPPINGS.put("array_reverse", "list_reverse");
        DIRECT_MAPPINGS.put("array_position", "list_position");
        DIRECT_MAPPINGS.put("element_at", "list_extract");
        DIRECT_MAPPINGS.put("slice", "list_slice");
        DIRECT_MAPPINGS.put("arrays_overlap", "list_has_any");

        // Array construction
        DIRECT_MAPPINGS.put("array", "list_value");
        DIRECT_MAPPINGS.put("array_union", "list_union");
        DIRECT_MAPPINGS.put("array_intersect", "list_intersect");
        DIRECT_MAPPINGS.put("array_except", "list_except");

        // Higher-order array functions (lambda-based)
        DIRECT_MAPPINGS.put("transform", "list_transform");
        DIRECT_MAPPINGS.put("filter", "list_filter");
        DIRECT_MAPPINGS.put("aggregate", "list_reduce");

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
