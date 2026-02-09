package com.thunderduck.expression;

import com.thunderduck.expression.window.WindowFrame;
import com.thunderduck.logical.Sort;
import com.thunderduck.types.ByteType;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DecimalType;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.FloatType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.LongType;
import com.thunderduck.types.ShortType;
import com.thunderduck.types.StringType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Expression representing a window function.
 *
 * <p>Window functions operate on a set of rows and return a value for each row.
 * Unlike aggregate functions, window functions do not group rows into a single
 * output row.
 *
 * <p>Examples:
 * <pre>
 *   ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC)
 *   RANK() OVER (ORDER BY score DESC)
 *   LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY date)
 *   AVG(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
 * </pre>
 *
 * <p>Supported window functions:
 * <ul>
 *   <li>ROW_NUMBER() - Sequential number for each row within partition</li>
 *   <li>RANK() - Rank with gaps for tied values</li>
 *   <li>DENSE_RANK() - Rank without gaps</li>
 *   <li>LAG(expr, offset, default) - Value from previous row</li>
 *   <li>LEAD(expr, offset, default) - Value from next row</li>
 *   <li>FIRST_VALUE(expr) - First value in window frame</li>
 *   <li>LAST_VALUE(expr) - Last value in window frame</li>
 *   <li>Aggregate functions with OVER clause (SUM, AVG, COUNT, etc.)</li>
 * </ul>
 *
 * <p>Window frames can be specified to control which rows are included in the
 * window function's computation. See {@link WindowFrame} for details.
 *
 * @see WindowFrame
 */
public final class WindowFunction implements Expression {

    private final String function;
    private final List<Expression> arguments;
    private final List<Expression> partitionBy;
    private final List<Sort.SortOrder> orderBy;
    private final WindowFrame frame;
    private final String windowName;  // Optional reference to named window

    /**
     * Creates a window function with an optional frame specification.
     *
     * @param function the function name (ROW_NUMBER, RANK, LAG, etc.)
     * @param arguments the function arguments (empty for ROW_NUMBER, RANK, etc.)
     * @param partitionBy the partition by expressions (empty for no partitioning)
     * @param orderBy the order by specifications (empty for no ordering)
     * @param frame the window frame specification (null for default frame)
     */
    public WindowFunction(String function,
                         List<Expression> arguments,
                         List<Expression> partitionBy,
                         List<Sort.SortOrder> orderBy,
                         WindowFrame frame) {
        this.function = Objects.requireNonNull(function, "function must not be null");
        this.arguments = new ArrayList<>(
            Objects.requireNonNull(arguments, "arguments must not be null"));
        this.partitionBy = new ArrayList<>(
            Objects.requireNonNull(partitionBy, "partitionBy must not be null"));
        this.orderBy = new ArrayList<>(
            Objects.requireNonNull(orderBy, "orderBy must not be null"));
        this.frame = frame;  // Can be null
        this.windowName = null;  // Not using named window
    }

    /**
     * Creates a window function that references a named window.
     *
     * <p>When using a named window, the window specification is defined in the
     * WINDOW clause and referenced by name here.
     *
     * <p>Example:
     * <pre>
     * SELECT RANK() OVER w FROM employees
     * WINDOW w AS (PARTITION BY department_id ORDER BY salary DESC)
     * </pre>
     *
     * @param function the function name (ROW_NUMBER, RANK, LAG, etc.)
     * @param arguments the function arguments (empty for ROW_NUMBER, RANK, etc.)
     * @param windowName the name of the window defined in WINDOW clause
     */
    public WindowFunction(String function,
                         List<Expression> arguments,
                         String windowName) {
        this.function = Objects.requireNonNull(function, "function must not be null");
        this.arguments = new ArrayList<>(
            Objects.requireNonNull(arguments, "arguments must not be null"));
        this.windowName = Objects.requireNonNull(windowName, "windowName must not be null");
        this.partitionBy = Collections.emptyList();
        this.orderBy = Collections.emptyList();
        this.frame = null;
    }

    /**
     * Creates a window function without a frame specification.
     * This constructor is provided for backward compatibility.
     *
     * @param function the function name (ROW_NUMBER, RANK, LAG, etc.)
     * @param arguments the function arguments (empty for ROW_NUMBER, RANK, etc.)
     * @param partitionBy the partition by expressions (empty for no partitioning)
     * @param orderBy the order by specifications (empty for no ordering)
     */
    public WindowFunction(String function,
                         List<Expression> arguments,
                         List<Expression> partitionBy,
                         List<Sort.SortOrder> orderBy) {
        this(function, arguments, partitionBy, orderBy, null);
    }

    /**
     * Returns the named window reference, if this function uses one.
     *
     * @return an Optional containing the window name, or empty if inline specification is used
     */
    public Optional<String> windowName() {
        return Optional.ofNullable(windowName);
    }

    /**
     * Returns the function name.
     *
     * @return the function name
     */
    public String function() {
        return function;
    }

    /**
     * Returns the function arguments.
     *
     * @return an unmodifiable list of arguments
     */
    public List<Expression> arguments() {
        return Collections.unmodifiableList(arguments);
    }

    /**
     * Returns the partition by expressions.
     *
     * @return an unmodifiable list of partition expressions
     */
    public List<Expression> partitionBy() {
        return Collections.unmodifiableList(partitionBy);
    }

    /**
     * Returns the order by specifications.
     *
     * @return an unmodifiable list of sort orders
     */
    public List<Sort.SortOrder> orderBy() {
        return Collections.unmodifiableList(orderBy);
    }

    /**
     * Returns the window frame specification.
     *
     * @return an Optional containing the window frame, or empty if no frame is specified
     */
    public Optional<WindowFrame> frame() {
        return Optional.ofNullable(frame);
    }

    @Override
    public DataType dataType() {
        // Type depends on function
        String funcUpper = function.toUpperCase();
        switch (funcUpper) {
            case "ROW_NUMBER":
            case "RANK":
            case "DENSE_RANK":
            case "NTILE":
                return IntegerType.get();  // Spark returns INT for ranking functions
            case "COUNT":
                return LongType.get();
            case "LAG":
            case "LEAD":
            case "FIRST":
            case "FIRST_VALUE":
            case "LAST":
            case "LAST_VALUE":
            case "NTH_VALUE":
                // These return the type of their argument
                if (!arguments.isEmpty()) {
                    return arguments.get(0).dataType();
                }
                return LongType.get();
            case "SUM":
                // Apply Spark type promotion: SUM(int/long) -> BIGINT, SUM(decimal) -> decimal(p+10,s)
                if (!arguments.isEmpty() && arguments.get(0).dataType() != null) {
                    DataType argType = arguments.get(0).dataType();
                    if (argType instanceof IntegerType || argType instanceof LongType ||
                        argType instanceof ShortType || argType instanceof ByteType) {
                        return LongType.get();
                    }
                    if (argType instanceof FloatType || argType instanceof DoubleType) {
                        return DoubleType.get();
                    }
                    if (argType instanceof DecimalType) {
                        DecimalType dt = (DecimalType) argType;
                        int newPrecision = Math.min(dt.precision() + 10, 38);
                        return new DecimalType(newPrecision, dt.scale());
                    }
                    // StringType means unresolved column - default to LongType (most common SUM case)
                    if (argType instanceof StringType) {
                        return LongType.get();
                    }
                    return argType;
                }
                return LongType.get();
            case "AVG":
                if (!arguments.isEmpty() && arguments.get(0).dataType() != null) {
                    DataType argType = arguments.get(0).dataType();
                    if (argType instanceof DecimalType) {
                        DecimalType dt = (DecimalType) argType;
                        int newPrecision = Math.min(dt.precision() + 4, 38);
                        int newScale = Math.min(dt.scale() + 4, newPrecision);
                        return new DecimalType(newPrecision, newScale);
                    }
                }
                return DoubleType.get();
            case "MIN":
            case "MAX":
                if (!arguments.isEmpty() && arguments.get(0).dataType() != null) {
                    return arguments.get(0).dataType();
                }
                return LongType.get();
            case "PERCENT_RANK":
            case "CUME_DIST":
                return DoubleType.get();
            default:
                // Default to BIGINT for unknown window functions
                return LongType.get();
        }
    }

    @Override
    public boolean nullable() {
        String funcUpper = function.toUpperCase();

        // Ranking functions always return non-null values
        if (funcUpper.equals("ROW_NUMBER") ||
            funcUpper.equals("RANK") ||
            funcUpper.equals("DENSE_RANK") ||
            funcUpper.equals("NTILE") ||
            funcUpper.equals("PERCENT_RANK") ||
            funcUpper.equals("CUME_DIST")) {
            return false;
        }

        // COUNT always returns non-null (0 for empty groups)
        if (funcUpper.equals("COUNT")) {
            return false;
        }

        // LAG/LEAD with default value (3rd argument) are non-nullable
        if ((funcUpper.equals("LAG") || funcUpper.equals("LEAD")) && arguments.size() >= 3) {
            return false;
        }

        // All other window functions can return null
        return true;
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();

        // Function name - map to DuckDB equivalent
        String duckDbFunction = getDuckDbFunctionName(function);
        sql.append(duckDbFunction);
        sql.append("(");

        // Determine how to handle arguments based on function type
        String funcUpper = function.toUpperCase();
        boolean isFirstLast = funcUpper.equals("FIRST") || funcUpper.equals("FIRST_VALUE") ||
                              funcUpper.equals("LAST") || funcUpper.equals("LAST_VALUE");
        boolean isNthValue = funcUpper.equals("NTH_VALUE");
        boolean ignoreNulls = false;
        int argsToOutput = arguments.size();

        // Handle ignoreNulls parameter for FIRST/LAST/NTH_VALUE
        // Spark: first(col, ignoreNulls) → DuckDB: FIRST_VALUE(col) [IGNORE NULLS]
        // Spark: nth_value(col, n, ignoreNulls) → DuckDB: NTH_VALUE(col, n) [IGNORE NULLS]
        if (isFirstLast && arguments.size() >= 2) {
            // Second argument is ignoreNulls boolean
            Expression ignoreNullsArg = arguments.get(1);
            if (ignoreNullsArg instanceof Literal) {
                Object val = ((Literal) ignoreNullsArg).value();
                ignoreNulls = Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
            }
            argsToOutput = 1;  // Only output the first argument
        } else if (isNthValue && arguments.size() >= 3) {
            // Third argument is ignoreNulls boolean
            Expression ignoreNullsArg = arguments.get(2);
            if (ignoreNullsArg instanceof Literal) {
                Object val = ((Literal) ignoreNullsArg).value();
                ignoreNulls = Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
            }
            argsToOutput = 2;  // Output first two arguments (expr, n)
        }

        // Add arguments
        // Special case: COUNT(*) - output * without quotes
        if (arguments.size() == 1 &&
            function.equalsIgnoreCase("COUNT") &&
            arguments.get(0) instanceof Literal &&
            "*".equals(((Literal) arguments.get(0)).value())) {
            sql.append("*");
        } else {
            for (int i = 0; i < argsToOutput; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(arguments.get(i).toSQL());
            }
        }

        sql.append(")");

        // Add IGNORE NULLS if specified (must come after closing paren, before OVER)
        if (ignoreNulls && (isFirstLast || isNthValue)) {
            sql.append(" IGNORE NULLS");
        }

        // OVER clause
        sql.append(" OVER ");

        // If using named window, just reference the name
        if (windowName != null) {
            sql.append(windowName);
            return wrapWithCastIfRankingFunction(sql.toString());
        }

        // Otherwise, inline window specification
        sql.append("(");

        boolean hasPartition = !partitionBy.isEmpty();
        boolean hasOrder = !orderBy.isEmpty();

        // PARTITION BY clause
        if (hasPartition) {
            sql.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(partitionBy.get(i).toSQL());
            }
        }

        // ORDER BY clause
        if (hasOrder) {
            if (hasPartition) {
                sql.append(" ");
            }
            sql.append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }

                Sort.SortOrder order = orderBy.get(i);
                sql.append(order.expression().toSQL());

                // Add sort direction
                if (order.direction() == Sort.SortDirection.DESCENDING) {
                    sql.append(" DESC");
                } else {
                    sql.append(" ASC");
                }

                // Add null ordering
                if (order.nullOrdering() == Sort.NullOrdering.NULLS_FIRST) {
                    sql.append(" NULLS FIRST");
                } else if (order.nullOrdering() == Sort.NullOrdering.NULLS_LAST) {
                    sql.append(" NULLS LAST");
                }
            }
        }

        // Window frame clause
        if (frame != null) {
            if (hasPartition || hasOrder) {
                sql.append(" ");
            }
            sql.append(frame.toSQL());
        }

        sql.append(")");

        return wrapWithCastIfRankingFunction(sql.toString());
    }

    @Override
    public String toString() {
        return toSQL();
    }

    /**
     * Maps Spark function names to their DuckDB equivalents for window functions.
     *
     * <p>Some functions have different names in Spark vs DuckDB:
     * <ul>
     *   <li>{@code first} → {@code first_value} (DuckDB's aggregate first() doesn't work with OVER)</li>
     *   <li>{@code last} → {@code last_value} (DuckDB's aggregate last() doesn't work with OVER)</li>
     * </ul>
     *
     * @param sparkFunctionName the Spark function name
     * @return the equivalent DuckDB function name
     */
    private String getDuckDbFunctionName(String sparkFunctionName) {
        String funcUpper = sparkFunctionName.toUpperCase();
        switch (funcUpper) {
            case "FIRST":
                return "FIRST_VALUE";
            case "LAST":
                return "LAST_VALUE";
            default:
                return funcUpper;
        }
    }

    /**
     * Wraps the SQL with CAST to INTEGER if this is a ranking function.
     *
     * <p>DuckDB returns BIGINT for ranking functions, but Spark returns INTEGER.
     * This method wraps the SQL with a CAST to ensure type compatibility.
     *
     * @param sql the generated window function SQL
     * @return the SQL wrapped with CAST for ranking functions, or unchanged for others
     */
    private String wrapWithCastIfRankingFunction(String sql) {
        String funcUpper = function.toUpperCase();
        if (funcUpper.equals("ROW_NUMBER") ||
            funcUpper.equals("RANK") ||
            funcUpper.equals("DENSE_RANK") ||
            funcUpper.equals("NTILE")) {
            return "CAST(" + sql + " AS INTEGER)";
        }
        return sql;
    }

    // Note: toString() is intentionally NOT overridden here.
    // The base class Expression.toString() delegates to toSQL(), which is the correct behavior.
    // Having a debug-style toString() would cause incorrect SQL generation when WindowFunction
    // is used in string interpolation contexts.
}
