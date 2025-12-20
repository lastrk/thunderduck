package com.thunderduck.expression;

import com.thunderduck.expression.window.WindowFrame;
import com.thunderduck.logical.Sort;
import com.thunderduck.types.DataType;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.LongType;
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
public class WindowFunction extends Expression {

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
            case "FIRST_VALUE":
            case "LAST_VALUE":
            case "NTH_VALUE":
                // These return the type of their argument
                if (!arguments.isEmpty()) {
                    return arguments.get(0).dataType();
                }
                return LongType.get();
            case "SUM":
            case "AVG":
                // Aggregate window functions - return type depends on input
                if (!arguments.isEmpty() && arguments.get(0).dataType() != null) {
                    return arguments.get(0).dataType();
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
        // Ranking functions always return non-null values
        String funcUpper = function.toUpperCase();
        if (funcUpper.equals("ROW_NUMBER") ||
            funcUpper.equals("RANK") ||
            funcUpper.equals("DENSE_RANK") ||
            funcUpper.equals("NTILE")) {
            return false;
        }
        // All other window functions can return null
        return true;
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();

        // Function name and arguments
        sql.append(function.toUpperCase());
        sql.append("(");

        // Add arguments
        // Special case: COUNT(*) - output * without quotes
        if (arguments.size() == 1 &&
            function.equalsIgnoreCase("COUNT") &&
            arguments.get(0) instanceof Literal &&
            "*".equals(((Literal) arguments.get(0)).value())) {
            sql.append("*");
        } else {
            for (int i = 0; i < arguments.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(arguments.get(i).toSQL());
            }
        }

        sql.append(")");

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
