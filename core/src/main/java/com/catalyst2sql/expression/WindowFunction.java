package com.catalyst2sql.expression;

import com.catalyst2sql.logical.Sort;
import com.catalyst2sql.types.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
 * </ul>
 */
public class WindowFunction extends Expression {

    private final String function;
    private final List<Expression> arguments;
    private final List<Expression> partitionBy;
    private final List<Sort.SortOrder> orderBy;

    /**
     * Creates a window function.
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
        this.function = Objects.requireNonNull(function, "function must not be null");
        this.arguments = new ArrayList<>(
            Objects.requireNonNull(arguments, "arguments must not be null"));
        this.partitionBy = new ArrayList<>(
            Objects.requireNonNull(partitionBy, "partitionBy must not be null"));
        this.orderBy = new ArrayList<>(
            Objects.requireNonNull(orderBy, "orderBy must not be null"));
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

    @Override
    public DataType dataType() {
        // Type depends on function - will be refined during implementation
        return null;
    }

    @Override
    public boolean nullable() {
        // Window functions can generally produce nulls
        return true;
    }

    @Override
    public String toSQL() {
        StringBuilder sql = new StringBuilder();

        // Function name and arguments
        sql.append(function.toUpperCase());
        sql.append("(");

        // Add arguments
        for (int i = 0; i < arguments.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(arguments.get(i).toSQL());
        }

        sql.append(")");

        // OVER clause
        sql.append(" OVER (");

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

        sql.append(")");

        return sql.toString();
    }

    @Override
    public String toString() {
        return String.format("WindowFunction(%s, partitionBy=%s, orderBy=%s)",
                           function, partitionBy, orderBy);
    }
}
