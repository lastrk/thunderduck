package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Objects;

/**
 * Logical plan node representing a tail operation.
 *
 * <p>This node returns the last N rows from its child relation.
 * Unlike LIMIT which takes the first N rows, TAIL takes the last N.
 *
 * <p>The result maintains the original row order (i.e., if the input
 * has rows [1,2,3,4,5] and tail(3), the result is [3,4,5], not [5,4,3]).
 *
 * <p>Examples:
 * <pre>
 *   df.tail(10)  // Returns last 10 rows
 * </pre>
 *
 * <p>SQL generation uses window function with row numbering:
 * <pre>
 * SELECT * FROM (
 *   SELECT *, ROW_NUMBER() OVER () AS _rn,
 *          COUNT(*) OVER () AS _total
 *   FROM (child)
 * ) AS subquery
 * WHERE _rn > _total - n
 * </pre>
 *
 * <p>For memory-efficient streaming execution, see {@link com.thunderduck.runtime.TailBatchIterator}
 * which uses a circular buffer to collect only the last N rows while streaming.
 */
public class Tail extends LogicalPlan {

    private final long limit;

    /**
     * Creates a tail node.
     *
     * @param child the child node
     * @param limit the maximum number of rows to return from the end
     */
    public Tail(LogicalPlan child, long limit) {
        super(child);
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        this.limit = limit;
    }

    /**
     * Returns the limit (number of rows from end).
     *
     * @return the limit
     */
    public long limit() {
        return limit;
    }

    /**
     * Returns the child node.
     *
     * @return the child
     */
    public LogicalPlan child() {
        return children.get(0);
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        Objects.requireNonNull(generator, "generator must not be null");

        String childSQL = generator.generate(child());
        String alias1 = generator.generateSubqueryAlias();
        String alias2 = generator.generateSubqueryAlias();

        // Use window functions for tail semantics:
        // 1. Add row number and total count
        // 2. Filter to keep only rows where row_number > total - limit
        //
        // This approach:
        // - Preserves original row order
        // - Works correctly with any number of rows
        // - DuckDB optimizes window functions well
        return String.format(
            "SELECT * EXCLUDE (_td_rn, _td_total) FROM (" +
            "SELECT *, ROW_NUMBER() OVER () AS _td_rn, COUNT(*) OVER () AS _td_total " +
            "FROM (%s) AS %s" +
            ") AS %s WHERE _td_rn > _td_total - %d",
            childSQL, alias1, alias2, limit);
    }

    @Override
    public StructType inferSchema() {
        // Tail doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        return String.format("Tail(%d)", limit);
    }
}
