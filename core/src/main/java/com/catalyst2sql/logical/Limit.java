package com.catalyst2sql.logical;

import com.catalyst2sql.types.StructType;
import java.util.Objects;

/**
 * Logical plan node representing a limit (LIMIT clause).
 *
 * <p>This node limits the number of rows returned from its child.
 *
 * <p>Examples:
 * <pre>
 *   df.limit(10)
 *   df.limit(100).offset(50)  // Will be represented as Limit with offset
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT * FROM (child) LIMIT limit OFFSET offset</pre>
 */
public class Limit extends LogicalPlan {

    private final long limit;
    private final long offset;

    /**
     * Creates a limit node with an offset.
     *
     * @param child the child node
     * @param limit the maximum number of rows to return
     * @param offset the number of rows to skip
     */
    public Limit(LogicalPlan child, long limit, long offset) {
        super(child);
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Creates a limit node without an offset.
     *
     * @param child the child node
     * @param limit the maximum number of rows to return
     */
    public Limit(LogicalPlan child, long limit) {
        this(child, limit, 0);
    }

    /**
     * Returns the limit.
     *
     * @return the limit
     */
    public long limit() {
        return limit;
    }

    /**
     * Returns the offset.
     *
     * @return the offset
     */
    public long offset() {
        return offset;
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

        String childSQL = child().toSQL(generator);

        if (offset > 0) {
            return String.format("SELECT * FROM (%s) AS subquery LIMIT %d OFFSET %d",
                childSQL, limit, offset);
        } else {
            return String.format("SELECT * FROM (%s) AS subquery LIMIT %d",
                childSQL, limit);
        }
    }

    @Override
    public StructType inferSchema() {
        // Limit doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        if (offset > 0) {
            return String.format("Limit(limit=%d, offset=%d)", limit, offset);
        } else {
            return String.format("Limit(%d)", limit);
        }
    }
}
