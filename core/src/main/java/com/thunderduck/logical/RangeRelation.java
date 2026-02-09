package com.thunderduck.logical;

import com.thunderduck.types.LongType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;

import java.util.Collections;

/**
 * Logical plan node representing a range of sequential integers.
 *
 * <p>This corresponds to Spark's {@code spark.range(start, end, step)} operation,
 * which generates a DataFrame with a single column named "id" containing sequential
 * BIGINT values from start (inclusive) to end (exclusive) with the given step.
 *
 * <p>Examples:
 * <pre>
 *   spark.range(10)           → 0, 1, 2, ..., 9
 *   spark.range(5, 10)        → 5, 6, 7, 8, 9
 *   spark.range(0, 10, 2)     → 0, 2, 4, 6, 8
 *   spark.range(10, 0, -1)    → 10, 9, 8, ..., 1
 * </pre>
 *
 * <p>The SQL generation uses DuckDB's native {@code range()} table function:
 * <pre>
 *   SELECT range AS id FROM range(start, end, step)
 * </pre>
 *
 * @see <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.range.html">
 *      PySpark SparkSession.range()</a>
 */
public final class RangeRelation extends LogicalPlan {

    /** The schema for Range: single column "id" of type BIGINT, non-nullable */
    private static final StructType RANGE_SCHEMA = new StructType(
        Collections.singletonList(new StructField("id", LongType.get(), false))
    );

    private final long start;
    private final long end;
    private final long step;

    /**
     * Creates a range relation.
     *
     * @param start the starting value (inclusive)
     * @param end the ending value (exclusive)
     * @param step the increment between consecutive values
     * @throws IllegalArgumentException if step is zero
     */
    public RangeRelation(long start, long end, long step) {
        super(); // No children - this is a leaf node
        if (step == 0) {
            throw new IllegalArgumentException("step cannot be zero");
        }
        this.start = start;
        this.end = end;
        this.step = step;
        this.schema = RANGE_SCHEMA;
    }

    /**
     * Returns the starting value (inclusive).
     *
     * @return the start value
     */
    public long start() {
        return start;
    }

    /**
     * Returns the ending value (exclusive).
     *
     * @return the end value
     */
    public long end() {
        return end;
    }

    /**
     * Returns the step (increment between values).
     *
     * @return the step value
     */
    public long step() {
        return step;
    }

    /**
     * Calculates the number of elements in this range.
     *
     * @return the count of elements, or 0 if the range is empty
     */
    public long count() {
        if (step > 0) {
            if (start >= end) {
                return 0;
            }
            return (end - start - 1) / step + 1;
        } else {
            // step < 0
            if (start <= end) {
                return 0;
            }
            return (start - end - 1) / (-step) + 1;
        }
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // Delegate to the generator's visitor pattern
        // This will be called by SQLGenerator.visitRangeRelation()
        return generator.generate(this);
    }

    @Override
    public StructType inferSchema() {
        return RANGE_SCHEMA;
    }

    @Override
    public String toString() {
        return String.format("RangeRelation(start=%d, end=%d, step=%d)", start, end, step);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeRelation that = (RangeRelation) o;
        return start == that.start && end == that.end && step == that.step;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(start);
        result = 31 * result + Long.hashCode(end);
        result = 31 * result + Long.hashCode(step);
        return result;
    }
}
