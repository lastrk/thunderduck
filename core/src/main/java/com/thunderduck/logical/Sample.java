package com.thunderduck.logical;

import com.thunderduck.types.StructType;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Logical plan node representing a sample operation.
 *
 * <p>This node returns a random sample of rows from its child relation.
 * The sampling uses Bernoulli sampling (per-row probabilistic inclusion)
 * to match Spark's sampling semantics.
 *
 * <p>Examples:
 * <pre>
 *   df.sample(0.1)           // Sample ~10% of rows
 *   df.sample(0.1, seed=42)  // Reproducible sampling
 * </pre>
 *
 * <p>SQL generation uses DuckDB's USING SAMPLE clause with bernoulli method:
 * <pre>
 *   SELECT * FROM (child) USING SAMPLE 10% (bernoulli)
 *   SELECT * FROM (child) USING SAMPLE 10% (bernoulli, 42)  -- with seed
 * </pre>
 *
 * <p>Important notes:
 * <ul>
 *   <li>This is a TRANSFORMATION, not an action - returns a DataFrame</li>
 *   <li>Can be chained: df.sample(0.1).filter(...).select(...)</li>
 *   <li>withReplacement=true is NOT supported (no DuckDB equivalent)</li>
 *   <li>Uses Bernoulli sampling to match Spark's per-row probability</li>
 * </ul>
 */
public class Sample extends LogicalPlan {

    private final double fraction;
    private final OptionalLong seed;

    /**
     * Creates a sample node with a seed for reproducibility.
     *
     * @param child the child node
     * @param fraction the fraction of rows to sample (0.0 to 1.0)
     * @param seed the random seed for reproducibility, or empty for non-deterministic
     * @throws IllegalArgumentException if fraction is not in [0.0, 1.0]
     */
    public Sample(LogicalPlan child, double fraction, OptionalLong seed) {
        super(child);
        if (fraction < 0.0 || fraction > 1.0) {
            throw new IllegalArgumentException(
                "fraction must be between 0.0 and 1.0, got: " + fraction);
        }
        this.fraction = fraction;
        this.seed = Objects.requireNonNull(seed, "seed must not be null (use OptionalLong.empty())");
    }

    /**
     * Creates a sample node without a seed (non-deterministic).
     *
     * @param child the child node
     * @param fraction the fraction of rows to sample (0.0 to 1.0)
     */
    public Sample(LogicalPlan child, double fraction) {
        this(child, fraction, OptionalLong.empty());
    }

    /**
     * Returns the sampling fraction.
     *
     * @return the fraction (0.0 to 1.0)
     */
    public double fraction() {
        return fraction;
    }

    /**
     * Returns the random seed if set.
     *
     * @return the seed, or empty if non-deterministic
     */
    public OptionalLong seed() {
        return seed;
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
        String alias = generator.generateSubqueryAlias();

        // Convert fraction to percentage for DuckDB SAMPLE clause
        // DuckDB accepts percentage (e.g., 10%) not fraction (0.1)
        double percentage = fraction * 100.0;

        // Use Bernoulli sampling to match Spark's per-row probability algorithm
        // DuckDB syntax: USING SAMPLE X% (bernoulli) or USING SAMPLE X% (bernoulli, seed)
        if (seed.isPresent()) {
            return String.format(
                "SELECT * FROM (%s) AS %s USING SAMPLE %.6f%% (bernoulli, %d)",
                childSQL, alias, percentage, seed.getAsLong());
        } else {
            return String.format(
                "SELECT * FROM (%s) AS %s USING SAMPLE %.6f%% (bernoulli)",
                childSQL, alias, percentage);
        }
    }

    @Override
    public StructType inferSchema() {
        // Sample doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        if (seed.isPresent()) {
            return String.format("Sample(fraction=%.4f, seed=%d)", fraction, seed.getAsLong());
        } else {
            return String.format("Sample(fraction=%.4f)", fraction);
        }
    }
}
