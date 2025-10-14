package com.catalyst2sql.optimizer;

import com.catalyst2sql.logical.LogicalPlan;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Query optimizer that applies optimization rules to logical plans.
 *
 * <p>The optimizer applies rules iteratively until no more changes occur
 * or a maximum iteration limit is reached. This allows rules to enable
 * each other (e.g., filter pushdown enabling column pruning).
 *
 * <p>Example usage:
 * <pre>
 *   QueryOptimizer optimizer = new QueryOptimizer();
 *   LogicalPlan optimized = optimizer.optimize(plan);
 * </pre>
 *
 * <p>The optimizer includes these rules by default:
 * <ul>
 *   <li>Filter pushdown - Move filters closer to sources</li>
 *   <li>Column pruning - Remove unused columns</li>
 *   <li>Projection pushdown - Push projections into scans</li>
 *   <li>Join reordering - Reorder joins for efficiency</li>
 * </ul>
 */
public class QueryOptimizer {

    private final List<OptimizationRule> rules;
    private final int maxIterations;

    /**
     * Creates a query optimizer with default rules and max iterations.
     */
    public QueryOptimizer() {
        this(createDefaultRules(), 10);
    }

    /**
     * Creates a query optimizer with custom rules and max iterations.
     *
     * @param rules the optimization rules to apply
     * @param maxIterations the maximum number of iterations
     */
    public QueryOptimizer(List<OptimizationRule> rules, int maxIterations) {
        this.rules = new ArrayList<>(rules);
        this.maxIterations = maxIterations;
    }

    /**
     * Optimizes a logical plan by applying optimization rules.
     *
     * <p>Rules are applied iteratively until no changes occur or max
     * iterations is reached. Each iteration applies all rules in order.
     *
     * @param plan the input plan
     * @return the optimized plan
     */
    public LogicalPlan optimize(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }

        LogicalPlan currentPlan = plan;

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            LogicalPlan previousPlan = currentPlan;

            // Apply each rule in sequence
            for (OptimizationRule rule : rules) {
                currentPlan = rule.apply(currentPlan);
                if (currentPlan == null) {
                    // Rule returned null, stop optimization
                    return previousPlan;
                }
            }

            // Check if plan changed
            if (currentPlan == previousPlan || currentPlan.equals(previousPlan)) {
                // No changes in this iteration, optimization complete
                break;
            }
        }

        return currentPlan;
    }

    /**
     * Creates the default set of optimization rules.
     *
     * @return the default rules
     */
    private static List<OptimizationRule> createDefaultRules() {
        return Arrays.asList(
            new FilterPushdownRule(),
            new ColumnPruningRule(),
            new ProjectionPushdownRule(),
            new JoinReorderingRule()
        );
    }

    /**
     * Returns the list of optimization rules.
     *
     * @return the rules
     */
    public List<OptimizationRule> rules() {
        return new ArrayList<>(rules);
    }

    /**
     * Returns the maximum number of iterations.
     *
     * @return the max iterations
     */
    public int maxIterations() {
        return maxIterations;
    }
}
