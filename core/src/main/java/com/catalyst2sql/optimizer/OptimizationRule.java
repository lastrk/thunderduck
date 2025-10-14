package com.catalyst2sql.optimizer;

import com.catalyst2sql.logical.LogicalPlan;

/**
 * Interface for query optimization rules.
 *
 * <p>Optimization rules transform a logical plan into an equivalent plan
 * that should execute more efficiently. Rules are applied iteratively until
 * no more changes occur or a maximum iteration limit is reached.
 *
 * <p>Common optimization rules include:
 * <ul>
 *   <li>Filter pushdown - Move filters closer to data sources</li>
 *   <li>Column pruning - Remove unused columns early</li>
 *   <li>Projection pushdown - Push projections into scans</li>
 *   <li>Join reordering - Reorder joins for better performance</li>
 *   <li>Predicate simplification - Simplify boolean expressions</li>
 * </ul>
 *
 * <p>Rules must preserve query semantics - the optimized plan must
 * produce the same results as the original plan.
 */
public interface OptimizationRule {

    /**
     * Applies this optimization rule to a logical plan.
     *
     * <p>The rule should return a transformed plan if optimizations are
     * applicable, or the original plan if no optimizations apply.
     *
     * <p>Rules should be idempotent - applying the same rule multiple
     * times should not cause further changes after the first application.
     *
     * @param plan the input plan
     * @return the optimized plan (or original if no optimization applied)
     */
    LogicalPlan apply(LogicalPlan plan);

    /**
     * Returns the name of this optimization rule.
     *
     * <p>Used for logging and debugging.
     *
     * @return the rule name
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
