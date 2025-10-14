package com.catalyst2sql.logical;

/**
 * Interface for SQL generation from logical plans.
 *
 * <p>The actual SQL generator implementation is provided in the generator package.
 * This interface is used to maintain a clean separation between the logical plan
 * representation and the SQL generation logic.
 *
 * <p>Implementations should provide methods to translate logical plan nodes to
 * DuckDB SQL strings.
 */
public interface SQLGenerator {

    /**
     * Generates SQL for a logical plan node.
     *
     * @param plan the logical plan to translate
     * @return the generated DuckDB SQL string
     */
    String generate(LogicalPlan plan);

    /**
     * Generates a unique subquery alias.
     *
     * @return a unique alias like "subquery_1", "subquery_2", etc.
     */
    String generateSubqueryAlias();
}
