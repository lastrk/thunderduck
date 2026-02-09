package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.StructType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a sort (ORDER BY clause).
 *
 * <p>This node sorts rows from its child based on one or more sort orders.
 *
 * <p>Examples:
 * <pre>
 *   df.orderBy("name")
 *   df.sort(col("age").desc(), col("name").asc())
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT * FROM (child) ORDER BY expr1 ASC, expr2 DESC, ...</pre>
 */
public final class Sort extends LogicalPlan {

    private final List<SortOrder> sortOrders;

    /**
     * Creates a sort node.
     *
     * @param child the child node
     * @param sortOrders the sort orders
     */
    public Sort(LogicalPlan child, List<SortOrder> sortOrders) {
        super(child);
        this.sortOrders = new ArrayList<>(Objects.requireNonNull(sortOrders, "sortOrders must not be null"));

        if (this.sortOrders.isEmpty()) {
            throw new IllegalArgumentException("sortOrders must not be empty");
        }
    }

    /**
     * Returns the sort orders.
     *
     * @return an unmodifiable list of sort orders
     */
    public List<SortOrder> sortOrders() {
        return Collections.unmodifiableList(sortOrders);
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

        // Check if child aggregate uses ROLLUP/CUBE/GROUPING SETS.
        // These produce NULL values for subtotal/grand-total rows, which must sort first
        // to match Spark's behavior. Force NULLS FIRST on all sort columns.
        boolean forceNullsFirst = hasRollupDescendant(child());

        List<String> orderClauses = new ArrayList<>();
        for (SortOrder order : sortOrders) {
            StringBuilder clause = new StringBuilder();
            clause.append(order.expression().toSQL());

            // Add direction
            if (order.direction() == SortDirection.DESCENDING) {
                clause.append(" DESC");
            } else {
                clause.append(" ASC");
            }

            // Add null ordering: force NULLS FIRST when ROLLUP is detected
            if (forceNullsFirst || order.nullOrdering() == NullOrdering.NULLS_FIRST) {
                clause.append(" NULLS FIRST");
            } else {
                clause.append(" NULLS LAST");
            }

            orderClauses.add(clause.toString());
        }

        return String.format("SELECT * FROM (%s) AS %s ORDER BY %s",
            childSQL, generator.generateSubqueryAlias(), String.join(", ", orderClauses));
    }

    /**
     * Checks if a descendant logical plan node contains an Aggregate with ROLLUP,
     * CUBE, or GROUPING SETS grouping type.
     *
     * <p>Walks through transparent intermediate nodes (Project, Filter, Limit)
     * that can appear between a Sort and its underlying Aggregate.
     *
     * @param plan the child plan node to inspect
     * @return true if a ROLLUP/CUBE/GROUPING SETS aggregate is found
     */
    private static boolean hasRollupDescendant(LogicalPlan plan) {
        if (plan instanceof Aggregate agg) {
            return agg.groupingSets() != null;
        }
        if (plan instanceof Project p) return hasRollupDescendant(p.child());
        if (plan instanceof Filter f) return hasRollupDescendant(f.child());
        if (plan instanceof Limit l) return hasRollupDescendant(l.child());
        return false;
    }

    @Override
    public StructType inferSchema() {
        // Sort doesn't change the schema
        return child().schema();
    }

    @Override
    public String toString() {
        return String.format("Sort(%s)", sortOrders);
    }

    /**
     * Represents a sort order (expression + direction + null handling).
     */
    public static class SortOrder {
        private final Expression expression;
        private final SortDirection direction;
        private final NullOrdering nullOrdering;

        public SortOrder(Expression expression, SortDirection direction, NullOrdering nullOrdering) {
            this.expression = Objects.requireNonNull(expression);
            this.direction = Objects.requireNonNull(direction);
            this.nullOrdering = Objects.requireNonNull(nullOrdering);
        }

        public SortOrder(Expression expression, SortDirection direction) {
            this(expression, direction,
                 direction == SortDirection.ASCENDING ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST);
        }

        public Expression expression() {
            return expression;
        }

        public SortDirection direction() {
            return direction;
        }

        public NullOrdering nullOrdering() {
            return nullOrdering;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s", expression, direction, nullOrdering);
        }
    }

    /**
     * Sort direction.
     */
    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    /**
     * Null ordering.
     */
    public enum NullOrdering {
        NULLS_FIRST,
        NULLS_LAST
    }
}
