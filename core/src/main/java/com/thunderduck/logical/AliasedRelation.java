package com.thunderduck.logical;

import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.types.StructType;
import java.util.Collections;
import java.util.Objects;

/**
 * Logical plan node representing a relation with a user-provided alias.
 *
 * <p>This is used for DataFrame.alias() operations. Unlike SQLRelation which
 * wraps everything in SQL text, AliasedRelation preserves the alias information
 * so that joins can reference it directly in the join condition.
 *
 * <p>Example:
 * <pre>
 *   date_dim.alias("d1").join(date_dim.alias("d2"), col("d1.d_date_sk") == col("d2.d_date_sk"))
 * </pre>
 *
 * <p>The aliases "d1" and "d2" must be accessible in the join condition, so we can't
 * wrap them in another subquery with a generated alias. This class preserves the
 * user alias so Join.toSQL() can use it directly.
 */
public class AliasedRelation extends LogicalPlan {

    private final LogicalPlan child;
    private final String alias;

    /**
     * Creates an aliased relation.
     *
     * @param child the underlying relation
     * @param alias the user-provided alias
     */
    public AliasedRelation(LogicalPlan child, String alias) {
        super(Collections.singletonList(child));
        this.child = Objects.requireNonNull(child, "child must not be null");
        this.alias = Objects.requireNonNull(alias, "alias must not be null");
        if (alias.isEmpty()) {
            throw new IllegalArgumentException("alias must not be empty");
        }
    }

    /**
     * Returns the child relation.
     *
     * @return the child plan
     */
    public LogicalPlan child() {
        return child;
    }

    /**
     * Returns the user-provided alias.
     *
     * @return the alias
     */
    public String alias() {
        return alias;
    }

    @Override
    public String toSQL(SQLGenerator generator) {
        // Generate SQL with explicit alias
        String childSql = generator.generate(child);
        return String.format("SELECT * FROM (%s) AS %s",
            childSql, SQLQuoting.quoteIdentifier(alias));
    }

    @Override
    public StructType inferSchema() {
        // Delegate to child
        return child.schema();
    }

    @Override
    public String toString() {
        return String.format("AliasedRelation[%s]", alias);
    }
}
