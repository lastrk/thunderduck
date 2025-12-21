package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node representing a projection (SELECT clause).
 *
 * <p>This node selects and potentially transforms columns from its child node.
 *
 * <p>Examples:
 * <pre>
 *   df.select("name", "age")
 *   df.select(col("price") * 1.1)
 *   df.withColumn("total", col("price") * col("quantity"))
 * </pre>
 *
 * <p>SQL generation:
 * <pre>SELECT expr1, expr2, ... FROM (child)</pre>
 */
public class Project extends LogicalPlan {

    private final List<Expression> projections;
    private final List<String> aliases;

    /**
     * Creates a projection node.
     *
     * @param child the child node
     * @param projections the projection expressions
     * @param aliases optional aliases for each projection (null for no alias)
     */
    public Project(LogicalPlan child, List<Expression> projections, List<String> aliases) {
        super(child);
        this.projections = new ArrayList<>(Objects.requireNonNull(projections, "projections must not be null"));
        this.aliases = aliases != null ? new ArrayList<>(aliases) : Collections.nCopies(projections.size(), null);

        if (this.projections.isEmpty()) {
            throw new IllegalArgumentException("projections must not be empty");
        }
        if (this.projections.size() != this.aliases.size()) {
            throw new IllegalArgumentException("projections and aliases must have the same size");
        }
    }

    /**
     * Creates a projection node without aliases.
     *
     * @param child the child node
     * @param projections the projection expressions
     */
    public Project(LogicalPlan child, List<Expression> projections) {
        this(child, projections, null);
    }

    /**
     * Returns the projection expressions.
     *
     * @return an unmodifiable list of projections
     */
    public List<Expression> projections() {
        return Collections.unmodifiableList(projections);
    }

    /**
     * Returns the aliases for each projection.
     *
     * @return an unmodifiable list of aliases (null elements for no alias)
     */
    public List<String> aliases() {
        return Collections.unmodifiableList(aliases);
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

        StringBuilder sql = new StringBuilder("SELECT ");

        // Generate projection list
        for (int i = 0; i < projections.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }

            Expression expr = projections.get(i);
            sql.append(expr.toSQL());

            // Add alias if provided
            String alias = aliases.get(i);
            if (alias != null && !alias.isEmpty()) {
                sql.append(" AS ");
                sql.append(com.thunderduck.generator.SQLQuoting.quoteIdentifier(alias));
            }
        }

        // Add FROM clause from child
        sql.append(" FROM (");
        sql.append(generator.generate(child()));
        sql.append(") AS subquery");

        return sql.toString();
    }

    @Override
    public StructType inferSchema() {
        // Get child schema for resolving column types
        StructType childSchema = null;
        try {
            childSchema = child().schema();
        } catch (Exception e) {
            // Child schema resolution failed - return null to trigger DuckDB inference
            return null;
        }

        // If child schema is null, return null to trigger DuckDB-based inference
        if (childSchema == null) {
            return null;
        }

        List<StructField> fields = new ArrayList<>();
        for (int i = 0; i < projections.size(); i++) {
            Expression expr = projections.get(i);
            String alias = aliases.get(i);
            String fieldName = (alias != null) ? alias : ("col_" + i);

            // Resolve data type and nullable from child schema
            DataType resolvedType = resolveDataType(expr, childSchema);
            boolean resolvedNullable = resolveNullable(expr, childSchema);

            fields.add(new StructField(fieldName, resolvedType, resolvedNullable));
        }
        return new StructType(fields);
    }

    /**
     * Resolves the data type of an expression using the centralized TypeInferenceEngine.
     */
    private DataType resolveDataType(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveType(expr, childSchema);
    }

    /**
     * Resolves the nullability of an expression using the centralized TypeInferenceEngine.
     */
    private boolean resolveNullable(Expression expr, StructType childSchema) {
        return TypeInferenceEngine.resolveNullable(expr, childSchema);
    }

    @Override
    public String toString() {
        return String.format("Project(%s)", projections);
    }
}
