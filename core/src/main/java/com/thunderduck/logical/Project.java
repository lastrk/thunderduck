package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
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
        StructType childSchema = child().schema();

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
     * Resolves the data type of an expression, looking up unresolved columns
     * in the child schema.
     */
    private DataType resolveDataType(Expression expr, StructType childSchema) {
        if (expr instanceof UnresolvedColumn) {
            String colName = ((UnresolvedColumn) expr).columnName();
            StructField field = childSchema.fieldByName(colName);
            if (field != null) {
                return field.dataType();
            }
        } else if (expr instanceof FunctionCall) {
            // For function calls, resolve argument types first and then compute result type
            FunctionCall func = (FunctionCall) expr;
            // Functions like abs, sqrt, etc. typically preserve the argument type
            // For now, try to get the type from the first argument if it's a column
            if (!func.arguments().isEmpty()) {
                DataType argType = resolveDataType(func.arguments().get(0), childSchema);
                // Many numeric functions preserve the argument type
                return argType;
            }
        }
        // Fall back to expression's declared type
        return expr.dataType();
    }

    /**
     * Resolves the nullability of an expression, looking up unresolved columns
     * in the child schema.
     */
    private boolean resolveNullable(Expression expr, StructType childSchema) {
        if (expr instanceof UnresolvedColumn) {
            String colName = ((UnresolvedColumn) expr).columnName();
            StructField field = childSchema.fieldByName(colName);
            if (field != null) {
                return field.nullable();
            }
        } else if (expr instanceof FunctionCall) {
            // Function calls are typically nullable if any argument is nullable
            FunctionCall func = (FunctionCall) expr;
            for (Expression arg : func.arguments()) {
                if (resolveNullable(arg, childSchema)) {
                    return true;
                }
            }
            return false;
        }
        return expr.nullable();
    }

    @Override
    public String toString() {
        return String.format("Project(%s)", projections);
    }
}
