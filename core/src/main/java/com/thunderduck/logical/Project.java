package com.thunderduck.logical;

import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.StarExpression;
import com.thunderduck.types.DataType;
import com.thunderduck.types.MapType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import com.thunderduck.types.TypeInferenceEngine;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public final class Project extends LogicalPlan {

    // Pre-compiled regex patterns for extractTrailingAlias()
    private static final Pattern TRAILING_ALIAS_SIMPLE =
        Pattern.compile("\\s+[Aa][Ss]\\s+([A-Za-z_][A-Za-z0-9_]*)\\s*$");
    private static final Pattern TRAILING_ALIAS_BACKTICK =
        Pattern.compile("\\s+[Aa][Ss]\\s+`([^`]+)`\\s*$");
    private static final Pattern TRAILING_ALIAS_DQUOTE =
        Pattern.compile("\\s+[Aa][Ss]\\s+\"([^\"]+)\"\\s*$");

    // Pre-compiled regex pattern for normalizeTypeCaseInColumnName()
    private static final Pattern CAST_TYPE_PATTERN = Pattern.compile(
        "(?i)(CAST\\([^)]*\\bAS\\s+)(decimal|int|integer|bigint|double|float|string|boolean|date|timestamp|tinyint|smallint)((?:\\([^)]*\\))?\\))");

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

            // Star expression (*) expands to all child schema columns
            if (expr instanceof StarExpression) {
                if (childSchema != null) {
                    for (StructField childField : childSchema.fields()) {
                        fields.add(childField);
                    }
                }
                continue;
            }

            // explode(map_col) on MAP expands to two columns: key and value.
            // Spark's explode on MAP produces columns named "key" and "value".
            if (expr instanceof FunctionCall fc
                    && fc.functionName().equalsIgnoreCase("explode")
                    && fc.argumentCount() == 1) {
                DataType argType = TypeInferenceEngine.resolveType(fc.arguments().get(0), childSchema);
                if (argType instanceof MapType mapType) {
                    fields.add(new StructField("key", mapType.keyType(), false));
                    fields.add(new StructField("value", mapType.valueType(), mapType.valueContainsNull()));
                    continue;
                }
            }

            String alias = aliases.get(i);
            String fieldName;
            if (alias != null) {
                fieldName = alias;
            } else if (expr instanceof com.thunderduck.expression.AliasExpression ae) {
                fieldName = ae.alias();
            } else if (expr instanceof com.thunderduck.expression.UnresolvedColumn uc) {
                fieldName = uc.columnName();
            } else {
                fieldName = buildSparkColumnName(expr);
            }

            // Resolve data type and nullable from child schema
            DataType resolvedType = resolveDataType(expr, childSchema);
            boolean resolvedNullable = resolveNullable(expr, childSchema);

            fields.add(new StructField(fieldName, resolvedType, resolvedNullable));
        }
        return new StructType(fields);
    }

    /**
     * Builds Spark-compatible column names for unaliased expressions.
     *
     * <p>Handles special cases:
     * <ul>
     *   <li>count(*) -> "count(1)" (Spark convention)</li>
     *   <li>RawSQLExpression with "AS alias" -> just the alias</li>
     *   <li>CastExpression -> uppercase DECIMAL in type names</li>
     *   <li>Default: use expr.toSQL()</li>
     * </ul>
     */
    public static String buildSparkColumnName(Expression expr) {
        // count(*) → Spark names this "count(1)"
        if (expr instanceof com.thunderduck.expression.FunctionCall fc
                && fc.functionName().equalsIgnoreCase("count")) {
            java.util.List<Expression> args = fc.arguments();
            if (args.isEmpty()) {
                return "count(1)";
            }
            if (args.size() == 1) {
                Expression arg = args.get(0);
                // count(*) with StarExpression argument
                if (arg instanceof com.thunderduck.expression.StarExpression) {
                    return "count(1)";
                }
                // count(*) with Literal("*") argument
                if (arg instanceof com.thunderduck.expression.Literal lit
                        && "*".equals(lit.value())) {
                    return "count(1)";
                }
            }
        }

        // RawSQLExpression: detect embedded "AS alias" and extract just the alias.
        // selectExpr("split(name, ' ') as name_parts") should produce column name "name_parts".
        if (expr instanceof com.thunderduck.expression.RawSQLExpression) {
            String sql = expr.toSQL();
            String extracted = extractTrailingAlias(sql);
            if (extracted != null) {
                return extracted;
            }
            return sql;
        }

        // For other expressions, generate SQL and normalize CAST type names to uppercase
        // to match Spark's convention (e.g., DECIMAL not decimal).
        String sql = expr.toSQL();
        return normalizeTypeCaseInColumnName(sql);
    }

    /**
     * Extracts a trailing alias from a SQL expression string.
     * Matches patterns like "expr AS alias" or "expr as alias" (case-insensitive).
     * Returns null if no trailing alias is found.
     */
    static String extractTrailingAlias(String sql) {
        // Match " AS identifier" or " as identifier" at the end of the expression.
        // The alias can be a simple identifier or a backtick/double-quoted identifier.
        Matcher m = TRAILING_ALIAS_SIMPLE.matcher(sql);
        if (m.find()) {
            return m.group(1);
        }
        // Try backtick-quoted alias
        m = TRAILING_ALIAS_BACKTICK.matcher(sql);
        if (m.find()) {
            return m.group(1);
        }
        // Try double-quoted alias
        m = TRAILING_ALIAS_DQUOTE.matcher(sql);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * Normalizes type names in CAST expressions within column names to uppercase,
     * matching Spark's convention (e.g., DECIMAL not decimal, INT not int).
     */
    static String normalizeTypeCaseInColumnName(String name) {
        // Replace lowercase type names after "AS " in CAST expressions with uppercase.
        // Pattern: CAST(... AS typename) — normalize typename to uppercase.
        return CAST_TYPE_PATTERN.matcher(name)
            .replaceAll(mr -> mr.group(1) + mr.group(2).toUpperCase() + mr.group(3));
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
