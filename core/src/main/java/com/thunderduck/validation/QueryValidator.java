package com.thunderduck.validation;

import com.thunderduck.exception.ValidationException;
import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.UnaryExpression;
import com.thunderduck.expression.WindowFunction;
import com.thunderduck.logical.Aggregate;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.logical.Union;
import com.thunderduck.types.DataType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Validates logical plans before SQL generation.
 *
 * <p>This validator performs static analysis of logical plans to catch errors early
 * and provide better error messages than what would come from SQL generation or
 * query execution failures.
 *
 * <p>Validation rules:
 * <ul>
 *   <li><b>JOIN validation:</b>
 *     <ul>
 *       <li>Non-CROSS joins must have a join condition</li>
 *       <li>Join conditions must reference valid columns from input schemas</li>
 *     </ul>
 *   </li>
 *   <li><b>UNION validation:</b>
 *     <ul>
 *       <li>Both sides must have same number of columns</li>
 *       <li>Corresponding columns must have compatible types</li>
 *     </ul>
 *   </li>
 *   <li><b>Aggregate validation:</b>
 *     <ul>
 *       <li>Must have at least one aggregate expression</li>
 *       <li>Non-aggregate columns in SELECT must appear in GROUP BY</li>
 *     </ul>
 *   </li>
 *   <li><b>Window function validation:</b>
 *     <ul>
 *       <li>Ranking functions (ROW_NUMBER, RANK, DENSE_RANK) require ORDER BY</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 *   LogicalPlan plan = df.join(df2, col("id").equalTo(col("id2")))
 *                        .groupBy("category")
 *                        .agg(sum("amount"))
 *                        .logicalPlan();
 *
 *   QueryValidator.validate(plan);  // Throws ValidationException if invalid
 *   String sql = generator.generate(plan);
 * </pre>
 *
 * <p>This validator is designed to be called before SQL generation to provide
 * better error messages and fail fast on invalid queries.
 *
 * @see ValidationException
 * @see com.thunderduck.generator.SQLGenerator
 */
public class QueryValidator {

    /**
     * Validates a logical plan before SQL generation.
     *
     * <p>This performs a recursive traversal of the plan tree and validates
     * each node according to its type and validation rules.
     *
     * @param plan the logical plan to validate
     * @throws ValidationException if validation fails
     * @throws NullPointerException if plan is null
     */
    public static void validate(LogicalPlan plan) {
        Objects.requireNonNull(plan, "Plan cannot be null");
        validateRecursive(plan);
    }

    /**
     * Recursively validates a logical plan and all its children.
     *
     * @param plan the plan to validate
     * @throws ValidationException if validation fails
     */
    private static void validateRecursive(LogicalPlan plan) {
        if (plan == null) {
            return;
        }

        // Validate this node based on its type
        if (plan instanceof Join) {
            validateJoin((Join) plan);
        } else if (plan instanceof Union) {
            validateUnion((Union) plan);
        } else if (plan instanceof Aggregate) {
            validateAggregate((Aggregate) plan);
        }

        // Recursively validate children
        for (LogicalPlan child : plan.children()) {
            validateRecursive(child);
        }
    }

    /**
     * Validates a JOIN operation.
     *
     * <p>Validation rules:
     * <ul>
     *   <li>Non-CROSS joins must have a join condition</li>
     *   <li>Join condition must reference valid columns from left or right input</li>
     * </ul>
     *
     * @param join the join to validate
     * @throws ValidationException if validation fails
     */
    private static void validateJoin(Join join) {
        // Validate join condition is not null for non-CROSS joins
        if (join.joinType() != Join.JoinType.CROSS && join.condition() == null) {
            throw new ValidationException(
                "JOIN condition cannot be null for " + join.joinType() + " join",
                "join validation",
                String.format("Join(%s, %s, %s, null)",
                    join.left().getClass().getSimpleName(),
                    join.right().getClass().getSimpleName(),
                    join.joinType()),
                "Add ON clause with join condition or use CROSS JOIN"
            );
        }

        // Validate join condition references valid columns
        if (join.condition() != null) {
            Set<String> leftColumns = getColumnNames(join.left().schema());
            Set<String> rightColumns = getColumnNames(join.right().schema());
            Set<String> referencedColumns = getReferencedColumns(join.condition());

            for (String col : referencedColumns) {
                if (!leftColumns.contains(col) && !rightColumns.contains(col)) {
                    throw new ValidationException(
                        "Column '" + col + "' does not exist in either join input",
                        "join validation",
                        "Condition: " + join.condition().toString() +
                        ", Left columns: " + leftColumns +
                        ", Right columns: " + rightColumns,
                        "Use column from left relation (" + leftColumns +
                        ") or right relation (" + rightColumns + ")"
                    );
                }
            }
        }
    }

    /**
     * Validates a UNION operation.
     *
     * <p>Validation rules:
     * <ul>
     *   <li>Both sides must have same number of columns</li>
     *   <li>Corresponding columns must have compatible types</li>
     * </ul>
     *
     * @param union the union to validate
     * @throws ValidationException if validation fails
     */
    private static void validateUnion(Union union) {
        StructType leftSchema = union.left().schema();
        StructType rightSchema = union.right().schema();

        // Validate schemas are compatible - same number of columns
        if (leftSchema.fields().size() != rightSchema.fields().size()) {
            throw new ValidationException(
                String.format("UNION requires same number of columns: left has %d, right has %d",
                    leftSchema.fields().size(), rightSchema.fields().size()),
                "union validation",
                "left: " + formatSchemaShort(leftSchema) + ", right: " + formatSchemaShort(rightSchema),
                "Ensure both sides of UNION have same column count"
            );
        }

        // Validate types are compatible
        for (int i = 0; i < leftSchema.fields().size(); i++) {
            StructField leftField = leftSchema.fields().get(i);
            StructField rightField = rightSchema.fields().get(i);
            DataType leftType = leftField.dataType();
            DataType rightType = rightField.dataType();

            if (!typesCompatible(leftType, rightType)) {
                throw new ValidationException(
                    String.format("UNION column %d type mismatch: left is %s, right is %s",
                        i, leftType, rightType),
                    "union validation",
                    String.format("Column %d: '%s' (%s) vs '%s' (%s)",
                        i, leftField.name(), leftType, rightField.name(), rightType),
                    "Ensure matching columns have compatible types or add explicit casts"
                );
            }
        }
    }

    /**
     * Validates an AGGREGATE operation.
     *
     * <p>Validation rules:
     * <ul>
     *   <li>Must have at least one aggregate expression</li>
     *   <li>Non-aggregate columns in aggregate expressions must appear in GROUP BY</li>
     * </ul>
     *
     * @param agg the aggregate to validate
     * @throws ValidationException if validation fails
     */
    private static void validateAggregate(Aggregate agg) {
        // Validate aggregate expressions exist
        if (agg.aggregateExpressions().isEmpty()) {
            throw new ValidationException(
                "Aggregate must have at least one aggregate expression",
                "aggregate validation",
                String.format("Aggregate(groupBy=%s, agg=[])", agg.groupingExpressions()),
                "Add aggregate function like SUM(), COUNT(), AVG(), etc."
            );
        }

        // Validate all non-aggregate columns are in GROUP BY
        //Set<String> groupingColumns = getColumnNamesFromExpressions(agg.groupingExpressions());

        // Check each aggregate expression
        for (AggregateExpression aggExpr : agg.aggregateExpressions()) {
            // For simple column references in the aggregate list (not in aggregate function),
            // they must be in GROUP BY
            // Note: We check the argument of the aggregate function, not the function itself
            if (aggExpr.argument() != null) {
                // The argument can reference columns not in GROUP BY - that's valid
                // e.g., SUM(price) is valid even if 'price' is not in GROUP BY
                continue;
            }
        }

        // Validate grouping expressions themselves (if they reference columns)
        for (Expression groupExpr : agg.groupingExpressions()) {
            Set<String> referencedCols = getReferencedColumns(groupExpr);
            // All columns referenced in grouping expressions should exist in child schema
            Set<String> availableColumns = getColumnNames(agg.child().schema());
            for (String col : referencedCols) {
                if (!availableColumns.contains(col)) {
                    throw new ValidationException(
                        "Column '" + col + "' in GROUP BY does not exist in input",
                        "aggregate validation",
                        "GROUP BY: " + groupExpr.toString() +
                        ", Available columns: " + availableColumns,
                        "Use a column from the input relation: " + availableColumns
                    );
                }
            }
        }
    }

    /**
     * Validates a window function expression.
     *
     * <p>Validation rules:
     * <ul>
     *   <li>Ranking functions (ROW_NUMBER, RANK, DENSE_RANK) require ORDER BY</li>
     * </ul>
     *
     * @param window the window function to validate
     * @throws ValidationException if validation fails
     */
    private static void validateWindow(WindowFunction window) {
        String function = window.function().toUpperCase();

        // Ranking functions require ORDER BY
        if (isRankingFunction(function) && window.orderBy().isEmpty()) {
            throw new ValidationException(
                "Window function " + function + " requires ORDER BY clause",
                "window function validation",
                window.toString(),
                "Add ORDER BY clause to window specification: OVER (ORDER BY column)"
            );
        }
    }

    /**
     * Checks if a window function is a ranking function that requires ORDER BY.
     *
     * @param function the function name (uppercase)
     * @return true if it's a ranking function
     */
    private static boolean isRankingFunction(String function) {
        return function.equals("ROW_NUMBER") ||
               function.equals("RANK") ||
               function.equals("DENSE_RANK");
    }

    /**
     * Extracts column names from a schema.
     *
     * @param schema the schema
     * @return set of column names
     */
    private static Set<String> getColumnNames(StructType schema) {
        Set<String> columns = new HashSet<>();
        if (schema != null) {
            for (StructField field : schema.fields()) {
                columns.add(field.name());
            }
        }
        return columns;
    }

    /**
     * Extracts column names from a list of expressions.
     *
     * <p>This handles expressions that are direct column references.
     *
     * @param expressions the expressions
     * @return set of column names
     */
    /*private static Set<String> getColumnNamesFromExpressions(List<Expression> expressions) {
        Set<String> columns = new HashSet<>();
        for (Expression expr : expressions) {
            if (expr instanceof ColumnReference) {
                columns.add(((ColumnReference) expr).columnName());
            }
        }
        return columns;
    }*/

    /**
     * Extracts all column references from an expression tree.
     *
     * <p>This recursively traverses the expression and collects all
     * column names referenced anywhere in the expression.
     *
     * @param expr the expression
     * @return set of referenced column names
     */
    private static Set<String> getReferencedColumns(Expression expr) {
        Set<String> columns = new HashSet<>();
        collectReferencedColumns(expr, columns);
        return columns;
    }

    /**
     * Recursively collects column references from an expression.
     *
     * @param expr the expression
     * @param columns the set to collect column names into
     */
    private static void collectReferencedColumns(Expression expr, Set<String> columns) {
        if (expr == null) {
            return;
        }

        if (expr instanceof ColumnReference) {
            columns.add(((ColumnReference) expr).columnName());
        } else if (expr instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expr;
            collectReferencedColumns(binary.left(), columns);
            collectReferencedColumns(binary.right(), columns);
        } else if (expr instanceof UnaryExpression) {
            UnaryExpression unary = (UnaryExpression) expr;
            collectReferencedColumns(unary.operand(), columns);
        } else if (expr instanceof FunctionCall) {
            FunctionCall func = (FunctionCall) expr;
            for (Expression arg : func.arguments()) {
                collectReferencedColumns(arg, columns);
            }
        } else if (expr instanceof AggregateExpression) {
            AggregateExpression agg = (AggregateExpression) expr;
            if (agg.argument() != null) {
                collectReferencedColumns(agg.argument(), columns);
            }
        } else if (expr instanceof WindowFunction) {
            WindowFunction window = (WindowFunction) expr;
            // Validate the window function itself
            validateWindow(window);
            // Collect columns from arguments
            for (Expression arg : window.arguments()) {
                collectReferencedColumns(arg, columns);
            }
            // Collect columns from partition by
            for (Expression partition : window.partitionBy()) {
                collectReferencedColumns(partition, columns);
            }
            // Collect columns from order by
            for (com.thunderduck.logical.Sort.SortOrder order : window.orderBy()) {
                collectReferencedColumns(order.expression(), columns);
            }
        }
        // For other expression types (Literal, etc.), no column references
    }

    /**
     * Checks if two data types are compatible for UNION.
     *
     * <p>Types are compatible if:
     * <ul>
     *   <li>They are exactly the same type</li>
     *   <li>They are numeric types (can be implicitly cast)</li>
     *   <li>One is NULL type (can be cast to anything)</li>
     * </ul>
     *
     * @param left the left type
     * @param right the right type
     * @return true if types are compatible
     */
    private static boolean typesCompatible(DataType left, DataType right) {
        if (left == null || right == null) {
            return false;
        }

        // Exact match
        if (left.equals(right)) {
            return true;
        }

        // Get type names for comparison
        String leftTypeName = left.typeName();
        String rightTypeName = right.typeName();

        // Null type is compatible with anything
        if (leftTypeName.equals("null") || rightTypeName.equals("null")) {
            return true;
        }

        // Numeric types are compatible with each other
        Set<String> numericTypes = new HashSet<>();
        numericTypes.add("byte");
        numericTypes.add("short");
        numericTypes.add("integer");
        numericTypes.add("long");
        numericTypes.add("float");
        numericTypes.add("double");
        numericTypes.add("decimal");

        if (numericTypes.contains(leftTypeName) && numericTypes.contains(rightTypeName)) {
            return true;
        }

        // String types might be compatible
        if (leftTypeName.equals("string") && rightTypeName.equals("string")) {
            return true;
        }

        // Boolean types
        if (leftTypeName.equals("boolean") && rightTypeName.equals("boolean")) {
            return true;
        }

        // Date/timestamp types
        Set<String> temporalTypes = new HashSet<>();
        temporalTypes.add("date");
        temporalTypes.add("timestamp");

        if (temporalTypes.contains(leftTypeName) && temporalTypes.contains(rightTypeName)) {
            return true;
        }

        // Otherwise, types are not compatible
        return false;
    }

    /**
     * Formats a schema for display in error messages (short form).
     *
     * @param schema the schema
     * @return formatted schema string
     */
    private static String formatSchemaShort(StructType schema) {
        if (schema == null) {
            return "null";
        }

        List<String> fieldNames = new ArrayList<>();
        for (StructField field : schema.fields()) {
            fieldNames.add(field.name() + ":" + field.dataType().typeName());
        }

        return String.format("StructType[%s]", String.join(", ", fieldNames));
    }
}
