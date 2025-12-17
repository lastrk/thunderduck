package com.thunderduck.connect.converter;

import com.thunderduck.expression.*;
import com.thunderduck.expression.window.*;
import com.thunderduck.logical.Sort;
import com.thunderduck.types.DataType;
import com.thunderduck.types.*;
import com.thunderduck.types.IntegerType;

import org.apache.spark.connect.proto.Expression.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Converts Spark Connect Expression types to thunderduck Expression objects.
 *
 * <p>This class handles all expression types including literals, column references,
 * functions, arithmetic operations, comparisons, casts, etc.
 */
public class ExpressionConverter {
    private static final Logger logger = LoggerFactory.getLogger(ExpressionConverter.class);

    /**
     * Stack of lambda variable scopes for nested lambda handling.
     * Each scope contains the variable names bound in that lambda.
     */
    private final Stack<Set<String>> lambdaScopes = new Stack<>();

    public ExpressionConverter() {
    }

    /**
     * Converts a Spark Connect Expression to a thunderduck Expression.
     *
     * @param expr the Protobuf expression
     * @return the converted Expression
     * @throws PlanConversionException if conversion fails
     */
    public com.thunderduck.expression.Expression convert(org.apache.spark.connect.proto.Expression expr) {
        logger.trace("Converting expression type: {}", expr.getExprTypeCase());

        switch (expr.getExprTypeCase()) {
            case LITERAL:
                return convertLiteral(expr.getLiteral());
            case UNRESOLVED_ATTRIBUTE:
                return convertUnresolvedAttribute(expr.getUnresolvedAttribute());
            case UNRESOLVED_FUNCTION:
                return convertUnresolvedFunction(expr.getUnresolvedFunction());
            case ALIAS:
                return convertAlias(expr.getAlias());
            case CAST:
                return convertCast(expr.getCast());
            case UNRESOLVED_STAR:
                return convertUnresolvedStar(expr.getUnresolvedStar());
            case EXPRESSION_STRING:
                return convertExpressionString(expr.getExpressionString());
            case WINDOW:
                return convertWindow(expr.getWindow());
            case SORT_ORDER:
                // SortOrder is handled specially in RelationConverter
                throw new PlanConversionException("SortOrder should be handled by RelationConverter");
            case LAMBDA_FUNCTION:
                return convertLambdaFunction(expr.getLambdaFunction());
            case UNRESOLVED_NAMED_LAMBDA_VARIABLE:
                return convertUnresolvedNamedLambdaVariable(expr.getUnresolvedNamedLambdaVariable());
            case CALL_FUNCTION:
                return convertCallFunction(expr.getCallFunction());
            case UNRESOLVED_EXTRACT_VALUE:
                return convertUnresolvedExtractValue(expr.getUnresolvedExtractValue());
            case UNRESOLVED_REGEX:
                return convertUnresolvedRegex(expr.getUnresolvedRegex());
            case UPDATE_FIELDS:
                return convertUpdateFields(expr.getUpdateFields());
            default:
                throw new PlanConversionException("Unsupported expression type: " + expr.getExprTypeCase());
        }
    }

    /**
     * Converts a Literal expression.
     */
    private com.thunderduck.expression.Expression convertLiteral(org.apache.spark.connect.proto.Expression.Literal literal) {
        switch (literal.getLiteralTypeCase()) {
            case NULL:
                return new com.thunderduck.expression.Literal(null, convertDataType(literal.getNull()));
            case BINARY:
                return new com.thunderduck.expression.Literal(
                    literal.getBinary().toByteArray(), BinaryType.get());
            case BOOLEAN:
                return new com.thunderduck.expression.Literal(literal.getBoolean(), BooleanType.get());
            case BYTE:
                return new com.thunderduck.expression.Literal((byte) literal.getByte(), ByteType.get());
            case SHORT:
                return new com.thunderduck.expression.Literal((short) literal.getShort(), ShortType.get());
            case INTEGER:
                return new com.thunderduck.expression.Literal(literal.getInteger(), IntegerType.get());
            case LONG:
                return new com.thunderduck.expression.Literal(literal.getLong(), LongType.get());
            case FLOAT:
                return new com.thunderduck.expression.Literal(literal.getFloat(), FloatType.get());
            case DOUBLE:
                return new com.thunderduck.expression.Literal(literal.getDouble(), DoubleType.get());
            case STRING:
                return new com.thunderduck.expression.Literal(literal.getString(), StringType.get());
            case DATE:
                // Date is stored as days since epoch
                return new com.thunderduck.expression.Literal(literal.getDate(), DateType.get());
            case TIMESTAMP:
                // Timestamp is stored as microseconds since epoch
                return new com.thunderduck.expression.Literal(literal.getTimestamp(), TimestampType.get());
            case DECIMAL:
                org.apache.spark.connect.proto.Expression.Literal.Decimal decimal = literal.getDecimal();
                return new com.thunderduck.expression.Literal(
                    decimal.getValue(),
                    new DecimalType(decimal.getPrecision(), decimal.getScale()));
            default:
                throw new PlanConversionException("Unsupported literal type: " + literal.getLiteralTypeCase());
        }
    }

    /**
     * Converts an UnresolvedAttribute (column reference).
     *
     * <p>Handles nested struct field access patterns like:
     * <ul>
     *   <li>simple column: "column" → UnresolvedColumn("column")</li>
     *   <li>qualified: "table.column" → UnresolvedColumn("column", "table")</li>
     *   <li>nested field: "column.field" → ExtractValue(UnresolvedColumn("column"), "field")</li>
     *   <li>deep nested: "col.a.b.c" → ExtractValue(ExtractValue(ExtractValue(UnresolvedColumn("col"), "a"), "b"), "c")</li>
     * </ul>
     */
    private com.thunderduck.expression.Expression convertUnresolvedAttribute(UnresolvedAttribute attr) {
        String identifier = attr.getUnparsedIdentifier();

        // Handle qualified names (table.column or nested fields)
        String[] parts = identifier.split("\\.");

        if (parts.length == 1) {
            // Simple column reference
            return new UnresolvedColumn(parts[0]);
        } else if (parts.length == 2) {
            // Could be table.column or column.field
            // We can't easily distinguish without schema, so treat as table.column
            // However, if the first part matches a known column, it's struct access
            // For now, assume column.field pattern (struct access) for consistency
            // This generates struct_extract which works for both cases
            com.thunderduck.expression.Expression base = new UnresolvedColumn(parts[0]);
            return ExtractValueExpression.structField(base, parts[1]);
        } else {
            // Nested field access: col.field1.field2.field3
            // Build chained ExtractValueExpression calls
            com.thunderduck.expression.Expression result = new UnresolvedColumn(parts[0]);
            for (int i = 1; i < parts.length; i++) {
                result = ExtractValueExpression.structField(result, parts[i]);
            }
            return result;
        }
    }

    /**
     * Converts an UnresolvedFunction (function call).
     */
    /**
     * Maps Spark function names to DuckDB equivalents.
     * This is critical for DataFrame API compatibility.
     */
    private String mapFunctionName(String sparkFunctionName) {
        String upperName = sparkFunctionName.toUpperCase();

        switch (upperName) {
            // String functions
            case "ENDSWITH":
                return "ENDS_WITH";
            case "STARTSWITH":
                return "STARTS_WITH";
            case "CONTAINS":
                return "CONTAINS";
            case "SUBSTRING":
                return "SUBSTR";
            case "RLIKE":
                return "REGEXP_MATCHES";

            // Date/time functions
            case "YEAR":
                return "YEAR";
            case "MONTH":
                return "MONTH";
            case "DAY":
                return "DAY";
            case "DAYOFMONTH":
                return "DAY";
            case "DAYOFWEEK":
                return "DAYOFWEEK";
            case "DAYOFYEAR":
                return "DAYOFYEAR";
            case "HOUR":
                return "HOUR";
            case "MINUTE":
                return "MINUTE";
            case "SECOND":
                return "SECOND";
            case "DATE_ADD":
                return "DATE_ADD";
            case "DATE_SUB":
                return "DATE_SUB";
            case "DATEDIFF":
                return "DATE_DIFF";

            // Math functions
            case "RAND":
                return "RANDOM";
            case "POW":
                return "POWER";
            case "LOG":
                return "LN";  // Natural log in DuckDB
            case "LOG10":
                return "LOG10";
            case "LOG2":
                return "LOG2";

            // Aggregate functions
            case "STDDEV":
                return "STDDEV_SAMP";
            case "STDDEV_POP":
                return "STDDEV_POP";
            case "STDDEV_SAMP":
                return "STDDEV_SAMP";
            case "VAR_POP":
                return "VAR_POP";
            case "VAR_SAMP":
                return "VAR_SAMP";
            case "VARIANCE":
                return "VAR_SAMP";
            case "COLLECT_LIST":
                return "LIST";
            case "COLLECT_SET":
                return "LIST";  // Note: Will need DISTINCT handling

            // Default: return as-is (most functions are compatible)
            default:
                return upperName;
        }
    }

    private com.thunderduck.expression.Expression convertUnresolvedFunction(UnresolvedFunction func) {
        // Don't map here - FunctionRegistry handles the mapping
        String functionName = func.getFunctionName();
        List<com.thunderduck.expression.Expression> arguments = func.getArgumentsList().stream()
                .map(this::convert)
                .collect(Collectors.toList());

        logger.debug("Processing function: {}", functionName);

        // Handle special cases - check with uppercase for operators
        String upperName = functionName.toUpperCase();

        // Handle ISIN function (IN clause)
        if (upperName.equals("ISIN") || upperName.equals("IN")) {
            return convertIsIn(arguments);
        }

        // Handle CASE/WHEN/OTHERWISE expressions
        if (upperName.equals("WHEN") || upperName.equals("CASE_WHEN")) {
            return convertCaseWhen(arguments);
        }

        // Handle OTHERWISE as ELSE in CASE expression
        if (upperName.equals("OTHERWISE")) {
            // OTHERWISE is typically the last part of a WHEN expression
            // It will be handled as part of the WHEN conversion
            if (arguments.size() != 2) {
                throw new PlanConversionException("OTHERWISE requires exactly 2 arguments");
            }
            // Convert to a nested CASE expression
            return convertCaseWhen(arguments);
        }

        // Handle date extraction functions - these should work with direct mapping
        // but we'll add explicit handling to ensure compatibility
        if (isDateExtractFunction(upperName)) {
            return handleDateExtractFunction(functionName, arguments);
        }

        // Handle date arithmetic functions
        if (isDateArithmeticFunction(upperName)) {
            return handleDateArithmeticFunction(functionName, arguments);
        }

        // Handle window functions - these need special treatment
        if (isWindowFunction(upperName)) {
            return handleWindowFunction(functionName, arguments);
        }

        // Handle higher-order array functions with special emulation
        if (isHigherOrderFunction(upperName)) {
            return handleHigherOrderFunction(upperName, arguments);
        }

        if (isBinaryOperator(upperName)) {
            if (arguments.size() != 2) {
                throw new PlanConversionException("Binary operator " + functionName + " requires exactly 2 arguments");
            }
            return mapBinaryOperator(upperName, arguments.get(0), arguments.get(1));
        }

        if (isUnaryOperator(upperName)) {
            if (arguments.size() != 1) {
                throw new PlanConversionException("Unary operator " + functionName + " requires exactly 1 argument");
            }
            return mapUnaryOperator(upperName, arguments.get(0));
        }

        // Handle aggregate functions with DISTINCT
        if (func.getIsDistinct()) {
            functionName = functionName + "_DISTINCT";
        }

        logger.trace("Creating function call: {} with {} arguments", functionName, arguments.size());
        // FunctionCall requires DataType - use StringType as default for now
        // TODO: Infer proper return type based on function
        return new FunctionCall(functionName, arguments, StringType.get());
    }

    /**
     * Checks if the function is a date extraction function.
     */
    private boolean isDateExtractFunction(String functionName) {
        switch (functionName) {
            case "YEAR":
            case "MONTH":
            case "DAY":
            case "DAYOFMONTH":
            case "DAYOFWEEK":
            case "DAYOFYEAR":
            case "HOUR":
            case "MINUTE":
            case "SECOND":
            case "QUARTER":
            case "WEEKOFYEAR":
                return true;
            default:
                return false;
        }
    }

    /**
     * Handles date extraction functions.
     * These functions extract date/time components from date/timestamp columns.
     */
    private com.thunderduck.expression.Expression handleDateExtractFunction(
            String functionName, List<com.thunderduck.expression.Expression> arguments) {
        if (arguments.size() != 1) {
            throw new PlanConversionException(
                "Date extraction function " + functionName + " requires exactly 1 argument");
        }

        // Create a FunctionCall with proper return type (IntegerType for date parts)
        return new FunctionCall(functionName.toLowerCase(), arguments, IntegerType.get());
    }

    /**
     * Checks if the function is a date arithmetic function.
     */
    private boolean isDateArithmeticFunction(String functionName) {
        switch (functionName) {
            case "DATE_ADD":
            case "DATE_SUB":
            case "DATEDIFF":
            case "ADD_MONTHS":
                return true;
            default:
                return false;
        }
    }

    /**
     * Handles date arithmetic functions.
     * These functions perform arithmetic operations on dates.
     */
    private com.thunderduck.expression.Expression handleDateArithmeticFunction(
            String functionName, List<com.thunderduck.expression.Expression> arguments) {
        String lowerName = functionName.toLowerCase();

        // Most date arithmetic functions take 2 arguments
        // datediff might take 2 or 3 (with optional unit)
        if (arguments.size() < 2) {
            throw new PlanConversionException(
                "Date arithmetic function " + functionName + " requires at least 2 arguments");
        }

        // Return type depends on the function
        DataType returnType;
        if (lowerName.equals("datediff")) {
            returnType = IntegerType.get();  // Returns number of days
        } else {
            returnType = DateType.get();  // Returns a date
        }

        return new FunctionCall(lowerName, arguments, returnType);
    }

    /**
     * Converts ISIN function to SQL IN clause.
     * First argument is the column/expression, rest are the values to check.
     */
    private com.thunderduck.expression.Expression convertIsIn(List<com.thunderduck.expression.Expression> arguments) {
        if (arguments.size() < 2) {
            throw new PlanConversionException("ISIN requires at least 2 arguments (expression and at least one value)");
        }

        StringBuilder inExpr = new StringBuilder();
        inExpr.append(arguments.get(0).toSQL()).append(" IN (");

        for (int i = 1; i < arguments.size(); i++) {
            if (i > 1) {
                inExpr.append(", ");
            }
            inExpr.append(arguments.get(i).toSQL());
        }

        inExpr.append(")");
        return new RawSQLExpression(inExpr.toString());
    }

    /**
     * Converts CASE/WHEN/OTHERWISE expressions into SQL CASE statements.
     * Spark sends these as nested function calls.
     */
    private com.thunderduck.expression.Expression convertCaseWhen(List<com.thunderduck.expression.Expression> arguments) {
        if (arguments.size() < 2) {
            throw new PlanConversionException("WHEN requires at least 2 arguments (condition and value)");
        }

        // Build a CASE expression
        // Format: CASE WHEN condition THEN value [WHEN ... THEN ...] [ELSE else_value] END
        StringBuilder caseExpr = new StringBuilder("CASE ");

        // Process WHEN/THEN pairs
        for (int i = 0; i < arguments.size() - 1; i += 2) {
            caseExpr.append("WHEN ").append(arguments.get(i).toSQL())
                   .append(" THEN ").append(arguments.get(i + 1).toSQL()).append(" ");
        }

        // If there's an odd number of arguments, the last one is the ELSE clause
        if (arguments.size() % 2 == 1) {
            caseExpr.append("ELSE ").append(arguments.get(arguments.size() - 1).toSQL()).append(" ");
        }

        caseExpr.append("END");

        return new RawSQLExpression(caseExpr.toString());
    }

    /**
     * Converts an Alias expression.
     */
    private com.thunderduck.expression.Expression convertAlias(Alias alias) {
        com.thunderduck.expression.Expression expr = convert(alias.getExpr());

        // Get the alias name
        List<String> names = alias.getNameList();
        if (names.isEmpty()) {
            throw new PlanConversionException("Alias must have at least one name");
        }

        // For scalar columns, use the first name
        String aliasName = names.get(0);
        return new AliasExpression(expr, aliasName);
    }

    /**
     * Converts a Cast expression.
     */
    private com.thunderduck.expression.Expression convertCast(Cast cast) {
        com.thunderduck.expression.Expression expr = convert(cast.getExpr());

        DataType targetType = null;
        if (cast.hasType()) {
            targetType = convertDataType(cast.getType());
        } else if (cast.hasTypeStr()) {
            targetType = parseTypeString(cast.getTypeStr());
        } else {
            throw new PlanConversionException("Cast must specify target type");
        }

        return new CastExpression(expr, targetType);
    }

    /**
     * Converts an UnresolvedStar (SELECT *).
     */
    private com.thunderduck.expression.Expression convertUnresolvedStar(UnresolvedStar star) {
        // Handle qualified star (table.*)
        if (star.hasUnparsedTarget() && !star.getUnparsedTarget().isEmpty()) {
            String target = star.getUnparsedTarget();
            return new StarExpression(target);
        }
        // Unqualified star
        return new StarExpression();
    }

    /**
     * Converts an ExpressionString (SQL expression as string).
     */
    private com.thunderduck.expression.Expression convertExpressionString(ExpressionString exprString) {
        // For now, wrap it as a raw SQL expression
        return new RawSQLExpression(exprString.getExpression());
    }

    /**
     * Checks if the function name is a binary operator.
     */
    private boolean isBinaryOperator(String functionName) {
        switch (functionName) {
            case "+":
            case "-":
            case "*":
            case "/":
            case "%":
            case "=":
            case "==":
            case "!=":
            case "<>":
            case "<":
            case "<=":
            case ">":
            case ">=":
            case "AND":
            case "OR":
            case "&&":
            case "||":
                return true;
            default:
                return false;
        }
    }

    /**
     * Checks if the function name is a unary operator.
     */
    private boolean isUnaryOperator(String functionName) {
        switch (functionName) {
            case "-":
            case "NOT":
            case "!":
            case "~":
            case "ISNULL":
            case "ISNOTNULL":
                return true;
            default:
                return false;
        }
    }

    /**
     * Maps a function name to a UnaryExpression.
     */
    private com.thunderduck.expression.Expression mapUnaryOperator(String functionName,
            com.thunderduck.expression.Expression operand) {
        switch (functionName) {
            case "-":
                return UnaryExpression.negate(operand);
            case "NOT":
            case "!":
                return UnaryExpression.not(operand);
            case "ISNULL":
                return UnaryExpression.isNull(operand);
            case "ISNOTNULL":
                return UnaryExpression.isNotNull(operand);
            default:
                throw new PlanConversionException("Unsupported unary operator: " + functionName);
        }
    }

    /**
     * Maps a function name to a BinaryExpression.
     */
    private com.thunderduck.expression.Expression mapBinaryOperator(String functionName,
            com.thunderduck.expression.Expression left, com.thunderduck.expression.Expression right) {
        switch (functionName) {
            case "+":
                return BinaryExpression.add(left, right);
            case "-":
                return BinaryExpression.subtract(left, right);
            case "*":
                return BinaryExpression.multiply(left, right);
            case "/":
                return BinaryExpression.divide(left, right);
            case "%":
                return BinaryExpression.modulo(left, right);
            case "=":
            case "==":
                return BinaryExpression.equal(left, right);
            case "!=":
            case "<>":
                return BinaryExpression.notEqual(left, right);
            case "<":
                return BinaryExpression.lessThan(left, right);
            case "<=":
                return new BinaryExpression(left, BinaryExpression.Operator.LESS_THAN_OR_EQUAL, right);
            case ">":
                return BinaryExpression.greaterThan(left, right);
            case ">=":
                return new BinaryExpression(left, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, right);
            case "AND":
            case "&&":
                return BinaryExpression.and(left, right);
            case "OR":
            case "||":
                return BinaryExpression.or(left, right);
            default:
                throw new PlanConversionException("Unsupported binary operator: " + functionName);
        }
    }

    /**
     * Converts a Spark Connect DataType to a thunderduck DataType.
     */
    private DataType convertDataType(org.apache.spark.connect.proto.DataType protoType) {
        switch (protoType.getKindCase()) {
            case BOOLEAN:
                return BooleanType.get();
            case BYTE:
                return ByteType.get();
            case SHORT:
                return ShortType.get();
            case INTEGER:
                return IntegerType.get();
            case LONG:
                return LongType.get();
            case FLOAT:
                return FloatType.get();
            case DOUBLE:
                return DoubleType.get();
            case STRING:
                return StringType.get();
            case BINARY:
                return BinaryType.get();
            case DATE:
                return DateType.get();
            case TIMESTAMP:
                return TimestampType.get();
            case DECIMAL:
                org.apache.spark.connect.proto.DataType.Decimal decimal = protoType.getDecimal();
                return new DecimalType(decimal.getPrecision(), decimal.getScale());
            case ARRAY:
                DataType elementType = convertDataType(protoType.getArray().getElementType());
                return new ArrayType(elementType);
            case MAP:
                DataType keyType = convertDataType(protoType.getMap().getKeyType());
                DataType valueType = convertDataType(protoType.getMap().getValueType());
                return new MapType(keyType, valueType);
            default:
                throw new PlanConversionException("Unsupported DataType: " + protoType.getKindCase());
        }
    }

    /**
     * Parses a type string (e.g., "INT", "DECIMAL(10,2)") to a thunderduck DataType.
     */
    private DataType parseTypeString(String typeStr) {
        return TypeMapper.toSparkType(typeStr);
    }

    /**
     * Checks if the function is a window function.
     */
    private boolean isWindowFunction(String functionName) {
        switch (functionName) {
            // Ranking functions
            case "ROW_NUMBER":
            case "RANK":
            case "DENSE_RANK":
            case "PERCENT_RANK":
            case "NTILE":
            case "CUME_DIST":
            // Analytic functions
            case "LAG":
            case "LEAD":
            case "FIRST_VALUE":
            case "LAST_VALUE":
            case "NTH_VALUE":
                return true;
            default:
                return false;
        }
    }

    /**
     * Handles window functions.
     * Note: This is for window functions called directly. Window expressions with OVER
     * clause are handled by convertWindow().
     */
    private com.thunderduck.expression.Expression handleWindowFunction(
            String functionName, List<com.thunderduck.expression.Expression> arguments) {
        // Window functions typically need special handling when used without OVER clause
        // For now, we'll create a basic FunctionCall and let the planner handle it
        // The proper window specification will be added via convertWindow when OVER is present

        // Infer return type based on window function
        DataType returnType = inferWindowFunctionReturnType(functionName);
        return new FunctionCall(functionName.toLowerCase(), arguments, returnType);
    }

    /**
     * Infers the return type for a window function.
     */
    private DataType inferWindowFunctionReturnType(String functionName) {
        switch (functionName.toUpperCase()) {
            case "ROW_NUMBER":
            case "RANK":
            case "DENSE_RANK":
            case "NTILE":
                return LongType.get();  // These return BIGINT
            case "PERCENT_RANK":
            case "CUME_DIST":
                return DoubleType.get();  // These return percentages
            case "LAG":
            case "LEAD":
            case "FIRST_VALUE":
            case "LAST_VALUE":
            case "NTH_VALUE":
                // These return the same type as their input - use StringType as default
                // TODO: Infer from argument type
                return StringType.get();
            default:
                return StringType.get();  // Default fallback
        }
    }

    /**
     * Converts a Window expression (function with OVER clause).
     */
    private com.thunderduck.expression.Expression convertWindow(Window window) {
        // Convert the window function expression
        com.thunderduck.expression.Expression function = convert(window.getWindowFunction());

        // Extract function name and arguments
        String functionName;
        List<com.thunderduck.expression.Expression> arguments;

        if (function instanceof FunctionCall) {
            FunctionCall fc = (FunctionCall) function;
            functionName = fc.functionName();
            arguments = fc.arguments();
        } else if (function instanceof UnresolvedColumn) {
            // Simple window functions like ROW_NUMBER() might come through as column references
            functionName = ((UnresolvedColumn) function).columnName();
            arguments = new ArrayList<>();
        } else {
            throw new PlanConversionException("Unexpected window function type: " + function.getClass());
        }

        // Convert partition by expressions
        List<com.thunderduck.expression.Expression> partitionBy = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression partExpr : window.getPartitionSpecList()) {
            partitionBy.add(convert(partExpr));
        }

        // Convert order by specifications
        List<Sort.SortOrder> orderBy = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression.SortOrder sortOrder : window.getOrderSpecList()) {
            com.thunderduck.expression.Expression expr = convert(sortOrder.getChild());

            // Map sort direction
            Sort.SortDirection direction = sortOrder.getDirection() ==
                org.apache.spark.connect.proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
                ? Sort.SortDirection.DESCENDING
                : Sort.SortDirection.ASCENDING;

            // Map null ordering
            Sort.NullOrdering nullOrdering = null;
            switch (sortOrder.getNullOrdering()) {
                case SORT_NULLS_FIRST:
                    nullOrdering = Sort.NullOrdering.NULLS_FIRST;
                    break;
                case SORT_NULLS_LAST:
                    nullOrdering = Sort.NullOrdering.NULLS_LAST;
                    break;
                case SORT_NULLS_UNSPECIFIED:
                case UNRECOGNIZED:
                default:
                    // Use default ordering based on sort direction
                    nullOrdering = direction == Sort.SortDirection.ASCENDING
                        ? Sort.NullOrdering.NULLS_FIRST
                        : Sort.NullOrdering.NULLS_LAST;
                    break;
            }

            orderBy.add(new Sort.SortOrder(expr, direction, nullOrdering));
        }

        // Convert window frame if present
        WindowFrame frame = null;
        if (window.hasFrameSpec()) {
            frame = convertWindowFrame(window.getFrameSpec());
        }

        // Create and return the WindowFunction
        return new WindowFunction(functionName, arguments, partitionBy, orderBy, frame);
    }

    /**
     * Converts a Spark Connect WindowFrame to a thunderduck WindowFrame.
     */
    private WindowFrame convertWindowFrame(Window.WindowFrame protoFrame) {
        // Map frame type
        WindowFrame.FrameType frameType;
        switch (protoFrame.getFrameType()) {
            case FRAME_TYPE_ROW:
                frameType = WindowFrame.FrameType.ROWS;
                break;
            case FRAME_TYPE_RANGE:
                frameType = WindowFrame.FrameType.RANGE;
                break;
            default:
                throw new PlanConversionException("Unsupported frame type: " + protoFrame.getFrameType());
        }

        // Convert frame boundaries
        FrameBoundary start = convertFrameBoundary(protoFrame.getLower(), true);
        FrameBoundary end = convertFrameBoundary(protoFrame.getUpper(), false);

        return new WindowFrame(frameType, start, end);
    }

    /**
     * Converts a Spark Connect FrameBoundary to a thunderduck FrameBoundary.
     */
    private FrameBoundary convertFrameBoundary(
            Window.WindowFrame.FrameBoundary protoBoundary, boolean isLower) {

        switch (protoBoundary.getBoundaryCase()) {
            case CURRENT_ROW:
                return FrameBoundary.CurrentRow.getInstance();

            case UNBOUNDED:
                // For lower bound, unbounded means unbounded preceding
                // For upper bound, unbounded means unbounded following
                if (isLower) {
                    return FrameBoundary.UnboundedPreceding.getInstance();
                } else {
                    return FrameBoundary.UnboundedFollowing.getInstance();
                }

            case VALUE:
                // Value represents offset for PRECEDING/FOLLOWING
                // The protobuf doesn't directly specify if it's preceding or following,
                // so we need to infer based on the sign and position
                long offset = protoBoundary.getValue().getLiteral().getLong();

                if (offset < 0) {
                    // Negative offset means preceding for lower, following for upper
                    offset = Math.abs(offset);
                    if (isLower) {
                        return new FrameBoundary.Preceding(offset);
                    } else {
                        return new FrameBoundary.Following(offset);
                    }
                } else if (offset > 0) {
                    // Positive offset typically means following for upper, preceding for lower
                    if (isLower) {
                        return new FrameBoundary.Preceding(offset);
                    } else {
                        return new FrameBoundary.Following(offset);
                    }
                } else {
                    // Zero offset means current row
                    return FrameBoundary.CurrentRow.getInstance();
                }

            default:
                throw new PlanConversionException("Unsupported boundary type: " + protoBoundary.getBoundaryCase());
        }
    }

    // ==================== Higher-Order Function Support ====================

    /**
     * Checks if the function is a higher-order function that needs special handling.
     *
     * <p>Some HOFs like exists and forall require emulation using list_transform
     * wrapped with list_any or list_all.
     */
    private boolean isHigherOrderFunction(String functionName) {
        switch (functionName) {
            case "EXISTS":
            case "FORALL":
            case "ARRAY_EXISTS":
            case "ARRAY_FORALL":
            case "AGGREGATE":
            case "REDUCE":
            case "ZIP_WITH":
            case "MAP_FILTER":
            case "TRANSFORM_KEYS":
            case "TRANSFORM_VALUES":
                return true;
            default:
                return false;
        }
    }

    /**
     * Handles higher-order array functions that require special emulation.
     *
     * <p>Mappings:
     * <ul>
     *   <li>exists(arr, x -> pred) → list_any(list_transform(arr, lambda x: pred))</li>
     *   <li>forall(arr, x -> pred) → list_all(list_transform(arr, lambda x: pred))</li>
     * </ul>
     *
     * @param functionName the upper-cased function name
     * @param arguments the converted function arguments
     * @return the emulated expression
     */
    private com.thunderduck.expression.Expression handleHigherOrderFunction(
            String functionName, List<com.thunderduck.expression.Expression> arguments) {

        if (arguments.size() < 2) {
            throw new PlanConversionException(functionName + " requires at least 2 arguments (array and lambda)");
        }

        com.thunderduck.expression.Expression arrayArg = arguments.get(0);
        com.thunderduck.expression.Expression lambdaArg = arguments.get(1);

        switch (functionName) {
            case "EXISTS":
            case "ARRAY_EXISTS": {
                // exists(arr, pred) → list_any(list_transform(arr, pred))
                // The lambda is already converted and will produce boolean results
                List<com.thunderduck.expression.Expression> transformArgs = new ArrayList<>();
                transformArgs.add(arrayArg);
                transformArgs.add(lambdaArg);
                com.thunderduck.expression.Expression listTransform =
                    new FunctionCall("list_transform", transformArgs, BooleanType.get());

                List<com.thunderduck.expression.Expression> anyArgs = new ArrayList<>();
                anyArgs.add(listTransform);
                return new FunctionCall("list_bool_or", anyArgs, BooleanType.get());
            }

            case "FORALL":
            case "ARRAY_FORALL": {
                // forall(arr, pred) → list_all(list_transform(arr, pred))
                List<com.thunderduck.expression.Expression> transformArgs = new ArrayList<>();
                transformArgs.add(arrayArg);
                transformArgs.add(lambdaArg);
                com.thunderduck.expression.Expression listTransform =
                    new FunctionCall("list_transform", transformArgs, BooleanType.get());

                List<com.thunderduck.expression.Expression> allArgs = new ArrayList<>();
                allArgs.add(listTransform);
                return new FunctionCall("list_bool_and", allArgs, BooleanType.get());
            }

            case "AGGREGATE":
            case "REDUCE": {
                // aggregate(arr, init, merge) → list_reduce(list_prepend(init, arr), merge)
                // Spark aggregate takes: array, initialValue, merge function, [finish function]
                // DuckDB list_reduce doesn't take an init value, so we prepend it to the array
                if (arguments.size() < 3) {
                    throw new PlanConversionException("aggregate requires at least 3 arguments (array, init, merge)");
                }

                com.thunderduck.expression.Expression initArg = arguments.get(1);
                com.thunderduck.expression.Expression mergeArg = arguments.get(2);

                // Create list_prepend(init, arr) to include initial value
                // DuckDB list_prepend signature is: list_prepend(element, list)
                List<com.thunderduck.expression.Expression> prependArgs = new ArrayList<>();
                prependArgs.add(initArg);  // element first
                prependArgs.add(arrayArg); // list second
                com.thunderduck.expression.Expression prependedList =
                    new FunctionCall("list_prepend", prependArgs, StringType.get());

                // Create list_reduce(prepended_list, merge_lambda)
                List<com.thunderduck.expression.Expression> reduceArgs = new ArrayList<>();
                reduceArgs.add(prependedList);
                reduceArgs.add(mergeArg);
                com.thunderduck.expression.Expression reduceResult =
                    new FunctionCall("list_reduce", reduceArgs, StringType.get());

                // If there's a finish function (4th argument), apply it
                if (arguments.size() >= 4) {
                    com.thunderduck.expression.Expression finishArg = arguments.get(3);
                    // The finish function in Spark takes the accumulated value
                    // We need to wrap the result: finish(reduce_result)
                    // This is complex because finishArg is a lambda, not a function call
                    // For now, we skip the finish function - most uses don't need it
                    logger.warn("aggregate finish function not yet supported, ignoring");
                }

                return reduceResult;
            }

            case "ZIP_WITH": {
                // zip_with(arr1, arr2, f) → list_transform(range(len(arr1)), lambda i: f(arr1[i+1], arr2[i+1]))
                // This is complex because DuckDB arrays are 1-indexed
                // For simplicity, we use list_zip to pair elements then transform
                // zip_with(a, b, f) → list_transform(list_zip(a, b), lambda p: f(p[1], p[2]))
                if (arguments.size() < 3) {
                    throw new PlanConversionException("zip_with requires 3 arguments (array1, array2, lambda)");
                }

                com.thunderduck.expression.Expression arr1 = arguments.get(0);
                com.thunderduck.expression.Expression arr2 = arguments.get(1);
                com.thunderduck.expression.Expression lambdaFunc = arguments.get(2);

                // For now, we just pass through to list_transform with both arrays
                // DuckDB 0.10+ has list_zip which could help
                // Simpler approach: use generate_subscripts pattern
                // But the cleanest is: list_transform(range(1, len(arr1)+1), i -> f(arr1[i], arr2[i]))

                // For DuckDB, list indexing is 1-based, range generates 0-based
                // list_transform(
                //   generate_series(1, length(arr1)),
                //   lambda i: original_lambda_body_with_arr1[i]_and_arr2[i]
                // )

                // This requires rewriting the lambda body, which is complex.
                // For now, fall back to a simpler but less efficient approach using list_apply
                // or just warn that zip_with is not fully supported yet.
                logger.warn("zip_with has limited support - complex lambda rewriting needed");

                // Basic implementation using array_zip from DuckDB (if available)
                // Otherwise, pass through with warning
                List<com.thunderduck.expression.Expression> zipArgs = new ArrayList<>();
                zipArgs.add(arr1);
                zipArgs.add(arr2);
                com.thunderduck.expression.Expression zipped =
                    new FunctionCall("list_zip", zipArgs, StringType.get());

                // The result is a list of structs - further transformation would need
                // the lambda to be rewritten to access struct fields
                // For now, return the zipped result (partial support)
                return zipped;
            }

            // ==================== Map Higher-Order Functions ====================
            // These functions operate on MAP types, converting to/from entries lists

            case "MAP_FILTER": {
                // map_filter(map, (k, v) -> pred) →
                //   map_from_entries(list_filter(map_entries(map), lambda e: pred_with_e.key_e.value))
                // This requires lambda body rewriting, which is complex.
                // For now, provide basic structure with warning.
                if (arguments.size() < 2) {
                    throw new PlanConversionException("map_filter requires 2 arguments (map, lambda)");
                }

                com.thunderduck.expression.Expression mapArg = arguments.get(0);
                com.thunderduck.expression.Expression lambdaFn = arguments.get(1);

                // Convert map to entries list
                List<com.thunderduck.expression.Expression> entriesArgs = new ArrayList<>();
                entriesArgs.add(mapArg);
                com.thunderduck.expression.Expression entriesList =
                    new FunctionCall("map_entries", entriesArgs, StringType.get());

                // Filter the entries list
                // The lambda needs to be adapted to work with struct entries
                // For now, pass the lambda as-is (may not work for complex lambdas)
                List<com.thunderduck.expression.Expression> filterArgs = new ArrayList<>();
                filterArgs.add(entriesList);
                filterArgs.add(lambdaFn);
                com.thunderduck.expression.Expression filteredEntries =
                    new FunctionCall("list_filter", filterArgs, StringType.get());

                // Convert back to map
                List<com.thunderduck.expression.Expression> fromEntriesArgs = new ArrayList<>();
                fromEntriesArgs.add(filteredEntries);
                return new FunctionCall("map_from_entries", fromEntriesArgs, StringType.get());
            }

            case "TRANSFORM_KEYS": {
                // transform_keys(map, (k, v) -> new_key) →
                //   map_from_entries(list_transform(map_entries(map), lambda e: struct_pack(key := f(e.key, e.value), value := e.value)))
                if (arguments.size() < 2) {
                    throw new PlanConversionException("transform_keys requires 2 arguments (map, lambda)");
                }

                com.thunderduck.expression.Expression mapArg = arguments.get(0);
                com.thunderduck.expression.Expression lambdaFn = arguments.get(1);

                // Convert map to entries list
                List<com.thunderduck.expression.Expression> entriesArgs = new ArrayList<>();
                entriesArgs.add(mapArg);
                com.thunderduck.expression.Expression entriesList =
                    new FunctionCall("map_entries", entriesArgs, StringType.get());

                // Transform the entries list
                // Note: The lambda body transformation would be complex
                List<com.thunderduck.expression.Expression> transformArgs = new ArrayList<>();
                transformArgs.add(entriesList);
                transformArgs.add(lambdaFn);
                com.thunderduck.expression.Expression transformedEntries =
                    new FunctionCall("list_transform", transformArgs, StringType.get());

                // Convert back to map
                List<com.thunderduck.expression.Expression> fromEntriesArgs = new ArrayList<>();
                fromEntriesArgs.add(transformedEntries);
                return new FunctionCall("map_from_entries", fromEntriesArgs, StringType.get());
            }

            case "TRANSFORM_VALUES": {
                // transform_values(map, (k, v) -> new_value) →
                //   map_from_entries(list_transform(map_entries(map), lambda e: struct_pack(key := e.key, value := f(e.key, e.value))))
                if (arguments.size() < 2) {
                    throw new PlanConversionException("transform_values requires 2 arguments (map, lambda)");
                }

                com.thunderduck.expression.Expression mapArg = arguments.get(0);
                com.thunderduck.expression.Expression lambdaFn = arguments.get(1);

                // Convert map to entries list
                List<com.thunderduck.expression.Expression> entriesArgs = new ArrayList<>();
                entriesArgs.add(mapArg);
                com.thunderduck.expression.Expression entriesList =
                    new FunctionCall("map_entries", entriesArgs, StringType.get());

                // Transform the entries list
                List<com.thunderduck.expression.Expression> transformArgs = new ArrayList<>();
                transformArgs.add(entriesList);
                transformArgs.add(lambdaFn);
                com.thunderduck.expression.Expression transformedEntries =
                    new FunctionCall("list_transform", transformArgs, StringType.get());

                // Convert back to map
                List<com.thunderduck.expression.Expression> fromEntriesArgs = new ArrayList<>();
                fromEntriesArgs.add(transformedEntries);
                return new FunctionCall("map_from_entries", fromEntriesArgs, StringType.get());
            }

            default:
                throw new PlanConversionException("Unknown higher-order function: " + functionName);
        }
    }

    // ==================== Lambda Function Support ====================

    /**
     * Checks if a variable name is currently in lambda scope.
     *
     * @param variableName the variable name to check
     * @return true if the variable is bound in a lambda scope
     */
    private boolean isLambdaVariable(String variableName) {
        for (Set<String> scope : lambdaScopes) {
            if (scope.contains(variableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts a LambdaFunction expression.
     *
     * <p>Lambda functions are anonymous functions used in higher-order functions like
     * transform, filter, aggregate, etc.
     *
     * <p>Examples:
     * <pre>
     *   x -> x + 1                 -- single parameter lambda
     *   (acc, x) -> acc + x        -- two parameter lambda
     * </pre>
     *
     * <p>Output uses DuckDB Python-style syntax:
     * <pre>
     *   lambda x: x + 1
     *   lambda acc, x: acc + x
     * </pre>
     */
    private com.thunderduck.expression.Expression convertLambdaFunction(
            org.apache.spark.connect.proto.Expression.LambdaFunction lambda) {

        // Extract parameter names
        List<String> parameterNames = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression.UnresolvedNamedLambdaVariable param :
                lambda.getArgumentsList()) {
            // Lambda variable names are stored in the name_parts list
            if (param.getNamePartsCount() > 0) {
                parameterNames.add(param.getNameParts(0));
            }
        }

        if (parameterNames.isEmpty()) {
            throw new PlanConversionException("Lambda must have at least one parameter");
        }

        // Push new lambda scope
        Set<String> scope = new HashSet<>(parameterNames);
        lambdaScopes.push(scope);

        try {
            // Convert the lambda body with the scope active
            com.thunderduck.expression.Expression body = convert(lambda.getFunction());

            // Create and return the LambdaExpression
            return new LambdaExpression(parameterNames, body);
        } finally {
            // Always pop the scope, even on exception
            lambdaScopes.pop();
        }
    }

    /**
     * Converts an UnresolvedNamedLambdaVariable expression.
     *
     * <p>These are lambda parameter references within a lambda body.
     * They are emitted as plain identifiers (no quoting).
     */
    private com.thunderduck.expression.Expression convertUnresolvedNamedLambdaVariable(
            org.apache.spark.connect.proto.Expression.UnresolvedNamedLambdaVariable lambdaVar) {

        // Get the variable name from name_parts
        String variableName;
        if (lambdaVar.getNamePartsCount() > 0) {
            variableName = lambdaVar.getNameParts(0);
        } else {
            throw new PlanConversionException("Lambda variable must have a name");
        }

        // Create LambdaVariableExpression
        return new LambdaVariableExpression(variableName);
    }

    /**
     * Converts a CallFunction expression.
     *
     * <p>CallFunction represents a dynamic function call by name.
     * This is used for UDFs and pre-resolved function references.
     */
    private com.thunderduck.expression.Expression convertCallFunction(
            org.apache.spark.connect.proto.CallFunction callFunction) {

        String functionName = callFunction.getFunctionName();

        // Convert arguments
        List<com.thunderduck.expression.Expression> arguments = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression arg : callFunction.getArgumentsList()) {
            arguments.add(convert(arg));
        }

        // Map function name if needed (same mapping as UnresolvedFunction)
        String mappedName = com.thunderduck.functions.FunctionRegistry.mapFunctionName(functionName);

        // Create FunctionCall with mapped name
        // Use StringType as default return type (same as convertUnresolvedFunction)
        return new FunctionCall(mappedName, arguments, StringType.get());
    }

    // ==================== Complex Type Extraction ====================

    /**
     * Converts an UnresolvedExtractValue expression.
     *
     * <p>UnresolvedExtractValue extracts a value from complex types:
     * <ul>
     *   <li>Struct field: {@code struct.field} or {@code struct['field']}</li>
     *   <li>Array element: {@code arr[index]} (0-based in Spark, 1-based in DuckDB)</li>
     *   <li>Map key: {@code map['key']}</li>
     * </ul>
     *
     * <p>Index conversion is critical:
     * <ul>
     *   <li>PySpark uses 0-based indexing for arrays</li>
     *   <li>DuckDB uses 1-based indexing for lists</li>
     *   <li>Negative indices in Spark mean from the end (-1 = last element)</li>
     * </ul>
     */
    private com.thunderduck.expression.Expression convertUnresolvedExtractValue(
            org.apache.spark.connect.proto.Expression.UnresolvedExtractValue extractValue) {

        // Convert child expression (the collection to extract from)
        com.thunderduck.expression.Expression child = convert(extractValue.getChild());

        // Convert extraction expression (index/key/field)
        com.thunderduck.expression.Expression extraction = convert(extractValue.getExtraction());

        // Determine extraction type based on the extraction expression
        ExtractValueExpression.ExtractionType extractionType = determineExtractionType(extraction);

        // Handle index conversion for array access
        if (extractionType == ExtractValueExpression.ExtractionType.ARRAY_INDEX) {
            extraction = convertArrayIndex(extraction, child);
        }

        return new ExtractValueExpression(child, extraction, extractionType);
    }

    /**
     * Converts an UnresolvedRegex expression.
     *
     * <p>UnresolvedRegex represents Spark's {@code df.colRegex()} which selects
     * columns matching a regex pattern. This is translated to DuckDB's
     * {@code COLUMNS('pattern')} expression.
     *
     * <p>Examples:
     * <pre>
     *   df.colRegex("`col_.*`")  → COLUMNS('col_.*')
     *   df.colRegex("`^test_`") → COLUMNS('^test_')
     * </pre>
     */
    private com.thunderduck.expression.Expression convertUnresolvedRegex(
            org.apache.spark.connect.proto.Expression.UnresolvedRegex unresolvedRegex) {

        // Get the column name (which is the pattern)
        String colName = unresolvedRegex.getColName();

        // Strip backticks from the pattern (Spark uses `pattern` format)
        String pattern = RegexColumnExpression.stripBackticks(colName);

        // Check if there's a plan_id for table qualification
        // The plan_id would need to be resolved to a table alias
        // For now, we don't handle table qualification
        if (unresolvedRegex.hasPlanId()) {
            logger.debug("UnresolvedRegex has plan_id {}, but table qualification not yet supported",
                unresolvedRegex.getPlanId());
        }

        return new RegexColumnExpression(pattern);
    }

    /**
     * Converts an UpdateFields expression.
     *
     * <p>UpdateFields represents Spark's struct field manipulation:
     * <ul>
     *   <li>{@code col.withField("field", value)} - add/replace a field</li>
     *   <li>{@code col.dropFields("field")} - remove a field (when valueExpr is absent)</li>
     * </ul>
     *
     * <p>DuckDB translation:
     * <ul>
     *   <li>Add/Replace: {@code struct_insert(struct, field := value)}</li>
     *   <li>Drop: Complex - requires struct reconstruction (limited support)</li>
     * </ul>
     */
    private com.thunderduck.expression.Expression convertUpdateFields(
            org.apache.spark.connect.proto.Expression.UpdateFields updateFields) {

        // Convert the struct expression
        com.thunderduck.expression.Expression structExpr = convert(updateFields.getStructExpression());

        // Get the field name
        String fieldName = updateFields.getFieldName();

        // Check if this is add/replace or drop
        if (updateFields.hasValueExpression()) {
            // Add or replace field
            com.thunderduck.expression.Expression valueExpr = convert(updateFields.getValueExpression());
            return UpdateFieldsExpression.withField(structExpr, fieldName, valueExpr);
        } else {
            // Drop field
            logger.warn("dropFields operation has limited support - struct reconstruction required");
            return UpdateFieldsExpression.dropField(structExpr, fieldName);
        }
    }

    /**
     * Determines the type of extraction based on the extraction expression.
     *
     * <p>Heuristics:
     * <ul>
     *   <li>Integer literal → array index</li>
     *   <li>String literal → struct field (unless child is a map type)</li>
     *   <li>Other → default to struct field or map key based on context</li>
     * </ul>
     */
    private ExtractValueExpression.ExtractionType determineExtractionType(
            com.thunderduck.expression.Expression extraction) {

        if (extraction instanceof com.thunderduck.expression.Literal) {
            Object value = ((com.thunderduck.expression.Literal) extraction).value();
            if (value instanceof Integer || value instanceof Long ||
                value instanceof Short || value instanceof Byte) {
                return ExtractValueExpression.ExtractionType.ARRAY_INDEX;
            } else if (value instanceof String) {
                // String can be struct field or map key
                // Default to struct field - map access should use element_at explicitly
                // In practice, if the child is a known MAP type, we'd use MAP_KEY
                // For now, use STRUCT_FIELD as default for string keys
                return ExtractValueExpression.ExtractionType.STRUCT_FIELD;
            }
        }

        // For dynamic expressions, default to struct field
        // The actual behavior will depend on runtime type
        return ExtractValueExpression.ExtractionType.STRUCT_FIELD;
    }

    /**
     * Converts a PySpark 0-based array index to DuckDB 1-based indexing.
     *
     * <p>Conversion rules:
     * <ul>
     *   <li>Positive index N → N + 1</li>
     *   <li>Negative index -N → length(arr) + 1 - N (e.g., -1 → length(arr))</li>
     *   <li>Dynamic index expr → (expr) + 1</li>
     * </ul>
     *
     * @param index the PySpark 0-based index expression
     * @param array the array expression (for negative index handling)
     * @return the DuckDB 1-based index expression
     */
    private com.thunderduck.expression.Expression convertArrayIndex(
            com.thunderduck.expression.Expression index,
            com.thunderduck.expression.Expression array) {

        if (index instanceof com.thunderduck.expression.Literal) {
            Object value = ((com.thunderduck.expression.Literal) index).value();
            if (value instanceof Number) {
                long pyIndex = ((Number) value).longValue();

                if (pyIndex >= 0) {
                    // Positive index: add 1
                    return new com.thunderduck.expression.Literal((int) (pyIndex + 1), IntegerType.get());
                } else {
                    // Negative index: arr[-1] → arr[length(arr)]
                    // arr[-2] → arr[length(arr) - 1]
                    // General: arr[-n] → arr[length(arr) + 1 - n]
                    // Which is: length(arr) - abs(n) + 1 = length(arr) + n + 1
                    // Since n is negative, length(arr) + n + 1 = length(arr) - abs(n) + 1

                    // Create: length(array) + pyIndex + 1
                    // e.g., for pyIndex = -1: length(array) + (-1) + 1 = length(array)
                    // e.g., for pyIndex = -2: length(array) + (-2) + 1 = length(array) - 1
                    List<com.thunderduck.expression.Expression> lengthArgs = new ArrayList<>();
                    lengthArgs.add(array);
                    com.thunderduck.expression.Expression lengthExpr =
                        new FunctionCall("length", lengthArgs, LongType.get());

                    // length(array) + pyIndex + 1
                    long offset = pyIndex + 1;
                    com.thunderduck.expression.Expression offsetLiteral =
                        new com.thunderduck.expression.Literal((int) offset, IntegerType.get());

                    return BinaryExpression.add(lengthExpr, offsetLiteral);
                }
            }
        }

        // Dynamic index: (expr) + 1
        com.thunderduck.expression.Expression one = new com.thunderduck.expression.Literal(1, IntegerType.get());
        return BinaryExpression.add(index, one);
    }
}