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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Spark Connect Expression types to thunderduck Expression objects.
 *
 * <p>This class handles all expression types including literals, column references,
 * functions, arithmetic operations, comparisons, casts, etc.
 */
public class ExpressionConverter {
    private static final Logger logger = LoggerFactory.getLogger(ExpressionConverter.class);

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
     */
    private com.thunderduck.expression.Expression convertUnresolvedAttribute(UnresolvedAttribute attr) {
        String identifier = attr.getUnparsedIdentifier();

        // Handle qualified names (table.column)
        String[] parts = identifier.split("\\.");
        if (parts.length == 2) {
            return new UnresolvedColumn(parts[1], parts[0]);
        } else if (parts.length == 1) {
            return new UnresolvedColumn(parts[0]);
        } else {
            // For now, just use the full identifier as column name
            return new UnresolvedColumn(identifier);
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
}