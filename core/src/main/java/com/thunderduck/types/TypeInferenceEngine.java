package com.thunderduck.types;

import com.thunderduck.expression.AliasExpression;
import com.thunderduck.expression.ArrayLiteralExpression;
import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.CaseWhenExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.InExpression;
import com.thunderduck.expression.Literal;
import com.thunderduck.expression.MapLiteralExpression;
import com.thunderduck.expression.StructLiteralExpression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.expression.WindowFunction;

import java.util.List;
import com.thunderduck.functions.FunctionCategories;

/**
 * Centralized engine for type inference and nullability resolution.
 *
 * <p>This class consolidates type inference logic following Spark's type
 * semantics for consistent behavior across all logical plan nodes.
 *
 * <h2>Type Resolution Rules</h2>
 * <ul>
 *   <li>Column references: look up type from child schema</li>
 *   <li>Window functions: type depends on function (ranking → IntegerType, analytic → arg type)</li>
 *   <li>Binary expressions: comparison → Boolean, arithmetic → promoted type</li>
 *   <li>Aggregate functions: COUNT → Long, SUM/AVG → promoted type, MIN/MAX → arg type</li>
 * </ul>
 *
 * <h2>Nullability Rules (Spark semantics)</h2>
 * <ul>
 *   <li>Literal (non-null): false</li>
 *   <li>Column reference: inherits from schema</li>
 *   <li>Arithmetic on nullable: true if any operand nullable</li>
 *   <li>COUNT: false (always returns 0 for empty groups)</li>
 *   <li>SUM/AVG/MIN/MAX: true (empty group → null)</li>
 *   <li>Window ranking (ROW_NUMBER, RANK): false</li>
 *   <li>Window analytic (LAG, LEAD): true (unless default provided AND column non-nullable)</li>
 * </ul>
 */
public final class TypeInferenceEngine {

    private TypeInferenceEngine() {
        // Utility class - prevent instantiation
    }

    // ========================================================================
    // Schema Lookup
    // ========================================================================

    /**
     * Finds a StructField by name (case-insensitive).
     *
     * @param columnName the column name to find
     * @param schema the schema to search in (may be null)
     * @return the matching field, or null if not found
     */
    public static StructField findField(String columnName, StructType schema) {
        if (schema == null || columnName == null) {
            return null;
        }
        for (StructField field : schema.fields()) {
            if (field.name().equalsIgnoreCase(columnName)) {
                return field;
            }
        }
        return null;
    }

    /**
     * Looks up a column's type from schema (case-insensitive).
     *
     * @param columnName the column name to look up
     * @param schema the schema to search in
     * @return the column's data type, or null if not found
     */
    public static DataType lookupColumnType(String columnName, StructType schema) {
        StructField field = findField(columnName, schema);
        return field != null ? field.dataType() : null;
    }

    /**
     * Looks up a column's nullability from schema (case-insensitive).
     *
     * @param columnName the column name to look up
     * @param schema the schema to search in
     * @return the column's nullability, or true if not found
     */
    public static boolean lookupColumnNullable(String columnName, StructType schema) {
        StructField field = findField(columnName, schema);
        return field != null ? field.nullable() : true;
    }

    // ========================================================================
    // Numeric Type Promotion
    // ========================================================================

    /**
     * Promotes numeric types according to Spark's type coercion rules.
     *
     * <p>Promotion order: Byte &lt; Short &lt; Integer &lt; Long &lt; Float &lt; Double.
     * Decimal types are handled separately (widest precision wins).
     *
     * @param left the left operand type
     * @param right the right operand type
     * @return the promoted type
     */
    public static DataType promoteNumericTypes(DataType left, DataType right) {
        // If either is Double, result is Double
        if (left instanceof DoubleType || right instanceof DoubleType) {
            return DoubleType.get();
        }

        // If either is Float, result is Float
        if (left instanceof FloatType || right instanceof FloatType) {
            return FloatType.get();
        }

        // If either is Decimal, result is Decimal with appropriate precision
        if (left instanceof DecimalType || right instanceof DecimalType) {
            DecimalType leftDec = toDecimalForUnification(left);
            DecimalType rightDec = toDecimalForUnification(right);
            return unifyDecimalTypes(leftDec, rightDec);
        }

        // If either is Long, result is Long
        if (left instanceof LongType || right instanceof LongType) {
            return LongType.get();
        }

        // If either is Integer, result is Integer
        if (left instanceof IntegerType || right instanceof IntegerType) {
            return IntegerType.get();
        }

        // If either is Short, result is Short
        if (left instanceof ShortType || right instanceof ShortType) {
            return ShortType.get();
        }

        // Default: return left type or Double for safety
        return left != null ? left : DoubleType.get();
    }

    /**
     * Converts a type to DecimalType for type unification in CASE/COALESCE expressions.
     *
     * <p>Spark promotes integral types to Decimal with fixed precision:
     * <ul>
     *   <li>ByteType → Decimal(3,0)</li>
     *   <li>ShortType → Decimal(5,0)</li>
     *   <li>IntegerType → Decimal(10,0)</li>
     *   <li>LongType → Decimal(20,0)</li>
     * </ul>
     */
    private static DecimalType toDecimalForUnification(DataType type) {
        if (type instanceof DecimalType) {
            return (DecimalType) type;
        }
        if (type instanceof ByteType) {
            return new DecimalType(3, 0);
        }
        if (type instanceof ShortType) {
            return new DecimalType(5, 0);
        }
        if (type instanceof IntegerType) {
            return new DecimalType(10, 0);
        }
        if (type instanceof LongType) {
            return new DecimalType(20, 0);
        }
        // Default for unknown types
        return new DecimalType(10, 0);
    }

    /**
     * Unifies two DecimalTypes per Spark's CASE/COALESCE rules.
     *
     * <p>Formula:
     * <ul>
     *   <li>resultScale = max(s1, s2)</li>
     *   <li>resultIntDigits = max(p1-s1, p2-s2)</li>
     *   <li>resultPrecision = resultIntDigits + resultScale</li>
     * </ul>
     */
    private static DecimalType unifyDecimalTypes(DecimalType left, DecimalType right) {
        int s1 = left.scale();
        int s2 = right.scale();
        int intDigits1 = left.precision() - s1;
        int intDigits2 = right.precision() - s2;

        int resultScale = Math.max(s1, s2);
        int resultIntDigits = Math.max(intDigits1, intDigits2);
        int resultPrecision = Math.min(resultIntDigits + resultScale, 38);

        return new DecimalType(resultPrecision, resultScale);
    }

    /**
     * Calculates the result type for Decimal division per Spark semantics.
     *
     * <p>Spark uses a two-step process:
     * <ol>
     *   <li>Calculate initial precision/scale:
     *       <ul>
     *         <li>Scale: max(6, s1 + p2 + 1)</li>
     *         <li>Precision: p1 - s1 + s2 + scale</li>
     *       </ul>
     *   </li>
     *   <li>If precision exceeds 38, apply precision loss adjustment:
     *       <ul>
     *         <li>intDigits = precision - scale</li>
     *         <li>minScale = min(scale, 6) (retain at least 6 fractional digits)</li>
     *         <li>adjustedScale = max(38 - intDigits, minScale)</li>
     *         <li>finalPrecision = 38</li>
     *       </ul>
     *   </li>
     * </ol>
     *
     * <p>Where p1, s1 are precision/scale of dividend and p2, s2 are precision/scale of divisor.
     *
     * @param dividend the dividend (left operand) type
     * @param divisor the divisor (right operand) type
     * @return the result DecimalType with calculated precision and scale
     */
    public static DecimalType promoteDecimalDivision(DecimalType dividend, DecimalType divisor) {
        int p1 = dividend.precision();
        int s1 = dividend.scale();
        int p2 = divisor.precision();
        int s2 = divisor.scale();

        // Step 1: Calculate initial precision and scale
        int scale = Math.max(6, s1 + p2 + 1);
        int precision = p1 - s1 + s2 + scale;

        // Step 2: Apply precision loss adjustment if precision exceeds maximum
        if (precision > 38) {
            int intDigits = precision - scale;
            int minScale = Math.min(scale, 6);  // retain at least 6 fractional digits
            scale = Math.max(38 - intDigits, minScale);
            precision = 38;
        }

        // Final safety cap (shouldn't be needed after adjustment)
        scale = Math.min(scale, 38);
        precision = Math.min(precision, 38);

        return new DecimalType(precision, scale);
    }

    /**
     * Calculates the result type for Decimal multiplication per Spark semantics.
     *
     * <p>Spark's multiplication formula:
     * <ul>
     *   <li>precision = p1 + p2 + 1 (capped at 38)</li>
     *   <li>scale = s1 + s2 (capped at precision)</li>
     * </ul>
     *
     * @param left the left operand type
     * @param right the right operand type
     * @return the result DecimalType
     */
    public static DecimalType promoteDecimalMultiplication(DecimalType left, DecimalType right) {
        int p1 = left.precision();
        int s1 = left.scale();
        int p2 = right.precision();
        int s2 = right.scale();

        // Spark's multiplication formula
        int precision = Math.min(p1 + p2 + 1, 38);
        int scale = Math.min(s1 + s2, precision);

        return new DecimalType(precision, scale);
    }

    /**
     * Converts a type to DecimalType if possible, for Decimal arithmetic operations.
     *
     * <p>Integer types are promoted to Decimal based on their value range or,
     * if the expression is a Literal, based on the actual value's digit count.
     *
     * @param type the source type
     * @param expr the expression (used to extract literal values)
     * @return DecimalType if convertible, null otherwise
     */
    private static DecimalType toDecimalType(DataType type, Expression expr) {
        if (type instanceof DecimalType) {
            return (DecimalType) type;
        }

        // For integer types, promote to Decimal
        // If it's a literal, use the actual value to determine precision
        if (type instanceof IntegerType || type instanceof LongType ||
            type instanceof ShortType || type instanceof ByteType) {

            if (expr instanceof Literal) {
                Literal lit = (Literal) expr;
                Object value = lit.value();
                if (value instanceof Number) {
                    // Calculate precision based on actual value
                    long absValue = Math.abs(((Number) value).longValue());
                    int precision = absValue == 0 ? 1 : (int) Math.log10(absValue) + 1;
                    return new DecimalType(precision, 0);
                }
            }

            // Default precision based on type
            if (type instanceof ByteType) return new DecimalType(3, 0);
            if (type instanceof ShortType) return new DecimalType(5, 0);
            if (type instanceof IntegerType) return new DecimalType(10, 0);
            if (type instanceof LongType) return new DecimalType(19, 0);
        }

        // Not convertible to Decimal
        return null;
    }

    // ========================================================================
    // Core Type Resolution
    // ========================================================================

    /**
     * Resolves the data type of an expression, looking up column types from the schema.
     *
     * @param expr the expression to resolve
     * @param schema the schema to look up column types from (may be null)
     * @return the resolved data type
     */
    public static DataType resolveType(Expression expr, StructType schema) {
        if (expr == null) {
            return StringType.get();
        }

        // Handle AliasExpression - resolve the underlying expression
        if (expr instanceof AliasExpression) {
            return resolveType(((AliasExpression) expr).expression(), schema);
        }

        // Handle UnresolvedColumn - look up type from schema
        if (expr instanceof UnresolvedColumn) {
            UnresolvedColumn col = (UnresolvedColumn) expr;
            DataType schemaType = lookupColumnType(col.columnName(), schema);
            return schemaType != null ? schemaType : col.dataType();
        }

        // Handle WindowFunction
        if (expr instanceof WindowFunction) {
            return resolveWindowFunctionType((WindowFunction) expr, schema);
        }

        // Handle BinaryExpression
        if (expr instanceof BinaryExpression) {
            return resolveBinaryExpressionType((BinaryExpression) expr, schema);
        }

        // Handle FunctionCall
        if (expr instanceof FunctionCall) {
            return resolveFunctionCallType((FunctionCall) expr, schema);
        }

        // Handle CaseWhenExpression - resolve branch types with schema
        if (expr instanceof CaseWhenExpression) {
            return resolveCaseWhenType((CaseWhenExpression) expr, schema);
        }

        // Handle ArrayLiteralExpression - resolve element types with schema
        if (expr instanceof ArrayLiteralExpression) {
            return resolveArrayLiteralType((ArrayLiteralExpression) expr, schema);
        }

        // Handle MapLiteralExpression - resolve key/value types with schema
        if (expr instanceof MapLiteralExpression) {
            return resolveMapLiteralType((MapLiteralExpression) expr, schema);
        }

        // Handle StructLiteralExpression - resolve field types with schema
        if (expr instanceof StructLiteralExpression) {
            return resolveStructLiteralType((StructLiteralExpression) expr, schema);
        }

        // Handle InExpression - always returns Boolean
        if (expr instanceof InExpression) {
            return BooleanType.get();
        }

        // Default: use the expression's declared type
        return expr.dataType();
    }

    // ========================================================================
    // Window Function Type Resolution
    // ========================================================================

    /**
     * Resolves window function return type.
     *
     * <p>Type rules:
     * <ul>
     *   <li>Ranking (ROW_NUMBER, RANK, DENSE_RANK, NTILE): IntegerType</li>
     *   <li>Distribution (PERCENT_RANK, CUME_DIST): DoubleType</li>
     *   <li>COUNT: LongType</li>
     *   <li>Analytic (LAG, LEAD, FIRST, LAST, NTH_VALUE): argument type</li>
     *   <li>MIN, MAX: argument type</li>
     *   <li>SUM: promoted type (Integer/Long→Long, Float/Double→Double, Decimal→Decimal(p+10,s))</li>
     *   <li>AVG: Double for numeric, Decimal for Decimal input</li>
     *   <li>STDDEV, VARIANCE, etc.: DoubleType</li>
     * </ul>
     */
    public static DataType resolveWindowFunctionType(WindowFunction wf, StructType schema) {
        String func = wf.function().toUpperCase();

        // Ranking functions return IntegerType
        if (func.equals("ROW_NUMBER") || func.equals("RANK") ||
            func.equals("DENSE_RANK") || func.equals("NTILE")) {
            return IntegerType.get();
        }

        // Distribution functions return DoubleType
        if (func.equals("PERCENT_RANK") || func.equals("CUME_DIST")) {
            return DoubleType.get();
        }

        // COUNT always returns LongType
        if (func.equals("COUNT")) {
            return LongType.get();
        }

        // Functions that return the type of their first argument
        if (func.equals("LAG") || func.equals("LEAD") ||
            func.equals("FIRST") || func.equals("FIRST_VALUE") ||
            func.equals("LAST") || func.equals("LAST_VALUE") ||
            func.equals("NTH_VALUE") ||
            func.equals("MIN") || func.equals("MAX")) {

            if (!wf.arguments().isEmpty()) {
                DataType argType = resolveType(wf.arguments().get(0), schema);
                if (argType != null) {
                    return argType;
                }
            }
        }

        // SUM promotes type
        if (func.equals("SUM")) {
            if (!wf.arguments().isEmpty()) {
                DataType argType = resolveType(wf.arguments().get(0), schema);
                if (argType != null) {
                    if (argType instanceof IntegerType || argType instanceof LongType ||
                        argType instanceof ShortType || argType instanceof ByteType) {
                        return LongType.get();
                    }
                    if (argType instanceof FloatType || argType instanceof DoubleType) {
                        return DoubleType.get();
                    }
                    if (argType instanceof DecimalType) {
                        DecimalType decType = (DecimalType) argType;
                        int newPrecision = Math.min(decType.precision() + 10, 38);
                        return new DecimalType(newPrecision, decType.scale());
                    }
                    return argType;
                }
            }
            return LongType.get();
        }

        // AVG returns Double for numeric, Decimal for Decimal
        if (func.equals("AVG")) {
            if (!wf.arguments().isEmpty()) {
                DataType argType = resolveType(wf.arguments().get(0), schema);
                if (argType instanceof DecimalType) {
                    return argType; // AVG(Decimal) returns same precision Decimal
                }
            }
            return DoubleType.get();
        }

        // Statistical functions return Double
        if (func.equals("STDDEV") || func.equals("STDDEV_POP") || func.equals("STDDEV_SAMP") ||
            func.equals("VARIANCE") || func.equals("VAR_POP") || func.equals("VAR_SAMP")) {
            return DoubleType.get();
        }

        // Default: use window function's declared type
        return wf.dataType();
    }

    // ========================================================================
    // Binary Expression Type Resolution
    // ========================================================================

    /**
     * Resolves binary expression return type.
     */
    public static DataType resolveBinaryExpressionType(BinaryExpression binExpr, StructType schema) {
        BinaryExpression.Operator op = binExpr.operator();

        // Comparison and logical operators always return boolean
        if (op.isComparison() || op.isLogical()) {
            return BooleanType.get();
        }

        // Resolve operand types
        DataType leftType = resolveType(binExpr.left(), schema);
        DataType rightType = resolveType(binExpr.right(), schema);

        // Division: Decimal/Decimal returns Decimal, otherwise Double
        if (op == BinaryExpression.Operator.DIVIDE) {
            if (leftType instanceof DecimalType && rightType instanceof DecimalType) {
                return promoteDecimalDivision((DecimalType) leftType, (DecimalType) rightType);
            }
            return DoubleType.get();
        }

        // Multiplication: Handle Decimal arithmetic specially per Spark rules
        // Only use decimal promotion when at least one operand is already DecimalType
        // Integer * Integer stays as integer (uses numeric type promotion below)
        if (op == BinaryExpression.Operator.MULTIPLY) {
            boolean leftIsDecimal = leftType instanceof DecimalType;
            boolean rightIsDecimal = rightType instanceof DecimalType;

            // Only apply decimal rules if at least one operand is already decimal
            if (leftIsDecimal || rightIsDecimal) {
                DecimalType leftDec = toDecimalType(leftType, binExpr.left());
                DecimalType rightDec = toDecimalType(rightType, binExpr.right());
                if (leftDec != null && rightDec != null) {
                    return promoteDecimalMultiplication(leftDec, rightDec);
                }
            }
            // Otherwise fall through to numeric type promotion
        }

        // For other arithmetic operators, use numeric type promotion
        if (op.isArithmetic()) {
            return promoteNumericTypes(leftType, rightType);
        }

        // String concatenation returns String
        if (op == BinaryExpression.Operator.CONCAT) {
            return StringType.get();
        }

        // Default to left operand type
        return leftType;
    }

    // ========================================================================
    // CASE WHEN Type Resolution
    // ========================================================================

    /**
     * Resolves CASE WHEN expression return type by examining all branches.
     *
     * <p>This method resolves the types of all THEN and ELSE branches using the
     * provided schema, then unifies them to determine the result type. This is
     * critical for proper type inference when branches contain column references.
     *
     * <p>Type unification rules:
     * <ul>
     *   <li>If all branches are numeric, use numeric type promotion</li>
     *   <li>If types don't match, use the first non-null type</li>
     * </ul>
     *
     * @param caseWhen the CASE WHEN expression
     * @param schema the schema for resolving column types
     * @return the unified result type
     */
    public static DataType resolveCaseWhenType(CaseWhenExpression caseWhen, StructType schema) {
        DataType resultType = null;

        // Resolve all THEN branch types
        for (Expression thenBranch : caseWhen.thenBranches()) {
            DataType branchType = resolveType(thenBranch, schema);
            resultType = unifyTypes(resultType, branchType);
        }

        // Resolve ELSE branch type if present
        if (caseWhen.elseBranch() != null) {
            DataType elseType = resolveType(caseWhen.elseBranch(), schema);
            resultType = unifyTypes(resultType, elseType);
        }

        return resultType != null ? resultType : StringType.get();
    }

    /**
     * Resolves the type of an array literal with schema awareness.
     *
     * <p>If the array is empty, returns ArrayType(StringType, true).
     * Otherwise, unifies all element types to determine the element type.
     *
     * @param array the array literal expression
     * @param schema the schema for resolving column types
     * @return ArrayType with unified element type
     */
    public static DataType resolveArrayLiteralType(ArrayLiteralExpression array, StructType schema) {
        if (array.isEmpty()) {
            return new ArrayType(StringType.get(), true);
        }

        DataType elementType = null;
        for (Expression elem : array.elements()) {
            DataType elemType = resolveType(elem, schema);
            elementType = unifyTypes(elementType, elemType);
        }

        return new ArrayType(elementType != null ? elementType : StringType.get(), true);
    }

    /**
     * Resolves the type of a map literal with schema awareness.
     *
     * <p>If the map is empty, returns MapType(StringType, StringType, true).
     * Otherwise, unifies all key and value types separately.
     *
     * @param map the map literal expression
     * @param schema the schema for resolving column types
     * @return MapType with unified key and value types
     */
    public static DataType resolveMapLiteralType(MapLiteralExpression map, StructType schema) {
        if (map.isEmpty()) {
            return new MapType(StringType.get(), StringType.get(), true);
        }

        DataType keyType = null;
        for (Expression key : map.keys()) {
            DataType kType = resolveType(key, schema);
            keyType = unifyTypes(keyType, kType);
        }

        DataType valueType = null;
        for (Expression value : map.values()) {
            DataType vType = resolveType(value, schema);
            valueType = unifyTypes(valueType, vType);
        }

        return new MapType(
            keyType != null ? keyType : StringType.get(),
            valueType != null ? valueType : StringType.get(),
            true);
    }

    /**
     * Resolves the type of a struct literal with schema awareness.
     *
     * <p>Returns a StructType with field names from the literal and types
     * resolved from the schema.
     *
     * @param struct the struct literal expression
     * @param schema the schema for resolving column types
     * @return StructType with resolved field types
     */
    public static DataType resolveStructLiteralType(StructLiteralExpression struct, StructType schema) {
        List<StructField> fields = new java.util.ArrayList<>();

        List<String> fieldNames = struct.fieldNames();
        List<Expression> fieldValues = struct.fieldValues();

        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType type = resolveType(fieldValues.get(i), schema);
            boolean nullable = fieldValues.get(i).nullable();
            fields.add(new StructField(name, type, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Unifies two types for CASE WHEN branches.
     *
     * <p>Returns the common supertype of two types:
     * <ul>
     *   <li>If either is null, return the other</li>
     *   <li>If both are numeric, use numeric type promotion</li>
     *   <li>Otherwise, return the first non-null type</li>
     * </ul>
     *
     * @param a first type (may be null)
     * @param b second type (may be null)
     * @return the unified type
     */
    public static DataType unifyTypes(DataType a, DataType b) {
        if (a == null) return b;
        if (b == null) return a;

        // If both are numeric, use promotion
        if (isNumericType(a) && isNumericType(b)) {
            return promoteNumericTypes(a, b);
        }

        // Default to first type
        return a;
    }

    /**
     * Checks if a type is numeric.
     */
    private static boolean isNumericType(DataType type) {
        return type instanceof IntegerType ||
               type instanceof LongType ||
               type instanceof ShortType ||
               type instanceof ByteType ||
               type instanceof FloatType ||
               type instanceof DoubleType ||
               type instanceof DecimalType;
    }

    // ========================================================================
    // Function Call Type Resolution
    // ========================================================================

    /**
     * Resolves function call return type.
     */
    public static DataType resolveFunctionCallType(FunctionCall func, StructType schema) {
        DataType declaredType = func.dataType();
        String funcName = func.functionName().toLowerCase();

        // Handle ArrayType with unresolved elements
        if (declaredType instanceof ArrayType) {
            ArrayType arrType = (ArrayType) declaredType;
            boolean hasUnresolved = UnresolvedType.containsUnresolved(arrType) ||
                                   arrType.elementType() instanceof StringType;
            if (hasUnresolved) {
                if (FunctionCategories.isArrayTypePreserving(funcName) ||
                    FunctionCategories.isArraySetOperation(funcName)) {
                    if (!func.arguments().isEmpty()) {
                        DataType argType = resolveType(func.arguments().get(0), schema);
                        if (argType instanceof ArrayType) {
                            return argType;
                        }
                    }
                }
            }
            return resolveNestedType(declaredType, schema);
        }

        // Handle MapType
        if (declaredType instanceof MapType) {
            return resolveNestedType(declaredType, schema);
        }

        // Handle unresolved function return types
        if ((UnresolvedType.isUnresolved(declaredType) ||
             declaredType instanceof StringType) &&
            !func.arguments().isEmpty()) {

            // Array functions that preserve input type
            if (FunctionCategories.isArrayTypePreserving(funcName) ||
                FunctionCategories.isArraySetOperation(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    return argType;
                }
            }

            // Map extraction functions
            if (FunctionCategories.isMapExtraction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof MapType) {
                    MapType mapType = (MapType) argType;
                    return funcName.equals("map_keys")
                        ? new ArrayType(mapType.keyType(), false)
                        : new ArrayType(mapType.valueType(), mapType.valueContainsNull());
                }
            }

            // Element extraction
            if (FunctionCategories.isElementExtraction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    return ((ArrayType) argType).elementType();
                }
                if (argType instanceof MapType) {
                    return ((MapType) argType).valueType();
                }
            }

            // Explode functions
            if (FunctionCategories.isExplodeFunction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    return ((ArrayType) argType).elementType();
                }
            }

            // Flatten
            if (funcName.equals("flatten")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    DataType elemType = ((ArrayType) argType).elementType();
                    return (elemType instanceof ArrayType) ? elemType : argType;
                }
            }

            // Type-preserving functions
            if (FunctionCategories.isTypePreserving(funcName)) {
                return resolveType(func.arguments().get(0), schema);
            }
        }

        return declaredType;
    }

    // ========================================================================
    // Aggregate Type Resolution
    // ========================================================================

    /**
     * Infers the return type for an aggregate function.
     *
     * @param function the aggregate function name (e.g., "COUNT", "SUM")
     * @param argType the data type of the aggregate argument (may be null for COUNT(*))
     * @return the inferred return type
     */
    public static DataType resolveAggregateReturnType(String function, DataType argType) {
        String func = function.toUpperCase();

        switch (func) {
            case "COUNT":
                return LongType.get();

            case "SUM":
                if (argType instanceof IntegerType || argType instanceof LongType ||
                    argType instanceof ShortType || argType instanceof ByteType) {
                    return LongType.get();
                }
                if (argType instanceof FloatType || argType instanceof DoubleType) {
                    return DoubleType.get();
                }
                if (argType instanceof DecimalType) {
                    DecimalType decType = (DecimalType) argType;
                    int newPrecision = Math.min(decType.precision() + 10, 38);
                    return new DecimalType(newPrecision, decType.scale());
                }
                return DoubleType.get();

            case "AVG":
                if (argType instanceof DecimalType) {
                    DecimalType decType = (DecimalType) argType;
                    int newPrecision = Math.min(decType.precision() + 4, 38);
                    int newScale = Math.min(decType.scale() + 4, newPrecision);
                    return new DecimalType(newPrecision, newScale);
                }
                return DoubleType.get();

            case "MIN":
            case "MAX":
            case "FIRST":
            case "LAST":
                return argType != null ? argType : StringType.get();

            // Count distinct always returns Long (same as COUNT)
            case "COUNT_DISTINCT":
                return LongType.get();

            // Collection aggregates return ArrayType of the argument type
            case "COLLECT_LIST":
            case "COLLECT_SET":
            case "LIST":           // DuckDB function name for collect_list
            case "LIST_DISTINCT":  // DuckDB function name for collect_set
                if (argType != null) {
                    return new ArrayType(argType, true);
                }
                return new ArrayType(StringType.get(), true);

            default:
                return argType != null ? argType : StringType.get();
        }
    }

    // ========================================================================
    // Nullability Resolution
    // ========================================================================

    /**
     * Resolves the nullability of an expression.
     *
     * @param expr the expression to resolve
     * @param schema the schema to look up column nullability from (may be null)
     * @return true if the expression can produce null values
     */
    public static boolean resolveNullable(Expression expr, StructType schema) {
        if (expr == null) {
            return true;
        }

        // Handle AliasExpression - resolve the underlying expression
        if (expr instanceof AliasExpression) {
            return resolveNullable(((AliasExpression) expr).expression(), schema);
        }

        // Handle UnresolvedColumn - look up nullable from schema
        if (expr instanceof UnresolvedColumn) {
            UnresolvedColumn col = (UnresolvedColumn) expr;
            StructField field = findField(col.columnName(), schema);
            return field != null ? field.nullable() : col.nullable();
        }

        // Handle WindowFunction - delegate to its proper nullable logic
        if (expr instanceof WindowFunction) {
            return expr.nullable();
        }

        // Handle BinaryExpression - nullable if any operand is nullable
        if (expr instanceof BinaryExpression) {
            BinaryExpression binExpr = (BinaryExpression) expr;
            boolean leftNullable = resolveNullable(binExpr.left(), schema);
            boolean rightNullable = resolveNullable(binExpr.right(), schema);
            return leftNullable || rightNullable;
        }

        // Handle FunctionCall
        if (expr instanceof FunctionCall) {
            return resolveFunctionCallNullable((FunctionCall) expr, schema);
        }

        // Default: use the expression's own nullable()
        return expr.nullable();
    }

    /**
     * Resolves function call nullability.
     */
    private static boolean resolveFunctionCallNullable(FunctionCall func, StructType schema) {
        String funcName = func.functionName().toLowerCase();

        // Null-coalescing functions: non-null if ANY argument is non-null
        if (FunctionCategories.isNullCoalescing(funcName)) {
            for (Expression arg : func.arguments()) {
                if (!resolveNullable(arg, schema)) {
                    return false;
                }
            }
            return true;
        }

        return func.nullable();
    }

    // ========================================================================
    // Nested Type Resolution
    // ========================================================================

    /**
     * Recursively resolves nested types within complex types (ArrayType, MapType).
     *
     * @param type the type to resolve
     * @param schema the child schema for resolving column types
     * @return the resolved type, or the original if no resolution needed
     */
    public static DataType resolveNestedType(DataType type, StructType schema) {
        if (type instanceof ArrayType) {
            ArrayType arrType = (ArrayType) type;
            DataType elementType = arrType.elementType();
            DataType resolvedElement = resolveNestedType(elementType, schema);
            if (resolvedElement != elementType) {
                return new ArrayType(resolvedElement, arrType.containsNull());
            }
            return arrType;
        } else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            DataType keyType = mapType.keyType();
            DataType valueType = mapType.valueType();
            DataType resolvedKey = resolveNestedType(keyType, schema);
            DataType resolvedValue = resolveNestedType(valueType, schema);
            if (resolvedKey != keyType || resolvedValue != valueType) {
                return new MapType(resolvedKey, resolvedValue, mapType.valueContainsNull());
            }
            return mapType;
        } else if (type instanceof UnresolvedType) {
            return StringType.get();
        }
        return type;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    /**
     * Unwraps AliasExpression to get underlying expression.
     *
     * @param expr the expression to unwrap
     * @return the underlying expression if aliased, otherwise the original expression
     */
    public static Expression unwrapAlias(Expression expr) {
        if (expr instanceof AliasExpression) {
            return ((AliasExpression) expr).expression();
        }
        return expr;
    }
}
