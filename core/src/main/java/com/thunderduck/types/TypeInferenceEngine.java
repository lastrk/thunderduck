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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li>SUM/AVG/MIN/MAX: depends on argument nullability (non-nullable input → non-nullable result)</li>
 *   <li>Window ranking (ROW_NUMBER, RANK): false</li>
 *   <li>Window analytic (LAG, LEAD): true (unless default provided AND column non-nullable)</li>
 * </ul>
 */
public final class TypeInferenceEngine {

    private static final Logger logger = LoggerFactory.getLogger(TypeInferenceEngine.class);

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
        return switch (type) {
            case DecimalType d  -> d;
            case ByteType b     -> new DecimalType(3, 0);
            case ShortType s    -> new DecimalType(5, 0);
            case IntegerType i  -> new DecimalType(10, 0);
            case LongType l     -> new DecimalType(20, 0);
            default             -> new DecimalType(10, 0);
        };
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
        if (type instanceof DecimalType d) {
            return d;
        }

        // For integer types, promote to Decimal
        // If it's a literal, use the actual value to determine precision
        if (type instanceof IntegerType || type instanceof LongType ||
            type instanceof ShortType || type instanceof ByteType) {

            if (expr instanceof Literal lit) {
                Object value = lit.value();
                if (value instanceof Number num) {
                    // Calculate precision based on actual value
                    long absValue = Math.abs(num.longValue());
                    int precision = absValue == 0 ? 1 : (int) Math.log10(absValue) + 1;
                    return new DecimalType(precision, 0);
                }
            }

            // Default precision based on type
            return switch (type) {
                case ByteType b    -> new DecimalType(3, 0);
                case ShortType s   -> new DecimalType(5, 0);
                case IntegerType i -> new DecimalType(10, 0);
                case LongType l    -> new DecimalType(19, 0);
                default            -> new DecimalType(10, 0);
            };
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

        if (expr instanceof AliasExpression alias) {
            return resolveType(alias.expression(), schema);
        }
        if (expr instanceof UnresolvedColumn col) {
            DataType schemaType = lookupColumnType(col.columnName(), schema);
            return schemaType != null ? schemaType : col.dataType();
        }
        if (expr instanceof WindowFunction wf) {
            return resolveWindowFunctionType(wf, schema);
        }
        if (expr instanceof BinaryExpression bin) {
            return resolveBinaryExpressionType(bin, schema);
        }
        if (expr instanceof FunctionCall func) {
            return resolveFunctionCallType(func, schema);
        }
        if (expr instanceof CaseWhenExpression cw) {
            return resolveCaseWhenType(cw, schema);
        }
        if (expr instanceof ArrayLiteralExpression arr) {
            return resolveArrayLiteralType(arr, schema);
        }
        if (expr instanceof MapLiteralExpression map) {
            return resolveMapLiteralType(map, schema);
        }
        if (expr instanceof StructLiteralExpression st) {
            return resolveStructLiteralType(st, schema);
        }
        if (expr instanceof InExpression) {
            return BooleanType.get();
        }
        if (expr instanceof com.thunderduck.expression.ExtractValueExpression extract) {
            return resolveExtractValueType(extract, schema);
        }
        if (expr instanceof com.thunderduck.expression.FieldAccessExpression fieldAccess) {
            return resolveFieldAccessType(fieldAccess, schema);
        }
        if (expr instanceof com.thunderduck.expression.CastExpression cast) {
            return cast.targetType();
        }
        if (expr instanceof com.thunderduck.expression.UpdateFieldsExpression update) {
            return resolveUpdateFieldsType(update, schema);
        }
        if (expr instanceof com.thunderduck.expression.RawSQLExpression raw) {
            // For raw SQL with explicit type metadata, use it
            DataType rawType = raw.dataType();
            if (rawType != null && !UnresolvedType.isUnresolved(rawType) &&
                !(rawType instanceof StringType)) {
                return rawType;
            }
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
                    if (isIntegralType(argType)) {
                        return LongType.get();
                    } else if (argType instanceof FloatType || argType instanceof DoubleType) {
                        return DoubleType.get();
                    } else if (argType instanceof DecimalType decType) {
                        return new DecimalType(Math.min(decType.precision() + 10, 38), decType.scale());
                    }
                    return argType;
                }
            }
            return LongType.get();
        }

        // AVG returns Double for numeric, Decimal for Decimal
        // Spark's AVG(DECIMAL(p,s)) returns DECIMAL(min(p+4,38), min(min(s+4,18), min(p+4,38)))
        if (func.equals("AVG")) {
            if (!wf.arguments().isEmpty()) {
                DataType argType = resolveType(wf.arguments().get(0), schema);
                if (argType instanceof DecimalType decType) {
                    int newPrecision = Math.min(decType.precision() + 4, 38);
                    int newScale = Math.min(Math.min(decType.scale() + 4, 18), newPrecision);
                    return new DecimalType(newPrecision, newScale);
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
            if (leftType instanceof DecimalType leftDec && rightType instanceof DecimalType rightDec) {
                return promoteDecimalDivision(leftDec, rightDec);
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
            logger.debug("CASE WHEN THEN branch type: {}", branchType);

            // Skip untyped NULL literals in type unification
            // These should not affect the result type per Spark semantics
            if (!isUntypedNull(thenBranch)) {
                resultType = unifyTypes(resultType, branchType);
                logger.debug("After unifying THEN: {}", resultType);
            } else {
                logger.debug("Skipping untyped NULL in THEN branch");
            }
        }

        // Resolve ELSE branch type if present
        if (caseWhen.elseBranch() != null) {
            DataType elseType = resolveType(caseWhen.elseBranch(), schema);
            logger.debug("CASE WHEN ELSE branch type: {}, isUntypedNull: {}",
                        elseType, isUntypedNull(caseWhen.elseBranch()));

            // Skip untyped NULL literals
            if (!isUntypedNull(caseWhen.elseBranch())) {
                resultType = unifyTypes(resultType, elseType);
                logger.debug("After unifying ELSE: {}", resultType);
            } else {
                logger.debug("Skipping untyped NULL in ELSE branch");
            }
        }

        logger.debug("CASE WHEN final result type: {}", resultType);
        return resultType != null ? resultType : StringType.get();
    }

    /**
     * Resolves the type of an array literal with schema awareness.
     *
     * <p>If the array is empty, returns ArrayType(StringType, true).
     * Otherwise, unifies all element types to determine the element type,
     * and computes containsNull based on actual element nullability.
     *
     * @param array the array literal expression
     * @param schema the schema for resolving column types
     * @return ArrayType with unified element type and computed containsNull
     */
    public static DataType resolveArrayLiteralType(ArrayLiteralExpression array, StructType schema) {
        if (array.isEmpty()) {
            return new ArrayType(StringType.get(), true);
        }

        DataType elementType = null;
        boolean containsNull = false;
        for (Expression elem : array.elements()) {
            DataType elemType = resolveType(elem, schema);
            elementType = unifyTypes(elementType, elemType);
            if (resolveNullable(elem, schema)) {
                containsNull = true;
            }
        }

        return new ArrayType(elementType != null ? elementType : StringType.get(), containsNull);
    }

    /**
     * Resolves the type of a map literal with schema awareness.
     *
     * <p>If the map is empty, returns MapType(StringType, StringType, true).
     * Otherwise, unifies all key and value types separately, and computes
     * valueContainsNull based on actual value nullability.
     *
     * @param map the map literal expression
     * @param schema the schema for resolving column types
     * @return MapType with unified key/value types and computed valueContainsNull
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
        boolean valueContainsNull = false;
        for (Expression value : map.values()) {
            DataType vType = resolveType(value, schema);
            valueType = unifyTypes(valueType, vType);
            if (resolveNullable(value, schema)) {
                valueContainsNull = true;
            }
        }

        return new MapType(
            keyType != null ? keyType : StringType.get(),
            valueType != null ? valueType : StringType.get(),
            valueContainsNull);
    }

    /**
     * Resolves the type of a struct literal with schema awareness.
     *
     * <p>Returns a StructType with field names from the literal and types
     * resolved from the schema. Field nullability is computed using resolveNullable
     * to properly handle column references and other expression types.
     *
     * @param struct the struct literal expression
     * @param schema the schema for resolving column types
     * @return StructType with resolved field types and computed nullability
     */
    public static DataType resolveStructLiteralType(StructLiteralExpression struct, StructType schema) {
        List<StructField> fields = new java.util.ArrayList<>();

        List<String> fieldNames = struct.fieldNames();
        List<Expression> fieldValues = struct.fieldValues();

        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType type = resolveType(fieldValues.get(i), schema);
            boolean nullable = resolveNullable(fieldValues.get(i), schema);
            fields.add(new StructField(name, type, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Resolves the type of an UpdateFieldsExpression.
     *
     * <p>For ADD_OR_REPLACE: returns the struct type with the field added/replaced.
     * For DROP: returns the struct type with the field removed.
     */
    private static DataType resolveUpdateFieldsType(
            com.thunderduck.expression.UpdateFieldsExpression update, StructType schema) {
        DataType baseType = resolveType(update.structExpr(), schema);
        if (!(baseType instanceof StructType structType)) {
            return baseType;
        }

        String fieldName = update.fieldName();
        java.util.List<StructField> fields = new java.util.ArrayList<>(structType.fields());

        if (update.operationType() == com.thunderduck.expression.UpdateFieldsExpression.OperationType.ADD_OR_REPLACE) {
            DataType valueType = update.valueExpr()
                .map(v -> resolveType(v, schema))
                .orElse(StringType.get());
            boolean valueNullable = update.valueExpr()
                .map(v -> resolveNullable(v, schema))
                .orElse(true);

            // Replace existing field or add new one
            boolean replaced = false;
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                    fields.set(i, new StructField(fieldName, valueType, valueNullable));
                    replaced = true;
                    break;
                }
            }
            if (!replaced) {
                fields.add(new StructField(fieldName, valueType, valueNullable));
            }
        } else {
            // DROP: remove the named field
            fields.removeIf(f -> f.name().equalsIgnoreCase(fieldName));
        }

        return new StructType(fields);
    }

    /**
     * Resolves the type of an ExtractValueExpression by examining the child type.
     *
     * <p>For array subscript: returns the element type of the array.
     * For map subscript: returns the value type of the map.
     * For struct field: returns the field type from the struct.
     */
    private static DataType resolveExtractValueType(
            com.thunderduck.expression.ExtractValueExpression extract, StructType schema) {
        DataType childType = resolveType(extract.child(), schema);

        return switch (extract.extractionType()) {
            case ARRAY_INDEX -> {
                // Could be array or map subscript (parser uses ARRAY_INDEX for both)
                if (childType instanceof ArrayType arrType) {
                    yield arrType.elementType();
                }
                if (childType instanceof MapType mapType) {
                    yield mapType.valueType();
                }
                yield extract.dataType();
            }
            case MAP_KEY -> {
                if (childType instanceof MapType mapType) {
                    yield mapType.valueType();
                }
                yield extract.dataType();
            }
            case STRUCT_FIELD -> {
                if (childType instanceof StructType structType) {
                    com.thunderduck.expression.Expression extractionExpr = extract.extraction();
                    if (extractionExpr instanceof com.thunderduck.expression.Literal lit
                            && lit.value() instanceof String fieldName) {
                        StructField field = findField(fieldName, structType);
                        if (field != null) {
                            yield field.dataType();
                        }
                    }
                }
                yield extract.dataType();
            }
        };
    }

    /**
     * Resolves the type of a FieldAccessExpression (base.fieldName) by looking up
     * the field in the base's struct type.
     */
    private static DataType resolveFieldAccessType(
            com.thunderduck.expression.FieldAccessExpression fieldAccess, StructType schema) {
        DataType baseType = resolveType(fieldAccess.base(), schema);

        if (baseType instanceof StructType structType) {
            StructField field = findField(fieldAccess.fieldName(), structType);
            if (field != null) {
                return field.dataType();
            }
        }

        // Fallback: try to look up as a schema column
        // e.g., table.column where table is an alias
        DataType schemaType = lookupColumnType(fieldAccess.fieldName(), schema);
        if (schemaType != null) {
            return schemaType;
        }

        return fieldAccess.dataType();
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
    /**
     * Checks if a type is an integral (non-decimal, non-float) numeric type.
     */
    private static boolean isIntegralType(DataType type) {
        return type instanceof IntegerType
            || type instanceof LongType
            || type instanceof ShortType
            || type instanceof ByteType;
    }

    /**
     * Checks if a type is numeric.
     */
    private static boolean isNumericType(DataType type) {
        return type instanceof IntegerType
            || type instanceof LongType
            || type instanceof ShortType
            || type instanceof ByteType
            || type instanceof FloatType
            || type instanceof DoubleType
            || type instanceof DecimalType;
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
        if (declaredType instanceof ArrayType arrType) {
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
                // Collection aggregates: resolve element type from argument
                // Spark's collect_list/collect_set skip nulls, so containsNull=false
                if (funcName.equals("collect_list") || funcName.equals("collect_set") ||
                    funcName.equals("list") || funcName.equals("list_distinct") ||
                    funcName.equals("array_agg")) {
                    if (!func.arguments().isEmpty()) {
                        DataType argType = resolveType(func.arguments().get(0), schema);
                        return new ArrayType(argType, false);
                    }
                }
                // Array constructor: resolve element type from arguments
                if (funcName.equals("array")) {
                    if (!func.arguments().isEmpty()) {
                        DataType elemType = resolveType(func.arguments().get(0), schema);
                        boolean containsNull = func.arguments().stream()
                            .anyMatch(arg -> resolveNullable(arg, schema));
                        return new ArrayType(elemType, containsNull);
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

            // Array constructor: array(1, 2, 3) -> ArrayType(IntegerType, false)
            if (funcName.equals("array")) {
                DataType elemType = resolveType(func.arguments().get(0), schema);
                boolean containsNull = func.arguments().stream()
                    .anyMatch(arg -> resolveNullable(arg, schema));
                return new ArrayType(elemType, containsNull);
            }

            // Map constructor: map(k1, v1, k2, v2) -> MapType(keyType, valueType, valueContainsNull)
            if (funcName.equals("map") || funcName.equals("create_map")) {
                if (func.arguments().size() >= 2) {
                    DataType keyType = resolveType(func.arguments().get(0), schema);
                    DataType valueType = resolveType(func.arguments().get(1), schema);
                    boolean valueContainsNull = false;
                    for (int i = 1; i < func.arguments().size(); i += 2) {
                        if (resolveNullable(func.arguments().get(i), schema)) {
                            valueContainsNull = true;
                            break;
                        }
                    }
                    return new MapType(keyType, valueType, valueContainsNull);
                }
            }

            // map_from_arrays: map_from_arrays(keys_array, values_array) -> MapType
            if (funcName.equals("map_from_arrays") && func.arguments().size() >= 2) {
                DataType keysType = resolveType(func.arguments().get(0), schema);
                DataType valuesType = resolveType(func.arguments().get(1), schema);
                if (keysType instanceof ArrayType keyArr && valuesType instanceof ArrayType valArr) {
                    return new MapType(keyArr.elementType(), valArr.elementType(), valArr.containsNull());
                }
            }

            // Struct constructor: named_struct/struct(name1, val1, ...) -> StructType
            if (funcName.equals("named_struct")) {
                java.util.List<StructField> fields = new java.util.ArrayList<>();
                for (int i = 0; i + 1 < func.arguments().size(); i += 2) {
                    Expression nameExpr = func.arguments().get(i);
                    Expression valExpr = func.arguments().get(i + 1);
                    String fieldName = (nameExpr instanceof Literal lit) ? String.valueOf(lit.value()) : nameExpr.toSQL();
                    DataType fieldType = resolveType(valExpr, schema);
                    boolean fieldNullable = resolveNullable(valExpr, schema);
                    fields.add(new StructField(fieldName, fieldType, fieldNullable));
                }
                return new StructType(fields);
            }

            // struct() with alias args: struct(lit(99).alias("id"), ...) -> StructType
            if (funcName.equals("struct")) {
                java.util.List<StructField> fields = new java.util.ArrayList<>();
                for (Expression arg : func.arguments()) {
                    String fieldName;
                    Expression valExpr;
                    if (arg instanceof AliasExpression alias) {
                        fieldName = alias.alias();
                        valExpr = alias.expression();
                    } else {
                        fieldName = arg.toSQL();
                        valExpr = arg;
                    }
                    DataType fieldType = resolveType(valExpr, schema);
                    boolean fieldNullable = resolveNullable(valExpr, schema);
                    fields.add(new StructField(fieldName, fieldType, fieldNullable));
                }
                return new StructType(fields);
            }

            // Collection aggregates: collect_list/collect_set -> ArrayType(argType, false)
            // Spark's collect_list/collect_set skip nulls, so containsNull=false
            if (funcName.equals("collect_list") || funcName.equals("collect_set") ||
                funcName.equals("list") || funcName.equals("list_distinct") ||
                funcName.equals("array_agg")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                return new ArrayType(argType, false);
            }

            // Array functions that preserve input type
            if (FunctionCategories.isArrayTypePreserving(funcName) ||
                FunctionCategories.isArraySetOperation(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    return argType;
                }
            }

            // Lambda-based array functions that return arrays
            // list_transform: transform(array, lambda) -> ArrayType with transformed element type
            // list_filter/filter: returns same ArrayType as input
            if (funcName.equals("list_transform") || funcName.equals("transform")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    // Transform may change element type, but without evaluating the lambda
                    // we return the input array type as best estimate
                    return argType;
                }
            }
            if (funcName.equals("list_filter") || funcName.equals("filter") ||
                funcName.equals("array_filter")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType) {
                    return argType;
                }
            }

            // Map extraction functions
            if (FunctionCategories.isMapExtraction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof MapType mapType) {
                    return funcName.equals("map_keys")
                        ? new ArrayType(mapType.keyType(), false)
                        : new ArrayType(mapType.valueType(), mapType.valueContainsNull());
                }
            }

            // Element extraction
            if (FunctionCategories.isElementExtraction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType a) {
                    return a.elementType();
                }
                if (argType instanceof MapType m) {
                    return m.valueType();
                }
            }

            // Explode functions
            if (FunctionCategories.isExplodeFunction(funcName)) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType a) {
                    return a.elementType();
                }
            }

            // Flatten
            if (funcName.equals("flatten")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType a) {
                    DataType elemType = a.elementType();
                    return (elemType instanceof ArrayType) ? elemType : argType;
                }
            }

            // Boolean-returning functions
            if (FunctionCategories.isBooleanReturning(funcName) ||
                funcName.equals("exists") || funcName.equals("forall") ||
                funcName.equals("list_bool_or") || funcName.equals("list_bool_and")) {
                return BooleanType.get();
            }

            // Type-preserving functions: some return the first argument's type,
            // others promote across all arguments (Spark's ANSI type coercion rules).
            if (FunctionCategories.isTypePreserving(funcName)) {
                if (func.arguments().size() <= 1) {
                    // Single-argument: just return the argument's type
                    return resolveType(func.arguments().get(0), schema);
                }
                // Multi-argument type-promoting functions:
                // coalesce, nvl, ifnull, greatest, least, mod, pmod, nvl2, if
                // These promote types across all (or relevant) arguments.
                // nullif is special: returns the first argument's type only.
                if (funcName.equals("nullif")) {
                    return resolveType(func.arguments().get(0), schema);
                }
                if (funcName.equals("if")) {
                    // IF(cond, then, else): promote THEN and ELSE (args 1 and 2)
                    if (func.arguments().size() >= 3) {
                        DataType thenType = resolveType(func.arguments().get(1), schema);
                        DataType elseType = resolveType(func.arguments().get(2), schema);
                        return unifyTypes(thenType, elseType);
                    }
                    return resolveType(func.arguments().get(1), schema);
                }
                if (funcName.equals("nvl2")) {
                    // nvl2(cond, valueIfNotNull, valueIfNull): promote args 1 and 2
                    if (func.arguments().size() >= 3) {
                        DataType notNullType = resolveType(func.arguments().get(1), schema);
                        DataType nullType = resolveType(func.arguments().get(2), schema);
                        return unifyTypes(notNullType, nullType);
                    }
                    return resolveType(func.arguments().get(1), schema);
                }
                // Default for multi-arg type-preserving: promote across all arguments
                // Covers: coalesce, nvl, ifnull, greatest, least, mod, pmod
                DataType resultType = null;
                for (Expression arg : func.arguments()) {
                    DataType argType = resolveType(arg, schema);
                    resultType = unifyTypes(resultType, argType);
                }
                return resultType != null ? resultType : resolveType(func.arguments().get(0), schema);
            }

            // list_reduce/aggregate: returns the accumulator type (same as initial value)
            if (funcName.equals("list_reduce") || funcName.equals("aggregate") ||
                funcName.equals("reduce")) {
                // The second argument is the initial value; its type is the result type
                if (func.arguments().size() >= 2) {
                    return resolveType(func.arguments().get(1), schema);
                }
            }

            // Aggregate functions: resolve return type based on argument type
            // This handles SUM/AVG/MIN/MAX/COUNT when used in composite expressions
            // (e.g., SUM(a) / SUM(b)) where the declared type is UnresolvedType
            String upperFuncName = funcName.toUpperCase();
            if (upperFuncName.equals("SUM") || upperFuncName.equals("SUM_DISTINCT") ||
                upperFuncName.equals("AVG") || upperFuncName.equals("AVG_DISTINCT") ||
                upperFuncName.equals("MIN") || upperFuncName.equals("MAX") ||
                upperFuncName.equals("FIRST") || upperFuncName.equals("LAST") ||
                upperFuncName.equals("COUNT") || upperFuncName.equals("COUNT_DISTINCT")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                DataType aggResult = resolveAggregateReturnType(upperFuncName, argType);
                if (aggResult != null) {
                    return aggResult;
                }
            }

            // String functions
            if (funcName.matches("concat|concat_ws|upper|lower|ucase|lcase|" +
                    "trim|ltrim|rtrim|" +
                    "lpad|rpad|repeat|" +
                    "substring|substr|left|right|" +
                    "replace|translate|regexp_replace|regexp_extract|" +
                    "split_part|initcap|format_string|printf|" +
                    "from_unixtime|base64|unbase64|hex|unhex|md5|sha1|sha2")) {
                return StringType.get();
            }

            // Date/time functions
            if (funcName.matches("to_date|last_day|next_day|date_add|date_sub|add_months")) {
                return DateType.get();
            }
            if (funcName.matches("to_timestamp|date_trunc")) {
                return TimestampType.get();
            }

            // Integer-returning functions
            if (FunctionCategories.isIntegerReturning(funcName) ||
                funcName.matches("year|month|day|dayofmonth|dayofweek|dayofyear|" +
                    "hour|minute|second|quarter|weekofyear|week|datediff|size")) {
                return IntegerType.get();
            }

            // String length functions return IntegerType in Spark 4.x
            if (funcName.matches("length|char_length|character_length")) {
                return IntegerType.get();
            }

            // Long-returning functions
            if (funcName.matches("unix_timestamp|array_position")) {
                return LongType.get();
            }

            // Double-returning functions
            if (funcName.matches("sqrt|log|ln|log10|log2|exp|expm1|" +
                    "sin|cos|tan|asin|acos|atan|atan2|sinh|cosh|tanh|" +
                    "radians|degrees|cbrt|hypot|pow|power|" +
                    "sign|signum|round|bround|truncate|months_between")) {
                return DoubleType.get();
            }

            // Ceil/floor return Long in Spark
            if (funcName.matches("ceil|ceiling|floor")) {
                return LongType.get();
            }

            // split returns ArrayType(StringType, false)
            if (funcName.equals("split")) {
                return new ArrayType(StringType.get(), false);
            }

            // map_entries(map) -> ArrayType(StructType([key, value]), false)
            if (funcName.equals("map_entries")) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof MapType mapType) {
                    java.util.List<StructField> entryFields = java.util.List.of(
                        new StructField("key", mapType.keyType(), false),
                        new StructField("value", mapType.valueType(), mapType.valueContainsNull()));
                    return new ArrayType(new StructType(entryFields), false);
                }
                return new ArrayType(StringType.get(), false);
            }

            // grouping() returns ByteType
            if (funcName.equals("grouping")) {
                return ByteType.get();
            }

            // grouping_id() returns LongType
            if (funcName.equals("grouping_id")) {
                return LongType.get();
            }
        }

        // Also handle functions with empty arguments but known return types
        if (UnresolvedType.isUnresolved(declaredType) || declaredType instanceof StringType) {
            // COUNT with no args (count(*))
            if (funcName.equals("count") || funcName.equals("count_distinct")) {
                return LongType.get();
            }
            // Boolean-returning functions
            if (FunctionCategories.isBooleanReturning(funcName)) {
                return BooleanType.get();
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
        // Normalize _DISTINCT suffix: SUM_DISTINCT -> SUM, AVG_DISTINCT -> AVG, etc.
        // ExpressionConverter appends _DISTINCT for DISTINCT aggregates.
        if (func.endsWith("_DISTINCT") && !func.equals("COUNT_DISTINCT")) {
            func = func.substring(0, func.length() - "_DISTINCT".length());
        }
        logger.debug("resolveAggregateReturnType: function={}, normalized={}, argType={}", function, func, argType);

        switch (func) {
            case "COUNT":
                return LongType.get();

            case "SUM":
                if (argType != null) {
                    if (isIntegralType(argType)) {
                        return LongType.get();
                    } else if (argType instanceof FloatType || argType instanceof DoubleType) {
                        return DoubleType.get();
                    } else if (argType instanceof DecimalType decType) {
                        int newPrecision = Math.min(decType.precision() + 10, 38);
                        DataType resultType = new DecimalType(newPrecision, decType.scale());
                        logger.debug("SUM decimal: input={}, result={}", decType, resultType);
                        return resultType;
                    }
                    return DoubleType.get();
                }
                return DoubleType.get();

            case "AVG":
                if (argType instanceof DecimalType decType) {
                    int newPrecision = Math.min(decType.precision() + 4, 38);
                    int newScale = Math.min(Math.min(decType.scale() + 4, 18), newPrecision);
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
            // Spark's collect_list/collect_set skip nulls, so containsNull=false
            case "COLLECT_LIST":
            case "COLLECT_SET":
            case "LIST":           // DuckDB function name for collect_list
            case "LIST_DISTINCT":  // DuckDB function name for collect_set
                if (argType != null) {
                    return new ArrayType(argType, false);
                }
                return new ArrayType(StringType.get(), false);

            default:
                return argType != null ? argType : StringType.get();
        }
    }

    /**
     * Resolves nullability for aggregate functions per Spark semantics.
     *
     * <p>Spark rules:
     * <ul>
     *   <li>COUNT(*), COUNT(col): always non-nullable (returns 0 for empty groups)</li>
     *   <li>SUM/AVG/MIN/MAX/FIRST/LAST: always nullable (returns NULL for empty groups)</li>
     *   <li>COLLECT_LIST/COLLECT_SET: always nullable (array itself can be null)</li>
     *   <li>Statistical functions (STDDEV, VAR): nullable</li>
     * </ul>
     *
     * @param function the aggregate function name
     * @param argument the aggregate argument expression (may be null for COUNT(*))
     * @param schema the schema to resolve argument nullability from
     * @return true if the aggregate result can be null
     */
    public static boolean resolveAggregateNullable(String function, Expression argument, StructType schema) {
        String funcUpper = function.toUpperCase();
        // Normalize _DISTINCT suffix: SUM_DISTINCT -> SUM, AVG_DISTINCT -> AVG, etc.
        if (funcUpper.endsWith("_DISTINCT") && !funcUpper.equals("COUNT_DISTINCT")) {
            funcUpper = funcUpper.substring(0, funcUpper.length() - "_DISTINCT".length());
        }

        // COUNT is always non-nullable (returns 0 for empty groups)
        if (funcUpper.equals("COUNT") || funcUpper.equals("COUNT_DISTINCT")) {
            return false;
        }

        // Collection aggregates are always nullable
        if (funcUpper.equals("COLLECT_LIST") || funcUpper.equals("COLLECT_SET") ||
            funcUpper.equals("ARRAY_AGG") || funcUpper.equals("LIST") ||
            funcUpper.equals("LIST_DISTINCT")) {
            return true;
        }

        // Statistical functions are always nullable
        if (funcUpper.equals("STDDEV") || funcUpper.equals("STDDEV_POP") ||
            funcUpper.equals("STDDEV_SAMP") || funcUpper.equals("VARIANCE") ||
            funcUpper.equals("VAR_POP") || funcUpper.equals("VAR_SAMP") ||
            funcUpper.equals("COVAR_POP") || funcUpper.equals("COVAR_SAMP") ||
            funcUpper.equals("CORR") || funcUpper.equals("PERCENTILE") ||
            funcUpper.equals("PERCENTILE_APPROX")) {
            return true;
        }

        // For SUM/AVG/MIN/MAX/FIRST/LAST: always nullable per Spark semantics
        // (returns NULL for empty groups or when all inputs are NULL)
        if (funcUpper.equals("SUM") || funcUpper.equals("AVG") ||
            funcUpper.equals("MIN") || funcUpper.equals("MAX") ||
            funcUpper.equals("FIRST") || funcUpper.equals("LAST") ||
            funcUpper.equals("FIRST_VALUE") || funcUpper.equals("LAST_VALUE") ||
            funcUpper.equals("ANY_VALUE")) {
            return true;
        }

        // Default: nullable
        return true;
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

        if (expr instanceof AliasExpression alias) {
            return resolveNullable(alias.expression(), schema);
        }
        if (expr instanceof Literal lit) {
            return lit.isNull();
        }
        if (expr instanceof UnresolvedColumn col) {
            StructField field = findField(col.columnName(), schema);
            return field != null ? field.nullable() : col.nullable();
        }
        if (expr instanceof WindowFunction) {
            return expr.nullable();
        }
        if (expr instanceof BinaryExpression bin) {
            // Comparison and logical operators follow the same rule:
            // nullable if any operand is nullable
            return resolveNullable(bin.left(), schema) || resolveNullable(bin.right(), schema);
        }
        if (expr instanceof com.thunderduck.expression.UnaryExpression unary) {
            // IS NULL/IS NOT NULL and IS TRUE/FALSE/UNKNOWN always return non-null boolean
            com.thunderduck.expression.UnaryExpression.Operator op = unary.operator();
            if (op == com.thunderduck.expression.UnaryExpression.Operator.IS_NULL ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_NOT_NULL ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_TRUE ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_NOT_TRUE ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_FALSE ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_NOT_FALSE ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_UNKNOWN ||
                op == com.thunderduck.expression.UnaryExpression.Operator.IS_NOT_UNKNOWN) {
                return false;
            }
            // Other operators (NEGATE, NOT): nullable follows operand
            return resolveNullable(unary.operand(), schema);
        }
        if (expr instanceof FunctionCall func) {
            return resolveFunctionCallNullable(func, schema);
        }
        if (expr instanceof com.thunderduck.expression.CastExpression cast) {
            // CAST nullable follows the inner expression's nullable
            return resolveNullable(cast.expression(), schema);
        }
        if (expr instanceof com.thunderduck.expression.UpdateFieldsExpression update) {
            return resolveNullable(update.structExpr(), schema);
        }
        if (expr instanceof com.thunderduck.expression.ExtractValueExpression) {
            // Value extraction from collections is always nullable
            // (out of bounds, missing key, etc.)
            return true;
        }
        if (expr instanceof com.thunderduck.expression.FieldAccessExpression fieldAccess) {
            // Spark rule: struct_expr.nullable || field.nullable
            // If the struct itself can be null, the field access can also be null.
            boolean baseNullable = resolveNullable(fieldAccess.base(), schema);
            DataType baseType = resolveType(fieldAccess.base(), schema);
            if (baseType instanceof StructType structType) {
                StructField field = findField(fieldAccess.fieldName(), structType);
                if (field != null) {
                    return baseNullable || field.nullable();
                }
            }
            // Fallback: also try schema lookup (for table.column patterns)
            StructField schemaField = findField(fieldAccess.fieldName(), schema);
            if (schemaField != null) {
                return baseNullable || schemaField.nullable();
            }
            return true;
        }
        if (expr instanceof ArrayLiteralExpression || expr instanceof MapLiteralExpression ||
            expr instanceof StructLiteralExpression) {
            // Complex type constructors: the container itself is never null
            return false;
        }
        if (expr instanceof CaseWhenExpression caseWhen) {
            // CASE WHEN is nullable if:
            // - No ELSE branch (implicit NULL)
            // - Any THEN or ELSE branch is nullable (resolved with schema)
            return resolveCaseWhenNullable(caseWhen, schema);
        }
        if (expr instanceof com.thunderduck.expression.LambdaExpression lambda) {
            // Lambda itself is not directly nullable in a meaningful sense;
            // the nullable semantics come from the HOF using it (transform, filter, etc.)
            return resolveNullable(lambda.body(), schema);
        }
        if (expr instanceof com.thunderduck.expression.LambdaVariableExpression) {
            // Lambda variables inherit nullability from the array element's containsNull.
            // Without context, default to true (safe fallback).
            return true;
        }
        if (expr instanceof InExpression) {
            // IN expressions return boolean, can be null if any operand is null
            return true;
        }
        if (expr instanceof com.thunderduck.expression.RawSQLExpression raw) {
            // Use explicit nullable if available
            return raw.nullable();
        }

        // Default: use the expression's own nullable()
        return expr.nullable();
    }

    /**
     * Resolves function call nullability per Spark semantics.
     *
     * <p>This is the centralized authority for function nullability. It must handle:
     * <ul>
     *   <li>Aggregate functions (COUNT always non-null, SUM/AVG depend on input)</li>
     *   <li>Complex type constructors (array, map, struct: never null)</li>
     *   <li>Boolean check functions (isnull, isnotnull: never null)</li>
     *   <li>Null-coalescing functions (non-null if any arg is non-null)</li>
     *   <li>General functions (nullable if any arg is nullable)</li>
     * </ul>
     */
    private static boolean resolveFunctionCallNullable(FunctionCall func, StructType schema) {
        String funcName = func.functionName().toLowerCase();
        String funcUpper = funcName.toUpperCase();

        // Normalize _DISTINCT suffix
        String normalizedUpper = funcUpper;
        if (funcUpper.endsWith("_DISTINCT") && !funcUpper.equals("COUNT_DISTINCT")) {
            normalizedUpper = funcUpper.substring(0, funcUpper.length() - "_DISTINCT".length());
        }

        // COUNT is always non-nullable (returns 0 for empty groups)
        if (normalizedUpper.equals("COUNT") || funcUpper.equals("COUNT_DISTINCT")) {
            return false;
        }

        // Complex type constructors are never null (the container itself is not null)
        if (funcName.matches("array|map|create_map|named_struct|struct")) {
            return false;
        }

        // Boolean check functions always return non-null boolean
        if (funcName.matches("isnull|isnotnull|isnan")) {
            return false;
        }

        // grouping/grouping_id are always non-nullable
        if (funcName.equals("grouping") || funcName.equals("grouping_id")) {
            return false;
        }

        // Null-coalescing functions: non-null if ANY argument is non-null
        if (FunctionCategories.isNullCoalescing(funcName)) {
            for (Expression arg : func.arguments()) {
                if (!resolveNullable(arg, schema)) {
                    return false;
                }
            }
            return true;
        }

        // concat_ws is only nullable if separator (first arg) is nullable
        if (funcName.equals("concat_ws") && !func.arguments().isEmpty()) {
            return resolveNullable(func.arguments().get(0), schema);
        }

        // nvl2(a, b, c): returns b if a is non-null, else c.
        // Result nullable = b.nullable AND c.nullable (nullable only if BOTH branches are nullable)
        if (funcName.equals("nvl2") && func.arguments().size() >= 3) {
            return resolveNullable(func.arguments().get(1), schema)
                && resolveNullable(func.arguments().get(2), schema);
        }

        // Aggregate functions: SUM, AVG, MIN, MAX, FIRST, LAST
        // Always nullable per Spark semantics (returns NULL for empty groups)
        // This must be consistent with resolveAggregateNullable().
        if (normalizedUpper.equals("SUM") || normalizedUpper.equals("AVG") ||
            normalizedUpper.equals("MIN") || normalizedUpper.equals("MAX") ||
            normalizedUpper.equals("FIRST") || normalizedUpper.equals("LAST") ||
            normalizedUpper.equals("FIRST_VALUE") || normalizedUpper.equals("LAST_VALUE") ||
            normalizedUpper.equals("ANY_VALUE")) {
            return true;
        }

        // Collection aggregates are always nullable
        if (normalizedUpper.equals("COLLECT_LIST") || normalizedUpper.equals("COLLECT_SET") ||
            normalizedUpper.equals("ARRAY_AGG") || normalizedUpper.equals("LIST") ||
            normalizedUpper.equals("LIST_DISTINCT")) {
            return true;
        }

        // Statistical functions are always nullable
        if (normalizedUpper.equals("STDDEV") || normalizedUpper.equals("STDDEV_POP") ||
            normalizedUpper.equals("STDDEV_SAMP") || normalizedUpper.equals("VARIANCE") ||
            normalizedUpper.equals("VAR_POP") || normalizedUpper.equals("VAR_SAMP") ||
            normalizedUpper.equals("COVAR_POP") || normalizedUpper.equals("COVAR_SAMP") ||
            normalizedUpper.equals("CORR") || normalizedUpper.equals("PERCENTILE") ||
            normalizedUpper.equals("PERCENTILE_APPROX")) {
            return true;
        }

        // Math functions: always nullable per Spark semantics.
        // Spark marks ceil/floor/round/log/exp/sqrt/pow etc. as nullable because
        // the function implementations can return null for edge cases (e.g., log(0),
        // division by zero). Even with non-nullable inputs, results are nullable.
        if (funcName.matches("ceil|ceiling|floor|round|bround|" +
                "log|ln|log10|log2|exp|expm1|sqrt|cbrt|" +
                "pow|power|hypot|" +
                "sin|cos|tan|asin|acos|atan|atan2|sinh|cosh|tanh|" +
                "radians|degrees|sign|signum|" +
                "abs|negative|positive|" +
                "truncate|months_between")) {
            return true;
        }

        // ---- Higher-Order Functions (Lambda) ----
        // transform(array, lambda): result nullable = array argument's nullable
        // The output array itself can only be null if the input array can be null.
        if (funcName.equals("transform") || funcName.equals("list_transform")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }
        // filter(array, lambda): result nullable = array argument's nullable
        if (funcName.equals("filter") || funcName.equals("list_filter") ||
            funcName.equals("array_filter")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }
        // exists(array, lambda): always returns a definite boolean
        // forall(array, lambda): always returns a definite boolean
        if (funcName.equals("exists") || funcName.equals("forall") ||
            funcName.equals("list_bool_or") || funcName.equals("list_bool_and")) {
            return false;
        }
        // aggregate/reduce(array, init, merge, finish):
        // Always nullable per Spark semantics (null if input array is null)
        if (funcName.equals("aggregate") || funcName.equals("reduce") ||
            funcName.equals("list_reduce")) {
            return true;
        }

        // ---- Array type-preserving functions ----
        // sort_array, array_sort, array_distinct, reverse, slice, shuffle
        // nullable = input array's nullable
        if (FunctionCategories.isArrayTypePreserving(funcName)) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- Array set operations ----
        // array_union, array_intersect, array_except: nullable if any input is nullable
        if (FunctionCategories.isArraySetOperation(funcName)) {
            for (Expression arg : func.arguments()) {
                if (resolveNullable(arg, schema)) {
                    return true;
                }
            }
            return false;
        }

        // ---- Size/length functions ----
        // size(array/map): nullable = input nullable (Spark returns null if input is null)
        if (funcName.equals("size") || funcName.equals("array_size") ||
            funcName.equals("cardinality") || funcName.equals("map_size")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- Map extraction functions ----
        // map_keys, map_values: nullable = input map's nullable
        if (FunctionCategories.isMapExtraction(funcName)) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- Element extraction ----
        // element_at: always nullable (key may not exist, index out of bounds)
        if (FunctionCategories.isElementExtraction(funcName)) {
            return true;
        }

        // ---- Explode functions ----
        // explode, explode_outer: nullable depends on array containsNull
        if (FunctionCategories.isExplodeFunction(funcName)) {
            if (!func.arguments().isEmpty()) {
                DataType argType = resolveType(func.arguments().get(0), schema);
                if (argType instanceof ArrayType arrType) {
                    return arrType.containsNull();
                }
            }
            return true;
        }

        // ---- split ----
        // split: nullable = input string's nullable
        if (funcName.equals("split")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- map_entries ----
        // map_entries: nullable = input map's nullable
        if (funcName.equals("map_entries")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- Flatten ----
        // flatten: nullable = input array's nullable
        if (funcName.equals("flatten")) {
            if (!func.arguments().isEmpty()) {
                return resolveNullable(func.arguments().get(0), schema);
            }
            return true;
        }

        // ---- map_from_arrays ----
        // map_from_arrays: nullable if either input array is nullable
        if (funcName.equals("map_from_arrays")) {
            for (Expression arg : func.arguments()) {
                if (resolveNullable(arg, schema)) {
                    return true;
                }
            }
            return false;
        }

        // ---- String functions: nullable if input is nullable ----
        // Most string functions follow "nullable if any arg nullable" which is
        // handled by the general rule below.

        // General functions: nullable if any argument is nullable
        // This matches Spark's default behavior
        if (!func.arguments().isEmpty()) {
            return func.arguments().stream().anyMatch(arg -> resolveNullable(arg, schema));
        }

        // No arguments (e.g., current_timestamp, current_date): non-nullable
        // These are deterministic functions that never return null
        return func.nullable();
    }

    /**
     * Resolves CASE WHEN expression nullability with schema awareness.
     *
     * <p>A CASE WHEN expression is nullable if:
     * <ul>
     *   <li>No ELSE branch is specified (implicit NULL when no condition matches)</li>
     *   <li>The ELSE branch is nullable (resolved with schema)</li>
     *   <li>Any THEN branch is nullable (resolved with schema)</li>
     * </ul>
     */
    private static boolean resolveCaseWhenNullable(CaseWhenExpression caseWhen, StructType schema) {
        // Nullable if no ELSE (implicit NULL when no condition matches)
        if (caseWhen.elseBranch() == null) {
            return true;
        }

        // Nullable if ELSE is nullable
        if (resolveNullable(caseWhen.elseBranch(), schema)) {
            return true;
        }

        // Nullable if any THEN branch is nullable
        for (Expression branch : caseWhen.thenBranches()) {
            if (resolveNullable(branch, schema)) {
                return true;
            }
        }

        return false;
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
        return switch (type) {
            case ArrayType arrType -> {
                DataType elementType = arrType.elementType();
                DataType resolvedElement = resolveNestedType(elementType, schema);
                yield resolvedElement != elementType
                    ? new ArrayType(resolvedElement, arrType.containsNull())
                    : arrType;
            }
            case MapType mapType -> {
                DataType keyType = mapType.keyType();
                DataType valueType = mapType.valueType();
                DataType resolvedKey = resolveNestedType(keyType, schema);
                DataType resolvedValue = resolveNestedType(valueType, schema);
                yield (resolvedKey != keyType || resolvedValue != valueType)
                    ? new MapType(resolvedKey, resolvedValue, mapType.valueContainsNull())
                    : mapType;
            }
            case UnresolvedType u -> StringType.get();
            default -> type;
        };
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
        if (expr instanceof AliasExpression alias) {
            return alias.expression();
        }
        return expr;
    }

    /**
     * Checks if an expression is an untyped NULL literal.
     *
     * <p>Untyped NULLs should not participate in CASE WHEN type unification per Spark semantics.
     * They are represented as Literal(null, StringType) where StringType is used as a placeholder
     * for truly untyped NULLs from the Spark Connect protocol.
     *
     * @param expr the expression to check
     * @return true if this is an untyped NULL literal, false otherwise
     */
    private static boolean isUntypedNull(Expression expr) {
        // Check if this is a NULL value with StringType (our marker for untyped NULLs)
        // Typed NULLs (e.g., Literal(null, DecimalType(7,2))) should participate in unification
        return expr instanceof Literal lit
            && lit.isNull()
            && lit.dataType() instanceof StringType;
    }
}
