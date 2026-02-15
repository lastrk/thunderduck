package com.thunderduck.types;

import com.thunderduck.expression.*;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Generative/property-based tests for {@link TypeInferenceEngine}.
 *
 * <p>Uses JUnit 5 {@code @ParameterizedTest} + {@code @MethodSource} to generate
 * exhaustive type combinations, covering paths that existing tests miss:
 * <ul>
 *   <li>unifyTypes() with non-numeric pairs</li>
 *   <li>isUntypedNull() completeness (StringType vs UnresolvedType markers)</li>
 *   <li>CASE WHEN with mixed types and untyped NULLs</li>
 *   <li>Binary expression type inference for all operators</li>
 *   <li>Aggregate return types for all functions × argument types</li>
 *   <li>Schema-aware vs schema-less resolution</li>
 *   <li>Function call return types</li>
 * </ul>
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("TypeInferenceEngine Generative Tests")
public class TypeInferenceEngineGenerativeTest extends TestBase {

    // ====================================================================
    // Shared type generators
    // ====================================================================

    static Stream<DataType> allConcreteTypes() {
        return Stream.of(
            ByteType.get(), ShortType.get(), IntegerType.get(), LongType.get(),
            FloatType.get(), DoubleType.get(), new DecimalType(10, 2),
            StringType.get(), BooleanType.get(), DateType.get(), TimestampType.get(),
            BinaryType.get(), new ArrayType(IntegerType.get(), false),
            new MapType(StringType.get(), IntegerType.get(), false)
        );
    }

    static Stream<DataType> numericTypes() {
        return Stream.of(
            ByteType.get(), ShortType.get(), IntegerType.get(), LongType.get(),
            FloatType.get(), DoubleType.get(), new DecimalType(10, 2),
            new DecimalType(38, 6), new DecimalType(5, 0)
        );
    }

    static Stream<DataType> integralTypes() {
        return Stream.of(
            ByteType.get(), ShortType.get(), IntegerType.get(), LongType.get()
        );
    }

    static Stream<Arguments> allDataTypePairs() {
        List<DataType> types = allConcreteTypes().toList();
        return types.stream().flatMap(a ->
            types.stream().map(b -> Arguments.of(a, b)));
    }

    static Stream<Arguments> numericTypePairs() {
        List<DataType> types = numericTypes().toList();
        return types.stream().flatMap(a ->
            types.stream().map(b -> Arguments.of(a, b)));
    }

    // ====================================================================
    // A. UnifyTypes Tests
    // ====================================================================

    @Nested
    @DisplayName("A. unifyTypes — Exhaustive type pair coverage")
    class UnifyTypesTests {

        @ParameterizedTest(name = "unifyTypes({0}, {1}) never returns UnresolvedType")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allDataTypePairs")
        void unifyTypes_neverReturnsUnresolved(DataType a, DataType b) {
            DataType result = TypeInferenceEngine.unifyTypes(a, b);
            assertThat(result)
                .as("unifyTypes(%s, %s) should not return UnresolvedType", a, b)
                .isNotInstanceOf(UnresolvedType.class);
        }

        @ParameterizedTest(name = "unifyTypes({0}, {1}) returns non-null")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allDataTypePairs")
        void unifyTypes_neverReturnsNull(DataType a, DataType b) {
            DataType result = TypeInferenceEngine.unifyTypes(a, b);
            assertThat(result)
                .as("unifyTypes(%s, %s) should not return null", a, b)
                .isNotNull();
        }

        // TODO: unifyTypes is NOT commutative for non-numeric types.
        // unifyTypes(Date, String) → Date, but unifyTypes(String, Date) → String.
        // This is because the fallback returns `a` blindly. Consider whether this
        // should be an error or use a more principled common-supertype resolution.
        @ParameterizedTest(name = "unifyTypes({0}, {1}) is commutative for numerics")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#numericTypePairs")
        void unifyTypes_commutativeForNumerics(DataType a, DataType b) {
            DataType ab = TypeInferenceEngine.unifyTypes(a, b);
            DataType ba = TypeInferenceEngine.unifyTypes(b, a);
            assertThat(ab)
                .as("unifyTypes(%s, %s) should equal unifyTypes(%s, %s)", a, b, b, a)
                .isEqualTo(ba);
        }

        @ParameterizedTest(name = "unifyTypes(null, {0}) == {0}")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allConcreteTypes")
        void unifyTypes_nullLeftReturnsRight(DataType type) {
            DataType result = TypeInferenceEngine.unifyTypes(null, type);
            assertThat(result).isSameAs(type);
        }

        @ParameterizedTest(name = "unifyTypes({0}, null) == {0}")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allConcreteTypes")
        void unifyTypes_nullRightReturnsLeft(DataType type) {
            DataType result = TypeInferenceEngine.unifyTypes(type, null);
            assertThat(result).isSameAs(type);
        }

        @Test
        @DisplayName("unifyTypes(null, null) == null")
        void unifyTypes_bothNullReturnsNull() {
            DataType result = TypeInferenceEngine.unifyTypes(null, null);
            assertThat(result).isNull();
        }

        @ParameterizedTest(name = "unifyTypes(UnresolvedType, {0}) == {0}")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allConcreteTypes")
        void unifyTypes_unresolvedLeftReturnsRight(DataType type) {
            DataType result = TypeInferenceEngine.unifyTypes(new UnresolvedType(), type);
            assertThat(result)
                .as("unifyTypes(UnresolvedType, %s) should return %s", type, type)
                .isSameAs(type);
        }

        @ParameterizedTest(name = "unifyTypes({0}, UnresolvedType) == {0}")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#allConcreteTypes")
        void unifyTypes_unresolvedRightReturnsLeft(DataType type) {
            DataType result = TypeInferenceEngine.unifyTypes(type, new UnresolvedType());
            assertThat(result)
                .as("unifyTypes(%s, UnresolvedType) should return %s", type, type)
                .isSameAs(type);
        }

        @ParameterizedTest(name = "promoteNumericTypes({0}, {1}) is commutative")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#numericTypePairs")
        void promoteNumericTypes_isCommutative(DataType a, DataType b) {
            DataType ab = TypeInferenceEngine.promoteNumericTypes(a, b);
            DataType ba = TypeInferenceEngine.promoteNumericTypes(b, a);
            assertThat(ab)
                .as("promoteNumericTypes(%s, %s) should equal promoteNumericTypes(%s, %s)", a, b, b, a)
                .isEqualTo(ba);
        }

        @ParameterizedTest(name = "promoteNumericTypes(Double, {0}) == Double")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#numericTypes")
        void promoteNumericTypes_doubleAlwaysWins(DataType other) {
            DataType result = TypeInferenceEngine.promoteNumericTypes(DoubleType.get(), other);
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // B. IsUntypedNull Tests (via resolveCaseWhenType)
    // ====================================================================

    @Nested
    @DisplayName("B. isUntypedNull — Null literal recognition")
    class IsUntypedNullTests {

        @Test
        @DisplayName("NULL with StringType is untyped — CASE WHEN ignores it")
        void nullWithStringType_isUntyped() {
            Expression nullExpr = Literal.nullValue(StringType.get());
            Expression typed = Literal.of(42);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(typed),
                nullExpr
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("NULL with UnresolvedType is untyped — CASE WHEN ignores it")
        void nullWithUnresolvedType_isUntyped() {
            Expression nullExpr = Literal.nullValue(UnresolvedType.expressionString());
            Expression typed = Literal.of(42);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(typed),
                nullExpr
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("NULL with DecimalType is typed — CASE WHEN includes it in unification")
        void nullWithDecimalType_isTyped() {
            Expression typedNull = Literal.nullValue(new DecimalType(10, 2));
            Expression otherVal = Literal.of(42);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(otherVal),
                typedNull
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // Integer(10,0) unified with Decimal(10,2) → Decimal should win via promotion
            assertThat(result).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("NULL with IntegerType is typed — CASE WHEN includes it")
        void nullWithIntegerType_isTyped() {
            Expression typedNull = Literal.nullValue(IntegerType.get());
            Expression doubleVal = Literal.of(3.14);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(doubleVal),
                typedNull
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // Double and Integer → Double via promotion
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("NULL with BooleanType is typed — not treated as untyped")
        void nullWithBooleanType_isTyped() {
            Expression typedNull = Literal.nullValue(BooleanType.get());
            Expression boolVal = Literal.of(true);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(boolVal),
                typedNull
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            assertThat(result).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Non-null literal is never untyped regardless of type")
        void nonNullLiteral_neverUntyped() {
            // Even with StringType, a non-null value is not untyped
            Expression strVal = Literal.of("hello");
            Expression intVal = Literal.of(42);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(intVal),
                strVal
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // Both are non-null: Integer unifies with String → Integer (first non-null type wins)
            assertThat(result).isNotNull();
        }

        @Test
        @DisplayName("CastExpression wrapping NULL is typed — CASE WHEN includes it")
        void castOfNull_isTyped() {
            // CAST(NULL AS DOUBLE) should be typed
            Expression castNull = new CastExpression(
                Literal.nullValue(StringType.get()), DoubleType.get());
            Expression intVal = Literal.of(42);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(intVal),
                castNull
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // CastExpression is not a Literal, so isUntypedNull returns false
            // resolveType on CastExpression returns targetType (DoubleType)
            // unifyTypes(IntegerType, DoubleType) → DoubleType
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // C. CASE WHEN Type Tests
    // ====================================================================

    @Nested
    @DisplayName("C. CASE WHEN type inference")
    class CaseWhenTypeTests {

        static Stream<Arguments> concreteTypeWithUntypedNull() {
            return Stream.of(
                ByteType.get(), ShortType.get(), IntegerType.get(), LongType.get(),
                FloatType.get(), DoubleType.get(), new DecimalType(10, 2),
                BooleanType.get(), DateType.get(), TimestampType.get(),
                StringType.get()
            ).flatMap(type -> Stream.of(
                // Null marker as StringType
                Arguments.of(type, StringType.get()),
                // Null marker as UnresolvedType
                Arguments.of(type, UnresolvedType.expressionString())
            ));
        }

        @ParameterizedTest(name = "CASE WHEN true THEN {0}-typed-value ELSE null({1}) → {0}")
        @MethodSource("concreteTypeWithUntypedNull")
        void caseWhen_untypedNullDoesNotOverrideThenType(DataType concreteType, DataType nullMarker) {
            // Create a typed literal for the THEN branch
            Expression thenExpr = createLiteralOfType(concreteType);
            Expression elseNull = Literal.nullValue(nullMarker);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(thenExpr),
                elseNull
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);

            // For StringType concreteType with StringType null marker:
            // Both branches have StringType. The THEN branch has a non-null literal,
            // so it passes the isUntypedNull check and contributes StringType.
            // The ELSE is untyped null and is skipped. Result: StringType.
            if (concreteType instanceof StringType) {
                assertThat(result).isInstanceOf(StringType.class);
            } else {
                assertThat(result)
                    .as("CASE with typed THEN(%s) and untyped NULL(%s) ELSE should return THEN type",
                        concreteType, nullMarker)
                    .isEqualTo(concreteType);
            }
        }

        @ParameterizedTest(name = "CASE WHEN true THEN null({1}) ELSE {0}-typed-value → {0}")
        @MethodSource("concreteTypeWithUntypedNull")
        void caseWhen_untypedNullDoesNotOverrideElseType(DataType concreteType, DataType nullMarker) {
            Expression thenNull = Literal.nullValue(nullMarker);
            Expression elseExpr = createLiteralOfType(concreteType);

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(thenNull),
                elseExpr
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);

            if (concreteType instanceof StringType) {
                assertThat(result).isInstanceOf(StringType.class);
            } else {
                assertThat(result)
                    .as("CASE with untyped NULL(%s) THEN and typed ELSE(%s) should return ELSE type",
                        nullMarker, concreteType)
                    .isEqualTo(concreteType);
            }
        }

        @Test
        @DisplayName("CASE with all untyped NULL branches returns StringType (fallback)")
        void caseWhen_allUntypedNulls_returnsFallback() {
            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(Literal.nullValue(StringType.get())),
                Literal.nullValue(UnresolvedType.expressionString())
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // All branches are untyped nulls → resultType stays null → fallback to StringType
            assertThat(result).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("CASE with multiple typed THEN branches unifies correctly")
        void caseWhen_multipleTypedBranches_unifies() {
            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true), Literal.of(false)),
                List.of(Literal.of(42), Literal.of(3.14)),  // Integer, Double
                Literal.nullValue(StringType.get())
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            // Integer + Double → Double
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("CASE with no ELSE and typed THEN returns THEN type")
        void caseWhen_noElse_returnsThenType() {
            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(Literal.of(42L)),
                null  // no ELSE
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, null);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("CASE with schema resolves column types in branches")
        void caseWhen_withSchema_resolvesColumnTypes() {
            StructType schema = new StructType(List.of(
                new StructField("amount", new DecimalType(7, 2), false)
            ));

            CaseWhenExpression caseWhen = new CaseWhenExpression(
                List.of(Literal.of(true)),
                List.of(new UnresolvedColumn("amount")),
                Literal.nullValue(StringType.get())
            );

            DataType result = TypeInferenceEngine.resolveCaseWhenType(caseWhen, schema);
            assertThat(result).isEqualTo(new DecimalType(7, 2));
        }
    }

    // ====================================================================
    // D. Binary Expression Type Tests
    // ====================================================================

    @Nested
    @DisplayName("D. Binary expression type inference")
    class BinaryExpressionTypeTests {

        static Stream<BinaryExpression.Operator> comparisonOperators() {
            return Stream.of(
                BinaryExpression.Operator.EQUAL,
                BinaryExpression.Operator.NOT_EQUAL,
                BinaryExpression.Operator.LESS_THAN,
                BinaryExpression.Operator.LESS_THAN_OR_EQUAL,
                BinaryExpression.Operator.GREATER_THAN,
                BinaryExpression.Operator.GREATER_THAN_OR_EQUAL
            );
        }

        static Stream<BinaryExpression.Operator> logicalOperators() {
            return Stream.of(
                BinaryExpression.Operator.AND,
                BinaryExpression.Operator.OR
            );
        }

        static Stream<BinaryExpression.Operator> arithmeticOperators() {
            return Stream.of(
                BinaryExpression.Operator.ADD,
                BinaryExpression.Operator.SUBTRACT,
                BinaryExpression.Operator.MULTIPLY,
                BinaryExpression.Operator.MODULO
            );
        }

        @ParameterizedTest(name = "Comparison {0} always returns Boolean")
        @MethodSource("comparisonOperators")
        void comparison_alwaysReturnsBoolean(BinaryExpression.Operator op) {
            BinaryExpression expr = new BinaryExpression(Literal.of(1), op, Literal.of(2));
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);
            assertThat(result).isInstanceOf(BooleanType.class);
        }

        @ParameterizedTest(name = "Logical {0} always returns Boolean")
        @MethodSource("logicalOperators")
        void logical_alwaysReturnsBoolean(BinaryExpression.Operator op) {
            BinaryExpression expr = new BinaryExpression(Literal.of(true), op, Literal.of(false));
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);
            assertThat(result).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Division always returns Double for non-decimal operands")
        void division_nonDecimal_returnsDouble() {
            BinaryExpression expr = BinaryExpression.divide(Literal.of(10), Literal.of(3));
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Division of Decimal returns Decimal")
        void division_decimal_returnsDecimal() {
            Expression left = new Literal("100.00", new DecimalType(10, 2));
            Expression right = new Literal("3.00", new DecimalType(10, 2));
            BinaryExpression expr = BinaryExpression.divide(left, right);
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);
            assertThat(result).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("Concat always returns String")
        void concat_returnsString() {
            BinaryExpression expr = new BinaryExpression(
                Literal.of("hello"), BinaryExpression.Operator.CONCAT, Literal.of(" world"));
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);
            assertThat(result).isInstanceOf(StringType.class);
        }

        static Stream<Arguments> arithmeticWithNumericPairs() {
            List<BinaryExpression.Operator> ops = List.of(
                BinaryExpression.Operator.ADD,
                BinaryExpression.Operator.SUBTRACT,
                BinaryExpression.Operator.MULTIPLY
            );
            List<DataType> types = List.of(
                IntegerType.get(), LongType.get(), FloatType.get(), DoubleType.get()
            );
            return ops.stream().flatMap(op ->
                types.stream().flatMap(a ->
                    types.stream().map(b -> Arguments.of(op, a, b))
                )
            );
        }

        @ParameterizedTest(name = "{1} {0} {2} returns numeric type")
        @MethodSource("arithmeticWithNumericPairs")
        void arithmetic_numericOperands_returnsNumeric(
                BinaryExpression.Operator op, DataType leftType, DataType rightType) {
            Expression left = createLiteralOfType(leftType);
            Expression right = createLiteralOfType(rightType);
            BinaryExpression expr = new BinaryExpression(left, op, right);
            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, null);

            assertThat(result)
                .as("%s %s %s should return a numeric type", leftType, op, rightType)
                .satisfiesAnyOf(
                    t -> assertThat(t).isInstanceOf(IntegerType.class),
                    t -> assertThat(t).isInstanceOf(LongType.class),
                    t -> assertThat(t).isInstanceOf(FloatType.class),
                    t -> assertThat(t).isInstanceOf(DoubleType.class),
                    t -> assertThat(t).isInstanceOf(DecimalType.class),
                    t -> assertThat(t).isInstanceOf(ShortType.class),
                    t -> assertThat(t).isInstanceOf(ByteType.class)
                );
        }

        @Test
        @DisplayName("Arithmetic with schema-resolved columns")
        void arithmetic_withSchema_resolvesColumnTypes() {
            StructType schema = new StructType(List.of(
                new StructField("price", new DecimalType(10, 2), false),
                new StructField("quantity", IntegerType.get(), false)
            ));

            BinaryExpression expr = BinaryExpression.multiply(
                new UnresolvedColumn("price"),
                new UnresolvedColumn("quantity")
            );

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(expr, schema);
            // Decimal(10,2) * Integer → Decimal via promotion
            assertThat(result).isInstanceOf(DecimalType.class);
        }
    }

    // ====================================================================
    // E. Aggregate Return Type Tests
    // ====================================================================

    @Nested
    @DisplayName("E. Aggregate return type inference")
    class AggregateReturnTypeTests {

        @Test
        @DisplayName("COUNT always returns LongType regardless of argument type")
        void count_alwaysReturnsLong() {
            allConcreteTypes().forEach(type ->
                assertThat(TypeInferenceEngine.resolveAggregateReturnType("COUNT", type))
                    .as("COUNT(%s)", type)
                    .isInstanceOf(LongType.class)
            );
        }

        @Test
        @DisplayName("COUNT with null argument returns LongType")
        void count_nullArg_returnsLong() {
            assertThat(TypeInferenceEngine.resolveAggregateReturnType("COUNT", null))
                .isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("COUNT_DISTINCT always returns LongType")
        void countDistinct_alwaysReturnsLong() {
            allConcreteTypes().forEach(type ->
                assertThat(TypeInferenceEngine.resolveAggregateReturnType("COUNT_DISTINCT", type))
                    .as("COUNT_DISTINCT(%s)", type)
                    .isInstanceOf(LongType.class)
            );
        }

        @ParameterizedTest(name = "SUM({0}) returns LongType for integral types")
        @MethodSource("com.thunderduck.types.TypeInferenceEngineGenerativeTest#integralTypes")
        void sum_integralTypes_returnsLong(DataType type) {
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("SUM", type);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("SUM(FloatType) returns DoubleType")
        void sum_float_returnsDouble() {
            assertThat(TypeInferenceEngine.resolveAggregateReturnType("SUM", FloatType.get()))
                .isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("SUM(DoubleType) returns DoubleType")
        void sum_double_returnsDouble() {
            assertThat(TypeInferenceEngine.resolveAggregateReturnType("SUM", DoubleType.get()))
                .isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("SUM(DecimalType) returns Decimal with +10 precision")
        void sum_decimal_returnsWiderDecimal() {
            DecimalType input = new DecimalType(10, 2);
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("SUM", input);
            assertThat(result).isInstanceOf(DecimalType.class);
            DecimalType decResult = (DecimalType) result;
            assertThat(decResult.precision()).isEqualTo(20);  // 10 + 10
            assertThat(decResult.scale()).isEqualTo(2);
        }

        @Test
        @DisplayName("SUM(Decimal(38,6)) caps precision at 38")
        void sum_decimal_capsAt38() {
            DecimalType input = new DecimalType(38, 6);
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("SUM", input);
            assertThat(result).isInstanceOf(DecimalType.class);
            DecimalType decResult = (DecimalType) result;
            assertThat(decResult.precision()).isEqualTo(38);  // capped
            assertThat(decResult.scale()).isEqualTo(6);
        }

        @Test
        @DisplayName("AVG returns DoubleType for non-decimal numerics")
        void avg_nonDecimal_returnsDouble() {
            Stream.of(ByteType.get(), ShortType.get(), IntegerType.get(),
                      LongType.get(), FloatType.get(), DoubleType.get())
                .forEach(type ->
                    assertThat(TypeInferenceEngine.resolveAggregateReturnType("AVG", type))
                        .as("AVG(%s)", type)
                        .isInstanceOf(DoubleType.class)
                );
        }

        @Test
        @DisplayName("AVG(DecimalType) returns Decimal with adjusted precision/scale")
        void avg_decimal_returnsDecimal() {
            DecimalType input = new DecimalType(10, 2);
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("AVG", input);
            assertThat(result).isInstanceOf(DecimalType.class);
            DecimalType decResult = (DecimalType) result;
            assertThat(decResult.precision()).isEqualTo(14);  // 10 + 4
            assertThat(decResult.scale()).isEqualTo(6);       // min(min(2+4, 18), 14) = 6
        }

        static Stream<String> statisticalFunctions() {
            return Stream.of(
                "STDDEV", "STDDEV_SAMP", "STDDEV_POP",
                "VARIANCE", "VAR_SAMP", "VAR_POP",
                "CORR", "COVAR_SAMP", "COVAR_POP"
            );
        }

        @ParameterizedTest(name = "{0} always returns DoubleType")
        @MethodSource("statisticalFunctions")
        void statisticalFunctions_alwaysReturnDouble(String func) {
            allConcreteTypes().forEach(type ->
                assertThat(TypeInferenceEngine.resolveAggregateReturnType(func, type))
                    .as("%s(%s)", func, type)
                    .isInstanceOf(DoubleType.class)
            );
        }

        static Stream<String> minMaxFirstLast() {
            return Stream.of("MIN", "MAX", "FIRST", "LAST");
        }

        @ParameterizedTest(name = "{0} preserves argument type")
        @MethodSource("minMaxFirstLast")
        void minMaxFirstLast_preservesArgType(String func) {
            allConcreteTypes().forEach(type ->
                assertThat(TypeInferenceEngine.resolveAggregateReturnType(func, type))
                    .as("%s(%s) should preserve type", func, type)
                    .isEqualTo(type)
            );
        }

        @ParameterizedTest(name = "{0} with null arg returns StringType (fallback)")
        @MethodSource("minMaxFirstLast")
        void minMaxFirstLast_nullArg_returnsFallback(String func) {
            // TODO: Returning StringType for null argType is the same fallback pattern that
            // caused the Q39 bug. Consider returning UnresolvedType or throwing instead,
            // so callers are forced to resolve the type rather than silently propagating StringType.
            DataType result = TypeInferenceEngine.resolveAggregateReturnType(func, null);
            assertThat(result).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("No aggregate returns StringType for numeric inputs")
        void noAggregate_returnsStringForNumericInputs() {
            List<String> aggregates = List.of(
                "COUNT", "SUM", "AVG", "MIN", "MAX",
                "STDDEV", "STDDEV_POP", "VARIANCE", "VAR_POP"
            );
            List<DataType> numericInputs = numericTypes().toList();

            for (String agg : aggregates) {
                for (DataType input : numericInputs) {
                    DataType result = TypeInferenceEngine.resolveAggregateReturnType(agg, input);
                    assertThat(result)
                        .as("%s(%s) should not return StringType", agg, input)
                        .isNotInstanceOf(StringType.class);
                }
            }
        }

        @Test
        @DisplayName("GROUPING returns ByteType")
        void grouping_returnsByte() {
            assertThat(TypeInferenceEngine.resolveAggregateReturnType("GROUPING", IntegerType.get()))
                .isInstanceOf(ByteType.class);
        }

        @Test
        @DisplayName("GROUPING_ID returns LongType")
        void groupingId_returnsLong() {
            assertThat(TypeInferenceEngine.resolveAggregateReturnType("GROUPING_ID", IntegerType.get()))
                .isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("COLLECT_LIST returns ArrayType of argument type")
        void collectList_returnsArrayOfArgType() {
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("COLLECT_LIST", IntegerType.get());
            assertThat(result).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) result).elementType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("COLLECT_SET returns ArrayType of argument type")
        void collectSet_returnsArrayOfArgType() {
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("COLLECT_SET", StringType.get());
            assertThat(result).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) result).elementType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("SUM_DISTINCT normalizes to SUM")
        void sumDistinct_normalizesToSum() {
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("SUM_DISTINCT", IntegerType.get());
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("AVG_DISTINCT normalizes to AVG")
        void avgDistinct_normalizesToAvg() {
            DataType result = TypeInferenceEngine.resolveAggregateReturnType("AVG_DISTINCT", IntegerType.get());
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // F. Schema Resolution Tests
    // ====================================================================

    @Nested
    @DisplayName("F. Schema-aware vs schema-less resolution")
    class SchemaResolutionTests {

        private final StructType schema = new StructType(List.of(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("price", new DecimalType(10, 2), false),
            new StructField("is_active", BooleanType.get(), false),
            new StructField("created_at", TimestampType.get(), true),
            new StructField("score", DoubleType.get(), true)
        ));

        @Test
        @DisplayName("Schema-aware resolution returns correct types for known columns")
        void schemaAware_returnsCorrectTypes() {
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("id"), schema))
                .isInstanceOf(IntegerType.class);
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("name"), schema))
                .isInstanceOf(StringType.class);
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("price"), schema))
                .isEqualTo(new DecimalType(10, 2));
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("is_active"), schema))
                .isInstanceOf(BooleanType.class);
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("created_at"), schema))
                .isInstanceOf(TimestampType.class);
        }

        @Test
        @DisplayName("Schema-aware resolution is case-insensitive")
        void schemaAware_caseInsensitive() {
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("ID"), schema))
                .isInstanceOf(IntegerType.class);
            assertThat(TypeInferenceEngine.resolveType(new UnresolvedColumn("Price"), schema))
                .isEqualTo(new DecimalType(10, 2));
        }

        @Test
        @DisplayName("Schema-less resolution falls back to expression's declared type")
        void schemaLess_fallsBackToDeclaredType() {
            UnresolvedColumn col = new UnresolvedColumn("id");
            DataType result = TypeInferenceEngine.resolveType(col, null);
            // Without schema, UnresolvedColumn returns its declared dataType()
            assertThat(result).isEqualTo(col.dataType());
        }

        @Test
        @DisplayName("Schema-aware nullability returns correct values")
        void schemaAware_correctNullability() {
            assertThat(TypeInferenceEngine.lookupColumnNullable("id", schema)).isFalse();
            assertThat(TypeInferenceEngine.lookupColumnNullable("name", schema)).isTrue();
            assertThat(TypeInferenceEngine.lookupColumnNullable("score", schema)).isTrue();
            assertThat(TypeInferenceEngine.lookupColumnNullable("price", schema)).isFalse();
        }

        @Test
        @DisplayName("Schema-less nullability defaults to true")
        void schemaLess_defaultsToNullable() {
            assertThat(TypeInferenceEngine.lookupColumnNullable("anything", null)).isTrue();
        }

        @Test
        @DisplayName("Unknown column with schema returns true for nullable")
        void unknownColumn_returnsNullable() {
            assertThat(TypeInferenceEngine.lookupColumnNullable("nonexistent", schema)).isTrue();
        }

        @Test
        @DisplayName("AliasExpression delegates to wrapped expression for type")
        void aliasExpression_delegatesToWrapped() {
            Expression aliased = new AliasExpression(new UnresolvedColumn("price"), "unit_price");
            DataType result = TypeInferenceEngine.resolveType(aliased, schema);
            assertThat(result).isEqualTo(new DecimalType(10, 2));
        }

        @Test
        @DisplayName("resolveType on null expression returns StringType")
        void nullExpression_returnsStringType() {
            DataType result = TypeInferenceEngine.resolveType(null, schema);
            assertThat(result).isInstanceOf(StringType.class);
        }
    }

    // ====================================================================
    // G. Function Call Type Tests
    // ====================================================================

    @Nested
    @DisplayName("G. Function call return types")
    class FunctionCallTypeTests {

        static Stream<String> stringFunctions() {
            return Stream.of(
                "concat", "upper", "lower", "trim", "ltrim", "rtrim",
                "substring", "replace", "regexp_replace", "lpad", "rpad",
                "initcap", "repeat"
            );
        }

        @ParameterizedTest(name = "{0} returns StringType")
        @MethodSource("stringFunctions")
        void stringFunctions_returnString(String funcName) {
            FunctionCall func = FunctionCall.of(funcName, Literal.of("hello"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result)
                .as("%s() should return StringType", funcName)
                .isInstanceOf(StringType.class);
        }

        static Stream<String> dateFunctions() {
            return Stream.of("to_date", "last_day", "date_add", "date_sub", "add_months");
        }

        @ParameterizedTest(name = "{0} returns DateType")
        @MethodSource("dateFunctions")
        void dateFunctions_returnDate(String funcName) {
            FunctionCall func = FunctionCall.of(funcName, Literal.of("2024-01-01"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result)
                .as("%s() should return DateType", funcName)
                .isInstanceOf(DateType.class);
        }

        @Test
        @DisplayName("to_timestamp returns TimestampType")
        void toTimestamp_returnsTimestamp() {
            FunctionCall func = FunctionCall.of("to_timestamp", Literal.of("2024-01-01"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(TimestampType.class);
        }

        static Stream<String> datePartFunctions() {
            return Stream.of("year", "month", "day", "dayofmonth", "dayofweek",
                           "hour", "minute", "second", "quarter");
        }

        @ParameterizedTest(name = "{0} returns IntegerType")
        @MethodSource("datePartFunctions")
        void datePartFunctions_returnInteger(String funcName) {
            FunctionCall func = FunctionCall.of(funcName, Literal.of("2024-01-01"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result)
                .as("%s() should return IntegerType", funcName)
                .isInstanceOf(IntegerType.class);
        }

        static Stream<String> doubleMathFunctions() {
            return Stream.of("sqrt", "log", "ln", "log10", "exp",
                           "sin", "cos", "tan", "asin", "acos", "atan",
                           "radians", "degrees", "pow", "power");
        }

        @ParameterizedTest(name = "{0} returns DoubleType")
        @MethodSource("doubleMathFunctions")
        void doubleMathFunctions_returnDouble(String funcName) {
            FunctionCall func = FunctionCall.of(funcName, Literal.of(1.0), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result)
                .as("%s() should return DoubleType", funcName)
                .isInstanceOf(DoubleType.class);
        }

        static Stream<String> ceilFloorFunctions() {
            return Stream.of("ceil", "ceiling", "floor");
        }

        @ParameterizedTest(name = "{0} returns LongType")
        @MethodSource("ceilFloorFunctions")
        void ceilFloor_returnsLong(String funcName) {
            FunctionCall func = FunctionCall.of(funcName, Literal.of(1.5), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result)
                .as("%s() should return LongType", funcName)
                .isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("length returns IntegerType")
        void length_returnsInteger() {
            FunctionCall func = FunctionCall.of("length", Literal.of("hello"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("unix_timestamp returns LongType")
        void unixTimestamp_returnsLong() {
            FunctionCall func = FunctionCall.of("unix_timestamp", Literal.of("2024-01-01"), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("isnull returns BooleanType")
        void isnull_returnsBoolean() {
            FunctionCall func = FunctionCall.of("isnull", Literal.of(1), StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("coalesce promotes types across all arguments")
        void coalesce_promotesTypes() {
            FunctionCall func = FunctionCall.of("coalesce", StringType.get(),
                Literal.of(1), Literal.of(2.0));
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            // Integer and Double → Double
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("round with Decimal preserves Decimal type")
        void round_decimal_preservesDecimal() {
            Expression decExpr = new Literal("123.456", new DecimalType(10, 3));
            FunctionCall func = FunctionCall.of("round", StringType.get(), decExpr, Literal.of(2));
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("round with non-Decimal returns Double")
        void round_nonDecimal_returnsDouble() {
            FunctionCall func = FunctionCall.of("round", StringType.get(),
                Literal.of(3.14), Literal.of(1));
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("count with no args returns LongType")
        void count_noArgs_returnsLong() {
            FunctionCall func = FunctionCall.of("count", StringType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("split returns ArrayType(StringType)")
        void split_returnsArrayOfString() {
            FunctionCall func = FunctionCall.of("split", StringType.get(),
                Literal.of("a,b,c"), Literal.of(","));
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(ArrayType.class);
            assertThat(((ArrayType) result).elementType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Function with concrete declared type returns it directly")
        void concreteDeclaredType_returnedDirectly() {
            // When function already has a concrete type, it's returned directly
            FunctionCall func = FunctionCall.of("custom_func", Literal.of(1), DoubleType.get());
            DataType result = TypeInferenceEngine.resolveFunctionCallType(func, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // H. Aggregate Nullability Tests
    // ====================================================================

    @Nested
    @DisplayName("H. Aggregate nullability")
    class AggregateNullabilityTests {

        @Test
        @DisplayName("COUNT is never nullable")
        void count_neverNullable() {
            assertThat(TypeInferenceEngine.resolveAggregateNullable("COUNT", null, null)).isFalse();
            assertThat(TypeInferenceEngine.resolveAggregateNullable("COUNT", Literal.of(1), null)).isFalse();
            assertThat(TypeInferenceEngine.resolveAggregateNullable("COUNT_DISTINCT", Literal.of(1), null)).isFalse();
        }

        @Test
        @DisplayName("GROUPING/GROUPING_ID are never nullable")
        void grouping_neverNullable() {
            assertThat(TypeInferenceEngine.resolveAggregateNullable("GROUPING", Literal.of(1), null)).isFalse();
            assertThat(TypeInferenceEngine.resolveAggregateNullable("GROUPING_ID", Literal.of(1), null)).isFalse();
        }

        static Stream<String> nullableAggregates() {
            return Stream.of(
                "SUM", "AVG", "MIN", "MAX", "FIRST", "LAST",
                "STDDEV", "STDDEV_POP", "VARIANCE", "VAR_POP",
                "CORR", "COVAR_SAMP", "COVAR_POP",
                "COLLECT_LIST", "COLLECT_SET"
            );
        }

        @ParameterizedTest(name = "{0} is always nullable")
        @MethodSource("nullableAggregates")
        void nonCountAggregates_areNullable(String func) {
            assertThat(TypeInferenceEngine.resolveAggregateNullable(func, Literal.of(1), null))
                .as("%s should be nullable", func)
                .isTrue();
        }
    }

    // ====================================================================
    // I. Window Function Type Tests
    // ====================================================================

    @Nested
    @DisplayName("I. Window function type inference")
    class WindowFunctionTypeTests {

        static Stream<String> rankingFunctions() {
            return Stream.of("ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE");
        }

        @ParameterizedTest(name = "{0} returns IntegerType")
        @MethodSource("rankingFunctions")
        void rankingFunctions_returnInteger(String funcName) {
            WindowFunction wf = new WindowFunction(funcName, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        static Stream<String> distributionFunctions() {
            return Stream.of("PERCENT_RANK", "CUME_DIST");
        }

        @ParameterizedTest(name = "{0} returns DoubleType")
        @MethodSource("distributionFunctions")
        void distributionFunctions_returnDouble(String funcName) {
            WindowFunction wf = new WindowFunction(funcName, Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Window COUNT returns LongType")
        void windowCount_returnsLong() {
            WindowFunction wf = new WindowFunction("COUNT", List.of(Literal.of(1)),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("Window SUM on integral type returns LongType")
        void windowSum_integral_returnsLong() {
            WindowFunction wf = new WindowFunction("SUM", List.of(Literal.of(1)),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("Window SUM on Double returns DoubleType")
        void windowSum_double_returnsDouble() {
            WindowFunction wf = new WindowFunction("SUM", List.of(Literal.of(1.0)),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Window AVG returns DoubleType for non-decimal")
        void windowAvg_nonDecimal_returnsDouble() {
            WindowFunction wf = new WindowFunction("AVG", List.of(Literal.of(1)),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Window AVG on Decimal returns Decimal")
        void windowAvg_decimal_returnsDecimal() {
            Expression decExpr = new Literal("1.00", new DecimalType(10, 2));
            WindowFunction wf = new WindowFunction("AVG", List.of(decExpr),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(DecimalType.class);
        }

        static Stream<String> windowStatisticalFunctions() {
            return Stream.of("STDDEV", "STDDEV_POP", "STDDEV_SAMP",
                           "VARIANCE", "VAR_POP", "VAR_SAMP");
        }

        @ParameterizedTest(name = "Window {0} returns DoubleType")
        @MethodSource("windowStatisticalFunctions")
        void windowStatFunctions_returnDouble(String funcName) {
            WindowFunction wf = new WindowFunction(funcName, List.of(Literal.of(1)),
                Collections.emptyList(), Collections.emptyList());
            DataType result = TypeInferenceEngine.resolveWindowFunctionType(wf, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // J. Decimal Arithmetic Precision Tests
    // ====================================================================

    @Nested
    @DisplayName("J. Decimal arithmetic precision")
    class DecimalArithmeticPrecisionTests {

        static Stream<Arguments> decimalPairsForAddition() {
            return Stream.of(
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 2), 11, 2),
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 4), 13, 4),
                Arguments.of(new DecimalType(5, 0), new DecimalType(5, 0), 6, 0),
                // rawPrec=39, rawScale=18: intDigits=21, adjustedScale=max(38-21,min(18,6))=max(17,6)=17
                Arguments.of(new DecimalType(38, 18), new DecimalType(38, 18), 38, 17)
            );
        }

        @ParameterizedTest(name = "{0} + {1} → Decimal({2},{3})")
        @MethodSource("decimalPairsForAddition")
        void decimalAddition_correctPrecision(
                DecimalType left, DecimalType right, int expectedPrec, int expectedScale) {
            DecimalType result = TypeInferenceEngine.promoteDecimalAddition(left, right);
            assertThat(result.precision()).isEqualTo(expectedPrec);
            assertThat(result.scale()).isEqualTo(expectedScale);
        }

        static Stream<Arguments> decimalPairsForMultiplication() {
            return Stream.of(
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 2), 21, 4),
                Arguments.of(new DecimalType(5, 1), new DecimalType(5, 1), 11, 2),
                Arguments.of(new DecimalType(20, 0), new DecimalType(20, 0), 38, 0)
            );
        }

        @ParameterizedTest(name = "{0} * {1} → Decimal({2},{3})")
        @MethodSource("decimalPairsForMultiplication")
        void decimalMultiplication_correctPrecision(
                DecimalType left, DecimalType right, int expectedPrec, int expectedScale) {
            DecimalType result = TypeInferenceEngine.promoteDecimalMultiplication(left, right);
            assertThat(result.precision()).isEqualTo(expectedPrec);
            assertThat(result.scale()).isEqualTo(expectedScale);
        }

        static Stream<Arguments> decimalPairsForModulo() {
            return Stream.of(
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 2), 10, 2),
                Arguments.of(new DecimalType(10, 2), new DecimalType(5, 1), 6, 2),
                Arguments.of(new DecimalType(5, 0), new DecimalType(3, 0), 3, 0)
            );
        }

        @ParameterizedTest(name = "{0} %% {1} → Decimal({2},{3})")
        @MethodSource("decimalPairsForModulo")
        void decimalModulo_correctPrecision(
                DecimalType left, DecimalType right, int expectedPrec, int expectedScale) {
            DecimalType result = TypeInferenceEngine.promoteDecimalModulo(left, right);
            assertThat(result.precision()).isEqualTo(expectedPrec);
            assertThat(result.scale()).isEqualTo(expectedScale);
        }

        @Test
        @DisplayName("All decimal ops keep precision <= 38")
        void allDecimalOps_precisionCappedAt38() {
            DecimalType big = new DecimalType(38, 10);
            assertThat(TypeInferenceEngine.promoteDecimalDivision(big, big).precision()).isLessThanOrEqualTo(38);
            assertThat(TypeInferenceEngine.promoteDecimalMultiplication(big, big).precision()).isLessThanOrEqualTo(38);
            assertThat(TypeInferenceEngine.promoteDecimalAddition(big, big).precision()).isLessThanOrEqualTo(38);
            assertThat(TypeInferenceEngine.promoteDecimalModulo(big, big).precision()).isLessThanOrEqualTo(38);
        }
    }

    // ====================================================================
    // K. resolveType dispatch tests
    // ====================================================================

    @Nested
    @DisplayName("K. resolveType dispatch")
    class ResolveTypeDispatchTests {

        @Test
        @DisplayName("CastExpression returns target type")
        void castExpression_returnsTargetType() {
            CastExpression cast = new CastExpression(Literal.of("123"), IntegerType.get());
            DataType result = TypeInferenceEngine.resolveType(cast, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("InExpression returns BooleanType")
        void inExpression_returnsBoolean() {
            InExpression in = new InExpression(Literal.of(1),
                List.of(Literal.of(1), Literal.of(2), Literal.of(3)));
            DataType result = TypeInferenceEngine.resolveType(in, null);
            assertThat(result).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Literal types are preserved")
        void literal_typesPreserved() {
            assertThat(TypeInferenceEngine.resolveType(Literal.of(42), null))
                .isInstanceOf(IntegerType.class);
            assertThat(TypeInferenceEngine.resolveType(Literal.of(42L), null))
                .isInstanceOf(LongType.class);
            assertThat(TypeInferenceEngine.resolveType(Literal.of(3.14), null))
                .isInstanceOf(DoubleType.class);
            assertThat(TypeInferenceEngine.resolveType(Literal.of(3.14f), null))
                .isInstanceOf(FloatType.class);
            assertThat(TypeInferenceEngine.resolveType(Literal.of("hello"), null))
                .isInstanceOf(StringType.class);
            assertThat(TypeInferenceEngine.resolveType(Literal.of(true), null))
                .isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("AliasExpression delegates to inner expression")
        void aliasExpression_delegates() {
            AliasExpression alias = new AliasExpression(Literal.of(42), "val");
            DataType result = TypeInferenceEngine.resolveType(alias, null);
            assertThat(result).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Nested AliasExpressions resolve correctly")
        void nestedAlias_resolvesCorrectly() {
            AliasExpression inner = new AliasExpression(Literal.of(3.14), "inner");
            AliasExpression outer = new AliasExpression(inner, "outer");
            DataType result = TypeInferenceEngine.resolveType(outer, null);
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ====================================================================
    // Helper methods
    // ====================================================================

    /**
     * Creates a representative Literal for a given DataType to use in tests.
     * Uses the two-arg Literal constructor to ensure the correct type is set,
     * since factory methods like Literal.of(1) always produce IntegerType.
     */
    private static Expression createLiteralOfType(DataType type) {
        return switch (type) {
            case ByteType b -> new Literal(1, ByteType.get());
            case ShortType s -> new Literal(1, ShortType.get());
            case IntegerType i -> Literal.of(42);
            case LongType l -> Literal.of(42L);
            case FloatType f -> Literal.of(3.14f);
            case DoubleType d -> Literal.of(3.14);
            case DecimalType dec -> new Literal("100.00", dec);
            case StringType s -> Literal.of("hello");
            case BooleanType b -> Literal.of(true);
            case DateType d -> new Literal("2024-01-01", DateType.get());
            case TimestampType t -> new Literal("2024-01-01T00:00:00", TimestampType.get());
            case BinaryType bin -> new Literal("binary", BinaryType.get());
            case ArrayType arr -> new Literal("array_val", arr);
            case MapType map -> new Literal("map_val", map);
            default -> Literal.of("unknown");
        };
    }
}
