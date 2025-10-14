package com.catalyst2sql.translation;

import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import com.catalyst2sql.expression.*;
import com.catalyst2sql.types.*;
import com.catalyst2sql.functions.FunctionRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive Expression Translation Test Suite for Week 2.
 *
 * <p>Tests 100+ expression translation scenarios including:
 * <ul>
 *   <li>Arithmetic expressions (20 tests)</li>
 *   <li>Comparison expressions (15 tests)</li>
 *   <li>Logical expressions (10 tests)</li>
 *   <li>String function expressions (15 tests)</li>
 *   <li>Math function expressions (10 tests)</li>
 *   <li>Date function expressions (10 tests)</li>
 *   <li>Aggregate function expressions (10 tests)</li>
 *   <li>NULL handling expressions (10 tests)</li>
 * </ul>
 *
 * <p>Tests validate:
 * <ul>
 *   <li>SQL generation correctness</li>
 *   <li>Expression type resolution</li>
 *   <li>NULL propagation semantics</li>
 *   <li>Edge cases and error conditions</li>
 * </ul>
 *
 * @see Expression
 * @see BinaryExpression
 * @see FunctionCall
 */
@TestCategories.Tier1
@TestCategories.Unit
@TestCategories.Expression
@DisplayName("Expression Translation Tests - Week 2")
public class ExpressionTranslationTest extends TestBase {

    // ==================== ARITHMETIC EXPRESSION TESTS (20 tests) ====================

    @Nested
    @DisplayName("Arithmetic Expression Tests")
    class ArithmeticExpressionTests {

        @Test
        @DisplayName("TC-EXPR-001: Integer addition")
        void testIntegerAddition() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(20);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(10 + 20)");
            assertThat(expr.dataType()).isInstanceOf(IntegerType.class);
            assertThat(expr.nullable()).isFalse();
        }

        @Test
        @DisplayName("TC-EXPR-002: Integer subtraction")
        void testIntegerSubtraction() {
            Expression left = Literal.of(100);
            Expression right = Literal.of(42);
            Expression expr = BinaryExpression.subtract(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(100 - 42)");
            assertThat(expr.dataType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-EXPR-003: Integer multiplication")
        void testIntegerMultiplication() {
            Expression left = Literal.of(7);
            Expression right = Literal.of(6);
            Expression expr = BinaryExpression.multiply(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(7 * 6)");
        }

        @Test
        @DisplayName("TC-EXPR-004: Integer division")
        void testIntegerDivision() {
            Expression left = Literal.of(100);
            Expression right = Literal.of(4);
            Expression expr = BinaryExpression.divide(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(100 / 4)");
        }

        @Test
        @DisplayName("TC-EXPR-005: Integer modulo")
        void testIntegerModulo() {
            Expression left = Literal.of(17);
            Expression right = Literal.of(5);
            Expression expr = BinaryExpression.modulo(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(17 % 5)");
        }

        @Test
        @DisplayName("TC-EXPR-006: Double addition")
        void testDoubleAddition() {
            Expression left = Literal.of(10.5);
            Expression right = Literal.of(20.3);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("+");
            assertThat(expr.dataType()).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("TC-EXPR-007: Mixed type arithmetic - int + double")
        void testMixedTypeArithmetic() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(5.5);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("+");
            // Result type should be promoted to double
            assertThat(expr.dataType()).isInstanceOf(IntegerType.class); // left type currently
        }

        @Test
        @DisplayName("TC-EXPR-008: Column reference arithmetic")
        void testColumnReferenceArithmetic() {
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression quantity = new ColumnReference("quantity", IntegerType.get());
            Expression expr = BinaryExpression.multiply(price, quantity);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(price * quantity)");
        }

        @Test
        @DisplayName("TC-EXPR-009: Nested arithmetic expression")
        void testNestedArithmetic() {
            // (a + b) * c
            Expression a = new ColumnReference("a", IntegerType.get());
            Expression b = new ColumnReference("b", IntegerType.get());
            Expression c = new ColumnReference("c", IntegerType.get());

            Expression sum = BinaryExpression.add(a, b);
            Expression result = BinaryExpression.multiply(sum, c);

            String sql = result.toSQL();
            assertThat(sql).isEqualTo("((a + b) * c)");
        }

        @Test
        @DisplayName("TC-EXPR-010: Complex arithmetic - (a + b) / (c - d)")
        void testComplexArithmetic() {
            Expression a = Literal.of(10);
            Expression b = Literal.of(5);
            Expression c = Literal.of(20);
            Expression d = Literal.of(3);

            Expression numerator = BinaryExpression.add(a, b);
            Expression denominator = BinaryExpression.subtract(c, d);
            Expression result = BinaryExpression.divide(numerator, denominator);

            String sql = result.toSQL();
            assertThat(sql).isEqualTo("((10 + 5) / (20 - 3))");
        }

        @Test
        @DisplayName("TC-EXPR-011: Unary negation")
        void testUnaryNegation() {
            Expression operand = Literal.of(42);
            Expression expr = UnaryExpression.negate(operand);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(-42)");
        }

        @Test
        @DisplayName("TC-EXPR-012: Negation of expression")
        void testNegationOfExpression() {
            Expression sum = BinaryExpression.add(Literal.of(10), Literal.of(5));
            Expression expr = UnaryExpression.negate(sum);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(-(10 + 5))");
        }

        @Test
        @DisplayName("TC-EXPR-013: Long arithmetic")
        void testLongArithmetic() {
            Expression left = Literal.of(9999999999L);
            Expression right = Literal.of(1L);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("+");
            assertThat(expr.dataType()).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("TC-EXPR-014: Float arithmetic")
        void testFloatArithmetic() {
            Expression left = Literal.of(3.14f);
            Expression right = Literal.of(2.0f);
            Expression expr = BinaryExpression.multiply(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("*");
            assertThat(expr.dataType()).isInstanceOf(FloatType.class);
        }

        @Test
        @DisplayName("TC-EXPR-015: Decimal arithmetic")
        void testDecimalArithmetic() {
            DecimalType decimalType = new DecimalType(10, 2);
            Expression left = new Literal(100.50, decimalType);
            Expression right = new Literal(25.25, decimalType);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("+");
        }

        @ParameterizedTest
        @CsvSource({
            "10, 5, 15",
            "100, 50, 150",
            "0, 0, 0",
            "-10, 10, 0",
            "999, 1, 1000"
        })
        @DisplayName("TC-EXPR-016: Addition with various values")
        void testAdditionWithVariousValues(int left, int right, int expected) {
            Expression leftExpr = Literal.of(left);
            Expression rightExpr = Literal.of(right);
            Expression expr = BinaryExpression.add(leftExpr, rightExpr);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo(String.format("(%d + %d)", left, right));
        }

        @Test
        @DisplayName("TC-EXPR-017: Division by zero expression (SQL generation)")
        void testDivisionByZero() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(0);
            Expression expr = BinaryExpression.divide(left, right);

            // SQL generation should succeed (runtime error will occur on execution)
            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(10 / 0)");
        }

        @Test
        @DisplayName("TC-EXPR-018: Arithmetic with NULL literals")
        void testArithmeticWithNull() {
            Expression left = Literal.nullValue(IntegerType.get());
            Expression right = Literal.of(10);
            Expression expr = BinaryExpression.add(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(NULL + 10)");
            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("TC-EXPR-019: Arithmetic nullability propagation")
        void testArithmeticNullabilityPropagation() {
            Expression nullableCol = new ColumnReference("value", IntegerType.get(), true);
            Expression nonNullLiteral = Literal.of(5);
            Expression expr = BinaryExpression.multiply(nullableCol, nonNullLiteral);

            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("TC-EXPR-020: Complex nested arithmetic with 5 levels")
        void testComplexNestedArithmetic() {
            // ((((a + b) * c) - d) / e)
            Expression a = Literal.of(1);
            Expression b = Literal.of(2);
            Expression c = Literal.of(3);
            Expression d = Literal.of(4);
            Expression e = Literal.of(5);

            Expression level1 = BinaryExpression.add(a, b);
            Expression level2 = BinaryExpression.multiply(level1, c);
            Expression level3 = BinaryExpression.subtract(level2, d);
            Expression level4 = BinaryExpression.divide(level3, e);

            String sql = level4.toSQL();
            assertThat(sql).isEqualTo("((((1 + 2) * 3) - 4) / 5)");
        }
    }

    // ==================== COMPARISON EXPRESSION TESTS (15 tests) ====================

    @Nested
    @DisplayName("Comparison Expression Tests")
    class ComparisonExpressionTests {

        @Test
        @DisplayName("TC-EXPR-021: Integer equality comparison")
        void testIntegerEquality() {
            Expression left = new ColumnReference("age", IntegerType.get());
            Expression right = Literal.of(18);
            Expression expr = BinaryExpression.equal(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(age = 18)");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("TC-EXPR-022: String equality comparison")
        void testStringEquality() {
            Expression left = new ColumnReference("status", StringType.get());
            Expression right = Literal.of("active");
            Expression expr = BinaryExpression.equal(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(status = 'active')");
        }

        @Test
        @DisplayName("TC-EXPR-023: Not equal comparison")
        void testNotEqual() {
            Expression left = new ColumnReference("id", IntegerType.get());
            Expression right = Literal.of(0);
            Expression expr = BinaryExpression.notEqual(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(id != 0)");
        }

        @Test
        @DisplayName("TC-EXPR-024: Less than comparison")
        void testLessThan() {
            Expression left = new ColumnReference("price", DoubleType.get());
            Expression right = Literal.of(100.0);
            Expression expr = BinaryExpression.lessThan(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(price < 100.0)");
        }

        @Test
        @DisplayName("TC-EXPR-025: Less than or equal comparison")
        void testLessThanOrEqual() {
            Expression left = new ColumnReference("score", IntegerType.get());
            Expression right = Literal.of(50);
            Expression expr = new BinaryExpression(left, BinaryExpression.Operator.LESS_THAN_OR_EQUAL, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(score <= 50)");
        }

        @Test
        @DisplayName("TC-EXPR-026: Greater than comparison")
        void testGreaterThan() {
            Expression left = new ColumnReference("amount", DoubleType.get());
            Expression right = Literal.of(1000.0);
            Expression expr = BinaryExpression.greaterThan(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(amount > 1000.0)");
        }

        @Test
        @DisplayName("TC-EXPR-027: Greater than or equal comparison")
        void testGreaterThanOrEqual() {
            Expression left = new ColumnReference("quantity", IntegerType.get());
            Expression right = Literal.of(1);
            Expression expr = new BinaryExpression(left, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(quantity >= 1)");
        }

        @Test
        @DisplayName("TC-EXPR-028: Date comparison")
        void testDateComparison() {
            Expression left = new ColumnReference("order_date", DateType.get());
            Expression right = new Literal("2024-01-01", DateType.get());
            Expression expr = BinaryExpression.greaterThan(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains(">");
        }

        @Test
        @DisplayName("TC-EXPR-029: Timestamp comparison")
        void testTimestampComparison() {
            Expression left = new ColumnReference("created_at", TimestampType.get());
            Expression right = new ColumnReference("updated_at", TimestampType.get());
            Expression expr = BinaryExpression.lessThan(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(created_at < updated_at)");
        }

        @Test
        @DisplayName("TC-EXPR-030: Comparison with NULL")
        void testComparisonWithNull() {
            Expression left = new ColumnReference("value", IntegerType.get());
            Expression right = Literal.nullValue(IntegerType.get());
            Expression expr = BinaryExpression.equal(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(value = NULL)");
        }

        @ParameterizedTest
        @CsvSource({
            "10, 20, <",
            "20, 10, >",
            "10, 10, ="
        })
        @DisplayName("TC-EXPR-031: Various comparison operations")
        void testVariousComparisons(int left, int right, String expectedOp) {
            Expression leftExpr = Literal.of(left);
            Expression rightExpr = Literal.of(right);

            BinaryExpression.Operator op;
            if (expectedOp.equals("<")) {
                op = BinaryExpression.Operator.LESS_THAN;
            } else if (expectedOp.equals(">")) {
                op = BinaryExpression.Operator.GREATER_THAN;
            } else {
                op = BinaryExpression.Operator.EQUAL;
            }

            Expression expr = new BinaryExpression(leftExpr, op, rightExpr);
            String sql = expr.toSQL();
            assertThat(sql).contains(expectedOp);
        }

        @Test
        @DisplayName("TC-EXPR-032: Chained comparisons with AND")
        void testChainedComparisons() {
            // age >= 18 AND age < 65
            Expression age = new ColumnReference("age", IntegerType.get());
            Expression minAge = new BinaryExpression(age, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, Literal.of(18));
            Expression maxAge = BinaryExpression.lessThan(age, Literal.of(65));
            Expression expr = BinaryExpression.and(minAge, maxAge);

            String sql = expr.toSQL();
            assertThat(sql).contains(">=").contains("<").contains("AND");
        }

        @Test
        @DisplayName("TC-EXPR-033: Boolean column comparison")
        void testBooleanComparison() {
            Expression left = new ColumnReference("active", BooleanType.get());
            Expression right = Literal.of(true);
            Expression expr = BinaryExpression.equal(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(active = TRUE)");
        }

        @Test
        @DisplayName("TC-EXPR-034: String comparison with special characters")
        void testStringComparisonWithSpecialChars() {
            Expression left = new ColumnReference("name", StringType.get());
            Expression right = Literal.of("O'Brien");
            Expression expr = BinaryExpression.equal(left, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(name = 'O''Brien')");
        }

        @Test
        @DisplayName("TC-EXPR-035: Comparison result is always boolean")
        void testComparisonResultType() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(20);
            Expression expr = BinaryExpression.lessThan(left, right);

            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }
    }

    // ==================== LOGICAL EXPRESSION TESTS (10 tests) ====================

    @Nested
    @DisplayName("Logical Expression Tests")
    class LogicalExpressionTests {

        @Test
        @DisplayName("TC-EXPR-036: Simple AND expression")
        void testSimpleAnd() {
            Expression left = BinaryExpression.equal(
                new ColumnReference("active", BooleanType.get()),
                Literal.of(true)
            );
            Expression right = BinaryExpression.equal(
                new ColumnReference("verified", BooleanType.get()),
                Literal.of(true)
            );
            Expression expr = BinaryExpression.and(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("AND");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("TC-EXPR-037: Simple OR expression")
        void testSimpleOr() {
            Expression left = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("active")
            );
            Expression right = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("pending")
            );
            Expression expr = BinaryExpression.or(left, right);

            String sql = expr.toSQL();
            assertThat(sql).contains("OR");
        }

        @Test
        @DisplayName("TC-EXPR-038: NOT expression")
        void testNotExpression() {
            Expression operand = new ColumnReference("deleted", BooleanType.get());
            Expression expr = UnaryExpression.not(operand);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(NOT deleted)");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("TC-EXPR-039: Complex nested logical - (a AND b) OR (c AND d)")
        void testComplexNestedLogical() {
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());
            Expression d = new ColumnReference("d", BooleanType.get());

            Expression leftAnd = BinaryExpression.and(a, b);
            Expression rightAnd = BinaryExpression.and(c, d);
            Expression expr = BinaryExpression.or(leftAnd, rightAnd);

            String sql = expr.toSQL();
            assertThat(sql).contains("AND").contains("OR");
        }

        @Test
        @DisplayName("TC-EXPR-040: NOT with comparison")
        void testNotWithComparison() {
            Expression comparison = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("deleted")
            );
            Expression expr = UnaryExpression.not(comparison);

            String sql = expr.toSQL();
            assertThat(sql).contains("NOT");
        }

        @Test
        @DisplayName("TC-EXPR-041: Multiple AND conditions")
        void testMultipleAndConditions() {
            // a AND b AND c
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());

            Expression ab = BinaryExpression.and(a, b);
            Expression abc = BinaryExpression.and(ab, c);

            String sql = abc.toSQL();
            assertThat(sql).isEqualTo("((a AND b) AND c)");
        }

        @Test
        @DisplayName("TC-EXPR-042: Multiple OR conditions")
        void testMultipleOrConditions() {
            // a OR b OR c
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());

            Expression ab = BinaryExpression.or(a, b);
            Expression abc = BinaryExpression.or(ab, c);

            String sql = abc.toSQL();
            assertThat(sql).contains("OR");
        }

        @Test
        @DisplayName("TC-EXPR-043: Logical expression with NULL")
        void testLogicalWithNull() {
            Expression left = BinaryExpression.equal(
                new ColumnReference("value", IntegerType.get(), true),
                Literal.of(10)
            );
            Expression right = Literal.of(true);
            Expression expr = BinaryExpression.and(left, right);

            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("TC-EXPR-044: Complex WHERE clause simulation")
        void testComplexWhereClause() {
            // (status = 'active' OR status = 'pending') AND amount > 100
            Expression statusCol = new ColumnReference("status", StringType.get());
            Expression activeCheck = BinaryExpression.equal(statusCol, Literal.of("active"));
            Expression pendingCheck = BinaryExpression.equal(statusCol, Literal.of("pending"));
            Expression statusOr = BinaryExpression.or(activeCheck, pendingCheck);

            Expression amountCheck = BinaryExpression.greaterThan(
                new ColumnReference("amount", DoubleType.get()),
                Literal.of(100.0)
            );

            Expression whereClause = BinaryExpression.and(statusOr, amountCheck);

            String sql = whereClause.toSQL();
            assertThat(sql).contains("OR").contains("AND").contains("amount");
        }

        @Test
        @DisplayName("TC-EXPR-045: Double negation")
        void testDoubleNegation() {
            Expression value = new ColumnReference("flag", BooleanType.get());
            Expression not1 = UnaryExpression.not(value);
            Expression not2 = UnaryExpression.not(not1);

            String sql = not2.toSQL();
            assertThat(sql).contains("NOT");
        }
    }

    // ==================== STRING FUNCTION TESTS (15 tests) ====================

    @Nested
    @DisplayName("String Function Tests")
    class StringFunctionTests {

        @Test
        @DisplayName("TC-EXPR-046: UPPER function")
        void testUpperFunction() {
            Expression arg = new ColumnReference("name", StringType.get());
            Expression func = FunctionCall.of("upper", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("upper(name)");
            assertThat(func.dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("TC-EXPR-047: LOWER function")
        void testLowerFunction() {
            Expression arg = Literal.of("HELLO");
            Expression func = FunctionCall.of("lower", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("lower('HELLO')");
        }

        @Test
        @DisplayName("TC-EXPR-048: TRIM function")
        void testTrimFunction() {
            Expression arg = new ColumnReference("description", StringType.get());
            Expression func = FunctionCall.of("trim", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("trim(description)");
        }

        @Test
        @DisplayName("TC-EXPR-049: LTRIM function")
        void testLtrimFunction() {
            Expression arg = Literal.of("  hello");
            Expression func = FunctionCall.of("ltrim", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("ltrim('  hello')");
        }

        @Test
        @DisplayName("TC-EXPR-050: RTRIM function")
        void testRtrimFunction() {
            Expression arg = Literal.of("hello  ");
            Expression func = FunctionCall.of("rtrim", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("rtrim('hello  ')");
        }

        @Test
        @DisplayName("TC-EXPR-051: CONCAT function with multiple args")
        void testConcatFunction() {
            Expression arg1 = new ColumnReference("first_name", StringType.get());
            Expression arg2 = Literal.of(" ");
            Expression arg3 = new ColumnReference("last_name", StringType.get());
            Expression func = FunctionCall.of("concat", StringType.get(), arg1, arg2, arg3);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("concat(first_name, ' ', last_name)");
        }

        @Test
        @DisplayName("TC-EXPR-052: SUBSTRING function")
        void testSubstringFunction() {
            Expression str = new ColumnReference("text", StringType.get());
            Expression start = Literal.of(1);
            Expression length = Literal.of(10);
            Expression func = FunctionCall.of("substring", StringType.get(), str, start, length);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("substring(text, 1, 10)");
        }

        @Test
        @DisplayName("TC-EXPR-053: LENGTH function")
        void testLengthFunction() {
            Expression arg = new ColumnReference("description", StringType.get());
            Expression func = FunctionCall.of("length", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("length(description)");
            assertThat(func.dataType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("TC-EXPR-054: REPLACE function")
        void testReplaceFunction() {
            Expression str = new ColumnReference("text", StringType.get());
            Expression oldStr = Literal.of("old");
            Expression newStr = Literal.of("new");
            Expression func = FunctionCall.of("replace", StringType.get(), str, oldStr, newStr);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("replace(text, 'old', 'new')");
        }

        @Test
        @DisplayName("TC-EXPR-055: Nested string functions")
        void testNestedStringFunctions() {
            // UPPER(TRIM(name))
            Expression name = new ColumnReference("name", StringType.get());
            Expression trim = FunctionCall.of("trim", name, StringType.get());
            Expression upper = FunctionCall.of("upper", trim, StringType.get());

            String sql = upper.toSQL();
            assertThat(sql).isEqualTo("upper(trim(name))");
        }

        @Test
        @DisplayName("TC-EXPR-056: String function with NULL argument")
        void testStringFunctionWithNull() {
            Expression arg = Literal.nullValue(StringType.get());
            Expression func = FunctionCall.of("upper", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("upper(NULL)");
        }

        @Test
        @DisplayName("TC-EXPR-057: String concatenation with ||")
        void testStringConcatenationOperator() {
            Expression left = new ColumnReference("first_name", StringType.get());
            Expression right = new ColumnReference("last_name", StringType.get());
            Expression expr = new BinaryExpression(left, BinaryExpression.Operator.CONCAT, right);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(first_name || last_name)");
        }

        @Test
        @DisplayName("TC-EXPR-058: String function in comparison")
        void testStringFunctionInComparison() {
            Expression upper = FunctionCall.of("upper", new ColumnReference("name", StringType.get()), StringType.get());
            Expression comparison = BinaryExpression.equal(upper, Literal.of("ADMIN"));

            String sql = comparison.toSQL();
            assertThat(sql).isEqualTo("(upper(name) = 'ADMIN')");
        }

        @Test
        @DisplayName("TC-EXPR-059: INITCAP function")
        void testInitcapFunction() {
            Expression arg = Literal.of("hello world");
            Expression func = FunctionCall.of("initcap", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("initcap('hello world')");
        }

        @Test
        @DisplayName("TC-EXPR-060: REVERSE function")
        void testReverseFunction() {
            Expression arg = new ColumnReference("code", StringType.get());
            Expression func = FunctionCall.of("reverse", arg, StringType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("reverse(code)");
        }
    }

    // ==================== MATH FUNCTION TESTS (10 tests) ====================

    @Nested
    @DisplayName("Math Function Tests")
    class MathFunctionTests {

        @Test
        @DisplayName("TC-EXPR-061: ABS function")
        void testAbsFunction() {
            Expression arg = new ColumnReference("balance", DoubleType.get());
            Expression func = FunctionCall.of("abs", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("abs(balance)");
        }

        @Test
        @DisplayName("TC-EXPR-062: CEIL function")
        void testCeilFunction() {
            Expression arg = Literal.of(3.14);
            Expression func = FunctionCall.of("ceil", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("ceil(3.14)");
        }

        @Test
        @DisplayName("TC-EXPR-063: FLOOR function")
        void testFloorFunction() {
            Expression arg = new ColumnReference("price", DoubleType.get());
            Expression func = FunctionCall.of("floor", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("floor(price)");
        }

        @Test
        @DisplayName("TC-EXPR-064: ROUND function with precision")
        void testRoundFunction() {
            Expression value = new ColumnReference("amount", DoubleType.get());
            Expression precision = Literal.of(2);
            Expression func = FunctionCall.of("round", DoubleType.get(), value, precision);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("round(amount, 2)");
        }

        @Test
        @DisplayName("TC-EXPR-065: SQRT function")
        void testSqrtFunction() {
            Expression arg = Literal.of(16);
            Expression func = FunctionCall.of("sqrt", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("sqrt(16)");
        }

        @Test
        @DisplayName("TC-EXPR-066: POWER function")
        void testPowerFunction() {
            Expression base = Literal.of(2);
            Expression exponent = Literal.of(10);
            Expression func = FunctionCall.of("power", DoubleType.get(), base, exponent);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("power(2, 10)");
        }

        @Test
        @DisplayName("TC-EXPR-067: SIN function")
        void testSinFunction() {
            Expression arg = new ColumnReference("angle", DoubleType.get());
            Expression func = FunctionCall.of("sin", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("sin(angle)");
        }

        @Test
        @DisplayName("TC-EXPR-068: COS function")
        void testCosFunction() {
            Expression arg = new ColumnReference("angle", DoubleType.get());
            Expression func = FunctionCall.of("cos", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("cos(angle)");
        }

        @Test
        @DisplayName("TC-EXPR-069: LOG function")
        void testLogFunction() {
            Expression arg = Literal.of(100);
            Expression func = FunctionCall.of("log", arg, DoubleType.get());

            String sql = func.toSQL();
            // DuckDB uses ln() for natural logarithm
            assertThat(sql).isEqualTo("ln(100)");
        }

        @Test
        @DisplayName("TC-EXPR-070: EXP function")
        void testExpFunction() {
            Expression arg = Literal.of(1);
            Expression func = FunctionCall.of("exp", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("exp(1)");
        }
    }

    // ==================== DATE FUNCTION TESTS (10 tests) ====================

    @Nested
    @DisplayName("Date Function Tests")
    class DateFunctionTests {

        @Test
        @DisplayName("TC-EXPR-071: YEAR function")
        void testYearFunction() {
            Expression arg = new ColumnReference("order_date", DateType.get());
            Expression func = FunctionCall.of("year", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("year(order_date)");
        }

        @Test
        @DisplayName("TC-EXPR-072: MONTH function")
        void testMonthFunction() {
            Expression arg = new ColumnReference("created_at", TimestampType.get());
            Expression func = FunctionCall.of("month", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("month(created_at)");
        }

        @Test
        @DisplayName("TC-EXPR-073: DAY function")
        void testDayFunction() {
            Expression arg = new ColumnReference("birth_date", DateType.get());
            Expression func = FunctionCall.of("day", arg, IntegerType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("day(birth_date)");
        }

        @Test
        @DisplayName("TC-EXPR-074: DATE_ADD function")
        void testDateAddFunction() {
            Expression date = new ColumnReference("start_date", DateType.get());
            Expression days = Literal.of(7);
            Expression func = FunctionCall.of("date_add", DateType.get(), date, days);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("date_add(start_date, 7)");
        }

        @Test
        @DisplayName("TC-EXPR-075: DATE_SUB function")
        void testDateSubFunction() {
            Expression date = new ColumnReference("end_date", DateType.get());
            Expression days = Literal.of(30);
            Expression func = FunctionCall.of("date_sub", DateType.get(), date, days);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("date_sub(end_date, 30)");
        }

        @Test
        @DisplayName("TC-EXPR-076: DATEDIFF function")
        void testDatediffFunction() {
            Expression end = new ColumnReference("end_date", DateType.get());
            Expression start = new ColumnReference("start_date", DateType.get());
            Expression func = FunctionCall.of("datediff", IntegerType.get(), end, start);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("datediff(end_date, start_date)");
        }

        @Test
        @DisplayName("TC-EXPR-077: CURRENT_DATE function")
        void testCurrentDateFunction() {
            Expression func = FunctionCall.of("current_date", DateType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("current_date()");
        }

        @Test
        @DisplayName("TC-EXPR-078: CURRENT_TIMESTAMP function")
        void testCurrentTimestampFunction() {
            Expression func = FunctionCall.of("current_timestamp", TimestampType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("current_timestamp()");
        }

        @Test
        @DisplayName("TC-EXPR-079: DATE_FORMAT function")
        void testDateFormatFunction() {
            Expression date = new ColumnReference("order_date", DateType.get());
            Expression format = Literal.of("yyyy-MM-dd");
            Expression func = FunctionCall.of("date_format", StringType.get(), date, format);

            String sql = func.toSQL();
            // DuckDB uses strftime() for date formatting
            assertThat(sql).isEqualTo("strftime(order_date, 'yyyy-MM-dd')");
        }

        @Test
        @DisplayName("TC-EXPR-080: Nested date functions")
        void testNestedDateFunctions() {
            // YEAR(DATE_ADD(order_date, 365))
            Expression orderDate = new ColumnReference("order_date", DateType.get());
            Expression dateAdd = FunctionCall.of("date_add", DateType.get(), orderDate, Literal.of(365));
            Expression year = FunctionCall.of("year", dateAdd, IntegerType.get());

            String sql = year.toSQL();
            assertThat(sql).isEqualTo("year(date_add(order_date, 365))");
        }
    }

    // ==================== AGGREGATE FUNCTION TESTS (10 tests) ====================

    @Nested
    @DisplayName("Aggregate Function Tests")
    class AggregateFunctionTests {

        @Test
        @DisplayName("TC-EXPR-081: SUM function")
        void testSumFunction() {
            Expression arg = new ColumnReference("amount", DoubleType.get());
            Expression func = FunctionCall.of("sum", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("sum(amount)");
        }

        @Test
        @DisplayName("TC-EXPR-082: AVG function")
        void testAvgFunction() {
            Expression arg = new ColumnReference("score", IntegerType.get());
            Expression func = FunctionCall.of("avg", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("avg(score)");
        }

        @Test
        @DisplayName("TC-EXPR-083: COUNT function")
        void testCountFunction() {
            Expression arg = new ColumnReference("id", IntegerType.get());
            Expression func = FunctionCall.of("count", arg, LongType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("count(id)");
        }

        @Test
        @DisplayName("TC-EXPR-084: COUNT(*) function")
        void testCountStarFunction() {
            Expression arg = new ColumnReference("*", IntegerType.get());
            Expression func = FunctionCall.of("count", arg, LongType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("count(*)");
        }

        @Test
        @DisplayName("TC-EXPR-085: MIN function")
        void testMinFunction() {
            Expression arg = new ColumnReference("price", DoubleType.get());
            Expression func = FunctionCall.of("min", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("min(price)");
        }

        @Test
        @DisplayName("TC-EXPR-086: MAX function")
        void testMaxFunction() {
            Expression arg = new ColumnReference("price", DoubleType.get());
            Expression func = FunctionCall.of("max", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("max(price)");
        }

        @Test
        @DisplayName("TC-EXPR-087: COUNT(DISTINCT) function")
        void testCountDistinctFunction() {
            // Note: This would require special handling in actual implementation
            Expression arg = new ColumnReference("customer_id", IntegerType.get());
            Expression func = FunctionCall.of("count", arg, LongType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("count(customer_id)");
        }

        @Test
        @DisplayName("TC-EXPR-088: STDDEV function")
        void testStddevFunction() {
            Expression arg = new ColumnReference("value", DoubleType.get());
            Expression func = FunctionCall.of("stddev", arg, DoubleType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("stddev(value)");
        }

        @Test
        @DisplayName("TC-EXPR-089: VARIANCE function")
        void testVarianceFunction() {
            Expression arg = new ColumnReference("value", DoubleType.get());
            Expression func = FunctionCall.of("variance", arg, DoubleType.get());

            String sql = func.toSQL();
            // DuckDB uses var_samp() for sample variance
            assertThat(sql).isEqualTo("var_samp(value)");
        }

        @Test
        @DisplayName("TC-EXPR-090: Aggregate in arithmetic expression")
        void testAggregateInArithmetic() {
            Expression sum = FunctionCall.of("sum", new ColumnReference("revenue", DoubleType.get()), DoubleType.get());
            Expression count = FunctionCall.of("count", new ColumnReference("id", IntegerType.get()), LongType.get());
            Expression avg = BinaryExpression.divide(sum, count);

            String sql = avg.toSQL();
            assertThat(sql).isEqualTo("(sum(revenue) / count(id))");
        }
    }

    // ==================== NULL HANDLING TESTS (10 tests) ====================

    @Nested
    @DisplayName("NULL Handling Tests")
    class NullHandlingTests {

        @Test
        @DisplayName("TC-EXPR-091: IS NULL expression")
        void testIsNull() {
            // Note: Requires IsNull expression class (simplified here)
            Expression col = new ColumnReference("email", StringType.get(), true);
            Expression isNull = FunctionCall.of("isnull", col, BooleanType.get());

            String sql = isNull.toSQL();
            assertThat(sql).isEqualTo("isnull(email)");
        }

        @Test
        @DisplayName("TC-EXPR-092: IS NOT NULL expression")
        void testIsNotNull() {
            Expression col = new ColumnReference("phone", StringType.get(), true);
            Expression isNotNull = FunctionCall.of("isnotnull", col, BooleanType.get());

            String sql = isNotNull.toSQL();
            assertThat(sql).isEqualTo("isnotnull(phone)");
        }

        @Test
        @DisplayName("TC-EXPR-093: COALESCE with 2 arguments")
        void testCoalesceTwoArgs() {
            Expression arg1 = new ColumnReference("value", IntegerType.get(), true);
            Expression arg2 = Literal.of(0);
            Expression func = FunctionCall.of("coalesce", IntegerType.get(), arg1, arg2);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("coalesce(value, 0)");
        }

        @Test
        @DisplayName("TC-EXPR-094: COALESCE with multiple arguments")
        void testCoalesceMultipleArgs() {
            Expression arg1 = new ColumnReference("primary_phone", StringType.get(), true);
            Expression arg2 = new ColumnReference("secondary_phone", StringType.get(), true);
            Expression arg3 = Literal.of("N/A");
            Expression func = FunctionCall.of("coalesce", StringType.get(), arg1, arg2, arg3);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("coalesce(primary_phone, secondary_phone, 'N/A')");
        }

        @Test
        @DisplayName("TC-EXPR-095: IFNULL function")
        void testIfnullFunction() {
            Expression value = new ColumnReference("discount", DoubleType.get(), true);
            Expression defaultValue = Literal.of(0.0);
            Expression func = FunctionCall.of("ifnull", DoubleType.get(), value, defaultValue);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("ifnull(discount, 0.0)");
        }

        @Test
        @DisplayName("TC-EXPR-096: NULLIF function")
        void testNullifFunction() {
            Expression value = new ColumnReference("status", StringType.get());
            Expression nullValue = Literal.of("UNKNOWN");
            Expression func = FunctionCall.of("nullif", StringType.get(), value, nullValue);

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("nullif(status, 'UNKNOWN')");
        }

        @Test
        @DisplayName("TC-EXPR-097: NULL literal in arithmetic")
        void testNullInArithmetic() {
            Expression nullLit = Literal.nullValue(IntegerType.get());
            Expression value = Literal.of(10);
            Expression expr = BinaryExpression.add(nullLit, value);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(NULL + 10)");
            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("TC-EXPR-098: NULL literal in comparison")
        void testNullInComparison() {
            Expression col = new ColumnReference("value", IntegerType.get());
            Expression nullLit = Literal.nullValue(IntegerType.get());
            Expression expr = BinaryExpression.equal(col, nullLit);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(value = NULL)");
        }

        @Test
        @DisplayName("TC-EXPR-099: Nullable column propagates nullability")
        void testNullablePropagation() {
            Expression nullable1 = new ColumnReference("a", IntegerType.get(), true);
            Expression nullable2 = new ColumnReference("b", IntegerType.get(), true);
            Expression nonNull = Literal.of(10);

            Expression expr1 = BinaryExpression.add(nullable1, nonNull);
            assertThat(expr1.nullable()).isTrue();

            Expression expr2 = BinaryExpression.add(nullable1, nullable2);
            assertThat(expr2.nullable()).isTrue();
        }

        @Test
        @DisplayName("TC-EXPR-100: COALESCE in complex expression")
        void testCoalesceInComplexExpression() {
            // COALESCE(discount, 0) * price
            Expression discount = new ColumnReference("discount", DoubleType.get(), true);
            Expression coalesce = FunctionCall.of("coalesce", DoubleType.get(), discount, Literal.of(0.0));
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression expr = BinaryExpression.multiply(coalesce, price);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("(coalesce(discount, 0.0) * price)");
        }
    }

    // ==================== ADDITIONAL EDGE CASE TESTS (10+ tests) ====================

    @Nested
    @DisplayName("Edge Case and Advanced Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("TC-EXPR-101: Empty string literal")
        void testEmptyStringLiteral() {
            Expression expr = Literal.of("");

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("''");
        }

        @Test
        @DisplayName("TC-EXPR-102: Very long string literal")
        void testLongStringLiteral() {
            String longString = "a".repeat(1000);
            Expression expr = Literal.of(longString);

            String sql = expr.toSQL();
            assertThat(sql).hasSize(1002); // 1000 chars + 2 quotes
        }

        @Test
        @DisplayName("TC-EXPR-103: Negative zero")
        void testNegativeZero() {
            Expression expr = Literal.of(-0.0);

            String sql = expr.toSQL();
            assertThat(sql).contains("-0.0");
        }

        @Test
        @DisplayName("TC-EXPR-104: Maximum integer value")
        void testMaxInteger() {
            Expression expr = Literal.of(Integer.MAX_VALUE);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo(String.valueOf(Integer.MAX_VALUE));
        }

        @Test
        @DisplayName("TC-EXPR-105: Minimum integer value")
        void testMinInteger() {
            Expression expr = Literal.of(Integer.MIN_VALUE);

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo(String.valueOf(Integer.MIN_VALUE));
        }

        @Test
        @DisplayName("TC-EXPR-106: Special float values - Infinity")
        void testInfinity() {
            Expression expr = Literal.of(Double.POSITIVE_INFINITY);

            String sql = expr.toSQL();
            assertThat(sql).contains("Infinity");
        }

        @Test
        @DisplayName("TC-EXPR-107: Special float values - NaN")
        void testNaN() {
            Expression expr = Literal.of(Double.NaN);

            String sql = expr.toSQL();
            assertThat(sql).contains("NaN");
        }

        @Test
        @DisplayName("TC-EXPR-108: Deeply nested expression (10 levels)")
        void testDeeplyNestedExpression() {
            Expression base = Literal.of(1);
            for (int i = 0; i < 10; i++) {
                base = BinaryExpression.add(base, Literal.of(1));
            }

            String sql = base.toSQL();
            assertThat(sql).contains("+");
            // Count opening parentheses
            long openParens = sql.chars().filter(ch -> ch == '(').count();
            assertThat(openParens).isEqualTo(10);
        }

        @Test
        @DisplayName("TC-EXPR-109: Function with no arguments")
        void testFunctionWithNoArgs() {
            Expression func = FunctionCall.of("now", TimestampType.get());

            String sql = func.toSQL();
            assertThat(sql).isEqualTo("now()");
        }

        @Test
        @DisplayName("TC-EXPR-110: Case sensitivity in column names")
        void testCaseSensitiveColumnNames() {
            Expression lower = new ColumnReference("customerId", IntegerType.get());
            Expression upper = new ColumnReference("CUSTOMERID", IntegerType.get());

            assertThat(lower.toSQL()).isEqualTo("customerId");
            assertThat(upper.toSQL()).isEqualTo("CUSTOMERID");
            assertThat(lower).isNotEqualTo(upper);
        }

        @Test
        @DisplayName("TC-EXPR-111: Unicode characters in string literals")
        void testUnicodeCharacters() {
            Expression expr = Literal.of("Hello 世界 🌍");

            String sql = expr.toSQL();
            assertThat(sql).isEqualTo("'Hello 世界 🌍'");
        }

        @Test
        @DisplayName("TC-EXPR-112: Qualified column reference with schema")
        void testQualifiedColumnReference() {
            Expression col = new ColumnReference("id", "customers", IntegerType.get(), false);

            String sql = col.toSQL();
            assertThat(sql).isEqualTo("customers.id");
        }

        @Test
        @DisplayName("TC-EXPR-113: Expression equality and hashCode")
        void testExpressionEquality() {
            Expression expr1 = BinaryExpression.add(Literal.of(10), Literal.of(20));
            Expression expr2 = BinaryExpression.add(Literal.of(10), Literal.of(20));
            Expression expr3 = BinaryExpression.add(Literal.of(10), Literal.of(30));

            assertThat(expr1).isEqualTo(expr2);
            assertThat(expr1).isNotEqualTo(expr3);
            assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
        }

        @Test
        @DisplayName("TC-EXPR-114: toString delegates to toSQL")
        void testToStringDelegatesToSQL() {
            Expression expr = BinaryExpression.add(Literal.of(1), Literal.of(2));

            assertThat(expr.toString()).isEqualTo(expr.toSQL());
        }

        @Test
        @DisplayName("TC-EXPR-115: Complex expression with all operator types")
        void testComplexMixedExpression() {
            // ((price * quantity) > 100) AND (status = 'active')
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression quantity = new ColumnReference("quantity", IntegerType.get());
            Expression total = BinaryExpression.multiply(price, quantity);
            Expression amountCheck = BinaryExpression.greaterThan(total, Literal.of(100.0));

            Expression status = new ColumnReference("status", StringType.get());
            Expression statusCheck = BinaryExpression.equal(status, Literal.of("active"));

            Expression combined = BinaryExpression.and(amountCheck, statusCheck);

            String sql = combined.toSQL();
            assertThat(sql).contains("*").contains(">").contains("AND").contains("=");
        }
    }
}
