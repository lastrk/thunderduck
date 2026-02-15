package com.thunderduck.expression;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.test.DifferentialAssertion;
import com.thunderduck.types.*;
import com.thunderduck.functions.FunctionRegistry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for Expression classes.
 *
 * <p>Tests 50+ scenarios including:
 * <ul>
 *   <li>Literal expressions (numeric, string, boolean, null) - 10 tests</li>
 *   <li>Column reference expressions - 5 tests</li>
 *   <li>Arithmetic operators (+, -, *, /, %) - 10 tests</li>
 *   <li>Comparison operators (=, !=, <, <=, >, >=) - 10 tests</li>
 *   <li>Logical operators (AND, OR, NOT) - 10 tests</li>
 *   <li>Function calls (string, math, date) - 10 tests</li>
 *   <li>Complex nested expressions - 5 tests</li>
 * </ul>
 */
@TestCategories.Tier1
@TestCategories.Unit
@TestCategories.Expression
@DisplayName("Expression Tests")
public class ExpressionTest extends TestBase {

    // ==================== Literal Expression Tests ====================

    @Nested
    @DisplayName("Literal Expression Tests")
    class LiteralExpressionTests {

        @Test
        @DisplayName("Integer literal creates correct SQL")
        void testIntegerLiteral() {
            Literal literal = Literal.of(42);
            
            assertThat(literal.value()).isEqualTo(42);
            assertThat(literal.dataType()).isInstanceOf(IntegerType.class);
            assertThat(literal.nullable()).isFalse();
            assertThat(literal.toSQL()).isEqualTo("42");
        }

        @Test
        @DisplayName("Long literal creates correct SQL")
        void testLongLiteral() {
            Literal literal = Literal.of(9999999999L);
            
            assertThat(literal.value()).isEqualTo(9999999999L);
            assertThat(literal.dataType()).isInstanceOf(LongType.class);
            assertThat(literal.toSQL()).isEqualTo("9999999999");
        }

        @Test
        @DisplayName("Float literal creates correct SQL")
        void testFloatLiteral() {
            Literal literal = Literal.of(3.14f);
            
            assertThat(literal.value()).isEqualTo(3.14f);
            assertThat(literal.dataType()).isInstanceOf(FloatType.class);
            assertThat(literal.toSQL()).contains("3.14");
        }

        @Test
        @DisplayName("Double literal creates correct SQL")
        void testDoubleLiteral() {
            Literal literal = Literal.of(2.718281828);
            
            assertThat(literal.value()).isEqualTo(2.718281828);
            assertThat(literal.dataType()).isInstanceOf(DoubleType.class);
            assertThat(literal.toSQL()).contains("2.718");
        }

        @Test
        @DisplayName("String literal creates correct SQL with quotes")
        void testStringLiteral() {
            Literal literal = Literal.of("hello world");
            
            assertThat(literal.value()).isEqualTo("hello world");
            assertThat(literal.dataType()).isInstanceOf(StringType.class);
            assertThat(literal.toSQL()).isEqualTo("'hello world'");
        }

        @Test
        @DisplayName("String literal with single quote escapes correctly")
        void testStringLiteralWithQuote() {
            Literal literal = Literal.of("it's");
            
            assertThat(literal.toSQL()).isEqualTo("'it''s'");
        }

        @Test
        @DisplayName("Boolean true literal creates correct SQL")
        void testBooleanTrueLiteral() {
            Literal literal = Literal.of(true);
            
            assertThat(literal.value()).isEqualTo(true);
            assertThat(literal.dataType()).isInstanceOf(BooleanType.class);
            assertThat(literal.toSQL()).isEqualTo("TRUE");
        }

        @Test
        @DisplayName("Boolean false literal creates correct SQL")
        void testBooleanFalseLiteral() {
            Literal literal = Literal.of(false);
            
            assertThat(literal.toSQL()).isEqualTo("FALSE");
        }

        @Test
        @DisplayName("NULL literal creates correct SQL")
        void testNullLiteral() {
            Literal literal = Literal.nullValue(IntegerType.get());
            
            assertThat(literal.value()).isNull();
            assertThat(literal.isNull()).isTrue();
            assertThat(literal.nullable()).isTrue();
            assertThat(literal.toSQL()).isEqualTo("NULL");
        }

        @Test
        @DisplayName("Literal equality works correctly")
        void testLiteralEquality() {
            Literal literal1 = Literal.of(42);
            Literal literal2 = Literal.of(42);
            Literal literal3 = Literal.of(43);
            
            assertThat(literal1).isEqualTo(literal2);
            assertThat(literal1).isNotEqualTo(literal3);
            assertThat(literal1.hashCode()).isEqualTo(literal2.hashCode());
        }
    }

    // ==================== Column Reference Tests ====================

    @Nested
    @DisplayName("Column Reference Tests")
    class ColumnReferenceTests {

        @Test
        @DisplayName("Simple column reference creates correct SQL")
        void testSimpleColumnReference() {
            ColumnReference col = new ColumnReference("customer_id", IntegerType.get());

            assertThat(col.columnName()).isEqualTo("customer_id");
            assertThat(col.dataType()).isInstanceOf(IntegerType.class);
            assertThat(col.nullable()).isTrue(); // Constructor with 2 args defaults to nullable=true
            assertThat(col.toSQL()).isEqualTo("customer_id");
        }

        @Test
        @DisplayName("Qualified column reference creates correct SQL")
        void testQualifiedColumnReference() {
            ColumnReference col = new ColumnReference("order_id", "orders", LongType.get(), true);

            assertThat(col.qualifier()).isEqualTo("orders");
            assertThat(col.columnName()).isEqualTo("order_id");
            assertThat(col.toSQL()).isEqualTo("orders.order_id");
        }

        @Test
        @DisplayName("Nullable column reference")
        void testNullableColumnReference() {
            ColumnReference col = new ColumnReference("email", StringType.get(), true);
            
            assertThat(col.nullable()).isTrue();
        }

        @Test
        @DisplayName("Column reference equality")
        void testColumnReferenceEquality() {
            ColumnReference col1 = new ColumnReference("id", IntegerType.get());
            ColumnReference col2 = new ColumnReference("id", IntegerType.get());
            ColumnReference col3 = new ColumnReference("name", StringType.get());
            
            assertThat(col1).isEqualTo(col2);
            assertThat(col1).isNotEqualTo(col3);
        }

        @Test
        @DisplayName("Column reference with complex type")
        void testColumnReferenceWithComplexType() {
            ArrayType arrayType = new ArrayType(IntegerType.get());
            ColumnReference col = new ColumnReference("tags", arrayType);
            
            assertThat(col.dataType()).isInstanceOf(ArrayType.class);
        }
    }

    // ==================== Arithmetic Expression Tests ====================

    @Nested
    @DisplayName("Arithmetic Expression Tests")
    class ArithmeticExpressionTests {

        @Test
        @DisplayName("Addition expression creates correct SQL")
        void testAddition() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(5);
            BinaryExpression expr = BinaryExpression.add(left, right);
            
            assertThat(expr.operator()).isEqualTo(BinaryExpression.Operator.ADD);
            assertThat(expr.toSQL()).isEqualTo("(10 + 5)");
        }

        @Test
        @DisplayName("Subtraction expression creates correct SQL")
        void testSubtraction() {
            Expression left = Literal.of(20);
            Expression right = Literal.of(7);
            BinaryExpression expr = BinaryExpression.subtract(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(20 - 7)");
        }

        @Test
        @DisplayName("Multiplication expression creates correct SQL")
        void testMultiplication() {
            Expression left = new ColumnReference("price", DoubleType.get());
            Expression right = new ColumnReference("quantity", IntegerType.get());
            BinaryExpression expr = BinaryExpression.multiply(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(price * quantity)");
        }

        @Test
        @DisplayName("Division expression creates correct SQL")
        void testDivision() {
            Expression left = Literal.of(100);
            Expression right = Literal.of(4);
            BinaryExpression expr = BinaryExpression.divide(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(100 / 4)");
        }

        @Test
        @DisplayName("Modulo expression creates correct SQL")
        void testModulo() {
            Expression left = Literal.of(17);
            Expression right = Literal.of(5);
            BinaryExpression expr = BinaryExpression.modulo(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(17 % 5)");
        }

        @Test
        @DisplayName("Nested arithmetic expression")
        void testNestedArithmetic() {
            // (a + b) * c
            Expression a = new ColumnReference("a", IntegerType.get());
            Expression b = new ColumnReference("b", IntegerType.get());
            Expression c = new ColumnReference("c", IntegerType.get());
            
            BinaryExpression sum = BinaryExpression.add(a, b);
            BinaryExpression product = BinaryExpression.multiply(sum, c);
            
            assertThat(product.toSQL()).isEqualTo("((a + b) * c)");
        }

        @Test
        @DisplayName("Complex arithmetic expression")
        void testComplexArithmetic() {
            // (a + b) / (c - d)
            Expression a = Literal.of(10);
            Expression b = Literal.of(5);
            Expression c = Literal.of(20);
            Expression d = Literal.of(3);
            
            BinaryExpression numerator = BinaryExpression.add(a, b);
            BinaryExpression denominator = BinaryExpression.subtract(c, d);
            BinaryExpression division = BinaryExpression.divide(numerator, denominator);
            
            assertThat(division.toSQL()).isEqualTo("((10 + 5) / (20 - 3))");
        }

        @Test
        @DisplayName("Arithmetic expression data type")
        void testArithmeticDataType() {
            Expression left = new ColumnReference("amount", DoubleType.get());
            Expression right = Literal.of(2);
            BinaryExpression expr = BinaryExpression.multiply(left, right);
            
            assertThat(expr.dataType()).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Arithmetic expression nullability")
        void testArithmeticNullability() {
            Expression left = new ColumnReference("value", IntegerType.get(), true);
            Expression right = Literal.of(5);
            BinaryExpression expr = BinaryExpression.add(left, right);
            
            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("Operator type checks")
        void testOperatorTypeChecks() {
            assertThat(BinaryExpression.Operator.ADD.isArithmetic()).isTrue();
            assertThat(BinaryExpression.Operator.ADD.isComparison()).isFalse();
            assertThat(BinaryExpression.Operator.ADD.isLogical()).isFalse();
        }
    }

    // ==================== Comparison Expression Tests ====================

    @Nested
    @DisplayName("Comparison Expression Tests")
    class ComparisonExpressionTests {

        @Test
        @DisplayName("Equal expression creates correct SQL")
        void testEqual() {
            Expression left = new ColumnReference("status", StringType.get());
            Expression right = Literal.of("active");
            BinaryExpression expr = BinaryExpression.equal(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(status = 'active')");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Not equal expression creates correct SQL")
        void testNotEqual() {
            Expression left = new ColumnReference("id", IntegerType.get());
            Expression right = Literal.of(0);
            BinaryExpression expr = BinaryExpression.notEqual(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(id != 0)");
        }

        @Test
        @DisplayName("Less than expression creates correct SQL")
        void testLessThan() {
            Expression left = new ColumnReference("age", IntegerType.get());
            Expression right = Literal.of(18);
            BinaryExpression expr = BinaryExpression.lessThan(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(age < 18)");
        }

        @Test
        @DisplayName("Less than or equal expression")
        void testLessThanOrEqual() {
            Expression left = new ColumnReference("price", DoubleType.get());
            Expression right = Literal.of(100.0);
            BinaryExpression expr = new BinaryExpression(
                left,
                BinaryExpression.Operator.LESS_THAN_OR_EQUAL,
                right
            );
            
            assertThat(expr.toSQL()).contains("<=");
        }

        @Test
        @DisplayName("Greater than expression creates correct SQL")
        void testGreaterThan() {
            Expression left = new ColumnReference("score", IntegerType.get());
            Expression right = Literal.of(50);
            BinaryExpression expr = BinaryExpression.greaterThan(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(score > 50)");
        }

        @Test
        @DisplayName("Greater than or equal expression")
        void testGreaterThanOrEqual() {
            Expression left = new ColumnReference("quantity", IntegerType.get());
            Expression right = Literal.of(1);
            BinaryExpression expr = new BinaryExpression(
                left,
                BinaryExpression.Operator.GREATER_THAN_OR_EQUAL,
                right
            );
            
            assertThat(expr.toSQL()).contains(">=");
        }

        @Test
        @DisplayName("Comparison expression returns boolean type")
        void testComparisonReturnsBoolean() {
            Expression left = Literal.of(10);
            Expression right = Literal.of(20);
            BinaryExpression expr = BinaryExpression.lessThan(left, right);
            
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Comparison with different types")
        void testComparisonDifferentTypes() {
            Expression left = new ColumnReference("total", DoubleType.get());
            Expression right = Literal.of(100);
            BinaryExpression expr = BinaryExpression.greaterThan(left, right);
            
            assertThat(expr.toSQL()).isEqualTo("(total > 100)");
        }

        @ParameterizedTest
        @CsvSource({
            "EQUAL, =",
            "NOT_EQUAL, !=",
            "LESS_THAN, <",
            "LESS_THAN_OR_EQUAL, <=",
            "GREATER_THAN, >",
            "GREATER_THAN_OR_EQUAL, >="
        })
        @DisplayName("All comparison operators have correct symbols")
        void testComparisonOperatorSymbols(String operatorName, String expectedSymbol) {
            BinaryExpression.Operator operator = BinaryExpression.Operator.valueOf(operatorName);
            assertThat(operator.symbol()).isEqualTo(expectedSymbol);
            assertThat(operator.isComparison()).isTrue();
        }

        @Test
        @DisplayName("Chained comparisons")
        void testChainedComparisons() {
            // age >= 18 AND age < 65
            Expression age = new ColumnReference("age", IntegerType.get());
            BinaryExpression minAge = BinaryExpression.greaterThan(age, Literal.of(18));
            BinaryExpression maxAge = BinaryExpression.lessThan(age, Literal.of(65));
            BinaryExpression combined = BinaryExpression.and(minAge, maxAge);
            
            assertThat(combined.toSQL()).contains("AND");
        }
    }

    // ==================== Logical Expression Tests ====================

    @Nested
    @DisplayName("Logical Expression Tests")
    class LogicalExpressionTests {

        @Test
        @DisplayName("AND expression creates correct SQL")
        void testAnd() {
            Expression left = BinaryExpression.equal(
                new ColumnReference("active", BooleanType.get()),
                Literal.of(true)
            );
            Expression right = BinaryExpression.equal(
                new ColumnReference("verified", BooleanType.get()),
                Literal.of(true)
            );
            BinaryExpression expr = BinaryExpression.and(left, right);
            
            assertThat(expr.toSQL()).contains("AND");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("OR expression creates correct SQL")
        void testOr() {
            Expression left = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("active")
            );
            Expression right = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("pending")
            );
            BinaryExpression expr = BinaryExpression.or(left, right);
            
            assertThat(expr.toSQL()).contains("OR");
        }

        @Test
        @DisplayName("NOT expression creates correct SQL")
        void testNot() {
            Expression operand = new ColumnReference("deleted", BooleanType.get());
            UnaryExpression expr = UnaryExpression.not(operand);
            
            assertThat(expr.toSQL()).isEqualTo("(NOT deleted)");
            assertThat(expr.dataType()).isInstanceOf(BooleanType.class);
        }

        @Test
        @DisplayName("Complex logical expression")
        void testComplexLogical() {
            // (a AND b) OR (c AND d)
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());
            Expression d = new ColumnReference("d", BooleanType.get());
            
            BinaryExpression leftAnd = BinaryExpression.and(a, b);
            BinaryExpression rightAnd = BinaryExpression.and(c, d);
            BinaryExpression orExpr = BinaryExpression.or(leftAnd, rightAnd);
            
            assertThat(orExpr.toSQL()).contains("AND").contains("OR");
        }

        @Test
        @DisplayName("Negation expression")
        void testNegation() {
            Expression operand = Literal.of(42);
            UnaryExpression expr = UnaryExpression.negate(operand);
            
            assertThat(expr.toSQL()).isEqualTo("(-42)");
        }

        @Test
        @DisplayName("NOT with comparison")
        void testNotWithComparison() {
            Expression comparison = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("deleted")
            );
            UnaryExpression notExpr = UnaryExpression.not(comparison);
            
            assertThat(notExpr.toSQL()).contains("NOT");
        }

        @Test
        @DisplayName("Logical operator type checks")
        void testLogicalOperatorTypeChecks() {
            assertThat(BinaryExpression.Operator.AND.isLogical()).isTrue();
            assertThat(BinaryExpression.Operator.OR.isLogical()).isTrue();
            assertThat(BinaryExpression.Operator.AND.isArithmetic()).isFalse();
        }

        @Test
        @DisplayName("Multiple AND conditions")
        void testMultipleAndConditions() {
            // a AND b AND c
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());
            
            BinaryExpression ab = BinaryExpression.and(a, b);
            BinaryExpression abc = BinaryExpression.and(ab, c);
            
            assertThat(abc.toSQL()).isEqualTo("((a AND b) AND c)");
        }

        @Test
        @DisplayName("Multiple OR conditions")
        void testMultipleOrConditions() {
            // a OR b OR c
            Expression a = new ColumnReference("a", BooleanType.get());
            Expression b = new ColumnReference("b", BooleanType.get());
            Expression c = new ColumnReference("c", BooleanType.get());
            
            BinaryExpression ab = BinaryExpression.or(a, b);
            BinaryExpression abc = BinaryExpression.or(ab, c);
            
            assertThat(abc.toSQL()).contains("OR");
        }

        @Test
        @DisplayName("Unary expression nullability")
        void testUnaryNullability() {
            Expression nullable = new ColumnReference("value", BooleanType.get(), true);
            UnaryExpression notExpr = UnaryExpression.not(nullable);
            
            assertThat(notExpr.nullable()).isTrue();
        }
    }

    // ==================== Function Call Tests ====================

    @Nested
    @DisplayName("Function Call Tests")
    class FunctionCallTests {

        @Test
        @DisplayName("String function call creates correct SQL")
        void testStringFunction() {
            Expression arg = new ColumnReference("name", StringType.get());
            FunctionCall func = FunctionCall.of("upper", arg, StringType.get());

            assertThat(func.functionName()).isEqualTo("upper");
            assertThat(func.arguments()).hasSize(1);
            assertThat(func.toSQL()).isEqualTo("upper(name)");
        }

        @Test
        @DisplayName("Math function call creates correct SQL")
        void testMathFunction() {
            Expression arg = new ColumnReference("value", DoubleType.get());
            FunctionCall func = FunctionCall.of("abs", arg, DoubleType.get());

            assertThat(func.toSQL()).isEqualTo("abs(value)");
        }

        @Test
        @DisplayName("Function with multiple arguments")
        void testFunctionWithMultipleArgs() {
            Expression arg1 = new ColumnReference("text", StringType.get());
            Expression arg2 = Literal.of(1);
            Expression arg3 = Literal.of(5);
            FunctionCall func = FunctionCall.of("substring", StringType.get(), arg1, arg2, arg3);

            assertThat(func.arguments()).hasSize(3);
            assertThat(func.toSQL()).isEqualTo("substring(text, 1, 5)");
        }

        @Test
        @DisplayName("Aggregate function call")
        void testAggregateFunction() {
            Expression arg = new ColumnReference("amount", DoubleType.get());
            FunctionCall func = FunctionCall.of("sum", arg, DoubleType.get());

            // SUM is wrapped with CAST to BIGINT for Spark compatibility
            assertThat(func.toSQL()).containsIgnoringCase("SUM(amount)");
        }

        @Test
        @DisplayName("Function with no arguments")
        void testFunctionWithNoArgs() {
            FunctionCall func = FunctionCall.of("now", TimestampType.get());

            assertThat(func.arguments()).isEmpty();
            assertThat(func.toSQL()).isEqualTo("now()");
        }

        @Test
        @DisplayName("Nested function calls")
        void testNestedFunctionCalls() {
            Expression inner = FunctionCall.of(
                "lower",
                new ColumnReference("name", StringType.get()),
                StringType.get()
            );
            FunctionCall outer = FunctionCall.of("trim", inner, StringType.get());

            assertThat(outer.toSQL()).isEqualTo("trim(lower(name))");
        }

        @Test
        @DisplayName("Function in arithmetic expression")
        void testFunctionInArithmetic() {
            Expression func = FunctionCall.of("abs", Literal.of(-10), IntegerType.get());
            Expression expr = BinaryExpression.multiply(func, Literal.of(2));

            assertThat(expr.toSQL()).isEqualTo("(abs(-10) * 2)");
        }

        @Test
        @DisplayName("Function registry integration")
        void testFunctionRegistryIntegration() {
            String translated = FunctionRegistry.translate("upper", "name");
            assertThat(translated).isEqualTo("upper(name)");
        }

        @Test
        @DisplayName("Function with literal arguments")
        void testFunctionWithLiterals() {
            FunctionCall func = FunctionCall.of(
                "concat",
                StringType.get(),
                Literal.of("Hello"),
                Literal.of(" "),
                Literal.of("World")
            );

            // concat is translated to || operator with CAST for correct NULL propagation
            assertThat(func.toSQL()).isEqualTo("(CAST('Hello' AS VARCHAR) || CAST(' ' AS VARCHAR) || CAST('World' AS VARCHAR))");
        }

        @Test
        @DisplayName("Function call equality")
        void testFunctionCallEquality() {
            Expression arg = new ColumnReference("x", IntegerType.get());
            FunctionCall func1 = FunctionCall.of("abs", arg, IntegerType.get());
            FunctionCall func2 = FunctionCall.of("abs", arg, IntegerType.get());
            FunctionCall func3 = FunctionCall.of("sqrt", arg, DoubleType.get());

            assertThat(func1).isEqualTo(func2);
            assertThat(func1).isNotEqualTo(func3);
        }
    }

    // ==================== Complex Nested Expression Tests ====================

    @Nested
    @DisplayName("Complex Nested Expression Tests")
    class ComplexNestedExpressionTests {

        @Test
        @DisplayName("Deeply nested expression")
        void testDeeplyNestedExpression() {
            // ((a + b) * (c - d)) / (e + f)
            Expression a = Literal.of(10);
            Expression b = Literal.of(5);
            Expression c = Literal.of(20);
            Expression d = Literal.of(3);
            Expression e = Literal.of(7);
            Expression f = Literal.of(2);
            
            BinaryExpression sum1 = BinaryExpression.add(a, b);
            BinaryExpression diff = BinaryExpression.subtract(c, d);
            BinaryExpression prod = BinaryExpression.multiply(sum1, diff);
            BinaryExpression sum2 = BinaryExpression.add(e, f);
            BinaryExpression result = BinaryExpression.divide(prod, sum2);
            
            assertThat(result.toSQL()).isEqualTo("(((10 + 5) * (20 - 3)) / (7 + 2))");
        }

        @Test
        @DisplayName("Expression with mixed operations")
        void testMixedOperations() {
            // upper(name) = 'ADMIN' AND age >= 18
            Expression upperFunc = FunctionCall.of(
                "upper",
                new ColumnReference("name", StringType.get()),
                StringType.get()
            );
            Expression nameCheck = BinaryExpression.equal(upperFunc, Literal.of("ADMIN"));

            Expression ageCheck = BinaryExpression.greaterThan(
                new ColumnReference("age", IntegerType.get()),
                Literal.of(18)
            );

            BinaryExpression combined = BinaryExpression.and(nameCheck, ageCheck);

            assertThat(combined.toSQL()).contains("upper(name)").contains("AND").contains("age");
        }

        @Test
        @DisplayName("Expression with function and arithmetic")
        void testFunctionAndArithmetic() {
            // abs(balance) * 1.05
            Expression balanceCol = new ColumnReference("balance", DoubleType.get());
            Expression absFunc = FunctionCall.of("abs", balanceCol, DoubleType.get());
            Expression result = BinaryExpression.multiply(absFunc, Literal.of(1.05));

            assertThat(result.toSQL()).contains("abs(balance)").contains("*");
        }

        @Test
        @DisplayName("Complex WHERE clause expression")
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
            
            BinaryExpression whereClause = BinaryExpression.and(statusOr, amountCheck);
            
            assertThat(whereClause.toSQL()).contains("OR").contains("AND").contains("amount");
        }

        @Test
        @DisplayName("Expression tree depth")
        void testExpressionTreeDepth() {
            // Build a tree 5 levels deep
            Expression level1 = Literal.of(1);
            Expression level2 = BinaryExpression.add(level1, Literal.of(2));
            Expression level3 = BinaryExpression.multiply(level2, Literal.of(3));
            Expression level4 = BinaryExpression.subtract(level3, Literal.of(4));
            Expression level5 = BinaryExpression.divide(level4, Literal.of(5));

            String sql = level5.toString(); // Use toString() which delegates to toSQL()
            assertThat(sql).contains("((((1 + 2) * 3) - 4) / 5)");
        }
    }

    // ==================== Edge Cases and Error Handling ====================

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesTests {

        @Test
        @DisplayName("Null left operand throws exception")
        void testNullLeftOperand() {
            assertThatThrownBy(() -> 
                new BinaryExpression(null, BinaryExpression.Operator.ADD, Literal.of(5))
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("Null right operand throws exception")
        void testNullRightOperand() {
            assertThatThrownBy(() -> 
                new BinaryExpression(Literal.of(5), BinaryExpression.Operator.ADD, null)
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("Null operator throws exception")
        void testNullOperator() {
            assertThatThrownBy(() -> 
                new BinaryExpression(Literal.of(5), null, Literal.of(10))
            ).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("Empty function name handling")
        void testEmptyFunctionName() {
            assertThatThrownBy(() ->
                FunctionCall.of("", Literal.of(42), IntegerType.get())
            ).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("functionName");
        }

        @Test
        @DisplayName("Expression toString works correctly")
        void testExpressionToString() {
            Expression expr = BinaryExpression.add(Literal.of(1), Literal.of(2));
            String str = expr.toString();
            assertThat(str).contains("+");
        }
    }
}
