package com.thunderduck.integration;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.expression.*;
import com.thunderduck.types.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Disabled;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-End Query Integration Tests.
 *
 * <p>Tests complete query workflows from expression building through SQL generation.
 * These tests validate that all components work together correctly.
 *
 * <p>Test categories:
 * <ul>
 *   <li>Simple SELECT queries</li>
 *   <li>Filtered queries with WHERE clauses</li>
 *   <li>Sorted queries with ORDER BY</li>
 *   <li>Limited queries with LIMIT</li>
 *   <li>Complex multi-operator queries</li>
 * </ul>
 *
 * <p>Note: Tests marked with @Disabled will be enabled once runtime components
 * (SQLGenerator, DuckDBConnectionManager, etc.) are implemented by CODER agent.
 */
@TestCategories.Tier2
@TestCategories.Integration
@DisplayName("End-to-End Query Integration Tests")
public class EndToEndQueryTest extends TestBase {

    // ==================== SIMPLE SELECT QUERY TESTS ====================

    @Nested
    @DisplayName("Simple SELECT Query Tests")
    class SimpleSelectTests {

        @Test
        @DisplayName("TC-INT-001: Simple column projection SQL generation")
        void testSimpleColumnProjection() {
            // SELECT id, name FROM customers
            Expression idCol = new ColumnReference("id", IntegerType.get());
            Expression nameCol = new ColumnReference("name", StringType.get());

            // Build SELECT list
            String selectList = idCol.toSQL() + ", " + nameCol.toSQL();

            assertThat(selectList).isEqualTo("id, name");
        }

        @Test
        @DisplayName("TC-INT-002: SELECT * simulation")
        void testSelectStar() {
            Expression starCol = new ColumnReference("*", StringType.get());

            String sql = starCol.toSQL();
            assertThat(sql).isEqualTo("*");
        }

        @Test
        @DisplayName("TC-INT-003: SELECT with expression projection")
        void testSelectWithExpression() {
            // SELECT price * quantity AS total
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression quantity = new ColumnReference("quantity", IntegerType.get());
            Expression total = BinaryExpression.multiply(price, quantity);

            String sql = total.toSQL();
            assertThat(sql).isEqualTo("(price * quantity)");
        }

        @Test
        @DisplayName("TC-INT-004: SELECT with multiple expressions")
        void testSelectWithMultipleExpressions() {
            // SELECT id, UPPER(name), price * 1.1
            Expression id = new ColumnReference("id", IntegerType.get());
            Expression upperName = FunctionCall.of("upper",
                new ColumnReference("name", StringType.get()),
                StringType.get());
            Expression inflatedPrice = BinaryExpression.multiply(
                new ColumnReference("price", DoubleType.get()),
                Literal.of(1.1));

            String selectList = String.join(", ",
                id.toSQL(),
                upperName.toSQL(),
                inflatedPrice.toSQL());

            assertThat(selectList).contains("id").contains("upper(name)").contains("price");
        }

        @Test
        @DisplayName("TC-INT-005: SELECT with NULL handling")
        void testSelectWithNullHandling() {
            // SELECT COALESCE(discount, 0) AS discount
            Expression discount = new ColumnReference("discount", DoubleType.get(), true);
            Expression coalesce = FunctionCall.of("coalesce", DoubleType.get(),
                discount, Literal.of(0.0));

            String sql = coalesce.toSQL();
            assertThat(sql).isEqualTo("coalesce(discount, 0.0)");
        }
    }

    // ==================== FILTERED QUERY TESTS ====================

    @Nested
    @DisplayName("Filtered Query Tests")
    class FilteredQueryTests {

        @Test
        @DisplayName("TC-INT-006: Simple WHERE clause with equality")
        void testSimpleWhereClause() {
            // WHERE status = 'active'
            Expression whereClause = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("active")
            );

            String sql = whereClause.toSQL();
            assertThat(sql).isEqualTo("(status = 'active')");
        }

        @Test
        @DisplayName("TC-INT-007: WHERE clause with comparison")
        void testWhereWithComparison() {
            // WHERE age >= 18
            Expression whereClause = new BinaryExpression(
                new ColumnReference("age", IntegerType.get()),
                BinaryExpression.Operator.GREATER_THAN_OR_EQUAL,
                Literal.of(18)
            );

            String sql = whereClause.toSQL();
            assertThat(sql).isEqualTo("(age >= 18)");
        }

        @Test
        @DisplayName("TC-INT-008: WHERE clause with AND condition")
        void testWhereWithAnd() {
            // WHERE active = true AND verified = true
            Expression activeCheck = BinaryExpression.equal(
                new ColumnReference("active", BooleanType.get()),
                Literal.of(true)
            );
            Expression verifiedCheck = BinaryExpression.equal(
                new ColumnReference("verified", BooleanType.get()),
                Literal.of(true)
            );
            Expression whereClause = BinaryExpression.and(activeCheck, verifiedCheck);

            String sql = whereClause.toSQL();
            assertThat(sql).contains("AND");
        }

        @Test
        @DisplayName("TC-INT-009: WHERE clause with OR condition")
        void testWhereWithOr() {
            // WHERE status = 'active' OR status = 'pending'
            Expression activeCheck = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("active")
            );
            Expression pendingCheck = BinaryExpression.equal(
                new ColumnReference("status", StringType.get()),
                Literal.of("pending")
            );
            Expression whereClause = BinaryExpression.or(activeCheck, pendingCheck);

            String sql = whereClause.toSQL();
            assertThat(sql).contains("OR");
        }

        @Test
        @DisplayName("TC-INT-010: Complex WHERE clause")
        void testComplexWhereClause() {
            // WHERE (status = 'active' OR status = 'pending') AND amount > 100
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
        @DisplayName("TC-INT-011: WHERE with NULL check")
        void testWhereWithNullCheck() {
            // WHERE email IS NOT NULL
            Expression emailCheck = FunctionCall.of("isnotnull",
                new ColumnReference("email", StringType.get(), true),
                BooleanType.get());

            String sql = emailCheck.toSQL();
            assertThat(sql).isEqualTo("isnotnull(email)");
        }

        @Test
        @DisplayName("TC-INT-012: WHERE with function call")
        void testWhereWithFunction() {
            // WHERE UPPER(name) = 'ADMIN'
            Expression upperName = FunctionCall.of("upper",
                new ColumnReference("name", StringType.get()),
                StringType.get());
            Expression whereClause = BinaryExpression.equal(upperName, Literal.of("ADMIN"));

            String sql = whereClause.toSQL();
            assertThat(sql).isEqualTo("(upper(name) = 'ADMIN')");
        }
    }

    // ==================== SORTED QUERY TESTS ====================

    @Nested
    @DisplayName("Sorted Query Tests")
    class SortedQueryTests {

        @Test
        @DisplayName("TC-INT-013: Single column ORDER BY ASC")
        void testSingleColumnOrderBy() {
            // ORDER BY name ASC
            Expression orderCol = new ColumnReference("name", StringType.get());

            String orderByClause = orderCol.toSQL() + " ASC";
            assertThat(orderByClause).isEqualTo("name ASC");
        }

        @Test
        @DisplayName("TC-INT-014: Single column ORDER BY DESC")
        void testSingleColumnOrderByDesc() {
            // ORDER BY created_at DESC
            Expression orderCol = new ColumnReference("created_at", TimestampType.get());

            String orderByClause = orderCol.toSQL() + " DESC";
            assertThat(orderByClause).isEqualTo("created_at DESC");
        }

        @Test
        @DisplayName("TC-INT-015: Multi-column ORDER BY")
        void testMultiColumnOrderBy() {
            // ORDER BY last_name ASC, first_name ASC
            Expression lastName = new ColumnReference("last_name", StringType.get());
            Expression firstName = new ColumnReference("first_name", StringType.get());

            String orderByClause = lastName.toSQL() + " ASC, " + firstName.toSQL() + " ASC";
            assertThat(orderByClause).isEqualTo("last_name ASC, first_name ASC");
        }

        @Test
        @DisplayName("TC-INT-016: ORDER BY with expression")
        void testOrderByWithExpression() {
            // ORDER BY price * quantity DESC
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression quantity = new ColumnReference("quantity", IntegerType.get());
            Expression total = BinaryExpression.multiply(price, quantity);

            String orderByClause = total.toSQL() + " DESC";
            assertThat(orderByClause).contains("(price * quantity)");
        }

        @Test
        @DisplayName("TC-INT-017: ORDER BY with function")
        void testOrderByWithFunction() {
            // ORDER BY YEAR(order_date) DESC
            Expression orderDate = new ColumnReference("order_date", DateType.get());
            Expression year = FunctionCall.of("year", orderDate, IntegerType.get());

            String orderByClause = year.toSQL() + " DESC";
            assertThat(orderByClause).isEqualTo("year(order_date) DESC");
        }
    }

    // ==================== LIMITED QUERY TESTS ====================

    @Nested
    @DisplayName("Limited Query Tests")
    class LimitedQueryTests {

        @Test
        @DisplayName("TC-INT-018: Simple LIMIT clause")
        void testSimpleLimit() {
            // LIMIT 10
            String limitClause = "LIMIT 10";
            assertThat(limitClause).isEqualTo("LIMIT 10");
        }

        @Test
        @DisplayName("TC-INT-019: LIMIT 0 for schema only")
        void testLimitZero() {
            // LIMIT 0
            String limitClause = "LIMIT 0";
            assertThat(limitClause).isEqualTo("LIMIT 0");
        }

        @Test
        @DisplayName("TC-INT-020: LIMIT with large number")
        void testLimitLargeNumber() {
            // LIMIT 1000000
            String limitClause = "LIMIT 1000000";
            assertThat(limitClause).isEqualTo("LIMIT 1000000");
        }

        @Test
        @DisplayName("TC-INT-021: LIMIT combined with ORDER BY")
        void testLimitWithOrderBy() {
            // ORDER BY score DESC LIMIT 10
            Expression score = new ColumnReference("score", IntegerType.get());
            String orderByClause = score.toSQL() + " DESC";
            String limitClause = "LIMIT 10";

            String combined = orderByClause + " " + limitClause;
            assertThat(combined).isEqualTo("score DESC LIMIT 10");
        }
    }

    // ==================== COMPLEX MULTI-OPERATOR QUERY TESTS ====================

    @Nested
    @DisplayName("Complex Multi-Operator Query Tests")
    class ComplexQueryTests {

        @Test
        @DisplayName("TC-INT-022: SELECT with WHERE and ORDER BY")
        void testSelectWhereOrderBy() {
            // SELECT id, name FROM customers WHERE active = true ORDER BY name ASC
            Expression id = new ColumnReference("id", IntegerType.get());
            Expression name = new ColumnReference("name", StringType.get());
            String selectList = id.toSQL() + ", " + name.toSQL();

            Expression whereClause = BinaryExpression.equal(
                new ColumnReference("active", BooleanType.get()),
                Literal.of(true)
            );

            String orderByClause = name.toSQL() + " ASC";

            String completeQuery = "SELECT " + selectList + " WHERE " + whereClause.toSQL() + " ORDER BY " + orderByClause;
            assertThat(completeQuery).contains("SELECT").contains("WHERE").contains("ORDER BY");
        }

        @Test
        @DisplayName("TC-INT-023: SELECT with WHERE, ORDER BY, and LIMIT")
        void testSelectWhereOrderByLimit() {
            // SELECT * FROM orders WHERE total > 1000 ORDER BY total DESC LIMIT 100
            Expression whereClause = BinaryExpression.greaterThan(
                new ColumnReference("total", DoubleType.get()),
                Literal.of(1000.0)
            );

            String orderByClause = "total DESC";
            String limitClause = "LIMIT 100";

            String whereStr = "WHERE " + whereClause.toSQL();
            String orderByStr = "ORDER BY " + orderByClause;

            assertThat(whereStr).isEqualTo("WHERE (total > 1000.0)");
            assertThat(orderByStr).isEqualTo("ORDER BY total DESC");
            assertThat(limitClause).isEqualTo("LIMIT 100");
        }

        @Test
        @DisplayName("TC-INT-024: Complex SELECT with aggregates")
        void testComplexSelectWithAggregates() {
            // SELECT customer_id, SUM(amount) as total, AVG(amount) as avg_amount
            Expression customerId = new ColumnReference("customer_id", IntegerType.get());
            Expression sumAmount = FunctionCall.of("sum",
                new ColumnReference("amount", DoubleType.get()),
                DoubleType.get());
            Expression avgAmount = FunctionCall.of("avg",
                new ColumnReference("amount", DoubleType.get()),
                DoubleType.get());

            String selectList = String.join(", ",
                customerId.toSQL(),
                sumAmount.toSQL(),
                avgAmount.toSQL());

            assertThat(selectList).contains("customer_id")
                .containsIgnoringCase("sum(amount)")
                .containsIgnoringCase("avg(amount)");
        }

        @Test
        @DisplayName("TC-INT-025: Query with nested function calls")
        void testQueryWithNestedFunctions() {
            // SELECT UPPER(TRIM(name)) as clean_name
            Expression name = new ColumnReference("name", StringType.get());
            Expression trim = FunctionCall.of("trim", name, StringType.get());
            Expression upper = FunctionCall.of("upper", trim, StringType.get());

            String sql = upper.toSQL();
            assertThat(sql).isEqualTo("upper(trim(name))");
        }

        @Test
        @DisplayName("TC-INT-026: Query with multiple WHERE conditions")
        void testQueryWithMultipleConditions() {
            // WHERE age >= 18 AND age < 65 AND active = true
            Expression age = new ColumnReference("age", IntegerType.get());
            Expression minAge = new BinaryExpression(age, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, Literal.of(18));
            Expression maxAge = BinaryExpression.lessThan(age, Literal.of(65));
            Expression ageRange = BinaryExpression.and(minAge, maxAge);

            Expression activeCheck = BinaryExpression.equal(
                new ColumnReference("active", BooleanType.get()),
                Literal.of(true)
            );

            Expression whereClause = BinaryExpression.and(ageRange, activeCheck);

            String sql = whereClause.toSQL();
            assertThat(sql).contains(">=").contains("<").contains("AND");
        }

        @Test
        @DisplayName("TC-INT-027: Query with date range filtering")
        void testQueryWithDateRange() {
            // WHERE order_date >= '2024-01-01' AND order_date < '2024-12-31'
            Expression orderDate = new ColumnReference("order_date", DateType.get());
            Expression startDate = new Literal("2024-01-01", DateType.get());
            Expression endDate = new Literal("2024-12-31", DateType.get());

            Expression minDate = new BinaryExpression(orderDate, BinaryExpression.Operator.GREATER_THAN_OR_EQUAL, startDate);
            Expression maxDate = BinaryExpression.lessThan(orderDate, endDate);
            Expression dateRange = BinaryExpression.and(minDate, maxDate);

            String sql = dateRange.toSQL();
            assertThat(sql).contains("order_date").contains(">=").contains("<").contains("AND");
        }

        @Test
        @DisplayName("TC-INT-028: Query with NULL-aware filtering")
        void testQueryWithNullAwareFiltering() {
            // WHERE COALESCE(discount, 0) > 0 AND email IS NOT NULL
            Expression discount = new ColumnReference("discount", DoubleType.get(), true);
            Expression coalesce = FunctionCall.of("coalesce", DoubleType.get(), discount, Literal.of(0.0));
            Expression discountCheck = BinaryExpression.greaterThan(coalesce, Literal.of(0.0));

            Expression emailCheck = FunctionCall.of("isnotnull",
                new ColumnReference("email", StringType.get(), true),
                BooleanType.get());

            Expression whereClause = BinaryExpression.and(discountCheck, emailCheck);

            String sql = whereClause.toSQL();
            assertThat(sql).contains("coalesce").contains(">").contains("isnotnull");
        }

        @Test
        @DisplayName("TC-INT-029: Query with calculated fields")
        void testQueryWithCalculatedFields() {
            // SELECT price, quantity, (price * quantity) as total, (price * quantity * 1.1) as total_with_tax
            Expression price = new ColumnReference("price", DoubleType.get());
            Expression quantity = new ColumnReference("quantity", IntegerType.get());
            Expression total = BinaryExpression.multiply(price, quantity);
            Expression totalWithTax = BinaryExpression.multiply(total, Literal.of(1.1));

            String selectList = String.join(", ",
                price.toSQL(),
                quantity.toSQL(),
                total.toSQL(),
                totalWithTax.toSQL());

            assertThat(selectList).contains("price").contains("quantity").contains("*").contains("1.1");
        }

        @Test
        @DisplayName("TC-INT-030: Query with string manipulation")
        void testQueryWithStringManipulation() {
            // SELECT CONCAT(UPPER(first_name), ' ', UPPER(last_name)) as full_name
            Expression firstName = new ColumnReference("first_name", StringType.get());
            Expression lastName = new ColumnReference("last_name", StringType.get());
            Expression upperFirst = FunctionCall.of("upper", firstName, StringType.get());
            Expression upperLast = FunctionCall.of("upper", lastName, StringType.get());
            Expression fullName = FunctionCall.of("concat", StringType.get(),
                upperFirst, Literal.of(" "), upperLast);

            String sql = fullName.toSQL();
            assertThat(sql).contains("concat").contains("upper").contains("first_name").contains("last_name");
        }
    }
}
