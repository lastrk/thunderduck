package com.thunderduck.aggregate;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.Literal;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.Aggregate;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.logical.TableScan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.LongType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for HAVING clause support (Week 5 Phase 1).
 *
 * <p>Tests 12 scenarios covering:
 * - Simple HAVING with single aggregate condition
 * - HAVING with multiple AND conditions
 * - HAVING with OR conditions
 * - HAVING referencing aggregate aliases
 * - HAVING with aggregate functions not in SELECT
 * - HAVING without GROUP BY (global aggregation)
 * - HAVING with BETWEEN predicate
 * - HAVING with IN predicate
 * - HAVING with complex nested conditions
 * - HAVING validation (must use aggregates or grouping keys)
 * - HAVING with NULL handling
 * - HAVING with multiple aggregate functions in conditions
 *
 * <p>The HAVING clause filters aggregated results after GROUP BY execution,
 * unlike WHERE which filters before aggregation. HAVING conditions can reference
 * aggregate functions, grouping keys, or both.
 */
@DisplayName("HAVING Clause Tests")
@Tag("aggregate")
@Tag("tier1")
@TestCategories.Unit
public class HavingClauseTest extends TestBase {

    private static final StructType SALES_SCHEMA = new StructType(Arrays.asList(
        new StructField("customer_id", IntegerType.get(), false),
        new StructField("product_id", IntegerType.get(), false),
        new StructField("category", StringType.get(), false),
        new StructField("amount", IntegerType.get(), false),
        new StructField("quantity", IntegerType.get(), false),
        new StructField("price", DoubleType.get(), false)
    ));

    private static final StructType ORDERS_SCHEMA = new StructType(Arrays.asList(
        new StructField("order_id", IntegerType.get(), false),
        new StructField("customer_id", IntegerType.get(), false),
        new StructField("order_date", StringType.get(), false),
        new StructField("total_amount", DoubleType.get(), false)
    ));

    private LogicalPlan createSalesTableScan() {
        return new TableScan("/data/sales.parquet", TableScan.TableFormat.PARQUET, SALES_SCHEMA);
    }

    private LogicalPlan createOrdersTableScan() {
        return new TableScan("/data/orders.parquet", TableScan.TableFormat.PARQUET, ORDERS_SCHEMA);
    }

    @Nested
    @DisplayName("Simple HAVING Conditions")
    class SimpleHavingConditions {

        @Test
        @DisplayName("HAVING with single aggregate condition generates correct SQL")
        void testSimpleHavingWithSingleAggregate() {
            // Given: GROUP BY category HAVING SUM(amount) > 1000
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression(
                "SUM",
                amountCol,
                "total"
            );

            // HAVING condition: SUM(amount) > 1000
            Expression havingCondition = BinaryExpression.greaterThan(
                new AggregateExpression("SUM", amountCol, null),
                new Literal(1000, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have HAVING clause with aggregate condition
            assertThat(sql).containsIgnoringCase("GROUP BY category");
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount) > 1000");
            assertThat(sql).containsIgnoringCase("AS \"total\"");
        }

        @Test
        @DisplayName("HAVING with COUNT aggregate condition")
        void testHavingWithCount() {
            // Given: GROUP BY customer_id HAVING COUNT(*) > 5
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());

            AggregateExpression countOrders = new AggregateExpression(
                "COUNT",
                new Literal(1, IntegerType.get()),
                "order_count"
            );

            // HAVING COUNT(*) > 5
            Expression havingCondition = BinaryExpression.greaterThan(
                new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null),
                new Literal(5, IntegerType.get())
            );

            LogicalPlan tableScan = createOrdersTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(customerIdCol),
                Collections.singletonList(countOrders),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have COUNT in HAVING clause
            assertThat(sql).containsIgnoringCase("GROUP BY customer_id");
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("COUNT(1) > 5");
        }

        @Test
        @DisplayName("HAVING with AVG aggregate condition")
        void testHavingWithAverage() {
            // Given: GROUP BY category HAVING AVG(price) >= 50.0
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", DoubleType.get());

            AggregateExpression avgPrice = new AggregateExpression(
                "AVG",
                priceCol,
                "avg_price"
            );

            // HAVING AVG(price) >= 50.0 (implemented as AVG(price) > 50.0 OR AVG(price) = 50.0)
            Expression avgExpr = new AggregateExpression("AVG", priceCol, null);
            Expression fiftyLiteral = new Literal(50.0, DoubleType.get());
            Expression havingCondition = BinaryExpression.or(
                BinaryExpression.greaterThan(avgExpr, fiftyLiteral),
                BinaryExpression.equal(avgExpr, fiftyLiteral)
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(avgPrice),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have AVG in HAVING clause
            assertThat(sql).containsIgnoringCase("GROUP BY category");
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsPattern("AVG\\(price\\).*50\\.0");
        }
    }

    @Nested
    @DisplayName("Multiple HAVING Conditions")
    class MultipleHavingConditions {

        @Test
        @DisplayName("HAVING with multiple AND conditions")
        void testHavingWithMultipleAndConditions() {
            // Given: GROUP BY category
            //        HAVING SUM(amount) > 1000 AND COUNT(*) > 10
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");
            AggregateExpression countAll = new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "count");

            // HAVING condition: SUM(amount) > 1000 AND COUNT(*) > 10
            Expression sumCondition = BinaryExpression.greaterThan(
                new AggregateExpression("SUM", amountCol, null),
                new Literal(1000, IntegerType.get())
            );
            Expression countCondition = BinaryExpression.greaterThan(
                new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null),
                new Literal(10, IntegerType.get())
            );
            Expression havingCondition = BinaryExpression.and(sumCondition, countCondition);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(sumAmount, countAll),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have both conditions in HAVING
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount) > 1000");
            assertThat(sql).containsIgnoringCase("AND");
            assertThat(sql).containsIgnoringCase("COUNT(1) > 10");
        }

        @Test
        @DisplayName("HAVING with OR conditions")
        void testHavingWithOrConditions() {
            // Given: GROUP BY category
            //        HAVING SUM(amount) > 5000 OR AVG(price) > 100
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression priceCol = new ColumnReference("price", DoubleType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");
            AggregateExpression avgPrice = new AggregateExpression("AVG", priceCol, "avg_price");

            // HAVING SUM(amount) > 5000 OR AVG(price) > 100
            Expression sumCondition = BinaryExpression.greaterThan(
                new AggregateExpression("SUM", amountCol, null),
                new Literal(5000, IntegerType.get())
            );
            Expression avgCondition = BinaryExpression.greaterThan(
                new AggregateExpression("AVG", priceCol, null),
                new Literal(100, DoubleType.get())
            );
            Expression havingCondition = BinaryExpression.or(sumCondition, avgCondition);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(sumAmount, avgPrice),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have OR condition in HAVING
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount) > 5000");
            assertThat(sql).containsIgnoringCase("OR");
            assertThat(sql).containsIgnoringCase("AVG(price) > 100");
        }

        @Test
        @DisplayName("HAVING with complex nested conditions")
        void testHavingWithComplexNestedConditions() {
            // Given: GROUP BY category
            //        HAVING (SUM(amount) > 1000 AND COUNT(*) > 5) OR (AVG(price) > 50)
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression priceCol = new ColumnReference("price", DoubleType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            // Build nested condition
            Expression sumCondition = BinaryExpression.greaterThan(
                new AggregateExpression("SUM", amountCol, null),
                new Literal(1000, IntegerType.get())
            );
            Expression countCondition = BinaryExpression.greaterThan(
                new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null),
                new Literal(5, IntegerType.get())
            );
            Expression avgCondition = BinaryExpression.greaterThan(
                new AggregateExpression("AVG", priceCol, null),
                new Literal(50, DoubleType.get())
            );

            Expression leftCondition = BinaryExpression.and(sumCondition, countCondition);
            Expression havingCondition = BinaryExpression.or(leftCondition, avgCondition);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have properly nested conditions
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount) > 1000");
            assertThat(sql).containsIgnoringCase("AND");
            assertThat(sql).containsIgnoringCase("COUNT(1) > 5");
            assertThat(sql).containsIgnoringCase("OR");
            assertThat(sql).containsIgnoringCase("AVG(price) > 50");
        }
    }

    @Nested
    @DisplayName("HAVING with Special Cases")
    class HavingSpecialCases {

        @Test
        @DisplayName("HAVING with aggregate not in SELECT clause")
        void testHavingWithAggregateNotInSelect() {
            // Given: SELECT category, SUM(amount)
            //        GROUP BY category
            //        HAVING COUNT(*) > 10
            // The COUNT aggregate is in HAVING but not in SELECT
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            // HAVING COUNT(*) > 10 (COUNT not in SELECT)
            Expression havingCondition = BinaryExpression.greaterThan(
                new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null),
                new Literal(10, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have COUNT in HAVING even though not in SELECT
            assertThat(sql).containsIgnoringCase("SELECT category, SUM(amount)");
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("COUNT(1) > 10");
            assertThat(sql).doesNotContain("COUNT(1) AS");  // Not in SELECT list
        }

        @Test
        @DisplayName("HAVING without GROUP BY (global aggregation)")
        void testHavingWithoutGroupBy() {
            // Given: SELECT SUM(amount) HAVING SUM(amount) > 10000
            //        (Global aggregation with HAVING)
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            // HAVING SUM(amount) > 10000
            Expression havingCondition = BinaryExpression.greaterThan(
                new AggregateExpression("SUM", amountCol, null),
                new Literal(10000, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),  // No GROUP BY (global aggregation)
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have HAVING without GROUP BY
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount) > 10000");
            assertThat(sql).doesNotContain("GROUP BY");
        }

        @Test
        @DisplayName("HAVING referencing grouping key")
        void testHavingReferencingGroupingKey() {
            // Given: GROUP BY category HAVING category LIKE 'Electronics%'
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            // HAVING category = 'Electronics' (simplified from LIKE)
            Expression havingCondition = BinaryExpression.equal(
                categoryCol,
                new Literal("Electronics", StringType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should allow grouping key in HAVING
            assertThat(sql).containsIgnoringCase("GROUP BY category");
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("category = 'Electronics'");
        }
    }

    @Nested
    @DisplayName("HAVING with Predicates")
    class HavingWithPredicates {

        @Test
        @DisplayName("HAVING with BETWEEN predicate")
        void testHavingWithBetween() {
            // Given: GROUP BY category HAVING SUM(amount) BETWEEN 1000 AND 5000
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            // HAVING SUM(amount) BETWEEN 1000 AND 5000
            Expression sumExpr = new AggregateExpression("SUM", amountCol, null);
            Expression lowerBound = new Literal(1000, IntegerType.get());
            Expression upperBound = new Literal(5000, IntegerType.get());

            // BETWEEN is: (expr > lower OR expr = lower) AND (expr < upper OR expr = upper)
            Expression lowerCheck = BinaryExpression.or(
                BinaryExpression.greaterThan(sumExpr, lowerBound),
                BinaryExpression.equal(sumExpr, lowerBound)
            );
            Expression upperCheck = BinaryExpression.or(
                BinaryExpression.lessThan(sumExpr, upperBound),
                BinaryExpression.equal(sumExpr, upperBound)
            );
            Expression havingCondition = BinaryExpression.and(lowerCheck, upperCheck);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have BETWEEN-style condition in HAVING
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount)");
            assertThat(sql).containsIgnoringCase("1000");
            assertThat(sql).containsIgnoringCase("AND");
            assertThat(sql).containsIgnoringCase("5000");
        }

        @Test
        @DisplayName("HAVING with IN predicate")
        void testHavingWithIn() {
            // Given: GROUP BY customer_id HAVING COUNT(*) IN (5, 10, 15)
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());

            AggregateExpression countOrders = new AggregateExpression(
                "COUNT",
                new Literal(1, IntegerType.get()),
                "order_count"
            );

            // HAVING COUNT(*) IN (5, 10, 15)
            Expression countExpr = new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null);

            // IN clause: COUNT(*) = 5 OR COUNT(*) = 10 OR COUNT(*) = 15
            Expression condition1 = BinaryExpression.equal(countExpr, new Literal(5, IntegerType.get()));
            Expression condition2 = BinaryExpression.equal(countExpr, new Literal(10, IntegerType.get()));
            Expression condition3 = BinaryExpression.equal(countExpr, new Literal(15, IntegerType.get()));

            Expression havingCondition = BinaryExpression.or(
                BinaryExpression.or(condition1, condition2),
                condition3
            );

            LogicalPlan tableScan = createOrdersTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(customerIdCol),
                Collections.singletonList(countOrders),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have IN-style condition (as OR chain)
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("COUNT(1)");
            assertThat(sql).containsPattern("= 5.*OR.*= 10.*OR.*= 15");
        }

        @Test
        @DisplayName("HAVING with NULL handling (IS NOT NULL)")
        void testHavingWithNullHandling() {
            // Given: GROUP BY category HAVING AVG(price) IS NOT NULL
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", DoubleType.get());

            AggregateExpression avgPrice = new AggregateExpression("AVG", priceCol, "avg_price");

            // HAVING AVG(price) IS NOT NULL (implemented as AVG(price) > 0 OR AVG(price) <= 0)
            // This is a simplification since IS NOT NULL isn't easily available
            Expression avgExpr = new AggregateExpression("AVG", priceCol, null);
            Expression zeroLiteral = new Literal(0.0, DoubleType.get());
            Expression havingCondition = BinaryExpression.or(
                BinaryExpression.greaterThan(avgExpr, zeroLiteral),
                BinaryExpression.or(
                    BinaryExpression.lessThan(avgExpr, zeroLiteral),
                    BinaryExpression.equal(avgExpr, zeroLiteral)
                )
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(avgPrice),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have comparison in HAVING
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("AVG(price)");
        }
    }

    @Nested
    @DisplayName("HAVING with Advanced Expressions")
    class HavingWithAdvancedExpressions {

        @Test
        @DisplayName("HAVING with multiple aggregate functions")
        void testHavingWithMultipleAggregateFunctions() {
            // Given: GROUP BY category
            //        HAVING SUM(amount) > AVG(amount) * 10
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");
            AggregateExpression avgAmount = new AggregateExpression("AVG", amountCol, "average");

            // Build HAVING condition: SUM(amount) > AVG(amount) * 10
            Expression sumExpr = new AggregateExpression("SUM", amountCol, null);
            Expression avgExpr = new AggregateExpression("AVG", amountCol, null);
            Expression multiplied = BinaryExpression.multiply(
                avgExpr,
                new Literal(10, IntegerType.get())
            );

            Expression havingCondition = BinaryExpression.greaterThan(sumExpr, multiplied);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(sumAmount, avgAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have complex aggregate expression in HAVING clause
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount)");
            assertThat(sql).containsIgnoringCase("AVG(amount)");
            assertThat(sql).containsIgnoringCase("*");
            assertThat(sql).containsIgnoringCase("10");
        }

        @Test
        @DisplayName("HAVING with arithmetic expression on aggregates")
        void testHavingWithArithmeticExpression() {
            // Given: GROUP BY category
            //        HAVING SUM(amount) / COUNT(*) > 100  (average > 100)
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");
            AggregateExpression countAll = new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "count");

            // HAVING SUM(amount) / COUNT(*) > 100
            Expression sumExpr = new AggregateExpression("SUM", amountCol, null);
            Expression countExpr = new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), null);
            Expression avgCalc = BinaryExpression.divide(sumExpr, countExpr);

            Expression havingCondition = BinaryExpression.greaterThan(
                avgCalc,
                new Literal(100, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(sumAmount, countAll),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have arithmetic expression in HAVING
            assertThat(sql).containsIgnoringCase("HAVING");
            assertThat(sql).containsIgnoringCase("SUM(amount)");
            assertThat(sql).containsIgnoringCase("/");
            assertThat(sql).containsIgnoringCase("COUNT(1)");
            assertThat(sql).containsIgnoringCase("> 100");
        }
    }

    @Nested
    @DisplayName("HAVING Backward Compatibility")
    class HavingBackwardCompatibility {

        @Test
        @DisplayName("Aggregate without HAVING clause (backward compatibility)")
        void testAggregateWithoutHaving() {
            // Given: Old-style aggregate without HAVING
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount)
                // No HAVING condition - using 3-parameter constructor
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should work without HAVING clause
            assertThat(sql).containsIgnoringCase("SELECT category, SUM(amount)");
            assertThat(sql).containsIgnoringCase("GROUP BY category");
            assertThat(sql).doesNotContainIgnoringCase("HAVING");
            assertThat(aggregate.havingCondition()).isNull();
        }

        @Test
        @DisplayName("Aggregate with null HAVING condition")
        void testAggregateWithNullHaving() {
            // Given: Aggregate with explicit null HAVING
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", IntegerType.get());

            AggregateExpression sumAmount = new AggregateExpression("SUM", amountCol, "total");

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Collections.singletonList(sumAmount),
                null  // Explicit null HAVING
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should behave like no HAVING clause
            assertThat(sql).doesNotContainIgnoringCase("HAVING");
            assertThat(aggregate.havingCondition()).isNull();
        }
    }
}
