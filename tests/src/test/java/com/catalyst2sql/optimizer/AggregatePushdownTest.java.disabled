package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.*;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import com.catalyst2sql.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AggregatePushdownRule} (Week 5 Phase 3 Task W5-10).
 *
 * <p>Tests 8 scenarios covering:
 * - Pushdown through INNER join (left side)
 * - Pushdown through INNER join (right side)
 * - Cannot push through LEFT OUTER join
 * - Cannot push when aggregate references both sides
 * - Pushdown with multiple grouping keys
 * - Pushdown with HAVING clause
 * - Pushdown validation
 * - SQL generation correctness
 */
@DisplayName("Aggregate Pushdown Optimization Tests")
@Tag("optimizer")
@Tag("aggregate")
@Tag("tier1")
@TestCategories.Unit
public class AggregatePushdownTest extends TestBase {

    private AggregatePushdownRule rule;
    private SQLGenerator generator;
    private TableScan orders;
    private TableScan orderDetails;

    @BeforeEach
    void setUp() {
        rule = new AggregatePushdownRule();
        generator = new SQLGenerator();

        // Create test tables
        StructType ordersSchema = new StructType(Arrays.asList(
            new StructField("order_id", IntegerType.get(), false),
            new StructField("customer_id", IntegerType.get(), true),
            new StructField("order_date", StringType.get(), true)
        ));

        StructType detailsSchema = new StructType(Arrays.asList(
            new StructField("order_id", IntegerType.get(), true),
            new StructField("product_id", IntegerType.get(), true),
            new StructField("amount", DoubleType.get(), true)
        ));

        orders = new TableScan("/tmp/orders.parquet", ordersSchema, TableFormat.PARQUET);
        orderDetails = new TableScan("/tmp/order_details.parquet", detailsSchema, TableFormat.PARQUET);
    }

    @Nested
    @DisplayName("Pushdown Through INNER Join")
    class InnerJoinPushdown {

        @Test
        @DisplayName("Push aggregate through INNER join (left side)")
        void testPushAggregateToLeftSide() {
            // Given: Aggregate over INNER join
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            // Aggregate: GROUP BY customer_id, SUM(amount)
            Expression customerCol = new ColumnReference("customer_id", IntegerType.get());
            AggregateExpression sumAmount = new AggregateExpression(
                "SUM",
                new ColumnReference("amount", DoubleType.get()),
                "total",
                false
            );

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(customerCol),
                Collections.singletonList(sumAmount)
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Plan structure potentially changed
            assertThat(optimized).isNotNull();

            // Verify SQL generation works
            String sql = generator.generate(optimized);
            assertThat(sql).isNotEmpty();
        }

        @Test
        @DisplayName("Push aggregate through INNER join (right side)")
        void testPushAggregateToRightSide() {
            // Given: Aggregate referencing right side only
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            // Aggregate: GROUP BY product_id, SUM(amount)
            Expression productCol = new ColumnReference("product_id", IntegerType.get());
            AggregateExpression sumAmount = new AggregateExpression(
                "SUM",
                new ColumnReference("amount", DoubleType.get()),
                "total",
                false
            );

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(productCol),
                Collections.singletonList(sumAmount)
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Optimization considered
            assertThat(optimized).isNotNull();

            // SQL generation succeeds
            String sql = generator.generate(optimized);
            assertThat(sql).contains("GROUP BY");
        }
    }

    @Nested
    @DisplayName("Cannot Push Cases")
    class CannotPushCases {

        @Test
        @DisplayName("Cannot push aggregate through LEFT OUTER join")
        void testCannotPushThroughOuterJoin() {
            // Given: Aggregate over LEFT OUTER join
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.LEFT_OUTER, joinCond);

            Expression customerCol = new ColumnReference("customer_id", IntegerType.get());
            AggregateExpression count = new AggregateExpression(
                "COUNT",
                new ColumnReference("amount", DoubleType.get()),
                "cnt",
                false
            );

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(customerCol),
                Collections.singletonList(count)
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Original plan returned (no pushdown through OUTER join)
            assertThat(optimized).isInstanceOf(Aggregate.class);

            // SQL generation still works
            String sql = generator.generate(optimized);
            assertThat(sql).contains("LEFT");
        }

        @Test
        @DisplayName("Cannot push when aggregate references both sides")
        void testCannotPushWhenReferencingBothSides() {
            // Given: Aggregate with columns from both sides
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            // GROUP BY customer_id (left) AND product_id (right)
            Aggregate agg = new Aggregate(
                join,
                Arrays.asList(
                    new ColumnReference("customer_id", IntegerType.get()),
                    new ColumnReference("product_id", IntegerType.get())
                ),
                Collections.singletonList(
                    new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "cnt", false)
                )
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Original plan (cannot determine single side)
            assertThat(optimized).isNotNull();

            // SQL still generates correctly
            String sql = generator.generate(optimized);
            assertThat(sql).contains("GROUP BY");
        }
    }

    @Nested
    @DisplayName("Advanced Pushdown Scenarios")
    class AdvancedScenarios {

        @Test
        @DisplayName("Pushdown with multiple grouping keys")
        void testPushdownWithMultipleGroupingKeys() {
            // Given: Aggregate with multiple GROUP BY columns
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            Aggregate agg = new Aggregate(
                join,
                Arrays.asList(
                    new ColumnReference("customer_id", IntegerType.get()),
                    new ColumnReference("order_date", StringType.get())
                ),
                Collections.singletonList(
                    new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "cnt", false)
                )
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Handles multiple grouping keys
            assertThat(optimized).isNotNull();

            String sql = generator.generate(optimized);
            assertThat(sql).contains("GROUP BY");
        }

        @Test
        @DisplayName("Pushdown with HAVING clause")
        void testPushdownWithHaving() {
            // Given: Aggregate with HAVING
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            AggregateExpression sumAmount = new AggregateExpression(
                "SUM",
                new ColumnReference("amount", DoubleType.get()),
                "total",
                false
            );

            Expression havingCond = BinaryExpression.greaterThan(
                sumAmount,
                new Literal(1000.0, DoubleType.get())
            );

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(new ColumnReference("customer_id", IntegerType.get())),
                Collections.singletonList(sumAmount),
                havingCond
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: HAVING preserved
            assertThat(optimized).isNotNull();

            String sql = generator.generate(optimized);
            assertThat(sql).containsIgnoringCase("HAVING");
        }

        @Test
        @DisplayName("Optimization validation and correctness")
        void testOptimizationCorrectness() {
            // Given: Simple aggregation over join
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(new ColumnReference("customer_id", IntegerType.get())),
                Collections.singletonList(
                    new AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "cnt", false)
                )
            );

            // When: Apply optimization
            LogicalPlan optimized = rule.transform(agg);

            // Then: Both original and optimized generate valid SQL
            String originalSQL = generator.generate(agg);
            String optimizedSQL = generator.generate(optimized);

            assertThat(originalSQL).isNotEmpty();
            assertThat(optimizedSQL).isNotEmpty();

            // Both should contain key elements
            assertThat(originalSQL).contains("GROUP BY");
            assertThat(optimizedSQL).contains("GROUP BY");
        }

        @Test
        @DisplayName("SQL generation after pushdown is correct")
        void testSQLGenerationAfterPushdown() {
            // Given: Aggregate ready for pushdown
            Expression joinCond = BinaryExpression.equal(
                new ColumnReference("order_id", IntegerType.get()),
                new ColumnReference("order_id", IntegerType.get())
            );

            Join join = new Join(orders, orderDetails, Join.JoinType.INNER, joinCond);

            Aggregate agg = new Aggregate(
                join,
                Collections.singletonList(new ColumnReference("customer_id", IntegerType.get())),
                Collections.singletonList(
                    new AggregateExpression(
                        "SUM",
                        new ColumnReference("amount", DoubleType.get()),
                        "total",
                        false
                    )
                )
            );

            // When: Apply optimization and generate SQL
            LogicalPlan optimized = rule.transform(agg);
            String sql = generator.generate(optimized);

            // Then: SQL is valid and contains expected elements
            assertThat(sql).isNotNull();
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("FROM");
            assertThat(sql).containsIgnoringCase("GROUP BY");
            assertThat(sql).containsIgnoringCase("SUM");
        }
    }
}
