package com.thunderduck.logical;

import com.thunderduck.expression.*;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Phase 2 logical plan operations: JOIN, Aggregate, UNION.
 *
 * <p>These tests verify that the SQL generation for advanced query operations
 * implemented in Week 3 Phase 2 produces valid, parseable SQL.
 */
@TestCategories.Integration
@TestCategories.Tier2
public class Phase2IntegrationTest extends TestBase {

    private SQLGenerator generator;
    private StructType customersSchema;
    private StructType ordersSchema;
    private TableScan customers;
    private TableScan orders;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();

        // Create schemas
        customersSchema = new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), false),
            new StructField("status", StringType.get(), true)
        ));

        ordersSchema = new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("customer_id", IntegerType.get(), false),
            new StructField("amount", DoubleType.get(), false),
            new StructField("order_date", StringType.get(), false)
        ));

        // Create test tables
        customers = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);
        orders = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);
    }

    @Nested
    @DisplayName("JOIN SQL Generation")
    class JoinSQLGeneration {

        @Test
        @DisplayName("INNER JOIN generates valid SQL")
        void testInnerJoinSQL() {
            // Given: Two tables with join condition
            Expression joinCondition = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );

            // When: Create INNER JOIN
            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCondition);
            String sql = generator.generate(join);

            // Then: Verify SQL structure
            logData("Generated SQL", sql);
            assertNotNull(sql, "SQL should not be null");
            assertFalse(sql.isEmpty(), "SQL should not be empty");
            assertTrue(sql.contains("INNER JOIN"), "Should contain INNER JOIN");
            assertTrue(sql.contains("ON"), "Should contain ON clause");
            assertTrue(sql.contains("read_parquet"), "Should use read_parquet for Parquet files");
        }

        @Test
        @DisplayName("LEFT OUTER JOIN generates valid SQL")
        void testLeftJoinSQL() {
            Expression condition = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );

            Join join = new Join(customers, orders, Join.JoinType.LEFT, condition);
            String sql = generator.generate(join);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("LEFT OUTER JOIN"), "Should contain LEFT OUTER JOIN");
        }

        @Test
        @DisplayName("CROSS JOIN generates valid SQL without ON clause")
        void testCrossJoinSQL() {
            Join join = new Join(customers, orders, Join.JoinType.CROSS, null);
            String sql = generator.generate(join);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("CROSS JOIN"), "Should contain CROSS JOIN");
            assertFalse(sql.contains(" ON "), "Should NOT contain ON clause");
        }

        @Test
        @DisplayName("All 7 join types generate distinct SQL")
        void testAllJoinTypes() {
            Expression condition = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );

            // Test all join types except CROSS (which doesn't have condition)
            Join innerJoin = new Join(customers, orders, Join.JoinType.INNER, condition);
            Join leftJoin = new Join(customers, orders, Join.JoinType.LEFT, condition);
            Join rightJoin = new Join(customers, orders, Join.JoinType.RIGHT, condition);
            Join fullJoin = new Join(customers, orders, Join.JoinType.FULL, condition);
            Join semiJoin = new Join(customers, orders, Join.JoinType.LEFT_SEMI, condition);
            Join antiJoin = new Join(customers, orders, Join.JoinType.LEFT_ANTI, condition);
            Join crossJoin = new Join(customers, orders, Join.JoinType.CROSS, null);

            String innerSQL = generator.generate(innerJoin);
            String leftSQL = generator.generate(leftJoin);
            String rightSQL = generator.generate(rightJoin);
            String fullSQL = generator.generate(fullJoin);
            String semiSQL = generator.generate(semiJoin);
            String antiSQL = generator.generate(antiJoin);
            String crossSQL = generator.generate(crossJoin);

            // Verify each produces different SQL
            assertNotEquals(innerSQL, leftSQL, "INNER and LEFT should produce different SQL");
            assertNotEquals(innerSQL, rightSQL, "INNER and RIGHT should produce different SQL");
            assertNotEquals(innerSQL, fullSQL, "INNER and FULL should produce different SQL");
            assertNotEquals(innerSQL, semiSQL, "INNER and SEMI should produce different SQL");
            assertNotEquals(innerSQL, antiSQL, "INNER and ANTI should produce different SQL");
            assertNotEquals(innerSQL, crossSQL, "INNER and CROSS should produce different SQL");

            logData("INNER JOIN SQL", innerSQL);
            logData("LEFT JOIN SQL", leftSQL);
            logData("CROSS JOIN SQL", crossSQL);
        }
    }

    @Nested
    @DisplayName("Aggregate SQL Generation")
    class AggregateSQLGeneration {

        @Test
        @DisplayName("Simple GROUP BY generates valid SQL")
        void testSimpleGroupBySQL() {
            List<Expression> groupingExprs = Collections.singletonList(
                new ColumnReference("status", StringType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "count")
            );

            Aggregate agg = new Aggregate(customers, groupingExprs, aggExprs);
            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            assertNotNull(sql, "SQL should not be null");
            assertTrue(sql.contains("SELECT"), "Should contain SELECT");
            assertTrue(sql.contains("GROUP BY"), "Should contain GROUP BY");
            // Use case-insensitive check for SQL function names
            assertTrue(sql.toLowerCase().contains("count"), "Should contain COUNT aggregate");
        }

        @Test
        @DisplayName("Multiple aggregates generate valid SQL")
        void testMultipleAggregatesSQL() {
            List<Expression> groupingExprs = Collections.singletonList(
                new ColumnReference("customer_id", IntegerType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Arrays.asList(
                new Aggregate.AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "count"),
                new Aggregate.AggregateExpression("SUM", new ColumnReference("amount", DoubleType.get()), "total"),
                new Aggregate.AggregateExpression("AVG", new ColumnReference("amount", DoubleType.get()), "avg_amount")
            );

            Aggregate agg = new Aggregate(orders, groupingExprs, aggExprs);
            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            // Use case-insensitive checks for SQL function names
            assertTrue(sql.toLowerCase().contains("count"), "Should contain COUNT");
            assertTrue(sql.toLowerCase().contains("sum"), "Should contain SUM");
            assertTrue(sql.toLowerCase().contains("avg"), "Should contain AVG");
            assertTrue(sql.contains("GROUP BY"), "Should contain GROUP BY");
        }

        @Test
        @DisplayName("Global aggregation (no GROUP BY) generates valid SQL")
        void testGlobalAggregationSQL() {
            List<Expression> groupingExprs = Collections.emptyList();
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "total")
            );

            Aggregate agg = new Aggregate(orders, groupingExprs, aggExprs);
            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            // Use case-insensitive check for SQL function names
            assertTrue(sql.toLowerCase().contains("count"), "Should contain COUNT");
            assertFalse(sql.contains("GROUP BY"), "Should NOT contain GROUP BY");
        }

        @Test
        @DisplayName("Aggregate with aliases generates AS clauses")
        void testAggregateAliasesSQL() {
            List<Expression> groupingExprs = Collections.singletonList(
                new ColumnReference("customer_id", IntegerType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("SUM", new ColumnReference("amount", DoubleType.get()), "total_amount")
            );

            Aggregate agg = new Aggregate(orders, groupingExprs, aggExprs);
            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("AS"), "Should contain AS keyword");
            assertTrue(sql.contains("total_amount"), "Should contain alias");
        }
    }

    @Nested
    @DisplayName("UNION SQL Generation")
    class UnionSQLGeneration {

        @Test
        @DisplayName("UNION ALL generates valid SQL")
        void testUnionAllSQL() {
            TableScan active = new TableScan("active.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan inactive = new TableScan("inactive.parquet", TableScan.TableFormat.PARQUET, customersSchema);

            Union union = new Union(active, inactive, true);
            String sql = generator.generate(union);

            logData("Generated SQL", sql);
            assertNotNull(sql, "SQL should not be null");
            assertTrue(sql.contains("UNION ALL"), "Should contain UNION ALL");
        }

        @Test
        @DisplayName("UNION (distinct) generates valid SQL")
        void testUnionDistinctSQL() {
            TableScan table1 = new TableScan("table1.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan table2 = new TableScan("table2.parquet", TableScan.TableFormat.PARQUET, customersSchema);

            Union union = new Union(table1, table2, false);
            String sql = generator.generate(union);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("UNION"), "Should contain UNION");
            // Verify it's just UNION, not UNION ALL
            assertFalse(sql.matches(".*UNION\\s+ALL.*"), "Should be UNION without ALL");
        }

        @Test
        @DisplayName("Chained UNIONs generate valid SQL")
        void testChainedUnionsSQL() {
            TableScan t1 = new TableScan("t1.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan t2 = new TableScan("t2.parquet", TableScan.TableFormat.PARQUET, customersSchema);
            TableScan t3 = new TableScan("t3.parquet", TableScan.TableFormat.PARQUET, customersSchema);

            Union union1 = new Union(t1, t2, true);
            Union union2 = new Union(union1, t3, true);
            String sql = generator.generate(union2);

            logData("Generated SQL", sql);
            int unionCount = sql.split("UNION ALL").length - 1;
            assertEquals(2, unionCount, "Should contain 2 UNION ALL operations");
        }
    }

    @Nested
    @DisplayName("Phase 2 Complex Integration")
    class ComplexIntegration {

        @Test
        @DisplayName("JOIN + Aggregate pipeline generates valid SQL")
        void testJoinAggregateSQL() {
            Expression joinCond = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );
            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCond);

            List<Expression> grouping = Collections.singletonList(
                new ColumnReference("name", StringType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("SUM", new ColumnReference("amount", DoubleType.get()), "total")
            );
            Aggregate agg = new Aggregate(join, grouping, aggExprs);

            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("INNER JOIN"), "Should contain JOIN");
            assertTrue(sql.contains("GROUP BY"), "Should contain GROUP BY");
            assertTrue(sql.toLowerCase().contains("sum"), "Should contain aggregate");
        }

        @Test
        @DisplayName("Filter + JOIN + Aggregate pipeline generates valid SQL")
        void testFilterJoinAggregateSQL() {
            Expression filter = new BinaryExpression(
                new ColumnReference("status", StringType.get()),
                BinaryExpression.Operator.EQUAL,
                new Literal("active", StringType.get())
            );
            Filter filteredCustomers = new Filter(customers, filter);

            Expression joinCond = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );
            Join join = new Join(filteredCustomers, orders, Join.JoinType.INNER, joinCond);

            List<Expression> grouping = Collections.singletonList(
                new ColumnReference("name", StringType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("COUNT", new Literal(1, IntegerType.get()), "order_count")
            );
            Aggregate agg = new Aggregate(join, grouping, aggExprs);

            String sql = generator.generate(agg);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("WHERE"), "Should contain WHERE filter");
            assertTrue(sql.contains("INNER JOIN"), "Should contain JOIN");
            assertTrue(sql.contains("GROUP BY"), "Should contain GROUP BY");
        }

        @Test
        @DisplayName("UNION of aggregates generates valid SQL")
        void testUnionOfAggregatesSQL() {
            List<Expression> grouping = Collections.singletonList(
                new ColumnReference("customer_id", IntegerType.get())
            );
            List<Aggregate.AggregateExpression> aggExprs = Collections.singletonList(
                new Aggregate.AggregateExpression("SUM", new ColumnReference("amount", DoubleType.get()), "total")
            );

            TableScan orders2023 = new TableScan("orders_2023.parquet", TableScan.TableFormat.PARQUET, ordersSchema);
            TableScan orders2024 = new TableScan("orders_2024.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            Aggregate agg2023 = new Aggregate(orders2023, grouping, aggExprs);
            Aggregate agg2024 = new Aggregate(orders2024, grouping, aggExprs);

            Union union = new Union(agg2023, agg2024, true);
            String sql = generator.generate(union);

            logData("Generated SQL", sql);
            assertTrue(sql.contains("UNION ALL"), "Should contain UNION ALL");
            int groupByCount = sql.split("GROUP BY").length - 1;
            assertTrue(groupByCount >= 2, "Should contain at least 2 GROUP BY clauses, found: " + groupByCount);
        }
    }

    @Nested
    @DisplayName("SQL Output Validation")
    class SQLValidation {

        @Test
        @DisplayName("Generated SQL contains no syntax errors (basic checks)")
        void testSQLSyntaxBasic() {
            Expression joinCond = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );
            Join join = new Join(customers, orders, Join.JoinType.INNER, joinCond);
            String sql = generator.generate(join);

            logData("Generated SQL", sql);

            // Basic syntax checks
            assertTrue(sql.contains("SELECT"), "Should contain SELECT");
            assertTrue(sql.contains("FROM"), "Should contain FROM");

            // Parentheses should be balanced
            long openParens = sql.chars().filter(ch -> ch == '(').count();
            long closeParens = sql.chars().filter(ch -> ch == ')').count();
            assertEquals(openParens, closeParens, "Parentheses should be balanced");

            // No double spaces (indicates formatting issues)
            assertFalse(sql.contains("  "), "Should not contain double spaces");
        }

        @Test
        @DisplayName("SQL Generator is stateless across multiple generations")
        void testGeneratorStateless() {
            // Create a fresh generator instance for this test to ensure proper isolation
            SQLGenerator freshGenerator = new SQLGenerator();

            Expression condition = new BinaryExpression(
                new ColumnReference("id", IntegerType.get()),
                BinaryExpression.Operator.EQUAL,
                new ColumnReference("customer_id", IntegerType.get())
            );

            // Generate SQL multiple times with the fresh generator
            Join join = new Join(customers, orders, Join.JoinType.INNER, condition);
            String sql1 = freshGenerator.generate(join);
            String sql2 = freshGenerator.generate(join);
            String sql3 = freshGenerator.generate(join);

            // All generations should be identical
            assertEquals(sql1, sql2, "First and second generation should match");
            assertEquals(sql2, sql3, "Second and third generation should match");

            logData("Generated SQL (consistent)", sql1);
        }
    }
}
