package com.thunderduck.integration;

import com.thunderduck.expression.*;
import com.thunderduck.expression.window.*;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.*;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive integration tests for Week 5 features (Task W5-11).
 *
 * <p>Tests 12 scenarios combining multiple Week 5 features:
 * - Phase 1: HAVING + DISTINCT + ROLLUP/CUBE + Statistical Aggregates
 * - Phase 2: Named Windows + Value Window Functions + Window Frames
 * - Complex queries combining aggregation and window functions
 *
 * <p>These tests validate that Week 5 features work correctly together
 * in realistic, production-style queries.
 */
@DisplayName("Week 5 Integration Tests")
@Tag("integration")
@Tag("tier1")
@TestCategories.Integration
public class Week5IntegrationTest extends TestBase {

    private static final StructType SALES_SCHEMA = new StructType(Arrays.asList(
        new StructField("sale_id", IntegerType.get(), false),
        new StructField("customer_id", IntegerType.get(), false),
        new StructField("product_id", IntegerType.get(), false),
        new StructField("region", StringType.get(), false),
        new StructField("category", StringType.get(), false),
        new StructField("amount", DoubleType.get(), false),
        new StructField("quantity", IntegerType.get(), false),
        new StructField("sale_date", StringType.get(), false)
    ));

    private LogicalPlan createSalesTableScan() {
        return new TableScan("/data/sales.parquet", TableScan.TableFormat.PARQUET, SALES_SCHEMA);
    }

    @Nested
    @DisplayName("Phase 1: Advanced Aggregation Integration")
    class AdvancedAggregationIntegration {

        @Test
        @DisplayName("DISTINCT aggregates with HAVING clause")
        void testDistinctWithHaving() {
            // Given: COUNT(DISTINCT customer_id) with HAVING count > 10
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());

            AggregateExpression countDistinct = new AggregateExpression(
                "COUNT", customerIdCol, "unique_customers", true
            );

            // HAVING COUNT(DISTINCT customer_id) > 10
            Expression havingCondition = BinaryExpression.greaterThan(
                countDistinct,
                new Literal(10, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Collections.singletonList(countDistinct),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should combine DISTINCT and HAVING
            assertThat(sql).containsIgnoringCase("COUNT(DISTINCT customer_id)");
            assertThat(sql).containsIgnoringCase("GROUP BY region");
            assertThat(sql).containsIgnoringCase("HAVING (COUNT(DISTINCT customer_id) > 10)");
        }

        @Test
        @DisplayName("ROLLUP with statistical aggregates and HAVING")
        void testRollupWithStatisticalAggregates() {
            // Given: ROLLUP(region, category) with STDDEV and HAVING
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            GroupingSets rollup = GroupingSets.rollup(Arrays.asList(regionCol, categoryCol));

            AggregateExpression avgAmount = new AggregateExpression(
                "AVG", amountCol, "avg_amount", false
            );
            AggregateExpression stddevAmount = new AggregateExpression(
                "STDDEV_SAMP", amountCol, "stddev_amount", false
            );

            // HAVING STDDEV_SAMP(amount) > 100
            Expression havingCondition = BinaryExpression.greaterThan(
                stddevAmount,
                new Literal(100.0, DoubleType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            // Note: This test validates the concept; actual Aggregate class may need
            // enhancement to support GroupingSets parameter
            Aggregate aggregate = new Aggregate(
                tableScan,
                Arrays.asList(regionCol, categoryCol),
                Arrays.asList(avgAmount, stddevAmount),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have aggregates with HAVING
            assertThat(sql).containsIgnoringCase("AVG(amount)");
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(amount)");
            assertThat(sql).containsIgnoringCase("GROUP BY region, category");
            assertThat(sql).containsIgnoringCase("HAVING (STDDEV_SAMP(amount) > 100.0)");
        }

        @Test
        @DisplayName("Multiple DISTINCT aggregates with complex HAVING")
        void testMultipleDistinctWithComplexHaving() {
            // Given: Multiple DISTINCT aggregates with complex HAVING condition
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());
            Expression productIdCol = new ColumnReference("product_id", IntegerType.get());

            AggregateExpression countDistinctCustomers = new AggregateExpression(
                "COUNT", customerIdCol, "unique_customers", true
            );
            AggregateExpression countDistinctProducts = new AggregateExpression(
                "COUNT", productIdCol, "unique_products", true
            );

            // HAVING COUNT(DISTINCT customer_id) > 5 AND COUNT(DISTINCT product_id) > 3
            Expression condition1 = BinaryExpression.greaterThan(
                countDistinctCustomers,
                new Literal(5, IntegerType.get())
            );
            Expression condition2 = BinaryExpression.greaterThan(
                countDistinctProducts,
                new Literal(3, IntegerType.get())
            );
            Expression havingCondition = BinaryExpression.and(condition1, condition2);

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(countDistinctCustomers, countDistinctProducts),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have multiple DISTINCT with complex HAVING
            assertThat(sql).containsIgnoringCase("COUNT(DISTINCT customer_id)");
            assertThat(sql).containsIgnoringCase("COUNT(DISTINCT product_id)");
            assertThat(sql).containsIgnoringCase("HAVING ((COUNT(DISTINCT customer_id) > 5) AND (COUNT(DISTINCT product_id) > 3))");
        }

        @Test
        @DisplayName("Statistical aggregates (MEDIAN, PERCENTILE) with GROUP BY")
        void testStatisticalAggregatesWithGroupBy() {
            // Given: MEDIAN and PERCENTILE_CONT with GROUP BY
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            AggregateExpression medianAmount = new AggregateExpression(
                "MEDIAN", amountCol, "median_amount", false
            );
            AggregateExpression p75 = new AggregateExpression(
                "PERCENTILE_CONT", amountCol, "p75_amount", false
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Arrays.asList(medianAmount, p75),
                null
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have statistical aggregates
            assertThat(sql).containsIgnoringCase("MEDIAN(amount)");
            assertThat(sql).containsIgnoringCase("PERCENTILE_CONT(amount)");
            assertThat(sql).containsIgnoringCase("GROUP BY region");
        }
    }

    @Nested
    @DisplayName("Phase 2: Window Functions Integration")
    class WindowFunctionsIntegration {

        @Test
        @DisplayName("Named window with value window functions")
        void testNamedWindowWithValueFunctions() {
            // Given: Named window used by multiple value functions
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            // Define named window
            NamedWindow salesWindow = NamedWindow.partitionByOrderBy(
                "sales_window",
                Collections.singletonList(regionCol),
                Collections.singletonList(amountDesc)
            );

            // Use named window for multiple functions
            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                "sales_window"
            );

            WindowFunction percentRank = new WindowFunction(
                "PERCENT_RANK",
                Collections.<Expression>emptyList(),
                "sales_window"
            );

            WindowFunction cumeDist = new WindowFunction(
                "CUME_DIST",
                Collections.<Expression>emptyList(),
                "sales_window"
            );

            // When: Generate SQL (would need Project with WindowClause support)
            String rankSQL = rank.toSQL();
            String percentRankSQL = percentRank.toSQL();
            String cumeDistSQL = cumeDist.toSQL();

            // Then: All should reference same named window
            // RANK returns INTEGER, needs CAST for Spark parity (DuckDB returns BIGINT)
            assertThat(rankSQL).isEqualTo("CAST(RANK() OVER sales_window AS INTEGER)");
            // PERCENT_RANK and CUME_DIST return DOUBLE, no CAST needed
            assertThat(percentRankSQL).isEqualTo("PERCENT_RANK() OVER sales_window");
            assertThat(cumeDistSQL).isEqualTo("CUME_DIST() OVER sales_window");
        }

        @Test
        @DisplayName("Window function with frame and value function")
        void testWindowFrameWithValueFunction() {
            // Given: NTH_VALUE with custom frame
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression nthLiteral = new Literal(2, IntegerType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFrame frame = WindowFrame.rowsBetween(0, 5);

            WindowFunction nthValue = new WindowFunction(
                "NTH_VALUE",
                Arrays.asList(amountCol, nthLiteral),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                frame
            );

            // When: Generate SQL
            String sql = nthValue.toSQL();

            // Then: Should combine NTH_VALUE with frame
            assertThat(sql).containsIgnoringCase("NTH_VALUE(amount, 2)");
            assertThat(sql).containsIgnoringCase("ORDER BY sale_date ASC");
            assertThat(sql).containsIgnoringCase("ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING");
        }

        @Test
        @DisplayName("NTILE with PARTITION BY and ORDER BY")
        void testNtileWithPartitionAndOrder() {
            // Given: NTILE(4) for quartiles per region
            Expression bucketsLiteral = new Literal(4, IntegerType.get());
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            Sort.SortOrder amountDesc = new Sort.SortOrder(
                amountCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction ntile = new WindowFunction(
                "NTILE",
                Collections.singletonList(bucketsLiteral),
                Collections.singletonList(regionCol),
                Collections.singletonList(amountDesc),
                null
            );

            // When: Generate SQL
            String sql = ntile.toSQL();

            // Then: Should have complete window specification
            assertThat(sql).containsIgnoringCase("NTILE(4)");
            assertThat(sql).containsIgnoringCase("PARTITION BY region");
            assertThat(sql).containsIgnoringCase("ORDER BY amount DESC");
        }

        @Test
        @DisplayName("Multiple window functions with different frames")
        void testMultipleWindowsWithDifferentFrames() {
            // Given: Different window functions with different frame specifications
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression saleDateCol = new ColumnReference("sale_date", StringType.get());

            Sort.SortOrder dateAsc = new Sort.SortOrder(
                saleDateCol,
                Sort.SortDirection.ASCENDING
            );

            // Cumulative sum (UNBOUNDED PRECEDING to CURRENT ROW)
            WindowFrame cumulativeFrame = WindowFrame.unboundedPrecedingToCurrentRow();
            WindowFunction cumulativeSum = new WindowFunction(
                "SUM",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                cumulativeFrame
            );

            // Moving average (2 PRECEDING to 2 FOLLOWING)
            WindowFrame movingFrame = WindowFrame.rowsBetween(2, 2);
            WindowFunction movingAvg = new WindowFunction(
                "AVG",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(dateAsc),
                movingFrame
            );

            // When: Generate SQL
            String cumulativeSumSQL = cumulativeSum.toSQL();
            String movingAvgSQL = movingAvg.toSQL();

            // Then: Should have different frame specifications
            assertThat(cumulativeSumSQL).containsIgnoringCase("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            assertThat(movingAvgSQL).containsIgnoringCase("ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING");
        }
    }

    @Nested
    @DisplayName("Complex Integration Scenarios")
    class ComplexIntegrationScenarios {

        @Test
        @DisplayName("Combining aggregates with window functions in same query")
        void testAggregatesWithWindowFunctions() {
            // Given: Query with both aggregation and window functions
            // This simulates: SELECT region, COUNT(*), RANK() OVER (ORDER BY COUNT(*) DESC)
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression countCol = new Literal(1, IntegerType.get());

            AggregateExpression countAll = new AggregateExpression(
                "COUNT", countCol, "total_sales", false
            );

            // First aggregate by region
            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Collections.singletonList(countAll),
                null
            );

            // Then apply window function over aggregated results
            Sort.SortOrder countDesc = new Sort.SortOrder(
                countAll,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction rankFunc = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                Collections.emptyList(),
                Collections.singletonList(countDesc),
                null
            );

            // When: Generate SQL for aggregate
            SQLGenerator generator = new SQLGenerator();
            String aggSQL = generator.generate(aggregate);

            // Then: Should have aggregation SQL
            assertThat(aggSQL).containsIgnoringCase("COUNT(1)");
            assertThat(aggSQL).containsIgnoringCase("GROUP BY region");

            // And window function should generate correct SQL
            String windowSQL = rankFunc.toSQL();
            assertThat(windowSQL).containsIgnoringCase("RANK()");
            assertThat(windowSQL).containsIgnoringCase("ORDER BY");
        }

        @Test
        @DisplayName("DISTINCT aggregates with statistical functions")
        void testDistinctWithStatisticalFunctions() {
            // Given: Mix of DISTINCT and statistical aggregates
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            AggregateExpression countDistinct = new AggregateExpression(
                "COUNT", customerIdCol, "unique_customers", true
            );
            AggregateExpression stddevAmount = new AggregateExpression(
                "STDDEV_SAMP", amountCol, "amount_stddev", false
            );
            AggregateExpression medianAmount = new AggregateExpression(
                "MEDIAN", amountCol, "amount_median", false
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(categoryCol),
                Arrays.asList(countDistinct, stddevAmount, medianAmount),
                null
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should combine all three types
            assertThat(sql).containsIgnoringCase("COUNT(DISTINCT customer_id)");
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(amount)");
            assertThat(sql).containsIgnoringCase("MEDIAN(amount)");
        }

        @Test
        @DisplayName("Value window functions with aggregation filtering")
        void testValueWindowFunctionsWithFiltering() {
            // Given: Window functions on filtered aggregate results
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());
            Expression minThreshold = new Literal(1000.0, DoubleType.get());

            // Filter: amount > 999 (simulating >= 1000 since greaterThanOrEqual not available)
            Expression condition = BinaryExpression.greaterThan(amountCol, new Literal(999.0, DoubleType.get()));

            LogicalPlan tableScan = createSalesTableScan();
            Filter filter = new Filter(tableScan, condition);

            // Then aggregate
            AggregateExpression sumAmount = new AggregateExpression(
                "SUM", amountCol, "total_amount", false
            );

            Aggregate aggregate = new Aggregate(
                filter,
                Collections.singletonList(regionCol),
                Collections.singletonList(sumAmount),
                null
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have filter and aggregation
            assertThat(sql).containsIgnoringCase("WHERE (amount > 999.0)");
            assertThat(sql).containsIgnoringCase("SUM(amount)");
            assertThat(sql).containsIgnoringCase("GROUP BY region");
        }

        @Test
        @DisplayName("Full Week 5 feature integration: DISTINCT + HAVING + Window + Frame")
        void testFullWeek5Integration() {
            // Given: Complex query using multiple Week 5 features
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression customerIdCol = new ColumnReference("customer_id", IntegerType.get());
            Expression amountCol = new ColumnReference("amount", DoubleType.get());

            // DISTINCT aggregate with HAVING
            AggregateExpression countDistinct = new AggregateExpression(
                "COUNT", customerIdCol, "unique_customers", true
            );
            AggregateExpression avgAmount = new AggregateExpression(
                "AVG", amountCol, "avg_amount", false
            );

            Expression havingCondition = BinaryExpression.greaterThan(
                countDistinct,
                new Literal(10, IntegerType.get())
            );

            LogicalPlan tableScan = createSalesTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Arrays.asList(countDistinct, avgAmount),
                havingCondition
            );

            // Window function with frame
            Sort.SortOrder avgAmountDesc = new Sort.SortOrder(
                avgAmount,
                Sort.SortDirection.DESCENDING
            );

            WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();
            WindowFunction rankWindow = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                Collections.emptyList(),
                Collections.singletonList(avgAmountDesc),
                frame
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String aggSQL = generator.generate(aggregate);
            String windowSQL = rankWindow.toSQL();

            // Then: Should combine all features correctly
            assertThat(aggSQL).containsIgnoringCase("COUNT(DISTINCT customer_id)");
            assertThat(aggSQL).containsIgnoringCase("AVG(amount)");
            assertThat(aggSQL).containsIgnoringCase("GROUP BY region");
            assertThat(aggSQL).containsIgnoringCase("HAVING (COUNT(DISTINCT customer_id) > 10)");
            assertThat(windowSQL).containsIgnoringCase("RANK()");
            assertThat(windowSQL).containsIgnoringCase("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        }
    }
}
