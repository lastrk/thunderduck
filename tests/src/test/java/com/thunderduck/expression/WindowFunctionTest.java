package com.thunderduck.expression;

import com.thunderduck.logical.Sort;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for Window Function expressions (Week 4).
 *
 * <p>Tests 15 scenarios covering:
 * - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
 * - Offset functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)
 * - Window frames (ROWS BETWEEN, RANGE BETWEEN)
 * - Complex scenarios (multiple functions, with aggregates, with filters)
 */
@DisplayName("Window Function Tests")
@Tag("expression")
@Tag("tier1")
@TestCategories.Unit
public class WindowFunctionTest extends TestBase {

    @Nested
    @DisplayName("Ranking Functions")
    class RankingFunctions {

        @Test
        @DisplayName("ROW_NUMBER with PARTITION BY and ORDER BY")
        void testRowNumberWithPartitionAndOrder() {
            // Given: Window function with partitioning and ordering
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                priceCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction windowFunc = new WindowFunction(
                "ROW_NUMBER",
                Collections.emptyList(),  // No arguments
                Collections.singletonList(categoryCol),  // PARTITION BY category
                Collections.singletonList(sortOrder)     // ORDER BY price DESC NULLS LAST
            );

            // When: Generate SQL
            String sql = windowFunc.toSQL();

            // Then: Should generate proper window function SQL with CAST for Spark compatibility
            // DuckDB returns BIGINT for ranking functions, Spark returns INTEGER
            assertThat(sql).isEqualTo("CAST(ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).contains("ROW_NUMBER()");
            assertThat(sql).contains("OVER");
            assertThat(sql).contains("PARTITION BY category");
            assertThat(sql).contains("ORDER BY price DESC NULLS LAST");
        }

        @Test
        @DisplayName("RANK with ties handling")
        void testRankWithTies() {
            // Given: RANK function with ORDER BY
            Expression scoreCol = new ColumnReference("score", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                scoreCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.emptyList(),  // No arguments
                Collections.emptyList(),  // No PARTITION BY
                Collections.singletonList(sortOrder)  // ORDER BY score DESC
            );

            // When: Generate SQL
            String sql = rank.toSQL();

            // Then: Should generate RANK function with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(RANK() OVER (ORDER BY score DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).contains("RANK()");
            assertThat(sql).doesNotContain("PARTITION BY");
        }

        @Test
        @DisplayName("DENSE_RANK with multiple partitions")
        void testDenseRankWithMultiplePartitions() {
            // Given: DENSE_RANK with multiple partition columns
            Expression departmentCol = new ColumnReference("department", StringType.get());
            Expression teamCol = new ColumnReference("team", StringType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            List<Expression> partitions = Arrays.asList(departmentCol, teamCol);

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction denseRank = new WindowFunction(
                "DENSE_RANK",
                Collections.emptyList(),
                partitions,
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = denseRank.toSQL();

            // Then: Should have multiple partition columns with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(DENSE_RANK() OVER (PARTITION BY department, team ORDER BY salary DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).contains("PARTITION BY department, team");
        }

        @Test
        @DisplayName("NTILE for bucket distribution")
        void testNTileBuckets() {
            // Given: NTILE(4) for quartiles
            Expression bucketCount = new Literal(4, IntegerType.get());
            Expression scoreCol = new ColumnReference("score", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                scoreCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction ntile = new WindowFunction(
                "NTILE",
                Collections.singletonList(bucketCount),  // Argument: number of buckets
                Collections.emptyList(),  // No PARTITION BY
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = ntile.toSQL();

            // Then: Should include bucket count argument with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(NTILE(4) OVER (ORDER BY score DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).contains("NTILE(4)");
        }
    }

    @Nested
    @DisplayName("Offset Functions")
    class OffsetFunctions {

        @Test
        @DisplayName("LAG with offset and default value")
        void testLagWithOffsetAndDefault() {
            // Given: LAG(amount, 1, 0)
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression offset = new Literal(1, IntegerType.get());
            Expression defaultValue = new Literal(0, IntegerType.get());

            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction lag = new WindowFunction(
                "LAG",
                Arrays.asList(amountCol, offset, defaultValue),
                Collections.emptyList(),  // No PARTITION BY
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = lag.toSQL();

            // Then: Should include all three arguments
            assertThat(sql).isEqualTo("LAG(amount, 1, 0) OVER (ORDER BY date ASC NULLS LAST)");
            assertThat(sql).contains("LAG(amount, 1, 0)");
        }

        @Test
        @DisplayName("LEAD with offset and missing values")
        void testLeadWithOffset() {
            // Given: LEAD(value, 2)
            Expression valueCol = new ColumnReference("value", IntegerType.get());
            Expression offset = new Literal(2, IntegerType.get());

            Expression timestampCol = new ColumnReference("timestamp", StringType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_FIRST
            );

            WindowFunction lead = new WindowFunction(
                "LEAD",
                Arrays.asList(valueCol, offset),
                Collections.singletonList(categoryCol),  // PARTITION BY category
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = lead.toSQL();

            // Then: Should generate LEAD with offset
            assertThat(sql).isEqualTo("LEAD(value, 2) OVER (PARTITION BY category ORDER BY timestamp ASC NULLS FIRST)");
            assertThat(sql).contains("LEAD(value, 2)");
        }

        @Test
        @DisplayName("FIRST_VALUE with frame specification")
        void testFirstValueWithFrame() {
            // Given: FIRST_VALUE(price) - frame is typically implicit
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction firstValue = new WindowFunction(
                "FIRST_VALUE",
                Collections.singletonList(priceCol),
                Collections.singletonList(categoryCol),
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = firstValue.toSQL();

            // Then: Should generate FIRST_VALUE
            assertThat(sql).isEqualTo("FIRST_VALUE(price) OVER (PARTITION BY category ORDER BY date ASC NULLS LAST)");
            assertThat(sql).contains("FIRST_VALUE(price)");
        }

        @Test
        @DisplayName("LAST_VALUE with frame specification")
        void testLastValueWithFrame() {
            // Given: LAST_VALUE(status)
            Expression statusCol = new ColumnReference("status", StringType.get());
            Expression userIdCol = new ColumnReference("user_id", IntegerType.get());
            Expression timestampCol = new ColumnReference("timestamp", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction lastValue = new WindowFunction(
                "LAST_VALUE",
                Collections.singletonList(statusCol),
                Collections.singletonList(userIdCol),
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = lastValue.toSQL();

            // Then: Should generate LAST_VALUE
            assertThat(sql).isEqualTo("LAST_VALUE(status) OVER (PARTITION BY user_id ORDER BY timestamp DESC NULLS LAST)");
            assertThat(sql).contains("LAST_VALUE(status)");
        }
    }

    @Nested
    @DisplayName("Window Frames")
    class WindowFrames {

        @Test
        @DisplayName("Empty OVER clause for entire result set")
        void testEmptyOverClause() {
            // Given: ROW_NUMBER() OVER () - no partitioning or ordering
            WindowFunction rowNumber = new WindowFunction(
                "ROW_NUMBER",
                Collections.emptyList(),
                Collections.emptyList(),  // No PARTITION BY
                Collections.emptyList()   // No ORDER BY
            );

            // When: Generate SQL
            String sql = rowNumber.toSQL();

            // Then: Should have empty OVER clause with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(ROW_NUMBER() OVER () AS INTEGER)");
            assertThat(sql).doesNotContain("PARTITION BY");
            assertThat(sql).doesNotContain("ORDER BY");
        }

        @Test
        @DisplayName("Window with only PARTITION BY, no ORDER BY")
        void testPartitionByOnly() {
            // Given: COUNT(*) OVER (PARTITION BY category)
            Expression categoryCol = new ColumnReference("category", StringType.get());

            WindowFunction count = new WindowFunction(
                "COUNT",
                Collections.singletonList(new Literal("*", StringType.get())),
                Collections.singletonList(categoryCol),  // PARTITION BY
                Collections.emptyList()  // No ORDER BY
            );

            // When: Generate SQL
            String sql = count.toSQL();

            // Then: Should have PARTITION BY but no ORDER BY
            assertThat(sql).isEqualTo("COUNT(*) OVER (PARTITION BY category)");
            assertThat(sql).contains("PARTITION BY category");
            assertThat(sql).doesNotContain("ORDER BY");
        }

        @Test
        @DisplayName("Window with only ORDER BY, no PARTITION BY")
        void testOrderByOnly() {
            // Given: RANK() OVER (ORDER BY score DESC)
            Expression scoreCol = new ColumnReference("score", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                scoreCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.emptyList(),
                Collections.emptyList(),  // No PARTITION BY
                Collections.singletonList(sortOrder)  // ORDER BY
            );

            // When: Generate SQL
            String sql = rank.toSQL();

            // Then: Should have ORDER BY but no PARTITION BY with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(RANK() OVER (ORDER BY score DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).doesNotContain("PARTITION BY");
            assertThat(sql).contains("ORDER BY");
        }

        @Test
        @DisplayName("Multiple ORDER BY columns with mixed directions")
        void testMultipleOrderByColumns() {
            // Given: RANK() OVER (ORDER BY department ASC, salary DESC)
            Expression deptCol = new ColumnReference("department", StringType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder order1 = new Sort.SortOrder(
                deptCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_FIRST
            );

            Sort.SortOrder order2 = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList(order1, order2)
            );

            // When: Generate SQL
            String sql = rank.toSQL();

            // Then: Should have multiple ORDER BY columns with CAST for Spark compatibility
            assertThat(sql).isEqualTo("CAST(RANK() OVER (ORDER BY department ASC NULLS FIRST, salary DESC NULLS LAST) AS INTEGER)");
            assertThat(sql).contains("department ASC NULLS FIRST");
            assertThat(sql).contains("salary DESC NULLS LAST");
        }
    }

    @Nested
    @DisplayName("Complex Scenarios")
    class ComplexScenarios {

        @Test
        @DisplayName("Window function with complex expression argument")
        void testComplexExpressionArgument() {
            // Given: LAG(price * quantity, 1)
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Expression quantityCol = new ColumnReference("quantity", IntegerType.get());
            Expression productExpr = BinaryExpression.multiply(priceCol, quantityCol);

            Expression offset = new Literal(1, IntegerType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction lag = new WindowFunction(
                "LAG",
                Arrays.asList(productExpr, offset),
                Collections.emptyList(),
                Collections.singletonList(sortOrder)
            );

            // When: Generate SQL
            String sql = lag.toSQL();

            // Then: Should handle complex expression
            assertThat(sql).contains("LAG((price * quantity), 1)");
            assertThat(sql).contains("OVER (ORDER BY date ASC NULLS LAST)");
        }

        @Test
        @DisplayName("Window function getters return correct values")
        void testWindowFunctionGetters() {
            // Given: A window function with all components
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Expression arg1 = new Literal(1, IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                priceCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction windowFunc = new WindowFunction(
                "LAG",
                Collections.singletonList(arg1),
                Collections.singletonList(categoryCol),
                Collections.singletonList(sortOrder)
            );

            // When: Query attributes
            String function = windowFunc.function();
            List<Expression> args = windowFunc.arguments();
            List<Expression> partitions = windowFunc.partitionBy();
            List<Sort.SortOrder> orders = windowFunc.orderBy();

            // Then: Should return correct values
            assertThat(function).isEqualTo("LAG");
            assertThat(args).hasSize(1);
            assertThat(partitions).hasSize(1);
            assertThat(orders).hasSize(1);

            // And: Lists should be unmodifiable
            assertThat(args).isInstanceOf(Collections.unmodifiableList(args).getClass());
            assertThat(partitions).isInstanceOf(Collections.unmodifiableList(partitions).getClass());
            assertThat(orders).isInstanceOf(Collections.unmodifiableList(orders).getClass());
        }

        @Test
        @DisplayName("Ranking functions are non-nullable")
        void testRankingFunctionNullable() {
            // Given: A ranking window function
            WindowFunction rowNumber = new WindowFunction(
                "ROW_NUMBER",
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList()
            );

            // When: Check nullable
            boolean nullable = rowNumber.nullable();

            // Then: Ranking functions always return non-null values
            assertThat(nullable).isFalse();
        }
    }
}
