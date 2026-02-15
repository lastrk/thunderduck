package com.thunderduck.expression.window;

import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.Literal;
import com.thunderduck.expression.WindowFunction;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for Window Frame specifications (Week 5 Phase 2).
 *
 * <p>Tests 15 scenarios covering:
 * - ROWS BETWEEN with various boundary types
 * - RANGE BETWEEN for value-based frames
 * - Frame interactions with PARTITION BY and ORDER BY
 * - Multiple window functions with different frames
 * - Frame validation and edge cases
 * - SQL generation correctness
 *
 * <p>Frame Types:
 * - ROWS: Physical offset-based frames
 * - RANGE: Logical value-based frames
 * - GROUPS: Group-based frames
 *
 * <p>Boundary Types:
 * - UNBOUNDED PRECEDING: Start of partition
 * - N PRECEDING: N rows/values before current
 * - CURRENT ROW: The current row
 * - N FOLLOWING: N rows/values after current
 * - UNBOUNDED FOLLOWING: End of partition
 */
@DisplayName("Window Frame Specification Tests")
@Tag("expression")
@Tag("window")
@Tag("tier1")
@TestCategories.Unit
public class WindowFrameTest extends TestBase {

    @Nested
    @DisplayName("ROWS BETWEEN Frame Specifications")
    class RowsBetweenFrames {

        @Test
        @DisplayName("1. ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW - cumulative frame")
        void testRowsUnboundedPrecedingToCurrentRow() {
            // Given: Window function with cumulative frame (default for many aggregates)
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // Create WindowFrame for: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(amountCol),
                Collections.emptyList(),  // No PARTITION BY
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = sumFunc.toSQL();

            // Then: Should generate cumulative sum frame
            assertThat(sql).contains("SUM(amount)");
            assertThat(sql).contains("OVER (");
            assertThat(sql).contains("ORDER BY date ASC");
            assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

            logData("Generated SQL", sql);
        }

        @Test
        @DisplayName("2. ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING - reverse cumulative")
        void testRowsCurrentRowToUnboundedFollowing() {
            // Given: Reverse cumulative frame (from current to end)
            Expression valueCol = new ColumnReference("value", IntegerType.get());
            Expression timestampCol = new ColumnReference("timestamp", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.currentRowToUnboundedFollowing();

            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(valueCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = sumFunc.toSQL();

            // Then: Should generate reverse cumulative frame
            assertThat(sql).contains("SUM(value)");
            assertThat(sql).contains("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
            assertThat(sql).doesNotContain("PARTITION BY");
        }

        @Test
        @DisplayName("3. ROWS BETWEEN 2 PRECEDING AND CURRENT ROW - moving window")
        void testRowsMovingWindow() {
            // Given: 3-row moving average window
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // Frame: 2 rows before + current row = 3 rows total
            WindowFrame frame = WindowFrame.rowsBetween(2, 0);

            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(priceCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = avgFunc.toSQL();

            // Then: Should generate moving average window (AVG casts arg to DOUBLE in relaxed mode)
            assertThat(sql).containsIgnoringCase("AVG(CAST(price AS DOUBLE))");
            assertThat(sql).contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW");

            logData("Moving average SQL", sql);
        }

        @Test
        @DisplayName("4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING - centered window")
        void testRowsCenteredWindow() {
            // Given: Centered 3-row window (1 before, current, 1 after)
            Expression valueCol = new ColumnReference("value", IntegerType.get());
            Expression indexCol = new ColumnReference("index", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                indexCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.rowsBetween(1, 1);

            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(valueCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = avgFunc.toSQL();

            // Then: Should generate centered moving average (AVG casts arg to DOUBLE in relaxed mode)
            assertThat(sql).containsIgnoringCase("AVG(CAST(value AS DOUBLE))");
            assertThat(sql).contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING");
        }

        @Test
        @DisplayName("5. ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING - entire partition")
        void testRowsEntirePartition() {
            // Given: Frame spanning entire partition (for FIRST_VALUE, LAST_VALUE)
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());
            Expression departmentCol = new ColumnReference("department", StringType.get());
            Expression salaryOrderCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                salaryOrderCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.entirePartition();

            WindowFunction lastValueFunc = new WindowFunction(
                "LAST_VALUE",
                Collections.singletonList(salaryCol),
                Collections.singletonList(departmentCol),  // PARTITION BY department
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = lastValueFunc.toSQL();

            // Then: Should span entire partition
            assertThat(sql).contains("LAST_VALUE(salary)");
            assertThat(sql).contains("PARTITION BY department");
            assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
        }
    }

    @Nested
    @DisplayName("RANGE BETWEEN Frame Specifications")
    class RangeBetweenFrames {

        @Test
        @DisplayName("6. RANGE BETWEEN - value-based frame")
        void testRangeFrame() {
            // Given: Range-based frame (value-based, not row-based)
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression timestampCol = new ColumnReference("timestamp", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // RANGE frame groups rows with same ORDER BY values
            WindowFrame frame = new WindowFrame(
                WindowFrame.FrameType.RANGE,
                FrameBoundary.UnboundedPreceding.getInstance(),
                FrameBoundary.CurrentRow.getInstance()
            );

            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = sumFunc.toSQL();

            // Then: Should generate RANGE frame
            assertThat(sql).contains("SUM(amount)");
            assertThat(sql).contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

            logData("RANGE frame SQL", sql);
        }
    }

    @Nested
    @DisplayName("Frames with PARTITION BY and ORDER BY")
    class FramesWithPartitionAndOrder {

        @Test
        @DisplayName("7. Frame with PARTITION BY - per-partition windowing")
        void testFrameWithPartitionBy() {
            // Given: Moving average per category
            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.rowsBetween(6, 0);  // 7-day moving average

            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(priceCol),
                Collections.singletonList(categoryCol),  // PARTITION BY category
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = avgFunc.toSQL();

            // Then: Should have both PARTITION BY and frame (AVG casts arg to DOUBLE in relaxed mode)
            assertThat(sql).containsIgnoringCase("AVG(CAST(price AS DOUBLE))");
            assertThat(sql).contains("PARTITION BY category");
            assertThat(sql).contains("ORDER BY date ASC");
            assertThat(sql).contains("ROWS BETWEEN 6 PRECEDING AND CURRENT ROW");
        }

        @Test
        @DisplayName("8. Frame with ORDER BY - frame requires ordering")
        void testFrameWithOrderBy() {
            // Given: Frame requires ORDER BY to be meaningful
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression timestampCol = new ColumnReference("timestamp", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = sumFunc.toSQL();

            // Then: Must have ORDER BY with frame
            assertThat(sql).contains("ORDER BY timestamp ASC");
            assertThat(sql).contains("ROWS BETWEEN");
        }
    }

    @Nested
    @DisplayName("Multiple Window Functions with Different Frames")
    class MultipleWindowsWithFrames {

        @Test
        @DisplayName("9. Multiple window functions with different frames")
        void testMultipleWindowFunctionsWithDifferentFrames() {
            // Given: Two window functions with different frame specifications
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // Cumulative sum
            WindowFrame cumulativeFrame = WindowFrame.unboundedPrecedingToCurrentRow();
            WindowFunction cumulativeSum = new WindowFunction(
                "SUM",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                cumulativeFrame
            );

            // Moving average (3 rows)
            WindowFrame movingFrame = WindowFrame.rowsBetween(2, 0);
            WindowFunction movingAvg = new WindowFunction(
                "AVG",
                Collections.singletonList(amountCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                movingFrame
            );

            // When: Generate SQL for both
            String sumSQL = cumulativeSum.toSQL();
            String avgSQL = movingAvg.toSQL();

            // Then: Each should have distinct frame specification
            assertThat(sumSQL).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            assertThat(avgSQL).contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW");
            assertThat(sumSQL).isNotEqualTo(avgSQL);

            logData("Cumulative sum", sumSQL);
            logData("Moving average", avgSQL);
        }
    }

    @Nested
    @DisplayName("Frame Validation")
    class FrameValidation {

        @Test
        @DisplayName("10. Frame validation - start must be before end")
        void testFrameStartBeforeEnd() {
            // Given/When/Then: Creating invalid frame should throw exception
            // CURRENT ROW comes before 1 PRECEDING - invalid!
            assertThatThrownBy(() -> {
                new WindowFrame(
                    WindowFrame.FrameType.ROWS,
                    FrameBoundary.CurrentRow.getInstance(),
                    new FrameBoundary.Preceding(1)
                );
            }).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("must come before");
        }

        @Test
        @DisplayName("10b. Frame validation - cannot start at UNBOUNDED FOLLOWING")
        void testFrameCannotStartAtUnboundedFollowing() {
            // Given/When/Then: Frame cannot start at UNBOUNDED FOLLOWING
            assertThatThrownBy(() -> {
                new WindowFrame(
                    WindowFrame.FrameType.ROWS,
                    FrameBoundary.UnboundedFollowing.getInstance(),
                    FrameBoundary.CurrentRow.getInstance()
                );
            }).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("cannot start at UNBOUNDED FOLLOWING");
        }

        @Test
        @DisplayName("10c. Frame validation - cannot end at UNBOUNDED PRECEDING")
        void testFrameCannotEndAtUnboundedPreceding() {
            // Given/When/Then: Frame cannot end at UNBOUNDED PRECEDING
            assertThatThrownBy(() -> {
                new WindowFrame(
                    WindowFrame.FrameType.ROWS,
                    FrameBoundary.CurrentRow.getInstance(),
                    FrameBoundary.UnboundedPreceding.getInstance()
                );
            }).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("cannot end at UNBOUNDED PRECEDING");
        }
    }

    @Nested
    @DisplayName("Frames with Aggregate Functions")
    class FramesWithAggregates {

        @Test
        @DisplayName("11. Frame with aggregate functions - SUM, AVG, COUNT")
        void testFrameWithAggregateFunctions() {
            // Given: Multiple aggregate functions with frame
            Expression quantityCol = new ColumnReference("quantity", IntegerType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.rowsBetween(3, 0);

            // SUM
            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(quantityCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // AVG
            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(quantityCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // COUNT
            WindowFunction countFunc = new WindowFunction(
                "COUNT",
                Collections.singletonList(quantityCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sumSQL = sumFunc.toSQL();
            String avgSQL = avgFunc.toSQL();
            String countSQL = countFunc.toSQL();

            // Then: All should have frame specification
            assertThat(sumSQL).contains("SUM(quantity)");
            assertThat(sumSQL).contains("ROWS BETWEEN 3 PRECEDING AND CURRENT ROW");

            assertThat(avgSQL).containsIgnoringCase("AVG(CAST(quantity AS DOUBLE))");
            assertThat(avgSQL).contains("ROWS BETWEEN 3 PRECEDING AND CURRENT ROW");

            assertThat(countSQL).contains("COUNT(quantity)");
            assertThat(countSQL).contains("ROWS BETWEEN 3 PRECEDING AND CURRENT ROW");
        }
    }

    @Nested
    @DisplayName("Frame with NULL Handling")
    class FrameWithNullHandling {

        @Test
        @DisplayName("12. Frame with NULL handling in ORDER BY")
        void testFrameWithNullHandling() {
            // Given: Frame with explicit NULL handling
            Expression valueCol = new ColumnReference("value", IntegerType.get());
            Expression orderCol = new ColumnReference("order_key", IntegerType.get());

            // ORDER BY with NULLS FIRST
            Sort.SortOrder sortOrderNullsFirst = new Sort.SortOrder(
                orderCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_FIRST
            );

            WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

            WindowFunction sumFuncNullsFirst = new WindowFunction(
                "SUM",
                Collections.singletonList(valueCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrderNullsFirst),
                frame
            );

            // ORDER BY with NULLS LAST
            Sort.SortOrder sortOrderNullsLast = new Sort.SortOrder(
                orderCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFunction sumFuncNullsLast = new WindowFunction(
                "SUM",
                Collections.singletonList(valueCol),
                Collections.emptyList(),
                Collections.singletonList(sortOrderNullsLast),
                frame
            );

            // When: Generate SQL
            String sqlNullsFirst = sumFuncNullsFirst.toSQL();
            String sqlNullsLast = sumFuncNullsLast.toSQL();

            // Then: NULL ordering should be specified
            assertThat(sqlNullsFirst).contains("ORDER BY order_key ASC NULLS FIRST");
            assertThat(sqlNullsLast).contains("ORDER BY order_key ASC NULLS LAST");
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("13. Empty partition with frame")
        void testEmptyPartitionWithFrame() {
            // Given: Frame over potentially empty partition
            Expression valueCol = new ColumnReference("value", IntegerType.get());
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            WindowFrame frame = WindowFrame.rowsBetween(2, 0);

            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(valueCol),
                Collections.singletonList(categoryCol),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = avgFunc.toSQL();

            // Then: Should handle empty partitions gracefully
            assertThat(sql).contains("PARTITION BY category");
            assertThat(sql).contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW");

            // Empty partition should return NULL for aggregate functions
        }

        @Test
        @DisplayName("14. Single row partition with frame")
        void testSingleRowPartitionWithFrame() {
            // Given: Frame specification on single-row partition
            Expression amountCol = new ColumnReference("amount", IntegerType.get());
            Expression userIdCol = new ColumnReference("user_id", IntegerType.get());
            Expression timestampCol = new ColumnReference("timestamp", StringType.get());

            Sort.SortOrder sortOrder = new Sort.SortOrder(
                timestampCol,
                Sort.SortDirection.ASCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // Large window, but partition might have only 1 row
            WindowFrame frame = WindowFrame.rowsBetween(10, 10);

            WindowFunction avgFunc = new WindowFunction(
                "AVG",
                Collections.singletonList(amountCol),
                Collections.singletonList(userIdCol),
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = avgFunc.toSQL();

            // Then: Should generate valid SQL even for large frame (AVG casts arg to DOUBLE in relaxed mode)
            assertThat(sql).containsIgnoringCase("AVG(CAST(amount AS DOUBLE))");
            assertThat(sql).contains("PARTITION BY user_id");
            assertThat(sql).contains("ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING");

            // Single-row partition: frame will only contain that one row
        }
    }

    @Nested
    @DisplayName("SQL Generation Correctness")
    class SqlGenerationCorrectness {

        @Test
        @DisplayName("15. Frame SQL generation correctness - comprehensive test")
        void testFrameSqlGenerationCorrectness() {
            // Given: Complex window function with all components
            Expression salesCol = new ColumnReference("sales", IntegerType.get());
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression productCol = new ColumnReference("product", StringType.get());
            Expression dateCol = new ColumnReference("date", StringType.get());

            // PARTITION BY multiple columns
            List<Expression> partitions = Arrays.asList(regionCol, productCol);

            // ORDER BY with direction and NULL handling
            Sort.SortOrder sortOrder = new Sort.SortOrder(
                dateCol,
                Sort.SortDirection.DESCENDING,
                Sort.NullOrdering.NULLS_LAST
            );

            // Moving window frame
            WindowFrame frame = WindowFrame.rowsBetween(5, 2);

            WindowFunction sumFunc = new WindowFunction(
                "SUM",
                Collections.singletonList(salesCol),
                partitions,
                Collections.singletonList(sortOrder),
                frame
            );

            // When: Generate SQL
            String sql = sumFunc.toSQL();

            // Then: Verify complete SQL structure
            logData("Complete window SQL", sql);

            // Function and argument
            assertThat(sql).startsWith("SUM(sales)");

            // OVER clause
            assertThat(sql).contains("OVER (");
            assertThat(sql).endsWith(")");

            // PARTITION BY
            assertThat(sql).contains("PARTITION BY region, product");

            // ORDER BY
            assertThat(sql).contains("ORDER BY date DESC NULLS LAST");

            // Frame specification
            assertThat(sql).contains("ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING");

            // Verify order of clauses (PARTITION BY, ORDER BY, frame)
            int partitionPos = sql.indexOf("PARTITION BY");
            int orderPos = sql.indexOf("ORDER BY");
            int framePos = sql.indexOf("ROWS BETWEEN");

            assertThat(partitionPos).isLessThan(orderPos);
            assertThat(orderPos).isLessThan(framePos);
        }
    }
}
