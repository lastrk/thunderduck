package com.thunderduck.expression.window;

import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
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
 * Comprehensive tests for named windows (Week 5 Phase 2 Task W5-6).
 *
 * <p>Tests 10 scenarios covering:
 * - Simple named window definitions
 * - Window composition (extending other windows)
 * - WindowClause with multiple windows
 * - WindowFunction using named windows
 * - Validation and error cases
 * - SQL generation correctness
 *
 * <p>Named windows improve query readability and allow window reuse.
 */
@DisplayName("Named Window Tests")
@Tag("window")
@Tag("tier1")
@TestCategories.Unit
public class NamedWindowTest extends TestBase {

    @Nested
    @DisplayName("NamedWindow Creation and SQL Generation")
    class NamedWindowCreation {

        @Test
        @DisplayName("Simple PARTITION BY named window generates correct SQL")
        void testSimplePartitionByWindow() {
            // Given: Named window with PARTITION BY department_id
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            NamedWindow window = NamedWindow.partitionBy("w1", Collections.singletonList(deptCol));

            // When: Generate SQL
            String sql = window.toSQL();

            // Then: Should generate w1 AS (PARTITION BY department_id)
            assertThat(sql).isEqualTo("w1 AS (PARTITION BY department_id)");
        }

        @Test
        @DisplayName("Named window with PARTITION BY and ORDER BY generates correct SQL")
        void testPartitionByOrderByWindow() {
            // Given: Named window with PARTITION BY and ORDER BY
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            NamedWindow window = NamedWindow.partitionByOrderBy(
                "w_dept_salary",
                Collections.singletonList(deptCol),
                Collections.singletonList(salaryDesc)
            );

            // When: Generate SQL
            String sql = window.toSQL();

            // Then: Should generate full window specification
            assertThat(sql).isEqualTo("w_dept_salary AS (PARTITION BY department_id ORDER BY salary DESC NULLS LAST)");
        }

        @Test
        @DisplayName("Named window with frame specification generates correct SQL")
        void testWindowWithFrame() {
            // Given: Named window with frame
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());
            Sort.SortOrder salaryAsc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFrame frame = WindowFrame.rowsBetween(1, 1);

            NamedWindow window = new NamedWindow(
                "w_moving",
                null,
                Collections.emptyList(),
                Collections.singletonList(salaryAsc),
                frame
            );

            // When: Generate SQL
            String sql = window.toSQL();

            // Then: Should include frame specification
            assertThat(sql).contains("w_moving AS (");
            assertThat(sql).contains("ORDER BY salary ASC");
            assertThat(sql).contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING");
        }

        @Test
        @DisplayName("Named window extending another window generates correct SQL")
        void testWindowExtension() {
            // Given: Named window extending another
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());
            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            NamedWindow extendingWindow = NamedWindow.extending(
                "w2",
                "w1",
                Collections.singletonList(salaryDesc)
            );

            // When: Generate SQL
            String sql = extendingWindow.toSQL();

            // Then: Should reference base window
            assertThat(sql).isEqualTo("w2 AS (w1 ORDER BY salary DESC NULLS LAST)");
        }
    }

    @Nested
    @DisplayName("WindowClause Tests")
    class WindowClauseTests {

        @Test
        @DisplayName("WindowClause with single window generates correct SQL")
        void testSingleWindowClause() {
            // Given: WindowClause with one window
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            NamedWindow window = NamedWindow.partitionBy("w", Collections.singletonList(deptCol));

            WindowClause clause = WindowClause.of(window);

            // When: Generate SQL
            String sql = clause.toSQL();

            // Then: Should generate WINDOW clause
            assertThat(sql).isEqualTo("WINDOW w AS (PARTITION BY department_id)");
        }

        @Test
        @DisplayName("WindowClause with multiple windows generates correct SQL")
        void testMultipleWindowsClause() {
            // Given: WindowClause with three windows
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            NamedWindow w1 = NamedWindow.partitionBy("w1", Collections.singletonList(deptCol));

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );
            NamedWindow w2 = NamedWindow.extending("w2", "w1", Collections.singletonList(salaryDesc));

            WindowFrame frame = WindowFrame.rowsBetween(1, 1);
            NamedWindow w3 = new NamedWindow("w3", "w2", Collections.emptyList(),
                                            Collections.emptyList(), frame);

            WindowClause clause = WindowClause.of(w1, w2, w3);

            // When: Generate SQL
            String sql = clause.toSQL();

            // Then: Should generate comma-separated windows
            assertThat(sql).startsWith("WINDOW ");
            assertThat(sql).contains("w1 AS (PARTITION BY department_id)");
            assertThat(sql).contains("w2 AS (w1 ORDER BY salary DESC NULLS LAST)");
            assertThat(sql).contains("w3 AS (w2 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)");
        }

        @Test
        @DisplayName("WindowClause validates window ordering")
        void testWindowOrderingValidation() {
            // Given: Windows defined in wrong order (w2 references w1 but w1 comes after)
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            // w2 references w1, but we'll try to define w2 first
            NamedWindow w2 = NamedWindow.extending("w2", "w1", Collections.singletonList(salaryDesc));
            NamedWindow w1 = NamedWindow.partitionBy("w1", Collections.singletonList(deptCol));

            // When/Then: Should throw exception about ordering
            assertThatThrownBy(() -> WindowClause.of(w2, w1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defined later");
        }

        @Test
        @DisplayName("WindowClause detects duplicate window names")
        void testDuplicateWindowNames() {
            // Given: Two windows with same name
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression regionCol = new ColumnReference("region", StringType.get());

            NamedWindow w1 = NamedWindow.partitionBy("w", Collections.singletonList(deptCol));
            NamedWindow w2 = NamedWindow.partitionBy("w", Collections.singletonList(regionCol));

            // When/Then: Should throw exception about duplicate
            assertThatThrownBy(() -> WindowClause.of(w1, w2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate window name");
        }
    }

    @Nested
    @DisplayName("WindowFunction with Named Windows")
    class WindowFunctionWithNamedWindows {

        @Test
        @DisplayName("WindowFunction referencing named window generates correct SQL")
        void testWindowFunctionWithNamedWindow() {
            // Given: RANK() OVER w
            WindowFunction rankFunc = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                "w"
            );

            // When: Generate SQL
            String sql = rankFunc.toSQL();

            // Then: Should generate CAST(RANK() OVER w AS INTEGER) for Spark parity
            assertThat(sql).isEqualTo("CAST(RANK() OVER w AS INTEGER)");
        }

        @Test
        @DisplayName("Multiple window functions referencing same named window")
        void testMultipleFunctionsWithSameWindow() {
            // Given: Multiple functions using same window
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                "w_salary"
            );

            WindowFunction rowNumber = new WindowFunction(
                "ROW_NUMBER",
                Collections.<Expression>emptyList(),
                "w_salary"
            );

            WindowFunction avg = new WindowFunction(
                "AVG",
                Collections.singletonList(salaryCol),
                "w_salary"
            );

            // When: Generate SQL
            String rankSQL = rank.toSQL();
            String rowNumSQL = rowNumber.toSQL();
            String avgSQL = avg.toSQL();

            // Then: All should reference same window (ranking functions get CAST for Spark parity)
            assertThat(rankSQL).isEqualTo("CAST(RANK() OVER w_salary AS INTEGER)");
            assertThat(rowNumSQL).isEqualTo("CAST(ROW_NUMBER() OVER w_salary AS INTEGER)");
            // AVG casts arg to DOUBLE in relaxed mode for precision
            assertThat(avgSQL).isEqualTo("AVG(CAST(salary AS DOUBLE)) OVER w_salary");
        }
    }

    @Nested
    @DisplayName("Validation and Edge Cases")
    class ValidationAndEdgeCases {

        @Test
        @DisplayName("Named window requires non-empty name")
        void testEmptyWindowName() {
            // Given: Attempt to create window with empty name
            Expression col = new ColumnReference("id", IntegerType.get());

            // When/Then: Should throw exception
            assertThatThrownBy(() ->
                NamedWindow.partitionBy("", Collections.singletonList(col))
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("non-empty name");
        }

        @Test
        @DisplayName("WindowClause requires at least one window")
        void testEmptyWindowClause() {
            // Given: Attempt to create empty WindowClause
            // When/Then: Should throw exception
            assertThatThrownBy(() ->
                new WindowClause(Collections.emptyList())
            ).isInstanceOf(IllegalArgumentException.class)
             .hasMessageContaining("at least one");
        }

        @Test
        @DisplayName("NamedWindow getters return correct values")
        void testNamedWindowGetters() {
            // Given: Named window with all components
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFrame frame = WindowFrame.unboundedPrecedingToCurrentRow();

            NamedWindow window = new NamedWindow(
                "test_window",
                null,
                Collections.singletonList(deptCol),
                Collections.singletonList(salaryDesc),
                frame
            );

            // When: Call getters
            // Then: Should return correct values
            assertThat(window.name()).isEqualTo("test_window");
            assertThat(window.baseWindow()).isNull();
            assertThat(window.partitionBy()).hasSize(1);
            assertThat(window.orderBy()).hasSize(1);
            assertThat(window.frame()).isNotNull();
        }
    }
}
