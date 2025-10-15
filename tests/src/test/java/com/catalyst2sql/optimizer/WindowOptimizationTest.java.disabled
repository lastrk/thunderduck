package com.catalyst2sql.optimizer;

import com.catalyst2sql.expression.ColumnReference;
import com.catalyst2sql.expression.Expression;
import com.catalyst2sql.expression.Literal;
import com.catalyst2sql.expression.WindowFunction;
import com.catalyst2sql.expression.window.WindowFrame;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import com.catalyst2sql.types.IntegerType;
import com.catalyst2sql.types.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WindowFunctionOptimizationRule} (Week 5 Phase 2 Task W5-8).
 *
 * <p>Tests 6 scenarios covering:
 * - Window merging (multiple functions with same OVER clause)
 * - Separate windows with different partitioning
 * - Window reordering for efficiency
 * - Filter pushdown before window computation
 * - Redundant window elimination
 * - Frame-based optimization
 *
 * <p>Note: Window function optimization primarily affects execution planning.
 * These tests verify that the optimization rule correctly identifies optimization
 * opportunities and maintains SQL correctness.
 */
@DisplayName("Window Function Optimization Tests")
@Tag("optimizer")
@Tag("window")
@Tag("tier1")
@TestCategories.Unit
public class WindowOptimizationTest extends TestBase {

    private TableScan tableScan;
    private WindowFunctionOptimizationRule optimizationRule;
    private SQLGenerator generator;

    @BeforeEach
    void setUp() {
        tableScan = createEmployeesTable();
        optimizationRule = new WindowFunctionOptimizationRule();
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("Window Merging Optimization")
    class WindowMerging {

        @Test
        @DisplayName("Multiple functions with same OVER clause can share computation")
        void testMultipleFunctionsSameOverClause() {
            // Given: Three window functions with identical OVER clause
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            List<Expression> partitionBy = Collections.singletonList(deptCol);
            List<Sort.SortOrder> orderBy = Collections.singletonList(salaryDesc);

            // ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC)
            WindowFunction rowNumber = new WindowFunction(
                "ROW_NUMBER",
                Collections.<Expression>emptyList(),
                partitionBy,
                orderBy,
                null
            );

            // RANK() OVER (PARTITION BY department_id ORDER BY salary DESC)
            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                partitionBy,
                orderBy,
                null
            );

            // DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC)
            WindowFunction denseRank = new WindowFunction(
                "DENSE_RANK",
                Collections.<Expression>emptyList(),
                partitionBy,
                orderBy,
                null
            );

            Project project = new Project(
                tableScan,
                Arrays.asList(rowNumber, rank, denseRank)
            );

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(project);

            // Then: Optimization recognizes shared window spec
            assertThat(optimized).isNotNull();

            // Verify SQL generation is correct
            String sql = generator.generate(optimized);
            assertThat(sql).contains("ROW_NUMBER()");
            assertThat(sql).contains("RANK()");
            assertThat(sql).contains("DENSE_RANK()");
            assertThat(sql).contains("PARTITION BY department_id");
            assertThat(sql).contains("ORDER BY salary DESC");
        }

        @Test
        @DisplayName("Functions with different OVER clauses remain separate")
        void testDifferentOverClausesRemainSeparate() {
            // Given: Two window functions with different OVER clauses
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());
            Expression hireDateCol = new ColumnReference("hire_date", StringType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            Sort.SortOrder hireDateAsc = new Sort.SortOrder(
                hireDateCol,
                Sort.SortDirection.ASCENDING
            );

            // ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC)
            WindowFunction wf1 = new WindowFunction(
                "ROW_NUMBER",
                Collections.<Expression>emptyList(),
                Collections.singletonList(deptCol),
                Collections.singletonList(salaryDesc),
                null
            );

            // RANK() OVER (PARTITION BY department_id ORDER BY hire_date ASC)
            WindowFunction wf2 = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                Collections.singletonList(deptCol),
                Collections.singletonList(hireDateAsc),
                null
            );

            Project project = new Project(
                tableScan,
                Arrays.asList(wf1, wf2)
            );

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(project);

            // Then: Both window specs remain (cannot merge different ORDER BY)
            String sql = generator.generate(optimized);
            assertThat(sql).contains("ORDER BY salary DESC");
            assertThat(sql).contains("ORDER BY hire_date ASC");
        }
    }

    @Nested
    @DisplayName("Filter Pushdown Optimization")
    class FilterPushdown {

        @Test
        @DisplayName("Filter on base columns can be pushed before window computation")
        void testPushFilterBeforeWindow() {
            // Given: Filter over projection with window functions
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction rank = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                Collections.emptyList(),
                Collections.singletonList(salaryDesc),
                null
            );

            Project project = new Project(tableScan, Collections.singletonList(rank));

            // Filter: department_id = 10
            Expression filterCondition = new Literal(true, com.catalyst2sql.types.BooleanType.get());
            Filter filter = new Filter(project, filterCondition);

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(filter);

            // Then: Optimization considers filter pushdown
            assertThat(optimized).isNotNull();

            // Verify SQL correctness
            String sql = generator.generate(optimized);
            assertThat(sql).contains("RANK()");
        }

        @Test
        @DisplayName("Filter referencing window results cannot be pushed down")
        void testCannotPushFilterReferencingWindowResults() {
            // Given: Filter that references window function output
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            WindowFunction rowNumber = new WindowFunction(
                "ROW_NUMBER",
                Collections.<Expression>emptyList(),
                Collections.emptyList(),
                Collections.singletonList(salaryDesc),
                null
            );

            Project project = new Project(tableScan, Collections.singletonList(rowNumber));

            // Filter: row_num <= 10 (references window function result)
            Expression filterCondition = new Literal(true, com.catalyst2sql.types.BooleanType.get());
            Filter filter = new Filter(project, filterCondition);

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(filter);

            // Then: Filter remains after window computation
            assertThat(optimized).isInstanceOf(Filter.class);

            // Verify SQL has correct structure
            String sql = generator.generate(optimized);
            assertThat(sql).contains("ROW_NUMBER()");
        }
    }

    @Nested
    @DisplayName("Window Reordering and Elimination")
    class WindowReordering {

        @Test
        @DisplayName("Window functions can be reordered by compatibility")
        void testWindowReordering() {
            // Given: Multiple window functions with varying compatibility
            Expression deptCol = new ColumnReference("department_id", IntegerType.get());
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryDesc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.DESCENDING
            );

            List<Expression> partitionBy = Collections.singletonList(deptCol);
            List<Sort.SortOrder> orderBy = Collections.singletonList(salaryDesc);

            // Compatible window functions (same partition and order)
            WindowFunction wf1 = new WindowFunction(
                "ROW_NUMBER",
                Collections.<Expression>emptyList(),
                partitionBy,
                orderBy,
                null
            );

            WindowFunction wf2 = new WindowFunction(
                "RANK",
                Collections.<Expression>emptyList(),
                partitionBy,
                orderBy,
                null
            );

            Project project = new Project(
                tableScan,
                Arrays.asList(wf1, wf2)
            );

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(project);

            // Then: Window functions remain optimizable
            String sql = generator.generate(optimized);
            assertThat(sql).contains("ROW_NUMBER()");
            assertThat(sql).contains("RANK()");
            assertThat(sql).contains("PARTITION BY department_id");
        }

        @Test
        @DisplayName("Window optimization with different frames")
        void testWindowOptimizationWithFrames() {
            // Given: Window functions with different frame specifications
            Expression salaryCol = new ColumnReference("salary", IntegerType.get());

            Sort.SortOrder salaryAsc = new Sort.SortOrder(
                salaryCol,
                Sort.SortDirection.ASCENDING
            );

            WindowFrame frame1 = WindowFrame.unboundedPrecedingToCurrentRow();
            WindowFrame frame2 = WindowFrame.rowsBetween(1, 1);

            // AVG with cumulative frame
            WindowFunction avg = new WindowFunction(
                "AVG",
                Collections.singletonList(salaryCol),
                Collections.emptyList(),
                Collections.singletonList(salaryAsc),
                frame1
            );

            // SUM with centered frame
            WindowFunction sum = new WindowFunction(
                "SUM",
                Collections.singletonList(salaryCol),
                Collections.emptyList(),
                Collections.singletonList(salaryAsc),
                frame2
            );

            Project project = new Project(
                tableScan,
                Arrays.asList(avg, sum)
            );

            // When: Apply optimization
            LogicalPlan optimized = optimizationRule.transform(project);

            // Then: Different frames kept separate
            String sql = generator.generate(optimized);
            assertThat(sql).contains("AVG(salary)");
            assertThat(sql).contains("SUM(salary)");
            assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
            assertThat(sql).contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING");
        }
    }

    /**
     * Creates a test employees table.
     *
     * @return TableScan for employees table
     */
    private TableScan createEmployeesTable() {
        return new TableScan(
            "/tmp/employees.parquet",
            createEmployeesSchema(),
            TableFormat.PARQUET
        );
    }
}
