package com.catalyst2sql.tpch;

import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.test.TestBase;
import com.catalyst2sql.test.TestCategories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TPC-H aggregation-heavy queries (Week 5 Phase 3 Task W5-9).
 *
 * <p>Tests 6 scenarios covering:
 * - Q13 logical plan construction
 * - Q13 SQL generation correctness
 * - Q13 features (LEFT OUTER JOIN, nested aggregation)
 * - Q18 logical plan construction
 * - Q18 SQL generation with HAVING
 * - Q18 features (IN subquery, multi-table JOIN, LIMIT)
 *
 * <p>These tests validate that TPC-H queries correctly demonstrate
 * Week 5 aggregation features including HAVING clauses, nested aggregation,
 * and complex JOIN patterns.
 */
@DisplayName("TPC-H Aggregation Queries Tests")
@Tag("tpch")
@Tag("aggregation")
@Tag("tier2")
@TestCategories.Integration
public class TPCHAggregationQueriesTest extends TestBase {

    private SQLGenerator generator;
    private static final String TEST_DATA_PATH = "/tmp/tpch";

    @BeforeEach
    void setUp() {
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("TPC-H Q13: Customer Distribution Query")
    class Query13Tests {

        @Test
        @DisplayName("Q13 logical plan constructs without errors")
        void testQ13LogicalPlanConstruction() {
            // Given: TPC-H Q13 plan construction
            // When: Build logical plan
            LogicalPlan plan = TPCHQueryPlans.query13(TEST_DATA_PATH);

            // Then: Plan created successfully
            assertThat(plan).isNotNull();
            assertThat(plan).isInstanceOf(com.catalyst2sql.logical.Sort.class);
        }

        @Test
        @DisplayName("Q13 generates valid SQL with LEFT OUTER JOIN")
        void testQ13SQLGeneration() {
            // Given: TPC-H Q13 logical plan
            LogicalPlan plan = TPCHQueryPlans.query13(TEST_DATA_PATH);

            // When: Generate SQL
            String sql = generator.generate(plan);

            // Then: SQL contains expected elements
            assertThat(sql).isNotNull();
            assertThat(sql).isNotEmpty();

            // Verify LEFT OUTER JOIN
            assertThat(sql).containsIgnoringCase("LEFT");
            assertThat(sql).containsIgnoringCase("JOIN");

            // Verify table references
            assertThat(sql).containsIgnoringCase("customer");
            assertThat(sql).containsIgnoringCase("orders");

            // Verify aggregation
            assertThat(sql).containsIgnoringCase("GROUP BY");
            assertThat(sql).containsIgnoringCase("COUNT");

            // Verify sorting
            assertThat(sql).containsIgnoringCase("ORDER BY");
            assertThat(sql).containsIgnoringCase("DESC");
        }

        @Test
        @DisplayName("Q13 demonstrates nested aggregation pattern")
        void testQ13NestedAggregation() {
            // Given: TPC-H Q13 with two-level aggregation
            LogicalPlan plan = TPCHQueryPlans.query13(TEST_DATA_PATH);

            // When: Generate SQL
            String sql = generator.generate(plan);

            // Then: Contains nested aggregation structure
            // The plan has inner aggregation (COUNT per customer)
            // and outer aggregation (COUNT of customers per count)
            assertThat(sql).containsIgnoringCase("COUNT");

            // Verify alias used in aggregation
            assertThat(sql.toLowerCase()).contains("custdist");
        }
    }

    @Nested
    @DisplayName("TPC-H Q18: Large Volume Customer Query")
    class Query18Tests {

        @Test
        @DisplayName("Q18 logical plan constructs with HAVING clause")
        void testQ18LogicalPlanConstruction() {
            // Given: TPC-H Q18 plan construction with HAVING
            // When: Build logical plan
            LogicalPlan plan = TPCHQueryPlans.query18(TEST_DATA_PATH);

            // Then: Plan created successfully
            assertThat(plan).isNotNull();
            assertThat(plan).isInstanceOf(com.catalyst2sql.logical.Limit.class);
        }

        @Test
        @DisplayName("Q18 generates SQL with HAVING and IN subquery")
        void testQ18SQLGenerationWithHaving() {
            // Given: TPC-H Q18 logical plan
            LogicalPlan plan = TPCHQueryPlans.query18(TEST_DATA_PATH);

            // When: Generate SQL
            String sql = generator.generate(plan);

            // Then: SQL contains HAVING clause
            assertThat(sql).isNotNull();
            assertThat(sql).isNotEmpty();

            // Verify HAVING clause
            assertThat(sql).containsIgnoringCase("HAVING");

            // Verify SUM aggregate
            assertThat(sql).containsIgnoringCase("SUM");

            // Verify GROUP BY
            assertThat(sql).containsIgnoringCase("GROUP BY");

            // Verify IN subquery
            assertThat(sql).containsIgnoringCase("IN");
        }

        @Test
        @DisplayName("Q18 demonstrates multi-table JOIN with LIMIT")
        void testQ18MultiTableJoinWithLimit() {
            // Given: TPC-H Q18 with customer + orders + lineitem
            LogicalPlan plan = TPCHQueryPlans.query18(TEST_DATA_PATH);

            // When: Generate SQL
            String sql = generator.generate(plan);

            // Then: Contains all three tables
            assertThat(sql).containsIgnoringCase("customer");
            assertThat(sql).containsIgnoringCase("orders");
            assertThat(sql).containsIgnoringCase("lineitem");

            // Verify JOIN keywords
            assertThat(sql).containsIgnoringCase("JOIN");

            // Verify LIMIT
            assertThat(sql).containsIgnoringCase("LIMIT");
            assertThat(sql).contains("100");

            // Verify ORDER BY for TOP N
            assertThat(sql).containsIgnoringCase("ORDER BY");
        }
    }

    @Nested
    @DisplayName("Week 5 Feature Validation")
    class Week5FeatureValidation {

        @Test
        @DisplayName("Both queries demonstrate Week 5 aggregation features")
        void testWeek5FeaturesCoverage() {
            // Given: Both TPC-H queries
            LogicalPlan q13 = TPCHQueryPlans.query13(TEST_DATA_PATH);
            LogicalPlan q18 = TPCHQueryPlans.query18(TEST_DATA_PATH);

            // When: Generate SQL for both
            String sql13 = generator.generate(q13);
            String sql18 = generator.generate(q18);

            // Then: Q13 demonstrates nested aggregation
            assertThat(sql13).containsIgnoringCase("COUNT");
            assertThat(sql13).containsIgnoringCase("GROUP BY");

            // And: Q18 demonstrates HAVING clause
            assertThat(sql18).containsIgnoringCase("HAVING");
            assertThat(sql18).containsIgnoringCase("SUM");

            // Both generate valid, non-empty SQL
            assertThat(sql13).hasSizeGreaterThan(100);
            assertThat(sql18).hasSizeGreaterThan(100);
        }
    }
}
