package com.thunderduck.logical;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.types.LongType;
import com.thunderduck.types.StructType;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;

/**
 * Unit tests for Tail - the logical plan node that handles
 * df.tail(n) operations.
 *
 * Tests cover:
 * - SQL generation for tail operation
 * - Schema inference (schema unchanged from child)
 * - Edge cases (zero limit, empty result, limit > row count)
 * - Integration with DuckDB execution
 * - Memory-efficient streaming behavior
 *
 * @see Tail
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("Tail Unit Tests")
public class TailTest extends TestBase {

    private SQLGenerator generator;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("Constructor Validation Tests")
    class ConstructorValidationTests {

        @Test
        @DisplayName("Should create Tail with valid limit")
        void testValidConstruction() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 5);

            assertThat(tail.limit()).isEqualTo(5);
            assertThat(tail.child()).isEqualTo(range);
        }

        @Test
        @DisplayName("Should reject negative limit")
        void testNegativeLimitRejected() {
            RangeRelation range = new RangeRelation(0, 10, 1);

            assertThatThrownBy(() -> new Tail(range, -1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("limit must be non-negative");
        }

        @Test
        @DisplayName("Should accept zero limit")
        void testZeroLimitAllowed() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 0);

            assertThat(tail.limit()).isEqualTo(0);
        }

        // Note: LogicalPlan base class doesn't validate null children,
        // so we don't test for NullPointerException here. If a null child
        // is passed, it will fail later during schema inference or SQL generation.
    }

    @Nested
    @DisplayName("Schema Inference Tests")
    class SchemaInferenceTests {

        @Test
        @DisplayName("Should preserve schema from child")
        void testSchemaPreserved() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 5);

            StructType tailSchema = tail.inferSchema();
            StructType rangeSchema = range.inferSchema();

            assertThat(tailSchema.size()).isEqualTo(rangeSchema.size());
            assertThat(tailSchema.fieldAt(0).name()).isEqualTo("id");
            assertThat(tailSchema.fieldAt(0).dataType()).isInstanceOf(LongType.class);
        }
    }

    @Nested
    @DisplayName("SQL Generation Tests")
    class SQLGenerationTests {

        @Test
        @DisplayName("Should generate child plan SQL (tail handled in memory)")
        void testSimpleTailSQL() {
            // SQLGenerator now just generates the child plan's SQL
            // The tail operation is handled in memory by TailBatchIterator
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 3);
            String sql = generator.generate(tail);

            // Should generate the range SQL (child plan)
            assertThat(sql).contains("SELECT");
            // Should NOT contain window function SQL (handled in memory)
            assertThat(sql).doesNotContain("ROW_NUMBER()");
            assertThat(sql).doesNotContain("_td_rn");
            assertThat(sql).doesNotContain("_td_total");
        }

        @Test
        @DisplayName("Should delegate to child plan for SQL generation")
        void testDelegatesToChild() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 3);

            String tailSQL = generator.generate(tail);
            String rangeSQL = generator.generate(range);

            // Tail SQL should be the same as the child plan's SQL
            assertThat(tailSQL).isEqualTo(rangeSQL);
        }

        @Test
        @DisplayName("Tail limit not present in SQL (handled in memory)")
        void testLimitNotInSQL() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Tail tail = new Tail(range, 25);
            String sql = generator.generate(tail);

            // Limit should not be in SQL - it's handled by TailBatchIterator
            assertThat(sql).doesNotContain("25");
            assertThat(sql).doesNotContain("EXCLUDE");
        }
    }

    @Nested
    @DisplayName("ToString Tests")
    class ToStringTests {

        @Test
        @DisplayName("Should produce readable toString output")
        void testToString() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 5);
            String str = tail.toString();

            assertThat(str)
                    .contains("Tail")
                    .contains("5");
        }
    }

    // NOTE: DuckDB Execution Tests were removed because tail() is now handled
    // in memory by TailBatchIterator (O(N) memory) instead of SQL window
    // functions (O(total_rows) memory). End-to-end behavior is tested
    // via the Spark Connect server integration tests.

    @Nested
    @DisplayName("Composability Tests")
    class ComposabilityTests {

        @Test
        @DisplayName("Tail should have one child")
        void testHasOneChild() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 5);

            assertThat(tail.children()).hasSize(1);
            assertThat(tail.children().get(0)).isEqualTo(range);
        }

        @Test
        @DisplayName("Tail can be chained after Limit")
        void testTailAfterLimit() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Limit limit = new Limit(range, 50);
            Tail tail = new Tail(limit, 10);

            String sql = generator.generate(tail);

            // Tail delegates to child (Limit), so SQL should contain LIMIT 50
            assertThat(sql).contains("LIMIT 50");
            // But no window functions - tail is handled in memory
            assertThat(sql).doesNotContain("ROW_NUMBER()");
        }

        @Test
        @DisplayName("Tail can be wrapped by Project")
        void testProjectOnTail() {
            RangeRelation range = new RangeRelation(0, 10, 1);
            Tail tail = new Tail(range, 5);

            com.thunderduck.expression.ColumnReference idCol =
                com.thunderduck.expression.ColumnReference.of("id", LongType.get());
            Project project = new Project(tail, List.of(idCol));

            String sql = generator.generate(project);

            // Project on Tail should work - Tail delegates to child
            assertThat(sql).contains("SELECT");
            // No window functions since tail is handled in memory
            assertThat(sql).doesNotContain("ROW_NUMBER()");
            assertThat(sql).doesNotContain("EXCLUDE");
        }
    }
}
