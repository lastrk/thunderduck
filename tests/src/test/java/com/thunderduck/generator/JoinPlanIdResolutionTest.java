package com.thunderduck.generator;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.TableScan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for plan_id-based column qualification in join conditions.
 *
 * <p>These tests verify that the SQLGenerator correctly qualifies column
 * references in join conditions using plan_id information from Spark Connect.
 *
 * <p>The plan_id mechanism enables proper handling of joins where both tables
 * have columns with the same name (e.g., df1["id"] == df2["id"]).
 */
@TestCategories.Unit
@TestCategories.Tier1
@DisplayName("Join Plan ID Resolution")
public class JoinPlanIdResolutionTest extends TestBase {

    private SQLGenerator generator;
    private StructType schema1;
    private StructType schema2;
    private TableScan table1;
    private TableScan table2;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();

        // Create schemas with same column name 'id' in both
        schema1 = new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true)
        ));

        schema2 = new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),  // Same 'id' column!
            new StructField("value", StringType.get(), true)
        ));

        table1 = new TableScan("table1.parquet", TableScan.TableFormat.PARQUET, schema1);
        table2 = new TableScan("table2.parquet", TableScan.TableFormat.PARQUET, schema2);
    }

    @Nested
    @DisplayName("UnresolvedColumn with plan_id")
    class UnresolvedColumnPlanIdTests {

        @Test
        @DisplayName("UnresolvedColumn stores and returns plan_id")
        void testPlanIdStorage() {
            UnresolvedColumn col = new UnresolvedColumn("id", null, OptionalLong.of(42L));

            assertThat(col.hasPlanId()).isTrue();
            assertThat(col.planId().getAsLong()).isEqualTo(42L);
            assertThat(col.columnName()).isEqualTo("id");
        }

        @Test
        @DisplayName("UnresolvedColumn without plan_id returns empty")
        void testNoPlanId() {
            UnresolvedColumn col = new UnresolvedColumn("id");

            assertThat(col.hasPlanId()).isFalse();
            assertThat(col.planId().isEmpty()).isTrue();
        }

        @Test
        @DisplayName("Two UnresolvedColumns with different plan_ids are not equal")
        void testPlanIdInEquality() {
            UnresolvedColumn col1 = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn col2 = new UnresolvedColumn("id", null, OptionalLong.of(2L));

            assertThat(col1).isNotEqualTo(col2);
        }

        @Test
        @DisplayName("Two UnresolvedColumns with same name and plan_id are equal")
        void testPlanIdEquality() {
            UnresolvedColumn col1 = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn col2 = new UnresolvedColumn("id", null, OptionalLong.of(1L));

            assertThat(col1).isEqualTo(col2);
            assertThat(col1.hashCode()).isEqualTo(col2.hashCode());
        }
    }

    @Nested
    @DisplayName("Join SQL Generation with Ambiguous Columns")
    class JoinSQLGenerationTests {

        @Test
        @DisplayName("Join with columns having different plan_ids should generate qualified SQL")
        void testJoinWithDifferentPlanIds() {
            // Given: Two columns with same name but different plan_ids
            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));

            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            // When: Create join and generate SQL
            // Note: We need to set plan_id on the table scans for this to work
            table1.setPlanId(1L);
            table2.setPlanId(2L);

            Join join = new Join(table1, table2, Join.JoinType.INNER, joinCondition);
            String sql = generator.generate(join);

            // Then: SQL should have qualified column references
            logData("Generated SQL", sql);

            // The condition should NOT be just "id = id" (ambiguous)
            // It should be qualified like "subquery_1.id = subquery_2.id"
            assertThat(sql).contains("INNER JOIN");
            assertThat(sql).contains("ON");

            // Verify columns are qualified (not just bare "id")
            // The exact format depends on implementation, but it should not
            // have unqualified "(id = id)" in the ON clause
            assertThat(sql).doesNotContain("ON (id = id)");
            assertThat(sql).doesNotContain("ON (\"id\" = \"id\")");
        }

        @Test
        @DisplayName("Join with columns having no plan_ids falls back to unqualified")
        void testJoinWithoutPlanIds() {
            // Given: Two columns without plan_ids (backward compatibility)
            UnresolvedColumn leftCol = new UnresolvedColumn("name");
            UnresolvedColumn rightCol = new UnresolvedColumn("value");

            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            // When: Create join and generate SQL
            Join join = new Join(table1, table2, Join.JoinType.INNER, joinCondition);
            String sql = generator.generate(join);

            // Then: SQL should still work (columns are not ambiguous)
            logData("Generated SQL", sql);
            assertThat(sql).contains("INNER JOIN");
            assertThat(sql).contains("ON");
        }

        @Test
        @DisplayName("Already qualified columns should be preserved")
        void testAlreadyQualifiedColumns() {
            // Given: Columns with explicit qualifiers
            UnresolvedColumn leftCol = new UnresolvedColumn("id", "t1", OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", "t2", OptionalLong.of(2L));

            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            // When: Create join and generate SQL
            table1.setPlanId(1L);
            table2.setPlanId(2L);

            Join join = new Join(table1, table2, Join.JoinType.INNER, joinCondition);
            String sql = generator.generate(join);

            // Then: Original qualifiers should be preserved
            logData("Generated SQL", sql);
            assertThat(sql).contains("t1");
            assertThat(sql).contains("t2");
        }

        @Test
        @DisplayName("Complex join condition with AND should qualify all columns")
        void testComplexJoinCondition() {
            // Given: Complex condition with AND
            UnresolvedColumn leftId = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightId = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            UnresolvedColumn leftName = new UnresolvedColumn("name", null, OptionalLong.of(1L));

            Expression idCondition = BinaryExpression.equal(leftId, rightId);
            Expression nameCondition = BinaryExpression.equal(
                leftName,
                new com.thunderduck.expression.Literal("test", StringType.get())
            );
            Expression complexCondition = BinaryExpression.and(idCondition, nameCondition);

            // When: Create join and generate SQL
            table1.setPlanId(1L);
            table2.setPlanId(2L);

            Join join = new Join(table1, table2, Join.JoinType.INNER, complexCondition);
            String sql = generator.generate(join);

            // Then: All columns should be qualified
            logData("Generated SQL", sql);
            assertThat(sql).contains("INNER JOIN");
            assertThat(sql).contains("ON");
            assertThat(sql).contains("AND");
        }
    }

    @Nested
    @DisplayName("Plan ID Registry")
    class PlanIdRegistryTests {

        @Test
        @DisplayName("LogicalPlan should store plan_id")
        void testLogicalPlanStoresPlanId() {
            // Given: A table scan
            TableScan scan = new TableScan("test.parquet", TableScan.TableFormat.PARQUET, schema1);

            // When: Set plan_id
            scan.setPlanId(123L);

            // Then: plan_id should be retrievable
            assertThat(scan.planId().isPresent()).isTrue();
            assertThat(scan.planId().getAsLong()).isEqualTo(123L);
        }

        @Test
        @DisplayName("LogicalPlan without plan_id should return empty")
        void testLogicalPlanNoPlanId() {
            // Given: A table scan without plan_id
            TableScan scan = new TableScan("test.parquet", TableScan.TableFormat.PARQUET, schema1);

            // Then: plan_id should be empty
            assertThat(scan.planId().isEmpty()).isTrue();
        }
    }
}
