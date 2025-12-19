package com.thunderduck.generator;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.Literal;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.Filter;
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
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for direct aliasing optimization in join SQL generation.
 *
 * <p>These tests verify that the SQLGenerator avoids unnecessary subquery
 * wrapping for simple table sources (TableScan) while still correctly wrapping
 * complex sources (Filter, Project, etc.).
 *
 * <p>This optimization produces cleaner SQL like:
 * <pre>
 *   SELECT * FROM "users" AS subquery_1 INNER JOIN "orders" AS subquery_2 ...
 * </pre>
 * instead of:
 * <pre>
 *   SELECT * FROM (SELECT * FROM "users") AS subquery_1 INNER JOIN ...
 * </pre>
 */
@TestCategories.Unit
@TestCategories.Tier1
@DisplayName("SQL Generator Direct Alias Optimization")
public class SQLGeneratorDirectAliasTest extends TestBase {

    private SQLGenerator generator;
    private StructType schema;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();

        schema = new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("value", IntegerType.get(), true)
        ));
    }

    @Nested
    @DisplayName("Regular Joins (INNER, LEFT, etc.)")
    class RegularJoinTests {

        @Test
        @DisplayName("TABLE sources should be directly aliased in joins")
        void testTableSourceDirectAlias() {
            // Given: Two TABLE format TableScans
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.INNER, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should NOT have double SELECT * FROM (direct aliasing)
            assertThat(sql).doesNotContain("SELECT * FROM (SELECT * FROM");

            // Should have direct aliasing pattern
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("JOIN \"orders\" AS subquery_2");
        }

        @Test
        @DisplayName("PARQUET sources should be directly aliased in joins")
        void testParquetSourceDirectAlias() {
            // Given: Two PARQUET format TableScans
            TableScan left = new TableScan("data.parquet", TableScan.TableFormat.PARQUET, schema);
            TableScan right = new TableScan("other.parquet", TableScan.TableFormat.PARQUET, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.INNER, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct parquet aliasing
            assertThat(sql).contains("read_parquet('data.parquet') AS subquery_1");
            assertThat(sql).contains("read_parquet('other.parquet') AS subquery_2");
            assertThat(sql).doesNotContain("(SELECT * FROM read_parquet");
        }

        @Test
        @DisplayName("DELTA sources should be directly aliased in joins")
        void testDeltaSourceDirectAlias() {
            // Given: Two DELTA format TableScans
            TableScan left = new TableScan("/path/to/delta1", TableScan.TableFormat.DELTA, schema);
            TableScan right = new TableScan("/path/to/delta2", TableScan.TableFormat.DELTA, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.INNER, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct delta_scan aliasing
            assertThat(sql).contains("delta_scan('/path/to/delta1') AS subquery_1");
            assertThat(sql).contains("delta_scan('/path/to/delta2') AS subquery_2");
        }

        @Test
        @DisplayName("ICEBERG sources should be directly aliased in joins")
        void testIcebergSourceDirectAlias() {
            // Given: Two ICEBERG format TableScans
            TableScan left = new TableScan("/path/to/iceberg1", TableScan.TableFormat.ICEBERG, schema);
            TableScan right = new TableScan("/path/to/iceberg2", TableScan.TableFormat.ICEBERG, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.INNER, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct iceberg_scan aliasing
            assertThat(sql).contains("iceberg_scan('/path/to/iceberg1') AS subquery_1");
            assertThat(sql).contains("iceberg_scan('/path/to/iceberg2') AS subquery_2");
        }

        @Test
        @DisplayName("Filter child should still be wrapped in subquery")
        void testFilterChildStillWrapped() {
            // Given: A Filter on a TableScan joined with a plain TableScan
            TableScan scan = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            scan.setPlanId(1L);

            Expression filterCondition = BinaryExpression.greaterThan(
                new UnresolvedColumn("value"),
                new Literal(100, IntegerType.get())
            );
            Filter filter = new Filter(scan, filterCondition);

            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(filter, right, Join.JoinType.INNER, joinCondition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Filter side should be wrapped (has SELECT * FROM (...))
            assertThat(sql).contains("SELECT * FROM (SELECT");
            // But TableScan side should be direct
            assertThat(sql).contains("JOIN \"orders\" AS");
        }

        @Test
        @DisplayName("Mixed sources: one direct, one wrapped")
        void testMixedSources() {
            // Given: TableScan (direct) joined with Filter (wrapped)
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);

            TableScan rightScan = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            rightScan.setPlanId(2L);
            Expression filterCondition = BinaryExpression.greaterThan(
                new UnresolvedColumn("value"),
                new Literal(50, IntegerType.get())
            );
            Filter rightFilter = new Filter(rightScan, filterCondition);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, rightFilter, Join.JoinType.INNER, joinCondition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Left should be direct, right should be wrapped
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("JOIN (SELECT");
        }

        @Test
        @DisplayName("LEFT OUTER join with direct aliasing")
        void testLeftOuterJoinDirectAlias() {
            // Given: Two TableScans with LEFT OUTER join
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.LEFT, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct aliasing with LEFT OUTER JOIN
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("LEFT OUTER JOIN \"orders\" AS subquery_2");
        }

        @Test
        @DisplayName("CROSS join with direct aliasing")
        void testCrossJoinDirectAlias() {
            // Given: Two TableScans with CROSS join
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);

            Join join = new Join(left, right, Join.JoinType.CROSS, null);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct aliasing with CROSS JOIN
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("CROSS JOIN \"orders\" AS subquery_2");
        }
    }

    @Nested
    @DisplayName("SEMI/ANTI Joins (EXISTS/NOT EXISTS)")
    class SemiAntiJoinTests {

        @Test
        @DisplayName("LEFT SEMI join with direct aliasing")
        void testLeftSemiJoinDirectAlias() {
            // Given: Two TableScans with LEFT SEMI join
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.LEFT_SEMI, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct aliasing with EXISTS pattern
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("EXISTS (SELECT 1 FROM \"orders\" AS subquery_2");
            assertThat(sql).doesNotContain("EXISTS (SELECT 1 FROM (SELECT");
        }

        @Test
        @DisplayName("LEFT ANTI join with direct aliasing")
        void testLeftAntiJoinDirectAlias() {
            // Given: Two TableScans with LEFT ANTI join
            TableScan left = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.LEFT_ANTI, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Should have direct aliasing with NOT EXISTS pattern
            assertThat(sql).contains("FROM \"users\" AS subquery_1");
            assertThat(sql).contains("NOT EXISTS (SELECT 1 FROM \"orders\" AS subquery_2");
        }

        @Test
        @DisplayName("SEMI join with Filter child should wrap Filter side only")
        void testSemiJoinFilterChildWrapped() {
            // Given: Filter joined with TableScan via LEFT SEMI
            TableScan leftScan = new TableScan("users", TableScan.TableFormat.TABLE, schema);
            leftScan.setPlanId(1L);
            Expression filterCondition = BinaryExpression.greaterThan(
                new UnresolvedColumn("value"),
                new Literal(100, IntegerType.get())
            );
            Filter leftFilter = new Filter(leftScan, filterCondition);

            TableScan right = new TableScan("orders", TableScan.TableFormat.TABLE, schema);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            Expression joinCondition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(leftFilter, right, Join.JoinType.LEFT_SEMI, joinCondition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Left (Filter) should be wrapped, right (TableScan) should be direct
            assertThat(sql).contains("SELECT * FROM (SELECT");  // Left wrapped
            assertThat(sql).contains("EXISTS (SELECT 1 FROM \"orders\" AS");  // Right direct
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Nested joins should apply direct aliasing at each level")
        void testNestedJoinsDirectAlias() {
            // Given: Three TableScans in nested join
            TableScan t1 = new TableScan("t1", TableScan.TableFormat.TABLE, schema);
            TableScan t2 = new TableScan("t2", TableScan.TableFormat.TABLE, schema);
            TableScan t3 = new TableScan("t3", TableScan.TableFormat.TABLE, schema);
            t1.setPlanId(1L);
            t2.setPlanId(2L);
            t3.setPlanId(3L);

            UnresolvedColumn col1 = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn col2 = new UnresolvedColumn("id", null, OptionalLong.of(2L));
            UnresolvedColumn col3 = new UnresolvedColumn("id", null, OptionalLong.of(3L));

            // First join: t1 INNER JOIN t2
            Expression cond1 = BinaryExpression.equal(col1, col2);
            Join innerJoin = new Join(t1, t2, Join.JoinType.INNER, cond1);

            // Second join: (t1 JOIN t2) INNER JOIN t3
            // Note: col1 now refers to inner join's output
            Expression cond2 = BinaryExpression.equal(col1, col3);
            Join outerJoin = new Join(innerJoin, t3, Join.JoinType.INNER, cond2);

            // When: Generate SQL
            String sql = generator.generate(outerJoin);
            logData("Generated SQL", sql);

            // Then: Should contain t3 direct aliasing (outer level)
            assertThat(sql).contains("JOIN \"t3\" AS");
            // Inner level uses direct aliasing too
            assertThat(sql).contains("\"t1\" AS");
            assertThat(sql).contains("\"t2\" AS");
        }

        @Test
        @DisplayName("Self-join should use direct aliasing on both sides")
        void testSelfJoinDirectAlias() {
            // Given: Same table joined with itself
            TableScan left = new TableScan("employees", TableScan.TableFormat.TABLE, schema);
            TableScan right = new TableScan("employees", TableScan.TableFormat.TABLE, schema);
            left.setPlanId(1L);
            right.setPlanId(2L);

            UnresolvedColumn leftCol = new UnresolvedColumn("id", null, OptionalLong.of(1L));
            UnresolvedColumn rightCol = new UnresolvedColumn("value", null, OptionalLong.of(2L));
            Expression condition = BinaryExpression.equal(leftCol, rightCol);

            Join join = new Join(left, right, Join.JoinType.INNER, condition);

            // When: Generate SQL
            String sql = generator.generate(join);
            logData("Generated SQL", sql);

            // Then: Both sides should be directly aliased
            assertThat(sql).contains("FROM \"employees\" AS subquery_1");
            assertThat(sql).contains("JOIN \"employees\" AS subquery_2");
            assertThat(sql).doesNotContain("(SELECT * FROM \"employees\")");
        }
    }
}
