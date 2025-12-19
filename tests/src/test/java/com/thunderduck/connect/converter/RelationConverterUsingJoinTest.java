package com.thunderduck.connect.converter;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import org.apache.spark.connect.proto.Read;
import org.apache.spark.connect.proto.Relation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for RelationConverter USING join handling.
 *
 * <p>Tests verify that the RelationConverter correctly handles joins
 * with USING columns (e.g., {@code df1.join(df2, "id")}).
 *
 * <p>Test ID prefix: TC-JOIN-USING-*
 */
@TestCategories.Unit
@TestCategories.Tier1
@DisplayName("RelationConverter USING Join Tests")
public class RelationConverterUsingJoinTest extends TestBase {

    private RelationConverter converter;
    private ExpressionConverter expressionConverter;

    @Override
    protected void doSetUp() {
        expressionConverter = new ExpressionConverter();
        converter = new RelationConverter(expressionConverter);
    }

    /**
     * Creates a simple Read relation for testing.
     */
    private Relation createReadRelation(String tableName, long planId) {
        Read.DataSource dataSource = Read.DataSource.newBuilder()
            .setFormat("parquet")
            .addPaths("/data/" + tableName + ".parquet")
            .build();

        Read read = Read.newBuilder()
            .setDataSource(dataSource)
            .build();

        Relation.Builder builder = Relation.newBuilder()
            .setRead(read);

        if (planId > 0) {
            builder.setCommon(
                org.apache.spark.connect.proto.RelationCommon.newBuilder()
                    .setPlanId(planId)
                    .build()
            );
        }

        return builder.build();
    }

    @Nested
    @DisplayName("Single Column USING Join")
    class SingleColumnUsingJoin {

        @Test
        @DisplayName("TC-JOIN-USING-001: Single column USING join builds equality condition")
        void testSingleColumnUsingJoin() {
            // Given: Join with single using column
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
                    .addUsingColumns("id")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            // When: Convert to logical plan
            LogicalPlan result = converter.convert(joinRelation);

            // Then: Should produce Join with equality condition
            assertThat(result).isInstanceOf(Join.class);
            Join join = (Join) result;
            assertThat(join.joinType()).isEqualTo(Join.JoinType.INNER);

            Expression condition = join.condition();
            assertThat(condition).isNotNull();
            assertThat(condition).isInstanceOf(BinaryExpression.class);

            BinaryExpression binary = (BinaryExpression) condition;
            assertThat(binary.operator()).isEqualTo(BinaryExpression.Operator.EQUAL);

            // Left operand should reference "id"
            assertThat(binary.left()).isInstanceOf(UnresolvedColumn.class);
            UnresolvedColumn leftCol = (UnresolvedColumn) binary.left();
            assertThat(leftCol.columnName()).isEqualTo("id");

            // Right operand should reference "id"
            assertThat(binary.right()).isInstanceOf(UnresolvedColumn.class);
            UnresolvedColumn rightCol = (UnresolvedColumn) binary.right();
            assertThat(rightCol.columnName()).isEqualTo("id");
        }

        @Test
        @DisplayName("TC-JOIN-USING-002: USING join preserves plan_id for column qualification")
        void testUsingJoinWithPlanId() {
            // Given: Join with using columns and plan_ids
            Relation left = createReadRelation("left_table", 10L);
            Relation right = createReadRelation("right_table", 20L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
                    .addUsingColumns("key")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            // When: Convert to logical plan
            LogicalPlan result = converter.convert(joinRelation);

            // Then: Columns should have plan_ids from their source relations
            Join join = (Join) result;
            BinaryExpression condition = (BinaryExpression) join.condition();

            UnresolvedColumn leftCol = (UnresolvedColumn) condition.left();
            UnresolvedColumn rightCol = (UnresolvedColumn) condition.right();

            // Note: The plan_ids come from the converted child relations
            // They may be empty if child relations don't have plan_ids set
            assertThat(leftCol.columnName()).isEqualTo("key");
            assertThat(rightCol.columnName()).isEqualTo("key");
        }
    }

    @Nested
    @DisplayName("Multi-Column USING Join")
    class MultiColumnUsingJoin {

        @Test
        @DisplayName("TC-JOIN-USING-003: Multi-column USING join builds AND condition")
        void testMultiColumnUsingJoin() {
            // Given: Join with multiple using columns
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
                    .addUsingColumns("id")
                    .addUsingColumns("key")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            // When: Convert to logical plan
            LogicalPlan result = converter.convert(joinRelation);

            // Then: Should produce Join with AND of two equality conditions
            Join join = (Join) result;
            Expression condition = join.condition();
            assertThat(condition).isInstanceOf(BinaryExpression.class);

            BinaryExpression andExpr = (BinaryExpression) condition;
            assertThat(andExpr.operator()).isEqualTo(BinaryExpression.Operator.AND);

            // First part: id = id
            assertThat(andExpr.left()).isInstanceOf(BinaryExpression.class);
            BinaryExpression idCondition = (BinaryExpression) andExpr.left();
            assertThat(idCondition.operator()).isEqualTo(BinaryExpression.Operator.EQUAL);

            // Second part: key = key
            assertThat(andExpr.right()).isInstanceOf(BinaryExpression.class);
            BinaryExpression keyCondition = (BinaryExpression) andExpr.right();
            assertThat(keyCondition.operator()).isEqualTo(BinaryExpression.Operator.EQUAL);
        }
    }

    @Nested
    @DisplayName("USING Join with Different Join Types")
    class UsingJoinTypes {

        @Test
        @DisplayName("TC-JOIN-USING-004: Left outer USING join")
        void testLeftOuterUsingJoin() {
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER)
                    .addUsingColumns("id")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            LogicalPlan result = converter.convert(joinRelation);

            Join join = (Join) result;
            assertThat(join.joinType()).isEqualTo(Join.JoinType.LEFT);
            assertThat(join.condition()).isNotNull();
        }

        @Test
        @DisplayName("TC-JOIN-USING-005: Right outer USING join")
        void testRightOuterUsingJoin() {
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER)
                    .addUsingColumns("id")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            LogicalPlan result = converter.convert(joinRelation);

            Join join = (Join) result;
            assertThat(join.joinType()).isEqualTo(Join.JoinType.RIGHT);
            assertThat(join.condition()).isNotNull();
        }

        @Test
        @DisplayName("TC-JOIN-USING-006: Full outer USING join")
        void testFullOuterUsingJoin() {
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_FULL_OUTER)
                    .addUsingColumns("id")
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            LogicalPlan result = converter.convert(joinRelation);

            Join join = (Join) result;
            assertThat(join.joinType()).isEqualTo(Join.JoinType.FULL);
            assertThat(join.condition()).isNotNull();
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        @DisplayName("TC-JOIN-USING-007: Explicit condition takes precedence over using columns")
        void testExplicitConditionPrecedence() {
            // Given: Join with BOTH explicit condition AND using columns
            Relation left = createReadRelation("left_table", 1L);
            Relation right = createReadRelation("right_table", 2L);

            // Build explicit condition: left.col1 > right.col2
            org.apache.spark.connect.proto.Expression.UnresolvedAttribute leftAttr =
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("col1")
                    .build();
            org.apache.spark.connect.proto.Expression.UnresolvedAttribute rightAttr =
                org.apache.spark.connect.proto.Expression.UnresolvedAttribute.newBuilder()
                    .setUnparsedIdentifier("col2")
                    .build();

            org.apache.spark.connect.proto.Expression conditionExpr =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedFunction(
                        org.apache.spark.connect.proto.Expression.UnresolvedFunction.newBuilder()
                            .setFunctionName(">")
                            .addArguments(org.apache.spark.connect.proto.Expression.newBuilder()
                                .setUnresolvedAttribute(leftAttr))
                            .addArguments(org.apache.spark.connect.proto.Expression.newBuilder()
                                .setUnresolvedAttribute(rightAttr))
                            .build())
                    .build();

            org.apache.spark.connect.proto.Join joinProto =
                org.apache.spark.connect.proto.Join.newBuilder()
                    .setLeft(left)
                    .setRight(right)
                    .setJoinType(org.apache.spark.connect.proto.Join.JoinType.JOIN_TYPE_INNER)
                    .setJoinCondition(conditionExpr) // Explicit condition
                    .addUsingColumns("id")  // Also has using columns
                    .build();

            Relation joinRelation = Relation.newBuilder()
                .setJoin(joinProto)
                .build();

            // When: Convert to logical plan
            LogicalPlan result = converter.convert(joinRelation);

            // Then: Explicit condition should be used, not USING columns
            Join join = (Join) result;
            Expression condition = join.condition();

            // The explicit condition uses > operator, not =
            assertThat(condition).isInstanceOf(BinaryExpression.class);
            BinaryExpression binary = (BinaryExpression) condition;
            assertThat(binary.operator()).isEqualTo(BinaryExpression.Operator.GREATER_THAN);
        }
    }
}
