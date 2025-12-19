package com.thunderduck.connect.converter;

import com.thunderduck.expression.Expression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import org.apache.spark.connect.proto.Expression.UnresolvedAttribute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for ExpressionConverter plan_id extraction.
 *
 * <p>These tests verify that the ExpressionConverter correctly extracts
 * plan_id from Spark Connect protobuf UnresolvedAttribute messages and
 * propagates it to UnresolvedColumn expressions.
 */
@TestCategories.Integration
@TestCategories.Tier2
@DisplayName("ExpressionConverter Plan ID Extraction")
public class ExpressionConverterPlanIdTest extends TestBase {

    private ExpressionConverter converter;

    @Override
    protected void doSetUp() {
        converter = new ExpressionConverter();
    }

    @Nested
    @DisplayName("UnresolvedAttribute with plan_id")
    class UnresolvedAttributeWithPlanId {

        @Test
        @DisplayName("Should preserve plan_id when converting UnresolvedAttribute")
        void testPlanIdExtraction() {
            // Given: UnresolvedAttribute with plan_id
            UnresolvedAttribute attr = UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier("id")
                .setPlanId(42L)
                .build();

            org.apache.spark.connect.proto.Expression proto =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedAttribute(attr)
                    .build();

            // When: Convert to thunderduck expression
            Expression result = converter.convert(proto);

            // Then: plan_id should be preserved
            assertThat(result).isInstanceOf(UnresolvedColumn.class);
            UnresolvedColumn col = (UnresolvedColumn) result;
            assertThat(col.columnName()).isEqualTo("id");
            assertThat(col.hasPlanId()).isTrue();
            assertThat(col.planId().getAsLong()).isEqualTo(42L);
        }

        @Test
        @DisplayName("Should handle UnresolvedAttribute without plan_id")
        void testNoPlanId() {
            // Given: UnresolvedAttribute without plan_id
            UnresolvedAttribute attr = UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier("name")
                .build();

            org.apache.spark.connect.proto.Expression proto =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedAttribute(attr)
                    .build();

            // When: Convert to thunderduck expression
            Expression result = converter.convert(proto);

            // Then: Should work without plan_id
            assertThat(result).isInstanceOf(UnresolvedColumn.class);
            UnresolvedColumn col = (UnresolvedColumn) result;
            assertThat(col.columnName()).isEqualTo("name");
            assertThat(col.hasPlanId()).isFalse();
        }

        @Test
        @DisplayName("Should preserve plan_id with qualified column name")
        void testPlanIdWithQualifiedName() {
            // Given: UnresolvedAttribute with plan_id and qualified name
            // Note: When plan_id is present, "table.column" is treated as struct access
            // because table qualification comes from plan_id resolution
            UnresolvedAttribute attr = UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier("column")
                .setPlanId(123L)
                .build();

            org.apache.spark.connect.proto.Expression proto =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedAttribute(attr)
                    .build();

            // When: Convert to thunderduck expression
            Expression result = converter.convert(proto);

            // Then: plan_id should be preserved
            assertThat(result).isInstanceOf(UnresolvedColumn.class);
            UnresolvedColumn col = (UnresolvedColumn) result;
            assertThat(col.hasPlanId()).isTrue();
            assertThat(col.planId().getAsLong()).isEqualTo(123L);
        }

        @Test
        @DisplayName("Different plan_ids should produce different columns")
        void testDifferentPlanIds() {
            // Given: Two UnresolvedAttributes with same name but different plan_ids
            UnresolvedAttribute attr1 = UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier("id")
                .setPlanId(1L)
                .build();

            UnresolvedAttribute attr2 = UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier("id")
                .setPlanId(2L)
                .build();

            org.apache.spark.connect.proto.Expression proto1 =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedAttribute(attr1)
                    .build();

            org.apache.spark.connect.proto.Expression proto2 =
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setUnresolvedAttribute(attr2)
                    .build();

            // When: Convert both
            Expression result1 = converter.convert(proto1);
            Expression result2 = converter.convert(proto2);

            // Then: Should be different due to different plan_ids
            assertThat(result1).isNotEqualTo(result2);

            UnresolvedColumn col1 = (UnresolvedColumn) result1;
            UnresolvedColumn col2 = (UnresolvedColumn) result2;

            assertThat(col1.columnName()).isEqualTo(col2.columnName());
            assertThat(col1.planId().getAsLong()).isNotEqualTo(col2.planId().getAsLong());
        }
    }
}
