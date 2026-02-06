package com.thunderduck.types;

import com.thunderduck.expression.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CASE WHEN with NULL and Decimal type inference.
 *
 * <p>This addresses the bug where:
 * <pre>
 * CASE WHEN condition THEN decimal_col ELSE null END
 * </pre>
 * was resolving to Decimal(38,2) instead of preserving the original Decimal(7,2) precision.
 *
 * <p>The issue was that untyped NULL literals were participating in decimal type unification,
 * causing precision inflation to the maximum (38).
 */
class CaseWhenDecimalNullTest {

    @Test
    void testCaseWhenWithNullPreservesDecimalPrecision() {
        // Simulate TPC-DS Q43 pattern: CASE WHEN condition THEN decimal_col ELSE NULL END
        StructType schema = new StructType(Arrays.asList(
            new StructField("ss_sales_price", new DecimalType(7, 2), false)
        ));

        Expression salesPrice = new UnresolvedColumn("ss_sales_price");
        Expression condition = Literal.of(true);

        // Create NULL literal with StringType (untyped NULL marker)
        Expression nullExpr = Literal.nullValue(StringType.get());

        CaseWhenExpression caseWhen = new CaseWhenExpression(
            Arrays.asList(condition),
            Arrays.asList(salesPrice),  // THEN
            nullExpr                     // ELSE NULL
        );

        DataType caseType = TypeInferenceEngine.resolveType(caseWhen, schema);

        // Should preserve original precision, not inflate to Decimal(38,2)
        assertThat(caseType).isEqualTo(new DecimalType(7, 2));
    }

    @Test
    void testCaseWhenWithNullAndSumAdds10Precision() {
        // Test that SUM(CASE WHEN ... THEN decimal_col ELSE NULL END) adds +10 precision
        StructType schema = new StructType(Arrays.asList(
            new StructField("ss_sales_price", new DecimalType(7, 2), false)
        ));

        Expression salesPrice = new UnresolvedColumn("ss_sales_price");
        Expression condition = Literal.of(true);
        Expression nullExpr = Literal.nullValue(StringType.get());

        CaseWhenExpression caseWhen = new CaseWhenExpression(
            Arrays.asList(condition),
            Arrays.asList(salesPrice),
            nullExpr
        );

        // Resolve CASE WHEN type
        DataType caseType = TypeInferenceEngine.resolveType(caseWhen, schema);
        assertThat(caseType).isEqualTo(new DecimalType(7, 2));

        // SUM should add +10 precision per Spark semantics
        DataType sumType = TypeInferenceEngine.resolveAggregateReturnType("SUM", caseType);
        assertThat(sumType).isEqualTo(new DecimalType(17, 2));
    }

    @Test
    void testCaseWhenWithTypedNullStillUnifies() {
        // If NULL is typed (e.g., CAST(NULL AS DECIMAL(10,2))), it should participate in unification
        StructType schema = new StructType(Arrays.asList(
            new StructField("price", new DecimalType(7, 2), false)
        ));

        Expression price = new UnresolvedColumn("price");
        Expression condition = Literal.of(true);

        // Create typed NULL - this SHOULD participate in unification
        Expression typedNull = Literal.nullValue(new DecimalType(10, 2));

        CaseWhenExpression caseWhen = new CaseWhenExpression(
            Arrays.asList(condition),
            Arrays.asList(price),     // Decimal(7,2)
            typedNull                  // Decimal(10,2)
        );

        DataType caseType = TypeInferenceEngine.resolveType(caseWhen, schema);

        // Should unify to Decimal(10,2) - the typed NULL has higher precision
        // Decimal(7,2) has 5 int digits, Decimal(10,2) has 8 int digits
        // Result: max(5, 8) = 8 int digits, max(2, 2) = 2 scale → Decimal(10,2)
        assertThat(caseType).isEqualTo(new DecimalType(10, 2));
    }

    @Test
    void testCaseWhenWithMultipleBranchesAndNull() {
        // Test multiple WHEN branches with NULL else
        StructType schema = new StructType(Arrays.asList(
            new StructField("price1", new DecimalType(7, 2), false),
            new StructField("price2", new DecimalType(8, 3), false)
        ));

        Expression price1 = new UnresolvedColumn("price1");
        Expression price2 = new UnresolvedColumn("price2");
        Expression cond1 = Literal.of(true);
        Expression cond2 = Literal.of(false);
        Expression nullExpr = Literal.nullValue(StringType.get());

        CaseWhenExpression caseWhen = new CaseWhenExpression(
            Arrays.asList(cond1, cond2),
            Arrays.asList(price1, price2),  // Decimal(7,2), Decimal(8,3)
            nullExpr
        );

        DataType caseType = TypeInferenceEngine.resolveType(caseWhen, schema);

        // Should unify price1 and price2, ignoring NULL
        // Decimal(7,2) and Decimal(8,3):
        //   intDigits = max(7-2, 8-3) = max(5, 5) = 5
        //   scale = max(2, 3) = 3
        //   precision = min(5+3, 38) = 8
        assertThat(caseType).isEqualTo(new DecimalType(8, 3));
    }
}
