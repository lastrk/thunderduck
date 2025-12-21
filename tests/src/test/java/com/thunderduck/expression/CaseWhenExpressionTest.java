package com.thunderduck.expression;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for CaseWhenExpression.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("CaseWhenExpression Tests")
public class CaseWhenExpressionTest extends TestBase {

    // Helper to create a simple literal
    private Expression lit(int value) {
        return Literal.of(value);
    }

    private Expression lit(String value) {
        return Literal.of(value);
    }

    private Expression col(String name) {
        return new UnresolvedColumn(name);
    }

    private Expression condition() {
        // Simple boolean condition
        return BinaryExpression.greaterThan(col("x"), lit(0));
    }

    @Nested
    @DisplayName("Construction")
    class Construction {

        @Test
        @DisplayName("Basic CASE WHEN with one branch")
        void testBasicConstruction() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );

            assertThat(expr.conditions()).hasSize(1);
            assertThat(expr.thenBranches()).hasSize(1);
            assertThat(expr.elseBranch()).isNotNull();
        }

        @Test
        @DisplayName("CASE WHEN with multiple branches")
        void testMultipleBranches() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Arrays.asList(condition(), condition()),
                Arrays.asList(lit(1), lit(2)),
                lit(0)
            );

            assertThat(expr.conditions()).hasSize(2);
            assertThat(expr.thenBranches()).hasSize(2);
        }

        @Test
        @DisplayName("CASE WHEN without ELSE")
        void testWithoutElse() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                null
            );

            assertThat(expr.elseBranch()).isNull();
        }

        @Test
        @DisplayName("Throws on mismatched sizes")
        void testMismatchedSizes() {
            assertThatThrownBy(() -> new CaseWhenExpression(
                Arrays.asList(condition(), condition()),
                Collections.singletonList(lit(1)),  // Only one then branch
                null
            )).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Throws on empty conditions")
        void testEmptyConditions() {
            assertThatThrownBy(() -> new CaseWhenExpression(
                Collections.emptyList(),
                Collections.emptyList(),
                null
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Returns IntegerType from integer literals")
        void testIntegerLiterals() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );

            assertThat(expr.dataType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Returns StringType from string literals")
        void testStringLiterals() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit("yes")),
                lit("no")
            );

            assertThat(expr.dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Falls back to StringType for unresolved columns")
        void testUnresolvedColumns() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(col("some_column")),
                col("other_column")
            );

            // Without schema, unresolved columns return StringType
            assertThat(expr.dataType()).isInstanceOf(StringType.class);
        }
    }

    @Nested
    @DisplayName("Schema-Aware Type Resolution")
    class SchemaAwareTypeResolution {

        @Test
        @DisplayName("Resolves column type from schema")
        void testColumnTypeFromSchema() {
            // Schema with decimal column
            StructType schema = new StructType(Arrays.asList(
                new StructField("amount", new DecimalType(10, 2), true)
            ));

            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(col("amount")),
                lit(0)
            );

            DataType resolved = TypeInferenceEngine.resolveType(expr, schema);

            // Should resolve to DecimalType from schema (wider than IntegerType)
            assertThat(resolved).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("Unifies numeric types from branches")
        void testNumericTypeUnification() {
            StructType schema = new StructType(Arrays.asList(
                new StructField("int_col", IntegerType.get(), true),
                new StructField("long_col", LongType.get(), true)
            ));

            CaseWhenExpression expr = new CaseWhenExpression(
                Arrays.asList(condition(), condition()),
                Arrays.asList(col("int_col"), col("long_col")),
                null
            );

            DataType resolved = TypeInferenceEngine.resolveType(expr, schema);

            // Long is wider than Integer
            assertThat(resolved).isInstanceOf(LongType.class);
        }

        @Test
        @DisplayName("Resolves to IntegerType with literal values")
        void testLiteralValues() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );

            DataType resolved = TypeInferenceEngine.resolveType(expr, null);

            assertThat(resolved).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("SUM of CASE WHEN with Decimal column returns Decimal")
        void testSumOfCaseWhenWithDecimalColumn() {
            // Schema like TPC-DS catalog_sales with cs_sales_price as Decimal(7,2)
            StructType schema = new StructType(Arrays.asList(
                new StructField("cs_sales_price", new DecimalType(7, 2), true),
                new StructField("d_day_name", StringType.get(), false)
            ));

            // CASE WHEN d_day_name = 'Sunday' THEN cs_sales_price ELSE 0 END
            CaseWhenExpression caseWhen = new CaseWhenExpression(
                Collections.singletonList(
                    BinaryExpression.equal(col("d_day_name"), lit("Sunday"))
                ),
                Collections.singletonList(col("cs_sales_price")),
                lit(0)
            );

            // Resolve CASE WHEN type with schema
            DataType caseWhenType = TypeInferenceEngine.resolveType(caseWhen, schema);

            // Should resolve to Decimal(12,2) after unifying Decimal(7,2) with Integer(->Decimal(10,0))
            // Formula: intDigits = max(7-2, 10-0) = 10, scale = max(2,0) = 2, precision = 10+2 = 12
            assertThat(caseWhenType).isInstanceOf(DecimalType.class);
            DecimalType caseWhenDecimal = (DecimalType) caseWhenType;
            assertThat(caseWhenDecimal.precision()).isEqualTo(12);
            assertThat(caseWhenDecimal.scale()).isEqualTo(2);

            // Now test SUM aggregate return type
            // SUM(Decimal(12,2)) should return Decimal(22,2) per Spark rules (precision + 10)
            DataType sumReturnType = TypeInferenceEngine.resolveAggregateReturnType("SUM", caseWhenType);

            assertThat(sumReturnType).isInstanceOf(DecimalType.class);
            DecimalType resultDecimal = (DecimalType) sumReturnType;
            assertThat(resultDecimal.precision()).isEqualTo(22);
            assertThat(resultDecimal.scale()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Nullable")
    class Nullable {

        @Test
        @DisplayName("Nullable without ELSE")
        void testNullableWithoutElse() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                null  // No ELSE means implicit NULL
            );

            assertThat(expr.nullable()).isTrue();
        }

        @Test
        @DisplayName("Non-nullable with non-nullable branches")
        void testNonNullableWithNonNullableBranches() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),  // Literal is not nullable
                lit(0)  // Literal is not nullable
            );

            assertThat(expr.nullable()).isFalse();
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("Generates correct SQL for single branch")
        void testSingleBranch() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(BinaryExpression.greaterThan(col("x"), lit(0))),
                Collections.singletonList(lit(1)),
                lit(0)
            );

            String sql = expr.toSQL();

            assertThat(sql).contains("CASE");
            assertThat(sql).contains("WHEN");
            assertThat(sql).contains("THEN");
            assertThat(sql).contains("ELSE");
            assertThat(sql).contains("END");
        }

        @Test
        @DisplayName("Generates correct SQL without ELSE")
        void testWithoutElseSql() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                null
            );

            String sql = expr.toSQL();

            assertThat(sql).doesNotContain("ELSE");
            assertThat(sql).endsWith("END");
        }

        @Test
        @DisplayName("Generates correct SQL for multiple branches")
        void testMultipleBranchesSql() {
            CaseWhenExpression expr = new CaseWhenExpression(
                Arrays.asList(
                    BinaryExpression.equal(col("status"), lit(1)),
                    BinaryExpression.equal(col("status"), lit(2))
                ),
                Arrays.asList(lit("active"), lit("pending")),
                lit("unknown")
            );

            String sql = expr.toSQL();

            // Should have two WHEN clauses
            int whenCount = (sql.length() - sql.replace("WHEN", "").length()) / 4;
            assertThat(whenCount).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Equal expressions are equal")
        void testEquality() {
            CaseWhenExpression expr1 = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );
            CaseWhenExpression expr2 = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );

            assertThat(expr1).isEqualTo(expr2);
            assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
        }

        @Test
        @DisplayName("Different expressions are not equal")
        void testInequality() {
            CaseWhenExpression expr1 = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(1)),
                lit(0)
            );
            CaseWhenExpression expr2 = new CaseWhenExpression(
                Collections.singletonList(condition()),
                Collections.singletonList(lit(2)),  // Different value
                lit(0)
            );

            assertThat(expr1).isNotEqualTo(expr2);
        }
    }
}
