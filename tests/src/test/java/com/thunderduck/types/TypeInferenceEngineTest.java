package com.thunderduck.types;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Literal;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for TypeInferenceEngine.
 *
 * <p>Tests type inference logic including:
 * <ul>
 *   <li>Decimal division precision/scale calculation</li>
 *   <li>Numeric type promotion</li>
 *   <li>Binary expression type resolution</li>
 * </ul>
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("TypeInferenceEngine Tests")
public class TypeInferenceEngineTest extends TestBase {

    // ==================== Decimal Division ====================

    @Nested
    @DisplayName("Decimal Division Type Promotion")
    class DecimalDivision {

        @Test
        @DisplayName("Decimal / Decimal returns Decimal with correct precision and scale")
        void testDecimalDivisionBasic() {
            // Decimal(10, 2) / Decimal(5, 1)
            // Scale = max(6, s1 + p2 + 1) = max(6, 2 + 5 + 1) = 8
            // Precision = p1 - s1 + s2 + scale = 10 - 2 + 1 + 8 = 17
            DecimalType dividend = new DecimalType(10, 2);
            DecimalType divisor = new DecimalType(5, 1);

            DecimalType result = TypeInferenceEngine.promoteDecimalDivision(dividend, divisor);

            assertThat(result.precision()).isEqualTo(17);
            assertThat(result.scale()).isEqualTo(8);
        }

        @Test
        @DisplayName("Decimal division with minimum scale of 6")
        void testDecimalDivisionMinimumScale() {
            // Decimal(10, 0) / Decimal(5, 0)
            // Scale = max(6, 0 + 5 + 1) = 6 (minimum kicks in)
            // Precision = 10 - 0 + 0 + 6 = 16
            DecimalType dividend = new DecimalType(10, 0);
            DecimalType divisor = new DecimalType(5, 0);

            DecimalType result = TypeInferenceEngine.promoteDecimalDivision(dividend, divisor);

            assertThat(result.scale()).isEqualTo(6);
            assertThat(result.precision()).isEqualTo(16);
        }

        @Test
        @DisplayName("Decimal division caps precision and scale at 38")
        void testDecimalDivisionPrecisionCap() {
            // Decimal(38, 2) / Decimal(38, 2)
            // Scale = max(6, 2 + 38 + 1) = 41, capped to 38
            // Precision = 38 - 2 + 2 + 41 = 79, capped to 38
            DecimalType dividend = new DecimalType(38, 2);
            DecimalType divisor = new DecimalType(38, 2);

            DecimalType result = TypeInferenceEngine.promoteDecimalDivision(dividend, divisor);

            assertThat(result.precision()).isEqualTo(38);
            assertThat(result.scale()).isEqualTo(38);
        }

        @ParameterizedTest
        @DisplayName("Decimal division with various precision/scale combinations")
        @CsvSource({
            // p1, s1, p2, s2, expectedPrecision, expectedScale
            // Formula: scale = max(6, s1 + p2 + 1), precision = p1 - s1 + s2 + scale
            "10, 2, 5, 1, 17, 8",       // scale=max(6,8)=8, prec=10-2+1+8=17
            "18, 6, 10, 2, 31, 17",     // scale=max(6,17)=17, prec=18-6+2+17=31
            "5, 0, 3, 0, 11, 6",        // scale=max(6,4)=6, prec=5-0+0+6=11
            "20, 10, 10, 5, 36, 21",    // scale=max(6,21)=21, prec=20-10+5+21=36
            "38, 0, 38, 0, 38, 38"      // scale=39->38(cap), prec=77->38(cap)
        })
        void testDecimalDivisionVariations(int p1, int s1, int p2, int s2,
                                           int expectedPrecision, int expectedScale) {
            DecimalType dividend = new DecimalType(p1, s1);
            DecimalType divisor = new DecimalType(p2, s2);

            DecimalType result = TypeInferenceEngine.promoteDecimalDivision(dividend, divisor);

            assertThat(result.precision()).isEqualTo(expectedPrecision);
            assertThat(result.scale()).isEqualTo(expectedScale);
        }
    }

    // ==================== Binary Expression Division ====================

    @Nested
    @DisplayName("Binary Expression Division Type Resolution")
    class BinaryExpressionDivision {

        @Test
        @DisplayName("Division of two Decimal literals returns Decimal type")
        void testDecimalLiteralsDivision() {
            // Create Decimal literals using constructor
            Literal left = new Literal("100.00", new DecimalType(10, 2));
            Literal right = new Literal("10.00", new DecimalType(10, 2));
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            assertThat(result).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("Division of Integer literals returns Double type")
        void testIntegerLiteralsDivision() {
            Literal left = Literal.of(100);
            Literal right = Literal.of(10);
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Division of Decimal by Integer returns Double type")
        void testMixedDecimalIntegerDivision() {
            Literal left = new Literal("100.00", new DecimalType(10, 2));
            Literal right = Literal.of(10);
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            // Mixed types fall back to Double
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Division of Integer by Decimal returns Double type")
        void testMixedIntegerDecimalDivision() {
            Literal left = Literal.of(100);
            Literal right = new Literal("10.00", new DecimalType(10, 2));
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            // Mixed types fall back to Double
            assertThat(result).isInstanceOf(DoubleType.class);
        }
    }

    // ==================== Numeric Type Promotion ====================

    @Nested
    @DisplayName("Numeric Type Promotion")
    class NumericTypePromotion {

        @Test
        @DisplayName("Double + any numeric returns Double")
        void testDoublePromotion() {
            DataType result = TypeInferenceEngine.promoteNumericTypes(
                DoubleType.get(), IntegerType.get());
            assertThat(result).isInstanceOf(DoubleType.class);
        }

        @Test
        @DisplayName("Decimal + Decimal returns Decimal with max precision/scale")
        void testDecimalPromotion() {
            DecimalType left = new DecimalType(10, 2);
            DecimalType right = new DecimalType(15, 4);

            DataType result = TypeInferenceEngine.promoteNumericTypes(left, right);

            assertThat(result).isInstanceOf(DecimalType.class);
            DecimalType decResult = (DecimalType) result;
            assertThat(decResult.precision()).isEqualTo(15);
            assertThat(decResult.scale()).isEqualTo(4);
        }

        @Test
        @DisplayName("Long + Integer returns Long")
        void testLongPromotion() {
            DataType result = TypeInferenceEngine.promoteNumericTypes(
                LongType.get(), IntegerType.get());
            assertThat(result).isInstanceOf(LongType.class);
        }
    }
}
