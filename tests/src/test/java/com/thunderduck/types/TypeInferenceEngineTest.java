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
        @DisplayName("Decimal division applies precision loss adjustment when precision > 38")
        void testDecimalDivisionPrecisionCap() {
            // Decimal(38, 2) / Decimal(38, 2)
            // Step 1: Initial calculation
            //   Scale = max(6, 2 + 38 + 1) = 41
            //   Precision = 38 - 2 + 2 + 41 = 79
            // Step 2: Precision loss adjustment (since 79 > 38)
            //   intDigits = 79 - 41 = 38
            //   minScale = min(41, 6) = 6  (retain at least 6 fractional digits)
            //   scale = max(38 - 38, 6) = 6
            //   precision = 38
            DecimalType dividend = new DecimalType(38, 2);
            DecimalType divisor = new DecimalType(38, 2);

            DecimalType result = TypeInferenceEngine.promoteDecimalDivision(dividend, divisor);

            assertThat(result.precision()).isEqualTo(38);
            assertThat(result.scale()).isEqualTo(6);
        }

        @ParameterizedTest
        @DisplayName("Decimal division with various precision/scale combinations")
        @CsvSource({
            // p1, s1, p2, s2, expectedPrecision, expectedScale
            // Step 1: scale = max(6, s1 + p2 + 1), precision = p1 - s1 + s2 + scale
            // Step 2: If precision > 38, apply precision loss adjustment
            "10, 2, 5, 1, 17, 8",       // scale=8, prec=17 (no adjustment needed)
            "18, 6, 10, 2, 31, 17",     // scale=17, prec=31 (no adjustment needed)
            "5, 0, 3, 0, 11, 6",        // scale=6, prec=11 (no adjustment needed)
            "20, 10, 10, 5, 36, 21",    // scale=21, prec=36 (no adjustment needed)
            "38, 0, 38, 0, 38, 6"       // Initial: scale=39, prec=77. Adjustment: intDigits=38, scale=max(0,6)=6
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
        @DisplayName("Division of Decimal by Integer returns Decimal type (Spark promotes to Decimal)")
        void testMixedDecimalIntegerDivision() {
            Literal left = new Literal("100.00", new DecimalType(10, 2));
            Literal right = Literal.of(10);
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            // Spark promotes mixed Decimal/Integer to Decimal with computed precision
            assertThat(result).isInstanceOf(DecimalType.class);
        }

        @Test
        @DisplayName("Division of Integer by Decimal returns Decimal type (Spark promotes to Decimal)")
        void testMixedIntegerDecimalDivision() {
            Literal left = Literal.of(100);
            Literal right = new Literal("10.00", new DecimalType(10, 2));
            BinaryExpression division = BinaryExpression.divide(left, right);

            DataType result = TypeInferenceEngine.resolveBinaryExpressionType(division, null);

            // Spark promotes mixed Integer/Decimal to Decimal with computed precision
            assertThat(result).isInstanceOf(DecimalType.class);
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
