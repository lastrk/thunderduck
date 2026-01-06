package com.thunderduck.functions;

import com.thunderduck.test.TestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for FunctionRegistry class.
 *
 * Tests 20+ scenarios covering:
 * - String function mappings (5 tests)
 * - Math function mappings (5 tests)
 * - Date/time function mappings (5 tests)
 * - Aggregate function mappings (5 tests)
 * - Function support checking (5+ tests)
 */
@DisplayName("FunctionRegistry Comprehensive Tests")
public class FunctionRegistryTest extends TestBase {

    // ==================== String Function Tests ====================

    @Nested
    @DisplayName("String Function Mappings")
    class StringFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-001: upper function translation")
        void testUpperFunctionTranslation() {
            logStep("Translate Spark upper() to DuckDB");

            String result = FunctionRegistry.translate("upper", "name");

            assertThat(result).isEqualTo("upper(name)");
        }

        @Test
        @DisplayName("TC-FUNC-002: lower function translation")
        void testLowerFunctionTranslation() {
            logStep("Translate Spark lower() to DuckDB");

            String result = FunctionRegistry.translate("lower", "name");

            assertThat(result).isEqualTo("lower(name)");
        }

        @Test
        @DisplayName("TC-FUNC-003: substring function translation")
        void testSubstringFunctionTranslation() {
            logStep("Translate Spark substring() to DuckDB");

            String result = FunctionRegistry.translate("substring", "name", "1", "5");

            assertThat(result).isEqualTo("substring(name, 1, 5)");
        }

        @Test
        @DisplayName("TC-FUNC-004: concat function translation")
        void testConcatFunctionTranslation() {
            logStep("Translate Spark concat() to DuckDB");

            String result = FunctionRegistry.translate("concat", "first_name", "' '", "last_name");

            assertThat(result).isEqualTo("concat(first_name, ' ', last_name)");
        }

        @Test
        @DisplayName("TC-FUNC-005: trim function translation")
        void testTrimFunctionTranslation() {
            logStep("Translate Spark trim() to DuckDB");

            String result = FunctionRegistry.translate("trim", "name");

            assertThat(result).isEqualTo("trim(name)");
        }

        @Test
        @DisplayName("TC-FUNC-006: replace function translation")
        void testReplaceFunctionTranslation() {
            logStep("Translate Spark replace() to DuckDB");

            String result = FunctionRegistry.translate("replace", "text", "'old'", "'new'");

            assertThat(result).isEqualTo("replace(text, 'old', 'new')");
        }
    }

    // ==================== Math Function Tests ====================

    @Nested
    @DisplayName("Math Function Mappings")
    class MathFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-007: abs function translation")
        void testAbsFunctionTranslation() {
            logStep("Translate Spark abs() to DuckDB");

            String result = FunctionRegistry.translate("abs", "value");

            assertThat(result).isEqualTo("abs(value)");
        }

        @Test
        @DisplayName("TC-FUNC-008: ceil function translation")
        void testCeilFunctionTranslation() {
            logStep("Translate Spark ceil() to DuckDB");

            String result = FunctionRegistry.translate("ceil", "price");

            assertThat(result).isEqualTo("ceil(price)");
        }

        @Test
        @DisplayName("TC-FUNC-009: floor function translation")
        void testFloorFunctionTranslation() {
            logStep("Translate Spark floor() to DuckDB");

            String result = FunctionRegistry.translate("floor", "price");

            assertThat(result).isEqualTo("floor(price)");
        }

        @Test
        @DisplayName("TC-FUNC-010: round function translation")
        void testRoundFunctionTranslation() {
            logStep("Translate Spark round() to DuckDB");

            String result = FunctionRegistry.translate("round", "price", "2");

            assertThat(result).isEqualTo("round(price, 2)");
        }

        @Test
        @DisplayName("TC-FUNC-011: sqrt function translation")
        void testSqrtFunctionTranslation() {
            logStep("Translate Spark sqrt() to DuckDB");

            String result = FunctionRegistry.translate("sqrt", "area");

            assertThat(result).isEqualTo("sqrt(area)");
        }

        @Test
        @DisplayName("TC-FUNC-012: pow function translation")
        void testPowFunctionTranslation() {
            logStep("Translate Spark pow() to DuckDB");

            String result = FunctionRegistry.translate("pow", "base", "exponent");

            assertThat(result).isEqualTo("pow(base, exponent)");
        }
    }

    // ==================== Date/Time Function Tests ====================

    @Nested
    @DisplayName("Date/Time Function Mappings")
    class DateTimeFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-013: year function translation")
        void testYearFunctionTranslation() {
            logStep("Translate Spark year() to DuckDB");

            String result = FunctionRegistry.translate("year", "order_date");

            assertThat(result).isEqualTo("year(order_date)");
        }

        @Test
        @DisplayName("TC-FUNC-014: month function translation")
        void testMonthFunctionTranslation() {
            logStep("Translate Spark month() to DuckDB");

            String result = FunctionRegistry.translate("month", "order_date");

            assertThat(result).isEqualTo("month(order_date)");
        }

        @Test
        @DisplayName("TC-FUNC-015: day function translation")
        void testDayFunctionTranslation() {
            logStep("Translate Spark day() to DuckDB");

            String result = FunctionRegistry.translate("day", "order_date");

            assertThat(result).isEqualTo("day(order_date)");
        }

        @Test
        @DisplayName("TC-FUNC-016: date_add function translation")
        void testDateAddFunctionTranslation() {
            logStep("Translate Spark date_add() to DuckDB");

            String result = FunctionRegistry.translate("date_add", "order_date", "7");

            assertThat(result).isEqualTo("date_add(order_date, 7)");
        }

        @Test
        @DisplayName("TC-FUNC-017: datediff function translation")
        void testDateDiffFunctionTranslation() {
            logStep("Translate Spark datediff() to DuckDB");

            String result = FunctionRegistry.translate("datediff", "end_date", "start_date");

            // DuckDB datediff requires unit ('day') and swapped argument order
            assertThat(result).containsIgnoringCase("datediff('day', start_date, end_date)");
        }

        @Test
        @DisplayName("TC-FUNC-018: current_date function translation")
        void testCurrentDateFunctionTranslation() {
            logStep("Translate Spark current_date() to DuckDB");

            String result = FunctionRegistry.translate("current_date");

            assertThat(result).isEqualTo("current_date()");
        }
    }

    // ==================== Aggregate Function Tests ====================

    @Nested
    @DisplayName("Aggregate Function Mappings")
    class AggregateFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-019: sum function translation")
        void testSumFunctionTranslation() {
            logStep("Translate Spark sum() to DuckDB");

            String result = FunctionRegistry.translate("sum", "amount");

            // SUM is wrapped with CAST to BIGINT for Spark compatibility
            assertThat(result).containsIgnoringCase("CAST(SUM(amount) AS BIGINT)");
        }

        @Test
        @DisplayName("TC-FUNC-020: avg function translation")
        void testAvgFunctionTranslation() {
            logStep("Translate Spark avg() to DuckDB");

            String result = FunctionRegistry.translate("avg", "score");

            assertThat(result).isEqualTo("avg(score)");
        }

        @Test
        @DisplayName("TC-FUNC-021: count function translation")
        void testCountFunctionTranslation() {
            logStep("Translate Spark count() to DuckDB");

            String result = FunctionRegistry.translate("count", "*");

            assertThat(result).isEqualTo("count(*)");
        }

        @Test
        @DisplayName("TC-FUNC-022: min function translation")
        void testMinFunctionTranslation() {
            logStep("Translate Spark min() to DuckDB");

            String result = FunctionRegistry.translate("min", "price");

            assertThat(result).isEqualTo("min(price)");
        }

        @Test
        @DisplayName("TC-FUNC-023: max function translation")
        void testMaxFunctionTranslation() {
            logStep("Translate Spark max() to DuckDB");

            String result = FunctionRegistry.translate("max", "price");

            assertThat(result).isEqualTo("max(price)");
        }

        @Test
        @DisplayName("TC-FUNC-024: stddev function translation")
        void testStddevFunctionTranslation() {
            logStep("Translate Spark stddev() to DuckDB");

            String result = FunctionRegistry.translate("stddev", "score");

            assertThat(result).isEqualTo("stddev(score)");
        }
    }

    // ==================== DISTINCT Aggregate Function Tests ====================

    @Nested
    @DisplayName("DISTINCT Aggregate Function Mappings")
    class DistinctAggregateFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-047: count_distinct generates COUNT(DISTINCT ...)")
        void testCountDistinctFunctionTranslation() {
            logStep("Translate count_distinct to COUNT(DISTINCT ...)");

            String result = FunctionRegistry.translate("count_distinct", "customer_id");

            assertThat(result).isEqualTo("COUNT(DISTINCT customer_id)");
        }

        @Test
        @DisplayName("TC-FUNC-048: sum_distinct generates SUM(DISTINCT ...)")
        void testSumDistinctFunctionTranslation() {
            logStep("Translate sum_distinct to SUM(DISTINCT ...)");

            String result = FunctionRegistry.translate("sum_distinct", "amount");

            assertThat(result).isEqualTo("CAST(SUM(DISTINCT amount) AS BIGINT)");
        }

        @Test
        @DisplayName("TC-FUNC-049: avg_distinct generates AVG(DISTINCT ...)")
        void testAvgDistinctFunctionTranslation() {
            logStep("Translate avg_distinct to AVG(DISTINCT ...)");

            String result = FunctionRegistry.translate("avg_distinct", "price");

            assertThat(result).isEqualTo("AVG(DISTINCT price)");
        }

        @Test
        @DisplayName("TC-FUNC-050: count_distinct with multiple args uses ROW")
        void testCountDistinctMultipleArgs() {
            logStep("Translate count_distinct with multiple arguments");

            String result = FunctionRegistry.translate("count_distinct", "col1", "col2");

            // DuckDB uses ROW() for counting distinct tuples
            assertThat(result).containsIgnoringCase("COUNT(DISTINCT ROW(col1, col2))");
        }
    }

    // ==================== Function Support Tests ====================

    @Nested
    @DisplayName("Function Support Checking")
    class FunctionSupportTests {

        @Test
        @DisplayName("TC-FUNC-025: isSupported returns true for registered function")
        void testIsSupportedTrue() {
            logStep("Check if upper() is supported");

            boolean supported = FunctionRegistry.isSupported("upper");

            assertThat(supported).isTrue();
        }

        @Test
        @DisplayName("TC-FUNC-026: isSupported returns false for unregistered function")
        void testIsSupportedFalse() {
            logStep("Check if nonexistent_function() is supported");

            boolean supported = FunctionRegistry.isSupported("nonexistent_function");

            assertThat(supported).isFalse();
        }

        @Test
        @DisplayName("TC-FUNC-027: isSupported handles null input")
        void testIsSupportedNull() {
            logStep("Check isSupported with null input");

            boolean supported = FunctionRegistry.isSupported(null);

            assertThat(supported).isFalse();
        }

        @Test
        @DisplayName("TC-FUNC-028: isSupported handles empty string")
        void testIsSupportedEmpty() {
            logStep("Check isSupported with empty string");

            boolean supported = FunctionRegistry.isSupported("");

            assertThat(supported).isFalse();
        }

        @Test
        @DisplayName("TC-FUNC-029: isSupported is case-insensitive")
        void testIsSupportedCaseInsensitive() {
            logStep("Check isSupported with different cases");

            boolean lowerCase = FunctionRegistry.isSupported("upper");
            boolean upperCase = FunctionRegistry.isSupported("UPPER");
            boolean mixedCase = FunctionRegistry.isSupported("Upper");

            assertThat(lowerCase).isTrue();
            assertThat(upperCase).isTrue();
            assertThat(mixedCase).isTrue();
        }

        @Test
        @DisplayName("TC-FUNC-030: getDuckDBFunction returns correct mapping")
        void testGetDuckDBFunction() {
            logStep("Get DuckDB function name for Spark function");

            var duckdbFunc = FunctionRegistry.getDuckDBFunction("upper");

            assertThat(duckdbFunc).isPresent();
            assertThat(duckdbFunc.get()).isEqualTo("upper");
        }

        @Test
        @DisplayName("TC-FUNC-031: getDuckDBFunction returns empty for unmapped")
        void testGetDuckDBFunctionUnmapped() {
            logStep("Get DuckDB function for unmapped Spark function");

            var duckdbFunc = FunctionRegistry.getDuckDBFunction("nonexistent");

            assertThat(duckdbFunc).isEmpty();
        }

        @Test
        @DisplayName("TC-FUNC-032: registeredFunctionCount returns positive number")
        void testRegisteredFunctionCount() {
            logStep("Get total count of registered functions");

            int count = FunctionRegistry.registeredFunctionCount();

            assertThat(count).isPositive();
            assertThat(count).isGreaterThan(100); // Should have 100+ functions
        }
    }

    // ==================== Error Handling Tests ====================

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("TC-FUNC-033: translate throws exception for unsupported function")
        void testTranslateUnsupportedFunction() {
            logStep("Translate unsupported function should throw exception");

            assertThatThrownBy(() -> FunctionRegistry.translate("unsupported_func", "arg"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported function");
        }

        @Test
        @DisplayName("TC-FUNC-034: translate throws exception for null function name")
        void testTranslateNullFunctionName() {
            logStep("Translate with null function name should throw exception");

            assertThatThrownBy(() -> FunctionRegistry.translate(null, "arg"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
        }

        @Test
        @DisplayName("TC-FUNC-035: translate throws exception for empty function name")
        void testTranslateEmptyFunctionName() {
            logStep("Translate with empty function name should throw exception");

            assertThatThrownBy(() -> FunctionRegistry.translate("", "arg"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
        }

        @Test
        @DisplayName("TC-FUNC-036: translate handles case-insensitive function names")
        void testTranslateCaseInsensitive() {
            logStep("Translate with different case variations");

            String lower = FunctionRegistry.translate("upper", "name");
            String upper = FunctionRegistry.translate("UPPER", "name");
            String mixed = FunctionRegistry.translate("Upper", "name");

            assertThat(lower).isEqualTo("upper(name)");
            assertThat(upper).isEqualTo("upper(name)");
            assertThat(mixed).isEqualTo("upper(name)");
        }
    }

    // ==================== Window Function Tests ====================

    @Nested
    @DisplayName("Window Function Mappings")
    class WindowFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-037: row_number function translation")
        void testRowNumberFunctionTranslation() {
            logStep("Translate Spark row_number() to DuckDB");

            String result = FunctionRegistry.translate("row_number");

            assertThat(result).isEqualTo("row_number()");
        }

        @Test
        @DisplayName("TC-FUNC-038: rank function translation")
        void testRankFunctionTranslation() {
            logStep("Translate Spark rank() to DuckDB");

            String result = FunctionRegistry.translate("rank");

            assertThat(result).isEqualTo("rank()");
        }

        @Test
        @DisplayName("TC-FUNC-039: dense_rank function translation")
        void testDenseRankFunctionTranslation() {
            logStep("Translate Spark dense_rank() to DuckDB");

            String result = FunctionRegistry.translate("dense_rank");

            assertThat(result).isEqualTo("dense_rank()");
        }

        @Test
        @DisplayName("TC-FUNC-040: lag function translation")
        void testLagFunctionTranslation() {
            logStep("Translate Spark lag() to DuckDB");

            String result = FunctionRegistry.translate("lag", "price", "1");

            assertThat(result).isEqualTo("lag(price, 1)");
        }

        @Test
        @DisplayName("TC-FUNC-041: lead function translation")
        void testLeadFunctionTranslation() {
            logStep("Translate Spark lead() to DuckDB");

            String result = FunctionRegistry.translate("lead", "price", "1");

            assertThat(result).isEqualTo("lead(price, 1)");
        }
    }

    // ==================== Array Function Tests ====================

    @Nested
    @DisplayName("Array Function Mappings")
    class ArrayFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-042: array_contains maps to list_contains")
        void testArrayContainsFunctionTranslation() {
            logStep("Translate Spark array_contains() to DuckDB");

            String result = FunctionRegistry.translate("array_contains", "tags", "'spark'");

            assertThat(result).isEqualTo("list_contains(tags, 'spark')");
        }

        @Test
        @DisplayName("TC-FUNC-043: size maps to len with INTEGER cast")
        void testSizeFunctionTranslation() {
            logStep("Translate Spark size() to DuckDB");

            String result = FunctionRegistry.translate("size", "items");

            // size() is cast to INTEGER to match Spark's return type
            assertThat(result).containsIgnoringCase("CAST(len(items) AS INTEGER)");
        }

        @Test
        @DisplayName("TC-FUNC-044: explode maps to unnest")
        void testExplodeFunctionTranslation() {
            logStep("Translate Spark explode() to DuckDB");

            String result = FunctionRegistry.translate("explode", "array_column");

            assertThat(result).isEqualTo("unnest(array_column)");
        }
    }

    // ==================== Conditional Function Tests ====================

    @Nested
    @DisplayName("Conditional Function Mappings")
    class ConditionalFunctionMappings {

        @Test
        @DisplayName("TC-FUNC-045: coalesce function translation")
        void testCoalesceFunctionTranslation() {
            logStep("Translate Spark coalesce() to DuckDB");

            String result = FunctionRegistry.translate("coalesce", "col1", "col2", "0");

            assertThat(result).isEqualTo("coalesce(col1, col2, 0)");
        }

        @Test
        @DisplayName("TC-FUNC-046: if function translation")
        void testIfFunctionTranslation() {
            logStep("Translate Spark if() to DuckDB");

            String result = FunctionRegistry.translate("if", "age > 18", "'adult'", "'minor'");

            assertThat(result).isEqualTo("if(age > 18, 'adult', 'minor')");
        }
    }
}
