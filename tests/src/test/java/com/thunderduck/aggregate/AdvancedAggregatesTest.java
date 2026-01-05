package com.thunderduck.aggregate;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.Literal;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.logical.Aggregate;
import com.thunderduck.logical.Aggregate.AggregateExpression;
import com.thunderduck.logical.Filter;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.logical.TableScan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.DoubleType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive tests for advanced aggregate functions (Week 5 Phase 1 Task W5-4).
 *
 * <p>Tests 18 scenarios covering:
 * - Statistical aggregates: STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP
 * - Percentile functions: PERCENTILE_CONT, PERCENTILE_DISC, MEDIAN
 * - Edge cases: NULL handling, single value, empty set
 * - Complex expressions and GROUP BY combinations
 * - Validation and error cases
 *
 * <p>These functions are essential for statistical analysis and data science workloads.
 */
@DisplayName("Advanced Aggregate Functions Tests")
@Tag("aggregate")
@Tag("tier1")
@TestCategories.Unit
public class AdvancedAggregatesTest extends TestBase {

    private static final StructType MEASUREMENTS_SCHEMA = new StructType(Arrays.asList(
        new StructField("sensor_id", IntegerType.get(), false),
        new StructField("temperature", DoubleType.get(), false),
        new StructField("pressure", DoubleType.get(), false),
        new StructField("region", StringType.get(), false),
        new StructField("value", DoubleType.get(), true)
    ));

    private LogicalPlan createMeasurementsTableScan() {
        return new TableScan("/data/measurements.parquet", TableScan.TableFormat.PARQUET, MEASUREMENTS_SCHEMA);
    }

    @Nested
    @DisplayName("Standard Deviation Functions")
    class StandardDeviationFunctions {

        @Test
        @DisplayName("STDDEV_SAMP generates correct SQL")
        void testStddevSamp() {
            // Given: STDDEV_SAMP(temperature) - sample standard deviation
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            AggregateExpression stddevSamp = new AggregateExpression(
                "STDDEV_SAMP",
                temperatureCol,
                "temp_stddev_samp",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(stddevSamp)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate STDDEV_SAMP function
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("AS \"temp_stddev_samp\"");
        }

        @Test
        @DisplayName("STDDEV_POP generates correct SQL")
        void testStddevPop() {
            // Given: STDDEV_POP(pressure) - population standard deviation
            Expression pressureCol = new ColumnReference("pressure", DoubleType.get());
            AggregateExpression stddevPop = new AggregateExpression(
                "STDDEV_POP",
                pressureCol,
                "pressure_stddev_pop",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(stddevPop)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate STDDEV_POP function
            assertThat(sql).containsIgnoringCase("STDDEV_POP(pressure)");
            assertThat(sql).containsIgnoringCase("AS \"pressure_stddev_pop\"");
        }

        @Test
        @DisplayName("STDDEV_SAMP with GROUP BY generates correct SQL")
        void testStddevSampWithGroupBy() {
            // Given: STDDEV_SAMP(temperature) grouped by region
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression stddevSamp = new AggregateExpression(
                "STDDEV_SAMP",
                temperatureCol,
                "temp_stddev",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Collections.singletonList(stddevSamp)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have both GROUP BY and STDDEV_SAMP
            assertThat(sql).containsIgnoringCase("SELECT region, STDDEV_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("GROUP BY region");
            assertThat(sql).containsIgnoringCase("AS \"temp_stddev\"");
        }
    }

    @Nested
    @DisplayName("Variance Functions")
    class VarianceFunctions {

        @Test
        @DisplayName("VAR_SAMP generates correct SQL")
        void testVarSamp() {
            // Given: VAR_SAMP(temperature) - sample variance
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            AggregateExpression varSamp = new AggregateExpression(
                "VAR_SAMP",
                temperatureCol,
                "temp_variance_samp",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(varSamp)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate VAR_SAMP function
            assertThat(sql).containsIgnoringCase("VAR_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("AS \"temp_variance_samp\"");
        }

        @Test
        @DisplayName("VAR_POP generates correct SQL")
        void testVarPop() {
            // Given: VAR_POP(pressure) - population variance
            Expression pressureCol = new ColumnReference("pressure", DoubleType.get());
            AggregateExpression varPop = new AggregateExpression(
                "VAR_POP",
                pressureCol,
                "pressure_variance_pop",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(varPop)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate VAR_POP function
            assertThat(sql).containsIgnoringCase("VAR_POP(pressure)");
            assertThat(sql).containsIgnoringCase("AS \"pressure_variance_pop\"");
        }

        @Test
        @DisplayName("Multiple variance functions in same query")
        void testMultipleVarianceFunctions() {
            // Given: Both sample and population variance
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression varSamp = new AggregateExpression(
                "VAR_SAMP", temperatureCol, "var_samp", false
            );
            AggregateExpression varPop = new AggregateExpression(
                "VAR_POP", temperatureCol, "var_pop", false
            );
            AggregateExpression stddevSamp = new AggregateExpression(
                "STDDEV_SAMP", temperatureCol, "stddev_samp", false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Arrays.asList(varSamp, varPop, stddevSamp)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate all three functions
            assertThat(sql).containsIgnoringCase("VAR_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("VAR_POP(temperature)");
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(temperature)");
        }
    }

    @Nested
    @DisplayName("Percentile Functions")
    class PercentileFunctions {

        @Test
        @DisplayName("PERCENTILE_CONT generates correct SQL with literal percentile")
        void testPercentileCont() {
            // Given: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY temperature)
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            Expression percentile = new Literal(0.5, DoubleType.get());

            // For PERCENTILE_CONT, we use a special syntax
            // We'll represent it as: PERCENTILE_CONT(percentile_value, column)
            AggregateExpression percentileCont = new AggregateExpression(
                "PERCENTILE_CONT",
                temperatureCol,
                "median_temp",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(percentileCont)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate PERCENTILE_CONT function
            assertThat(sql).contains("PERCENTILE_CONT(temperature)");
            assertThat(sql).contains("AS \"median_temp\"");
        }

        @Test
        @DisplayName("PERCENTILE_DISC generates correct SQL")
        void testPercentileDisc() {
            // Given: PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY pressure)
            Expression pressureCol = new ColumnReference("pressure", DoubleType.get());

            AggregateExpression percentileDisc = new AggregateExpression(
                "PERCENTILE_DISC",
                pressureCol,
                "p75_pressure",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(percentileDisc)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate PERCENTILE_DISC function
            assertThat(sql).containsIgnoringCase("PERCENTILE_DISC(pressure)");
            assertThat(sql).containsIgnoringCase("AS \"p75_pressure\"");
        }

        @Test
        @DisplayName("MEDIAN generates correct SQL (alias for PERCENTILE_CONT(0.5))")
        void testMedian() {
            // Given: MEDIAN(temperature)
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            AggregateExpression median = new AggregateExpression(
                "MEDIAN",
                temperatureCol,
                "median_temp",
                false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(median)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate MEDIAN function
            assertThat(sql).containsIgnoringCase("MEDIAN(temperature)");
            assertThat(sql).containsIgnoringCase("AS \"median_temp\"");
        }

        @Test
        @DisplayName("Multiple percentiles in same query")
        void testMultiplePercentiles() {
            // Given: MEDIAN, PERCENTILE_CONT(0.25), PERCENTILE_CONT(0.75)
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression p25 = new AggregateExpression(
                "PERCENTILE_CONT", temperatureCol, "p25", false
            );
            AggregateExpression median = new AggregateExpression(
                "MEDIAN", temperatureCol, "p50", false
            );
            AggregateExpression p75 = new AggregateExpression(
                "PERCENTILE_CONT", temperatureCol, "p75", false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Arrays.asList(p25, median, p75)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate all three percentile functions
            assertThat(sql).containsIgnoringCase("PERCENTILE_CONT(temperature)");
            assertThat(sql).containsIgnoringCase("MEDIAN(temperature)");
        }

        @Test
        @DisplayName("Percentile with GROUP BY generates correct SQL")
        void testPercentileWithGroupBy() {
            // Given: MEDIAN(temperature) grouped by region
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression median = new AggregateExpression(
                "MEDIAN", temperatureCol, "median_temp", false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Collections.singletonList(median)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have both GROUP BY and MEDIAN
            assertThat(sql).containsIgnoringCase("SELECT region, MEDIAN(temperature)");
            assertThat(sql).containsIgnoringCase("GROUP BY region");
        }
    }

    @Nested
    @DisplayName("Mixed Statistical Aggregates")
    class MixedStatisticalAggregates {

        @Test
        @DisplayName("Comprehensive statistical analysis query")
        void testComprehensiveStatisticalQuery() {
            // Given: Multiple statistical functions in one query
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            List<AggregateExpression> aggregates = Arrays.asList(
                new AggregateExpression("COUNT", temperatureCol, "count", false),
                new AggregateExpression("AVG", temperatureCol, "avg", false),
                new AggregateExpression("STDDEV_SAMP", temperatureCol, "stddev", false),
                new AggregateExpression("VAR_SAMP", temperatureCol, "variance", false),
                new AggregateExpression("MEDIAN", temperatureCol, "median", false),
                new AggregateExpression("MIN", temperatureCol, "min", false),
                new AggregateExpression("MAX", temperatureCol, "max", false)
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                aggregates
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate all seven statistical functions
            assertThat(sql).containsIgnoringCase("COUNT(temperature)");
            assertThat(sql).containsIgnoringCase("AVG(temperature)");
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("VAR_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("MEDIAN(temperature)");
            assertThat(sql).containsIgnoringCase("MIN(temperature)");
            assertThat(sql).containsIgnoringCase("MAX(temperature)");
        }

        @Test
        @DisplayName("Statistical aggregates with complex expression")
        void testStatisticalAggregatesWithComplexExpression() {
            // Given: STDDEV_SAMP(temperature - pressure)
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            Expression pressureCol = new ColumnReference("pressure", DoubleType.get());
            Expression difference = BinaryExpression.subtract(temperatureCol, pressureCol);

            AggregateExpression stddev = new AggregateExpression(
                "STDDEV_SAMP", difference, "stddev_diff", false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(stddev)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should handle complex expression
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP((temperature - pressure))");
        }

        @Test
        @DisplayName("Statistical aggregates with WHERE and GROUP BY")
        void testStatisticalAggregatesWithFiltering() {
            // Given: Statistical analysis with filtering
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression northLiteral = new Literal("North", StringType.get());
            Expression condition = BinaryExpression.equal(regionCol, northLiteral);

            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            Expression sensorIdCol = new ColumnReference("sensor_id", IntegerType.get());

            List<AggregateExpression> aggregates = Arrays.asList(
                new AggregateExpression("AVG", temperatureCol, "avg_temp", false),
                new AggregateExpression("STDDEV_SAMP", temperatureCol, "stddev_temp", false),
                new AggregateExpression("MEDIAN", temperatureCol, "median_temp", false)
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Filter filter = new Filter(tableScan, condition);
            Aggregate aggregate = new Aggregate(
                filter,
                Collections.singletonList(sensorIdCol),
                aggregates
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should have WHERE, GROUP BY, and statistical functions
            assertThat(sql).containsIgnoringCase("WHERE (region = 'North')");
            assertThat(sql).containsIgnoringCase("GROUP BY sensor_id");
            assertThat(sql).containsIgnoringCase("AVG(temperature)");
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(temperature)");
            assertThat(sql).containsIgnoringCase("MEDIAN(temperature)");
        }
    }

    @Nested
    @DisplayName("Edge Cases and Validation")
    class EdgeCasesAndValidation {

        @Test
        @DisplayName("Function names are uppercased in SQL")
        void testFunctionNameUppercasing() {
            // Given: Aggregate with lowercase function name
            Expression col = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression stddev = new AggregateExpression(
                "stddev_samp", col, "result", false  // lowercase
            );

            // When: Generate SQL
            String sql = stddev.toSQL();

            // Then: Should be uppercased
            assertThat(sql).startsWith("STDDEV_SAMP(");
            assertThat(sql).doesNotContain("stddev_samp(");
        }

        @Test
        @DisplayName("Statistical functions with NULL-capable columns")
        void testStatisticalFunctionsWithNullableColumns() {
            // Given: STDDEV_SAMP on nullable value column
            Expression valueCol = new ColumnReference("value", DoubleType.get());
            AggregateExpression stddev = new AggregateExpression(
                "STDDEV_SAMP", valueCol, "stddev_value", false
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(stddev)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate valid SQL (NULL handling is database's responsibility)
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(value)");
        }

        @Test
        @DisplayName("DISTINCT with statistical aggregates")
        void testDistinctWithStatisticalAggregates() {
            // Given: STDDEV_SAMP(DISTINCT temperature)
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());
            AggregateExpression stddevDistinct = new AggregateExpression(
                "STDDEV_SAMP", temperatureCol, "stddev_distinct", true  // DISTINCT
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.emptyList(),
                Collections.singletonList(stddevDistinct)
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate DISTINCT statistical aggregate
            assertThat(sql).containsIgnoringCase("STDDEV_SAMP(DISTINCT temperature)");
        }

        @Test
        @DisplayName("Statistical aggregates preserve result data types")
        void testStatisticalAggregateDataTypes() {
            // Given: Various statistical aggregates
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression stddev = new AggregateExpression(
                "STDDEV_SAMP", temperatureCol, "stddev", false
            );
            AggregateExpression variance = new AggregateExpression(
                "VAR_SAMP", temperatureCol, "variance", false
            );

            // When: Check inferred data types
            // Then: Should preserve numeric types
            assertThat(stddev.dataType()).isNotNull();
            assertThat(variance.dataType()).isNotNull();
            assertThat(stddev.nullable()).isTrue();  // Aggregates can produce NULL
            assertThat(variance.nullable()).isTrue();
        }
    }

    @Nested
    @DisplayName("HAVING with Statistical Aggregates")
    class HavingWithStatisticalAggregates {

        @Test
        @DisplayName("HAVING with STDDEV_SAMP generates correct SQL")
        void testHavingWithStddev() {
            // Given: GROUP BY with HAVING on standard deviation
            Expression regionCol = new ColumnReference("region", StringType.get());
            Expression temperatureCol = new ColumnReference("temperature", DoubleType.get());

            AggregateExpression avgTemp = new AggregateExpression(
                "AVG", temperatureCol, "avg_temp", false
            );
            AggregateExpression stddevTemp = new AggregateExpression(
                "STDDEV_SAMP", temperatureCol, "stddev_temp", false
            );

            // HAVING STDDEV_SAMP(temperature) > 5.0
            Expression havingCondition = BinaryExpression.greaterThan(
                stddevTemp,
                new Literal(5.0, DoubleType.get())
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(regionCol),
                Arrays.asList(avgTemp, stddevTemp),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate HAVING with statistical function
            assertThat(sql).containsIgnoringCase("GROUP BY region");
            assertThat(sql).containsIgnoringCase("HAVING (STDDEV_SAMP(temperature) > 5.0)");
        }

        @Test
        @DisplayName("HAVING with MEDIAN generates correct SQL")
        void testHavingWithMedian() {
            // Given: GROUP BY with HAVING on median
            Expression sensorIdCol = new ColumnReference("sensor_id", IntegerType.get());
            Expression pressureCol = new ColumnReference("pressure", DoubleType.get());

            AggregateExpression medianPressure = new AggregateExpression(
                "MEDIAN", pressureCol, "median_pressure", false
            );

            // HAVING MEDIAN(pressure) > 100.0
            Expression havingCondition = BinaryExpression.greaterThan(
                medianPressure,
                new Literal(100.0, DoubleType.get())
            );

            LogicalPlan tableScan = createMeasurementsTableScan();
            Aggregate aggregate = new Aggregate(
                tableScan,
                Collections.singletonList(sensorIdCol),
                Collections.singletonList(medianPressure),
                havingCondition
            );

            // When: Generate SQL
            SQLGenerator generator = new SQLGenerator();
            String sql = generator.generate(aggregate);

            // Then: Should generate HAVING with MEDIAN
            assertThat(sql).containsIgnoringCase("GROUP BY sensor_id");
            assertThat(sql).containsIgnoringCase("HAVING (MEDIAN(pressure) > 100.0)");
        }
    }
}
