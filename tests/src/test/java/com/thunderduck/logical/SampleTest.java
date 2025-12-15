package com.thunderduck.logical;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.types.LongType;
import com.thunderduck.types.StructType;

import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.OptionalLong;

/**
 * Unit tests for Sample - the logical plan node that handles
 * df.sample(fraction) operations.
 *
 * Tests cover:
 * - SQL generation with Bernoulli sampling
 * - Seed support for reproducible sampling
 * - Schema inference (schema unchanged from child)
 * - Edge cases (boundary fractions, missing seed)
 * - Composability with other transformations
 *
 * @see Sample
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("Sample Unit Tests")
public class SampleTest extends TestBase {

    private SQLGenerator generator;

    @Override
    protected void doSetUp() {
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("Constructor Validation Tests")
    class ConstructorValidationTests {

        @Test
        @DisplayName("Should create Sample with valid fraction")
        void testValidConstruction() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1);

            assertThat(sample.fraction()).isEqualTo(0.1);
            assertThat(sample.seed()).isEmpty();
            assertThat(sample.child()).isEqualTo(range);
        }

        @Test
        @DisplayName("Should create Sample with fraction and seed")
        void testConstructionWithSeed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.5, OptionalLong.of(42L));

            assertThat(sample.fraction()).isEqualTo(0.5);
            assertThat(sample.seed()).isPresent();
            assertThat(sample.seed().getAsLong()).isEqualTo(42L);
        }

        @Test
        @DisplayName("Should reject negative fraction")
        void testNegativeFractionRejected() {
            RangeRelation range = new RangeRelation(0, 100, 1);

            assertThatThrownBy(() -> new Sample(range, -0.1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fraction must be between 0.0 and 1.0");
        }

        @Test
        @DisplayName("Should reject fraction greater than 1.0")
        void testFractionOverOneRejected() {
            RangeRelation range = new RangeRelation(0, 100, 1);

            assertThatThrownBy(() -> new Sample(range, 1.5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("fraction must be between 0.0 and 1.0");
        }

        @Test
        @DisplayName("Should accept boundary fraction 0.0")
        void testZeroFractionAllowed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.0);

            assertThat(sample.fraction()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("Should accept boundary fraction 1.0")
        void testOneFractionAllowed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 1.0);

            assertThat(sample.fraction()).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("Schema Inference Tests")
    class SchemaInferenceTests {

        @Test
        @DisplayName("Should preserve schema from child")
        void testSchemaPreserved() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1);

            StructType sampleSchema = sample.inferSchema();
            StructType rangeSchema = range.inferSchema();

            assertThat(sampleSchema.size()).isEqualTo(rangeSchema.size());
            assertThat(sampleSchema.fieldAt(0).name()).isEqualTo("id");
            assertThat(sampleSchema.fieldAt(0).dataType()).isInstanceOf(LongType.class);
        }
    }

    @Nested
    @DisplayName("SQL Generation Tests")
    class SQLGenerationTests {

        @Test
        @DisplayName("Should generate SQL with Bernoulli sampling")
        void testSimpleSampleSQL() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1);
            String sql = generator.generate(sample);

            // Should contain USING SAMPLE with bernoulli method
            assertThat(sql).contains("USING SAMPLE");
            assertThat(sql).containsIgnoringCase("bernoulli");
            // Should have 10% (0.1 * 100)
            assertThat(sql).contains("10.0");
        }

        @Test
        @DisplayName("Should generate SQL with seed")
        void testSampleSQLWithSeed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.25, OptionalLong.of(42L));
            String sql = generator.generate(sample);

            // Should contain USING SAMPLE with bernoulli method and seed
            assertThat(sql).contains("USING SAMPLE");
            assertThat(sql).containsIgnoringCase("bernoulli");
            // Should have 25% (0.25 * 100)
            assertThat(sql).contains("25.0");
            // Should include seed
            assertThat(sql).contains("42");
        }

        @Test
        @DisplayName("Should wrap child SQL correctly")
        void testChildWrapping() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.5);
            String sql = generator.generate(sample);

            // Should wrap child in SELECT FROM
            assertThat(sql).contains("SELECT * FROM (");
            assertThat(sql).contains(") AS");
        }

        @Test
        @DisplayName("Should handle very small fractions")
        void testSmallFraction() {
            RangeRelation range = new RangeRelation(0, 1000000, 1);
            Sample sample = new Sample(range, 0.001);  // 0.1%
            String sql = generator.generate(sample);

            assertThat(sql).contains("USING SAMPLE");
            assertThat(sql).contains("0.1");
            assertThat(sql).containsIgnoringCase("bernoulli");
        }
    }

    @Nested
    @DisplayName("ToString Tests")
    class ToStringTests {

        @Test
        @DisplayName("Should produce readable toString output without seed")
        void testToStringWithoutSeed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1);
            String str = sample.toString();

            assertThat(str)
                    .contains("Sample")
                    .contains("0.1");
        }

        @Test
        @DisplayName("Should produce readable toString output with seed")
        void testToStringWithSeed() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1, OptionalLong.of(42L));
            String str = sample.toString();

            assertThat(str)
                    .contains("Sample")
                    .contains("0.1")
                    .contains("42");
        }
    }

    @Nested
    @DisplayName("Composability Tests")
    class ComposabilityTests {

        @Test
        @DisplayName("Sample should have one child")
        void testHasOneChild() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.1);

            assertThat(sample.children()).hasSize(1);
            assertThat(sample.children().get(0)).isEqualTo(range);
        }

        @Test
        @DisplayName("Sample can be chained after Filter")
        void testSampleAfterFilter() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            // Create a simple filter condition: id > 50
            com.thunderduck.expression.Expression filterCondition =
                com.thunderduck.expression.BinaryExpression.greaterThan(
                    com.thunderduck.expression.ColumnReference.of("id", LongType.get()),
                    com.thunderduck.expression.Literal.of(50L)
                );
            Filter filtered = new Filter(range, filterCondition);
            Sample sample = new Sample(filtered, 0.2);

            String sql = generator.generate(sample);

            // Should contain both filter and sample
            assertThat(sql).contains("WHERE");
            assertThat(sql).contains("USING SAMPLE");
        }

        @Test
        @DisplayName("Sample can be wrapped by Project")
        void testProjectOnSample() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.5);

            com.thunderduck.expression.ColumnReference idCol =
                com.thunderduck.expression.ColumnReference.of("id", LongType.get());
            Project project = new Project(sample, List.of(idCol));

            String sql = generator.generate(project);

            // Should wrap sample in project
            assertThat(sql).contains("SELECT");
            assertThat(sql).contains("USING SAMPLE");
        }

        @Test
        @DisplayName("Sample can be followed by Limit")
        void testLimitAfterSample() {
            RangeRelation range = new RangeRelation(0, 100, 1);
            Sample sample = new Sample(range, 0.5);
            Limit limit = new Limit(sample, 10);

            String sql = generator.generate(limit);

            // Should have both sample and limit
            assertThat(sql).contains("USING SAMPLE");
            assertThat(sql).contains("LIMIT 10");
        }

        @Test
        @DisplayName("Multiple Samples can be chained")
        void testChainedSamples() {
            RangeRelation range = new RangeRelation(0, 1000, 1);
            Sample sample1 = new Sample(range, 0.5);  // First take 50%
            Sample sample2 = new Sample(sample1, 0.5);  // Then take 50% of that

            String sql = generator.generate(sample2);

            // Both samples should appear in SQL
            // Count occurrences of "USING SAMPLE"
            int count = 0;
            int index = 0;
            while ((index = sql.indexOf("USING SAMPLE", index)) != -1) {
                count++;
                index++;
            }
            assertThat(count).isEqualTo(2);
        }
    }
}
