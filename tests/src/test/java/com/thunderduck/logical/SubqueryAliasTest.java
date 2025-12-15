package com.thunderduck.logical;

import com.thunderduck.connect.converter.ExpressionConverter;
import com.thunderduck.connect.converter.RelationConverter;
import com.thunderduck.connect.converter.PlanConversionException;
import com.thunderduck.generator.SQLGenerator;
import org.apache.spark.connect.proto.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SubqueryAlias relation conversion.
 *
 * SubqueryAlias provides a name for a relation, enabling:
 * - Column qualification: df.alias("t").select("t.col1")
 * - Self-joins: df.alias("a").join(df.alias("b"), ...)
 * - Disambiguation in complex queries
 */
@DisplayName("SubqueryAlias Tests")
public class SubqueryAliasTest {

    private RelationConverter relationConverter;
    private SQLGenerator sqlGenerator;

    @BeforeEach
    void setUp() {
        ExpressionConverter expressionConverter = new ExpressionConverter();
        relationConverter = new RelationConverter(expressionConverter);
        sqlGenerator = new SQLGenerator();
    }

    /**
     * Creates a simple Range relation as input for SubqueryAlias tests.
     */
    private Relation createRangeRelation(long start, long end, long step) {
        return Relation.newBuilder()
                .setRange(Range.newBuilder()
                        .setStart(start)
                        .setEnd(end)
                        .setStep(step)
                        .build())
                .build();
    }

    /**
     * Creates a SubqueryAlias relation wrapping the given input.
     */
    private Relation createSubqueryAlias(Relation input, String alias) {
        return Relation.newBuilder()
                .setSubqueryAlias(SubqueryAlias.newBuilder()
                        .setInput(input)
                        .setAlias(alias)
                        .build())
                .build();
    }

    /**
     * Creates a SubqueryAlias relation with qualifiers.
     */
    private Relation createSubqueryAliasWithQualifier(Relation input, String alias, String... qualifiers) {
        SubqueryAlias.Builder builder = SubqueryAlias.newBuilder()
                .setInput(input)
                .setAlias(alias);
        for (String q : qualifiers) {
            builder.addQualifier(q);
        }
        return Relation.newBuilder()
                .setSubqueryAlias(builder.build())
                .build();
    }

    @Nested
    @DisplayName("Basic Aliasing Tests")
    class BasicAliasingTests {

        @Test
        @DisplayName("Simple alias generates correct SQL")
        void simpleAliasGeneratesCorrectSQL() {
            Relation input = createRangeRelation(0, 10, 1);
            Relation aliased = createSubqueryAlias(input, "my_alias");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("AS \"my_alias\""),
                    "SQL should contain alias: " + sql);
            assertTrue(sql.contains("SELECT *"),
                    "SQL should be a SELECT * wrapper: " + sql);
        }

        @Test
        @DisplayName("Alias with special characters is properly quoted")
        void aliasWithSpecialCharactersIsQuoted() {
            Relation input = createRangeRelation(0, 5, 1);
            Relation aliased = createSubqueryAlias(input, "my-table");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("\"my-table\""),
                    "Alias with dash should be quoted: " + sql);
        }

        @Test
        @DisplayName("Alias with spaces is properly quoted")
        void aliasWithSpacesIsQuoted() {
            Relation input = createRangeRelation(0, 5, 1);
            Relation aliased = createSubqueryAlias(input, "my table");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("\"my table\""),
                    "Alias with space should be quoted: " + sql);
        }

        @Test
        @DisplayName("Single character alias works")
        void singleCharacterAliasWorks() {
            Relation input = createRangeRelation(0, 5, 1);
            Relation aliased = createSubqueryAlias(input, "t");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("AS \"t\""),
                    "Single char alias should work: " + sql);
        }
    }

    @Nested
    @DisplayName("Nested Operations Tests")
    class NestedOperationsTests {

        @Test
        @DisplayName("Alias wraps Range relation correctly")
        void aliasWrapsRangeRelation() {
            Relation input = createRangeRelation(0, 100, 1);
            Relation aliased = createSubqueryAlias(input, "numbers");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("range(0, 100, 1)"),
                    "SQL should contain range function: " + sql);
            assertTrue(sql.contains("AS \"numbers\""),
                    "SQL should contain alias: " + sql);
        }

        @Test
        @DisplayName("Chained aliases work correctly")
        void chainedAliasesWork() {
            Relation input = createRangeRelation(0, 10, 1);
            Relation aliased1 = createSubqueryAlias(input, "first");
            Relation aliased2 = createSubqueryAlias(aliased1, "second");

            LogicalPlan plan = relationConverter.convert(aliased2);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("AS \"second\""),
                    "SQL should contain outer alias: " + sql);
            assertTrue(sql.contains("AS \"first\""),
                    "SQL should contain inner alias: " + sql);
        }

        @Test
        @DisplayName("Alias after Filter works")
        void aliasAfterFilterWorks() {
            // Create Range -> Filter -> SubqueryAlias
            Relation range = createRangeRelation(0, 100, 1);

            // Add a filter using proto Filter
            Relation filtered = Relation.newBuilder()
                    .setFilter(org.apache.spark.connect.proto.Filter.newBuilder()
                            .setInput(range)
                            .setCondition(Expression.newBuilder()
                                    .setUnresolvedFunction(Expression.UnresolvedFunction.newBuilder()
                                            .setFunctionName(">")
                                            .addArguments(Expression.newBuilder()
                                                    .setUnresolvedAttribute(
                                                            Expression.UnresolvedAttribute.newBuilder()
                                                                    .setUnparsedIdentifier("id")))
                                            .addArguments(Expression.newBuilder()
                                                    .setLiteral(Expression.Literal.newBuilder()
                                                            .setInteger(50))))
                                    .build())
                            .build())
                    .build();

            Relation aliased = createSubqueryAlias(filtered, "filtered_data");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("AS \"filtered_data\""),
                    "SQL should contain alias: " + sql);
            assertTrue(sql.contains("> 50") || sql.contains(">50"),
                    "SQL should contain filter condition: " + sql);
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Empty alias throws exception")
        void emptyAliasThrowsException() {
            Relation input = createRangeRelation(0, 10, 1);

            // Build with empty alias
            Relation aliased = Relation.newBuilder()
                    .setSubqueryAlias(SubqueryAlias.newBuilder()
                            .setInput(input)
                            .setAlias("")
                            .build())
                    .build();

            assertThrows(PlanConversionException.class, () -> {
                relationConverter.convert(aliased);
            }, "Empty alias should throw exception");
        }

        @Test
        @DisplayName("Qualifier is logged but ignored")
        void qualifierIsIgnored() {
            Relation input = createRangeRelation(0, 10, 1);
            Relation aliased = createSubqueryAliasWithQualifier(input, "my_table", "catalog", "database");

            // Should not throw - qualifier is just logged and ignored
            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("AS \"my_table\""),
                    "SQL should contain alias even with qualifiers: " + sql);
            // Qualifier is not part of the SQL (intentionally ignored)
            assertFalse(sql.contains("catalog") || sql.contains("database"),
                    "Qualifiers should not appear in SQL: " + sql);
        }

        @Test
        @DisplayName("Numeric alias name works")
        void numericAliasNameWorks() {
            Relation input = createRangeRelation(0, 5, 1);
            Relation aliased = createSubqueryAlias(input, "123");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("\"123\""),
                    "Numeric alias should be quoted: " + sql);
        }

        @Test
        @DisplayName("Reserved word alias is properly quoted")
        void reservedWordAliasIsQuoted() {
            Relation input = createRangeRelation(0, 5, 1);
            Relation aliased = createSubqueryAlias(input, "select");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            assertTrue(sql.contains("\"select\""),
                    "Reserved word alias should be quoted: " + sql);
        }
    }

    @Nested
    @DisplayName("SQL Structure Tests")
    class SQLStructureTests {

        @Test
        @DisplayName("Generated SQL is valid subquery wrapper")
        void generatedSQLIsValidSubqueryWrapper() {
            Relation input = createRangeRelation(0, 10, 1);
            Relation aliased = createSubqueryAlias(input, "data");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            // Should have the pattern: SELECT * FROM (...) AS "alias"
            assertTrue(sql.startsWith("SELECT * FROM ("),
                    "SQL should start with SELECT * FROM (: " + sql);
            assertTrue(sql.contains(") AS \"data\""),
                    "SQL should end with ) AS alias: " + sql);
        }

        @Test
        @DisplayName("Alias preserves input relation structure")
        void aliasPreservesInputStructure() {
            Relation input = createRangeRelation(5, 15, 2);
            Relation aliased = createSubqueryAlias(input, "stepped_range");

            LogicalPlan plan = relationConverter.convert(aliased);
            String sql = sqlGenerator.generate(plan);

            // The inner range should be preserved
            assertTrue(sql.contains("range(5, 15, 2)"),
                    "Input range parameters should be preserved: " + sql);
        }
    }
}
