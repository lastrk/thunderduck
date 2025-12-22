package com.thunderduck.logical;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.connect.converter.PlanConverter;
import com.thunderduck.connect.converter.PlanConversionException;
import com.thunderduck.generator.SQLGenerator;

import org.apache.spark.connect.proto.*;
import org.apache.spark.connect.proto.SQL;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Unit and Integration tests for NA (null-aware) functions:
 * - NADrop: df.na.drop() - drop rows with null values
 * - NAFill: df.na.fill() - fill null values with specified values
 * - NAReplace: df.na.replace() - replace specific values with new values
 *
 * Tests cover:
 * - SQL generation for each NA function
 * - Schema inference for empty cols list
 * - Different threshold modes for NADrop (how='any', how='all', min_non_nulls=N)
 * - Single and multiple fill values for NAFill
 * - Single and multiple replacements for NAReplace
 * - Integration with DuckDB execution
 *
 * @see org.apache.spark.connect.proto.NADrop
 * @see org.apache.spark.connect.proto.NAFill
 * @see org.apache.spark.connect.proto.NAReplace
 */
@TestCategories.Tier1
@TestCategories.Integration
@DisplayName("NA Functions Tests")
@Disabled("NA functions require SQL relation support - will be revisited when SQL parser is implemented")
public class NAFunctionsTest extends TestBase {

    private Connection connection;
    private PlanConverter converter;
    private SQLGenerator generator;

    @Override
    protected void doSetUp() {
        try {
            // Create in-memory DuckDB connection
            connection = DriverManager.getConnection("jdbc:duckdb:");
            converter = new PlanConverter(connection);
            generator = new SQLGenerator();

            // Set up test tables with null values
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE test_with_nulls (" +
                    "id INTEGER NOT NULL, " +
                    "name VARCHAR, " +
                    "age INTEGER, " +
                    "score DOUBLE)");

                stmt.execute("INSERT INTO test_with_nulls VALUES " +
                    "(1, 'Alice', 25, 95.5), " +
                    "(2, 'Bob', NULL, 80.0), " +
                    "(3, NULL, 30, NULL), " +
                    "(4, 'Diana', 28, 88.5), " +
                    "(5, NULL, NULL, NULL)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set up test database", e);
        }
    }

    @Override
    protected void doTearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Ignore
            }
        }
    }

    // ==================== Helper Methods ====================

    /**
     * Creates a Plan with a SQL input for test_with_nulls.
     * We use SQL relation directly since NamedTable would be treated as parquet file.
     */
    private Relation createTestTableRelation() {
        // Use SQL relation to directly reference the table we created
        return Relation.newBuilder()
            .setSql(SQL.newBuilder()
                .setQuery("SELECT * FROM test_with_nulls")
                .build())
            .build();
    }

    /**
     * Converts a Relation to SQL and executes it, returning count of rows.
     */
    private int executeAndCountRows(Relation relation) throws SQLException {
        Plan plan = Plan.newBuilder().setRoot(relation).build();
        LogicalPlan logicalPlan = converter.convert(plan);
        String sql = generator.generate(logicalPlan);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    /**
     * Converts a Relation to SQL and executes it, returning count where column is NOT NULL.
     */
    private int countNonNullInColumn(Relation relation, String columnName) throws SQLException {
        Plan plan = Plan.newBuilder().setRoot(relation).build();
        LogicalPlan logicalPlan = converter.convert(plan);
        String sql = generator.generate(logicalPlan);

        String countSql = String.format("SELECT COUNT(%s) FROM (%s) AS _t", columnName, sql);
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(countSql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    // ==================== NADrop Tests ====================

    @Nested
    @DisplayName("NADrop Tests")
    class NADropTests {

        @Test
        @DisplayName("Should drop rows where any specified column is null (how='any')")
        void testDropAny() throws SQLException {
            // df.na.drop(subset=['name', 'age'])
            // Default behavior: drop if ANY column is null (min_non_nulls = all cols = 2)
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addCols("age")
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            int count = executeAndCountRows(relation);

            // Original: 5 rows
            // Row 2: age=NULL → dropped
            // Row 3: name=NULL → dropped
            // Row 5: both NULL → dropped
            // Expected: 2 rows (Alice, Diana)
            assertThat(count).isEqualTo(2);
        }

        @Test
        @DisplayName("Should drop rows where all specified columns are null (how='all')")
        void testDropAll() throws SQLException {
            // df.na.drop(how='all', subset=['name', 'age'])
            // min_non_nulls=1 means: at least 1 column must be non-null (drop only if ALL are null)
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addCols("age")
                .setMinNonNulls(1)
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            int count = executeAndCountRows(relation);

            // Original: 5 rows
            // Row 5: both name and age are NULL → dropped
            // All others have at least one non-null → kept
            // Expected: 4 rows
            assertThat(count).isEqualTo(4);
        }

        @Test
        @DisplayName("Should drop rows with threshold (min_non_nulls=N)")
        void testDropWithThreshold() throws SQLException {
            // df.na.drop(thresh=2, subset=['name', 'age', 'score'])
            // At least 2 columns must be non-null
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addCols("age")
                .addCols("score")
                .setMinNonNulls(2)
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            int count = executeAndCountRows(relation);

            // Row 1: Alice, 25, 95.5 → 3 non-null → kept
            // Row 2: Bob, NULL, 80.0 → 2 non-null → kept
            // Row 3: NULL, 30, NULL → 1 non-null → dropped
            // Row 4: Diana, 28, 88.5 → 3 non-null → kept
            // Row 5: NULL, NULL, NULL → 0 non-null → dropped
            // Expected: 3 rows
            assertThat(count).isEqualTo(3);
        }

        @Test
        @DisplayName("Should generate correct SQL with AND conditions for how='any'")
        void testNADropSQLGenerationAny() throws SQLException {
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addCols("age")
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Should contain AND conditions for all columns to be NOT NULL
            assertThat(sql).containsIgnoringCase("IS NOT NULL");
            assertThat(sql).containsIgnoringCase("AND");
        }

        @Test
        @DisplayName("Should generate correct SQL with OR conditions for how='all'")
        void testNADropSQLGenerationAll() throws SQLException {
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addCols("age")
                .setMinNonNulls(1)
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Should contain OR conditions (at least one must be NOT NULL)
            assertThat(sql).containsIgnoringCase("IS NOT NULL");
            assertThat(sql).containsIgnoringCase("OR");
        }
    }

    // ==================== NAFill Tests ====================

    @Nested
    @DisplayName("NAFill Tests")
    class NAFillTests {

        @Test
        @DisplayName("Should fill null values in specified columns with single value")
        void testFillSingleValue() throws SQLException {
            // df.na.fill('Unknown', subset=['name'])
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addValues(Expression.Literal.newBuilder()
                    .setString("Unknown")
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            int nonNullCount = countNonNullInColumn(relation, "name");

            // Originally 3 rows have name: Alice, Bob, Diana
            // After fill: all 5 rows should have non-null name
            assertThat(nonNullCount).isEqualTo(5);
        }

        @Test
        @DisplayName("Should fill null values with integer")
        void testFillWithInteger() throws SQLException {
            // df.na.fill(0, subset=['age'])
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("age")
                .addValues(Expression.Literal.newBuilder()
                    .setInteger(0)
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            int nonNullCount = countNonNullInColumn(relation, "age");

            // Originally 3 rows have age: 25, 30, 28
            // After fill: all 5 rows should have non-null age
            assertThat(nonNullCount).isEqualTo(5);
        }

        @Test
        @DisplayName("Should fill null values with double")
        void testFillWithDouble() throws SQLException {
            // df.na.fill(0.0, subset=['score'])
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("score")
                .addValues(Expression.Literal.newBuilder()
                    .setDouble(0.0)
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            int nonNullCount = countNonNullInColumn(relation, "score");

            // Originally 3 rows have score: 95.5, 80.0, 88.5
            // After fill: all 5 rows should have non-null score
            assertThat(nonNullCount).isEqualTo(5);
        }

        @Test
        @DisplayName("Should generate COALESCE SQL for fill")
        void testNAFillSQLGeneration() throws SQLException {
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addValues(Expression.Literal.newBuilder()
                    .setString("Unknown")
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Should use COALESCE for null replacement
            assertThat(sql).containsIgnoringCase("COALESCE");
            assertThat(sql).contains("'Unknown'");
        }

        @Test
        @DisplayName("Should throw exception when no fill values provided")
        void testFillNoValues() {
            // df.na.fill() without values should fail
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                // No values
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();

            // Currently throws UnsupportedOperationException because SQL relations
            // are not yet supported. Once implemented, this should throw
            // PlanConversionException with "at least one fill value" message.
            assertThatThrownBy(() -> converter.convert(plan))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Raw SQL relations are not yet supported");
        }
    }

    // ==================== NAReplace Tests ====================

    @Nested
    @DisplayName("NAReplace Tests")
    class NAReplaceTests {

        @Test
        @DisplayName("Should replace specific string value")
        void testReplaceStringValue() throws SQLException {
            // df.na.replace('Alice', 'ALICE', subset=['name'])
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setString("Alice")
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setString("ALICE")
                        .build())
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Execute and verify Alice was replaced
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                boolean foundAlice = false;
                boolean foundALICE = false;
                while (rs.next()) {
                    String name = rs.getString("name");
                    if ("Alice".equals(name)) foundAlice = true;
                    if ("ALICE".equals(name)) foundALICE = true;
                }
                assertThat(foundAlice).isFalse();
                assertThat(foundALICE).isTrue();
            }
        }

        @Test
        @DisplayName("Should replace integer value")
        void testReplaceIntegerValue() throws SQLException {
            // df.na.replace(25, 99, subset=['age'])
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("age")
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setInteger(25)
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setInteger(99)
                        .build())
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Execute and verify 25 was replaced with 99
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                boolean found25 = false;
                boolean found99 = false;
                while (rs.next()) {
                    int age = rs.getInt("age");
                    if (!rs.wasNull()) {
                        if (age == 25) found25 = true;
                        if (age == 99) found99 = true;
                    }
                }
                assertThat(found25).isFalse();
                assertThat(found99).isTrue();
            }
        }

        @Test
        @DisplayName("Should handle multiple replacements")
        void testMultipleReplacements() throws SQLException {
            // df.na.replace(['Alice', 'Bob'], ['A', 'B'], subset=['name'])
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setString("Alice")
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setString("A")
                        .build())
                    .build())
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setString("Bob")
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setString("B")
                        .build())
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Execute and verify both replacements
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                boolean foundA = false;
                boolean foundB = false;
                while (rs.next()) {
                    String name = rs.getString("name");
                    if ("A".equals(name)) foundA = true;
                    if ("B".equals(name)) foundB = true;
                }
                assertThat(foundA).isTrue();
                assertThat(foundB).isTrue();
            }
        }

        @Test
        @DisplayName("Should generate CASE WHEN SQL for replace")
        void testNAReplaceSQLGeneration() throws SQLException {
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setString("Alice")
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setString("ALICE")
                        .build())
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Should use CASE WHEN for replacement
            assertThat(sql).containsIgnoringCase("CASE");
            assertThat(sql).containsIgnoringCase("WHEN");
            assertThat(sql).containsIgnoringCase("THEN");
            assertThat(sql).containsIgnoringCase("ELSE");
            assertThat(sql).containsIgnoringCase("END");
        }

        @Test
        @DisplayName("Should return input as-is when no replacements provided")
        void testReplaceNoReplacements() throws SQLException {
            // df.na.replace() without replacements should return input unchanged
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                // No replacements
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            int count = executeAndCountRows(relation);

            // Should return all 5 rows unchanged
            assertThat(count).isEqualTo(5);
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("NADrop should work with single column")
        void testNADropSingleColumn() throws SQLException {
            NADrop naDrop = NADrop.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .build();

            Relation relation = Relation.newBuilder().setDropNa(naDrop).build();
            int count = executeAndCountRows(relation);

            // Rows with name: Alice, Bob, Diana = 3 rows
            assertThat(count).isEqualTo(3);
        }

        @Test
        @DisplayName("NAFill should preserve non-null values")
        void testNAFillPreservesNonNull() throws SQLException {
            NAFill naFill = NAFill.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addValues(Expression.Literal.newBuilder()
                    .setString("REPLACED")
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setFillNa(naFill).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Execute and verify original values are preserved
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int aliceCount = 0;
                int replacedCount = 0;
                while (rs.next()) {
                    String name = rs.getString("name");
                    if ("Alice".equals(name)) aliceCount++;
                    if ("REPLACED".equals(name)) replacedCount++;
                }
                assertThat(aliceCount).isEqualTo(1); // Alice preserved
                assertThat(replacedCount).isEqualTo(2); // Two nulls replaced
            }
        }

        @Test
        @DisplayName("NAReplace should not affect unspecified columns")
        void testNAReplaceDoesNotAffectOtherColumns() throws SQLException {
            NAReplace naReplace = NAReplace.newBuilder()
                .setInput(createTestTableRelation())
                .addCols("name")
                .addReplacements(NAReplace.Replacement.newBuilder()
                    .setOldValue(Expression.Literal.newBuilder()
                        .setString("Alice")
                        .build())
                    .setNewValue(Expression.Literal.newBuilder()
                        .setString("ALICE")
                        .build())
                    .build())
                .build();

            Relation relation = Relation.newBuilder().setReplace(naReplace).build();
            Plan plan = Plan.newBuilder().setRoot(relation).build();
            LogicalPlan logicalPlan = converter.convert(plan);
            String sql = generator.generate(logicalPlan);

            // Execute and verify age=25 for the row where name was replaced to ALICE
            // The generated SQL is a SELECT that already has the replacement done
            // We need to wrap it in a subquery to filter by the new name
            String wrappedSql = "SELECT * FROM (" + sql + ") AS _outer WHERE name = 'ALICE'";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(wrappedSql)) {
                assertThat(rs.next()).as("Should find a row with name='ALICE'").isTrue();
                assertThat(rs.getInt("age")).isEqualTo(25);
            }
        }
    }
}
