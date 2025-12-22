package com.thunderduck.logical;

import com.thunderduck.connect.converter.ExpressionConverter;
import com.thunderduck.connect.converter.RelationConverter;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.schema.SchemaInferrer;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;

import org.apache.spark.connect.proto.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for Unpivot relation conversion.
 *
 * <p>Tests cover:
 * - Basic unpivot with explicit value columns
 * - Unpivot with inferred value columns (values=None)
 * - Multiple ID columns
 * - Custom variable/value column names
 * - SQL generation correctness
 * - End-to-end execution with DuckDB
 *
 * @see com.thunderduck.connect.converter.RelationConverter#convertUnpivot
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("Unpivot Relation Tests")
@Disabled("Unpivot requires SQL relation support - will be revisited when SQL parser is implemented")
public class UnpivotTest extends TestBase {

    private Connection connection;
    private SchemaInferrer schemaInferrer;
    private RelationConverter relationConverter;
    private SQLGenerator sqlGenerator;

    @Override
    protected void doSetUp() {
        try {
            // Create in-memory DuckDB connection
            connection = DriverManager.getConnection("jdbc:duckdb:");
            schemaInferrer = new SchemaInferrer(connection);
            relationConverter = new RelationConverter(new ExpressionConverter(), schemaInferrer);
            sqlGenerator = new SQLGenerator();

            // Set up test tables for unpivot operations
            try (Statement stmt = connection.createStatement()) {
                // Wide format table for unpivot tests
                stmt.execute("CREATE TABLE sales_wide (" +
                    "product VARCHAR NOT NULL, " +
                    "region VARCHAR NOT NULL, " +
                    "q1_sales INTEGER, " +
                    "q2_sales INTEGER, " +
                    "q3_sales INTEGER, " +
                    "q4_sales INTEGER)");

                stmt.execute("INSERT INTO sales_wide VALUES " +
                    "('Widget', 'North', 100, 150, 200, 250), " +
                    "('Gadget', 'South', 80, 90, 110, 130), " +
                    "('Widget', 'South', 120, 140, 180, 220)");

                // Simple table for basic tests
                stmt.execute("CREATE TABLE simple_wide (" +
                    "id INTEGER NOT NULL, " +
                    "a INTEGER, " +
                    "b INTEGER, " +
                    "c INTEGER)");

                stmt.execute("INSERT INTO simple_wide VALUES " +
                    "(1, 10, 20, 30), " +
                    "(2, 40, 50, 60)");
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

    /**
     * Creates an UnresolvedAttribute expression for a column name.
     */
    private Expression createColumnExpr(String columnName) {
        return Expression.newBuilder()
            .setUnresolvedAttribute(Expression.UnresolvedAttribute.newBuilder()
                .setUnparsedIdentifier(columnName)
                .build())
            .build();
    }

    /**
     * Creates a SQL relation for the input table.
     */
    private Relation createSqlRelation(String sql) {
        return Relation.newBuilder()
            .setSql(SQL.newBuilder()
                .setQuery(sql)
                .build())
            .build();
    }

    @Nested
    @DisplayName("Basic Unpivot Tests")
    class BasicUnpivotTests {

        @Test
        @DisplayName("Should generate basic UNPIVOT SQL with explicit value columns")
        void testBasicUnpivotSQL() {
            // Build Unpivot: unpivot columns a, b, c with id as identifier
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .addValues(createColumnExpr("b"))
                    .addValues(createColumnExpr("c"))
                    .build())
                .setVariableColumnName("variable")
                .setValueColumnName("value");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            // Verify SQL structure
            assertThat(sql).containsIgnoringCase("UNPIVOT");
            assertThat(sql).containsIgnoringCase("FOR");
            assertThat(sql).containsIgnoringCase("IN");
            assertThat(sql).contains("\"a\"", "\"b\"", "\"c\"");
            assertThat(sql).contains("\"variable\"");
            assertThat(sql).contains("\"value\"");
        }

        @Test
        @DisplayName("Should execute basic unpivot and return correct number of rows")
        void testBasicUnpivotExecution() throws SQLException {
            // 2 rows * 3 value columns = 6 output rows
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .addValues(createColumnExpr("b"))
                    .addValues(createColumnExpr("c"))
                    .build())
                .setVariableColumnName("col_name")
                .setValueColumnName("col_value");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 2 input rows * 3 value columns = 6 output rows
                assertThat(rowCount).isEqualTo(6);
            }
        }

        @Test
        @DisplayName("Should unpivot with correct values")
        void testUnpivotValues() throws SQLException {
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide WHERE id = 1"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .addValues(createColumnExpr("b"))
                    .build())
                .setVariableColumnName("column")
                .setValueColumnName("val");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            List<String[]> results = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    results.add(new String[]{
                        rs.getString("id"),
                        rs.getString("column"),
                        rs.getString("val")
                    });
                }
            }

            // Should have 2 rows (a and b columns)
            assertThat(results).hasSize(2);

            // Verify values exist (order may vary)
            assertThat(results).anySatisfy(row -> {
                assertThat(row[0]).isEqualTo("1");
                assertThat(row[1]).isEqualTo("a");
                assertThat(row[2]).isEqualTo("10");
            });
            assertThat(results).anySatisfy(row -> {
                assertThat(row[0]).isEqualTo("1");
                assertThat(row[1]).isEqualTo("b");
                assertThat(row[2]).isEqualTo("20");
            });
        }
    }

    @Nested
    @DisplayName("Schema Inference Tests")
    class SchemaInferenceTests {

        @Test
        @DisplayName("Should infer value columns when values is not specified")
        void testInferValueColumns() throws SQLException {
            // Unpivot with values=None should infer all non-ID columns
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                // Note: no setValues() call - values will be inferred
                .setVariableColumnName("column")
                .setValueColumnName("val");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            // Should include all non-ID columns (a, b, c)
            assertThat(sql).contains("\"a\"", "\"b\"", "\"c\"");

            // Execute and verify
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 2 input rows * 3 inferred value columns = 6 output rows
                assertThat(rowCount).isEqualTo(6);
            }
        }

        @Test
        @DisplayName("Should handle multiple ID columns when inferring values")
        void testMultipleIdColumnsInference() throws SQLException {
            // Unpivot sales_wide with product and region as IDs
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM sales_wide"))
                .addIds(createColumnExpr("product"))
                .addIds(createColumnExpr("region"))
                // No values - infer q1_sales, q2_sales, q3_sales, q4_sales
                .setVariableColumnName("quarter")
                .setValueColumnName("sales");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            // Should include all non-ID columns
            assertThat(sql).contains("\"q1_sales\"", "\"q2_sales\"", "\"q3_sales\"", "\"q4_sales\"");

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 3 input rows * 4 quarters = 12 output rows
                assertThat(rowCount).isEqualTo(12);
            }
        }
    }

    @Nested
    @DisplayName("Column Naming Tests")
    class ColumnNamingTests {

        @Test
        @DisplayName("Should use custom variable and value column names")
        void testCustomColumnNames() throws SQLException {
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .addValues(createColumnExpr("b"))
                    .build())
                .setVariableColumnName("metric_name")
                .setValueColumnName("metric_value");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            assertThat(sql).contains("\"metric_name\"");
            assertThat(sql).contains("\"metric_value\"");

            // Verify column names in result
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                assertThat(rs.next()).isTrue();
                // Column names should be accessible
                assertThat(rs.getString("metric_name")).isNotNull();
                assertThat(rs.getString("metric_value")).isNotNull();
            }
        }

        @Test
        @DisplayName("Should use default column names when not specified")
        void testDefaultColumnNames() throws SQLException {
            // Build without setting column names explicitly
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .build())
                .setVariableColumnName("")  // Empty = use default
                .setValueColumnName("");    // Empty = use default

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            // Should use defaults: "variable" and "value"
            assertThat(sql).contains("\"variable\"");
            assertThat(sql).contains("\"value\"");
        }
    }

    @Nested
    @DisplayName("Sales Data Unpivot Tests")
    class SalesDataUnpivotTests {

        @Test
        @DisplayName("Should unpivot quarterly sales data correctly")
        void testQuarterlySalesUnpivot() throws SQLException {
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM sales_wide WHERE product = 'Widget' AND region = 'North'"))
                .addIds(createColumnExpr("product"))
                .addIds(createColumnExpr("region"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("q1_sales"))
                    .addValues(createColumnExpr("q2_sales"))
                    .addValues(createColumnExpr("q3_sales"))
                    .addValues(createColumnExpr("q4_sales"))
                    .build())
                .setVariableColumnName("quarter")
                .setValueColumnName("sales");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            List<String[]> results = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    results.add(new String[]{
                        rs.getString("product"),
                        rs.getString("region"),
                        rs.getString("quarter"),
                        rs.getString("sales")
                    });
                }
            }

            // Should have 4 rows (one per quarter)
            assertThat(results).hasSize(4);

            // Verify Q1 sales
            assertThat(results).anySatisfy(row -> {
                assertThat(row[0]).isEqualTo("Widget");
                assertThat(row[1]).isEqualTo("North");
                assertThat(row[2]).isEqualTo("q1_sales");
                assertThat(row[3]).isEqualTo("100");
            });

            // Verify Q4 sales
            assertThat(results).anySatisfy(row -> {
                assertThat(row[0]).isEqualTo("Widget");
                assertThat(row[1]).isEqualTo("North");
                assertThat(row[2]).isEqualTo("q4_sales");
                assertThat(row[3]).isEqualTo("250");
            });
        }

        @Test
        @DisplayName("Should handle partial column selection in unpivot")
        void testPartialColumnUnpivot() throws SQLException {
            // Only unpivot Q1 and Q2
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM sales_wide"))
                .addIds(createColumnExpr("product"))
                .addIds(createColumnExpr("region"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("q1_sales"))
                    .addValues(createColumnExpr("q2_sales"))
                    .build())
                .setVariableColumnName("quarter")
                .setValueColumnName("sales");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    String quarter = rs.getString("quarter");
                    // Only Q1 and Q2 should appear
                    assertThat(quarter).isIn("q1_sales", "q2_sales");
                    rowCount++;
                }
                // 3 input rows * 2 quarters = 6 output rows
                assertThat(rowCount).isEqualTo(6);
            }
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle single value column")
        void testSingleValueColumn() throws SQLException {
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT * FROM simple_wide"))
                .addIds(createColumnExpr("id"))
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .build())
                .setVariableColumnName("col")
                .setValueColumnName("val");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    assertThat(rs.getString("col")).isEqualTo("a");
                    rowCount++;
                }
                // 2 input rows * 1 value column = 2 output rows
                assertThat(rowCount).isEqualTo(2);
            }
        }

        @Test
        @DisplayName("Should handle no ID columns (all columns become values)")
        void testNoIdColumns() throws SQLException {
            // Special case: unpivot all columns
            Unpivot.Builder unpivotBuilder = Unpivot.newBuilder()
                .setInput(createSqlRelation("SELECT a, b FROM simple_wide WHERE id = 1"))
                // No ID columns
                .setValues(Unpivot.Values.newBuilder()
                    .addValues(createColumnExpr("a"))
                    .addValues(createColumnExpr("b"))
                    .build())
                .setVariableColumnName("col")
                .setValueColumnName("val");

            Relation relation = Relation.newBuilder()
                .setUnpivot(unpivotBuilder.build())
                .build();

            LogicalPlan plan = relationConverter.convert(relation);
            String sql = sqlGenerator.generate(plan);

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                // 1 input row * 2 value columns = 2 output rows
                assertThat(rowCount).isEqualTo(2);
            }
        }
    }
}
