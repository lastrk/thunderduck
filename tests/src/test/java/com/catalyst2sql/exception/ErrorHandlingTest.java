package com.catalyst2sql.exception;

import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.expression.*;
import com.catalyst2sql.types.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Collections;

/**
 * Tests for comprehensive error handling and user-friendly error messages.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Exceptions provide useful context about what went wrong</li>
 *   <li>Error messages are user-friendly and actionable</li>
 *   <li>Technical details are available for debugging</li>
 *   <li>Failed operations include SQL/plan context</li>
 *   <li>Common errors are translated into helpful guidance</li>
 * </ul>
 */
@DisplayName("Error Handling Tests")
public class ErrorHandlingTest {

    @Nested
    @DisplayName("SQL Generation Exception Tests")
    class SQLGenerationExceptionTests {

        private SQLGenerator generator;

        @BeforeEach
        void setup() {
            generator = new SQLGenerator();
        }

        @Test
        @DisplayName("Unsupported operator throws exception with context")
        void testUnsupportedOperatorThrowsException() {
            // Given: Unsupported Aggregate plan
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            ColumnReference col = new ColumnReference("id", IntegerType.get());
            Aggregate.AggregateExpression aggExpr = new Aggregate.AggregateExpression("COUNT", col, "count_id");
            Aggregate aggregate = new Aggregate(
                scan,
                Collections.emptyList(), // No grouping
                Collections.singletonList(aggExpr)
            );

            // When/Then: Should throw exception
            assertThatThrownBy(() -> generator.generate(aggregate))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Aggregate");
        }

        @Test
        @DisplayName("SQL generation exception includes plan type")
        void testSQLGenerationExceptionIncludesPlanType() {
            // Given: Failed plan
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            SQLGenerationException ex = new SQLGenerationException(
                "Test error", scan);

            // Then: Message includes plan type
            assertThat(ex.getMessage()).contains("TableScan");
            assertThat(ex.getFailedPlan()).isSameAs(scan);
        }

        @Test
        @DisplayName("User message is simplified and actionable")
        void testGetUserMessageIsActionable() {
            // Given: SQL generation exception for unsupported Join
            TableScan scan1 = new TableScan("/tmp/left.parquet", TableScan.TableFormat.PARQUET, null);
            TableScan scan2 = new TableScan("/tmp/right.parquet", TableScan.TableFormat.PARQUET, null);
            Join join = new Join(scan1, scan2, Join.JoinType.INNER, Literal.of(true));

            SQLGenerationException ex = new SQLGenerationException(
                "Join not yet implemented", join);

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should be helpful and actionable
            assertThat(userMsg)
                .contains("join query")
                .contains("limited")
                .containsAnyOf("simplify", "support", "contact");
        }

        @Test
        @DisplayName("Technical message includes full context")
        void testTechnicalMessageIncludesContext() {
            // Given: Exception with cause
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            RuntimeException cause = new RuntimeException("Root cause");
            SQLGenerationException ex = new SQLGenerationException(
                "Generation failed", cause, scan);

            // When: Get technical message
            String techMsg = ex.getTechnicalMessage();

            // Then: Should include all details
            assertThat(techMsg)
                .contains("SQL Generation Failed")
                .contains("TableScan")
                .contains("Root cause");
        }

        @Test
        @DisplayName("Different plan types have specific user messages")
        void testDifferentPlanTypesHaveSpecificMessages() {
            // Given: Various unsupported plan types
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);

            // Aggregate message
            Aggregate agg = new Aggregate(scan, Collections.emptyList(), Collections.emptyList());
            SQLGenerationException aggEx = new SQLGenerationException("Failed", agg);
            assertThat(aggEx.getUserMessage())
                .containsIgnoringCase("aggregation");

            // Union message
            Union union = new Union(scan, scan);
            SQLGenerationException unionEx = new SQLGenerationException("Failed", union);
            assertThat(unionEx.getUserMessage())
                .containsIgnoringCase("union");

            // InMemoryRelation message
            InMemoryRelation inMem = new InMemoryRelation(
                Collections.emptyList(),
                new StructType(Collections.emptyList())
            );
            SQLGenerationException inMemEx = new SQLGenerationException("Failed", inMem);
            assertThat(inMemEx.getUserMessage())
                .containsIgnoringCase("in-memory")
                .containsIgnoringCase("Parquet");
        }
    }

    @Nested
    @DisplayName("Query Execution Exception Tests")
    class QueryExecutionExceptionTests {

        @Test
        @DisplayName("Column not found error with suggestions")
        void testColumnNotFoundTranslation() {
            // Given: Column not found error (from DuckDB)
            String duckdbError = "Binder Error: column \"nam\" not found\n" +
                                "Candidate Bindings: \"name\", \"age\", \"email\"";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELECT nam FROM users");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should suggest alternatives
            assertThat(userMsg)
                .contains("nam")
                .contains("not found")
                .containsAnyOf("name", "age", "email");
        }

        @Test
        @DisplayName("Type conversion error translation")
        void testConversionErrorTranslation() {
            // Given: Type conversion error
            String duckdbError = "Conversion Error: Could not convert string \"abc\" to 'INTEGER'";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELECT CAST('abc' AS INTEGER)");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should provide helpful message
            assertThat(userMsg)
                .contains("abc")
                .contains("INTEGER")
                .containsAnyOf("type", "convert", "mismatch");
        }

        @Test
        @DisplayName("Out of memory error with solutions")
        void testOutOfMemoryTranslation() {
            // Given: Out of memory error
            String duckdbError = "Out of Memory Error: failed to allocate block";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELECT * FROM huge_table");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should suggest solution
            assertThat(userMsg)
                .containsIgnoringCase("memory")
                .containsAnyOf("filter", "limit", "increase", "batch");
        }

        @Test
        @DisplayName("Syntax error includes context")
        void testSyntaxErrorTranslation() {
            // Given: Syntax error
            String duckdbError = "Parser Error: syntax error at or near \"SELCT\"";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELCT * FROM users");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should identify the problem
            assertThat(userMsg)
                .containsIgnoringCase("syntax")
                .containsAnyOf("SELCT", "typo", "error");
        }

        @Test
        @DisplayName("Query context is preserved")
        void testQueryContextIncluded() {
            // Given: Query execution exception with SQL
            String sql = "SELECT * FROM users WHERE age > 18";
            QueryExecutionException ex = new QueryExecutionException(
                "Execution failed", sql);

            // Then: SQL is accessible for debugging
            assertThat(ex.getFailedSQL()).isEqualTo(sql);
        }

        @Test
        @DisplayName("File not found error translation")
        void testFileNotFoundTranslation() {
            // Given: Catalog error for missing file
            String duckdbError = "Catalog Error: Table \"data.parquet\" does not exist";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELECT * FROM read_parquet('/tmp/data.parquet')");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should provide helpful guidance
            assertThat(userMsg)
                .containsIgnoringCase("not found")
                .containsAnyOf("file", "table", "path", "exists");
        }

        @Test
        @DisplayName("Technical message includes full details")
        void testTechnicalMessageComplete() {
            // Given: Exception with SQL and cause
            String sql = "SELECT * FROM users";
            RuntimeException cause = new RuntimeException("Connection timeout");
            QueryExecutionException ex = new QueryExecutionException(
                "Execution failed", cause, sql);

            // When: Get technical message
            String techMsg = ex.getTechnicalMessage();

            // Then: Should include everything
            assertThat(techMsg)
                .contains("Query Execution Failed")
                .contains(sql)
                .contains("Connection timeout");
        }

        @Test
        @DisplayName("Permission denied error translation")
        void testPermissionDeniedTranslation() {
            // Given: IO error for permission denied
            String duckdbError = "IO Error: Permission denied accessing file '/restricted/data.parquet'";

            QueryExecutionException ex = new QueryExecutionException(
                duckdbError, "SELECT * FROM read_parquet('/restricted/data.parquet')");

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should identify permission issue
            assertThat(userMsg)
                .containsIgnoringCase("permission")
                .containsAnyOf("denied", "access", "rights");
        }
    }

    @Nested
    @DisplayName("Error Message Quality Tests")
    class ErrorMessageQualityTests {

        @Test
        @DisplayName("User messages avoid technical jargon")
        void testUserMessagesAvoidJargon() {
            // Given: Various exceptions
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            SQLGenerationException sqlEx = new SQLGenerationException("Failed", scan);

            String duckdbError = "Binder Error: column not found";
            QueryExecutionException queryEx = new QueryExecutionException(duckdbError, null);

            // When: Get user messages
            String sqlMsg = sqlEx.getUserMessage();
            String queryMsg = queryEx.getUserMessage();

            // Then: Should not contain jargon
            assertThat(sqlMsg).doesNotContain("plan node", "visitor", "traversal");
            assertThat(queryMsg).doesNotContain("Binder Error", "Parser Error");
        }

        @Test
        @DisplayName("User messages are concise")
        void testUserMessagesAreConcise() {
            // Given: Exception
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            SQLGenerationException ex = new SQLGenerationException("Failed", scan);

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should be reasonably short
            assertThat(userMsg.length()).isLessThan(300);
        }

        @Test
        @DisplayName("User messages suggest action")
        void testUserMessagesSuggestAction() {
            // Given: Query exception
            String duckdbError = "Out of Memory Error: failed to allocate";
            QueryExecutionException ex = new QueryExecutionException(duckdbError, null);

            // When: Get user message
            String userMsg = ex.getUserMessage();

            // Then: Should suggest what to do
            assertThat(userMsg).containsAnyOf(
                "Try", "Check", "increase", "reduce", "add"
            );
        }
    }

    @Nested
    @DisplayName("Exception Context Tests")
    class ExceptionContextTests {

        @Test
        @DisplayName("SQL generation exception preserves plan")
        void testSQLGenerationExceptionPreservesPlan() {
            // Given: Failed plan
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            Filter filter = new Filter(scan, Literal.of(true));

            // When: Create exception
            SQLGenerationException ex = new SQLGenerationException("Failed", filter);

            // Then: Plan is preserved
            assertThat(ex.getFailedPlan()).isSameAs(filter);
            assertThat(ex.getFailedPlan()).isInstanceOf(Filter.class);
        }

        @Test
        @DisplayName("Query exception preserves SQL")
        void testQueryExceptionPreservesSQL() {
            // Given: Failed SQL
            String sql = "SELECT * FROM (SELECT * FROM users) AS t WHERE t.age > 18";

            // When: Create exception
            QueryExecutionException ex = new QueryExecutionException("Failed", sql);

            // Then: SQL is preserved
            assertThat(ex.getFailedSQL()).isEqualTo(sql);
        }

        @Test
        @DisplayName("Exception with cause preserves stack trace")
        void testExceptionPreservesStackTrace() {
            // Given: Exception with cause
            RuntimeException cause = new RuntimeException("Root cause");
            cause.fillInStackTrace();

            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);
            SQLGenerationException ex = new SQLGenerationException("Failed", cause, scan);

            // Then: Cause is preserved
            assertThat(ex.getCause()).isSameAs(cause);
            assertThat(ex.getCause().getStackTrace()).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Exception with null plan")
        void testExceptionWithNullPlan() {
            // Given: Exception with null plan
            SQLGenerationException ex = new SQLGenerationException("Failed", (LogicalPlan) null);

            // Then: Should handle gracefully
            assertThat(ex.getFailedPlan()).isNull();
            assertThat(ex.getUserMessage()).isNotNull();
            assertThat(ex.getMessage()).contains("null");
        }

        @Test
        @DisplayName("Exception with null SQL")
        void testExceptionWithNullSQL() {
            // Given: Exception with null SQL
            QueryExecutionException ex = new QueryExecutionException("Failed", (String) null);

            // Then: Should handle gracefully
            assertThat(ex.getFailedSQL()).isNull();
            assertThat(ex.getUserMessage()).isNotNull();
        }

        @Test
        @DisplayName("Exception with empty message")
        void testExceptionWithEmptyMessage() {
            // Given: Exception with empty message
            QueryExecutionException ex = new QueryExecutionException("", "SELECT 1");

            // Then: Should provide fallback message
            assertThat(ex.getUserMessage()).isNotEmpty();
        }

        @Test
        @DisplayName("Exception with very long SQL")
        void testExceptionWithLongSQL() {
            // Given: Very long SQL
            String longSQL = "SELECT * FROM users WHERE " +
                           "a".repeat(1000) + " = 1";

            QueryExecutionException ex = new QueryExecutionException("Failed", longSQL);

            // Then: Should handle gracefully
            assertThat(ex.getFailedSQL()).isEqualTo(longSQL);
            assertThat(ex.getUserMessage()).isNotNull();
        }
    }
}
