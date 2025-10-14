package com.catalyst2sql.security;

import com.catalyst2sql.generator.SQLQuoting;
import com.catalyst2sql.generator.SQLGenerator;
import com.catalyst2sql.logical.*;
import com.catalyst2sql.expression.*;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Arrays;

/**
 * Security tests for SQL injection prevention.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Identifiers are properly quoted to prevent injection</li>
 *   <li>String literals are safely escaped</li>
 *   <li>File paths are validated before use</li>
 *   <li>Malicious inputs are rejected or neutralized</li>
 *   <li>End-to-end SQL generation is secure</li>
 * </ul>
 */
@DisplayName("SQL Injection Prevention Tests")
public class SQLInjectionTest {

    private SQLGenerator generator;

    @BeforeEach
    void setup() {
        generator = new SQLGenerator();
    }

    @Nested
    @DisplayName("Identifier Quoting Tests")
    class IdentifierQuotingTests {

        @Test
        @DisplayName("Quote simple identifier")
        void testQuoteSimpleIdentifier() {
            String quoted = SQLQuoting.quoteIdentifier("user_id");
            assertThat(quoted).isEqualTo("\"user_id\"");
        }

        @Test
        @DisplayName("Quote identifier with space")
        void testQuoteIdentifierWithSpace() {
            String quoted = SQLQuoting.quoteIdentifier("user name");
            assertThat(quoted).isEqualTo("\"user name\"");
        }

        @Test
        @DisplayName("Quote identifier with embedded quote")
        void testQuoteIdentifierWithQuote() {
            String quoted = SQLQuoting.quoteIdentifier("user\"name");
            // Should escape internal quote by doubling
            assertThat(quoted).isEqualTo("\"user\"\"name\"");
        }

        @Test
        @DisplayName("Quote SQL reserved word")
        void testQuoteReservedWord() {
            String quoted = SQLQuoting.quoteIdentifier("select");
            assertThat(quoted).isEqualTo("\"select\"");
        }

        @Test
        @DisplayName("Empty identifier throws exception")
        void testQuoteEmptyIdentifierThrows() {
            assertThatThrownBy(() -> SQLQuoting.quoteIdentifier(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or empty");
        }

        @Test
        @DisplayName("Null identifier throws exception")
        void testQuoteNullIdentifierThrows() {
            assertThatThrownBy(() -> SQLQuoting.quoteIdentifier(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be null or empty");
        }

        @Test
        @DisplayName("Multiple special characters in identifier")
        void testQuoteIdentifierWithMultipleSpecialChars() {
            String quoted = SQLQuoting.quoteIdentifier("user.name@domain");
            assertThat(quoted).isEqualTo("\"user.name@domain\"");
        }
    }

    @Nested
    @DisplayName("Literal Quoting Tests")
    class LiteralQuotingTests {

        @Test
        @DisplayName("Quote simple string literal")
        void testQuoteSimpleLiteral() {
            String quoted = SQLQuoting.quoteLiteral("hello");
            assertThat(quoted).isEqualTo("'hello'");
        }

        @Test
        @DisplayName("Quote literal with single quote")
        void testQuoteLiteralWithSingleQuote() {
            String quoted = SQLQuoting.quoteLiteral("it's");
            // Should escape quote by doubling
            assertThat(quoted).isEqualTo("'it''s'");
        }

        @Test
        @DisplayName("Quote null literal")
        void testQuoteNullLiteral() {
            String quoted = SQLQuoting.quoteLiteral(null);
            assertThat(quoted).isEqualTo("NULL");
        }

        @Test
        @DisplayName("Quote literal with multiple quotes")
        void testQuoteLiteralWithMultipleQuotes() {
            String quoted = SQLQuoting.quoteLiteral("'quoted' string");
            assertThat(quoted).isEqualTo("'''quoted'' string'");
        }

        @Test
        @DisplayName("Quote empty string literal")
        void testQuoteEmptyStringLiteral() {
            String quoted = SQLQuoting.quoteLiteral("");
            assertThat(quoted).isEqualTo("''");
        }

        @Test
        @DisplayName("Quote literal with SQL keywords")
        void testQuoteLiteralWithSQLKeywords() {
            String quoted = SQLQuoting.quoteLiteral("SELECT * FROM users");
            assertThat(quoted).isEqualTo("'SELECT * FROM users'");
            // Should be safely quoted, not executable
        }
    }

    @Nested
    @DisplayName("File Path Quoting Tests")
    class FilePathQuotingTests {

        @Test
        @DisplayName("Quote valid file path")
        void testQuoteValidFilePath() {
            String quoted = SQLQuoting.quoteFilePath("/tmp/data.parquet");
            assertThat(quoted).isEqualTo("'/tmp/data.parquet'");
        }

        @Test
        @DisplayName("Quote file path with spaces")
        void testQuoteFilePathWithSpaces() {
            String quoted = SQLQuoting.quoteFilePath("/tmp/my data.parquet");
            assertThat(quoted).isEqualTo("'/tmp/my data.parquet'");
        }

        @Test
        @DisplayName("SQL injection in file path is rejected")
        void testSQLInjectionInFilePathRejected() {
            String maliciousPath = "/tmp/data.parquet'; DROP TABLE users; --";

            assertThatThrownBy(() -> SQLQuoting.quoteFilePath(maliciousPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SQL injection");
        }

        @Test
        @DisplayName("SQL comment in file path is rejected")
        void testSQLCommentInFilePathRejected() {
            String maliciousPath = "/tmp/data.parquet -- comment";

            assertThatThrownBy(() -> SQLQuoting.quoteFilePath(maliciousPath))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Multi-line comment in file path is rejected")
        void testMultilineCommentInFilePathRejected() {
            String maliciousPath = "/tmp/data.parquet /* comment */";

            assertThatThrownBy(() -> SQLQuoting.quoteFilePath(maliciousPath))
                .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Multiple malicious patterns rejected")
        void testMultipleMaliciousPatternsRejected() {
            String[] maliciousPaths = {
                "/tmp/file'; DROP DATABASE; --",
                "/tmp/file; DELETE FROM users",
                "/tmp/file' OR '1'='1",
                "/tmp/file' UNION SELECT * FROM passwords --"
            };

            for (String path : maliciousPaths) {
                assertThatThrownBy(() -> SQLQuoting.quoteFilePath(path))
                    .as("Should reject: " + path)
                    .isInstanceOf(IllegalArgumentException.class);
            }
        }

        @Test
        @DisplayName("Null file path is rejected")
        void testNullFilePathRejected() {
            assertThatThrownBy(() -> SQLQuoting.quoteFilePath(null))
                .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("Table Name Security Tests")
    class TableNameSecurityTests {

        @Test
        @DisplayName("Quote simple table name")
        void testQuoteSimpleTableName() {
            String quoted = SQLQuoting.quoteTableName("users");
            assertThat(quoted).isEqualTo("\"users\"");
        }

        @Test
        @DisplayName("Malicious table name with semicolon rejected")
        void testMaliciousTableNameWithSemicolon() {
            String malicious = "users; DROP TABLE users";

            assertThatThrownBy(() -> SQLQuoting.quoteTableName(malicious))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid characters");
        }

        @Test
        @DisplayName("Malicious table name with comment rejected")
        void testMaliciousTableNameWithComment() {
            String malicious = "users-- comment";

            assertThatThrownBy(() -> SQLQuoting.quoteTableName(malicious))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid characters");
        }
    }

    @Nested
    @DisplayName("End-to-End SQL Injection Tests")
    class EndToEndSQLInjectionTests {

        @Test
        @DisplayName("Malicious table name in TableScan is escaped")
        void testMaliciousTableNameInTableScan() {
            // Given: Table scan with malicious name (file path)
            // Note: TableScan uses file paths, not table names directly
            String maliciousPath = "/tmp/users.parquet";
            TableScan scan = new TableScan(maliciousPath, TableScan.TableFormat.PARQUET, null);

            // When: Generate SQL
            String sql = generator.generate(scan);

            // Then: Path should be properly quoted
            assertThat(sql).contains("'/tmp/users.parquet'");
            assertThat(sql).contains("read_parquet");
        }

        @Test
        @DisplayName("Malicious column alias is escaped")
        void testMaliciousColumnAlias() {
            // Given: Project with malicious alias
            TableScan scan = new TableScan("/tmp/users.parquet", TableScan.TableFormat.PARQUET, null);
            ColumnReference col = new ColumnReference("id", com.catalyst2sql.types.IntegerType.get());
            String maliciousAlias = "user\"; DROP TABLE users; --";

            Project project = new Project(
                scan,
                Collections.singletonList(col),
                Collections.singletonList(maliciousAlias)
            );

            // When: Generate SQL
            String sql = generator.generate(project);

            // Then: Alias should be escaped (double quotes doubled)
            assertThat(sql).contains("\"user\"\"; DROP TABLE users; --\"");
            // And: Should not contain unescaped semicolon that could execute DROP
            assertThat(sql).doesNotMatch(".*[^\"]; DROP TABLE.*");
        }

        @Test
        @DisplayName("Malicious file path in TableScan is rejected")
        void testMaliciousFilePathInTableScan() {
            // Given: Table scan with malicious file path
            String maliciousPath = "/tmp/data.parquet'; DROP DATABASE; --";

            // Then: Should be rejected during SQL generation
            assertThatThrownBy(() -> {
                TableScan scan = new TableScan(maliciousPath, TableScan.TableFormat.PARQUET, null);
                generator.generate(scan);
            }).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("Complex query with multiple potential injection points")
        void testComplexQueryWithMultipleInjectionPoints() {
            // Given: Complex query with multiple potentially dangerous strings
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);

            // Column with SQL keywords
            ColumnReference col1 = new ColumnReference("SELECT", com.catalyst2sql.types.StringType.get());
            String alias1 = "result"; // Safe

            // Column with special characters
            ColumnReference col2 = new ColumnReference("user\"name", com.catalyst2sql.types.StringType.get());
            String alias2 = "name's_value"; // Has quote

            Project project = new Project(
                scan,
                Arrays.asList(col1, col2),
                Arrays.asList(alias1, alias2)
            );

            // When: Generate SQL
            String sql = generator.generate(project);

            // Then: All identifiers should be properly quoted
            assertThat(sql).contains("\"SELECT\"");
            assertThat(sql).contains("\"result\"");
            assertThat(sql).contains("\"user\"\"name\""); // Double quote escaped
            assertThat(sql).contains("\"name's_value\""); // Single quote in identifier is OK
        }

        @Test
        @DisplayName("Filter with malicious condition value")
        void testFilterWithMaliciousCondition() {
            // Given: Filter with literal that looks like SQL injection
            TableScan scan = new TableScan("/tmp/data.parquet", TableScan.TableFormat.PARQUET, null);

            // Create filter: WHERE name = 'malicious' OR '1'='1'
            ColumnReference nameCol = new ColumnReference("name", com.catalyst2sql.types.StringType.get());
            Literal maliciousValue = Literal.of("' OR '1'='1");

            BinaryExpression condition = new BinaryExpression(
                nameCol,
                BinaryExpression.Operator.EQUAL,
                maliciousValue
            );

            Filter filter = new Filter(scan, condition);

            // When: Generate SQL
            String sql = generator.generate(filter);

            // Then: The literal should be properly escaped
            // The single quotes in the value should be doubled
            assertThat(sql).contains("''' OR ''1''=''1'"); // All quotes doubled
        }
    }

    @Nested
    @DisplayName("Safety Validation Tests")
    class SafetyValidationTests {

        @Test
        @DisplayName("isSafe returns true for safe strings")
        void testIsSafeReturnsTrueForSafeStrings() {
            assertThat(SQLQuoting.isSafe("users")).isTrue();
            assertThat(SQLQuoting.isSafe("user_id")).isTrue();
            assertThat(SQLQuoting.isSafe("/tmp/data.parquet")).isTrue();
            assertThat(SQLQuoting.isSafe("")).isTrue();
            assertThat(SQLQuoting.isSafe(null)).isTrue();
        }

        @Test
        @DisplayName("isSafe returns false for dangerous strings")
        void testIsSafeReturnsFalseForDangerousStrings() {
            assertThat(SQLQuoting.isSafe("users'; DROP TABLE users; --")).isFalse();
            assertThat(SQLQuoting.isSafe("data -- comment")).isFalse();
            assertThat(SQLQuoting.isSafe("/* comment */")).isFalse();
            assertThat(SQLQuoting.isSafe("' OR '1'='1")).isFalse();
        }
    }

    @Nested
    @DisplayName("Edge Cases and Special Characters")
    class EdgeCasesTests {

        @Test
        @DisplayName("Unicode characters in identifiers")
        void testUnicodeCharactersInIdentifiers() {
            String unicode = "用户名"; // Chinese characters
            String quoted = SQLQuoting.quoteIdentifier(unicode);
            assertThat(quoted).isEqualTo("\"" + unicode + "\"");
        }

        @Test
        @DisplayName("Very long identifier")
        void testVeryLongIdentifier() {
            String longId = "a".repeat(1000);
            String quoted = SQLQuoting.quoteIdentifier(longId);
            assertThat(quoted).startsWith("\"");
            assertThat(quoted).endsWith("\"");
            assertThat(quoted).hasSize(1002); // Original + 2 quotes
        }

        @Test
        @DisplayName("Identifier with newlines")
        void testIdentifierWithNewlines() {
            String withNewline = "user\nname";
            String quoted = SQLQuoting.quoteIdentifier(withNewline);
            assertThat(quoted).isEqualTo("\"user\nname\"");
        }

        @Test
        @DisplayName("Literal with backslashes")
        void testLiteralWithBackslashes() {
            String withBackslash = "path\\to\\file";
            String quoted = SQLQuoting.quoteLiteral(withBackslash);
            assertThat(quoted).isEqualTo("'path\\to\\file'");
        }
    }
}
