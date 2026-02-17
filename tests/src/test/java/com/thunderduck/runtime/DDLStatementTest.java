package com.thunderduck.runtime;

import com.thunderduck.exception.QueryExecutionException;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for DDL/DML statement execution.
 *
 * Tests verify:
 * 1. Statement execution (INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
 * 2. Row count verification
 * 3. Data persistence and consistency
 * 4. Error handling (invalid syntax, constraint violations)
 * 5. Integration workflows (CREATE → INSERT → SELECT)
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("DDL/DML Statement Tests")
public class DDLStatementTest extends TestBase {

    private DuckDBRuntime runtime;
    private QueryExecutor executor;

    @BeforeEach
    public void setUp() {
        // Use unique in-memory database per test for isolation
        runtime = DuckDBRuntime.create("jdbc:duckdb::memory:test_ddl_" + System.nanoTime());
        executor = new QueryExecutor(runtime);
    }

    @AfterEach
    public void tearDown() {
        if (runtime != null) {
            runtime.close();
        }
    }

    // Helper method to get row count from a table
    private long getRowCount(String tableName) {
        try {
            VectorSchemaRoot result = executor.executeQuery("SELECT COUNT(*) as cnt FROM " + tableName);
            return result.getVector(0).getObject(0) != null ?
                   ((Number) result.getVector(0).getObject(0)).longValue() : 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Helper method to verify a single row result
    private Object getScalarValue(String sql, String columnName) {
        try {
            VectorSchemaRoot result = executor.executeQuery(sql);
            if (result.getRowCount() == 0) {
                return null;
            }
            for (int i = 0; i < result.getSchema().getFields().size(); i++) {
                if (result.getSchema().getFields().get(i).getName().equals(columnName)) {
                    return result.getVector(i).getObject(0);
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ========================================
    // CATEGORY 1: INSERT OPERATIONS (5 tests)
    // ========================================

    @Test
    @DisplayName("INSERT single row should return row count 1")
    public void testInsertSingleRow() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE test_insert (id INT, name VARCHAR)");

        // Execute
        int rowsAffected = executor.executeUpdate("INSERT INTO test_insert VALUES (1, 'Alice')");

        // Verify
        assertThat(rowsAffected).isEqualTo(1);

        // Validate data
        long count = getRowCount("test_insert");
        assertThat(count).isEqualTo(1);

        Object id = getScalarValue("SELECT id FROM test_insert", "id");
        assertThat(id).isEqualTo(1);
    }

    @Test
    @DisplayName("INSERT multiple rows should return correct row count")
    public void testInsertMultipleRows() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE test_insert (id INT, name VARCHAR)");

        // Execute
        int rowsAffected = executor.executeUpdate(
            "INSERT INTO test_insert VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
        );

        // Verify
        assertThat(rowsAffected).isEqualTo(3);

        // Validate data
        long count = getRowCount("test_insert");
        assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("INSERT...SELECT should work correctly")
    public void testInsertSelect() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE source (id INT, name VARCHAR)");
        executor.executeUpdate("CREATE TABLE target (id INT, name VARCHAR)");
        executor.executeUpdate("INSERT INTO source VALUES (1, 'Alice'), (2, 'Bob')");

        // Execute
        int rowsAffected = executor.executeUpdate("INSERT INTO target SELECT * FROM source");

        // Verify
        assertThat(rowsAffected).isEqualTo(2);

        // Validate data
        long count = getRowCount("target");
        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("INSERT with NULL values should work")
    public void testInsertWithNull() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE test_null (id INT, name VARCHAR)");

        // Execute
        int rowsAffected = executor.executeUpdate("INSERT INTO test_null VALUES (1, NULL)");

        // Verify
        assertThat(rowsAffected).isEqualTo(1);

        // Validate NULL is stored
        Object name = getScalarValue("SELECT name FROM test_null", "name");
        assertThat(name).isNull();
    }

    @Test
    @DisplayName("INSERT with special characters should work")
    public void testInsertWithSpecialCharacters() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE test_special (id INT, text VARCHAR)");

        // Execute - using single quotes properly escaped
        int rowsAffected = executor.executeUpdate(
            "INSERT INTO test_special VALUES (1, 'O''Brien')"
        );

        // Verify
        assertThat(rowsAffected).isEqualTo(1);

        // Validate data
        Object text = getScalarValue("SELECT text FROM test_special WHERE id = 1", "text");
        assertThat(text.toString()).isEqualTo("O'Brien");
    }

    // CATEGORY 2: UPDATE OPERATIONS - REMOVED
    // UPDATE is only supported on V2 tables (Delta Lake, Iceberg, Hudi) in Spark 4.1.1
    // Standard CREATE TABLE creates V1 tables that don't support UPDATE

    // CATEGORY 3: DELETE OPERATIONS - REMOVED
    // DELETE is only supported on V2 tables (Delta Lake, Iceberg, Hudi) in Spark 4.1.1
    // Standard CREATE TABLE creates V1 tables that don't support DELETE

    // ========================================
    // CATEGORY 4: CREATE/DROP OPERATIONS (4 tests)
    // ========================================

    @Test
    @DisplayName("CREATE TABLE should work")
    public void testCreateTable() throws Exception {
        // Execute
        executor.executeUpdate("CREATE TABLE test_create (id INT, name VARCHAR)");

        // Verify table exists and has correct schema
        VectorSchemaRoot result = executor.executeQuery(
            "SELECT column_name, data_type FROM information_schema.columns " +
            "WHERE table_name = 'test_create' ORDER BY ordinal_position"
        );

        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getVector("column_name").getObject(0).toString()).isEqualTo("id");
        assertThat(result.getVector("column_name").getObject(1).toString()).isEqualTo("name");
    }

    @Test
    @DisplayName("DROP TABLE should work")
    public void testDropTable() throws Exception {
        // Setup
        executor.executeUpdate("CREATE TABLE test_drop (id INT)");

        // Execute
        executor.executeUpdate("DROP TABLE test_drop");

        // Verify table doesn't exist
        assertThatThrownBy(() -> {
            executor.executeQuery("SELECT * FROM test_drop");
        }).isInstanceOf(QueryExecutionException.class);
    }

    @Test
    @DisplayName("CREATE TABLE IF NOT EXISTS should be idempotent")
    public void testCreateTableIfNotExists() throws Exception {
        // Execute twice
        executor.executeUpdate("CREATE TABLE IF NOT EXISTS test_idempotent (id INT)");
        executor.executeUpdate("CREATE TABLE IF NOT EXISTS test_idempotent (id INT)");

        // Verify table exists
        long count = getRowCount("test_idempotent");
        assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("DROP TABLE IF EXISTS should be idempotent")
    public void testDropTableIfExists() throws Exception {
        // Execute twice (table doesn't exist second time)
        executor.executeUpdate("CREATE TABLE test_drop_idempotent (id INT)");
        executor.executeUpdate("DROP TABLE IF EXISTS test_drop_idempotent");
        executor.executeUpdate("DROP TABLE IF EXISTS test_drop_idempotent");

        // No exception should be thrown
    }

    // ========================================
    // CATEGORY 5: ERROR HANDLING (6 tests)
    // ========================================

    @Test
    @DisplayName("INSERT into non-existent table should throw exception")
    public void testInsertNonExistentTable() {
        assertThatThrownBy(() -> {
            executor.executeUpdate("INSERT INTO non_existent VALUES (1, 'test')");
        }).isInstanceOf(QueryExecutionException.class);
    }

    @Test
    @DisplayName("Invalid SQL syntax should throw exception")
    public void testInvalidSyntax() {
        assertThatThrownBy(() -> {
            executor.executeUpdate("INSERT INTO INVALID SYNTAX");
        }).isInstanceOf(QueryExecutionException.class);
    }

    @Test
    @DisplayName("Duplicate table creation should throw exception")
    public void testDuplicateTableCreation() throws Exception {
        executor.executeUpdate("CREATE TABLE test_duplicate (id INT)");

        assertThatThrownBy(() -> {
            executor.executeUpdate("CREATE TABLE test_duplicate (id INT)");
        }).isInstanceOf(QueryExecutionException.class);
    }

    @Test
    @DisplayName("Type mismatch in INSERT should throw exception")
    public void testTypeMismatchInsert() throws Exception {
        executor.executeUpdate("CREATE TABLE test_type (id INT, value INT)");

        assertThatThrownBy(() -> {
            executor.executeUpdate("INSERT INTO test_type VALUES (1, 'not_a_number')");
        }).isInstanceOf(QueryExecutionException.class);
    }

    // ========================================
    // CATEGORY 6: INTEGRATION WORKFLOWS (5 tests)
    // ========================================

    @Test
    @DisplayName("CREATE → INSERT → SELECT workflow")
    public void testCreateInsertSelectWorkflow() throws Exception {
        // CREATE
        executor.executeUpdate("CREATE TABLE workflow1 (id INT, name VARCHAR)");

        // INSERT
        executor.executeUpdate("INSERT INTO workflow1 VALUES (1, 'Alice'), (2, 'Bob')");

        // SELECT
        VectorSchemaRoot result = executor.executeQuery("SELECT * FROM workflow1 ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getVector("id").getObject(0)).isEqualTo(1);
        assertThat(result.getVector("name").getObject(0).toString()).isEqualTo("Alice");
        assertThat(result.getVector("id").getObject(1)).isEqualTo(2);
        assertThat(result.getVector("name").getObject(1).toString()).isEqualTo("Bob");
    }

    @Test
    @DisplayName("Transaction consistency - changes persist")
    public void testTransactionConsistency() throws Exception {
        executor.executeUpdate("CREATE TABLE trans (id INT, value INT)");
        executor.executeUpdate("INSERT INTO trans VALUES (1, 10)");

        // Create new executor with same runtime (same connection)
        QueryExecutor executor2 = new QueryExecutor(runtime);
        VectorSchemaRoot result = executor2.executeQuery("SELECT * FROM trans");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getVector("id").getObject(0)).isEqualTo(1);
        assertThat(result.getVector("value").getObject(0)).isEqualTo(10);
    }

    // ========================================
    // CATEGORY 7: EDGE CASES (5 tests)
    // ========================================

    @Test
    @DisplayName("INSERT with large number of rows")
    public void testInsertLargeNumberOfRows() throws Exception {
        executor.executeUpdate("CREATE TABLE test_large (id INT, value INT)");

        // Insert 1000 rows
        StringBuilder sql = new StringBuilder("INSERT INTO test_large VALUES ");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) sql.append(", ");
            sql.append("(").append(i).append(", ").append(i * 10).append(")");
        }

        int rowsAffected = executor.executeUpdate(sql.toString());
        assertThat(rowsAffected).isEqualTo(1000);

        // Verify count
        long count = getRowCount("test_large");
        assertThat(count).isEqualTo(1000);
    }

    @Test
    @DisplayName("TRUNCATE should work")
    public void testTruncate() throws Exception {
        executor.executeUpdate("CREATE TABLE test_truncate (id INT, value INT)");
        executor.executeUpdate("INSERT INTO test_truncate VALUES (1, 10), (2, 20), (3, 30)");

        // Execute TRUNCATE
        executor.executeUpdate("TRUNCATE test_truncate");

        // Verify table is empty
        long count = getRowCount("test_truncate");
        assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("ALTER TABLE should work")
    public void testAlterTable() throws Exception {
        executor.executeUpdate("CREATE TABLE test_alter (id INT)");

        // Add column
        executor.executeUpdate("ALTER TABLE test_alter ADD COLUMN name VARCHAR");

        // Verify new column exists
        VectorSchemaRoot result = executor.executeQuery(
            "SELECT column_name FROM information_schema.columns " +
            "WHERE table_name = 'test_alter' ORDER BY ordinal_position"
        );

        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getVector("column_name").getObject(0).toString()).isEqualTo("id");
        assertThat(result.getVector("column_name").getObject(1).toString()).isEqualTo("name");
    }
}
