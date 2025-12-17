package com.thunderduck.generator;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.ExtractValueExpression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.TableScan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JoinSQLTest {

    @Test
    void testSimpleJoinWithQualifiedColumns() {
        // Create table scans
        TableScan employees = new TableScan("employees", TableScan.TableFormat.TABLE, null);
        TableScan departments = new TableScan("departments", TableScan.TableFormat.TABLE, null);

        // Create join condition: employees.department = departments.name
        // This mimics how Spark Connect sends qualified column references
        UnresolvedColumn empBase = new UnresolvedColumn("employees");
        ExtractValueExpression empDept = ExtractValueExpression.structField(empBase, "department");

        UnresolvedColumn deptBase = new UnresolvedColumn("departments");
        ExtractValueExpression deptName = ExtractValueExpression.structField(deptBase, "name");

        BinaryExpression condition = BinaryExpression.equal(empDept, deptName);

        // Create join
        Join join = new Join(employees, departments, Join.JoinType.INNER, condition);

        // Generate SQL
        SQLGenerator generator = new SQLGenerator();
        String sql = generator.generate(join);

        // Check that the SQL uses table names directly (not subqueries)
        assertTrue(sql.contains("FROM \"employees\"") || sql.contains("FROM employees"),
                   "Should use table name directly: " + sql);
        assertTrue(sql.contains("JOIN \"departments\"") || sql.contains("JOIN departments"),
                   "Should use table name directly: " + sql);

        // Check that the ON clause has qualified column names
        assertTrue(sql.contains("employees.department") || sql.contains("\"employees\".\"department\""),
                   "ON clause should have qualified column: " + sql);
        assertTrue(sql.contains("departments.name") || sql.contains("\"departments\".\"name\""),
                   "ON clause should have qualified column: " + sql);
    }
}
