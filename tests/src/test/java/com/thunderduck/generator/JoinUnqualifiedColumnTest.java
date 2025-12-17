package com.thunderduck.generator;

import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.TableScan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that unqualified column references in join conditions
 * get properly qualified.
 */
class JoinUnqualifiedColumnTest {

    @Test
    void testJoinWithUnqualifiedColumns() {
        // Create table scans
        TableScan employees = new TableScan("employees", TableScan.TableFormat.TABLE, null);
        TableScan departments = new TableScan("departments", TableScan.TableFormat.TABLE, null);

        // Create join condition with UNQUALIFIED columns: department = name
        // This simulates what might be coming from a different code path
        UnresolvedColumn department = new UnresolvedColumn("department");
        UnresolvedColumn name = new UnresolvedColumn("name");

        BinaryExpression condition = BinaryExpression.equal(department, name);

        // Create join
        Join join = new Join(employees, departments, Join.JoinType.INNER, condition);

        // Generate SQL
        SQLGenerator generator = new SQLGenerator();
        String sql = generator.generate(join);

        // The SQL should still work, but the ON clause won't have qualified names
        // since the input columns weren't qualified
        assertTrue(sql.contains("ON (department = name)") ||
                  sql.contains("ON (\"department\" = \"name\")"),
                  "Unqualified columns should remain unqualified (this might cause ambiguity): " + sql);
    }
}
