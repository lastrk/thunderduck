package com.thunderduck.expression;

import com.thunderduck.logical.Aggregate;
import com.thunderduck.logical.Filter;
import com.thunderduck.logical.Join;
import com.thunderduck.logical.Project;
import com.thunderduck.logical.TableScan;
import com.thunderduck.logical.LogicalPlan;
import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
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

/**
 * Comprehensive tests for Subquery expressions (Week 4).
 *
 * <p>Tests 15 scenarios covering:
 * - Scalar subqueries (SELECT clause, WHERE clause, correlated, with aggregation)
 * - IN subqueries (simple, NOT IN, correlated, with JOINs)
 * - EXISTS subqueries (correlated, NOT EXISTS, with JOINs, multiple)
 */
@DisplayName("Subquery Tests")
@Tag("expression")
@Tag("tier1")
@TestCategories.Unit
public class SubqueryTest extends TestBase {

    // Helper method to create a simple products schema
    private StructType createProductsSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("price", IntegerType.get(), true),
            new StructField("category", StringType.get(), true)
        ));
    }

    // Helper method to create a simple orders schema
    private StructType createOrdersSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("product_id", IntegerType.get(), true),
            new StructField("quantity", IntegerType.get(), true),
            new StructField("customer_id", IntegerType.get(), true)
        ));
    }

    // Helper method to create a simple customers schema
    private StructType createCustomersSchema() {
        return new StructType(Arrays.asList(
            new StructField("id", IntegerType.get(), false),
            new StructField("name", StringType.get(), true),
            new StructField("status", StringType.get(), true)
        ));
    }

    @Nested
    @DisplayName("Scalar Subqueries")
    class ScalarSubqueries {

        @Test
        @DisplayName("Scalar subquery in SELECT clause")
        void testScalarSubqueryInSelect() {
            // Given: A scalar subquery that returns MAX(price) from products
            StructType productsSchema = createProductsSchema();
            LogicalPlan productsTable = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, productsSchema);

            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Aggregate.AggregateExpression maxPrice = new Aggregate.AggregateExpression("MAX", priceCol, "max_price");

            LogicalPlan maxPriceAgg = new Aggregate(
                productsTable,
                Collections.emptyList(),  // No grouping - global aggregation
                Collections.singletonList(maxPrice)
            );

            ScalarSubquery scalarSub = new ScalarSubquery(maxPriceAgg);

            // When: Generate SQL
            String sql = scalarSub.toSQL();

            // Then: Should be wrapped in parentheses with subquery
            assertThat(sql).startsWith("(");
            assertThat(sql).endsWith(")");
            assertThat(sql).containsIgnoringCase("SELECT MAX(price)");
            assertThat(sql).containsIgnoringCase("FROM");
            assertThat(sql).containsIgnoringCase("products.parquet");
        }

        @Test
        @DisplayName("Scalar subquery in WHERE clause")
        void testScalarSubqueryInWhere() {
            // Given: WHERE price > (SELECT AVG(price) FROM products)
            StructType productsSchema = createProductsSchema();
            LogicalPlan productsTable = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, productsSchema);

            Expression priceCol = new ColumnReference("price", IntegerType.get());
            Aggregate.AggregateExpression avgPrice = new Aggregate.AggregateExpression("AVG", priceCol, "avg_price");

            LogicalPlan avgPriceSubquery = new Aggregate(
                productsTable,
                Collections.emptyList(),
                Collections.singletonList(avgPrice)
            );

            ScalarSubquery scalarSub = new ScalarSubquery(avgPriceSubquery);

            // When: Generate SQL
            String sql = scalarSub.toSQL();

            // Then: Should generate valid subquery SQL
            assertThat(sql).containsIgnoringCase("SELECT AVG(price)");
            assertThat(sql).startsWith("(");
            assertThat(sql).endsWith(")");
            assertThat(scalarSub.nullable()).isTrue();
        }

        @Test
        @DisplayName("Correlated scalar subquery with outer reference")
        void testCorrelatedScalarSubquery() {
            // Given: A correlated subquery that references outer table
            // SELECT *, (SELECT COUNT(*) FROM orders WHERE orders.product_id = products.id) as order_count FROM products
            StructType productsSchema = createProductsSchema();
            StructType ordersSchema = createOrdersSchema();

            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Correlated filter: orders.product_id = products.id (outer reference)
            Expression orderProductId = new ColumnReference("product_id", IntegerType.get());
            Expression productId = new ColumnReference("id", IntegerType.get());  // outer reference
            Expression correlationPredicate = BinaryExpression.equal(orderProductId, productId);

            LogicalPlan filteredOrders = new Filter(ordersTable, correlationPredicate);

            // COUNT(*) aggregation
            Expression countStar = new Literal("*", StringType.get());
            Aggregate.AggregateExpression countExpr = new Aggregate.AggregateExpression("COUNT", countStar, "order_count");

            LogicalPlan countAgg = new Aggregate(
                filteredOrders,
                Collections.emptyList(),
                Collections.singletonList(countExpr)
            );

            ScalarSubquery scalarSub = new ScalarSubquery(countAgg);

            // When: Generate SQL
            String sql = scalarSub.toSQL();

            // Then: Should contain correlation condition
            assertThat(sql).containsIgnoringCase("SELECT COUNT(*)");
            assertThat(sql).containsIgnoringCase("orders.parquet");
            assertThat(sql).containsIgnoringCase("WHERE");
            assertThat(sql).containsIgnoringCase("product_id");
            assertThat(sql).containsIgnoringCase("id");
        }

        @Test
        @DisplayName("Scalar subquery with aggregation")
        void testScalarSubqueryWithAggregation() {
            // Given: Scalar subquery with SUM aggregation
            StructType ordersSchema = createOrdersSchema();
            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            Expression quantityCol = new ColumnReference("quantity", IntegerType.get());
            Aggregate.AggregateExpression sumQuantity = new Aggregate.AggregateExpression("SUM", quantityCol, "total_quantity");

            LogicalPlan sumAgg = new Aggregate(
                ordersTable,
                Collections.emptyList(),
                Collections.singletonList(sumQuantity)
            );

            ScalarSubquery scalarSub = new ScalarSubquery(sumAgg);

            // When: Generate SQL and check data type
            String sql = scalarSub.toSQL();

            // Then: Should generate aggregation SQL (SUM may be wrapped with CAST)
            assertThat(sql).containsIgnoringCase("sum(quantity)");
            assertThat(sql).containsIgnoringCase("FROM");
            assertThat(sql).startsWith("(");
            assertThat(sql).endsWith(")");

            // Data type should match aggregate result type
            assertThat(scalarSub.dataType()).isNotNull();
        }

        @Test
        @DisplayName("Multiple scalar subqueries in one query")
        void testMultipleScalarSubqueries() {
            // Given: Two scalar subqueries - MAX(price) and MIN(price)
            StructType productsSchema = createProductsSchema();
            LogicalPlan productsTable1 = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, productsSchema);
            LogicalPlan productsTable2 = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, productsSchema);

            Expression priceCol1 = new ColumnReference("price", IntegerType.get());
            Aggregate.AggregateExpression maxPrice = new Aggregate.AggregateExpression("MAX", priceCol1, "max_price");
            LogicalPlan maxAgg = new Aggregate(productsTable1, Collections.emptyList(), Collections.singletonList(maxPrice));

            Expression priceCol2 = new ColumnReference("price", IntegerType.get());
            Aggregate.AggregateExpression minPrice = new Aggregate.AggregateExpression("MIN", priceCol2, "min_price");
            LogicalPlan minAgg = new Aggregate(productsTable2, Collections.emptyList(), Collections.singletonList(minPrice));

            ScalarSubquery maxSubquery = new ScalarSubquery(maxAgg);
            ScalarSubquery minSubquery = new ScalarSubquery(minAgg);

            // When: Generate SQL for both
            String maxSql = maxSubquery.toSQL();
            String minSql = minSubquery.toSQL();

            // Then: Both should be valid subqueries
            assertThat(maxSql).containsIgnoringCase("SELECT MAX(price)");
            assertThat(minSql).containsIgnoringCase("SELECT MIN(price)");
            assertThat(maxSubquery.nullable()).isTrue();
            assertThat(minSubquery.nullable()).isTrue();
            assertThat(maxSubquery.toString()).contains("ScalarSubquery");
            assertThat(minSubquery.toString()).contains("ScalarSubquery");
        }
    }

    @Nested
    @DisplayName("IN Subqueries")
    class InSubqueries {

        @Test
        @DisplayName("IN subquery with simple value list")
        void testInSubquerySimple() {
            // Given: category IN (SELECT name FROM categories WHERE active = true)
            StructType categoriesSchema = new StructType(Arrays.asList(
                new StructField("id", IntegerType.get(), false),
                new StructField("name", StringType.get(), true),
                new StructField("active", IntegerType.get(), true)
            ));

            LogicalPlan categoriesTable = new TableScan("categories.parquet", TableScan.TableFormat.PARQUET, categoriesSchema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            LogicalPlan nameProject = new Project(categoriesTable, Collections.singletonList(nameCol));

            Expression testExpr = new ColumnReference("category", StringType.get());
            InSubquery inSubquery = new InSubquery(testExpr, nameProject, false);

            // When: Generate SQL
            String sql = inSubquery.toSQL();

            // Then: Should generate IN subquery SQL
            assertThat(sql).containsIgnoringCase("category");
            assertThat(sql).containsIgnoringCase(" IN ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("name");
            assertThat(inSubquery.dataType()).isEqualTo(com.thunderduck.types.BooleanType.get());
            assertThat(inSubquery.isNegated()).isFalse();
            assertThat(inSubquery.testExpression()).isEqualTo(testExpr);
        }

        @Test
        @DisplayName("NOT IN subquery")
        void testNotInSubquery() {
            // Given: status NOT IN (SELECT name FROM invalid_statuses)
            StructType statusesSchema = new StructType(Arrays.asList(
                new StructField("id", IntegerType.get(), false),
                new StructField("name", StringType.get(), true)
            ));

            LogicalPlan statusesTable = new TableScan("invalid_statuses.parquet", TableScan.TableFormat.PARQUET, statusesSchema);

            Expression nameCol = new ColumnReference("name", StringType.get());
            LogicalPlan nameProject = new Project(statusesTable, Collections.singletonList(nameCol));

            Expression testExpr = new ColumnReference("status", StringType.get());
            InSubquery notInSubquery = new InSubquery(testExpr, nameProject, true);

            // When: Generate SQL
            String sql = notInSubquery.toSQL();

            // Then: Should generate NOT IN SQL
            assertThat(sql).containsIgnoringCase("status");
            assertThat(sql).containsIgnoringCase(" NOT IN ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(notInSubquery.isNegated()).isTrue();
            assertThat(notInSubquery.dataType()).isEqualTo(com.thunderduck.types.BooleanType.get());
            assertThat(notInSubquery.toString()).containsIgnoringCase("NOT IN");
        }

        @Test
        @DisplayName("IN subquery with correlated reference")
        void testInSubqueryCorrelated() {
            // Given: Correlated IN subquery
            // WHERE product_id IN (SELECT product_id FROM orders WHERE orders.customer_id = customers.id)
            StructType ordersSchema = createOrdersSchema();
            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Correlated filter
            Expression orderCustomerId = new ColumnReference("customer_id", IntegerType.get());
            Expression customerId = new ColumnReference("id", IntegerType.get());  // outer reference
            Expression correlationPredicate = BinaryExpression.equal(orderCustomerId, customerId);

            LogicalPlan filteredOrders = new Filter(ordersTable, correlationPredicate);

            Expression productIdCol = new ColumnReference("product_id", IntegerType.get());
            LogicalPlan productIdProject = new Project(filteredOrders, Collections.singletonList(productIdCol));

            Expression testExpr = new ColumnReference("product_id", IntegerType.get());
            InSubquery inSubquery = new InSubquery(testExpr, productIdProject);

            // When: Generate SQL
            String sql = inSubquery.toSQL();

            // Then: Should contain correlation condition
            assertThat(sql).containsIgnoringCase("product_id");
            assertThat(sql).containsIgnoringCase(" IN ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("WHERE");
            assertThat(sql).containsIgnoringCase("customer_id");
        }

        @Test
        @DisplayName("IN subquery with JOIN in subquery")
        void testInSubqueryWithJoin() {
            // Given: IN subquery with JOIN
            // WHERE id IN (SELECT product_id FROM orders INNER JOIN customers ON orders.customer_id = customers.id)
            StructType ordersSchema = createOrdersSchema();
            StructType customersSchema = createCustomersSchema();

            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);
            LogicalPlan customersTable = new TableScan("customers.parquet", TableScan.TableFormat.PARQUET, customersSchema);

            // Join condition: orders.customer_id = customers.id
            Expression orderCustomerId = new ColumnReference("customer_id", IntegerType.get());
            Expression customerId = new ColumnReference("id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(orderCustomerId, customerId);

            LogicalPlan joinedTables = new Join(
                ordersTable,
                customersTable,
                Join.JoinType.INNER,
                joinCondition
            );

            Expression productIdCol = new ColumnReference("product_id", IntegerType.get());
            LogicalPlan productIdProject = new Project(joinedTables, Collections.singletonList(productIdCol));

            Expression testExpr = new ColumnReference("id", IntegerType.get());
            InSubquery inSubquery = new InSubquery(testExpr, productIdProject);

            // When: Generate SQL
            String sql = inSubquery.toSQL();

            // Then: Should contain JOIN in subquery
            assertThat(sql).containsIgnoringCase("id");
            assertThat(sql).containsIgnoringCase(" IN ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("INNER JOIN");
            assertThat(inSubquery.nullable()).isTrue();
        }

        @Test
        @DisplayName("Multiple IN subqueries")
        void testMultipleInSubqueries() {
            // Given: Two IN subqueries - one for category, one for status
            StructType schema1 = new StructType(Collections.singletonList(
                new StructField("name", StringType.get(), true)
            ));
            StructType schema2 = new StructType(Collections.singletonList(
                new StructField("code", StringType.get(), true)
            ));

            LogicalPlan table1 = new TableScan("categories.parquet", TableScan.TableFormat.PARQUET, schema1);
            LogicalPlan table2 = new TableScan("statuses.parquet", TableScan.TableFormat.PARQUET, schema2);

            Expression nameCol = new ColumnReference("name", StringType.get());
            Expression codeCol = new ColumnReference("code", StringType.get());

            LogicalPlan project1 = new Project(table1, Collections.singletonList(nameCol));
            LogicalPlan project2 = new Project(table2, Collections.singletonList(codeCol));

            Expression testExpr1 = new ColumnReference("category", StringType.get());
            Expression testExpr2 = new ColumnReference("status", StringType.get());

            InSubquery inSubquery1 = new InSubquery(testExpr1, project1);
            InSubquery inSubquery2 = new InSubquery(testExpr2, project2);

            // When: Generate SQL for both
            String sql1 = inSubquery1.toSQL();
            String sql2 = inSubquery2.toSQL();

            // Then: Both should be valid IN subqueries
            assertThat(sql1).containsIgnoringCase("category");
            assertThat(sql1).containsIgnoringCase(" IN ");
            assertThat(sql2).containsIgnoringCase("status");
            assertThat(sql2).containsIgnoringCase(" IN ");
            assertThat(inSubquery1.subquery()).isEqualTo(project1);
            assertThat(inSubquery2.subquery()).isEqualTo(project2);
        }
    }

    @Nested
    @DisplayName("EXISTS Subqueries")
    class ExistsSubqueries {

        @Test
        @DisplayName("EXISTS subquery with correlated predicate")
        void testExistsSubqueryCorrelated() {
            // Given: EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
            StructType ordersSchema = createOrdersSchema();
            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Correlated filter
            Expression orderCustomerId = new ColumnReference("customer_id", IntegerType.get());
            Expression customerId = new ColumnReference("id", IntegerType.get());  // outer reference
            Expression correlationPredicate = BinaryExpression.equal(orderCustomerId, customerId);

            LogicalPlan filteredOrders = new Filter(ordersTable, correlationPredicate);

            // Project SELECT 1 (common pattern for EXISTS)
            Expression one = new Literal(1, IntegerType.get());
            LogicalPlan oneProject = new Project(filteredOrders, Collections.singletonList(one));

            ExistsSubquery existsSubquery = new ExistsSubquery(oneProject, false);

            // When: Generate SQL
            String sql = existsSubquery.toSQL();

            // Then: Should generate EXISTS SQL
            assertThat(sql).startsWithIgnoringCase("EXISTS ");
            assertThat(sql).containsIgnoringCase("(");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("WHERE");
            assertThat(sql).containsIgnoringCase("customer_id");
            assertThat(existsSubquery.dataType()).isEqualTo(com.thunderduck.types.BooleanType.get());
            assertThat(existsSubquery.isNegated()).isFalse();
        }

        @Test
        @DisplayName("NOT EXISTS subquery")
        void testNotExistsSubquery() {
            // Given: NOT EXISTS (SELECT 1 FROM refunds WHERE refunds.order_id = orders.id)
            StructType refundsSchema = new StructType(Arrays.asList(
                new StructField("id", IntegerType.get(), false),
                new StructField("order_id", IntegerType.get(), true),
                new StructField("amount", IntegerType.get(), true)
            ));

            LogicalPlan refundsTable = new TableScan("refunds.parquet", TableScan.TableFormat.PARQUET, refundsSchema);

            // Correlated filter
            Expression refundOrderId = new ColumnReference("order_id", IntegerType.get());
            Expression orderId = new ColumnReference("id", IntegerType.get());  // outer reference
            Expression correlationPredicate = BinaryExpression.equal(refundOrderId, orderId);

            LogicalPlan filteredRefunds = new Filter(refundsTable, correlationPredicate);

            // Project SELECT 1
            Expression one = new Literal(1, IntegerType.get());
            LogicalPlan oneProject = new Project(filteredRefunds, Collections.singletonList(one));

            ExistsSubquery notExistsSubquery = new ExistsSubquery(oneProject, true);

            // When: Generate SQL
            String sql = notExistsSubquery.toSQL();

            // Then: Should generate NOT EXISTS SQL
            assertThat(sql).startsWithIgnoringCase("NOT EXISTS ");
            assertThat(sql).containsIgnoringCase("(");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(notExistsSubquery.isNegated()).isTrue();
            assertThat(notExistsSubquery.dataType()).isEqualTo(com.thunderduck.types.BooleanType.get());
            assertThat(notExistsSubquery.toString()).containsIgnoringCase("NOT EXISTS");
        }

        @Test
        @DisplayName("EXISTS with complex JOIN in subquery")
        void testExistsWithJoin() {
            // Given: EXISTS with JOIN
            // EXISTS (SELECT 1 FROM orders o INNER JOIN products p ON o.product_id = p.id WHERE p.category = 'electronics')
            StructType ordersSchema = createOrdersSchema();
            StructType productsSchema = createProductsSchema();

            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);
            LogicalPlan productsTable = new TableScan("products.parquet", TableScan.TableFormat.PARQUET, productsSchema);

            // Join condition: orders.product_id = products.id
            Expression orderProductId = new ColumnReference("product_id", IntegerType.get());
            Expression productId = new ColumnReference("id", IntegerType.get());
            Expression joinCondition = BinaryExpression.equal(orderProductId, productId);

            LogicalPlan joinedTables = new Join(
                ordersTable,
                productsTable,
                Join.JoinType.INNER,
                joinCondition
            );

            // Filter: WHERE category = 'electronics'
            Expression categoryCol = new ColumnReference("category", StringType.get());
            Expression electronics = new Literal("electronics", StringType.get());
            Expression filterCondition = BinaryExpression.equal(categoryCol, electronics);

            LogicalPlan filteredJoin = new Filter(joinedTables, filterCondition);

            // Project SELECT 1
            Expression one = new Literal(1, IntegerType.get());
            LogicalPlan oneProject = new Project(filteredJoin, Collections.singletonList(one));

            ExistsSubquery existsSubquery = new ExistsSubquery(oneProject);

            // When: Generate SQL
            String sql = existsSubquery.toSQL();

            // Then: Should contain JOIN and filter
            assertThat(sql).startsWithIgnoringCase("EXISTS ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("INNER JOIN");
            assertThat(sql).containsIgnoringCase("WHERE");
            assertThat(sql).containsIgnoringCase("category");
        }

        @Test
        @DisplayName("Multiple EXISTS subqueries (AND/OR)")
        void testMultipleExistsSubqueries() {
            // Given: Two EXISTS subqueries
            StructType ordersSchema = createOrdersSchema();
            StructType refundsSchema = new StructType(Arrays.asList(
                new StructField("id", IntegerType.get(), false),
                new StructField("order_id", IntegerType.get(), true)
            ));

            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);
            LogicalPlan refundsTable = new TableScan("refunds.parquet", TableScan.TableFormat.PARQUET, refundsSchema);

            Expression one = new Literal(1, IntegerType.get());
            LogicalPlan ordersProject = new Project(ordersTable, Collections.singletonList(one));
            LogicalPlan refundsProject = new Project(refundsTable, Collections.singletonList(one));

            ExistsSubquery existsOrders = new ExistsSubquery(ordersProject);
            ExistsSubquery existsRefunds = new ExistsSubquery(refundsProject);

            // When: Generate SQL for both
            String sql1 = existsOrders.toSQL();
            String sql2 = existsRefunds.toSQL();

            // Then: Both should be valid EXISTS subqueries
            assertThat(sql1).startsWithIgnoringCase("EXISTS ");
            assertThat(sql1).containsIgnoringCase("orders.parquet");
            assertThat(sql2).startsWithIgnoringCase("EXISTS ");
            assertThat(sql2).containsIgnoringCase("refunds.parquet");
            assertThat(existsOrders.nullable()).isTrue();
            assertThat(existsRefunds.nullable()).isTrue();
        }

        @Test
        @DisplayName("EXISTS with aggregation in subquery")
        void testExistsWithAggregation() {
            // Given: EXISTS with aggregation
            // EXISTS (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id HAVING COUNT(*) > 5)
            StructType ordersSchema = createOrdersSchema();
            LogicalPlan ordersTable = new TableScan("orders.parquet", TableScan.TableFormat.PARQUET, ordersSchema);

            // Correlated filter
            Expression orderCustomerId = new ColumnReference("customer_id", IntegerType.get());
            Expression customerId = new ColumnReference("id", IntegerType.get());  // outer reference
            Expression correlationPredicate = BinaryExpression.equal(orderCustomerId, customerId);

            LogicalPlan filteredOrders = new Filter(ordersTable, correlationPredicate);

            // COUNT(*) aggregation
            Expression countStar = new Literal("*", StringType.get());
            Aggregate.AggregateExpression countExpr = new Aggregate.AggregateExpression("COUNT", countStar, "count");

            LogicalPlan countAgg = new Aggregate(
                filteredOrders,
                Collections.emptyList(),
                Collections.singletonList(countExpr)
            );

            ExistsSubquery existsSubquery = new ExistsSubquery(countAgg);

            // When: Generate SQL
            String sql = existsSubquery.toSQL();

            // Then: Should contain aggregation
            assertThat(sql).startsWithIgnoringCase("EXISTS ");
            assertThat(sql).containsIgnoringCase("SELECT");
            assertThat(sql).containsIgnoringCase("COUNT");
            assertThat(sql).containsIgnoringCase("FROM");
            assertThat(existsSubquery.subquery()).isEqualTo(countAgg);
        }
    }
}
