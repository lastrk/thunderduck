package com.thunderduck.expression;

import com.thunderduck.test.TestBase;
import com.thunderduck.test.TestCategories;
import com.thunderduck.types.DecimalType;
import com.thunderduck.types.IntegerType;
import com.thunderduck.types.StringType;
import com.thunderduck.types.DoubleType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for RawSQLExpression type handling.
 */
@TestCategories.Tier1
@TestCategories.Unit
@DisplayName("RawSQLExpression Tests")
public class RawSQLExpressionTest extends TestBase {

    @Nested
    @DisplayName("Type Inference")
    class TypeInference {

        @Test
        @DisplayName("Default constructor returns StringType")
        void testDefaultTypeIsString() {
            RawSQLExpression expr = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END");

            assertThat(expr.dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Constructor with explicit type returns that type")
        void testExplicitTypeIsPreserved() {
            RawSQLExpression expr = new RawSQLExpression(
                "CASE WHEN x THEN price ELSE 0 END",
                new DecimalType(10, 2)
            );

            assertThat(expr.dataType()).isInstanceOf(DecimalType.class);
            DecimalType decType = (DecimalType) expr.dataType();
            assertThat(decType.precision()).isEqualTo(10);
            assertThat(decType.scale()).isEqualTo(2);
        }

        @Test
        @DisplayName("Constructor with null type defaults to StringType")
        void testNullTypeDefaultsToString() {
            RawSQLExpression expr = new RawSQLExpression("SELECT 1", null);

            assertThat(expr.dataType()).isInstanceOf(StringType.class);
        }

        @Test
        @DisplayName("Integer type is preserved")
        void testIntegerTypePreserved() {
            RawSQLExpression expr = new RawSQLExpression(
                "CASE WHEN active THEN 1 ELSE 0 END",
                IntegerType.get()
            );

            assertThat(expr.dataType()).isInstanceOf(IntegerType.class);
        }

        @Test
        @DisplayName("Double type is preserved")
        void testDoubleTypePreserved() {
            RawSQLExpression expr = new RawSQLExpression(
                "CASE WHEN active THEN 1.5 ELSE 0.0 END",
                DoubleType.get()
            );

            assertThat(expr.dataType()).isInstanceOf(DoubleType.class);
        }
    }

    @Nested
    @DisplayName("SQL Generation")
    class SqlGeneration {

        @Test
        @DisplayName("toSQL returns the raw SQL string")
        void testToSqlReturnsRawSql() {
            String sql = "CASE WHEN status = 'active' THEN 1 ELSE 0 END";
            RawSQLExpression expr = new RawSQLExpression(sql);

            assertThat(expr.toSQL()).isEqualTo(sql);
        }

        @Test
        @DisplayName("sql() method returns the raw SQL string")
        void testSqlMethodReturnsRawSql() {
            String sql = "COALESCE(a, b, c)";
            RawSQLExpression expr = new RawSQLExpression(sql);

            assertThat(expr.sql()).isEqualTo(sql);
        }
    }

    @Nested
    @DisplayName("Equality and HashCode")
    class EqualityAndHashCode {

        @Test
        @DisplayName("Same SQL and type are equal")
        void testSameSqlAndTypeAreEqual() {
            RawSQLExpression expr1 = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", IntegerType.get());
            RawSQLExpression expr2 = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", IntegerType.get());

            assertThat(expr1).isEqualTo(expr2);
            assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
        }

        @Test
        @DisplayName("Same SQL different types are not equal")
        void testSameSqlDifferentTypesNotEqual() {
            RawSQLExpression expr1 = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", IntegerType.get());
            RawSQLExpression expr2 = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", StringType.get());

            assertThat(expr1).isNotEqualTo(expr2);
        }

        @Test
        @DisplayName("Different SQL same type are not equal")
        void testDifferentSqlSameTypeNotEqual() {
            RawSQLExpression expr1 = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", IntegerType.get());
            RawSQLExpression expr2 = new RawSQLExpression("CASE WHEN y THEN 1 ELSE 0 END", IntegerType.get());

            assertThat(expr1).isNotEqualTo(expr2);
        }

        @Test
        @DisplayName("Null type equals null type")
        void testNullTypeEqualsNullType() {
            RawSQLExpression expr1 = new RawSQLExpression("SELECT 1");
            RawSQLExpression expr2 = new RawSQLExpression("SELECT 1", null);

            assertThat(expr1).isEqualTo(expr2);
        }
    }

    @Nested
    @DisplayName("Nullable")
    class Nullable {

        @Test
        @DisplayName("RawSQLExpression is always nullable")
        void testAlwaysNullable() {
            RawSQLExpression expr = new RawSQLExpression("CASE WHEN x THEN 1 ELSE 0 END", IntegerType.get());

            assertThat(expr.nullable()).isTrue();
        }
    }
}
