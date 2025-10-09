package com.spark2sql.test;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * Base class for differential testing between Spark and our implementation.
 * Provides utilities to run the same operations on both systems and compare results.
 */
public abstract class DifferentialTestFramework {

    protected SparkSession originalSpark;    // Real Spark
    protected SparkSession embeddedSpark;    // Our implementation
    protected TestDataGenerator dataGen;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize original Spark session
        originalSpark = createOriginalSparkSession();

        // Initialize our embedded implementation
        embeddedSpark = createEmbeddedSparkSession();

        // Initialize test data generator
        dataGen = new TestDataGenerator();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (originalSpark != null) {
            originalSpark.stop();
        }
        if (embeddedSpark != null) {
            embeddedSpark.close();
        }
    }

    protected SparkSession createOriginalSparkSession() {
        return org.apache.spark.sql.SparkSession.builder()
            .appName("DifferentialTest-Original")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-" + UUID.randomUUID())
            .getOrCreate();
    }

    protected SparkSession createEmbeddedSparkSession() throws Exception {
        // Use our implementation
        return org.apache.spark.sql.SparkSession.builder()
            .appName("DifferentialTest-Embedded")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    }

    /**
     * Creates the same DataFrame in both Spark and our implementation.
     */
    protected DataFramePair createDataFrame(List<Row> data, StructType schema) {
        Dataset<Row> sparkDf = originalSpark.createDataFrame(data, schema);
        Dataset<Row> embeddedDf = embeddedSpark.createDataFrame(data, schema);
        return new DataFramePair(sparkDf, embeddedDf);
    }

    /**
     * Executes the same transformation on both DataFrames and compares results.
     */
    protected void assertSameResults(Dataset<Row> sparkDf, Dataset<Row> embeddedDf) {
        assertSameResults(sparkDf, embeddedDf, true);
    }

    protected void assertSameResults(Dataset<Row> sparkDf, Dataset<Row> embeddedDf,
                                    boolean checkOrder) {
        // Compare schemas
        StructType sparkSchema = sparkDf.schema();
        StructType embeddedSchema = embeddedDf.schema();
        assertSchemasEqual(sparkSchema, embeddedSchema);

        // Collect results
        List<Row> sparkRows = sparkDf.collectAsList();
        List<Row> embeddedRows = embeddedDf.collectAsList();

        // Compare row counts
        assertThat(embeddedRows)
            .as("Row count mismatch")
            .hasSize(sparkRows.size());

        // Sort if order doesn't matter
        if (!checkOrder) {
            sparkRows.sort(new RowComparator());
            embeddedRows.sort(new RowComparator());
        }

        // Compare row contents
        for (int i = 0; i < sparkRows.size(); i++) {
            Row sparkRow = sparkRows.get(i);
            Row embeddedRow = embeddedRows.get(i);
            assertRowsEqual(sparkRow, embeddedRow, sparkSchema, i);
        }
    }

    protected void assertSchemasEqual(StructType expected, StructType actual) {
        StructField[] expectedFields = expected.fields();
        StructField[] actualFields = actual.fields();

        assertThat(actualFields)
            .as("Schema field count mismatch")
            .hasSize(expectedFields.length);

        for (int i = 0; i < expectedFields.length; i++) {
            StructField expectedField = expectedFields[i];
            StructField actualField = actualFields[i];

            assertThat(actualField.name())
                .as("Field name mismatch at position %d", i)
                .isEqualTo(expectedField.name());

            assertThat(actualField.dataType())
                .as("Data type mismatch for field '%s'", expectedField.name())
                .isEqualTo(expectedField.dataType());

            assertThat(actualField.nullable())
                .as("Nullable mismatch for field '%s'", expectedField.name())
                .isEqualTo(expectedField.nullable());
        }
    }

    protected void assertRowsEqual(Row expected, Row actual, StructType schema, int rowIndex) {
        assertThat(actual.length())
            .as("Column count mismatch at row %d", rowIndex)
            .isEqualTo(expected.length());

        for (int i = 0; i < expected.length(); i++) {
            Object expectedValue = expected.get(i);
            Object actualValue = actual.get(i);
            DataType dataType = schema.fields()[i].dataType();

            assertValuesEqual(expectedValue, actualValue, dataType,
                String.format("row %d, column %d (%s)", rowIndex, i, schema.fields()[i].name()));
        }
    }

    protected void assertValuesEqual(Object expected, Object actual, DataType dataType,
                                    String location) {
        if (expected == null) {
            assertThat(actual)
                .as("Value mismatch at %s: expected null", location)
                .isNull();
            return;
        }

        assertThat(actual)
            .as("Value mismatch at %s: expected non-null", location)
            .isNotNull();

        if (dataType instanceof DoubleType || dataType instanceof FloatType) {
            // Use tolerance for floating point comparison
            double expectedDouble = ((Number) expected).doubleValue();
            double actualDouble = ((Number) actual).doubleValue();

            if (Double.isNaN(expectedDouble)) {
                assertThat(Double.isNaN(actualDouble))
                    .as("NaN mismatch at %s", location)
                    .isTrue();
            } else if (Double.isInfinite(expectedDouble)) {
                assertThat(actualDouble)
                    .as("Infinity mismatch at %s", location)
                    .isEqualTo(expectedDouble);
            } else {
                assertThat(actualDouble)
                    .as("Float value mismatch at %s", location)
                    .isCloseTo(expectedDouble, within(1e-9));
            }
        } else if (dataType instanceof DecimalType) {
            // Compare BigDecimal values
            BigDecimal expectedBD = toBigDecimal(expected);
            BigDecimal actualBD = toBigDecimal(actual);

            assertThat(actualBD.compareTo(expectedBD))
                .as("Decimal value mismatch at %s: expected %s, got %s",
                    location, expectedBD, actualBD)
                .isEqualTo(0);
        } else {
            // Direct comparison for other types
            assertThat(actual)
                .as("Value mismatch at %s", location)
                .isEqualTo(expected);
        }
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof org.apache.spark.sql.types.Decimal) {
            return ((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal();
        } else if (value instanceof Number) {
            return new BigDecimal(value.toString());
        } else {
            throw new IllegalArgumentException("Cannot convert to BigDecimal: " + value.getClass());
        }
    }

    /**
     * Helper class to hold paired DataFrames for testing.
     */
    protected static class DataFramePair {
        public final Dataset<Row> spark;
        public final Dataset<Row> embedded;

        public DataFramePair(Dataset<Row> spark, Dataset<Row> embedded) {
            this.spark = spark;
            this.embedded = embedded;
        }

        public DataFramePair select(String... cols) {
            return new DataFramePair(
                spark.select(cols[0], Arrays.copyOfRange(cols, 1, cols.length)),
                embedded.select(cols[0], Arrays.copyOfRange(cols, 1, cols.length))
            );
        }

        public DataFramePair filter(String condition) {
            return new DataFramePair(
                spark.filter(condition),
                embedded.filter(condition)
            );
        }

        public DataFramePair limit(int n) {
            return new DataFramePair(
                spark.limit(n),
                embedded.limit(n)
            );
        }

        public DataFramePair orderBy(String... cols) {
            return new DataFramePair(
                spark.orderBy(cols[0], Arrays.copyOfRange(cols, 1, cols.length)),
                embedded.orderBy(cols[0], Arrays.copyOfRange(cols, 1, cols.length))
            );
        }

        public void assertSameResults() {
            assertSameResults(true);
        }

        public void assertSameResults(boolean checkOrder) {
            // Get the containing test framework instance
            List<Row> sparkRows = spark.collectAsList();
            List<Row> embeddedRows = embedded.collectAsList();

            assertThat(embeddedRows.size())
                .as("Row count mismatch")
                .isEqualTo(sparkRows.size());

            if (!checkOrder) {
                sparkRows.sort(new RowComparator());
                embeddedRows.sort(new RowComparator());
            }

            for (int i = 0; i < sparkRows.size(); i++) {
                Row sparkRow = sparkRows.get(i);
                Row embeddedRow = embeddedRows.get(i);
                assertThat(embeddedRow.length())
                    .as("Column count mismatch at row " + i)
                    .isEqualTo(sparkRow.length());

                for (int j = 0; j < sparkRow.length(); j++) {
                    Object sparkVal = sparkRow.get(j);
                    Object embeddedVal = embeddedRow.get(j);

                    if (sparkVal == null) {
                        assertThat(embeddedVal)
                            .as("Value mismatch at row " + i + ", column " + j)
                            .isNull();
                    } else {
                        assertThat(embeddedVal)
                            .as("Value mismatch at row " + i + ", column " + j)
                            .isEqualTo(sparkVal);
                    }
                }
            }
        }
    }

    /**
     * Row comparator for sorting.
     */
    protected static class RowComparator implements Comparator<Row> {
        @Override
        public int compare(Row r1, Row r2) {
            for (int i = 0; i < Math.min(r1.length(), r2.length()); i++) {
                Object v1 = r1.get(i);
                Object v2 = r2.get(i);

                if (v1 == null && v2 == null) continue;
                if (v1 == null) return -1;
                if (v2 == null) return 1;

                int cmp = compareValues(v1, v2);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(r1.length(), r2.length());
        }

        @SuppressWarnings("unchecked")
        private int compareValues(Object v1, Object v2) {
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                try {
                    return ((Comparable) v1).compareTo(v2);
                } catch (ClassCastException e) {
                    // Different types, compare by string
                    return v1.toString().compareTo(v2.toString());
                }
            }
            return v1.toString().compareTo(v2.toString());
        }
    }
}