package com.spark2sql.test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.sql.Timestamp;
import java.util.*;

/**
 * Utility class for generating test data for differential testing.
 */
public class TestDataGenerator {
    private final Random random = new Random(42); // Fixed seed for reproducibility

    /**
     * Generate random rows with various data types.
     */
    public List<Row> generateMixedTypeRows(int count) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(RowFactory.create(
                i,                                    // id
                "name_" + i,                         // name
                random.nextInt(100),                 // age
                random.nextDouble() * 10000,        // salary
                random.nextBoolean(),                // active
                i % 10 == 0 ? null : "value_" + i   // nullable string
            ));
        }
        return rows;
    }

    /**
     * Generate schema for mixed type rows.
     */
    public StructType getMixedTypeSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false),
            DataTypes.createStructField("salary", DataTypes.DoubleType, false),
            DataTypes.createStructField("active", DataTypes.BooleanType, false),
            DataTypes.createStructField("notes", DataTypes.StringType, true)
        ));
    }

    /**
     * Generate numeric test data with edge cases.
     */
    public List<Row> generateNumericEdgeCases() {
        return Arrays.asList(
            RowFactory.create(0, 0.0, 0L),
            RowFactory.create(1, 1.0, 1L),
            RowFactory.create(-1, -1.0, -1L),
            RowFactory.create(Integer.MAX_VALUE, Double.MAX_VALUE, Long.MAX_VALUE),
            RowFactory.create(Integer.MIN_VALUE, Double.MIN_VALUE, Long.MIN_VALUE),
            RowFactory.create(null, null, null),
            RowFactory.create(42, Double.NaN, 42L),
            RowFactory.create(100, Double.POSITIVE_INFINITY, 100L),
            RowFactory.create(-100, Double.NEGATIVE_INFINITY, -100L)
        );
    }

    /**
     * Generate schema for numeric edge cases.
     */
    public StructType getNumericEdgeCaseSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("int_val", DataTypes.IntegerType, true),
            DataTypes.createStructField("double_val", DataTypes.DoubleType, true),
            DataTypes.createStructField("long_val", DataTypes.LongType, true)
        ));
    }

    /**
     * Generate string test data with special characters.
     */
    public List<Row> generateStringEdgeCases() {
        return Arrays.asList(
            RowFactory.create(""),                  // Empty string
            RowFactory.create(" "),                 // Single space
            RowFactory.create("  "),                // Multiple spaces
            RowFactory.create(null),                // Null
            RowFactory.create("'quotes'"),          // Single quotes
            RowFactory.create("\"double\""),        // Double quotes
            RowFactory.create("line\\nbreak"),      // Line break
            RowFactory.create("tab\\there"),        // Tab
            RowFactory.create("unicode: 你好"),      // Unicode
            RowFactory.create("special!@#$%^&*()"), // Special characters
            RowFactory.create("\\backslash\\"),     // Backslashes
            RowFactory.create("SQL'; DROP TABLE;")  // SQL injection attempt
        );
    }

    /**
     * Generate schema for string edge cases.
     */
    public StructType getStringEdgeCaseSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("text", DataTypes.StringType, true)
        ));
    }

    /**
     * Generate date/timestamp test data.
     */
    public List<Row> generateTemporalData() {
        return Arrays.asList(
            RowFactory.create(Timestamp.valueOf("1970-01-01 00:00:00")),  // Epoch
            RowFactory.create(Timestamp.valueOf("2024-01-15 12:30:45")),  // Normal date
            RowFactory.create(Timestamp.valueOf("2024-12-31 23:59:59")),  // End of year
            RowFactory.create(Timestamp.valueOf("2024-02-29 00:00:00")),  // Leap year
            RowFactory.create(null)                                        // Null
        );
    }

    /**
     * Generate schema for temporal data.
     */
    public StructType getTemporalSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, true)
        ));
    }

    /**
     * Generate test data for join operations.
     */
    public static class JoinTestData {
        public final List<Row> leftRows;
        public final List<Row> rightRows;
        public final StructType leftSchema;
        public final StructType rightSchema;

        public JoinTestData(List<Row> leftRows, List<Row> rightRows,
                          StructType leftSchema, StructType rightSchema) {
            this.leftRows = leftRows;
            this.rightRows = rightRows;
            this.leftSchema = leftSchema;
            this.rightSchema = rightSchema;
        }
    }

    /**
     * Generate data for join testing.
     */
    public JoinTestData generateJoinTestData() {
        StructType leftSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false)
        ));

        StructType rightSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("score", DataTypes.IntegerType, false)
        ));

        List<Row> leftRows = Arrays.asList(
            RowFactory.create(1, "Alice"),
            RowFactory.create(2, "Bob"),
            RowFactory.create(3, "Charlie"),
            RowFactory.create(4, "David")
        );

        List<Row> rightRows = Arrays.asList(
            RowFactory.create(1, 100),
            RowFactory.create(2, 200),
            RowFactory.create(3, 300),
            RowFactory.create(5, 500)  // No match in left
        );

        return new JoinTestData(leftRows, rightRows, leftSchema, rightSchema);
    }

    /**
     * Generate large dataset for performance testing.
     */
    public List<Row> generateLargeDataset(int size) {
        List<Row> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            rows.add(RowFactory.create(
                i,
                "name_" + (i % 1000),
                i % 100,
                random.nextDouble() * 1000000,
                new Timestamp(System.currentTimeMillis() - random.nextInt(365 * 24 * 60 * 60 * 1000))
            ));
        }
        return rows;
    }

    /**
     * Generate schema for large dataset.
     */
    public StructType getLargeDatasetSchema() {
        return DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("department_id", DataTypes.IntegerType, false),
            DataTypes.createStructField("amount", DataTypes.DoubleType, false),
            DataTypes.createStructField("created_at", DataTypes.TimestampType, false)
        ));
    }
}