package com.catalyst2sql.differential.datagen;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates synthetic test data for differential testing.
 *
 * <p>Features:
 * <ul>
 *   <li>Deterministic generation with fixed seeds</li>
 *   <li>Support for all primitive types</li>
 *   <li>Configurable null percentage</li>
 *   <li>Various value distributions</li>
 * </ul>
 */
public class SyntheticDataGenerator {

    private final Random random;
    private final long seed;

    public SyntheticDataGenerator() {
        this(12345L);
    }

    public SyntheticDataGenerator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    /**
     * Generate a simple test dataset with ID, name, and value columns.
     */
    public Dataset<Row> generateSimpleDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                          // id
                    "name_" + i,                                // name
                    random.nextDouble() * 100.0                 // value
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with all primitive types.
     */
    public Dataset<Row> generateAllTypesDataset(SparkSession spark, int rowCount) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,                                          // id (int)
                    (long) i * 1000,                            // long_val
                    (short) (i % 100),                          // short_val
                    (byte) (i % 10),                            // byte_val
                    random.nextFloat(),                         // float_val
                    random.nextDouble(),                        // double_val
                    "string_" + i,                              // string_val
                    i % 2 == 0                                  // boolean_val
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("long_val", DataTypes.LongType, false),
                DataTypes.createStructField("short_val", DataTypes.ShortType, false),
                DataTypes.createStructField("byte_val", DataTypes.ByteType, false),
                DataTypes.createStructField("float_val", DataTypes.FloatType, false),
                DataTypes.createStructField("double_val", DataTypes.DoubleType, false),
                DataTypes.createStructField("string_val", DataTypes.StringType, false),
                DataTypes.createStructField("boolean_val", DataTypes.BooleanType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with nullable columns and null values.
     */
    public Dataset<Row> generateNullableDataset(SparkSession spark, int rowCount, double nullPercentage) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            Integer id = i;
            String name = random.nextDouble() < nullPercentage ? null : "name_" + i;
            Double value = random.nextDouble() < nullPercentage ? null : random.nextDouble() * 100.0;

            rows.add(RowFactory.create(id, name, value));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.DoubleType, true)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset for join operations.
     */
    public Dataset<Row> generateJoinDataset(SparkSession spark, int rowCount, String prefix) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed + prefix.hashCode()); // Different seed per table

        for (int i = 0; i < rowCount; i++) {
            rows.add(RowFactory.create(
                    i,
                    prefix + "_" + i,
                    random.nextInt(100)
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with specific value ranges.
     */
    public Dataset<Row> generateRangeDataset(SparkSession spark, int rowCount, int minValue, int maxValue) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            int value = minValue + random.nextInt(maxValue - minValue + 1);
            rows.add(RowFactory.create(i, value));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with duplicate values for aggregate testing.
     */
    public Dataset<Row> generateGroupByDataset(SparkSession spark, int rowCount, int numGroups) {
        List<Row> rows = new ArrayList<>();

        random.setSeed(seed); // Reset for determinism

        for (int i = 0; i < rowCount; i++) {
            String group = "group_" + (i % numGroups);
            rows.add(RowFactory.create(
                    i,
                    group,
                    random.nextInt(1000)
            ));
        }

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("category", DataTypes.StringType, false),
                DataTypes.createStructField("amount", DataTypes.IntegerType, false)
        });

        return spark.createDataFrame(rows, schema);
    }
}
