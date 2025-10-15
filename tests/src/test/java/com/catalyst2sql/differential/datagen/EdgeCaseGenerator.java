package com.catalyst2sql.differential.datagen;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Generates edge case test data for differential testing.
 *
 * <p>Edge cases include:
 * <ul>
 *   <li>Empty datasets</li>
 *   <li>Single row datasets</li>
 *   <li>All NULL values</li>
 *   <li>MIN/MAX values</li>
 *   <li>NaN and Infinity</li>
 *   <li>Special characters in strings</li>
 * </ul>
 */
public class EdgeCaseGenerator {

    /**
     * Generate an empty dataset.
     */
    public static Dataset<Row> emptyDataset(SparkSession spark) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        return spark.createDataFrame(Collections.emptyList(), schema);
    }

    /**
     * Generate a single-row dataset.
     */
    public static Dataset<Row> singleRowDataset(SparkSession spark) {
        Row row = RowFactory.create(1, 42.0);

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        return spark.createDataFrame(Collections.singletonList(row), schema);
    }

    /**
     * Generate dataset with all NULL values.
     */
    public static Dataset<Row> allNullsDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, null, null),
                RowFactory.create(2, null, null),
                RowFactory.create(3, null, null)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.DoubleType, true)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with MIN/MAX integer values.
     */
    public static Dataset<Row> minMaxIntegersDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(Integer.MIN_VALUE, Long.MIN_VALUE),
                RowFactory.create(Integer.MAX_VALUE, Long.MAX_VALUE),
                RowFactory.create(0, 0L),
                RowFactory.create(-1, -1L),
                RowFactory.create(1, 1L)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("int_val", DataTypes.IntegerType, false),
                DataTypes.createStructField("long_val", DataTypes.LongType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with NaN and Infinity values.
     */
    public static Dataset<Row> nanInfinityDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, Double.NaN, Float.NaN),
                RowFactory.create(2, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY),
                RowFactory.create(3, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY),
                RowFactory.create(4, 0.0, 0.0f),
                RowFactory.create(5, -0.0, -0.0f)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("double_val", DataTypes.DoubleType, false),
                DataTypes.createStructField("float_val", DataTypes.FloatType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with special characters in strings.
     */
    public static Dataset<Row> specialCharactersDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "normal string"),
                RowFactory.create(2, ""),                           // empty string
                RowFactory.create(3, "string with 'quotes'"),
                RowFactory.create(4, "string with \"double quotes\""),
                RowFactory.create(5, "string with\nnewline"),
                RowFactory.create(6, "string with\ttab"),
                RowFactory.create(7, "unicode: \u00E9\u00F1\u00FC"),
                RowFactory.create(8, "SQL: SELECT * FROM table; DROP TABLE users;"),
                RowFactory.create(9, null)                          // null string
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("text", DataTypes.StringType, true)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with extreme floating point values.
     */
    public static Dataset<Row> extremeFloatsDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, Double.MIN_VALUE, Float.MIN_VALUE),
                RowFactory.create(2, Double.MAX_VALUE, Float.MAX_VALUE),
                RowFactory.create(3, Double.MIN_NORMAL, Float.MIN_NORMAL),
                RowFactory.create(4, 1e-100, 1e-30f),
                RowFactory.create(5, 1e100, 1e30f)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("double_val", DataTypes.DoubleType, false),
                DataTypes.createStructField("float_val", DataTypes.FloatType, false)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generate dataset with mixed null and non-null values.
     */
    public static Dataset<Row> mixedNullsDataset(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "value1", 10.0),
                RowFactory.create(2, null, 20.0),
                RowFactory.create(3, "value3", null),
                RowFactory.create(4, null, null),
                RowFactory.create(5, "value5", 50.0)
        );

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.DoubleType, true)
        });

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Get all edge case datasets.
     */
    public static List<Dataset<Row>> getAllEdgeCases(SparkSession spark) {
        List<Dataset<Row>> edgeCases = new ArrayList<>();
        edgeCases.add(emptyDataset(spark));
        edgeCases.add(singleRowDataset(spark));
        edgeCases.add(allNullsDataset(spark));
        edgeCases.add(minMaxIntegersDataset(spark));
        edgeCases.add(nanInfinityDataset(spark));
        edgeCases.add(specialCharactersDataset(spark));
        edgeCases.add(extremeFloatsDataset(spark));
        edgeCases.add(mixedNullsDataset(spark));
        return edgeCases;
    }
}
