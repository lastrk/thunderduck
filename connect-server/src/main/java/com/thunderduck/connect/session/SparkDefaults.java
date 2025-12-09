package com.thunderduck.connect.session;

import java.util.HashMap;
import java.util.Map;

/**
 * Default Spark configuration values.
 *
 * This class provides standard Spark SQL configuration defaults
 * that are expected by PySpark clients when connecting via Spark Connect.
 *
 * These defaults match Apache Spark 3.5+ behavior.
 */
public class SparkDefaults {

    /**
     * Get standard Spark SQL configuration defaults.
     *
     * @return Map of default configuration key-value pairs
     */
    public static Map<String, String> getDefaults() {
        Map<String, String> defaults = new HashMap<>();

        // Session configuration
        defaults.put("spark.sql.session.timeZone", java.util.TimeZone.getDefault().getID());

        // Timestamp configuration
        defaults.put("spark.sql.timestampType", "TIMESTAMP_LTZ");

        // Arrow configuration
        defaults.put("spark.sql.execution.arrow.useLargeVarTypes", "false");
        defaults.put("spark.sql.execution.arrow.pyspark.enabled", "true");
        defaults.put("spark.sql.execution.arrow.pyspark.fallback.enabled", "true");

        // Pandas configuration
        defaults.put("spark.sql.execution.pandas.convertToArrowArraySafely", "false");

        // Adaptive Query Execution
        defaults.put("spark.sql.adaptive.enabled", "true");
        defaults.put("spark.sql.adaptive.coalescePartitions.enabled", "true");

        // SQL behavior
        defaults.put("spark.sql.ansi.enabled", "true");
        defaults.put("spark.sql.caseSensitive", "false");
        defaults.put("spark.sql.sources.default", "parquet");
        defaults.put("spark.sql.legacy.timeParserPolicy", "EXCEPTION");
        defaults.put("spark.sql.session.localRelationCacheThreshold", "67108864");


        // Shuffle configuration
        defaults.put("spark.sql.shuffle.partitions", "200");

        // File format defaults
        defaults.put("spark.sql.parquet.compression.codec", "snappy");
        defaults.put("spark.sql.orc.compression.codec", "snappy");

        // Catalog configuration
        defaults.put("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.connector.catalog.InMemoryCatalog");

        // Pyspark nested types inference
        defaults.put("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false");
        defaults.put("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", "false");
        defaults.put("spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled", "false");

        return defaults;
    }

    private SparkDefaults() {
        // Utility class - prevent instantiation
    }
}
