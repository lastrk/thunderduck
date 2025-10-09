package org.apache.spark.sql;

import com.spark2sql.execution.ExecutionEngine;
import com.spark2sql.execution.DuckDBExecutor;
import com.spark2sql.plan.PlanBuilder;
import com.spark2sql.plan.nodes.LocalRelation;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point to Spark SQL functionality.
 * This is a drop-in replacement for the Spark SparkSession that translates to SQL.
 */
public class SparkSession implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkSession.class);

    private final String appName;
    private final PlanBuilder planBuilder;
    private final ExecutionEngine executionEngine;
    private final Map<String, Object> configs;
    private final Catalog catalog;

    private SparkSession(Builder builder) {
        this.appName = builder.appName;
        this.configs = new HashMap<>(builder.configs);
        this.planBuilder = new PlanBuilder();
        this.executionEngine = builder.createExecutionEngine();
        this.catalog = new Catalog(this, executionEngine);

        LOG.info("SparkSession initialized with app: {}", appName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public DataFrameReader read() {
        return new DataFrameReader(this, planBuilder);
    }

    public Dataset<Row> sql(String sqlQuery) {
        LOG.debug("Executing SQL: {}", sqlQuery);
        return executionEngine.sql(sqlQuery, this);
    }

    public Dataset<Row> table(String tableName) {
        return catalog.getTable(tableName);
    }

    public Catalog catalog() {
        return catalog;
    }

    public RuntimeConfig conf() {
        return new RuntimeConfig(configs);
    }

    @Override
    public void close() {
        LOG.info("Closing SparkSession: {}", appName);
        try {
            executionEngine.close();
        } catch (Exception e) {
            LOG.error("Error closing execution engine", e);
            throw new RuntimeException("Failed to close SparkSession", e);
        }
    }

    // Alias for close() for compatibility with real Spark
    public void stop() {
        close();
    }

    // Create a DataFrame from a list of Rows
    public Dataset<Row> createDataFrame(List<Row> rows, StructType schema) {
        return new Dataset<>(this, new LocalRelation(rows, schema), RowEncoder.apply(schema));
    }

    // Create a DataFrame from Java beans
    public <T> Dataset<T> createDataFrame(List<T> data, Class<T> beanClass) {
        throw new UnsupportedOperationException("Bean-based DataFrame creation not yet supported");
    }

    // Create a DataFrame from an RDD (not supported in embedded mode)
    public Dataset<Row> createDataFrame(Object rdd, StructType schema) {
        throw new UnsupportedOperationException("RDD-based DataFrame creation not supported in embedded mode");
    }

    // Create an empty DataFrame with schema
    public Dataset<Row> emptyDataFrame() {
        return createDataFrame(new java.util.ArrayList<>(), new StructType());
    }

    // Package-private methods for internal use
    ExecutionEngine getExecutionEngine() {
        return executionEngine;
    }

    PlanBuilder getPlanBuilder() {
        return planBuilder;
    }

    public static class Builder {
        private String appName = "Spark2SQL Application";
        private String master = "local[*]";
        private final Map<String, Object> configs = new HashMap<>();

        public Builder appName(String name) {
            this.appName = name;
            return this;
        }

        public Builder master(String master) {
            // Ignored for embedded mode, kept for API compatibility
            this.master = master;
            LOG.debug("Master setting '{}' ignored in embedded mode", master);
            return this;
        }

        public Builder config(String key, String value) {
            configs.put(key, value);
            return this;
        }

        public Builder config(String key, boolean value) {
            configs.put(key, value);
            return this;
        }

        public Builder config(String key, long value) {
            configs.put(key, value);
            return this;
        }

        public Builder config(String key, double value) {
            configs.put(key, value);
            return this;
        }

        public Builder enableHiveSupport() {
            // No-op for compatibility
            LOG.debug("Hive support not implemented in embedded mode");
            return this;
        }

        public SparkSession getOrCreate() {
            return new SparkSession(this);
        }

        private ExecutionEngine createExecutionEngine() {
            try {
                return new DuckDBExecutor();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize DuckDB executor", e);
            }
        }
    }

    // Inner classes for API compatibility
    public static class RuntimeConfig {
        private final Map<String, Object> configs;

        RuntimeConfig(Map<String, Object> configs) {
            this.configs = configs;
        }

        public String get(String key) {
            Object value = configs.get(key);
            return value != null ? value.toString() : null;
        }

        public void set(String key, String value) {
            configs.put(key, value);
        }
    }

    public static class Catalog {
        private final SparkSession session;
        private final ExecutionEngine engine;
        private final Map<String, Dataset<Row>> tempViews = new HashMap<>();

        Catalog(SparkSession session, ExecutionEngine engine) {
            this.session = session;
            this.engine = engine;
        }

        public void createOrReplaceTempView(String viewName, Dataset<?> dataset) {
            LOG.debug("Creating temp view: {}", viewName);
            engine.createTempView(viewName, dataset.logicalPlan);
            tempViews.put(viewName, (Dataset<Row>) dataset);
        }

        public void createTempView(String viewName, Dataset<?> dataset) {
            if (tempViews.containsKey(viewName)) {
                throw new AnalysisException("Temp view '" + viewName + "' already exists");
            }
            createOrReplaceTempView(viewName, dataset);
        }

        public void dropTempView(String viewName) {
            LOG.debug("Dropping temp view: {}", viewName);
            engine.dropTempView(viewName);
            tempViews.remove(viewName);
        }

        Dataset<Row> getTable(String tableName) {
            if (tempViews.containsKey(tableName)) {
                return tempViews.get(tableName);
            }
            return engine.getTable(tableName, session);
        }
    }
}