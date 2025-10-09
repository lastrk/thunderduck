package org.apache.spark.sql;

import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.plan.nodes.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Main abstraction for DataFrame operations.
 * Provides lazy evaluation and transformation operations.
 */
public class Dataset<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Dataset.class);

    protected final LogicalPlan logicalPlan;
    protected final SparkSession sparkSession;
    protected final Encoder<T> encoder;

    public Dataset(SparkSession session, LogicalPlan plan, Encoder<T> encoder) {
        this.sparkSession = session;
        this.logicalPlan = plan;
        this.encoder = encoder;
    }

    // Transformation Operations (Lazy)
    public Dataset<T> filter(Column condition) {
        LOG.debug("Adding filter: {}", condition);
        return new Dataset<>(sparkSession,
            new Filter(logicalPlan, condition.expr()),
            encoder);
    }

    public Dataset<T> filter(String conditionExpr) {
        return filter(functions.expr(conditionExpr));
    }

    public Dataset<T> where(Column condition) {
        return filter(condition);
    }

    public Dataset<T> where(String condition) {
        return filter(condition);
    }

    @SuppressWarnings("unchecked")
    public Dataset<Row> select(Column... columns) {
        LOG.debug("Selecting {} columns", columns.length);
        return new Dataset<>(sparkSession,
            new Project(logicalPlan, Arrays.asList(columns)),
            (Encoder<Row>) RowEncoder.apply(deriveSchema(columns)));
    }

    public Dataset<Row> select(String col, String... cols) {
        Column[] columns = new Column[cols.length + 1];
        columns[0] = functions.col(col);
        for (int i = 0; i < cols.length; i++) {
            columns[i + 1] = functions.col(cols[i]);
        }
        return select(columns);
    }

    public Dataset<Row> selectExpr(String... exprs) {
        Column[] columns = Arrays.stream(exprs)
            .map(functions::expr)
            .toArray(Column[]::new);
        return select(columns);
    }

    public Dataset<T> limit(int n) {
        LOG.debug("Adding limit: {}", n);
        return new Dataset<>(sparkSession,
            new Limit(logicalPlan, n),
            encoder);
    }

    public Dataset<T> distinct() {
        LOG.debug("Adding distinct");
        return new Dataset<>(sparkSession,
            new Distinct(logicalPlan),
            encoder);
    }

    public Dataset<T> orderBy(Column... sortExprs) {
        LOG.debug("Adding orderBy with {} expressions", sortExprs.length);
        return new Dataset<>(sparkSession,
            new Sort(logicalPlan, Arrays.asList(sortExprs), true),
            encoder);
    }

    public Dataset<T> orderBy(String col, String... cols) {
        Column[] columns = new Column[cols.length + 1];
        columns[0] = functions.col(col);
        for (int i = 0; i < cols.length; i++) {
            columns[i + 1] = functions.col(cols[i]);
        }
        return orderBy(columns);
    }

    public Dataset<T> sort(Column... sortExprs) {
        return orderBy(sortExprs);
    }

    public Dataset<T> sort(String col, String... cols) {
        return orderBy(col, cols);
    }

    // Join operations with various overloads
    public Dataset<Row> join(Dataset<?> right) {
        // Cross join (no condition)
        return join(right, null, "cross");
    }

    public Dataset<Row> join(Dataset<?> right, Column joinExpression) {
        return join(right, joinExpression, "inner");
    }

    @SuppressWarnings("unchecked")
    public Dataset<Row> join(Dataset<?> right, Column joinExpression, String joinType) {
        LOG.debug("Adding {} join", joinType);
        return new Dataset<>(sparkSession,
            new Join(logicalPlan, right.logicalPlan,
                     joinExpression != null ? joinExpression.expr() : null,
                     joinType),
            (Encoder<Row>) RowEncoder.apply(deriveJoinSchema(right)));
    }

    public Dataset<Row> join(Dataset<?> right, String usingColumn) {
        // Natural join on same column name
        Column leftCol = functions.col(usingColumn);
        Column rightCol = functions.col(usingColumn);
        return join(right, leftCol.equalTo(rightCol));
    }


    public Dataset<Row> join(Dataset<?> right, String[] usingColumns) {
        if (usingColumns.length == 0) {
            return join(right); // Cross join
        }

        Column condition = null;
        for (String col : usingColumns) {
            Column eq = functions.col(col).equalTo(functions.col(col));
            condition = (condition == null) ? eq : condition.and(eq);
        }
        return join(right, condition);
    }

    // Convenience methods for specific join types
    public Dataset<Row> crossJoin(Dataset<?> right) {
        return join(right, null, "cross");
    }

    public Dataset<Row> innerJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "inner");
    }

    public Dataset<Row> leftJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "left");
    }

    public Dataset<Row> rightJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "right");
    }

    public Dataset<Row> fullOuterJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "full_outer");
    }

    public Dataset<Row> leftSemiJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "left_semi");
    }

    public Dataset<Row> leftAntiJoin(Dataset<?> right, Column condition) {
        return join(right, condition, "left_anti");
    }

    public RelationalGroupedDataset groupBy(Column... columns) {
        LOG.debug("Grouping by {} columns", columns.length);
        return new RelationalGroupedDataset(sparkSession, logicalPlan, Arrays.asList(columns));
    }

    public RelationalGroupedDataset groupBy(String col1, String... cols) {
        Column[] columns = new Column[cols.length + 1];
        columns[0] = functions.col(col1);
        for (int i = 0; i < cols.length; i++) {
            columns[i + 1] = functions.col(cols[i]);
        }
        return groupBy(columns);
    }

    public Dataset<Row> agg(Column... exprs) {
        return groupBy().agg(exprs);
    }

    // Action Operations (Trigger Execution)
    public List<T> collect() {
        LOG.info("Collecting results");
        return sparkSession.getExecutionEngine().execute(logicalPlan, encoder);
    }

    public List<T> collectAsList() {
        return collect();
    }

    public T[] collect(Class<T> clazz) {
        List<T> list = collect();
        @SuppressWarnings("unchecked")
        T[] array = (T[]) java.lang.reflect.Array.newInstance(clazz, list.size());
        return list.toArray(array);
    }

    public long count() {
        LOG.info("Counting rows");
        Dataset<Row> counted = this.agg(functions.count("*").as("count"));
        Row result = counted.first();
        return result.getLong(0);
    }

    public T first() {
        LOG.info("Getting first row");
        List<T> results = limit(1).collect();
        if (results.isEmpty()) {
            throw new AnalysisException("first() called on empty Dataset");
        }
        return results.get(0);
    }

    public T head() {
        return first();
    }

    public T[] head(int n) {
        List<T> results = limit(n).collect();
        @SuppressWarnings("unchecked")
        T[] array = (T[]) new Object[results.size()];
        return results.toArray(array);
    }

    public List<T> take(int n) {
        return limit(n).collect();
    }

    public List<T> takeAsList(int n) {
        return take(n);
    }

    public void show() {
        show(20, true);
    }

    public void show(int numRows) {
        show(numRows, true);
    }

    public void show(boolean truncate) {
        show(20, truncate);
    }

    public void show(int numRows, boolean truncate) {
        show(numRows, 20, truncate);
    }

    public void show(int numRows, int truncateLength, boolean truncate) {
        LOG.info("Showing {} rows", numRows);
        List<T> rows = limit(numRows).collect();
        TablePrinter.print(rows, schema(), truncateLength, truncate);
    }

    // Schema and metadata
    public StructType schema() {
        return logicalPlan.schema();
    }

    public void printSchema() {
        schema().printTreeString();
    }

    public String[] columns() {
        return schema().fieldNames();
    }

    public Column col(String colName) {
        return new Column(colName);
    }

    public Column apply(String colName) {
        return col(colName);
    }

    // Caching (no-op for compatibility)
    public Dataset<T> cache() {
        LOG.debug("cache() called - no-op in embedded mode");
        return this;
    }

    public Dataset<T> persist() {
        return cache();
    }

    public Dataset<T> unpersist() {
        LOG.debug("unpersist() called - no-op in embedded mode");
        return this;
    }

    // Temporary views
    public void createOrReplaceTempView(String viewName) {
        sparkSession.catalog().createOrReplaceTempView(viewName, this);
    }

    public void createTempView(String viewName) {
        sparkSession.catalog().createTempView(viewName, this);
    }

    public void createGlobalTempView(String viewName) {
        // For now, treat global temp views same as regular temp views
        createOrReplaceTempView("global_temp." + viewName);
    }

    public void createOrReplaceGlobalTempView(String viewName) {
        createOrReplaceTempView("global_temp." + viewName);
    }

    // Type conversions
    @SuppressWarnings("unchecked")
    public Dataset<Row> toDF() {
        if (encoder.getClass().getName().equals("org.apache.spark.sql.RowEncoder")) {
            return (Dataset<Row>) this;
        }
        return new Dataset<>(sparkSession, logicalPlan,
            (Encoder<Row>) RowEncoder.apply(schema()));
    }

    public Dataset<Row> toDF(String... colNames) {
        Dataset<Row> df = toDF();
        if (colNames.length > 0) {
            String[] oldNames = columns();
            if (colNames.length != oldNames.length) {
                throw new IllegalArgumentException(
                    "Number of column names (" + colNames.length +
                    ") does not match number of columns (" + oldNames.length + ")");
            }

            Column[] selectExprs = new Column[colNames.length];
            for (int i = 0; i < colNames.length; i++) {
                selectExprs[i] = col(oldNames[i]).as(colNames[i]);
            }
            return df.select(selectExprs);
        }
        return df;
    }

    // Partitioning (no-op for compatibility)
    public Dataset<T> repartition(int numPartitions) {
        LOG.debug("repartition({}) called - no-op in embedded mode", numPartitions);
        return this;
    }

    public Dataset<T> coalesce(int numPartitions) {
        LOG.debug("coalesce({}) called - no-op in embedded mode", numPartitions);
        return this;
    }

    // Helper methods
    private StructType deriveSchema(Column[] columns) {
        // This is simplified - in real implementation would derive from expressions
        return schema();
    }

    private StructType deriveJoinSchema(Dataset<?> right) {
        // Combine schemas from both datasets
        return schema();
    }

    // Package private getters
    LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    SparkSession getSparkSession() {
        return sparkSession;
    }
}