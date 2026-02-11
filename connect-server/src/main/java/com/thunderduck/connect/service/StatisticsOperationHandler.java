package com.thunderduck.connect.service;

import com.google.protobuf.ByteString;
import com.thunderduck.connect.session.Session;
import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.runtime.ArrowBatchIterator;
import com.thunderduck.runtime.ArrowStreamingExecutor;
import com.thunderduck.runtime.QueryExecutor;
import com.thunderduck.schema.SchemaInferrer;
import com.thunderduck.types.StructField;
import com.thunderduck.types.StructType;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.connect.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.thunderduck.generator.SQLQuoting.quoteIdentifier;

/**
 * Handles Spark Connect Statistics operations by delegating to DuckDB.
 *
 * <p>This handler maps Spark DataFrame statistics operations to DuckDB SQL.
 * Statistics operations allow computing various statistical measures on DataFrames.
 *
 * <p>Implemented operations:
 * <ul>
 *   <li>COV - Sample covariance of two columns</li>
 *   <li>CORR - Pearson correlation of two columns</li>
 *   <li>APPROX_QUANTILE - Approximate quantiles of columns</li>
 *   <li>DESCRIBE - Basic statistics (count, mean, stddev, min, max)</li>
 *   <li>SUMMARY - Configurable statistics with percentiles</li>
 *   <li>CROSSTAB - Contingency table (pivot)</li>
 *   <li>FREQ_ITEMS - Frequent items in columns</li>
 *   <li>SAMPLE_BY - Stratified sampling</li>
 * </ul>
 */
public class StatisticsOperationHandler {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsOperationHandler.class);

    private final SchemaInferrer schemaInferrer;

    public StatisticsOperationHandler(SchemaInferrer schemaInferrer) {
        this.schemaInferrer = schemaInferrer;
    }

    /**
     * Handle StatCov operation - compute sample covariance of two columns.
     *
     * @param cov the StatCov proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleCov(StatCov cov, String inputSql, Session session,
                          StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            String col1 = cov.getCol1();
            String col2 = cov.getCol2();

            // Build SQL using DuckDB's COVAR_SAMP function
            // Spark's stat.cov() replaces NULLs with 0.0 before computing covariance
            // (see StatFunctions.calculateCovImpl in Spark source)
            String sql = String.format(
                "SELECT COVAR_SAMP(COALESCE(%s, 0.0), COALESCE(%s, 0.0)) AS cov FROM (%s) AS _stat_input",
                quoteIdentifier(col1),
                quoteIdentifier(col2),
                inputSql
            );

            logger.debug("StatCov SQL: {}", sql);

            // Execute and stream scalar result
            executeAndStreamScalar(sql, "cov", session, responseObserver);

        } catch (Exception e) {
            logger.error("StatCov failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatCov failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatCorr operation - compute Pearson correlation of two columns.
     *
     * @param corr the StatCorr proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleCorr(StatCorr corr, String inputSql, Session session,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            String col1 = corr.getCol1();
            String col2 = corr.getCol2();

            // Build SQL using DuckDB's CORR function
            String sql = String.format(
                "SELECT CORR(%s, %s) AS corr FROM (%s) AS _stat_input",
                quoteIdentifier(col1),
                quoteIdentifier(col2),
                inputSql
            );

            logger.debug("StatCorr SQL: {}", sql);

            // Execute and stream scalar result
            executeAndStreamScalar(sql, "corr", session, responseObserver);

        } catch (Exception e) {
            logger.error("StatCorr failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatCorr failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatApproxQuantile operation - compute approximate quantiles.
     *
     * <p>Returns a 2D array: outer array per column, inner array per quantile probability.
     *
     * @param approxQuantile the StatApproxQuantile proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleApproxQuantile(StatApproxQuantile approxQuantile, String inputSql, Session session,
                                      StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            List<String> cols = approxQuantile.getColsList();
            List<Double> probabilities = approxQuantile.getProbabilitiesList();
            double relativeError = approxQuantile.getRelativeError();

            if (cols.isEmpty()) {
                throw new IllegalArgumentException("At least one column required for approxQuantile");
            }
            if (probabilities.isEmpty()) {
                throw new IllegalArgumentException("At least one probability required for approxQuantile");
            }

            // Build SQL to compute quantiles for each column
            // DuckDB's QUANTILE_CONT takes a list of quantiles
            StringBuilder probList = new StringBuilder();
            for (int i = 0; i < probabilities.size(); i++) {
                if (i > 0) probList.append(", ");
                probList.append(probabilities.get(i));
            }

            StringBuilder selectList = new StringBuilder();
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) selectList.append(", ");
                selectList.append(String.format(
                    "QUANTILE_CONT(%s, [%s]) AS %s",
                    quoteIdentifier(cols.get(i)),
                    probList.toString(),
                    quoteIdentifier(cols.get(i) + "_quantile")
                ));
            }

            String sql = String.format(
                "SELECT %s FROM (%s) AS _stat_input",
                selectList.toString(),
                inputSql
            );

            logger.debug("StatApproxQuantile SQL: {}", sql);

            // Execute and return as 2D array result
            executeAndStreamQuantileResult(sql, cols, probabilities, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatApproxQuantile failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatApproxQuantile failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatDescribe operation - compute basic statistics for columns.
     *
     * <p>Computes count, mean, stddev, min, max for specified columns.
     * If no columns specified, computes for all numeric columns.
     *
     * @param describe the StatDescribe proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleDescribe(StatDescribe describe, String inputSql, Session session,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            List<String> cols = new ArrayList<>(describe.getColsList());

            // If no columns specified, infer columns from schema
            if (cols.isEmpty()) {
                StructType schema = schemaInferrer.inferSchema(inputSql);
                for (StructField field : schema.fields()) {
                    cols.add(field.name());
                }
            }

            if (cols.isEmpty()) {
                throw new IllegalArgumentException("No columns to describe");
            }

            // Build UNION ALL query for 5 statistics rows: count, mean, stddev, min, max
            // Each row has: summary (stat name), col1, col2, ...
            String[] stats = {"count", "mean", "stddev", "min", "max"};
            StringBuilder unionQuery = new StringBuilder();

            for (int s = 0; s < stats.length; s++) {
                if (s > 0) {
                    unionQuery.append(" UNION ALL ");
                }

                unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stats[s])).append(" AS summary");

                for (String col : cols) {
                    String quotedCol = quoteIdentifier(col);
                    String aggExpr;

                    switch (stats[s]) {
                        case "count":
                            aggExpr = String.format("CAST(COUNT(%s) AS VARCHAR)", quotedCol);
                            break;
                        case "mean":
                            aggExpr = String.format("CAST(AVG(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
                            break;
                        case "stddev":
                            aggExpr = String.format("CAST(STDDEV_SAMP(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
                            break;
                        case "min":
                            aggExpr = String.format("CAST(MIN(%s) AS VARCHAR)", quotedCol);
                            break;
                        case "max":
                            aggExpr = String.format("CAST(MAX(%s) AS VARCHAR)", quotedCol);
                            break;
                        default:
                            aggExpr = "NULL";
                    }

                    unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
                }

                unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
            }

            String sql = unionQuery.toString();
            logger.debug("StatDescribe SQL: {}", sql);

            // Execute and stream DataFrame result
            executeAndStreamDataFrame(sql, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatDescribe failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatDescribe failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatSummary operation - compute configurable statistics.
     *
     * <p>Default statistics: count, mean, stddev, min, 25%, 50%, 75%, max.
     * Supports custom statistics list.
     *
     * @param summary the StatSummary proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleSummary(StatSummary summary, String inputSql, Session session,
                              StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            List<String> statistics = new ArrayList<>(summary.getStatisticsList());

            // Default statistics if not specified
            if (statistics.isEmpty()) {
                statistics = Arrays.asList("count", "mean", "stddev", "min", "25%", "50%", "75%", "max");
            }

            // Get all columns from schema
            StructType schema = schemaInferrer.inferSchema(inputSql);
            List<String> cols = new ArrayList<>();
            for (StructField field : schema.fields()) {
                cols.add(field.name());
            }

            if (cols.isEmpty()) {
                throw new IllegalArgumentException("No columns in input DataFrame");
            }

            // Build UNION ALL query for each statistic row
            StringBuilder unionQuery = new StringBuilder();

            for (int s = 0; s < statistics.size(); s++) {
                String stat = statistics.get(s);

                if (s > 0) {
                    unionQuery.append(" UNION ALL ");
                }

                unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stat)).append(" AS summary");

                for (String col : cols) {
                    String quotedCol = quoteIdentifier(col);
                    String aggExpr = getStatisticExpression(stat, quotedCol);
                    unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
                }

                unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
            }

            String sql = unionQuery.toString();
            logger.debug("StatSummary SQL: {}", sql);

            // Execute and stream DataFrame result
            executeAndStreamDataFrame(sql, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatSummary failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatSummary failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatCrosstab operation - compute contingency table.
     *
     * <p>Creates a pivot table counting occurrences of each col2 value for each col1 value.
     *
     * @param crosstab the StatCrosstab proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleCrosstab(StatCrosstab crosstab, String inputSql, Session session,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            String col1 = crosstab.getCol1();
            String col2 = crosstab.getCol2();

            // First, get distinct values of col2 to build column names
            String distinctSql = String.format(
                "SELECT DISTINCT CAST(%s AS VARCHAR) AS val FROM (%s) AS _stat_input WHERE %s IS NOT NULL ORDER BY val",
                quoteIdentifier(col2), inputSql, quoteIdentifier(col2)
            );

            QueryExecutor executor = new QueryExecutor(session.getRuntime());
            List<String> col2Values = new ArrayList<>();

            try (VectorSchemaRoot distinctResult = executor.executeQuery(distinctSql)) {
                VarCharVector valVector = (VarCharVector) distinctResult.getVector("val");
                for (int i = 0; i < distinctResult.getRowCount(); i++) {
                    if (!valVector.isNull(i)) {
                        col2Values.add(new String(valVector.get(i), StandardCharsets.UTF_8));
                    }
                }
            }

            if (col2Values.isEmpty()) {
                // Return empty DataFrame with just the index column
                String emptyResult = String.format(
                    "SELECT CAST(%s AS VARCHAR) AS %s FROM (%s) AS _stat_input WHERE FALSE",
                    quoteIdentifier(col1),
                    quoteIdentifier(col1 + "_" + col2),
                    inputSql
                );
                executeAndStreamDataFrame(emptyResult, session, responseObserver);
                return;
            }

            // Build PIVOT query
            // Result column name is col1_col2 to match Spark naming
            StringBuilder pivotCols = new StringBuilder();
            for (int i = 0; i < col2Values.size(); i++) {
                if (i > 0) pivotCols.append(", ");
                String val = col2Values.get(i);
                // Escape single quotes in values
                String escapedVal = val.replace("'", "''");
                pivotCols.append("'").append(escapedVal).append("'");
            }

            // DuckDB PIVOT syntax
            String sql = String.format(
                "PIVOT (SELECT CAST(%s AS VARCHAR) AS c1, CAST(%s AS VARCHAR) AS c2 FROM (%s) AS _stat_input) " +
                "ON c2 IN (%s) USING COUNT(*) GROUP BY c1 ORDER BY c1",
                quoteIdentifier(col1),
                quoteIdentifier(col2),
                inputSql,
                pivotCols.toString()
            );

            logger.debug("StatCrosstab SQL: {}", sql);

            // Execute and rename first column to col1_col2
            executeAndStreamCrosstabResult(sql, col1, col2, col2Values, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatCrosstab failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatCrosstab failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatFreqItems operation - find frequent items in columns.
     *
     * <p>Returns a DataFrame with columns col_freqItems containing arrays of frequent items.
     *
     * @param freqItems the StatFreqItems proto message
     * @param inputSql the SQL for the input relation
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleFreqItems(StatFreqItems freqItems, String inputSql, Session session,
                                StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            List<String> cols = freqItems.getColsList();
            double support = freqItems.hasSupport() ? freqItems.getSupport() : 0.01;

            if (cols.isEmpty()) {
                throw new IllegalArgumentException("At least one column required for freqItems");
            }

            // For each column, find items appearing in at least support fraction of rows
            // We need to: 1) count total rows, 2) group by value, 3) filter by count >= support * total
            // Then aggregate into arrays

            // Use a CTE to get total count
            StringBuilder selectList = new StringBuilder();
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) selectList.append(", ");
                String col = cols.get(i);
                String quotedCol = quoteIdentifier(col);

                // Subquery to find frequent items for this column
                selectList.append(String.format(
                    "(SELECT LIST(%s ORDER BY %s) FROM (" +
                        "SELECT %s, COUNT(*) AS cnt FROM (%s) AS _inner WHERE %s IS NOT NULL GROUP BY %s " +
                        "HAVING COUNT(*) >= %f * (SELECT COUNT(*) FROM (%s) AS _total)" +
                    ") AS _freq) AS %s",
                    quotedCol, quotedCol,
                    quotedCol, inputSql, quotedCol, quotedCol,
                    support, inputSql,
                    quoteIdentifier(col + "_freqItems")
                ));
            }

            String sql = "SELECT " + selectList.toString();

            logger.debug("StatFreqItems SQL: {}", sql);

            // Execute and stream DataFrame result
            executeAndStreamDataFrame(sql, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatFreqItems failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatFreqItems failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    /**
     * Handle StatSampleBy operation - stratified sampling.
     *
     * <p>Returns a sample of the DataFrame where each stratum (distinct value of col)
     * is sampled according to its specified fraction.
     *
     * @param sampleBy the StatSampleBy proto message
     * @param inputSql the SQL for the input relation
     * @param colExpr the column expression for stratification
     * @param session the session
     * @param responseObserver gRPC response observer
     */
    public void handleSampleBy(StatSampleBy sampleBy, String inputSql, String colExpr, Session session,
                               StreamObserver<ExecutePlanResponse> responseObserver) {
        try {
            List<StatSampleBy.Fraction> fractions = sampleBy.getFractionsList();
            Long seed = sampleBy.hasSeed() ? sampleBy.getSeed() : null;

            if (fractions.isEmpty()) {
                // No fractions specified, return empty DataFrame
                String emptyResult = inputSql + " WHERE FALSE";
                executeAndStreamDataFrame(emptyResult, session, responseObserver);
                return;
            }

            // Build WHERE clause for stratified sampling
            // WHERE (col = 'A' AND RANDOM() < 0.5) OR (col = 'B' AND RANDOM() < 1.0) ...
            StringBuilder whereClause = new StringBuilder();

            // Set seed if provided
            String seedPrefix = "";
            if (seed != null) {
                seedPrefix = String.format("SETSEED(%f); ", (seed % 1000000) / 1000000.0);
            }

            for (int i = 0; i < fractions.size(); i++) {
                if (i > 0) whereClause.append(" OR ");

                StatSampleBy.Fraction f = fractions.get(i);
                String stratumValue = convertLiteralToSQL(f.getStratum());
                double fraction = f.getFraction();

                whereClause.append("(")
                    .append(colExpr)
                    .append(" = ")
                    .append(stratumValue)
                    .append(" AND RANDOM() < ")
                    .append(fraction)
                    .append(")");
            }

            String sql = String.format(
                "SELECT * FROM (%s) AS _stat_input WHERE %s",
                inputSql,
                whereClause.toString()
            );

            logger.debug("StatSampleBy SQL: {}", sql);

            // Execute and stream DataFrame result
            executeAndStreamDataFrame(sql, session, responseObserver);

        } catch (Exception e) {
            logger.error("StatSampleBy failed", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("StatSampleBy failed: " + e.getMessage())
                .asRuntimeException());
        }
    }

    // ===== SQL Generation Methods (for schema analysis) =====

    /**
     * Generate SQL for StatCov operation.
     */
    public String generateCovSql(StatCov cov, String inputSql) {
        String col1 = cov.getCol1();
        String col2 = cov.getCol2();
        // Spark's stat.cov() replaces NULLs with 0.0 before computing covariance
        // (see StatFunctions.calculateCovImpl in Spark source).
        return String.format(
            "SELECT COVAR_SAMP(COALESCE(%s, 0.0), COALESCE(%s, 0.0)) AS cov FROM (%s) AS _stat_input",
            quoteIdentifier(col1), quoteIdentifier(col2), inputSql
        );
    }

    /**
     * Generate SQL for StatCorr operation.
     */
    public String generateCorrSql(StatCorr corr, String inputSql) {
        String col1 = corr.getCol1();
        String col2 = corr.getCol2();
        return String.format(
            "SELECT CORR(%s, %s) AS corr FROM (%s) AS _stat_input",
            quoteIdentifier(col1), quoteIdentifier(col2), inputSql
        );
    }

    /**
     * Generate SQL for StatApproxQuantile operation.
     */
    public String generateApproxQuantileSql(StatApproxQuantile approxQuantile, String inputSql) {
        List<String> cols = approxQuantile.getColsList();
        List<Double> probabilities = approxQuantile.getProbabilitiesList();

        StringBuilder probList = new StringBuilder();
        for (int i = 0; i < probabilities.size(); i++) {
            if (i > 0) probList.append(", ");
            probList.append(probabilities.get(i));
        }

        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) selectList.append(", ");
            selectList.append(String.format(
                "QUANTILE_CONT(%s, [%s]) AS %s",
                quoteIdentifier(cols.get(i)),
                probList.toString(),
                quoteIdentifier(cols.get(i) + "_quantile")
            ));
        }

        return String.format(
            "SELECT %s FROM (%s) AS _stat_input",
            selectList.toString(), inputSql
        );
    }

    /**
     * Generate SQL for StatDescribe operation.
     */
    public String generateDescribeSql(StatDescribe describe, String inputSql) {
        List<String> cols = new ArrayList<>(describe.getColsList());

        // If no columns specified, infer columns from schema
        if (cols.isEmpty()) {
            StructType schema = schemaInferrer.inferSchema(inputSql);
            for (StructField field : schema.fields()) {
                cols.add(field.name());
            }
        }

        if (cols.isEmpty()) {
            return "SELECT 'count' AS summary WHERE FALSE";
        }

        // Build UNION ALL query
        String[] stats = {"count", "mean", "stddev", "min", "max"};
        StringBuilder unionQuery = new StringBuilder();

        for (int s = 0; s < stats.length; s++) {
            if (s > 0) unionQuery.append(" UNION ALL ");
            unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stats[s])).append(" AS summary");

            for (String col : cols) {
                String quotedCol = quoteIdentifier(col);
                String aggExpr;
                switch (stats[s]) {
                    case "count":
                        aggExpr = String.format("CAST(COUNT(%s) AS VARCHAR)", quotedCol);
                        break;
                    case "mean":
                        aggExpr = String.format("CAST(AVG(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
                        break;
                    case "stddev":
                        aggExpr = String.format("CAST(STDDEV_SAMP(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
                        break;
                    case "min":
                        aggExpr = String.format("CAST(MIN(%s) AS VARCHAR)", quotedCol);
                        break;
                    case "max":
                        aggExpr = String.format("CAST(MAX(%s) AS VARCHAR)", quotedCol);
                        break;
                    default:
                        aggExpr = "NULL";
                }
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return unionQuery.toString();
    }

    /**
     * Generate SQL for StatDescribe with pre-resolved column list.
     * Used by RelationConverter which resolves columns from the plan schema.
     */
    public static String generateDescribeSql(List<String> cols, String inputSql) {
        if (cols.isEmpty()) {
            return "SELECT 'count' AS summary WHERE FALSE";
        }

        String[] stats = {"count", "mean", "stddev", "min", "max"};
        StringBuilder unionQuery = new StringBuilder();

        for (int s = 0; s < stats.length; s++) {
            if (s > 0) unionQuery.append(" UNION ALL ");
            unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stats[s])).append(" AS summary");

            for (String col : cols) {
                String quotedCol = quoteIdentifier(col);
                String aggExpr = getStatisticExpression(stats[s], quotedCol);
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return unionQuery.toString();
    }

    /**
     * Generate SQL for StatSummary operation.
     */
    public String generateSummarySql(StatSummary summary, String inputSql) {
        List<String> statistics = new ArrayList<>(summary.getStatisticsList());

        if (statistics.isEmpty()) {
            statistics = Arrays.asList("count", "mean", "stddev", "min", "25%", "50%", "75%", "max");
        }

        StructType schema = schemaInferrer.inferSchema(inputSql);
        List<String> cols = new ArrayList<>();
        for (StructField field : schema.fields()) {
            cols.add(field.name());
        }

        if (cols.isEmpty()) {
            return "SELECT 'count' AS summary WHERE FALSE";
        }

        StringBuilder unionQuery = new StringBuilder();
        for (int s = 0; s < statistics.size(); s++) {
            String stat = statistics.get(s);
            if (s > 0) unionQuery.append(" UNION ALL ");

            unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stat)).append(" AS summary");

            for (String col : cols) {
                String quotedCol = quoteIdentifier(col);
                String aggExpr = getStatisticExpression(stat, quotedCol);
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return unionQuery.toString();
    }

    /**
     * Generate SQL for StatSummary with pre-resolved column list and statistics.
     * Used by RelationConverter which resolves columns from the plan schema.
     */
    public static String generateSummarySql(List<String> statistics, List<String> cols, String inputSql) {
        if (cols.isEmpty()) {
            return "SELECT 'count' AS summary WHERE FALSE";
        }

        StringBuilder unionQuery = new StringBuilder();
        for (int s = 0; s < statistics.size(); s++) {
            String stat = statistics.get(s);
            if (s > 0) unionQuery.append(" UNION ALL ");

            unionQuery.append("SELECT ").append(SQLQuoting.quoteLiteral(stat)).append(" AS summary");

            for (String col : cols) {
                String quotedCol = quoteIdentifier(col);
                String aggExpr = getStatisticExpression(stat, quotedCol);
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return unionQuery.toString();
    }

    /**
     * Generate SQL for StatCrosstab operation (schema analysis only).
     * Uses DuckDB's dynamic PIVOT to get correct column structure.
     */
    public String generateCrosstabSql(StatCrosstab crosstab, String inputSql) {
        String col1 = crosstab.getCol1();
        String col2 = crosstab.getCol2();

        // Use dynamic PIVOT which automatically determines column values
        String pivotSql = String.format(
            "PIVOT (SELECT CAST(%s AS VARCHAR) AS c1, CAST(%s AS VARCHAR) AS c2 FROM (%s) AS _stat_input WHERE %s IS NOT NULL AND %s IS NOT NULL) " +
            "ON c2 USING COUNT(*) GROUP BY c1 ORDER BY c1",
            quoteIdentifier(col1), quoteIdentifier(col2), inputSql,
            quoteIdentifier(col1), quoteIdentifier(col2)
        );

        return String.format(
            "SELECT c1 AS %s, * EXCLUDE (c1) FROM (%s)",
            quoteIdentifier(col1 + "_" + col2), pivotSql
        );
    }

    /**
     * Generate SQL for StatFreqItems operation.
     */
    public String generateFreqItemsSql(StatFreqItems freqItems, String inputSql) {
        List<String> cols = freqItems.getColsList();
        double support = freqItems.hasSupport() ? freqItems.getSupport() : 0.01;

        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) selectList.append(", ");
            String col = cols.get(i);
            String quotedCol = quoteIdentifier(col);

            selectList.append(String.format(
                "(SELECT LIST(%s ORDER BY %s) FROM (" +
                    "SELECT %s, COUNT(*) AS cnt FROM (%s) AS _inner WHERE %s IS NOT NULL GROUP BY %s " +
                    "HAVING COUNT(*) >= %f * (SELECT COUNT(*) FROM (%s) AS _total)" +
                ") AS _freq) AS %s",
                quotedCol, quotedCol,
                quotedCol, inputSql, quotedCol, quotedCol,
                support, inputSql,
                quoteIdentifier(col + "_freqItems")
            ));
        }

        return "SELECT " + selectList.toString();
    }

    /**
     * Generate SQL for StatSampleBy operation.
     */
    public String generateSampleBySql(StatSampleBy sampleBy, String inputSql, String colExpr) {
        List<StatSampleBy.Fraction> fractions = sampleBy.getFractionsList();

        if (fractions.isEmpty()) {
            return inputSql + " WHERE FALSE";
        }

        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < fractions.size(); i++) {
            if (i > 0) whereClause.append(" OR ");

            StatSampleBy.Fraction f = fractions.get(i);
            String stratumValue = convertLiteralToSQL(f.getStratum());
            double fraction = f.getFraction();

            whereClause.append("(")
                .append(colExpr)
                .append(" = ")
                .append(stratumValue)
                .append(" AND RANDOM() < ")
                .append(fraction)
                .append(")");
        }

        return String.format(
            "SELECT * FROM (%s) AS _stat_input WHERE %s",
            inputSql, whereClause.toString()
        );
    }

    // ===== Helper Methods =====

    /**
     * Get the SQL expression for a statistic.
     */
    static String getStatisticExpression(String stat, String quotedCol) {
        // Check for percentile format (e.g., "25%", "50%", "75%")
        // Spark's summary() uses nearest-rank method (PERCENTILE_DISC/QUANTILE_DISC)
        if (stat.endsWith("%")) {
            try {
                double percentile = Double.parseDouble(stat.substring(0, stat.length() - 1)) / 100.0;
                return String.format(
                    "CAST(QUANTILE_DISC(TRY_CAST(%s AS DOUBLE), %f) AS VARCHAR)",
                    quotedCol, percentile
                );
            } catch (NumberFormatException e) {
                return "NULL";
            }
        }

        switch (stat.toLowerCase()) {
            case "count":
                return String.format("CAST(COUNT(%s) AS VARCHAR)", quotedCol);
            case "mean":
                return String.format("CAST(AVG(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
            case "stddev":
                return String.format("CAST(STDDEV_SAMP(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
            case "min":
                return String.format("CAST(MIN(%s) AS VARCHAR)", quotedCol);
            case "max":
                return String.format("CAST(MAX(%s) AS VARCHAR)", quotedCol);
            case "count_distinct":
                return String.format("CAST(COUNT(DISTINCT %s) AS VARCHAR)", quotedCol);
            case "approx_count_distinct":
                return String.format("CAST(APPROX_COUNT_DISTINCT(%s) AS VARCHAR)", quotedCol);
            default:
                return "NULL";
        }
    }

    /**
     * Convert a proto Literal to SQL string.
     */
    private String convertLiteralToSQL(Expression.Literal literal) {
        switch (literal.getLiteralTypeCase()) {
            case BOOLEAN:
                return literal.getBoolean() ? "TRUE" : "FALSE";
            case INTEGER:
                return String.valueOf(literal.getInteger());
            case LONG:
                return String.valueOf(literal.getLong());
            case FLOAT:
                return String.valueOf(literal.getFloat());
            case DOUBLE:
                return String.valueOf(literal.getDouble());
            case STRING:
                return "'" + literal.getString().replace("'", "''") + "'";
            case NULL:
                return "NULL";
            default:
                throw new IllegalArgumentException("Unsupported literal type: " + literal.getLiteralTypeCase());
        }
    }

    /**
     * Extract a double value from a vector at the given index.
     * Handles both Float8Vector and DecimalVector.
     */
    private double getDoubleFromVector(ValueVector vector, int index) {
        if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof DecimalVector) {
            return ((DecimalVector) vector).getObject(index).doubleValue();
        } else {
            // Try to get as object and convert
            Object obj = vector.getObject(index);
            if (obj instanceof Number) {
                return ((Number) obj).doubleValue();
            }
            throw new IllegalArgumentException("Cannot convert vector type to double: " + vector.getClass().getName());
        }
    }

    /**
     * Execute SQL and stream a scalar double result.
     */
    private void executeAndStreamScalar(String sql, String columnName, Session session,
                                        StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        QueryExecutor executor = new QueryExecutor(session.getRuntime());

        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            double value = Double.NaN;
            if (result.getRowCount() > 0) {
                Float8Vector vector = (Float8Vector) result.getVector(0);
                if (!vector.isNull(0)) {
                    value = vector.get(0);
                }
            }

            // Build Arrow batch with single double value
            streamDoubleResult(value, columnName, session.getSessionId(), responseObserver);
        }
    }

    /**
     * Stream a single double result as an Arrow batch.
     */
    private void streamDoubleResult(double value, String columnName, String sessionId,
                                    StreamObserver<ExecutePlanResponse> responseObserver) throws IOException {
        Field field = new Field(columnName, FieldType.nullable(new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
        Schema schema = new Schema(Collections.singletonList(field));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            Float8Vector vector = (Float8Vector) root.getVector(columnName);
            vector.allocateNew(1);
            vector.set(0, value);
            vector.setValueCount(1);
            root.setRowCount(1);

            // Serialize to Arrow IPC
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            byte[] arrowData = out.toByteArray();

            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(UUID.randomUUID().toString())
                .setResponseId(UUID.randomUUID().toString())
                .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(1)
                    .setData(ByteString.copyFrom(arrowData))
                    .build())
                .build();

            responseObserver.onNext(response);

            // Send completion
            ExecutePlanResponse complete = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(UUID.randomUUID().toString())
                .setResponseId(UUID.randomUUID().toString())
                .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
                .build();

            responseObserver.onNext(complete);
            responseObserver.onCompleted();
        }
    }

    /**
     * Execute SQL and stream quantile results as 2D array.
     */
    private void executeAndStreamQuantileResult(String sql, List<String> cols, List<Double> probabilities,
                                                 Session session,
                                                 StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        QueryExecutor executor = new QueryExecutor(session.getRuntime());

        // Execute query to get quantiles (arrays)
        try (VectorSchemaRoot result = executor.executeQuery(sql)) {
            List<List<Double>> results = new ArrayList<>();

            if (result.getRowCount() > 0) {
                for (int i = 0; i < cols.size(); i++) {
                    List<Double> colQuantiles = new ArrayList<>();

                    // DuckDB returns arrays in ListVector
                    if (result.getVector(i) instanceof ListVector) {
                        ListVector listVector = (ListVector) result.getVector(i);
                        if (!listVector.isNull(0)) {
                            int start = listVector.getElementStartIndex(0);
                            int end = listVector.getElementEndIndex(0);
                            ValueVector dataVector = listVector.getDataVector();

                            // Handle different vector types (DuckDB may return Float8 or Decimal)
                            for (int j = start; j < end; j++) {
                                if (!dataVector.isNull(j)) {
                                    double value = getDoubleFromVector(dataVector, j);
                                    colQuantiles.add(value);
                                } else {
                                    colQuantiles.add(Double.NaN);
                                }
                            }
                        }
                    } else if (result.getVector(i) instanceof Float8Vector) {
                        // Single value case
                        Float8Vector vec = (Float8Vector) result.getVector(i);
                        if (!vec.isNull(0)) {
                            colQuantiles.add(vec.get(0));
                        }
                    } else if (result.getVector(i) instanceof DecimalVector) {
                        // Single decimal value case
                        DecimalVector vec = (DecimalVector) result.getVector(i);
                        if (!vec.isNull(0)) {
                            colQuantiles.add(vec.getObject(0).doubleValue());
                        }
                    }

                    results.add(colQuantiles);
                }
            }

            // Stream as nested list result
            streamQuantileArrayResult(results, cols, session.getSessionId(), responseObserver);
        }
    }

    /**
     * Stream quantile results as nested arrays.
     *
     * PySpark expects a single row with a nested list structure: List<List<Double>>
     * The outer list has one element per input column.
     * Each inner list contains the quantile values for that column.
     *
     * PySpark code (dataframe.py line 1608):
     *   jaq = [q.as_py() for q in table[0][0]]
     *   jaq_list = [list(j) for j in jaq]
     *
     * This means:
     * - table[0] = first column (the only column)
     * - table[0][0] = first row value (a nested list)
     * - iterating gives inner lists (one per column)
     */
    private void streamQuantileArrayResult(List<List<Double>> results, List<String> cols,
                                           String sessionId,
                                           StreamObserver<ExecutePlanResponse> responseObserver) throws IOException {
        // Create schema: List<List<Double>> (nested list)
        // Inner list holds quantile values
        Field doubleField = new Field("item", FieldType.nullable(new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
        Field innerListField = new Field("inner", FieldType.nullable(new ArrowType.List()), Collections.singletonList(doubleField));
        Field outerListField = new Field("value", FieldType.nullable(new ArrowType.List()), Collections.singletonList(innerListField));
        Schema schema = new Schema(Collections.singletonList(outerListField));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            // Get the outer list vector
            ListVector outerList = (ListVector) root.getVector("value");
            outerList.allocateNew();

            // Get the inner list vector (nested inside outer)
            ListVector innerList = (ListVector) outerList.getDataVector();

            // Get the double vector (nested inside inner)
            Float8Vector doubleVector = (Float8Vector) innerList.getDataVector();

            // Calculate total number of double elements
            int totalDoubles = 0;
            for (List<Double> quantiles : results) {
                totalDoubles += quantiles.size();
            }
            doubleVector.allocateNew(totalDoubles);

            // Write the nested structure:
            // Row 0 contains: [[q1, q2, q3], [q4, q5, q6], ...]
            //                 ^outer list   ^inner lists

            // Start the outer list (row 0)
            outerList.startNewValue(0);

            int doubleIndex = 0;
            // Write each inner list (one per input column)
            for (int colIdx = 0; colIdx < cols.size(); colIdx++) {
                List<Double> quantiles = colIdx < results.size() ? results.get(colIdx) : Collections.emptyList();

                // Start inner list
                innerList.startNewValue(colIdx);

                // Write quantile values
                for (Double q : quantiles) {
                    doubleVector.setSafe(doubleIndex++, q);
                }

                // End inner list
                innerList.endValue(colIdx, quantiles.size());
            }

            // End outer list (contains cols.size() inner lists)
            outerList.endValue(0, cols.size());

            // Set value counts
            doubleVector.setValueCount(doubleIndex);
            innerList.setValueCount(cols.size());
            outerList.setValueCount(1);
            root.setRowCount(1);

            // Serialize to Arrow IPC
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            byte[] arrowData = out.toByteArray();

            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(UUID.randomUUID().toString())
                .setResponseId(UUID.randomUUID().toString())
                .setArrowBatch(ExecutePlanResponse.ArrowBatch.newBuilder()
                    .setRowCount(1)
                    .setData(ByteString.copyFrom(arrowData))
                    .build())
                .build();

            responseObserver.onNext(response);

            // Send completion
            ExecutePlanResponse complete = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .setOperationId(UUID.randomUUID().toString())
                .setResponseId(UUID.randomUUID().toString())
                .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
                .build();

            responseObserver.onNext(complete);
            responseObserver.onCompleted();
        }
    }

    /**
     * Execute SQL and stream DataFrame result.
     */
    private void executeAndStreamDataFrame(String sql, Session session,
                                           StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        // Use cached executor from session (reuses shared allocator)
        ArrowStreamingExecutor executor = session.getStreamingExecutor();
        try (ArrowBatchIterator iterator = executor.executeStreaming(sql)) {
            StreamingResultHandler handler = new StreamingResultHandler(
                responseObserver,
                session.getSessionId(),
                UUID.randomUUID().toString()
            );
            handler.streamResults(iterator);
        }
        // Executor is NOT closed - session manages its lifecycle
    }

    /**
     * Execute crosstab query and stream with proper column naming.
     */
    private void executeAndStreamCrosstabResult(String sql, String col1, String col2,
                                                List<String> col2Values, Session session,
                                                StreamObserver<ExecutePlanResponse> responseObserver) throws Exception {
        // Execute the PIVOT query and rename first column
        // DuckDB PIVOT names the group column as the original column name
        // Spark expects it to be named "col1_col2"
        String wrapperSql = String.format(
            "SELECT c1 AS %s, * EXCLUDE (c1) FROM (%s)",
            quoteIdentifier(col1 + "_" + col2),
            sql
        );

        executeAndStreamDataFrame(wrapperSql, session, responseObserver);
    }
}
