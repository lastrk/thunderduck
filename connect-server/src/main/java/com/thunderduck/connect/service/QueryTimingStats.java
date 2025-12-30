package com.thunderduck.connect.service;

/**
 * Timing statistics collector for query execution phases.
 *
 * <p>Tracks timing for each phase of query execution:
 * <ul>
 *   <li>plan_convert: Protobuf → LogicalPlan conversion</li>
 *   <li>sql_generate: LogicalPlan → SQL string generation</li>
 *   <li>duckdb_execute: DuckDB query execution (time to first batch)</li>
 *   <li>result_stream: Arrow batch streaming to gRPC</li>
 *   <li>total: End-to-end time</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * QueryTimingStats timing = new QueryTimingStats();
 * timing.startTotal();
 *
 * timing.startPlanConvert();
 * // ... plan conversion ...
 * timing.stopPlanConvert();
 *
 * timing.startSqlGenerate();
 * // ... SQL generation ...
 * timing.stopSqlGenerate();
 *
 * timing.startDuckdbExecute();
 * // ... DuckDB execution ...
 * timing.stopDuckdbExecute();
 *
 * timing.startResultStream();
 * // ... result streaming ...
 * timing.stopResultStream();
 *
 * timing.stopTotal();
 * logger.info("Query timing: {}", timing.toLogString());
 * }</pre>
 */
public class QueryTimingStats {

    private long totalStartNanos;
    private long planConvertStartNanos;
    private long sqlGenerateStartNanos;
    private long duckdbExecuteStartNanos;
    private long resultStreamStartNanos;

    private long planConvertNanos;
    private long sqlGenerateNanos;
    private long duckdbExecuteNanos;
    private long resultStreamNanos;
    private long totalNanos;

    // Track number of batches and rows for context
    private int batchCount;
    private long rowCount;

    public QueryTimingStats() {
        // All values default to 0
    }

    // Total timing
    public void startTotal() {
        totalStartNanos = System.nanoTime();
    }

    public void stopTotal() {
        totalNanos = System.nanoTime() - totalStartNanos;
    }

    // Plan conversion timing
    public void startPlanConvert() {
        planConvertStartNanos = System.nanoTime();
    }

    public void stopPlanConvert() {
        planConvertNanos = System.nanoTime() - planConvertStartNanos;
    }

    // SQL generation timing
    public void startSqlGenerate() {
        sqlGenerateStartNanos = System.nanoTime();
    }

    public void stopSqlGenerate() {
        sqlGenerateNanos = System.nanoTime() - sqlGenerateStartNanos;
    }

    // DuckDB execution timing (time to first batch)
    public void startDuckdbExecute() {
        duckdbExecuteStartNanos = System.nanoTime();
    }

    public void stopDuckdbExecute() {
        duckdbExecuteNanos = System.nanoTime() - duckdbExecuteStartNanos;
    }

    // Result streaming timing
    public void startResultStream() {
        resultStreamStartNanos = System.nanoTime();
    }

    public void stopResultStream() {
        resultStreamNanos = System.nanoTime() - resultStreamStartNanos;
    }

    // Batch/row tracking
    public void setBatchCount(int count) {
        this.batchCount = count;
    }

    public void setRowCount(long count) {
        this.rowCount = count;
    }

    // Getters for milliseconds
    public double getPlanConvertMs() {
        return planConvertNanos / 1_000_000.0;
    }

    public double getSqlGenerateMs() {
        return sqlGenerateNanos / 1_000_000.0;
    }

    public double getDuckdbExecuteMs() {
        return duckdbExecuteNanos / 1_000_000.0;
    }

    public double getResultStreamMs() {
        return resultStreamNanos / 1_000_000.0;
    }

    public double getTotalMs() {
        return totalNanos / 1_000_000.0;
    }

    /**
     * Format timing stats as a log-friendly string.
     *
     * @return formatted string like "plan_convert=2.3ms, sql_generate=0.8ms, ..."
     */
    public String toLogString() {
        StringBuilder sb = new StringBuilder();

        if (planConvertNanos > 0) {
            sb.append(String.format("plan_convert=%.1fms, ", getPlanConvertMs()));
        }
        if (sqlGenerateNanos > 0) {
            sb.append(String.format("sql_generate=%.1fms, ", getSqlGenerateMs()));
        }
        if (duckdbExecuteNanos > 0) {
            sb.append(String.format("duckdb_execute=%.1fms, ", getDuckdbExecuteMs()));
        }
        if (resultStreamNanos > 0) {
            sb.append(String.format("result_stream=%.1fms, ", getResultStreamMs()));
        }
        if (totalNanos > 0) {
            sb.append(String.format("total=%.1fms", getTotalMs()));
        }
        if (batchCount > 0 || rowCount > 0) {
            sb.append(String.format(" (%d batches, %d rows)", batchCount, rowCount));
        }

        return sb.toString();
    }

    /**
     * Calculate overhead (plan_convert + sql_generate) as percentage of total.
     *
     * @return overhead percentage (0-100)
     */
    public double getOverheadPercent() {
        if (totalNanos == 0) return 0;
        long overheadNanos = planConvertNanos + sqlGenerateNanos;
        return (overheadNanos * 100.0) / totalNanos;
    }
}
