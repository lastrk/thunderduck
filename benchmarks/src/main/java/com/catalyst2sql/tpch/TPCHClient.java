package com.catalyst2sql.tpch;

import com.catalyst2sql.runtime.DuckDBConnectionManager;
import com.catalyst2sql.runtime.QueryExecutor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Programmatic client for executing TPC-H queries using catalyst2sql.
 *
 * <p>This client provides a simple Java API for executing TPC-H benchmark queries
 * with support for EXPLAIN and EXPLAIN ANALYZE modes.
 *
 * <p>Usage example:
 * <pre>
 * TPCHClient client = new TPCHClient("./data/tpch_sf001", 0.01);
 *
 * // Execute query
 * VectorSchemaRoot result = client.executeQuery(1);
 *
 * // Get EXPLAIN output
 * String plan = client.explainQuery(1);
 *
 * // Get EXPLAIN ANALYZE output
 * String stats = client.explainAnalyzeQuery(1);
 *
 * client.close();
 * </pre>
 */
public class TPCHClient implements AutoCloseable {

    private final String dataPath;
    private final double scaleFactor;
    private final DuckDBConnectionManager connectionManager;
    private final QueryExecutor executor;

    /**
     * Creates a new TPC-H client.
     *
     * @param dataPath path to TPC-H data directory containing Parquet files
     * @param scaleFactor TPC-H scale factor (0.01, 1, 10, 100)
     */
    public TPCHClient(String dataPath, double scaleFactor) {
        this.dataPath = dataPath;
        this.scaleFactor = scaleFactor;
        this.connectionManager = new DuckDBConnectionManager();
        this.executor = new QueryExecutor(connectionManager);
    }

    /**
     * Executes a TPC-H query and returns the result.
     *
     * @param queryNumber TPC-H query number (1-22)
     * @return VectorSchemaRoot containing query results
     */
    public VectorSchemaRoot executeQuery(int queryNumber) {
        String sql = loadQuerySQL(queryNumber);
        return executor.executeQuery(sql);
    }

    /**
     * Returns the EXPLAIN output for a TPC-H query.
     *
     * @param queryNumber TPC-H query number (1-22)
     * @return EXPLAIN output as a String
     */
    public String explainQuery(int queryNumber) {
        String sql = "EXPLAIN " + loadQuerySQL(queryNumber);
        VectorSchemaRoot result = executor.executeQuery(sql);

        try {
            return extractTextFromResult(result);
        } finally {
            result.close();
        }
    }

    /**
     * Returns the EXPLAIN ANALYZE output for a TPC-H query.
     *
     * @param queryNumber TPC-H query number (1-22)
     * @return EXPLAIN ANALYZE output as a String with execution statistics
     */
    public String explainAnalyzeQuery(int queryNumber) {
        String sql = "EXPLAIN ANALYZE " + loadQuerySQL(queryNumber);
        VectorSchemaRoot result = executor.executeQuery(sql);

        try {
            return extractTextFromResult(result);
        } finally {
            result.close();
        }
    }

    /**
     * Loads the SQL for a specific TPC-H query.
     *
     * @param queryNumber TPC-H query number (1-22)
     * @return SQL query string with data paths substituted
     */
    private String loadQuerySQL(int queryNumber) {
        // For now, return hardcoded queries for the essential ones
        // TODO: Load from SQL files in resources
        switch (queryNumber) {
            case 1:
                return getQuery1SQL();
            case 3:
                return getQuery3SQL();
            case 6:
                return getQuery6SQL();
            default:
                throw new IllegalArgumentException(
                    "Query " + queryNumber + " not yet implemented. " +
                    "Currently supported queries: 1, 3, 6");
        }
    }

    /**
     * TPC-H Query 1: Pricing Summary Report
     */
    private String getQuery1SQL() {
        return String.format(
            "SELECT\n" +
            "    l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    SUM(l_quantity) as sum_qty,\n" +
            "    SUM(l_extendedprice) as sum_base_price,\n" +
            "    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
            "    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
            "    AVG(l_quantity) as avg_qty,\n" +
            "    AVG(l_extendedprice) as avg_price,\n" +
            "    AVG(l_discount) as avg_disc,\n" +
            "    COUNT(*) as count_order\n" +
            "FROM read_parquet('%s/lineitem.parquet')\n" +
            "WHERE l_shipdate <= DATE '1998-12-01'\n" +
            "GROUP BY l_returnflag, l_linestatus\n" +
            "ORDER BY l_returnflag, l_linestatus",
            dataPath
        );
    }

    /**
     * TPC-H Query 3: Shipping Priority
     */
    private String getQuery3SQL() {
        return String.format(
            "SELECT\n" +
            "    l_orderkey,\n" +
            "    SUM(l_extendedprice * (1 - l_discount)) as revenue,\n" +
            "    o_orderdate,\n" +
            "    o_shippriority\n" +
            "FROM read_parquet('%s/customer.parquet') c,\n" +
            "     read_parquet('%s/orders.parquet') o,\n" +
            "     read_parquet('%s/lineitem.parquet') l\n" +
            "WHERE c_mktsegment = 'BUILDING'\n" +
            "  AND c_custkey = o_custkey\n" +
            "  AND l_orderkey = o_orderkey\n" +
            "  AND o_orderdate < DATE '1995-03-15'\n" +
            "  AND l_shipdate > DATE '1995-03-15'\n" +
            "GROUP BY l_orderkey, o_orderdate, o_shippriority\n" +
            "ORDER BY revenue DESC, o_orderdate\n" +
            "LIMIT 10",
            dataPath, dataPath, dataPath
        );
    }

    /**
     * TPC-H Query 6: Forecasting Revenue Change
     */
    private String getQuery6SQL() {
        return String.format(
            "SELECT\n" +
            "    SUM(l_extendedprice * l_discount) as revenue\n" +
            "FROM read_parquet('%s/lineitem.parquet')\n" +
            "WHERE l_shipdate >= DATE '1994-01-01'\n" +
            "  AND l_shipdate < DATE '1995-01-01'\n" +
            "  AND l_discount BETWEEN 0.05 AND 0.07\n" +
            "  AND l_quantity < 24",
            dataPath
        );
    }

    /**
     * Extracts text from a VectorSchemaRoot result (for EXPLAIN output).
     */
    private String extractTextFromResult(VectorSchemaRoot result) {
        StringBuilder output = new StringBuilder();
        VarCharVector vector = (VarCharVector) result.getVector(0);

        for (int i = 0; i < result.getRowCount(); i++) {
            if (vector.isNull(i)) {
                output.append("\n");
            } else {
                output.append(vector.getObject(i).toString()).append("\n");
            }
        }

        return output.toString();
    }

    @Override
    public void close() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    /**
     * Returns the data path being used.
     */
    public String getDataPath() {
        return dataPath;
    }

    /**
     * Returns the scale factor.
     */
    public double getScaleFactor() {
        return scaleFactor;
    }
}
