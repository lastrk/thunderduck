package com.catalyst2sql.tpch;

import org.apache.arrow.vector.*;

/**
 * Command-line interface for executing TPC-H queries.
 *
 * <p>This tool allows execution of individual TPC-H queries with support for
 * different modes (execute, explain, analyze) and scale factors.
 *
 * <p>Usage examples:
 * <pre>
 * # Run single query with EXPLAIN
 * java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \
 *   --query 1 --mode explain --data ./data/tpch_sf001
 *
 * # Run with EXPLAIN ANALYZE
 * java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \
 *   --query 1 --mode analyze --data ./data/tpch_sf001
 *
 * # Execute query and show results
 * java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \
 *   --query 1 --mode execute --data ./data/tpch_sf1
 *
 * # Run all queries
 * java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \
 *   --query all --mode execute --data ./data/tpch_sf1
 * </pre>
 */
public class TPCHCommandLine {

    private static final String USAGE =
        "TPC-H Command Line Tool\n\n" +
        "Usage: java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine [OPTIONS]\n\n" +
        "Options:\n" +
        "  --query NUM     Query number (1-22) or 'all' for all queries\n" +
        "  --mode MODE     Execution mode: execute, explain, or analyze\n" +
        "  --data PATH     Path to TPC-H data directory\n" +
        "  --scale NUM     Scale factor (0.01, 1, 10, 100) - optional, inferred from path\n" +
        "  --help          Show this help message\n\n" +
        "Examples:\n" +
        "  # Run query 1 with EXPLAIN\n" +
        "  java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \\\n" +
        "    --query 1 --mode explain --data ./data/tpch_sf001\n\n" +
        "  # Run query 6 with EXPLAIN ANALYZE\n" +
        "  java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \\\n" +
        "    --query 6 --mode analyze --data ./data/tpch_sf001\n\n" +
        "  # Execute query 3 and show results\n" +
        "  java -cp benchmarks.jar com.catalyst2sql.tpch.TPCHCommandLine \\\n" +
        "    --query 3 --mode execute --data ./data/tpch_sf1\n";

    public static void main(String[] args) {
        try {
            // Parse command-line arguments
            CommandLineArgs parsedArgs = parseArguments(args);

            if (parsedArgs.help) {
                System.out.println(USAGE);
                System.exit(0);
            }

            // Validate arguments
            if (parsedArgs.dataPath == null || parsedArgs.mode == null) {
                System.err.println("Error: --data and --mode are required\n");
                System.err.println(USAGE);
                System.exit(1);
            }

            // Infer scale factor from data path if not provided
            double scaleFactor = parsedArgs.scaleFactor;
            if (scaleFactor == 0) {
                scaleFactor = inferScaleFactor(parsedArgs.dataPath);
            }

            // Create client
            try (TPCHClient client = new TPCHClient(parsedArgs.dataPath, scaleFactor)) {

                // Execute based on query parameter
                if ("all".equalsIgnoreCase(parsedArgs.query)) {
                    executeAllQueries(client, parsedArgs.mode);
                } else {
                    int queryNumber = Integer.parseInt(parsedArgs.query);
                    executeSingleQuery(client, queryNumber, parsedArgs.mode);
                }
            }

        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid query number: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Executes a single TPC-H query.
     */
    private static void executeSingleQuery(TPCHClient client, int queryNumber, String mode) {
        System.out.println("============================================================");
        System.out.println("TPC-H Query " + queryNumber);
        System.out.println("Mode: " + mode.toUpperCase());
        System.out.println("Data Path: " + client.getDataPath());
        System.out.println("Scale Factor: " + client.getScaleFactor());
        System.out.println("============================================================\n");

        long startTime = System.currentTimeMillis();

        try {
            switch (mode.toLowerCase()) {
                case "explain":
                    String explainOutput = client.explainQuery(queryNumber);
                    System.out.println(explainOutput);
                    break;

                case "analyze":
                    String analyzeOutput = client.explainAnalyzeQuery(queryNumber);
                    System.out.println(analyzeOutput);
                    break;

                case "execute":
                    VectorSchemaRoot result = client.executeQuery(queryNumber);
                    printResults(result);
                    result.close();
                    break;

                default:
                    throw new IllegalArgumentException("Invalid mode: " + mode +
                        ". Must be 'execute', 'explain', or 'analyze'");
            }

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("\n============================================================");
            System.out.println("Execution time: " + duration + " ms");
            System.out.println("============================================================");

        } catch (Exception e) {
            System.err.println("\nError executing query " + queryNumber + ": " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes all supported TPC-H queries.
     */
    private static void executeAllQueries(TPCHClient client, String mode) {
        int[] supportedQueries = {1, 3, 6};  // Currently supported queries

        System.out.println("============================================================");
        System.out.println("Executing All TPC-H Queries");
        System.out.println("Mode: " + mode.toUpperCase());
        System.out.println("Data Path: " + client.getDataPath());
        System.out.println("Scale Factor: " + client.getScaleFactor());
        System.out.println("============================================================\n");

        for (int queryNumber : supportedQueries) {
            try {
                executeSingleQuery(client, queryNumber, mode);
                System.out.println();  // Blank line between queries
            } catch (Exception e) {
                System.err.println("Failed to execute query " + queryNumber + ": " + e.getMessage());
                // Continue with next query
            }
        }
    }

    /**
     * Prints query results in a tabular format.
     */
    private static void printResults(VectorSchemaRoot root) {
        if (root.getRowCount() == 0) {
            System.out.println("(No results)");
            return;
        }

        // Print column headers
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            FieldVector vector = root.getFieldVectors().get(i);
            System.out.print(vector.getField().getName());
            if (i < root.getFieldVectors().size() - 1) {
                System.out.print("\t| ");
            }
        }
        System.out.println();

        // Print separator
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            System.out.print("----------------");
            if (i < root.getFieldVectors().size() - 1) {
                System.out.print("-+-");
            }
        }
        System.out.println();

        // Print rows (limit to first 100 rows for readability)
        int maxRows = Math.min(root.getRowCount(), 100);
        for (int row = 0; row < maxRows; row++) {
            for (int col = 0; col < root.getFieldVectors().size(); col++) {
                FieldVector vector = root.getFieldVectors().get(col);
                Object value = vector.getObject(row);
                System.out.print(value != null ? value.toString() : "NULL");
                if (col < root.getFieldVectors().size() - 1) {
                    System.out.print("\t| ");
                }
            }
            System.out.println();
        }

        if (root.getRowCount() > maxRows) {
            System.out.println("... (" + (root.getRowCount() - maxRows) + " more rows)");
        }

        System.out.println("\nTotal rows: " + root.getRowCount());
    }

    /**
     * Infers the scale factor from the data path.
     */
    private static double inferScaleFactor(String dataPath) {
        if (dataPath.contains("sf001") || dataPath.contains("sf_001")) {
            return 0.01;
        } else if (dataPath.contains("sf1") || dataPath.contains("sf_1")) {
            return 1.0;
        } else if (dataPath.contains("sf10") || dataPath.contains("sf_10")) {
            return 10.0;
        } else if (dataPath.contains("sf100") || dataPath.contains("sf_100")) {
            return 100.0;
        } else {
            System.out.println("Warning: Could not infer scale factor from path, assuming 1.0");
            return 1.0;
        }
    }

    /**
     * Parses command-line arguments.
     */
    private static CommandLineArgs parseArguments(String[] args) {
        CommandLineArgs result = new CommandLineArgs();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--query":
                    if (i + 1 < args.length) {
                        result.query = args[++i];
                    }
                    break;
                case "--mode":
                    if (i + 1 < args.length) {
                        result.mode = args[++i];
                    }
                    break;
                case "--data":
                    if (i + 1 < args.length) {
                        result.dataPath = args[++i];
                    }
                    break;
                case "--scale":
                    if (i + 1 < args.length) {
                        result.scaleFactor = Double.parseDouble(args[++i]);
                    }
                    break;
                case "--help":
                case "-h":
                    result.help = true;
                    break;
                default:
                    System.err.println("Warning: Unknown option: " + args[i]);
            }
        }

        return result;
    }

    /**
     * Container for parsed command-line arguments.
     */
    private static class CommandLineArgs {
        String query = "1";
        String mode = "explain";
        String dataPath = null;
        double scaleFactor = 0;  // 0 means infer from path
        boolean help = false;
    }
}
