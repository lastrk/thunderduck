package com.thunderduck.connect.converter;

import com.thunderduck.logical.*;
import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.AliasExpression;
import com.thunderduck.expression.UnresolvedColumn;
import com.thunderduck.logical.LocalDataRelation;
import com.thunderduck.logical.RangeRelation;
import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.generator.SQLGenerator;
import com.thunderduck.schema.SchemaInferrer;
import com.thunderduck.types.StructType;
import com.thunderduck.types.StructField;

import org.apache.spark.connect.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts Spark Connect Relation types to thunderduck LogicalPlan nodes.
 *
 * <p>This class handles the conversion of all relation types including:
 * Read, Project, Filter, Aggregate, Sort, Limit, Join, Union, etc.
 */
public class RelationConverter {
    private static final Logger logger = LoggerFactory.getLogger(RelationConverter.class);

    private final ExpressionConverter expressionConverter;
    private final SchemaInferrer schemaInferrer;

    /**
     * Creates a RelationConverter without schema inference capability.
     */
    public RelationConverter(ExpressionConverter expressionConverter) {
        this(expressionConverter, null);
    }

    /**
     * Creates a RelationConverter with optional schema inference capability.
     *
     * @param expressionConverter the expression converter
     * @param schemaInferrer the schema inferrer (nullable)
     */
    public RelationConverter(ExpressionConverter expressionConverter, SchemaInferrer schemaInferrer) {
        this.expressionConverter = expressionConverter;
        this.schemaInferrer = schemaInferrer;
    }

    /**
     * Converts a Spark Connect Relation to a LogicalPlan.
     *
     * <p>Also extracts and preserves the optional plan_id from the Relation's common
     * field. The plan_id uniquely identifies this relation within a Spark Connect
     * session and is used to resolve ambiguous column references in joins.
     *
     * @param relation the Protobuf relation
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convert(Relation relation) {
        logger.debug("Converting relation type: {}", relation.getRelTypeCase());

        LogicalPlan result = convertInternal(relation);

        // Extract and set plan_id if present in the common field
        if (relation.hasCommon() && relation.getCommon().hasPlanId()) {
            long planId = relation.getCommon().getPlanId();
            result.setPlanId(planId);
            logger.trace("Set plan_id {} on {}", planId, result.getClass().getSimpleName());
        }

        return result;
    }

    /**
     * Internal conversion method that handles specific relation types.
     */
    private LogicalPlan convertInternal(Relation relation) {
        switch (relation.getRelTypeCase()) {
            case READ:
                return convertRead(relation.getRead());
            case PROJECT:
                return convertProject(relation.getProject());
            case FILTER:
                return convertFilter(relation.getFilter());
            case AGGREGATE:
                return convertAggregate(relation.getAggregate());
            case LOCAL_RELATION:
                return convertLocalRelation(relation.getLocalRelation());
            case SORT:
                return convertSort(relation.getSort());
            case LIMIT:
                return convertLimit(relation.getLimit());
            case TAIL:
                return convertTail(relation.getTail());
            case JOIN:
                return convertJoin(relation.getJoin());
            case SET_OP:
                return convertSetOp(relation.getSetOp());
            case SQL:
                return convertSQL(relation.getSql());
            case SHOW_STRING:
                // ShowString is handled at root level in SparkConnectServiceImpl.executeShowString()
                // If we reach here, something is wrong - ShowString should never be nested
                throw new PlanConversionException(
                    "ShowString should be handled at root level, not in RelationConverter");
            case DEDUPLICATE:
                // Handle distinct() operation
                return convertDeduplicate(relation.getDeduplicate());
            case RANGE:
                return convertRange(relation.getRange());
            case DROP:
                return convertDrop(relation.getDrop());
            case WITH_COLUMNS_RENAMED:
                return convertWithColumnsRenamed(relation.getWithColumnsRenamed());
            case WITH_COLUMNS:
                return convertWithColumns(relation.getWithColumns());
            case OFFSET:
                return convertOffset(relation.getOffset());
            case TO_DF:
                return convertToDF(relation.getToDf());
            case SAMPLE:
                return convertSample(relation.getSample());
            case HINT:
                // Hints are no-ops in DuckDB - pass through to child relation
                // DuckDB's optimizer handles join ordering and strategies automatically
                return convert(relation.getHint().getInput());
            case REPARTITION:
                // Repartition is a no-op in single-node DuckDB
                return convert(relation.getRepartition().getInput());
            case REPARTITION_BY_EXPRESSION:
                // RepartitionByExpression is a no-op in single-node DuckDB
                return convert(relation.getRepartitionByExpression().getInput());
            case DROP_NA:
                return convertNADrop(relation.getDropNa());
            case FILL_NA:
                return convertNAFill(relation.getFillNa());
            case REPLACE:
                return convertNAReplace(relation.getReplace());
            case UNPIVOT:
                return convertUnpivot(relation.getUnpivot());
            case SUBQUERY_ALIAS:
                return convertSubqueryAlias(relation.getSubqueryAlias());
            case SAMPLE_BY:
                return convertSampleBy(relation.getSampleBy());
            case TO_SCHEMA:
                return convertToSchema(relation.getToSchema());
            case DESCRIBE:
                return convertDescribe(relation.getDescribe());
            case SUMMARY:
                return convertSummary(relation.getSummary());
            case COV:
                return convertCov(relation.getCov());
            case CORR:
                return convertCorr(relation.getCorr());
            default:
                throw new PlanConversionException("Unsupported relation type: " + relation.getRelTypeCase());
        }
    }

    /**
     * Converts a Read relation to a TableScan.
     *
     * @param read the Read relation
     * @return a TableScan logical plan
     */
    private LogicalPlan convertRead(Read read) {
        if (read.hasDataSource()) {
            Read.DataSource dataSource = read.getDataSource();
            String format = dataSource.getFormat();

            // Handle parquet files
            if ("parquet".equals(format) || format == null || format.isEmpty()) {
                Map<String, String> options = dataSource.getOptionsMap();

                // Get the path from options or paths list (PySpark uses various methods)
                String path = null;

                // Method 1: Check paths list first (most common for read.parquet())
                if (dataSource.getPathsCount() > 0) {
                    path = dataSource.getPaths(0);
                    logger.debug("Got path from paths list: {}", path);
                }

                // Method 2: Check options map
                if (path == null || path.isEmpty()) {
                    path = options.get("path");
                    if (path != null && !path.isEmpty()) {
                        logger.debug("Got path from options['path']: {}", path);
                    }
                }

                // Method 3: Check paths option
                if (path == null || path.isEmpty()) {
                    String pathsOption = options.get("paths");
                    if (pathsOption != null && !pathsOption.isEmpty()) {
                        path = pathsOption.split(",")[0].trim();
                        logger.debug("Got path from options['paths']: {}", path);
                    }
                }

                if (path == null || path.isEmpty()) {
                    logger.error("No path found. Options: {}, Paths count: {}",
                        options, dataSource.getPathsCount());
                    throw new PlanConversionException("No path specified for parquet read");
                }

                logger.debug("Creating TableScan for parquet file: {}", path);
                // Infer schema from parquet file if schemaInferrer is available
                StructType schema = inferParquetSchema(path);
                return new TableScan(path, TableScan.TableFormat.PARQUET, schema);
            }

            // Add support for other formats as needed
            throw new PlanConversionException("Unsupported data source format: " + format);
        } else if (read.hasNamedTable()) {
            Read.NamedTable namedTable = read.getNamedTable();
            String tableName = namedTable.getUnparsedIdentifier();
            logger.debug("Creating TableScan for table: {}", tableName);
            // Infer schema from named table if schemaInferrer is available
            StructType schema = inferTableSchema(tableName);
            return new TableScan(tableName, TableScan.TableFormat.TABLE, schema);
        } else {
            throw new PlanConversionException("Read relation must have either data_source or named_table");
        }
    }

    /**
     * Converts a Project relation.
     *
     * @param project the Project relation
     * @return a Project logical plan
     */
    private LogicalPlan convertProject(org.apache.spark.connect.proto.Project project) {
        // Convert input relation if present
        LogicalPlan input = null;
        if (project.hasInput()) {
            input = convert(project.getInput());
        } else {
            // Project without input (e.g., SELECT 1)
            // Create a dummy single-row relation
            input = new SingleRowRelation();
        }

        // Convert projection expressions and extract aliases
        List<Expression> convertedExpressions = project.getExpressionsList().stream()
                .map(expressionConverter::convert)
                .toList();

        // Extract aliases and unwrap AliasExpressions
        List<Expression> expressions = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for (Expression expr : convertedExpressions) {
            if (expr instanceof AliasExpression) {
                // Explicit alias provided
                AliasExpression aliasExpr = (AliasExpression) expr;
                expressions.add(aliasExpr.expression());
                aliases.add(aliasExpr.alias());
            } else if (expr instanceof UnresolvedColumn) {
                // Simple column selection: use column name as alias
                UnresolvedColumn col = (UnresolvedColumn) expr;
                expressions.add(expr);
                aliases.add(col.columnName());
            } else {
                // Complex expression without alias
                expressions.add(expr);
                aliases.add(null);  // No alias
            }
        }

        logger.debug("Creating Project with {} expressions", expressions.size());
        return new com.thunderduck.logical.Project(input, expressions, aliases);
    }

    /**
     * Converts a Filter relation.
     *
     * @param filter the Filter relation
     * @return a Filter logical plan
     */
    private LogicalPlan convertFilter(org.apache.spark.connect.proto.Filter filter) {
        LogicalPlan input = convert(filter.getInput());
        Expression condition = expressionConverter.convert(filter.getCondition());

        logger.debug("Creating Filter with condition: {}", condition);
        return new com.thunderduck.logical.Filter(input, condition);
    }

    /**
     * Converts an Aggregate relation.
     *
     * @param aggregate the Aggregate relation
     * @return an Aggregate logical plan
     */
    private LogicalPlan convertAggregate(org.apache.spark.connect.proto.Aggregate aggregate) {
        LogicalPlan input = convert(aggregate.getInput());

        // Convert grouping expressions
        List<Expression> groupingExprs = aggregate.getGroupingExpressionsList().stream()
                .map(expressionConverter::convert)
                .toList();

        // Convert aggregate expressions
        List<Expression> aggregateExprs = aggregate.getAggregateExpressionsList().stream()
                .map(expressionConverter::convert)
                .toList();

        // Check for PIVOT group type
        if (aggregate.getGroupType() == org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_PIVOT) {
            logger.debug("Creating PIVOT aggregate with {} grouping and {} aggregate expressions",
                    groupingExprs.size(), aggregateExprs.size());
            return convertPivotAggregate(aggregate, input, groupingExprs, aggregateExprs);
        }

        logger.debug("Creating Aggregate with {} grouping and {} aggregate expressions",
                groupingExprs.size(), aggregateExprs.size());

        // Convert aggregate expressions to AggregateExpression objects
        List<com.thunderduck.logical.Aggregate.AggregateExpression> aggExprs = new ArrayList<>();
        for (Expression expr : aggregateExprs) {
            // Extract function name, argument, and alias from expression
            String functionName = null;
            Expression argument = null;
            String alias = null;

            // Unwrap AliasExpression if present
            if (expr instanceof AliasExpression) {
                AliasExpression aliasExpr = (AliasExpression) expr;
                alias = aliasExpr.alias();
                expr = aliasExpr.expression();  // Unwrap to get inner expression
            }

            // Extract function name and arguments
            if (expr instanceof FunctionCall) {
                FunctionCall func = (FunctionCall) expr;
                functionName = func.functionName();

                // Handle multi-argument aggregate functions
                List<Expression> args = func.arguments();
                if (args.isEmpty()) {
                    argument = null;
                } else if (args.size() == 1) {
                    argument = args.get(0);
                } else {
                    // Multiple arguments - for countDistinct, wrap in ROW() for proper tuple semantics
                    // DuckDB's COUNT(DISTINCT col1, col2) only counts distinct col1 (wrong!)
                    // Instead, use COUNT(DISTINCT ROW(col1, col2)) to count distinct tuples
                    String funcLower = functionName.toLowerCase();
                    if (funcLower.equals("count_distinct") || funcLower.equals("countdistinct")) {
                        // Create an inline expression that outputs ROW(col1, col2, ...)
                        final List<Expression> capturedArgs = args;
                        argument = new Expression() {
                            @Override
                            public com.thunderduck.types.DataType dataType() {
                                return com.thunderduck.types.StringType.get(); // Placeholder
                            }
                            @Override
                            public boolean nullable() {
                                return true;
                            }
                            @Override
                            public String toSQL() {
                                String argsSQL = capturedArgs.stream()
                                    .map(Expression::toSQL)
                                    .collect(Collectors.joining(", "));
                                return "ROW(" + argsSQL + ")";
                            }
                            @Override
                            public String toString() {
                                return toSQL();
                            }
                        };
                        logger.debug("Wrapped {} arguments in ROW() for countDistinct", args.size());
                    } else if (funcLower.equals("grouping_id") || funcLower.equals("grouping")) {
                        // grouping_id needs all arguments preserved — use composite expression.
                        // The FunctionRegistry custom translator handles bit-order reversal
                        // (Spark vs DuckDB use opposite bit ordering for grouping_id).
                        aggExprs.add(new com.thunderduck.logical.Aggregate.AggregateExpression(
                            new FunctionCall(functionName, args, func.dataType(), func.nullable(), func.distinct()),
                            alias));
                        continue;
                    } else {
                        // For other multi-arg functions, just use the first argument
                        // (may need to handle more cases in the future)
                        argument = args.get(0);
                        logger.warn("Multi-argument aggregate {} only using first argument", functionName);
                    }
                }
            } else {
                // Composite aggregate expression (e.g., SUM(a) / SUM(b), SUM(a) * 0.5)
                // The expression tree already has correct toSQL() rendering
                logger.debug("Composite aggregate expression: {}", expr.getClass().getSimpleName());
                aggExprs.add(new com.thunderduck.logical.Aggregate.AggregateExpression(expr, alias));
                continue;
            }

            if (functionName != null) {
                com.thunderduck.logical.Aggregate.AggregateExpression aggExpr =
                    new com.thunderduck.logical.Aggregate.AggregateExpression(
                        functionName,
                        argument,
                        alias
                    );
                aggExprs.add(aggExpr);
                logger.debug("Added aggregate: {}({}) AS {}", functionName, argument, alias);
            }
        }

        if (aggExprs.isEmpty()) {
            logger.error("No aggregate expressions extracted. Input had {} expressions", aggregateExprs.size());
            throw new PlanConversionException("Failed to convert aggregate expressions");
        }

        // Detect CUBE/ROLLUP/GROUPING_SETS group types
        GroupingSets groupingSets = null;
        org.apache.spark.connect.proto.Aggregate.GroupType groupType = aggregate.getGroupType();
        if (groupType == org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_CUBE) {
            groupingSets = GroupingSets.cube(groupingExprs);
            logger.debug("Creating CUBE aggregate with {} grouping expressions", groupingExprs.size());
        } else if (groupType == org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP) {
            groupingSets = GroupingSets.rollup(groupingExprs);
            logger.debug("Creating ROLLUP aggregate with {} grouping expressions", groupingExprs.size());
        } else if (groupType == org.apache.spark.connect.proto.Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS) {
            List<List<Expression>> sets = new ArrayList<>();
            for (org.apache.spark.connect.proto.Aggregate.GroupingSets gs : aggregate.getGroupingSetsList()) {
                List<Expression> set = gs.getGroupingSetList().stream()
                        .map(expressionConverter::convert)
                        .toList();
                sets.add(set);
            }
            groupingSets = GroupingSets.groupingSets(sets);
            logger.debug("Creating GROUPING SETS aggregate with {} sets", sets.size());
        }

        return new com.thunderduck.logical.Aggregate(input, groupingExprs, aggExprs, null, groupingSets);
    }

    /**
     * Converts a PIVOT aggregate operation.
     *
     * <p>Pivot transforms rows into columns based on distinct values of a pivot column.
     * For example, given:
     * <pre>
     * | country | year | sales |
     * |---------|------|-------|
     * | US      | 2023 | 1000  |
     * | US      | 2024 | 1200  |
     * </pre>
     *
     * With pivot on "year" and aggregate SUM(sales), produces:
     * <pre>
     * | country | 2023 | 2024 |
     * |---------|------|------|
     * | US      | 1000 | 1200 |
     * </pre>
     *
     * <p>Uses DuckDB's native PIVOT syntax:
     * <pre>
     * PIVOT (input) ON pivot_col IN (val1, val2, ...) USING agg(col) GROUP BY grouping_cols
     * </pre>
     *
     * @param aggregate the Aggregate protobuf message with PIVOT group type
     * @param input the input logical plan
     * @param groupingExprs the GROUP BY expressions
     * @param aggregateExprs the aggregate expressions (already converted)
     * @return a SQLRelation with DuckDB PIVOT syntax
     */
    private LogicalPlan convertPivotAggregate(
            org.apache.spark.connect.proto.Aggregate aggregate,
            LogicalPlan input,
            List<Expression> groupingExprs,
            List<Expression> aggregateExprs) {

        // Get pivot metadata
        org.apache.spark.connect.proto.Aggregate.Pivot pivotMsg = aggregate.getPivot();
        String pivotColName = extractColumnName(pivotMsg.getCol());
        if (pivotColName == null) {
            throw new PlanConversionException("Pivot column must be a column reference");
        }

        // Generate SQL for input
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // Get pivot values - either explicit or auto-discovered
        List<Object> pivotValues = new ArrayList<>();
        if (pivotMsg.getValuesCount() > 0) {
            // Explicit values provided
            for (org.apache.spark.connect.proto.Expression.Literal lit : pivotMsg.getValuesList()) {
                pivotValues.add(convertLiteralToJava(lit));
            }
            logger.debug("Pivot: using {} explicit values", pivotValues.size());
        } else {
            // Auto-discover distinct values (full Spark parity)
            pivotValues = discoverPivotValues(inputSql, pivotColName);
            logger.debug("Pivot: discovered {} values", pivotValues.size());
        }

        if (pivotValues.isEmpty()) {
            // Return empty result if no pivot values found
            logger.warn("Pivot: no values found, returning empty result");
            return createEmptyPivotResult(groupingExprs, aggregateExprs, pivotColName, pivotValues);
        }

        // Build aggregate expressions SQL for USING clause
        String aggregatesSql = formatPivotAggregates(aggregateExprs);

        // Build GROUP BY clause
        String groupBySql = formatGroupByColumns(groupingExprs);

        // Build pivot values IN clause
        String valuesSql = formatPivotValuesForSQL(pivotValues);

        // Build DuckDB PIVOT SQL
        String pivotSql;
        if (groupBySql.isEmpty()) {
            pivotSql = String.format(
                "PIVOT (%s) ON %s IN (%s) USING %s",
                inputSql,
                SQLQuoting.quoteIdentifier(pivotColName),
                valuesSql,
                aggregatesSql
            );
        } else {
            pivotSql = String.format(
                "PIVOT (%s) ON %s IN (%s) USING %s GROUP BY %s",
                inputSql,
                SQLQuoting.quoteIdentifier(pivotColName),
                valuesSql,
                aggregatesSql,
                groupBySql
            );
        }

        // Wrap in SELECT with type casts for Spark compatibility
        String sql = buildPivotSelectWrapper(pivotSql, groupingExprs, aggregateExprs, pivotValues);

        logger.debug("Creating Pivot SQL: {}", sql);

        // Build output schema with correct nullable flags
        StructType outputSchema = buildPivotOutputSchema(input.schema(), groupingExprs, aggregateExprs, pivotValues);

        // SQLRelation with schema for nullable flag correction
        return new SQLRelation(sql, outputSchema);
    }

    /**
     * Builds the output schema for a pivot operation with correct nullable flags.
     *
     * <p>Grouping columns preserve their nullability from the input schema.
     * Pivot value columns (aggregates) are nullable because some groups may
     * not have values for all pivot column values.
     */
    private StructType buildPivotOutputSchema(
            StructType inputSchema,
            List<Expression> groupingExprs,
            List<Expression> aggregateExprs,
            List<Object> pivotValues) {

        List<StructField> fields = new ArrayList<>();
        boolean multipleAggregates = aggregateExprs.size() > 1;

        // Add grouping columns - preserve nullability from input schema
        for (Expression expr : groupingExprs) {
            String colName = extractExpressionColumnName(expr);
            StructField inputField = inputSchema.fieldByName(colName);

            if (inputField != null) {
                // Preserve input nullability and type
                fields.add(inputField);
            } else {
                // Fallback if not found in schema
                fields.add(new StructField(colName, com.thunderduck.types.StringType.get(), true));
            }
        }

        // Add pivot value columns - these are always nullable because some groups
        // may not have values for all pivot column values
        for (Object value : pivotValues) {
            String valueStr = formatPivotValueAsColumnName(value);

            for (Expression aggExpr : aggregateExprs) {
                String colName;
                String aggAlias = extractAggregateAlias(aggExpr);

                if (multipleAggregates) {
                    colName = valueStr + "_" + aggAlias;
                } else {
                    colName = valueStr;
                }

                // Get the Spark type for this aggregate
                com.thunderduck.types.DataType dataType = getSparkDataTypeForAggregate(aggExpr, inputSchema);
                fields.add(new StructField(colName, dataType, true));
            }
        }

        return new StructType(fields);
    }

    /**
     * Gets the Spark DataType for an aggregate expression.
     *
     * <p>For type-preserving aggregates (MAX, MIN), we need the input schema
     * to determine the correct return type.
     */
    private com.thunderduck.types.DataType getSparkDataTypeForAggregate(
            Expression aggExpr, StructType inputSchema) {
        Expression innerExpr = aggExpr;
        if (aggExpr instanceof AliasExpression) {
            innerExpr = ((AliasExpression) aggExpr).expression();
        }

        if (innerExpr instanceof FunctionCall) {
            FunctionCall funcCall = (FunctionCall) innerExpr;
            String funcName = funcCall.functionName().toLowerCase();
            switch (funcName) {
                case "sum":
                case "count":
                    return com.thunderduck.types.LongType.get();
                case "avg":
                    return com.thunderduck.types.DoubleType.get();
                case "max":
                case "min":
                    // MAX/MIN preserve the input type
                    return getInputTypeFromAggregate(funcCall, inputSchema);
                default:
                    return com.thunderduck.types.LongType.get();
            }
        }
        return com.thunderduck.types.LongType.get();
    }

    /**
     * Gets the input type for a type-preserving aggregate (MAX, MIN).
     */
    private com.thunderduck.types.DataType getInputTypeFromAggregate(
            FunctionCall funcCall, StructType inputSchema) {
        // Get the argument expression
        List<Expression> args = funcCall.arguments();
        if (args.isEmpty()) {
            return com.thunderduck.types.LongType.get();
        }

        Expression arg = args.get(0);

        // If it's a column reference, look up the type in the schema
        if (arg instanceof UnresolvedColumn) {
            String colName = ((UnresolvedColumn) arg).columnName();
            StructField field = inputSchema.fieldByName(colName);
            if (field != null) {
                return field.dataType();
            }
        }

        // Default to Long
        return com.thunderduck.types.LongType.get();
    }

    /**
     * Builds a SELECT wrapper around PIVOT for Spark type compatibility.
     *
     * <p>DuckDB's PIVOT returns different types than Spark for some aggregates:
     * - SUM on integers: DuckDB returns DECIMAL(38,0), Spark returns BIGINT
     *
     * This wrapper adds CASTs to ensure type parity.
     */
    private String buildPivotSelectWrapper(
            String pivotSql,
            List<Expression> groupingExprs,
            List<Expression> aggregateExprs,
            List<Object> pivotValues) {

        List<String> selectColumns = new ArrayList<>();
        boolean multipleAggregates = aggregateExprs.size() > 1;

        // Add grouping columns
        for (Expression expr : groupingExprs) {
            String colName = extractExpressionColumnName(expr);
            selectColumns.add(SQLQuoting.quoteIdentifier(colName));
        }

        // Add pivot columns with type casts
        for (Object value : pivotValues) {
            String valueStr = formatPivotValueAsColumnName(value);

            for (Expression aggExpr : aggregateExprs) {
                String colName;
                String aggAlias = extractAggregateAlias(aggExpr);

                if (multipleAggregates) {
                    colName = valueStr + "_" + aggAlias;
                } else {
                    colName = valueStr;
                }

                // Determine if we need a type cast
                String castType = getSparkTypeForAggregate(aggExpr);
                String quotedCol = SQLQuoting.quoteIdentifier(colName);

                if (castType != null) {
                    // Cast to Spark-compatible type
                    selectColumns.add("CAST(" + quotedCol + " AS " + castType + ") AS " + quotedCol);
                } else {
                    selectColumns.add(quotedCol);
                }
            }
        }

        return "SELECT " + String.join(", ", selectColumns) + " FROM (" + pivotSql + ")";
    }

    /**
     * Determines the DuckDB type to cast to for Spark compatibility.
     *
     * @return the DuckDB type name to cast to, or null if no cast needed
     */
    private String getSparkTypeForAggregate(Expression aggExpr) {
        // Unwrap alias
        Expression innerExpr = aggExpr;
        if (aggExpr instanceof AliasExpression) {
            innerExpr = ((AliasExpression) aggExpr).expression();
        }

        // Check if it's a function call
        if (innerExpr instanceof FunctionCall) {
            String funcName = ((FunctionCall) innerExpr).functionName().toLowerCase();

            // SUM on integers in Spark returns BIGINT, DuckDB returns DECIMAL
            // COUNT also returns BIGINT in Spark
            switch (funcName) {
                case "sum":
                    return "BIGINT";
                case "count":
                    return "BIGINT";
                default:
                    return null;  // No cast needed for AVG, MAX, MIN, etc.
            }
        }

        return null;
    }

    /**
     * Discovers distinct pivot values from the input data.
     *
     * <p>When pivot values are not explicitly provided, Spark discovers them
     * by querying distinct values of the pivot column.
     *
     * @param inputSql the input SQL query
     * @param pivotColName the pivot column name
     * @return list of distinct pivot values (sorted, excluding NULLs)
     */
    private List<Object> discoverPivotValues(String inputSql, String pivotColName) {
        if (schemaInferrer == null) {
            throw new PlanConversionException(
                "Pivot with auto-discovery requires schema inference, but no connection available");
        }

        String discoverySql = String.format(
            "SELECT DISTINCT %s FROM (%s) WHERE %s IS NOT NULL ORDER BY %s",
            SQLQuoting.quoteIdentifier(pivotColName),
            inputSql,
            SQLQuoting.quoteIdentifier(pivotColName),
            SQLQuoting.quoteIdentifier(pivotColName)
        );

        List<Object> values = new ArrayList<>();
        try {
            java.sql.ResultSet rs = schemaInferrer.getConnection().createStatement().executeQuery(discoverySql);
            while (rs.next()) {
                values.add(rs.getObject(1));
            }
            rs.close();
        } catch (java.sql.SQLException e) {
            throw new PlanConversionException("Failed to discover pivot values: " + e.getMessage(), e);
        }

        return values;
    }

    /**
     * Converts a protobuf Literal to a Java object.
     */
    private Object convertLiteralToJava(org.apache.spark.connect.proto.Expression.Literal lit) {
        switch (lit.getLiteralTypeCase()) {
            case INTEGER:
                return lit.getInteger();
            case LONG:
                return lit.getLong();
            case DOUBLE:
                return lit.getDouble();
            case FLOAT:
                return (double) lit.getFloat();
            case STRING:
                return lit.getString();
            case BOOLEAN:
                return lit.getBoolean();
            case SHORT:
                return lit.getShort();
            case BYTE:
                return lit.getByte();
            case DATE:
                // DuckDB date is days since epoch
                return java.time.LocalDate.ofEpochDay(lit.getDate());
            case NULL:
                return null;
            default:
                // For other types, convert to string
                return lit.toString();
        }
    }

    /**
     * Formats pivot values for SQL IN clause.
     */
    private String formatPivotValuesForSQL(List<Object> values) {
        return values.stream()
            .map(this::formatValueForSQL)
            .collect(Collectors.joining(", "));
    }

    /**
     * Formats a single value for SQL (handles strings, numbers, dates).
     */
    private String formatValueForSQL(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof java.time.LocalDate) {
            return "DATE '" + value + "'";
        }
        if (value instanceof java.sql.Date) {
            return "DATE '" + value + "'";
        }
        if (value instanceof Boolean) {
            return value.toString().toUpperCase();
        }
        // Default: treat as string
        return "'" + value.toString().replace("'", "''") + "'";
    }

    /**
     * Formats aggregate expressions for USING clause with aliases.
     *
     * <p>DuckDB PIVOT supports aliases in the USING clause:
     * {@code PIVOT ... USING SUM(sales) AS total, COUNT(*) AS cnt}
     */
    private String formatPivotAggregates(List<Expression> aggregateExprs) {
        List<String> aggStrings = new ArrayList<>();
        for (Expression expr : aggregateExprs) {
            if (expr instanceof AliasExpression) {
                AliasExpression aliasExpr = (AliasExpression) expr;
                // Include alias: SUM(sales) AS total
                aggStrings.add(aliasExpr.expression().toSQL() + " AS " +
                    SQLQuoting.quoteIdentifier(aliasExpr.alias()));
            } else {
                aggStrings.add(expr.toSQL());
            }
        }
        return String.join(", ", aggStrings);
    }

    /**
     * Formats GROUP BY columns for PIVOT clause.
     */
    private String formatGroupByColumns(List<Expression> groupingExprs) {
        if (groupingExprs.isEmpty()) {
            return "";
        }
        return groupingExprs.stream()
            .map(Expression::toSQL)
            .collect(Collectors.joining(", "));
    }

    /**
     * Creates an empty pivot result when no pivot values are found.
     */
    private LogicalPlan createEmptyPivotResult(
            List<Expression> groupingExprs,
            List<Expression> aggregateExprs,
            String pivotColName,
            List<Object> pivotValues) {
        // Return an empty SELECT that will yield no rows
        String sql = "SELECT * FROM (SELECT 1) WHERE FALSE";
        return new SQLRelation(sql);
    }

    /**
     * Infers the output schema for a pivot operation.
     *
     * <p>Output schema consists of:
     * <ul>
     *   <li>Grouping columns (if any)</li>
     *   <li>Pivoted columns: {pivot_value} for single aggregate, {pivot_value}_{agg} for multiple</li>
     * </ul>
     */
    private StructType inferPivotSchema(
            LogicalPlan input,
            List<Expression> groupingExprs,
            List<Expression> aggregateExprs,
            String pivotColName,
            List<Object> pivotValues) {

        List<StructField> fields = new ArrayList<>();

        // Add grouping columns
        for (Expression expr : groupingExprs) {
            String colName = extractExpressionColumnName(expr);
            // Default to StringType for simplicity; schema inference will correct this
            fields.add(new StructField(colName, com.thunderduck.types.StringType.get(), true));
        }

        // Add pivoted columns
        // For single aggregate: column name is just the pivot value
        // For multiple aggregates: column name is {value}_{aggregate_alias}
        boolean multipleAggregates = aggregateExprs.size() > 1;

        for (Object value : pivotValues) {
            String valueStr = formatPivotValueAsColumnName(value);

            for (Expression aggExpr : aggregateExprs) {
                String colName;
                if (multipleAggregates) {
                    String aggName = extractAggregateAlias(aggExpr);
                    colName = valueStr + "_" + aggName;
                } else {
                    colName = valueStr;
                }
                // Use the aggregate return type; default to DoubleType for numeric aggregates
                fields.add(new StructField(colName, com.thunderduck.types.DoubleType.get(), true));
            }
        }

        return new StructType(fields);
    }

    /**
     * Extracts column name from an expression.
     */
    private String extractExpressionColumnName(Expression expr) {
        if (expr instanceof UnresolvedColumn) {
            return ((UnresolvedColumn) expr).columnName();
        }
        if (expr instanceof AliasExpression) {
            return ((AliasExpression) expr).alias();
        }
        return expr.toSQL();
    }

    /**
     * Extracts the alias or function name from an aggregate expression.
     */
    private String extractAggregateAlias(Expression expr) {
        if (expr instanceof AliasExpression) {
            return ((AliasExpression) expr).alias();
        }
        if (expr instanceof FunctionCall) {
            return ((FunctionCall) expr).functionName().toLowerCase();
        }
        return "value";
    }

    /**
     * Formats a pivot value as a valid column name.
     */
    private String formatPivotValueAsColumnName(Object value) {
        if (value == null) {
            return "null";
        }
        String str = value.toString();
        // DuckDB/Spark use the string representation of the value as the column name
        // For most types this is straightforward
        return str;
    }

    /**
     * Converts a Sort relation.
     *
     * @param sort the Sort relation
     * @return a Sort logical plan
     */
    private LogicalPlan convertSort(org.apache.spark.connect.proto.Sort sort) {
        LogicalPlan input = convert(sort.getInput());

        // Convert sort orders
        List<com.thunderduck.logical.Sort.SortOrder> sortOrders = sort.getOrderList().stream()
                .map(this::convertSortOrder)
                .toList();

        logger.debug("Creating Sort with {} sort orders", sortOrders.size());
        return new com.thunderduck.logical.Sort(input, sortOrders);
    }

    /**
     * Converts a Limit relation.
     *
     * @param limit the Limit relation
     * @return a Limit logical plan
     */
    private LogicalPlan convertLimit(org.apache.spark.connect.proto.Limit limit) {
        LogicalPlan input = convert(limit.getInput());
        int limitValue = limit.getLimit();

        logger.debug("Creating Limit with value: {}", limitValue);
        return new com.thunderduck.logical.Limit(input, limitValue);
    }

    /**
     * Converts a Tail relation.
     *
     * <p>Tail returns the last N rows from a DataFrame, preserving
     * the original row order.
     *
     * @param tail the Tail relation
     * @return a Tail logical plan
     */
    private LogicalPlan convertTail(org.apache.spark.connect.proto.Tail tail) {
        LogicalPlan input = convert(tail.getInput());
        int limitValue = tail.getLimit();

        logger.debug("Creating Tail with value: {}", limitValue);
        return new com.thunderduck.logical.Tail(input, limitValue);
    }

    /**
     * Converts a Join relation.
     *
     * <p>Supports two patterns:
     * <ul>
     *   <li>Explicit condition: {@code df1.join(df2, df1["id"] == df2["id"])}</li>
     *   <li>USING columns: {@code df1.join(df2, "id")} or {@code df1.join(df2, ["id", "key"])}</li>
     * </ul>
     *
     * @param join the Join relation
     * @return a Join logical plan
     */
    private LogicalPlan convertJoin(org.apache.spark.connect.proto.Join join) {
        LogicalPlan left = convert(join.getLeft());
        LogicalPlan right = convert(join.getRight());

        Expression condition = null;
        List<String> usingColumnNames = java.util.Collections.emptyList();

        // Pattern 1: Explicit join condition
        if (join.hasJoinCondition()) {
            condition = expressionConverter.convert(join.getJoinCondition());
        }
        // Pattern 2: USING columns - build equality condition from column names
        else if (join.getUsingColumnsCount() > 0) {
            usingColumnNames = join.getUsingColumnsList();
            condition = buildUsingCondition(usingColumnNames, left, right);
        }

        // Map join type
        com.thunderduck.logical.Join.JoinType joinType = mapJoinType(join.getJoinType());

        logger.debug("Creating Join of type: {} with {} using columns",
                     joinType, join.getUsingColumnsCount());
        // Pass usingColumnNames for USING join column deduplication
        return new com.thunderduck.logical.Join(left, right, joinType, condition, usingColumnNames);
    }

    /**
     * Builds a join condition from USING columns.
     *
     * <p>For USING (col1, col2), generates:
     * {@code left.col1 = right.col1 AND left.col2 = right.col2}
     *
     * @param usingColumns list of column names to join on
     * @param left the left relation (for plan_id)
     * @param right the right relation (for plan_id)
     * @return the combined equality condition
     */
    private Expression buildUsingCondition(List<String> usingColumns,
                                           LogicalPlan left, LogicalPlan right) {
        if (usingColumns.isEmpty()) {
            throw new PlanConversionException("USING clause requires at least one column");
        }

        // Get plan_ids for column qualification
        OptionalLong leftPlanId = left.planId();
        OptionalLong rightPlanId = right.planId();

        Expression combined = null;

        for (String columnName : usingColumns) {
            // Create qualified column references with plan_ids
            UnresolvedColumn leftCol = new UnresolvedColumn(columnName, null, leftPlanId);
            UnresolvedColumn rightCol = new UnresolvedColumn(columnName, null, rightPlanId);

            // Build equality: left.col = right.col
            Expression equality = BinaryExpression.equal(leftCol, rightCol);

            // Combine with AND if multiple columns
            if (combined == null) {
                combined = equality;
            } else {
                combined = BinaryExpression.and(combined, equality);
            }
        }

        logger.debug("Built USING condition for columns: {}", usingColumns);
        return combined;
    }

    /**
     * Converts a Deduplicate relation (distinct operation).
     *
     * @param deduplicate the Deduplicate relation
     * @return a LogicalPlan that removes duplicates
     */
    private LogicalPlan convertDeduplicate(Deduplicate deduplicate) {
        LogicalPlan input = convert(deduplicate.getInput());

        // If columns are specified, deduplicate based on those columns
        // Otherwise, deduplicate based on all columns
        List<String> columns = deduplicate.getColumnNamesList();

        logger.debug("Creating Deduplicate with {} specified columns", columns.size());

        // For now, we'll create a Distinct logical plan
        // In SQL, this becomes SELECT DISTINCT ...
        return new Distinct(input, columns.isEmpty() ? null : columns);
    }

    /**
     * Converts a SetOperation (Union, Intersect, Except).
     *
     * @param setOp the SetOperation
     * @return a LogicalPlan for the set operation
     */
    private LogicalPlan convertSetOp(SetOperation setOp) {
        LogicalPlan left = convert(setOp.getLeftInput());
        LogicalPlan right = convert(setOp.getRightInput());

        // Note: In Spark Connect protocol, isAll=true means keep duplicates (ALL variant)
        // Union: isAll=true → UNION ALL, isAll=false → UNION (distinct)
        // Intersect: isAll=true → INTERSECT ALL, isAll=false → INTERSECT (distinct)
        // Except: isAll=true → EXCEPT ALL, isAll=false → EXCEPT (distinct)
        boolean keepDuplicates = setOp.getIsAll();

        // Union-specific options for unionByName
        boolean byName = setOp.getByName();
        boolean allowMissingColumns = setOp.getAllowMissingColumns();

        switch (setOp.getSetOpType()) {
            case SET_OP_TYPE_UNION:
                return new Union(left, right, keepDuplicates, byName, allowMissingColumns);
            case SET_OP_TYPE_INTERSECT:
                return new Intersect(left, right, !keepDuplicates);  // Intersect uses 'distinct' flag (inverted)
            case SET_OP_TYPE_EXCEPT:
                return new Except(left, right, !keepDuplicates);  // Except uses 'distinct' flag (inverted)
            default:
                throw new PlanConversionException("Unsupported set operation: " + setOp.getSetOpType());
        }
    }

    /**
     * Converts a Spark Connect SortOrder to thunderduck SortOrder.
     */
    private com.thunderduck.logical.Sort.SortOrder convertSortOrder(org.apache.spark.connect.proto.Expression.SortOrder sortOrder) {
        Expression expr = expressionConverter.convert(sortOrder.getChild());

        // Map direction
        com.thunderduck.logical.Sort.SortDirection direction =
            sortOrder.getDirectionValue() == org.apache.spark.connect.proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING_VALUE
            ? com.thunderduck.logical.Sort.SortDirection.DESCENDING
            : com.thunderduck.logical.Sort.SortDirection.ASCENDING;

        // Map null ordering
        com.thunderduck.logical.Sort.NullOrdering nullOrdering =
            sortOrder.getNullOrdering() == org.apache.spark.connect.proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
            ? com.thunderduck.logical.Sort.NullOrdering.NULLS_FIRST
            : com.thunderduck.logical.Sort.NullOrdering.NULLS_LAST;

        return new com.thunderduck.logical.Sort.SortOrder(expr, direction, nullOrdering);
    }

    /**
     * Maps Spark Connect join types to thunderduck join types.
     */
    private com.thunderduck.logical.Join.JoinType mapJoinType(org.apache.spark.connect.proto.Join.JoinType sparkJoinType) {
        switch (sparkJoinType) {
            case JOIN_TYPE_INNER:
                return com.thunderduck.logical.Join.JoinType.INNER;
            case JOIN_TYPE_LEFT_OUTER:
                return com.thunderduck.logical.Join.JoinType.LEFT;
            case JOIN_TYPE_RIGHT_OUTER:
                return com.thunderduck.logical.Join.JoinType.RIGHT;
            case JOIN_TYPE_FULL_OUTER:
                return com.thunderduck.logical.Join.JoinType.FULL;
            case JOIN_TYPE_LEFT_SEMI:
                return com.thunderduck.logical.Join.JoinType.LEFT_SEMI;
            case JOIN_TYPE_LEFT_ANTI:
                return com.thunderduck.logical.Join.JoinType.LEFT_ANTI;
            case JOIN_TYPE_CROSS:
                return com.thunderduck.logical.Join.JoinType.CROSS;
            default:
                throw new PlanConversionException("Unsupported join type: " + sparkJoinType);
        }
    }

    /**
     * Converts a LocalRelation to a LogicalPlan.
     *
     * <p>LocalRelation represents data sent from Spark in Arrow IPC format.
     * This is used for pre-computed results (like count()), small DataFrames,
     * or cached local data.
     *
     * @param localRelation the LocalRelation protobuf message
     * @return a LocalDataRelation logical plan
     */
    private LogicalPlan convertLocalRelation(org.apache.spark.connect.proto.LocalRelation localRelation) {
        byte[] arrowData = null;
        String schema = null;

        // Extract Arrow IPC data if present
        if (localRelation.hasData()) {
            arrowData = localRelation.getData().toByteArray();
            logger.debug("LocalRelation has {} bytes of Arrow IPC data", arrowData.length);
        }

        // Extract schema if present
        if (localRelation.hasSchema()) {
            schema = localRelation.getSchema();
            logger.debug("LocalRelation has schema: {}", schema);
        }

        // Validate that we have at least data or schema
        if ((arrowData == null || arrowData.length == 0) && (schema == null || schema.isEmpty())) {
            throw new PlanConversionException("LocalRelation must have either data or schema");
        }

        logger.debug("Creating LocalDataRelation");
        return new LocalDataRelation(arrowData, schema);
    }

    /**
     * Converts a Range relation to a RangeRelation.
     *
     * <p>Range generates a DataFrame with a single column "id" containing
     * sequential BIGINT values from start (inclusive) to end (exclusive).
     *
     * @param range the Range protobuf message
     * @return a RangeRelation logical plan
     */
    private LogicalPlan convertRange(org.apache.spark.connect.proto.Range range) {
        // Start defaults to 0 if not specified
        long start = range.hasStart() ? range.getStart() : 0;
        long end = range.getEnd();
        long step = range.getStep();

        // Validate step is not zero (protobuf may allow it)
        if (step == 0) {
            throw new PlanConversionException("Range step cannot be zero");
        }

        logger.debug("Creating RangeRelation(start={}, end={}, step={})", start, end, step);
        return new RangeRelation(start, end, step);
    }

    /**
     * Converts a Drop relation to a LogicalPlan.
     *
     * <p>Drop removes specified columns from the DataFrame. Uses DuckDB's
     * EXCLUDE syntax: SELECT * EXCLUDE (col1, col2) FROM ...
     *
     * @param drop the Drop protobuf message
     * @return a LogicalPlan that excludes the specified columns
     */
    private LogicalPlan convertDrop(org.apache.spark.connect.proto.Drop drop) {
        LogicalPlan input = convert(drop.getInput());

        // Collect column names to drop
        List<String> columnsToDrop = new ArrayList<>();

        // From column_names field (string list)
        columnsToDrop.addAll(drop.getColumnNamesList());

        // From columns field (Expression list - extract column names)
        for (org.apache.spark.connect.proto.Expression expr : drop.getColumnsList()) {
            if (expr.hasUnresolvedAttribute()) {
                String colName = expr.getUnresolvedAttribute().getUnparsedIdentifier();
                columnsToDrop.add(colName);
            } else {
                logger.warn("Drop: ignoring non-column expression: {}", expr);
            }
        }

        if (columnsToDrop.isEmpty()) {
            logger.debug("Drop: no columns to drop, returning input as-is");
            return input;
        }

        // Generate SQL using DuckDB's EXCLUDE syntax
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        StringBuilder excludeList = new StringBuilder();
        for (int i = 0; i < columnsToDrop.size(); i++) {
            if (i > 0) {
                excludeList.append(", ");
            }
            excludeList.append(SQLQuoting.quoteIdentifier(columnsToDrop.get(i)));
        }

        String sql = String.format("SELECT * EXCLUDE (%s) FROM (%s) AS _drop_subquery",
            excludeList.toString(), inputSql);

        // Build output schema by removing dropped columns from input schema
        StructType inputSchema = input.schema();
        StructType outputSchema = null;
        if (inputSchema != null && !inputSchema.fields().isEmpty()) {
            Set<String> droppedCols = new HashSet<>(columnsToDrop);
            List<StructField> outputFields = new ArrayList<>();
            for (StructField field : inputSchema.fields()) {
                if (!droppedCols.contains(field.name())) {
                    outputFields.add(field);
                }
            }
            outputSchema = new StructType(outputFields);
        } else if (schemaInferrer != null) {
            // Fallback: infer schema from DuckDB
            StructType fullSchema = schemaInferrer.inferSchema(inputSql);
            if (fullSchema != null) {
                Set<String> droppedCols = new HashSet<>(columnsToDrop);
                List<StructField> outputFields = new ArrayList<>();
                for (StructField field : fullSchema.fields()) {
                    if (!droppedCols.contains(field.name())) {
                        outputFields.add(field);
                    }
                }
                outputSchema = new StructType(outputFields);
            }
        }

        logger.debug("Creating Drop SQL: {}", sql);
        return new SQLRelation(sql, outputSchema);
    }

    /**
     * Converts a WithColumnsRenamed relation to a LogicalPlan.
     *
     * <p>Renames columns according to the provided mapping. Columns not in
     * the mapping are passed through unchanged.
     *
     * @param withColumnsRenamed the WithColumnsRenamed protobuf message
     * @return a LogicalPlan with renamed columns
     */
    @SuppressWarnings("deprecation") // getRenameColumnsMapMap() deprecated in Spark 4.0+, kept for backward compatibility
    private LogicalPlan convertWithColumnsRenamed(org.apache.spark.connect.proto.WithColumnsRenamed withColumnsRenamed) {
        LogicalPlan input = convert(withColumnsRenamed.getInput());

        // Build rename map from both the deprecated map and the new renames list
        // PySpark 4.0+ uses the renames list, older versions use rename_columns_map
        Map<String, String> renameMap = new java.util.LinkedHashMap<>();

        // Add from deprecated map (for backward compatibility with older clients)
        renameMap.putAll(withColumnsRenamed.getRenameColumnsMapMap());

        // Add from new renames list (PySpark 4.0+)
        for (org.apache.spark.connect.proto.WithColumnsRenamed.Rename rename : withColumnsRenamed.getRenamesList()) {
            renameMap.put(rename.getColName(), rename.getNewColName());
        }

        if (renameMap.isEmpty()) {
            logger.debug("WithColumnsRenamed: no columns to rename, returning input as-is");
            return input;
        }

        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // Compute output schema first so we can generate SQL preserving column order
        StructType inputSchema = input.schema();
        String sql;

        if (inputSchema != null && !inputSchema.fields().isEmpty()) {
            // Generate explicit SELECT list preserving column order
            StringBuilder selectList = new StringBuilder();
            for (int i = 0; i < inputSchema.fields().size(); i++) {
                if (i > 0) selectList.append(", ");
                StructField field = inputSchema.fieldAt(i);

                if (renameMap.containsKey(field.name())) {
                    // Rename: output old_col AS new_col at this position
                    String newName = renameMap.get(field.name());
                    selectList.append(SQLQuoting.quoteIdentifier(field.name()))
                              .append(" AS ")
                              .append(SQLQuoting.quoteIdentifier(newName));
                } else {
                    // Keep original column name
                    selectList.append(SQLQuoting.quoteIdentifier(field.name()));
                }
            }
            sql = String.format("SELECT %s FROM (%s) AS _rename_subquery",
                selectList.toString(), inputSql);
        } else {
            // Fallback: use EXCLUDE approach when schema unavailable (renamed columns at end)
            StringBuilder excludeList = new StringBuilder();
            StringBuilder selectAdditions = new StringBuilder();
            int i = 0;
            for (Map.Entry<String, String> entry : renameMap.entrySet()) {
                String oldName = entry.getKey();
                String newName = entry.getValue();
                if (i > 0) {
                    excludeList.append(", ");
                    selectAdditions.append(", ");
                }
                excludeList.append(SQLQuoting.quoteIdentifier(oldName));
                selectAdditions.append(SQLQuoting.quoteIdentifier(oldName));
                selectAdditions.append(" AS ");
                selectAdditions.append(SQLQuoting.quoteIdentifier(newName));
                i++;
            }
            sql = String.format("SELECT * EXCLUDE (%s), %s FROM (%s) AS _rename_subquery",
                excludeList.toString(), selectAdditions.toString(), inputSql);
        }

        logger.debug("Creating WithColumnsRenamed SQL: {}", sql);

        // Compute output schema by applying renames to input schema
        StructType outputSchema = null;
        if (inputSchema != null && !inputSchema.fields().isEmpty()) {
            List<StructField> outputFields = new ArrayList<>();
            for (StructField field : inputSchema.fields()) {
                String newName = renameMap.getOrDefault(field.name(), field.name());
                outputFields.add(new StructField(newName, field.dataType(), field.nullable()));
            }
            outputSchema = new StructType(outputFields);
        }

        return new SQLRelation(sql, outputSchema);
    }

    /**
     * Converts a WithColumns relation to a LogicalPlan.
     *
     * <p>Adds new columns or replaces existing columns with the given expressions.
     * Each alias in the list provides both the column name and the expression.
     *
     * <p>Uses schema introspection to determine whether each column already exists:
     * <ul>
     *   <li>For existing columns: uses DuckDB's * REPLACE (expr AS col) syntax
     *   <li>For new columns: appends expr AS col after the SELECT *
     * </ul>
     *
     * @param withColumns the WithColumns protobuf message
     * @return a LogicalPlan with the new/replaced columns
     */
    private LogicalPlan convertWithColumns(org.apache.spark.connect.proto.WithColumns withColumns) {
        LogicalPlan input = convert(withColumns.getInput());

        List<org.apache.spark.connect.proto.Expression.Alias> aliases = withColumns.getAliasesList();

        if (aliases.isEmpty()) {
            logger.debug("WithColumns: no columns to add/replace, returning input as-is");
            return input;
        }

        // Convert all column names and expressions
        List<String> colNames = new ArrayList<>();
        List<Expression> colExprs = new ArrayList<>();

        for (org.apache.spark.connect.proto.Expression.Alias alias : aliases) {
            // Convert the expression
            Expression expr = expressionConverter.convert(alias.getExpr());

            // Get the column name (first name part)
            String colName = alias.getName(0);
            colNames.add(colName);
            colExprs.add(expr);

            logger.debug("WithColumns: adding/replacing column '{}'", colName);
        }

        // Return WithColumns plan that properly infers schema
        com.thunderduck.logical.WithColumns plan =
            new com.thunderduck.logical.WithColumns(input, colNames, colExprs);
        logger.debug("Creating WithColumns plan with {} columns", colNames.size());
        return plan;
    }

    /**
     * Converts an Offset relation to a LogicalPlan.
     *
     * <p>Offset skips the first N rows from the input. Uses the existing Limit
     * class which already supports offset natively.
     *
     * @param offset the Offset protobuf message
     * @return a Limit logical plan with offset
     */
    private LogicalPlan convertOffset(org.apache.spark.connect.proto.Offset offset) {
        LogicalPlan input = convert(offset.getInput());
        int offsetValue = offset.getOffset();

        logger.debug("Creating Offset with value: {}", offsetValue);
        // Use Limit with Long.MAX_VALUE to represent "all remaining rows after offset"
        return new com.thunderduck.logical.Limit(input, Long.MAX_VALUE, offsetValue);
    }

    /**
     * Converts a ToDF relation to a LogicalPlan.
     *
     * <p>ToDF renames all columns in the DataFrame to the provided names.
     * The number of names must match the number of columns.
     *
     * <p>Uses DuckDB's positional column aliasing syntax.
     *
     * @param toDF the ToDF protobuf message
     * @return a LogicalPlan with renamed columns
     */
    private LogicalPlan convertToDF(org.apache.spark.connect.proto.ToDF toDF) {
        LogicalPlan input = convert(toDF.getInput());
        List<String> newNames = toDF.getColumnNamesList();

        if (newNames.isEmpty()) {
            logger.debug("ToDF: no column names provided, returning input as-is");
            return input;
        }

        logger.debug("ToDF: renaming columns to: {}", newNames);
        return new com.thunderduck.logical.ToDF(input, newNames);
    }

    /**
     * Converts a Sample relation to a LogicalPlan.
     *
     * <p>Sample returns a random subset of rows from the input using Bernoulli
     * sampling to match Spark's per-row probability algorithm.
     *
     * <p>The Spark Connect protocol uses lower_bound/upper_bound to specify the
     * sampling fraction. Typically lower_bound=0.0 and upper_bound is the fraction.
     *
     * <p>IMPORTANT: withReplacement=true is NOT supported. DuckDB does not have
     * an equivalent to Spark's Poisson sampling, so we fail fast with an error.
     *
     * @param sample the Sample protobuf message
     * @return a Sample logical plan
     * @throws PlanConversionException if withReplacement=true is requested
     */
    private LogicalPlan convertSample(org.apache.spark.connect.proto.Sample sample) {
        LogicalPlan input = convert(sample.getInput());

        // Check for unsupported withReplacement=true
        if (sample.hasWithReplacement() && sample.getWithReplacement()) {
            throw new PlanConversionException(
                "sample() with withReplacement=true is not supported. " +
                "DuckDB does not support sampling with replacement.");
        }

        // The fraction is specified via upper_bound (lower_bound is typically 0.0)
        double fraction = sample.getUpperBound();

        // Check for seed
        java.util.OptionalLong seed;
        if (sample.hasSeed()) {
            seed = java.util.OptionalLong.of(sample.getSeed());
            logger.debug("Creating Sample with fraction: {}, seed: {}", fraction, sample.getSeed());
        } else {
            seed = java.util.OptionalLong.empty();
            logger.debug("Creating Sample with fraction: {} (no seed)", fraction);
        }

        return new com.thunderduck.logical.Sample(input, fraction, seed);
    }

    /**
     * Converts an NADrop relation to a LogicalPlan.
     *
     * <p>NADrop removes rows containing null values based on the specified columns
     * and minimum non-null count threshold.
     *
     * <p>Behavior:
     * <ul>
     *   <li>If cols is empty, considers all columns (requires schema inference)</li>
     *   <li>If min_non_nulls is not set, all specified columns must be non-null (how='any')</li>
     *   <li>If min_non_nulls=1, at least one column must be non-null (how='all')</li>
     *   <li>Otherwise, at least min_non_nulls columns must be non-null</li>
     * </ul>
     *
     * @param naDrop the NADrop protobuf message
     * @return a Filter logical plan that drops rows with nulls
     */
    private LogicalPlan convertNADrop(NADrop naDrop) {
        LogicalPlan input = convert(naDrop.getInput());

        // Get columns to consider
        List<String> cols = naDrop.getColsList();

        // If cols is empty, we need to get all columns via schema inference
        if (cols.isEmpty()) {
            if (schemaInferrer == null) {
                throw new PlanConversionException(
                    "NADrop with empty cols requires schema inference, but no connection available");
            }

            // Generate SQL for input to infer schema
            SQLGenerator generator = new SQLGenerator();
            String inputSql = generator.generate(input);

            StructType schema = schemaInferrer.inferSchema(inputSql);
            cols = new ArrayList<>();
            for (StructField field : schema.fields()) {
                cols.add(field.name());
            }
            logger.debug("NADrop: inferred {} columns from schema", cols.size());
        }

        if (cols.isEmpty()) {
            // No columns to check, return input unchanged
            logger.debug("NADrop: no columns to check, returning input as-is");
            return input;
        }

        // Determine min_non_nulls threshold
        // If not set, default is the column count (all must be non-null, i.e., how='any')
        // If set to 1, at least one must be non-null (how='all')
        int minNonNulls = naDrop.hasMinNonNulls() ? naDrop.getMinNonNulls() : cols.size();

        logger.debug("NADrop: cols={}, minNonNulls={}", cols, minNonNulls);

        // Generate the filter SQL
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        String filterCondition;
        if (minNonNulls >= cols.size()) {
            // All columns must be non-null: col1 IS NOT NULL AND col2 IS NOT NULL ...
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append(SQLQuoting.quoteIdentifier(cols.get(i)));
                sb.append(" IS NOT NULL");
            }
            filterCondition = sb.toString();
        } else if (minNonNulls == 1) {
            // At least one column must be non-null: col1 IS NOT NULL OR col2 IS NOT NULL ...
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append(SQLQuoting.quoteIdentifier(cols.get(i)));
                sb.append(" IS NOT NULL");
            }
            filterCondition = sb.toString();
        } else {
            // At least minNonNulls columns must be non-null
            // Use: (CASE WHEN col1 IS NOT NULL THEN 1 ELSE 0 END + ...) >= minNonNulls
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) {
                    sb.append(" + ");
                }
                sb.append("CASE WHEN ");
                sb.append(SQLQuoting.quoteIdentifier(cols.get(i)));
                sb.append(" IS NOT NULL THEN 1 ELSE 0 END");
            }
            sb.append(") >= ");
            sb.append(minNonNulls);
            filterCondition = sb.toString();
        }

        String sql = String.format("SELECT * FROM (%s) AS _nadrop_subquery WHERE %s",
            inputSql, filterCondition);

        logger.debug("Creating NADrop SQL: {}", sql);
        return new SQLRelation(sql);
    }

    /**
     * Converts an NAFill relation to a LogicalPlan.
     *
     * <p>NAFill replaces null values with specified fill values using COALESCE.
     *
     * <p>Behavior:
     * <ul>
     *   <li>If cols is empty, applies to all columns (requires schema inference)</li>
     *   <li>If values has one value, uses it for all specified columns</li>
     *   <li>If values has multiple values, each col[i] gets values[i]</li>
     * </ul>
     *
     * @param naFill the NAFill protobuf message
     * @return a Project logical plan with COALESCE for null replacement
     */
    private LogicalPlan convertNAFill(NAFill naFill) {
        LogicalPlan input = convert(naFill.getInput());

        // Get columns and values
        List<String> cols = naFill.getColsList();
        List<org.apache.spark.connect.proto.Expression.Literal> values = naFill.getValuesList();

        if (values.isEmpty()) {
            throw new PlanConversionException("NAFill requires at least one fill value");
        }

        // If cols is empty, we need to get all columns via schema inference
        if (cols.isEmpty()) {
            if (schemaInferrer == null) {
                throw new PlanConversionException(
                    "NAFill with empty cols requires schema inference, but no connection available");
            }

            SQLGenerator generator = new SQLGenerator();
            String inputSql = generator.generate(input);

            StructType schema = schemaInferrer.inferSchema(inputSql);
            cols = new ArrayList<>();
            for (StructField field : schema.fields()) {
                cols.add(field.name());
            }
            logger.debug("NAFill: inferred {} columns from schema", cols.size());
        }

        if (cols.isEmpty()) {
            logger.debug("NAFill: no columns to fill, returning input as-is");
            return input;
        }

        // Convert fill values to SQL literals
        List<String> fillValuesSql = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression.Literal literal : values) {
            com.thunderduck.expression.Expression expr = expressionConverter.convert(
                org.apache.spark.connect.proto.Expression.newBuilder()
                    .setLiteral(literal)
                    .build());
            if (expr instanceof com.thunderduck.expression.Literal) {
                fillValuesSql.add(((com.thunderduck.expression.Literal) expr).toSQL());
            } else {
                throw new PlanConversionException("Expected Literal expression for NAFill value");
            }
        }

        logger.debug("NAFill: cols={}, values={}", cols, fillValuesSql);

        // Generate SQL with COALESCE for fill columns
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // We need schema to know all columns
        // For columns being filled: COALESCE(col, value) AS col
        // For other columns: col
        StructType schema;
        if (schemaInferrer != null) {
            schema = schemaInferrer.inferSchema(inputSql);
        } else {
            throw new PlanConversionException(
                "NAFill requires schema inference, but no connection available");
        }

        // Track which columns actually get COALESCE applied (for nullable inference)
        Set<String> filledCols = new HashSet<>();

        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < schema.fields().size(); i++) {
            if (i > 0) {
                selectList.append(", ");
            }
            String colName = schema.fieldAt(i).name();
            int colIndex = cols.indexOf(colName);

            if (colIndex >= 0) {
                // This column should be filled
                String fillValue;
                if (values.size() == 1) {
                    // Single value for all columns
                    fillValue = fillValuesSql.get(0);
                    filledCols.add(colName);
                } else if (colIndex < fillValuesSql.size()) {
                    // Multiple values matched by position
                    fillValue = fillValuesSql.get(colIndex);
                    filledCols.add(colName);
                } else {
                    // Not enough values, don't fill this column
                    selectList.append(SQLQuoting.quoteIdentifier(colName));
                    continue;
                }
                selectList.append("COALESCE(");
                selectList.append(SQLQuoting.quoteIdentifier(colName));
                selectList.append(", ");
                selectList.append(fillValue);
                selectList.append(") AS ");
                selectList.append(SQLQuoting.quoteIdentifier(colName));
            } else {
                // Column not in fill list, pass through
                selectList.append(SQLQuoting.quoteIdentifier(colName));
            }
        }

        String sql = String.format("SELECT %s FROM (%s) AS _nafill_subquery",
            selectList.toString(), inputSql);

        // Build output schema with filled columns marked as non-nullable
        // COALESCE(col, literal) is guaranteed to never return null
        List<StructField> outputFields = new ArrayList<>();
        for (StructField field : schema.fields()) {
            if (filledCols.contains(field.name())) {
                // Filled column is now non-nullable
                outputFields.add(new StructField(field.name(), field.dataType(), false));
            } else {
                // Keep original nullability
                outputFields.add(field);
            }
        }
        StructType outputSchema = new StructType(outputFields);

        logger.debug("Creating NAFill SQL: {}", sql);
        return new SQLRelation(sql, outputSchema);
    }

    /**
     * Converts an NAReplace relation to a LogicalPlan.
     *
     * <p>NAReplace replaces specific values with new values using CASE WHEN expressions.
     *
     * <p>Behavior:
     * <ul>
     *   <li>If cols is empty, applies to all type-compatible columns (requires schema inference)</li>
     *   <li>Each replacement specifies an old_value and new_value pair</li>
     *   <li>Generates CASE WHEN col = old_value THEN new_value ... ELSE col END</li>
     * </ul>
     *
     * @param naReplace the NAReplace protobuf message
     * @return a Project logical plan with CASE WHEN for value replacement
     */
    private LogicalPlan convertNAReplace(NAReplace naReplace) {
        LogicalPlan input = convert(naReplace.getInput());

        // Get columns and replacements
        List<String> cols = naReplace.getColsList();
        List<NAReplace.Replacement> replacements = naReplace.getReplacementsList();

        if (replacements.isEmpty()) {
            logger.debug("NAReplace: no replacements, returning input as-is");
            return input;
        }

        // Generate SQL for input
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // We always need schema to enumerate columns
        if (schemaInferrer == null) {
            throw new PlanConversionException(
                "NAReplace requires schema inference, but no connection available");
        }

        StructType schema = schemaInferrer.inferSchema(inputSql);

        // If cols is empty, apply to all columns
        if (cols.isEmpty()) {
            cols = new ArrayList<>();
            for (StructField field : schema.fields()) {
                cols.add(field.name());
            }
            logger.debug("NAReplace: applying to all {} columns", cols.size());
        }

        // Convert replacement values to SQL
        List<String[]> replacementsSql = new ArrayList<>();
        for (NAReplace.Replacement repl : replacements) {
            String oldValueSql = convertLiteralToSQL(repl.getOldValue());
            String newValueSql = convertLiteralToSQL(repl.getNewValue());
            replacementsSql.add(new String[]{oldValueSql, newValueSql});
        }

        logger.debug("NAReplace: cols={}, replacements={}", cols, replacementsSql.size());

        // Build SELECT list with CASE WHEN for replaced columns
        StringBuilder selectList = new StringBuilder();
        for (int i = 0; i < schema.fields().size(); i++) {
            if (i > 0) {
                selectList.append(", ");
            }
            String colName = schema.fieldAt(i).name();
            String quotedCol = SQLQuoting.quoteIdentifier(colName);

            if (cols.contains(colName)) {
                // This column should have replacements applied
                // Build nested CASE WHEN
                selectList.append("CASE");
                for (String[] repl : replacementsSql) {
                    String oldVal = repl[0];
                    String newVal = repl[1];
                    if ("NULL".equals(oldVal)) {
                        selectList.append(" WHEN ");
                        selectList.append(quotedCol);
                        selectList.append(" IS NULL THEN ");
                        selectList.append(newVal);
                    } else {
                        selectList.append(" WHEN ");
                        selectList.append(quotedCol);
                        selectList.append(" = ");
                        selectList.append(oldVal);
                        selectList.append(" THEN ");
                        selectList.append(newVal);
                    }
                }
                selectList.append(" ELSE ");
                selectList.append(quotedCol);
                selectList.append(" END AS ");
                selectList.append(quotedCol);
            } else {
                // Column not in replace list, pass through
                selectList.append(quotedCol);
            }
        }

        String sql = String.format("SELECT %s FROM (%s) AS _nareplace_subquery",
            selectList.toString(), inputSql);

        logger.debug("Creating NAReplace SQL: {}", sql);
        return new SQLRelation(sql);
    }

    /**
     * Converts a proto Literal to its SQL string representation.
     *
     * @param literal the proto literal
     * @return the SQL string
     */
    private String convertLiteralToSQL(org.apache.spark.connect.proto.Expression.Literal literal) {
        com.thunderduck.expression.Expression expr = expressionConverter.convert(
            org.apache.spark.connect.proto.Expression.newBuilder()
                .setLiteral(literal)
                .build());
        if (expr instanceof com.thunderduck.expression.Literal) {
            return ((com.thunderduck.expression.Literal) expr).toSQL();
        }
        throw new PlanConversionException("Expected Literal expression");
    }

    /**
     * Converts an Unpivot relation to a LogicalPlan.
     *
     * <p>Unpivot transforms a DataFrame from wide format to long format by rotating
     * value columns into rows. This is the inverse of pivot.
     *
     * <p>Example:
     * <pre>
     * Input:  id | sales_q1 | sales_q2
     *         1  | 100      | 200
     *
     * Output (unpivot sales_q1, sales_q2): id | quarter  | sales
     *                                      1  | sales_q1 | 100
     *                                      1  | sales_q2 | 200
     * </pre>
     *
     * <p>Uses DuckDB's native UNPIVOT syntax:
     * <pre>
     * SELECT * FROM (input) UNPIVOT (
     *     value_col FOR variable_col IN (col1, col2, ...)
     * )
     * </pre>
     *
     * @param unpivot the Unpivot protobuf message
     * @return a SQLRelation with DuckDB UNPIVOT syntax
     */
    private LogicalPlan convertUnpivot(Unpivot unpivot) {
        LogicalPlan input = convert(unpivot.getInput());

        // Get ID columns (columns to keep as identifiers)
        List<String> idCols = new ArrayList<>();
        for (org.apache.spark.connect.proto.Expression expr : unpivot.getIdsList()) {
            String colName = extractColumnName(expr);
            if (colName != null) {
                idCols.add(colName);
            }
        }

        // Get value columns (columns to unpivot)
        List<String> valueCols = new ArrayList<>();
        if (unpivot.hasValues()) {
            for (org.apache.spark.connect.proto.Expression expr : unpivot.getValues().getValuesList()) {
                String colName = extractColumnName(expr);
                if (colName != null) {
                    valueCols.add(colName);
                }
            }
        }

        // Get output column names
        String variableColumnName = unpivot.getVariableColumnName();
        String valueColumnName = unpivot.getValueColumnName();

        // Default column names if not specified
        if (variableColumnName == null || variableColumnName.isEmpty()) {
            variableColumnName = "variable";
        }
        if (valueColumnName == null || valueColumnName.isEmpty()) {
            valueColumnName = "value";
        }

        // Generate SQL for input
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // If valueCols is empty, we need to infer all non-ID columns
        if (valueCols.isEmpty()) {
            if (schemaInferrer == null) {
                throw new PlanConversionException(
                    "Unpivot with empty values requires schema inference, but no connection available");
            }

            StructType schema = schemaInferrer.inferSchema(inputSql);
            for (StructField field : schema.fields()) {
                String colName = field.name();
                // Include all columns except ID columns
                if (!idCols.contains(colName)) {
                    valueCols.add(colName);
                }
            }
            logger.debug("Unpivot: inferred {} value columns from schema", valueCols.size());
        }

        if (valueCols.isEmpty()) {
            throw new PlanConversionException("Unpivot requires at least one value column to unpivot");
        }

        // Build the UNPIVOT SQL
        // SELECT * FROM (input) UNPIVOT (value_col FOR variable_col IN (col1, col2, ...))
        StringBuilder valueColList = new StringBuilder();
        for (int i = 0; i < valueCols.size(); i++) {
            if (i > 0) {
                valueColList.append(", ");
            }
            valueColList.append(SQLQuoting.quoteIdentifier(valueCols.get(i)));
        }

        String sql = String.format(
            "SELECT * FROM (%s) UNPIVOT (%s FOR %s IN (%s))",
            inputSql,
            SQLQuoting.quoteIdentifier(valueColumnName),
            SQLQuoting.quoteIdentifier(variableColumnName),
            valueColList.toString()
        );

        // Build output schema for unpivot
        // Output columns are: ID columns + variable column + value column
        StructType outputSchema = null;
        if (schemaInferrer != null) {
            StructType inputSchema = schemaInferrer.inferSchema(inputSql);
            List<StructField> outputFields = new ArrayList<>();

            // 1. ID columns keep their original type/nullable from input
            for (String idCol : idCols) {
                for (StructField field : inputSchema.fields()) {
                    if (field.name().equals(idCol)) {
                        outputFields.add(field);
                        break;
                    }
                }
            }

            // 2. Variable column (contains column names as strings) - always non-null
            outputFields.add(new StructField(variableColumnName,
                com.thunderduck.types.StringType.get(), false));

            // 3. Value column - determine type from value columns, nullable if any was nullable
            com.thunderduck.types.DataType valueType = com.thunderduck.types.LongType.get(); // default
            boolean valueNullable = false;
            for (StructField field : inputSchema.fields()) {
                if (valueCols.contains(field.name())) {
                    valueType = field.dataType(); // Use first value column's type
                    if (field.nullable()) {
                        valueNullable = true;
                    }
                }
            }
            outputFields.add(new StructField(valueColumnName, valueType, valueNullable));

            outputSchema = new StructType(outputFields);
        }

        logger.debug("Creating Unpivot SQL: {}", sql);
        return new SQLRelation(sql, outputSchema);
    }

    /**
     * Extracts the column name from a Spark Connect Expression.
     *
     * <p>Supports UnresolvedAttribute expressions which contain column names.
     *
     * @param expr the expression
     * @return the column name, or null if not a column reference
     */
    private String extractColumnName(org.apache.spark.connect.proto.Expression expr) {
        if (expr.hasUnresolvedAttribute()) {
            return expr.getUnresolvedAttribute().getUnparsedIdentifier();
        }
        logger.warn("Unpivot: expected column reference but got: {}", expr.getExprTypeCase());
        return null;
    }

    /**
     * Converts a SubqueryAlias relation to a LogicalPlan.
     *
     * <p>SubqueryAlias provides a name for a relation, enabling:
     * <ul>
     *   <li>Column qualification: {@code df.alias("t").select("t.col1")}</li>
     *   <li>Self-joins: {@code df.alias("a").join(df.alias("b"), ...)}</li>
     *   <li>Disambiguation in complex queries</li>
     * </ul>
     *
     * <p>Implementation wraps the input SQL in a subquery with the specified alias:
     * <pre>
     * SELECT * FROM (input_sql) AS alias_name
     * </pre>
     *
     * <p><b>Limitation:</b> The optional {@code qualifier} field (for multi-catalog
     * scenarios) is not yet supported and is ignored.
     *
     * @param subqueryAlias the SubqueryAlias protobuf message
     * @return a SQLRelation with the aliased subquery
     * @throws PlanConversionException if the alias is null or empty
     */
    private LogicalPlan convertSubqueryAlias(SubqueryAlias subqueryAlias) {
        LogicalPlan input = convert(subqueryAlias.getInput());
        String alias = subqueryAlias.getAlias();

        // Validate alias
        if (alias == null || alias.isEmpty()) {
            throw new PlanConversionException("SubqueryAlias requires a non-empty alias");
        }

        // Log qualifier if present (not yet supported)
        if (subqueryAlias.getQualifierCount() > 0) {
            logger.warn("SubqueryAlias: qualifier field is not yet supported, ignoring: {}",
                subqueryAlias.getQualifierList());
        }

        // Return AliasedRelation to preserve alias for joins to use directly
        // This allows join conditions like col("d1.column") to work correctly
        // when the alias "d1" is referenced in the ON clause
        logger.debug("Creating AliasedRelation with alias '{}'", alias);
        return new AliasedRelation(input, alias);
    }

    /**
     * Converts a SQL relation (from spark.sql()) to a SQLRelation logical plan.
     *
     * <p>SQL relations contain raw SQL queries that will be passed through to DuckDB.
     * Parameter substitution is not handled here (it's done at the service layer).
     *
     * @param sql the SQL proto message
     * @return a SQLRelation logical plan
     */
    private LogicalPlan convertSQL(org.apache.spark.connect.proto.SQL sql) {
        String query = sql.getQuery();

        logger.debug("Converting SQL relation: {}", query);

        // Parameter substitution is handled at the service layer (SparkConnectServiceImpl)
        // before the query reaches this point, so we just wrap the query in SQLRelation
        return new com.thunderduck.logical.SQLRelation(query);
    }

    /**
     * Converts a SampleBy relation to a SQL relation with stratified sampling.
     *
     * @param sampleBy the SampleBy proto message
     * @return a LogicalPlan representing the sampled relation
     */
    private LogicalPlan convertSampleBy(StatSampleBy sampleBy) {
        LogicalPlan input = convert(sampleBy.getInput());

        java.util.List<StatSampleBy.Fraction> fractions = sampleBy.getFractionsList();
        if (fractions.isEmpty()) {
            // No fractions specified, return empty result
            SQLGenerator generator = new SQLGenerator();
            String inputSql = generator.generate(input);
            return new SQLRelation(inputSql + " WHERE FALSE");
        }

        // Get the column expression
        String colExpr = expressionConverter.convert(sampleBy.getCol()).toSQL();

        // Build WHERE clause for stratified sampling
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

        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        String sql = String.format(
            "SELECT * FROM (%s) AS _sample_input WHERE %s",
            inputSql, whereClause.toString()
        );

        logger.debug("Creating SampleBy SQL: {}", sql);
        return new SQLRelation(sql);
    }

    /**
     * Converts a ToSchema relation to a LogicalPlan.
     *
     * <p>ToSchema reconciles a DataFrame to match a specified schema by:
     * <ul>
     *   <li>Reordering columns by name to match target schema order</li>
     *   <li>Projecting away columns not in target schema</li>
     *   <li>Casting columns to match target data types</li>
     * </ul>
     *
     * <p>The generated SQL has the form:
     * <pre>SELECT CAST(col1 AS type1) AS col1, CAST(col2 AS type2) AS col2, ... FROM (input)</pre>
     *
     * @param toSchema the ToSchema protobuf message
     * @return a SQLRelation with the schema transformation
     * @throws PlanConversionException if the schema is not a StructType
     */
    private LogicalPlan convertToSchema(org.apache.spark.connect.proto.ToSchema toSchema) {
        LogicalPlan input = convert(toSchema.getInput());

        // Validate that we have a StructType schema
        if (!toSchema.hasSchema() || !toSchema.getSchema().hasStruct()) {
            throw new PlanConversionException("ToSchema requires a StructType schema");
        }

        org.apache.spark.connect.proto.DataType.Struct targetStruct =
            toSchema.getSchema().getStruct();

        // Build SELECT list with column order and casts
        List<String> selectItems = new ArrayList<>();
        for (org.apache.spark.connect.proto.DataType.StructField field :
             targetStruct.getFieldsList()) {
            String colName = field.getName();
            String quotedCol = SQLQuoting.quoteIdentifier(colName);

            // Generate CAST expression to ensure type matches target
            String duckdbType = SparkDataTypeConverter.toDuckDBType(field.getDataType());
            selectItems.add(String.format("CAST(%s AS %s) AS %s",
                quotedCol, duckdbType, quotedCol));
        }

        // Generate final SQL
        SQLGenerator generator = new SQLGenerator();
        String inputSQL = generator.generate(input);
        String sql = String.format("SELECT %s FROM (%s)",
            String.join(", ", selectItems),
            inputSQL);

        logger.debug("Creating ToSchema SQL: {}", sql);
        return new SQLRelation(sql);
    }

    /**
     * Infers the schema of a named table using DuckDB's DESCRIBE.
     *
     * @param tableName the table name
     * @return the inferred schema, or null if schema inference is unavailable
     */
    private StructType inferTableSchema(String tableName) {
        if (schemaInferrer == null) {
            logger.debug("Schema inferrer not available, returning null schema for table: {}", tableName);
            return null;
        }

        try {
            // Use DESCRIBE to get table schema
            StructType schema = schemaInferrer.inferSchema("SELECT * FROM " + SQLQuoting.quoteIdentifier(tableName));
            logger.debug("Inferred schema for table {}: {}", tableName, schema);
            return schema;
        } catch (Exception e) {
            logger.warn("Failed to infer schema for table {}: {}", tableName, e.getMessage());
            return null;
        }
    }

    /**
     * Infers the schema of a parquet file using DuckDB's DESCRIBE.
     *
     * @param path the parquet file path
     * @return the inferred schema, or null if schema inference is unavailable
     */
    private StructType inferParquetSchema(String path) {
        if (schemaInferrer == null) {
            logger.debug("Schema inferrer not available, returning null schema for parquet: {}", path);
            return null;
        }

        try {
            // Use read_parquet to get schema from parquet file
            String sql = "SELECT * FROM read_parquet('" + path.replace("'", "''") + "')";
            StructType schema = schemaInferrer.inferSchema(sql);
            logger.debug("Inferred schema for parquet {}: {}", path, schema);
            return schema;
        } catch (Exception e) {
            logger.warn("Failed to infer schema for parquet {}: {}", path, e.getMessage());
            return null;
        }
    }

    // ===================== Statistics Relation Converters =====================

    /**
     * Converts a StatDescribe relation to a SQLRelation.
     *
     * <p>Generates UNION ALL SQL for 5 statistics rows: count, mean, stddev, min, max.
     * Each row has a 'summary' column plus one column per input column.
     */
    private LogicalPlan convertDescribe(StatDescribe describe) {
        LogicalPlan inputPlan = convert(describe.getInput());
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(inputPlan);

        List<String> cols = new ArrayList<>(describe.getColsList());

        // If no columns specified, infer from schema
        if (cols.isEmpty()) {
            StructType inputSchema = inputPlan.schema();
            if (inputSchema != null) {
                for (StructField field : inputSchema.fields()) {
                    cols.add(field.name());
                }
            }
            // Fallback to DuckDB schema inference if plan schema is empty
            if (cols.isEmpty() && schemaInferrer != null) {
                StructType dbSchema = schemaInferrer.inferSchema(inputSql);
                if (dbSchema != null) {
                    for (StructField field : dbSchema.fields()) {
                        cols.add(field.name());
                    }
                }
            }
        }

        if (cols.isEmpty()) {
            return new SQLRelation("SELECT 'count' AS summary WHERE FALSE");
        }

        String[] stats = {"count", "mean", "stddev", "min", "max"};
        StringBuilder unionQuery = new StringBuilder();

        for (int s = 0; s < stats.length; s++) {
            if (s > 0) unionQuery.append(" UNION ALL ");
            unionQuery.append("SELECT '").append(stats[s]).append("' AS summary");

            for (String col : cols) {
                String quotedCol = SQLQuoting.quoteIdentifier(col);
                String aggExpr = getDescribeStatExpression(stats[s], quotedCol);
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return new SQLRelation(unionQuery.toString());
    }

    /**
     * Converts a StatSummary relation to a SQLRelation.
     *
     * <p>Generates UNION ALL SQL for configurable statistics including percentiles.
     */
    private LogicalPlan convertSummary(StatSummary summary) {
        LogicalPlan inputPlan = convert(summary.getInput());
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(inputPlan);

        List<String> statistics = new ArrayList<>(summary.getStatisticsList());
        if (statistics.isEmpty()) {
            statistics = Arrays.asList("count", "mean", "stddev", "min", "25%", "50%", "75%", "max");
        }

        // Get all columns from input schema
        List<String> cols = new ArrayList<>();
        StructType inputSchema = inputPlan.schema();
        if (inputSchema != null) {
            for (StructField field : inputSchema.fields()) {
                cols.add(field.name());
            }
        }
        // Fallback to DuckDB schema inference
        if (cols.isEmpty() && schemaInferrer != null) {
            StructType dbSchema = schemaInferrer.inferSchema(inputSql);
            if (dbSchema != null) {
                for (StructField field : dbSchema.fields()) {
                    cols.add(field.name());
                }
            }
        }

        if (cols.isEmpty()) {
            return new SQLRelation("SELECT 'count' AS summary WHERE FALSE");
        }

        StringBuilder unionQuery = new StringBuilder();

        for (int s = 0; s < statistics.size(); s++) {
            String stat = statistics.get(s);
            if (s > 0) unionQuery.append(" UNION ALL ");
            unionQuery.append("SELECT '").append(stat).append("' AS summary");

            for (String col : cols) {
                String quotedCol = SQLQuoting.quoteIdentifier(col);
                String aggExpr = getSummaryStatExpression(stat, quotedCol);
                unionQuery.append(", ").append(aggExpr).append(" AS ").append(quotedCol);
            }

            unionQuery.append(" FROM (").append(inputSql).append(") AS _stat_input");
        }

        return new SQLRelation(unionQuery.toString());
    }

    /**
     * Converts a StatCov relation to a SQLRelation.
     *
     * <p>Spark's stat.cov() replaces NULLs with 0.0 before computing covariance
     * (see StatFunctions.calculateCovImpl in Spark source).
     */
    private LogicalPlan convertCov(StatCov cov) {
        LogicalPlan inputPlan = convert(cov.getInput());
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(inputPlan);

        String col1 = SQLQuoting.quoteIdentifier(cov.getCol1());
        String col2 = SQLQuoting.quoteIdentifier(cov.getCol2());

        String sql = String.format(
            "SELECT COVAR_SAMP(COALESCE(%s, 0.0), COALESCE(%s, 0.0)) AS cov FROM (%s) AS _stat_input",
            col1, col2, inputSql
        );

        return new SQLRelation(sql);
    }

    /**
     * Converts a StatCorr relation to a SQLRelation.
     */
    private LogicalPlan convertCorr(StatCorr corr) {
        LogicalPlan inputPlan = convert(corr.getInput());
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(inputPlan);

        String col1 = SQLQuoting.quoteIdentifier(corr.getCol1());
        String col2 = SQLQuoting.quoteIdentifier(corr.getCol2());

        String sql = String.format(
            "SELECT CORR(%s, %s) AS corr FROM (%s) AS _stat_input",
            col1, col2, inputSql
        );

        return new SQLRelation(sql);
    }

    /**
     * Get the SQL expression for a describe statistic.
     */
    private String getDescribeStatExpression(String stat, String quotedCol) {
        return switch (stat) {
            case "count" -> String.format("CAST(COUNT(%s) AS VARCHAR)", quotedCol);
            case "mean" -> String.format("CAST(AVG(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
            case "stddev" -> String.format("CAST(STDDEV_SAMP(TRY_CAST(%s AS DOUBLE)) AS VARCHAR)", quotedCol);
            case "min" -> String.format("CAST(MIN(%s) AS VARCHAR)", quotedCol);
            case "max" -> String.format("CAST(MAX(%s) AS VARCHAR)", quotedCol);
            default -> "NULL";
        };
    }

    /**
     * Get the SQL expression for a summary statistic.
     */
    private String getSummaryStatExpression(String stat, String quotedCol) {
        // Check for percentile format (e.g., "25%", "50%", "75%")
        // Spark's summary() uses nearest-rank method (PERCENTILE_DISC), not linear interpolation
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
        return getDescribeStatExpression(stat, quotedCol);
    }
}
