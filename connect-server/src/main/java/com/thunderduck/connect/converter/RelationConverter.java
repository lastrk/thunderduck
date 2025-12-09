package com.thunderduck.connect.converter;

import com.thunderduck.logical.*;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.AliasExpression;
import com.thunderduck.logical.LocalDataRelation;

import org.apache.spark.connect.proto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public RelationConverter(ExpressionConverter expressionConverter) {
        this.expressionConverter = expressionConverter;
    }

    /**
     * Converts a Spark Connect Relation to a LogicalPlan.
     *
     * @param relation the Protobuf relation
     * @return the converted LogicalPlan
     * @throws PlanConversionException if conversion fails
     */
    public LogicalPlan convert(Relation relation) {
        logger.debug("Converting relation type: {}", relation.getRelTypeCase());

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
            case JOIN:
                return convertJoin(relation.getJoin());
            case SET_OP:
                return convertSetOp(relation.getSetOp());
            case SQL:
                // Direct SQL relation - we'll handle this as a special case
                return new SQLRelation(relation.getSql().getQuery());
            case SHOW_STRING:
                // ShowString wraps another relation - just unwrap and convert the inner relation
                return convert(relation.getShowString().getInput());
            case DEDUPLICATE:
                // Handle distinct() operation
                return convertDeduplicate(relation.getDeduplicate());
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
                // Schema will be inferred by DuckDB from Parquet file
                return new TableScan(path, TableScan.TableFormat.PARQUET, null);
            }

            // Add support for other formats as needed
            throw new PlanConversionException("Unsupported data source format: " + format);
        } else if (read.hasNamedTable()) {
            Read.NamedTable namedTable = read.getNamedTable();
            String tableName = namedTable.getUnparsedIdentifier();
            logger.debug("Creating TableScan for table: {}", tableName);
            // For named tables, assume parquet format (will be enhanced later)
            return new TableScan(tableName, TableScan.TableFormat.PARQUET, null);
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

        // Convert projection expressions
        List<Expression> expressions = project.getExpressionsList().stream()
                .map(expressionConverter::convert)
                .collect(Collectors.toList());

        logger.debug("Creating Project with {} expressions", expressions.size());
        return new com.thunderduck.logical.Project(input, expressions);
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
                .collect(Collectors.toList());

        // Convert aggregate expressions
        List<Expression> aggregateExprs = aggregate.getAggregateExpressionsList().stream()
                .map(expressionConverter::convert)
                .collect(Collectors.toList());

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
                argument = func.arguments().isEmpty() ? null : func.arguments().get(0);
            } else {
                logger.warn("Unexpected aggregate expression type: {}", expr.getClass().getSimpleName());
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

        return new com.thunderduck.logical.Aggregate(input, groupingExprs, aggExprs);
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
                .collect(Collectors.toList());

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
     * Converts a Join relation.
     *
     * @param join the Join relation
     * @return a Join logical plan
     */
    private LogicalPlan convertJoin(org.apache.spark.connect.proto.Join join) {
        LogicalPlan left = convert(join.getLeft());
        LogicalPlan right = convert(join.getRight());

        // Convert join condition if present
        Expression condition = null;
        if (join.hasJoinCondition()) {
            condition = expressionConverter.convert(join.getJoinCondition());
        }

        // Map join type
        com.thunderduck.logical.Join.JoinType joinType = mapJoinType(join.getJoinType());

        logger.debug("Creating Join of type: {}", joinType);
        return new com.thunderduck.logical.Join(left, right, joinType, condition);
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

        switch (setOp.getSetOpType()) {
            case SET_OP_TYPE_UNION:
                return new Union(left, right, !setOp.getIsAll());
            case SET_OP_TYPE_INTERSECT:
                return new Intersect(left, right, !setOp.getIsAll());
            case SET_OP_TYPE_EXCEPT:
                return new Except(left, right, !setOp.getIsAll());
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
}
