package com.thunderduck.connect.converter;

import com.thunderduck.logical.*;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.ColumnReference;
import com.thunderduck.expression.BinaryExpression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.types.StructType;
import com.thunderduck.types.StructField;

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

                // Get the path from options
                String path = options.get("path");
                if (path == null || path.isEmpty()) {
                    // Try paths for multiple files
                    String paths = options.get("paths");
                    if (paths != null && !paths.isEmpty()) {
                        // Take the first path for now
                        path = paths.split(",")[0].trim();
                    }
                }

                if (path == null || path.isEmpty()) {
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
            // For now, wrap each expression as an aggregate
            // TODO: Extract function name and alias from the expression
            if (expr instanceof FunctionCall) {
                FunctionCall func = (FunctionCall) expr;
                com.thunderduck.logical.Aggregate.AggregateExpression aggExpr =
                    new com.thunderduck.logical.Aggregate.AggregateExpression(
                        func.functionName(),
                        func.arguments().isEmpty() ? null : func.arguments().get(0),
                        null  // alias will be set later
                    );
                aggExprs.add(aggExpr);
            }
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
}