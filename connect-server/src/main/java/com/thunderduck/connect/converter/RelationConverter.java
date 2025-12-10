package com.thunderduck.connect.converter;

import com.thunderduck.logical.*;
import com.thunderduck.expression.Expression;
import com.thunderduck.expression.FunctionCall;
import com.thunderduck.expression.AliasExpression;
import com.thunderduck.logical.LocalDataRelation;
import com.thunderduck.logical.RangeRelation;
import com.thunderduck.generator.SQLQuoting;
import com.thunderduck.generator.SQLGenerator;

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
            case RANGE:
                return convertRange(relation.getRange());
            case DROP:
                return convertDrop(relation.getDrop());
            case WITH_COLUMNS_RENAMED:
                return convertWithColumnsRenamed(relation.getWithColumnsRenamed());
            case WITH_COLUMNS:
                return convertWithColumns(relation.getWithColumns());
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

        logger.debug("Creating Drop SQL: {}", sql);
        return new SQLRelation(sql);
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
    private LogicalPlan convertWithColumnsRenamed(org.apache.spark.connect.proto.WithColumnsRenamed withColumnsRenamed) {
        LogicalPlan input = convert(withColumnsRenamed.getInput());

        Map<String, String> renameMap = withColumnsRenamed.getRenameColumnsMapMap();

        if (renameMap.isEmpty()) {
            logger.debug("WithColumnsRenamed: no columns to rename, returning input as-is");
            return input;
        }

        // Generate SQL using COLUMNS() with lambdas for renaming
        // DuckDB supports: SELECT COLUMNS(c -> IF(c='old', 'new', c)) FROM ...
        // But a simpler approach is to use explicit aliasing with COLUMNS(*) replacement
        //
        // For renaming columns, we use DuckDB's COLUMNS expression with CASE:
        // SELECT COLUMNS(*) AS (CASE WHEN c.name = 'old' THEN 'new' ELSE c.name END)
        //
        // Actually the cleanest approach for renaming is:
        // SELECT * EXCLUDE (old_col), old_col AS new_col FROM ...
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // Build the SELECT clause with exclusions and renamed columns
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

        // SELECT * EXCLUDE (old_cols), old_col AS new_col, ... FROM ...
        String sql = String.format("SELECT * EXCLUDE (%s), %s FROM (%s) AS _rename_subquery",
            excludeList.toString(), selectAdditions.toString(), inputSql);

        logger.debug("Creating WithColumnsRenamed SQL: {}", sql);
        return new SQLRelation(sql);
    }

    /**
     * Converts a WithColumns relation to a LogicalPlan.
     *
     * <p>Adds new columns or replaces existing columns with the given expressions.
     * Each alias in the list provides both the column name and the expression.
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

        // Generate SQL
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // Build column expressions
        // For simplicity, we'll use: SELECT *, expr1 AS name1, expr2 AS name2 FROM ...
        // This adds new columns. For replacing, DuckDB's REPLACE would be better,
        // but that requires knowing which columns already exist.
        //
        // Strategy: Use SELECT * REPLACE (...) for all columns, which handles both
        // adding new columns and replacing existing ones.
        StringBuilder columnExprs = new StringBuilder();
        for (int i = 0; i < aliases.size(); i++) {
            if (i > 0) {
                columnExprs.append(", ");
            }
            org.apache.spark.connect.proto.Expression.Alias alias = aliases.get(i);

            // Convert the expression
            Expression expr = expressionConverter.convert(alias.getExpr());
            String exprSql = expr.toSQL();

            // Get the column name (first name part)
            String colName = alias.getName(0);

            columnExprs.append(exprSql);
            columnExprs.append(" AS ");
            columnExprs.append(SQLQuoting.quoteIdentifier(colName));
        }

        // Use SELECT *, new_cols FROM ... which adds columns at the end
        // For replacing existing columns, we'd need schema awareness
        String sql = String.format("SELECT *, %s FROM (%s) AS _withcol_subquery",
            columnExprs.toString(), inputSql);

        logger.debug("Creating WithColumns SQL: {}", sql);
        return new SQLRelation(sql);
    }
}
