package com.thunderduck.connect.converter;

import com.thunderduck.logical.*;
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
            case TAIL:
                return convertTail(relation.getTail());
            case JOIN:
                return convertJoin(relation.getJoin());
            case SET_OP:
                return convertSetOp(relation.getSetOp());
            case SQL:
                // Direct SQL relation - we'll handle this as a special case
                return new SQLRelation(relation.getSql().getQuery());
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
            // For named tables, use TABLE format (regular DuckDB table)
            return new TableScan(tableName, TableScan.TableFormat.TABLE, null);
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
                .collect(Collectors.toList());

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

        // Generate SQL
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        // Collect all column names being added/replaced
        List<String> colNames = new ArrayList<>();
        StringBuilder newColExprs = new StringBuilder();

        for (int i = 0; i < aliases.size(); i++) {
            org.apache.spark.connect.proto.Expression.Alias alias = aliases.get(i);

            // Convert the expression
            Expression expr = expressionConverter.convert(alias.getExpr());
            String exprSql = expr.toSQL();

            // Get the column name (first name part)
            String colName = alias.getName(0);
            colNames.add(colName);
            String quotedColName = SQLQuoting.quoteIdentifier(colName);

            if (i > 0) {
                newColExprs.append(", ");
            }
            newColExprs.append(exprSql).append(" AS ").append(quotedColName);

            logger.debug("WithColumns: adding/replacing column '{}'", colName);
        }

        // Build the COLUMNS filter to exclude columns being replaced
        // This works for both add (no match, keeps all) and replace (filters out existing)
        // Using DuckDB's COLUMNS(c -> c NOT IN (...)) lambda syntax
        StringBuilder excludeFilter = new StringBuilder();
        excludeFilter.append("COLUMNS(c -> c NOT IN (");
        for (int i = 0; i < colNames.size(); i++) {
            if (i > 0) {
                excludeFilter.append(", ");
            }
            // Column names need to be strings for comparison
            excludeFilter.append("'").append(colNames.get(i).replace("'", "''")).append("'");
        }
        excludeFilter.append("))");

        // Build: SELECT COLUMNS(c -> c NOT IN ('col1', 'col2')), expr1 AS col1, expr2 AS col2 FROM (...)
        String sql = String.format("SELECT %s, %s FROM (%s) AS _withcol_subquery",
            excludeFilter.toString(), newColExprs.toString(), inputSql);

        logger.debug("Creating WithColumns SQL: {}", sql);
        return new SQLRelation(sql);
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

        // Generate SQL to rename all columns positionally
        // SELECT col1 AS new1, col2 AS new2, ... FROM (input)
        // Since we don't have schema info, use DuckDB's positional syntax:
        // SELECT * FROM (input) AS _todf_subquery(new1, new2, ...)
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < newNames.size(); i++) {
            if (i > 0) {
                columnList.append(", ");
            }
            columnList.append(SQLQuoting.quoteIdentifier(newNames.get(i)));
        }

        // DuckDB supports table alias with column names: AS alias(col1, col2, ...)
        String sql = String.format("SELECT * FROM (%s) AS _todf_subquery(%s)",
            inputSql, columnList.toString());

        logger.debug("Creating ToDF SQL: {}", sql);
        return new SQLRelation(sql);
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
                } else if (colIndex < fillValuesSql.size()) {
                    // Multiple values matched by position
                    fillValue = fillValuesSql.get(colIndex);
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

        logger.debug("Creating NAFill SQL: {}", sql);
        return new SQLRelation(sql);
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

        logger.debug("Creating Unpivot SQL: {}", sql);
        return new SQLRelation(sql);
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

        // Generate SQL with explicit alias
        SQLGenerator generator = new SQLGenerator();
        String inputSql = generator.generate(input);

        String sql = String.format("SELECT * FROM (%s) AS %s",
            inputSql, SQLQuoting.quoteIdentifier(alias));

        logger.debug("Creating SubqueryAlias SQL with alias '{}': {}", alias, sql);
        return new SQLRelation(sql);
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
}
