# Step-by-Step Implementation Plan: Embedded Spark DataFrame API

## Executive Summary

This document provides a concrete implementation plan for building an embedded Spark DataFrame API replacement that translates operations to Apache Calcite RelNodes and executes them on DuckDB via PostgreSQL-compatible SQL. The architecture follows a clean layered design with clear separation of concerns.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│   Client Application (Scala/Java)               │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│   Layer 1: Spark DataFrame API (Drop-in)        │
│   - Dataset/DataFrame interfaces                │
│   - Column expressions                          │
│   - Lazy evaluation                            │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│   Layer 2: Plan Representation                  │
│   - Internal LogicalPlan tree                   │
│   - Expression tree                            │
│   - Deferred execution                         │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│   Layer 3: Translation Layer                    │
│   - Spark Plan → Calcite RelNode               │
│   - Type mapping                               │
│   - Function registry                          │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│   Layer 4: SQL Generation                       │
│   - RelNode → PostgreSQL dialect               │
│   - Query optimization                         │
│   - Pushdown rules                            │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│   Layer 5: Execution Engine                     │
│   - DuckDB (embedded)                          │
│   - Result materialization                     │
│   - Arrow format conversion                    │
└─────────────────────────────────────────────────┘
```

## Project Structure

```
spark-connect2sql/
├── pom.xml
├── spark-api/                     # Layer 1: Spark API
│   ├── src/main/java/
│   │   └── org/apache/spark/sql/
│   │       ├── Dataset.java
│   │       ├── DataFrame.java
│   │       ├── Column.java
│   │       ├── Row.java
│   │       ├── SparkSession.java
│   │       └── functions.java
│   └── pom.xml
├── spark-plan/                    # Layer 2: Plan Representation
│   ├── src/main/java/
│   │   └── com/spark2sql/plan/
│   │       ├── LogicalPlan.java
│   │       ├── Expression.java
│   │       ├── PlanBuilder.java
│   │       └── nodes/
│   │           ├── Project.java
│   │           ├── Filter.java
│   │           ├── Join.java
│   │           └── Aggregate.java
│   └── pom.xml
├── translator/                    # Layer 3: Translation
│   ├── src/main/java/
│   │   └── com/spark2sql/translator/
│   │       ├── PlanTranslator.java
│   │       ├── ExpressionTranslator.java
│   │       ├── TypeMapper.java
│   │       └── FunctionRegistry.java
│   └── pom.xml
├── sql-generator/                 # Layer 4: SQL Generation
│   ├── src/main/java/
│   │   └── com/spark2sql/sql/
│   │       ├── CalciteDialect.java
│   │       ├── PostgreSQLGenerator.java
│   │       └── OptimizationRules.java
│   └── pom.xml
├── execution/                     # Layer 5: Execution
│   ├── src/main/java/
│   │   └── com/spark2sql/execution/
│   │       ├── DuckDBExecutor.java
│   │       ├── ResultSetMapper.java
│   │       └── ArrowConverter.java
│   └── pom.xml
└── testing/                       # Testing Framework
    ├── src/test/java/
    │   └── com/spark2sql/test/
    │       ├── DifferentialTest.java
    │       ├── SparkTestHarness.java
    │       └── SemanticValidator.java
    └── pom.xml
```

## Implementation Steps

### Phase 1: Foundation (Weeks 1-2)

#### Step 1.1: Project Setup
```xml
<!-- Root pom.xml -->
<project>
    <groupId>com.spark2sql</groupId>
    <artifactId>spark-connect2sql-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <!-- Core Dependencies -->
        <spark.version>3.5.3</spark.version>
        <calcite.version>1.37.0</calcite.version>  <!-- Latest as of Nov 2024 -->
        <duckdb.version>1.4.1</duckdb.version>

        <!-- Supporting Libraries -->
        <arrow.version>14.0.0</arrow.version>
        <avatica.version>1.25.0</avatica.version>
        <guava.version>32.1.2-jre</guava.version>
        <slf4j.version>2.0.9</slf4j.version>

        <!-- Testing -->
        <junit.version>5.10.0</junit.version>
        <jqwik.version>1.8.2</jqwik.version>
        <assertj.version>3.24.2</assertj.version>
        <jmh.version>1.37</jmh.version>
    </properties>

    <modules>
        <module>spark-api</module>
        <module>spark-plan</module>
        <module>translator</module>
        <module>sql-generator</module>
        <module>execution</module>
        <module>testing</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <!-- Spark Dependencies -->
            <dependency>
                <groupId>org.apache.spark</groupId>
                <artifactId>spark-sql_2.13</artifactId>
                <version>${spark.version}</version>
                <scope>provided</scope>
            </dependency>

            <!-- Apache Calcite -->
            <dependency>
                <groupId>org.apache.calcite</groupId>
                <artifactId>calcite-core</artifactId>
                <version>${calcite.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.calcite</groupId>
                <artifactId>calcite-linq4j</artifactId>
                <version>${calcite.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.calcite.avatica</groupId>
                <artifactId>avatica-core</artifactId>
                <version>${avatica.version}</version>
            </dependency>

            <!-- DuckDB -->
            <dependency>
                <groupId>org.duckdb</groupId>
                <artifactId>duckdb_jdbc</artifactId>
                <version>${duckdb.version}</version>
            </dependency>

            <!-- Apache Arrow -->
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-vector</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-memory-netty</artifactId>
                <version>${arrow.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.arrow</groupId>
                <artifactId>arrow-jdbc</artifactId>
                <version>${arrow.version}</version>
            </dependency>

            <!-- Testing -->
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>net.jqwik</groupId>
                <artifactId>jqwik</artifactId>
                <version>${jqwik.version}</version>
                <scope>test</scope>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

#### Step 1.2: Spark API Interfaces (Layer 1)
```java
// spark-api/src/main/java/org/apache/spark/sql/SparkSession.java
package org.apache.spark.sql;

import com.spark2sql.plan.PlanBuilder;
import com.spark2sql.execution.ExecutionEngine;

public class SparkSession {
    private final PlanBuilder planBuilder;
    private final ExecutionEngine executionEngine;

    private SparkSession(Builder builder) {
        this.planBuilder = new PlanBuilder();
        this.executionEngine = builder.createExecutionEngine();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Dataset<Row> sql(String sqlQuery) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public DataFrameReader read() {
        return new DataFrameReader(this, planBuilder);
    }

    // Compatibility with Spark 3.5.3 API
    public RuntimeConfig conf() {
        return new RuntimeConfig();
    }

    public void close() {
        try {
            executionEngine.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close session", e);
        }
    }

    public static class Builder {
        private String appName = "Spark2SQL";
        private boolean enableDuckDB = true;
        private String master = "local[*]";

        public Builder appName(String name) {
            this.appName = name;
            return this;
        }

        public Builder master(String master) {
            // Ignored for embedded mode, kept for API compatibility
            this.master = master;
            return this;
        }

        public Builder config(String key, String value) {
            // Store configurations for later use
            return this;
        }

        public SparkSession getOrCreate() {
            return new SparkSession(this);
        }

        private ExecutionEngine createExecutionEngine() {
            // Initialize DuckDB 1.4.1 connection
            return new DuckDBExecutionEngine();
        }
    }
}
```

```java
// spark-api/src/main/java/org/apache/spark/sql/Dataset.java
package org.apache.spark.sql;

import com.spark2sql.plan.LogicalPlan;
import com.spark2sql.plan.nodes.*;
import java.util.List;

public class Dataset<T> {
    protected final LogicalPlan logicalPlan;
    protected final SparkSession sparkSession;
    protected final Encoder<T> encoder;

    protected Dataset(SparkSession session, LogicalPlan plan, Encoder<T> encoder) {
        this.sparkSession = session;
        this.logicalPlan = plan;
        this.encoder = encoder;
    }

    // Transformation Operations (Lazy)
    public Dataset<T> filter(Column condition) {
        return new Dataset<>(sparkSession,
            new Filter(logicalPlan, condition.getExpression()),
            encoder);
    }

    public Dataset<T> filter(String conditionExpr) {
        return filter(functions.expr(conditionExpr));
    }

    public Dataset<T> select(Column... columns) {
        return new Dataset<>(sparkSession,
            new Project(logicalPlan, Arrays.asList(columns)),
            Encoders.row());
    }

    public Dataset<T> select(String col, String... cols) {
        Column[] columns = new Column[cols.length + 1];
        columns[0] = functions.col(col);
        for (int i = 0; i < cols.length; i++) {
            columns[i + 1] = functions.col(cols[i]);
        }
        return select(columns);
    }

    public Dataset<Row> join(Dataset<?> right, Column joinExpression) {
        return join(right, joinExpression, "inner");
    }

    public Dataset<Row> join(Dataset<?> right, Column joinExpression, String joinType) {
        return new Dataset<>(sparkSession,
            new Join(logicalPlan, right.logicalPlan, joinExpression, joinType),
            Encoders.row());
    }

    public RelationalGroupedDataset groupBy(Column... columns) {
        return new RelationalGroupedDataset(sparkSession, logicalPlan, columns);
    }

    public RelationalGroupedDataset groupBy(String col1, String... cols) {
        Column[] columns = new Column[cols.length + 1];
        columns[0] = functions.col(col1);
        for (int i = 0; i < cols.length; i++) {
            columns[i + 1] = functions.col(cols[i]);
        }
        return groupBy(columns);
    }

    public Dataset<T> orderBy(Column... sortExprs) {
        return new Dataset<>(sparkSession,
            new Sort(logicalPlan, Arrays.asList(sortExprs)),
            encoder);
    }

    public Dataset<T> limit(int n) {
        return new Dataset<>(sparkSession,
            new Limit(logicalPlan, n),
            encoder);
    }

    public Dataset<T> distinct() {
        return new Dataset<>(sparkSession,
            new Distinct(logicalPlan),
            encoder);
    }

    public Dataset<Row> agg(Column... exprs) {
        return groupBy().agg(exprs);
    }

    // Action Operations (Trigger Execution)
    public List<T> collect() {
        return sparkSession.executionEngine.execute(logicalPlan, encoder);
    }

    public List<T> collectAsList() {
        return collect();
    }

    public long count() {
        Dataset<Row> counted = this.agg(functions.count("*").as("count"));
        return (Long) counted.collect().get(0).get(0);
    }

    public void show() {
        show(20, true);
    }

    public void show(int numRows) {
        show(numRows, true);
    }

    public void show(int numRows, boolean truncate) {
        Dataset<T> limited = this.limit(numRows);
        List<T> rows = limited.collect();
        TablePrinter.print(rows, logicalPlan.schema(), truncate);
    }

    public T first() {
        return limit(1).collect().get(0);
    }

    public T head() {
        return first();
    }

    public T[] head(int n) {
        return limit(n).collect().toArray((T[]) new Object[0]);
    }

    // Schema and metadata
    public StructType schema() {
        return logicalPlan.schema();
    }

    public String[] columns() {
        return schema().fieldNames();
    }

    public void printSchema() {
        schema().printTreeString();
    }

    // Write operations
    public DataFrameWriter<T> write() {
        return new DataFrameWriter<>(this);
    }

    // Caching (no-op for embedded mode, kept for API compatibility)
    public Dataset<T> cache() {
        return this;
    }

    public Dataset<T> persist() {
        return this;
    }

    public Dataset<T> unpersist() {
        return this;
    }

    // Temporary views
    public void createOrReplaceTempView(String viewName) {
        sparkSession.catalog().createOrReplaceTempView(viewName, this);
    }

    public void createTempView(String viewName) {
        sparkSession.catalog().createTempView(viewName, this);
    }

    // Spark 3.5.3 specific methods
    public Dataset<T> repartition(int numPartitions) {
        // No-op for embedded mode
        return this;
    }

    public Dataset<T> coalesce(int numPartitions) {
        // No-op for embedded mode
        return this;
    }

    public Dataset<Row> toDF(String... colNames) {
        if (colNames.length > 0) {
            return select(Arrays.stream(columns())
                .limit(colNames.length)
                .map(c -> functions.col(c))
                .toArray(Column[]::new))
                .toDF();
        }
        return new Dataset<>(sparkSession, logicalPlan, Encoders.row());
    }
}
```

```java
// spark-api/src/main/java/org/apache/spark/sql/DataFrame.java
package org.apache.spark.sql;

// DataFrame is just an alias for Dataset<Row> in Spark 3.5.x
public interface DataFrame extends Dataset<Row> {
    // Interface to maintain compatibility with Spark 3.5.3
}
```

#### Step 1.3: Column and Expression API
```java
// spark-api/src/main/java/org/apache/spark/sql/Column.java
package org.apache.spark.sql;

import com.spark2sql.plan.Expression;

public class Column {
    private final Expression expression;

    public Column(String name) {
        this.expression = new ColumnReference(name);
    }

    public Column(Expression expr) {
        this.expression = expr;
    }

    // Comparison operations
    public Column equalTo(Object other) {
        return new Column(new EqualTo(expression, lit(other).expression));
    }

    public Column notEqual(Object other) {
        return new Column(new NotEqualTo(expression, lit(other).expression));
    }

    public Column gt(Object other) {
        return new Column(new GreaterThan(expression, lit(other).expression));
    }

    public Column lt(Object other) {
        return new Column(new LessThan(expression, lit(other).expression));
    }

    public Column geq(Object other) {
        return new Column(new GreaterThanOrEqual(expression, lit(other).expression));
    }

    public Column leq(Object other) {
        return new Column(new LessThanOrEqual(expression, lit(other).expression));
    }

    // Arithmetic operations
    public Column plus(Object other) {
        return new Column(new Add(expression, lit(other).expression));
    }

    public Column minus(Object other) {
        return new Column(new Subtract(expression, lit(other).expression));
    }

    public Column multiply(Object other) {
        return new Column(new Multiply(expression, lit(other).expression));
    }

    public Column divide(Object other) {
        return new Column(new Divide(expression, lit(other).expression));
    }

    public Column mod(Object other) {
        return new Column(new Remainder(expression, lit(other).expression));
    }

    // String operations (Spark 3.5.3 compatible)
    public Column contains(Object other) {
        return new Column(new Contains(expression, lit(other).expression));
    }

    public Column startsWith(Object other) {
        return new Column(new StartsWith(expression, lit(other).expression));
    }

    public Column endsWith(Object other) {
        return new Column(new EndsWith(expression, lit(other).expression));
    }

    public Column like(String literal) {
        return new Column(new Like(expression, lit(literal).expression));
    }

    public Column rlike(String literal) {
        return new Column(new RLike(expression, lit(literal).expression));
    }

    // Null handling
    public Column isNull() {
        return new Column(new IsNull(expression));
    }

    public Column isNotNull() {
        return new Column(new IsNotNull(expression));
    }

    // Boolean operations
    public Column and(Column other) {
        return new Column(new And(expression, other.expression));
    }

    public Column or(Column other) {
        return new Column(new Or(expression, other.expression));
    }

    public Column not() {
        return new Column(new Not(expression));
    }

    // Aliasing
    public Column as(String alias) {
        return new Column(new Alias(expression, alias));
    }

    public Column alias(String alias) {
        return as(alias);
    }

    public Column name(String alias) {
        return as(alias);
    }

    // Casting
    public Column cast(DataType to) {
        return new Column(new Cast(expression, to));
    }

    public Column cast(String to) {
        return cast(DataType.fromDDL(to));
    }

    // Sorting
    public Column asc() {
        return new Column(new SortOrder(expression, true));
    }

    public Column desc() {
        return new Column(new SortOrder(expression, false));
    }

    Expression getExpression() {
        return expression;
    }
}
```

### Phase 2: Plan Representation (Week 3)

#### Step 2.1: Logical Plan Structure
```java
// spark-plan/src/main/java/com/spark2sql/plan/LogicalPlan.java
package com.spark2sql.plan;

import java.util.List;
import org.apache.spark.sql.types.StructType;

public abstract class LogicalPlan {
    protected List<LogicalPlan> children;
    protected StructType schema;

    public abstract String nodeName();
    public abstract List<Expression> expressions();

    public List<LogicalPlan> children() {
        return children;
    }

    public StructType schema() {
        if (schema == null) {
            schema = computeSchema();
        }
        return schema;
    }

    protected abstract StructType computeSchema();

    // Visitor pattern for tree traversal
    public abstract <T> T accept(PlanVisitor<T> visitor);

    // For debugging and logging
    public String treeString() {
        StringBuilder sb = new StringBuilder();
        treeString(sb, 0);
        return sb.toString();
    }

    private void treeString(StringBuilder sb, int indent) {
        sb.append(" ".repeat(indent)).append(nodeName()).append("\n");
        for (LogicalPlan child : children()) {
            child.treeString(sb, indent + 2);
        }
    }
}
```

```java
// spark-plan/src/main/java/com/spark2sql/plan/nodes/Project.java
package com.spark2sql.plan.nodes;

import org.apache.spark.sql.types.*;

public class Project extends LogicalPlan {
    private final List<Column> projectList;

    public Project(LogicalPlan child, List<Column> columns) {
        this.children = Arrays.asList(child);
        this.projectList = columns;
    }

    @Override
    public String nodeName() {
        return "Project " + projectList.stream()
            .map(c -> c.getExpression().toString())
            .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public List<Expression> expressions() {
        return projectList.stream()
            .map(Column::getExpression)
            .collect(Collectors.toList());
    }

    @Override
    protected StructType computeSchema() {
        // Compute output schema based on projected columns
        StructField[] fields = projectList.stream()
            .map(col -> {
                Expression expr = col.getExpression();
                return new StructField(
                    expr.name(),
                    expr.dataType(),
                    expr.nullable(),
                    Metadata.empty()
                );
            })
            .toArray(StructField[]::new);

        return new StructType(fields);
    }

    public List<Column> getProjectList() {
        return projectList;
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitProject(this);
    }
}
```

```java
// spark-plan/src/main/java/com/spark2sql/plan/nodes/Filter.java
package com.spark2sql.plan.nodes;

public class Filter extends LogicalPlan {
    private final Expression condition;

    public Filter(LogicalPlan child, Expression condition) {
        this.children = Arrays.asList(child);
        this.condition = condition;
    }

    @Override
    public String nodeName() {
        return "Filter " + condition.toString();
    }

    @Override
    public List<Expression> expressions() {
        return Arrays.asList(condition);
    }

    @Override
    protected StructType computeSchema() {
        // Filter doesn't change schema
        return children.get(0).schema();
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public <T> T accept(PlanVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }
}
```

### Phase 3: Translation Layer (Weeks 4-5)

#### Step 3.1: Plan to Calcite RelNode Translation
```java
// translator/src/main/java/com/spark2sql/translator/PlanTranslator.java
package com.spark2sql.translator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import com.spark2sql.plan.LogicalPlan;

public class PlanTranslator implements PlanVisitor<RelNode> {
    private final RelBuilder relBuilder;
    private final ExpressionTranslator exprTranslator;
    private final TypeMapper typeMapper;

    public PlanTranslator(CalciteContext context) {
        this.relBuilder = RelBuilder.create(context.getFrameworkConfig());
        this.exprTranslator = new ExpressionTranslator(relBuilder.getRexBuilder());
        this.typeMapper = new TypeMapper(relBuilder.getTypeFactory());
    }

    public RelNode translate(LogicalPlan sparkPlan) {
        return sparkPlan.accept(this);
    }

    @Override
    public RelNode visitProject(Project project) {
        // Translate child first
        RelNode child = project.children().get(0).accept(this);
        relBuilder.push(child);

        // Translate projection expressions
        List<RexNode> rexNodes = project.expressions().stream()
            .map(expr -> exprTranslator.translate(expr, child.getRowType()))
            .collect(Collectors.toList());

        List<String> fieldNames = project.expressions().stream()
            .map(Expression::name)
            .collect(Collectors.toList());

        return relBuilder.project(rexNodes, fieldNames).build();
    }

    @Override
    public RelNode visitFilter(Filter filter) {
        // Translate child first
        RelNode child = filter.children().get(0).accept(this);
        relBuilder.push(child);

        // Translate filter condition
        RexNode condition = exprTranslator.translate(
            filter.getCondition(),
            child.getRowType()
        );

        return relBuilder.filter(condition).build();
    }

    @Override
    public RelNode visitJoin(Join join) {
        // Translate both children
        RelNode left = join.getLeft().accept(this);
        RelNode right = join.getRight().accept(this);

        relBuilder.push(left);
        relBuilder.push(right);

        // Translate join condition
        RexNode condition = exprTranslator.translateJoinCondition(
            join.getCondition(),
            left.getRowType(),
            right.getRowType()
        );

        JoinRelType joinType = mapJoinType(join.getJoinType());
        return relBuilder.join(joinType, condition).build();
    }

    @Override
    public RelNode visitAggregate(Aggregate aggregate) {
        RelNode child = aggregate.getChild().accept(this);
        relBuilder.push(child);

        // Translate group by keys
        List<RexNode> groupKeys = aggregate.getGroupingExpressions().stream()
            .map(expr -> exprTranslator.translate(expr, child.getRowType()))
            .collect(Collectors.toList());

        // Translate aggregate functions
        List<AggregateCall> aggCalls = aggregate.getAggregateExpressions().stream()
            .map(expr -> translateAggregateFunction(expr, child.getRowType()))
            .collect(Collectors.toList());

        return relBuilder
            .aggregate(relBuilder.groupKey(groupKeys), aggCalls)
            .build();
    }

    @Override
    public RelNode visitTableScan(TableScan scan) {
        // Create table scan RelNode
        return relBuilder.scan(scan.getTableName()).build();
    }

    @Override
    public RelNode visitSort(Sort sort) {
        RelNode child = sort.getChild().accept(this);
        relBuilder.push(child);

        List<RexNode> sortKeys = sort.getSortExpressions().stream()
            .map(expr -> exprTranslator.translate(expr, child.getRowType()))
            .collect(Collectors.toList());

        return relBuilder.sort(sortKeys).build();
    }

    @Override
    public RelNode visitLimit(Limit limit) {
        RelNode child = limit.getChild().accept(this);
        relBuilder.push(child);

        return relBuilder.limit(0, limit.getN()).build();
    }

    @Override
    public RelNode visitDistinct(Distinct distinct) {
        RelNode child = distinct.getChild().accept(this);
        relBuilder.push(child);

        return relBuilder.distinct().build();
    }

    private JoinRelType mapJoinType(String sparkJoinType) {
        return switch (sparkJoinType.toLowerCase()) {
            case "inner" -> JoinRelType.INNER;
            case "left", "left_outer" -> JoinRelType.LEFT;
            case "right", "right_outer" -> JoinRelType.RIGHT;
            case "full", "full_outer", "outer" -> JoinRelType.FULL;
            case "semi", "left_semi" -> JoinRelType.SEMI;
            case "anti", "left_anti" -> JoinRelType.ANTI;
            default -> throw new UnsupportedOperationException(
                "Join type not supported: " + sparkJoinType);
        };
    }
}
```

#### Step 3.2: Expression Translation (Updated for Calcite 1.37.0)
```java
// translator/src/main/java/com/spark2sql/translator/ExpressionTranslator.java
package com.spark2sql.translator;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.rel.type.RelDataType;
import com.spark2sql.plan.Expression;

public class ExpressionTranslator {
    private final RexBuilder rexBuilder;
    private final FunctionRegistry functionRegistry;
    private final TypeMapper typeMapper;

    public ExpressionTranslator(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
        this.functionRegistry = new FunctionRegistry();
        this.typeMapper = new TypeMapper(rexBuilder.getTypeFactory());
    }

    public RexNode translate(Expression expr, RelDataType inputRowType) {
        return expr.accept(new ExpressionVisitor<RexNode>() {
            @Override
            public RexNode visitColumn(ColumnReference col) {
                // Find field index in input row type
                int fieldIndex = inputRowType.getFieldNames().indexOf(col.getName());
                if (fieldIndex == -1) {
                    throw new IllegalArgumentException(
                        "Column not found: " + col.getName());
                }
                return rexBuilder.makeInputRef(
                    inputRowType.getFieldList().get(fieldIndex).getType(),
                    fieldIndex
                );
            }

            @Override
            public RexNode visitLiteral(Literal lit) {
                RelDataType calciteType = typeMapper.toCalciteType(lit.dataType());
                return rexBuilder.makeLiteral(
                    lit.getValue(),
                    calciteType,
                    false
                );
            }

            @Override
            public RexNode visitBinaryOperation(BinaryOperation binOp) {
                RexNode left = translate(binOp.getLeft(), inputRowType);
                RexNode right = translate(binOp.getRight(), inputRowType);

                SqlOperator operator = mapOperator(binOp.getOperator());
                return rexBuilder.makeCall(operator, left, right);
            }

            @Override
            public RexNode visitFunction(FunctionCall func) {
                List<RexNode> args = func.getArguments().stream()
                    .map(arg -> translate(arg, inputRowType))
                    .collect(Collectors.toList());

                SqlOperator operator = functionRegistry.lookupFunction(
                    func.getName(),
                    args.size()
                );

                if (operator == null) {
                    // Create UDF call for unsupported functions
                    operator = createUDF(func.getName(), args.size());
                }

                return rexBuilder.makeCall(operator, args);
            }

            @Override
            public RexNode visitCast(Cast cast) {
                RexNode child = translate(cast.getChild(), inputRowType);
                RelDataType targetType = typeMapper.toCalciteType(cast.getTargetType());
                return rexBuilder.makeCast(targetType, child);
            }

            @Override
            public RexNode visitAlias(Alias alias) {
                // Aliases are handled at the project level
                return translate(alias.getChild(), inputRowType);
            }

            @Override
            public RexNode visitIsNull(IsNull isNull) {
                RexNode child = translate(isNull.getChild(), inputRowType);
                return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, child);
            }

            @Override
            public RexNode visitIsNotNull(IsNotNull isNotNull) {
                RexNode child = translate(isNotNull.getChild(), inputRowType);
                return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, child);
            }
        });
    }

    private SqlOperator mapOperator(String sparkOperator) {
        return switch (sparkOperator) {
            case "+" -> SqlStdOperatorTable.PLUS;
            case "-" -> SqlStdOperatorTable.MINUS;
            case "*" -> SqlStdOperatorTable.MULTIPLY;
            case "/" -> SqlStdOperatorTable.DIVIDE;
            case "%" -> SqlStdOperatorTable.MOD;
            case "=" -> SqlStdOperatorTable.EQUALS;
            case "!=" -> SqlStdOperatorTable.NOT_EQUALS;
            case ">" -> SqlStdOperatorTable.GREATER_THAN;
            case "<" -> SqlStdOperatorTable.LESS_THAN;
            case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case "AND" -> SqlStdOperatorTable.AND;
            case "OR" -> SqlStdOperatorTable.OR;
            case "NOT" -> SqlStdOperatorTable.NOT;
            case "LIKE" -> SqlStdOperatorTable.LIKE;
            case "IN" -> SqlStdOperatorTable.IN;
            case "BETWEEN" -> SqlStdOperatorTable.BETWEEN;
            default -> throw new UnsupportedOperationException(
                "Operator not supported: " + sparkOperator);
        };
    }
}
```

#### Step 3.3: Type Mapping (Updated for Spark 3.5.3 types)
```java
// translator/src/main/java/com/spark2sql/translator/TypeMapper.java
package com.spark2sql.translator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.sql.types.*;

public class TypeMapper {
    private final RelDataTypeFactory typeFactory;

    public TypeMapper(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    public RelDataType toCalciteType(DataType sparkType) {
        if (sparkType instanceof IntegerType) {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        } else if (sparkType instanceof LongType) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        } else if (sparkType instanceof ShortType) {
            return typeFactory.createSqlType(SqlTypeName.SMALLINT);
        } else if (sparkType instanceof ByteType) {
            return typeFactory.createSqlType(SqlTypeName.TINYINT);
        } else if (sparkType instanceof FloatType) {
            return typeFactory.createSqlType(SqlTypeName.REAL);
        } else if (sparkType instanceof DoubleType) {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        } else if (sparkType instanceof DecimalType) {
            DecimalType dt = (DecimalType) sparkType;
            return typeFactory.createSqlType(SqlTypeName.DECIMAL,
                dt.precision(), dt.scale());
        } else if (sparkType instanceof StringType) {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else if (sparkType instanceof BooleanType) {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        } else if (sparkType instanceof DateType) {
            return typeFactory.createSqlType(SqlTypeName.DATE);
        } else if (sparkType instanceof TimestampType) {
            // Spark 3.5.3 uses microsecond precision
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6);
        } else if (sparkType instanceof TimestampNTZType) {
            // Spark 3.5.3 timestamp without timezone
            return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 6);
        } else if (sparkType instanceof BinaryType) {
            return typeFactory.createSqlType(SqlTypeName.VARBINARY);
        } else if (sparkType instanceof ArrayType) {
            ArrayType at = (ArrayType) sparkType;
            RelDataType elementType = toCalciteType(at.elementType());
            return typeFactory.createArrayType(elementType, -1);
        } else if (sparkType instanceof MapType) {
            MapType mt = (MapType) sparkType;
            RelDataType keyType = toCalciteType(mt.keyType());
            RelDataType valueType = toCalciteType(mt.valueType());
            return typeFactory.createMapType(keyType, valueType);
        } else if (sparkType instanceof StructType) {
            StructType st = (StructType) sparkType;
            List<RelDataType> fieldTypes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();

            for (StructField field : st.fields()) {
                fieldNames.add(field.name());
                fieldTypes.add(toCalciteType(field.dataType()));
            }

            return typeFactory.createStructType(fieldTypes, fieldNames);
        } else {
            throw new UnsupportedOperationException(
                "Type mapping not supported for: " + sparkType);
        }
    }

    public DataType toSparkType(RelDataType calciteType) {
        SqlTypeName typeName = calciteType.getSqlTypeName();

        return switch (typeName) {
            case INTEGER -> DataTypes.IntegerType;
            case BIGINT -> DataTypes.LongType;
            case SMALLINT -> DataTypes.ShortType;
            case TINYINT -> DataTypes.ByteType;
            case REAL, FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case DECIMAL -> DataTypes.createDecimalType(
                calciteType.getPrecision(),
                calciteType.getScale());
            case VARCHAR, CHAR -> DataTypes.StringType;
            case BOOLEAN -> DataTypes.BooleanType;
            case DATE -> DataTypes.DateType;
            case TIMESTAMP -> DataTypes.TimestampType;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> DataTypes.TimestampNTZType;
            case VARBINARY, BINARY -> DataTypes.BinaryType;
            default -> throw new UnsupportedOperationException(
                "Type mapping not supported for: " + typeName);
        };
    }
}
```

### Phase 4: SQL Generation (Week 6)

#### Step 4.1: PostgreSQL SQL Generation
```java
// sql-generator/src/main/java/com/spark2sql/sql/PostgreSQLGenerator.java
package com.spark2sql.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

public class PostgreSQLGenerator {
    private final SqlDialect dialect;
    private final NumericalSemanticsHandler semanticsHandler;

    public PostgreSQLGenerator() {
        // Configure PostgreSQL dialect for DuckDB 1.4.1 compatibility
        this.dialect = PostgresqlSqlDialect.DEFAULT;
        this.semanticsHandler = new NumericalSemanticsHandler();
    }

    public String generateSQL(RelNode relNode) {
        // Apply optimizations first
        RelNode optimized = optimize(relNode);

        // Convert to SQL AST
        RelToSqlConverter converter = new RelToSqlConverter(dialect);
        RelToSqlConverter.Result result = converter.visitRoot(optimized);
        SqlNode sqlNode = result.asStatement();

        // Apply numerical semantics fixes
        sqlNode = semanticsHandler.adjustForSparkSemantics(sqlNode);

        // Generate final SQL string
        return sqlNode.toSqlString(dialect).getSql();
    }

    private RelNode optimize(RelNode relNode) {
        // Apply optimization rules using Calcite 1.37.0's optimizer
        HepProgramBuilder builder = new HepProgramBuilder();

        // Core optimization rules
        builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        builder.addRuleInstance(CoreRules.PROJECT_MERGE);
        builder.addRuleInstance(CoreRules.FILTER_MERGE);
        builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        builder.addRuleInstance(CoreRules.PROJECT_REMOVE);

        // Join optimization rules
        builder.addRuleInstance(CoreRules.JOIN_COMMUTE);
        builder.addRuleInstance(CoreRules.JOIN_PUSH_EXPRESSIONS);

        // Aggregate optimization rules
        builder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE);
        builder.addRuleInstance(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);

        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(relNode);
        return planner.findBestExp();
    }
}
```

#### Step 4.2: Numerical Semantics Handler
```java
// sql-generator/src/main/java/com/spark2sql/sql/NumericalSemanticsHandler.java
package com.spark2sql.sql;

import org.apache.calcite.sql.*;

public class NumericalSemanticsHandler {

    public SqlNode adjustForSparkSemantics(SqlNode sqlNode) {
        return sqlNode.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (call.getOperator() == SqlStdOperatorTable.DIVIDE) {
                    // Handle integer division semantics
                    return handleIntegerDivision(call);
                } else if (isArithmeticOperator(call.getOperator())) {
                    // Handle overflow semantics
                    return handleOverflow(call);
                }
                return super.visit(call);
            }

            private SqlNode handleIntegerDivision(SqlCall divCall) {
                SqlNode left = divCall.operand(0);
                SqlNode right = divCall.operand(1);

                // For integer division, use TRUNC(a::DOUBLE / b::DOUBLE)::INTEGER
                // to ensure truncation towards zero (Java semantics)
                // instead of floor division

                if (isIntegerType(left) && isIntegerType(right)) {
                    // DuckDB 1.1.0 specific: Use // for integer division
                    // But we need truncation semantics, not floor

                    // Cast to double, divide, truncate, cast back
                    SqlCall castLeft = SqlStdOperatorTable.CAST.createCall(
                        SqlParserPos.ZERO,
                        left,
                        SqlLiteral.createSymbol(SqlTypeName.DOUBLE, SqlParserPos.ZERO)
                    );

                    SqlCall castRight = SqlStdOperatorTable.CAST.createCall(
                        SqlParserPos.ZERO,
                        right,
                        SqlLiteral.createSymbol(SqlTypeName.DOUBLE, SqlParserPos.ZERO)
                    );

                    SqlCall divide = SqlStdOperatorTable.DIVIDE.createCall(
                        SqlParserPos.ZERO,
                        castLeft,
                        castRight
                    );

                    SqlCall trunc = createFunctionCall("TRUNC", divide);

                    return SqlStdOperatorTable.CAST.createCall(
                        SqlParserPos.ZERO,
                        trunc,
                        SqlLiteral.createSymbol(SqlTypeName.INTEGER, SqlParserPos.ZERO)
                    );
                }

                return divCall;
            }

            private SqlNode handleOverflow(SqlCall call) {
                // For overflow handling in DuckDB 1.4.1, we need to be careful
                // DuckDB by default throws on overflow, Spark wraps around
                // May need to use unchecked arithmetic operations or UDFs
                return call;
            }

            private SqlCall createFunctionCall(String functionName, SqlNode... args) {
                SqlIdentifier funcId = new SqlIdentifier(
                    functionName,
                    SqlParserPos.ZERO
                );
                SqlOperator funcOp = new SqlUnresolvedFunction(
                    funcId,
                    null,
                    null,
                    null,
                    null,
                    SqlFunctionCategory.USER_DEFINED_FUNCTION
                );
                return funcOp.createCall(SqlParserPos.ZERO, args);
            }
        });
    }
}
```

### Phase 5: Execution Engine (Week 7)

#### Step 5.1: DuckDB 1.4.1 Executor
```java
// execution/src/main/java/com/spark2sql/execution/DuckDBExecutor.java
package com.spark2sql.execution;

import org.duckdb.DuckDBConnection;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;

public class DuckDBExecutor implements ExecutionEngine {
    private final Connection connection;
    private final PostgreSQLGenerator sqlGenerator;
    private final ResultSetMapper resultMapper;
    private final PlanTranslator planTranslator;

    public DuckDBExecutor() throws SQLException {
        // Initialize embedded DuckDB 1.4.1
        Class.forName("org.duckdb.DuckDBDriver");
        this.connection = DriverManager.getConnection("jdbc:duckdb:");
        this.sqlGenerator = new PostgreSQLGenerator();
        this.resultMapper = new ResultSetMapper();
        this.planTranslator = new PlanTranslator(createCalciteContext());

        // Configure DuckDB for PostgreSQL compatibility
        configureDuckDB();
    }

    private void configureDuckDB() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // DuckDB 1.4.1 configuration for Spark compatibility

            // Set memory limit (optional, for testing)
            stmt.execute("SET memory_limit='4GB'");

            // Set threads (optional)
            stmt.execute("SET threads=4");

            // Enable progress bar for long queries (optional)
            stmt.execute("SET enable_progress_bar=false");

            // Configure for PostgreSQL compatibility
            stmt.execute("SET default_null_order='nulls_last'");

            // Configure decimal precision handling
            stmt.execute("SET preserve_insertion_order=true");

            // Register custom functions for Spark compatibility
            registerSparkUDFs();
        }
    }

    private void registerSparkUDFs() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Spark-compatible array_distinct (DuckDB 1.4.1 syntax)
            stmt.execute("""
                CREATE OR REPLACE MACRO spark_array_distinct(arr) AS (
                    list_distinct(arr)
                )
            """);

            // Spark-compatible regexp_extract
            stmt.execute("""
                CREATE OR REPLACE MACRO spark_regexp_extract(text, pattern, group_idx) AS (
                    regexp_extract(text, pattern, group_idx)
                )
            """);

            // Integer division with truncation semantics (Spark-compatible)
            stmt.execute("""
                CREATE OR REPLACE MACRO spark_int_divide(a, b) AS (
                    CASE
                        WHEN b = 0 THEN NULL
                        ELSE CAST(TRUNC(CAST(a AS DOUBLE) / CAST(b AS DOUBLE)) AS INTEGER)
                    END
                )
            """);

            // Spark-compatible coalesce (handles any number of arguments)
            stmt.execute("""
                CREATE OR REPLACE MACRO spark_coalesce(arg1, arg2) AS (
                    COALESCE(arg1, arg2)
                )
            """);

            // Spark-compatible greatest/least
            stmt.execute("""
                CREATE OR REPLACE MACRO spark_greatest(arg1, arg2) AS (
                    GREATEST(arg1, arg2)
                )
            """);

            stmt.execute("""
                CREATE OR REPLACE MACRO spark_least(arg1, arg2) AS (
                    LEAST(arg1, arg2)
                )
            """);
        }
    }

    @Override
    public <T> List<T> execute(LogicalPlan sparkPlan, Encoder<T> encoder) {
        try {
            // Translate to Calcite RelNode
            RelNode relNode = planTranslator.translate(sparkPlan);

            // Generate PostgreSQL-compatible SQL
            String sql = sqlGenerator.generateSQL(relNode);

            // Log the generated SQL for debugging
            logger.debug("Generated SQL: {}", sql);

            // Execute on DuckDB 1.4.1
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                // Map ResultSet to Spark Rows
                return resultMapper.mapResultSet(rs, sparkPlan.schema(), encoder);
            }

        } catch (Exception e) {
            throw new SparkException("Execution failed", e);
        }
    }

    @Override
    public void createOrReplaceTempView(String viewName, Dataset<?> dataset) {
        try {
            LogicalPlan plan = dataset.getLogicalPlan();
            RelNode relNode = planTranslator.translate(plan);
            String sql = sqlGenerator.generateSQL(relNode);

            // DuckDB 1.4.1 syntax for temporary views
            String createView = String.format(
                "CREATE OR REPLACE TEMPORARY VIEW %s AS %s",
                quoteIdentifier(viewName),
                sql
            );

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createView);
            }
        } catch (SQLException e) {
            throw new SparkException("Failed to create temp view: " + viewName, e);
        }
    }

    @Override
    public Dataset<Row> table(String tableName) {
        // Create a table scan logical plan
        LogicalPlan tableScan = new TableScan(tableName);
        return new Dataset<>(currentSession, tableScan, Encoders.row());
    }

    private String quoteIdentifier(String identifier) {
        // DuckDB uses double quotes for identifiers
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private CalciteContext createCalciteContext() {
        // Create Calcite context with DuckDB schema
        Properties info = new Properties();
        info.setProperty("lex", "POSTGRES");
        info.setProperty("conformance", "LENIENT");

        return new CalciteContext(connection, info);
    }
}
```

#### Step 5.2: Result Set Mapping for Spark 3.5.3
```java
// execution/src/main/java/com/spark2sql/execution/ResultSetMapper.java
package com.spark2sql.execution;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;

public class ResultSetMapper {

    public <T> List<T> mapResultSet(ResultSet rs, StructType schema, Encoder<T> encoder)
            throws SQLException {
        List<T> results = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Object[] values = new Object[columnCount];

            for (int i = 0; i < columnCount; i++) {
                int columnIndex = i + 1; // JDBC is 1-indexed
                values[i] = extractValue(rs, columnIndex, schema.fields()[i]);
            }

            Row row = new GenericRowWithSchema(values, schema);
            T result = encoder.fromRow(row);
            results.add(result);
        }

        return results;
    }

    private Object extractValue(ResultSet rs, int columnIndex, StructField field)
            throws SQLException {

        // Handle null values first
        if (rs.getObject(columnIndex) == null) {
            return null;
        }

        DataType dataType = field.dataType();

        if (dataType instanceof IntegerType) {
            return rs.getInt(columnIndex);
        } else if (dataType instanceof LongType) {
            return rs.getLong(columnIndex);
        } else if (dataType instanceof ShortType) {
            return rs.getShort(columnIndex);
        } else if (dataType instanceof ByteType) {
            return rs.getByte(columnIndex);
        } else if (dataType instanceof FloatType) {
            return rs.getFloat(columnIndex);
        } else if (dataType instanceof DoubleType) {
            return rs.getDouble(columnIndex);
        } else if (dataType instanceof DecimalType) {
            DecimalType dt = (DecimalType) dataType;
            BigDecimal bd = rs.getBigDecimal(columnIndex);
            // Ensure correct precision and scale
            return bd.setScale(dt.scale(), RoundingMode.HALF_UP);
        } else if (dataType instanceof StringType) {
            return rs.getString(columnIndex);
        } else if (dataType instanceof BooleanType) {
            return rs.getBoolean(columnIndex);
        } else if (dataType instanceof DateType) {
            Date sqlDate = rs.getDate(columnIndex);
            // Convert to Spark's internal date representation
            return sqlDate != null ? sqlDate.toLocalDate() : null;
        } else if (dataType instanceof TimestampType) {
            Timestamp ts = rs.getTimestamp(columnIndex);
            // Spark 3.5.3 uses microsecond precision
            return ts != null ? ts.toInstant() : null;
        } else if (dataType instanceof BinaryType) {
            return rs.getBytes(columnIndex);
        } else if (dataType instanceof ArrayType) {
            // DuckDB 1.4.1 array handling
            Array array = rs.getArray(columnIndex);
            if (array != null) {
                Object[] javaArray = (Object[]) array.getArray();
                ArrayType at = (ArrayType) dataType;
                return convertArray(javaArray, at.elementType());
            }
            return null;
        } else if (dataType instanceof MapType) {
            // DuckDB 1.4.1 map handling
            // Maps are returned as JSON in DuckDB, need custom parsing
            String mapJson = rs.getString(columnIndex);
            return parseMap(mapJson, (MapType) dataType);
        } else if (dataType instanceof StructType) {
            // DuckDB 1.4.1 struct handling
            Object struct = rs.getObject(columnIndex);
            return parseStruct(struct, (StructType) dataType);
        } else {
            // For unknown types, try generic object retrieval
            return rs.getObject(columnIndex);
        }
    }

    private Object convertArray(Object[] javaArray, DataType elementType) {
        // Convert DuckDB array to Spark-compatible array
        if (elementType instanceof IntegerType) {
            return Arrays.stream(javaArray).mapToInt(o -> (Integer) o).toArray();
        } else if (elementType instanceof LongType) {
            return Arrays.stream(javaArray).mapToLong(o -> (Long) o).toArray();
        } else if (elementType instanceof DoubleType) {
            return Arrays.stream(javaArray).mapToDouble(o -> (Double) o).toArray();
        } else {
            return javaArray;
        }
    }

    private Object parseMap(String mapJson, MapType mapType) {
        // Parse DuckDB map representation
        // Implementation depends on DuckDB's map output format
        // This is a placeholder
        return new HashMap<>();
    }

    private Object parseStruct(Object struct, StructType structType) {
        // Parse DuckDB struct representation
        // Implementation depends on DuckDB's struct output format
        // This is a placeholder
        return new GenericRowWithSchema(new Object[structType.size()], structType);
    }
}
```

### Phase 6: Testing Framework (Week 8)

#### Step 6.1: Differential Testing with Spark 3.5.3
```java
// testing/src/test/java/com/spark2sql/test/DifferentialTest.java
package com.spark2sql.test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

public class DifferentialTest {
    private SparkSession sparkSession;      // Original Spark 3.5.3
    private SparkSession embeddedSession;   // Our implementation
    private TestDataGenerator dataGenerator;

    @BeforeEach
    public void setup() {
        // Setup original Spark 3.5.3 session (in-memory)
        sparkSession = SparkSession.builder()
            .appName("DifferentialTest-Original")
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

        // Setup our embedded implementation
        embeddedSession = com.spark2sql.SparkSession.builder()
            .appName("DifferentialTest-Embedded")
            .getOrCreate();

        dataGenerator = new TestDataGenerator();
    }

    @Test
    public void testBasicSelect() {
        // Create test data compatible with Spark 3.5.3
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, "Alice", 25, new BigDecimal("1000.50")),
            RowFactory.create(2, "Bob", 30, new BigDecimal("2000.75")),
            RowFactory.create(3, "Charlie", 35, new BigDecimal("3000.00"))
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false),
            DataTypes.createStructField("salary", DataTypes.createDecimalType(10, 2), false)
        ));

        // Execute on original Spark 3.5.3
        Dataset<Row> sparkDf = sparkSession.createDataFrame(testData, schema);
        sparkDf.createOrReplaceTempView("test_table");
        Dataset<Row> sparkResult = sparkDf.select("id", "name", "salary");

        // Execute on our implementation
        Dataset<Row> embeddedDf = embeddedSession.createDataFrame(testData, schema);
        embeddedDf.createOrReplaceTempView("test_table");
        Dataset<Row> embeddedResult = embeddedDf.select("id", "name", "salary");

        // Compare results
        assertDataFramesEqual(sparkResult, embeddedResult);
    }

    @Test
    public void testFilterWithArithmetic() {
        Dataset<Row> sparkDf = createTestDataset(sparkSession);
        Dataset<Row> embeddedDf = createTestDataset(embeddedSession);

        // Test filter with arithmetic (Spark 3.5.3 syntax)
        Dataset<Row> sparkResult = sparkDf
            .filter(functions.col("age").gt(25))
            .select(functions.col("age").plus(10).as("age_plus_10"));

        Dataset<Row> embeddedResult = embeddedDf
            .filter(functions.col("age").gt(25))
            .select(functions.col("age").plus(10).as("age_plus_10"));

        assertDataFramesEqual(sparkResult, embeddedResult);
    }

    @Test
    public void testIntegerDivisionSemantics() {
        // Critical test for Spark 3.5.3 integer division semantics
        List<Row> testData = Arrays.asList(
            RowFactory.create(10, 3),
            RowFactory.create(-10, 3),
            RowFactory.create(10, -3),
            RowFactory.create(-10, -3),
            RowFactory.create(7, 2),
            RowFactory.create(-7, 2)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, false),
            DataTypes.createStructField("b", DataTypes.IntegerType, false)
        ));

        Dataset<Row> sparkDf = sparkSession.createDataFrame(testData, schema);
        Dataset<Row> embeddedDf = embeddedSession.createDataFrame(testData, schema);

        Dataset<Row> sparkResult = sparkDf.select(
            functions.col("a").divide(functions.col("b")).as("result")
        );

        Dataset<Row> embeddedResult = embeddedDf.select(
            functions.col("a").divide(functions.col("b")).as("result")
        );

        // Verify truncation towards zero (Java/Spark semantics)
        List<Row> sparkRows = sparkResult.collectAsList();
        List<Row> embeddedRows = embeddedResult.collectAsList();

        assertEquals(3, sparkRows.get(0).getInt(0));   // 10/3 = 3
        assertEquals(-3, sparkRows.get(1).getInt(0));  // -10/3 = -3 (not -4)
        assertEquals(-3, sparkRows.get(2).getInt(0));  // 10/-3 = -3 (not -4)
        assertEquals(3, sparkRows.get(3).getInt(0));   // -10/-3 = 3
        assertEquals(3, sparkRows.get(4).getInt(0));   // 7/2 = 3
        assertEquals(-3, sparkRows.get(5).getInt(0));  // -7/2 = -3 (not -4)

        assertDataFramesEqual(sparkResult, embeddedResult);
    }

    @Test
    public void testNullSemantics() {
        // Test Spark 3.5.3 null handling
        List<Row> testData = Arrays.asList(
            RowFactory.create(1, null),
            RowFactory.create(2, 10),
            RowFactory.create(null, 20),
            RowFactory.create(null, null)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("a", DataTypes.IntegerType, true),
            DataTypes.createStructField("b", DataTypes.IntegerType, true)
        ));

        Dataset<Row> sparkDf = sparkSession.createDataFrame(testData, schema);
        Dataset<Row> embeddedDf = embeddedSession.createDataFrame(testData, schema);

        // Test null propagation in arithmetic
        Dataset<Row> sparkArith = sparkDf.select(
            functions.col("a").plus(functions.col("b")).as("sum"),
            functions.col("a").equalTo(functions.col("b")).as("equal"),
            functions.when(functions.col("a").isNull(), "NULL_A")
                .otherwise("NOT_NULL_A").as("null_check")
        );

        Dataset<Row> embeddedArith = embeddedDf.select(
            functions.col("a").plus(functions.col("b")).as("sum"),
            functions.col("a").equalTo(functions.col("b")).as("equal"),
            functions.when(functions.col("a").isNull(), "NULL_A")
                .otherwise("NOT_NULL_A").as("null_check")
        );

        assertDataFramesEqual(sparkArith, embeddedArith);
    }

    @Test
    public void testTimestampHandling() {
        // Test Spark 3.5.3 timestamp with microsecond precision
        Timestamp ts1 = Timestamp.valueOf("2024-01-01 10:30:45.123456");
        Timestamp ts2 = Timestamp.valueOf("2024-06-15 14:25:30.987654");

        List<Row> testData = Arrays.asList(
            RowFactory.create(1, ts1),
            RowFactory.create(2, ts2)
        );

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("ts", DataTypes.TimestampType, false)
        ));

        Dataset<Row> sparkDf = sparkSession.createDataFrame(testData, schema);
        Dataset<Row> embeddedDf = embeddedSession.createDataFrame(testData, schema);

        // Test timestamp operations
        Dataset<Row> sparkResult = sparkDf.select(
            functions.year(functions.col("ts")).as("year"),
            functions.month(functions.col("ts")).as("month"),
            functions.dayofmonth(functions.col("ts")).as("day"),
            functions.hour(functions.col("ts")).as("hour")
        );

        Dataset<Row> embeddedResult = embeddedDf.select(
            functions.year(functions.col("ts")).as("year"),
            functions.month(functions.col("ts")).as("month"),
            functions.dayofmonth(functions.col("ts")).as("day"),
            functions.hour(functions.col("ts")).as("hour")
        );

        assertDataFramesEqual(sparkResult, embeddedResult);
    }

    @Test
    public void testComplexAggregations() {
        // Test Spark 3.5.3 aggregation functions
        Dataset<Row> sparkDf = createGroupTestData(sparkSession);
        Dataset<Row> embeddedDf = createGroupTestData(embeddedSession);

        Dataset<Row> sparkAgg = sparkDf.groupBy("category")
            .agg(
                functions.sum("value").as("total"),
                functions.avg("value").as("average"),
                functions.count("*").as("count"),
                functions.max("value").as("maximum"),
                functions.min("value").as("minimum"),
                functions.stddev("value").as("std_dev"),
                functions.collect_list("value").as("values_list")
            );

        Dataset<Row> embeddedAgg = embeddedDf.groupBy("category")
            .agg(
                functions.sum("value").as("total"),
                functions.avg("value").as("average"),
                functions.count("*").as("count"),
                functions.max("value").as("maximum"),
                functions.min("value").as("minimum"),
                functions.stddev("value").as("std_dev"),
                functions.collect_list("value").as("values_list")
            );

        // Compare results (excluding array columns for simplicity)
        Dataset<Row> sparkSimple = sparkAgg.drop("values_list");
        Dataset<Row> embeddedSimple = embeddedAgg.drop("values_list");

        assertDataFramesEqual(sparkSimple, embeddedSimple);
    }

    private void assertDataFramesEqual(Dataset<Row> expected, Dataset<Row> actual) {
        // Compare schemas
        assertEquals(expected.schema(), actual.schema(), "Schemas don't match");

        // Compare row counts
        long expectedCount = expected.count();
        long actualCount = actual.count();
        assertEquals(expectedCount, actualCount, "Row counts don't match");

        // Compare data
        List<Row> expectedRows = expected.collectAsList();
        List<Row> actualRows = actual.collectAsList();

        // Sort for comparison (order might differ)
        expectedRows.sort(new RowComparator());
        actualRows.sort(new RowComparator());

        for (int i = 0; i < expectedRows.size(); i++) {
            Row expectedRow = expectedRows.get(i);
            Row actualRow = actualRows.get(i);

            for (int j = 0; j < expectedRow.length(); j++) {
                Object expectedValue = expectedRow.get(j);
                Object actualValue = actualRow.get(j);

                if (expectedValue instanceof Double || expectedValue instanceof Float) {
                    // Use tolerance for floating point comparison
                    double exp = ((Number) expectedValue).doubleValue();
                    double act = ((Number) actualValue).doubleValue();
                    assertEquals(exp, act, 1e-9,
                        String.format("Values differ at row %d, col %d", i, j));
                } else if (expectedValue instanceof BigDecimal && actualValue instanceof BigDecimal) {
                    // Compare BigDecimal with scale consideration
                    assertEquals(0, ((BigDecimal) expectedValue).compareTo((BigDecimal) actualValue),
                        String.format("Decimal values differ at row %d, col %d", i, j));
                } else {
                    assertEquals(expectedValue, actualValue,
                        String.format("Values differ at row %d, col %d", i, j));
                }
            }
        }
    }

    @AfterEach
    public void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
        if (embeddedSession != null) {
            embeddedSession.close();
        }
    }
}
```

## Performance Benchmarks

### Benchmark Suite with JMH
```java
// testing/src/test/java/com/spark2sql/test/PerformanceBenchmark.java
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class PerformanceBenchmark {

    private SparkSession originalSpark;
    private SparkSession embeddedSpark;
    private List<Row> testData;

    @Setup
    public void setup() {
        // Setup Spark 3.5.3
        originalSpark = SparkSession.builder()
            .appName("Benchmark-Original")
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();

        // Setup embedded implementation
        embeddedSpark = com.spark2sql.SparkSession.builder()
            .appName("Benchmark-Embedded")
            .getOrCreate();

        // Create large test dataset
        testData = generateLargeDataset(100_000);
    }

    @Benchmark
    public long benchmarkSimpleFilterOriginal() {
        Dataset<Row> df = originalSpark.createDataFrame(testData, schema);
        return df.filter(functions.col("value").gt(500)).count();
    }

    @Benchmark
    public long benchmarkSimpleFilterEmbedded() {
        Dataset<Row> df = embeddedSpark.createDataFrame(testData, schema);
        return df.filter(functions.col("value").gt(500)).count();
    }

    @Benchmark
    public long benchmarkComplexAggregationOriginal() {
        Dataset<Row> df = originalSpark.createDataFrame(testData, schema);
        return df.groupBy("category")
            .agg(
                functions.sum("value").as("total"),
                functions.avg("value").as("average"),
                functions.stddev("value").as("std_deviation")
            )
            .count();
    }

    @Benchmark
    public long benchmarkComplexAggregationEmbedded() {
        Dataset<Row> df = embeddedSpark.createDataFrame(testData, schema);
        return df.groupBy("category")
            .agg(
                functions.sum("value").as("total"),
                functions.avg("value").as("average"),
                functions.stddev("value").as("std_deviation")
            )
            .count();
    }

    @TearDown
    public void tearDown() {
        originalSpark.stop();
        embeddedSpark.close();
    }
}
```

## Deliverables Checklist

### Phase 1 Deliverables
- [ ] Maven project structure with modules
- [ ] Basic SparkSession implementation (Spark 3.5.3 compatible)
- [ ] Dataset and DataFrame interfaces
- [ ] Column expression API
- [ ] Simple show() and collect() operations

### Phase 2 Deliverables
- [ ] LogicalPlan hierarchy
- [ ] Plan nodes for all operations
- [ ] Expression tree structure
- [ ] Plan visitor pattern

### Phase 3 Deliverables
- [ ] PlanTranslator implementation (Calcite 1.37.0)
- [ ] ExpressionTranslator
- [ ] TypeMapper with all Spark 3.5.3 types
- [ ] FunctionRegistry

### Phase 4 Deliverables
- [ ] PostgreSQL SQL generator
- [ ] Numerical semantics handler
- [ ] Optimization rules (Calcite 1.37.0)

### Phase 5 Deliverables
- [ ] DuckDB 1.4.1 executor
- [ ] ResultSet mapper
- [ ] UDF registration

### Phase 6 Deliverables
- [ ] Differential testing framework
- [ ] Property-based tests
- [ ] Semantic validation tests
- [ ] Performance benchmarks

### Phase 7 Deliverables
- [ ] Complete integration
- [ ] Documentation
- [ ] Performance report
- [ ] Known limitations document

## Success Metrics

1. **Functional Correctness**
   - 100% pass rate on differential tests against Spark 3.5.3
   - All numerical semantics tests pass
   - Null handling matches Spark exactly

2. **Performance**
   - < 10ms overhead for query translation
   - Competitive with Spark 3.5.3 for small datasets (< 1M rows)
   - Linear scaling for supported operations

3. **Coverage**
   - Core DataFrame operations implemented
   - 80% of common Spark 3.5.3 functions supported
   - Clear documentation of unsupported features

## Conclusion

This implementation plan provides a concrete path to building an embedded Spark DataFrame API replacement that:
- Maintains drop-in compatibility with Spark 3.5.3
- Uses latest Apache Calcite (1.37.0) for optimal query planning
- Executes efficiently on DuckDB 1.4.1
- Preserves Spark's numerical and null semantics exactly
- Validates correctness through comprehensive differential testing

The modular architecture ensures clean separation of concerns and allows for incremental development and testing of each layer independently.