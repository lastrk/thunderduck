# Java 21 Refactoring Proposal for Thunderduck Core Translation Engine

**Date**: 2026-02-08
**Status**: Proposal (Research)
**Current Java Version**: 17 (via `maven.compiler.release=17`)
**Target Java Version**: 21 (LTS)

---

## Overarching Principle

> **Every Spark plan node and expression variant is a sealed type whose translation to DuckDB SQL is exhaustively dispatched via pattern matching, making illegal states unrepresentable and missing cases a compile-time error.**

---

## 1. Current Architecture Overview

### 1.1 Translation Pipeline

The Thunderduck translation pipeline has four stages:

```
Spark Connect Protobuf ──> Logical Plan Tree ──> SQL String ──> DuckDB Execution
      (gRPC)                 (Java objects)      (StringBuilder)   (JDBC)
```

**Stage 1: Protobuf Deserialization** (`connect-server/`)
- `PlanConverter` (`/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/PlanConverter.java`) -- thin facade
- `RelationConverter` (2,290 lines, `/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java`) -- converts protobuf `Relation` nodes to `LogicalPlan` objects via a `switch` on `relation.getRelTypeCase()` (line 89, ~30 cases)
- `ExpressionConverter` (1,916 lines, `/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`) -- converts protobuf `Expression` to `Expression` objects via a `switch` on `expr.getExprTypeCase()` (line 52, ~15 cases) plus extensive string-matched dispatch for function names

**Stage 2: Logical Plan Tree** (`core/` -- `logical/` and `expression/` packages)
- `LogicalPlan` (`/workspace/core/src/main/java/com/thunderduck/logical/LogicalPlan.java`) -- abstract base class, 20 concrete subclasses
- `Expression` (`/workspace/core/src/main/java/com/thunderduck/expression/Expression.java`) -- abstract base class, 24+ concrete subclasses
- `DataType` (`/workspace/core/src/main/java/com/thunderduck/types/DataType.java`) -- abstract base class, 14 concrete subclasses

**Stage 3: SQL Generation** (`core/` -- `generator/` package)
- `SQLGenerator` (2,659 lines, `/workspace/core/src/main/java/com/thunderduck/generator/SQLGenerator.java`) -- visitor-style dispatch via `instanceof` chain (lines 142-188), 20 `visitXxx()` methods
- `FunctionRegistry` (1,237 lines, `/workspace/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`) -- string-keyed `HashMap` dispatch for 200+ Spark-to-DuckDB function mappings
- Each `Expression` subclass has its own `toSQL()` method for self-rendering

**Stage 4: Execution** (`core/` -- `runtime/` package)
- `DuckDBRuntime`, `QueryExecutor`, `ArrowBatchStream`, etc.

### 1.2 Class Hierarchies

**LogicalPlan hierarchy** (20 subclasses):
```
LogicalPlan (abstract)
├── Project          ├── Aggregate       ├── Join
├── Filter           ├── Sort            ├── Union
├── Limit            ├── Distinct        ├── Intersect
├── Except           ├── TableScan       ├── InMemoryRelation
├── LocalRelation    ├── LocalDataRelation├── SQLRelation
├── AliasedRelation  ├── RangeRelation   ├── Tail
├── Sample           ├── WithColumns     ├── ToDF
└── SingleRowRelation
```

**Expression hierarchy** (24+ subclasses):
```
Expression (abstract)
├── Literal              ├── ColumnReference      ├── UnresolvedColumn
├── BinaryExpression     ├── UnaryExpression      ├── FunctionCall
├── CastExpression       ├── AliasExpression      ├── CaseWhenExpression
├── WindowFunction       ├── InExpression         ├── InSubquery
├── ExistsSubquery       ├── ScalarSubquery       ├── SubqueryExpression
├── StarExpression       ├── RegexColumnExpression├── IntervalExpression
├── LambdaExpression     ├── LambdaVariableExpression
├── ArrayLiteralExpression├── MapLiteralExpression ├── StructLiteralExpression
├── ExtractValueExpression├── UpdateFieldsExpression
└── RawSQLExpression
```

**DataType hierarchy** (14 subclasses):
```
DataType (abstract)
├── BooleanType    ├── ByteType       ├── ShortType
├── IntegerType    ├── LongType       ├── FloatType
├── DoubleType     ├── DecimalType    ├── StringType
├── DateType       ├── TimestampType  ├── BinaryType
├── ArrayType      ├── MapType        ├── StructType
└── UnresolvedType
```

---

## 2. Pain Points

### 2.1 The 47-branch `instanceof` Chain in SQLGenerator

**File**: `/workspace/core/src/main/java/com/thunderduck/generator/SQLGenerator.java`, lines 139-188

```java
private void visit(LogicalPlan plan) {
    Objects.requireNonNull(plan, "plan must not be null");

    if (plan instanceof Project) {
        visitProject((Project) plan);
    } else if (plan instanceof Filter) {
        visitFilter((Filter) plan);
    } else if (plan instanceof TableScan) {
        visitTableScan((TableScan) plan);
    } else if (plan instanceof Sort) {
        visitSort((Sort) plan);
    } else if (plan instanceof Limit) {
        visitLimit((Limit) plan);
    } else if (plan instanceof Aggregate) {
        visitAggregate((Aggregate) plan);
    } else if (plan instanceof Join) {
        visitJoin((Join) plan);
    } else if (plan instanceof Union) {
        visitUnion((Union) plan);
    // ... 12 more cases ...
    } else {
        throw new UnsupportedOperationException(
            "SQL generation not implemented for: " + plan.getClass().getSimpleName());
    }
}
```

**Problems**:
1. Adding a new `LogicalPlan` subclass silently falls through to the `else` branch at runtime -- no compile-time safety.
2. The order of `instanceof` checks matters for correctness (e.g., `Filter` before `Join` matters for the `child instanceof Filter` check inside `visitProject`).
3. Each check does an implicit null-check, cast, and branch -- pattern matching would do this in one step.

### 2.2 The `qualifyCondition()` Method -- 100-line `instanceof` Chain

**File**: `/workspace/core/src/main/java/com/thunderduck/generator/SQLGenerator.java`, lines 1686-1785

```java
public String qualifyCondition(Expression expr, Map<Long, String> planIdToAlias) {
    if (expr instanceof UnresolvedColumn) {
        UnresolvedColumn col = (UnresolvedColumn) expr;
        // ...
    }
    if (expr instanceof BinaryExpression) {
        BinaryExpression binExpr = (BinaryExpression) expr;
        // ...
    }
    if (expr instanceof com.thunderduck.expression.UnaryExpression) {
        com.thunderduck.expression.UnaryExpression unaryExpr =
            (com.thunderduck.expression.UnaryExpression) expr;
        // ...
    }
    if (expr instanceof com.thunderduck.expression.AliasExpression) { ... }
    if (expr instanceof com.thunderduck.expression.CastExpression) { ... }
    if (expr instanceof com.thunderduck.expression.FunctionCall) { ... }
    if (expr instanceof com.thunderduck.expression.InExpression) { ... }
    // Fall through to expr.toSQL()
    return expr.toSQL();
}
```

**Problems**:
1. Not exhaustive -- new expression types silently fall through to `expr.toSQL()`, which may produce wrong results.
2. The casting is verbose and repeated -- every branch does `(Type) expr` after `instanceof Type`.
3. This pattern is duplicated in `transformAggregateExpression()` (lines 1153-1192) with a similar structure.

### 2.3 The `transformAggregateExpression()` -- Duplicated AST Walk

**File**: `/workspace/core/src/main/java/com/thunderduck/generator/SQLGenerator.java`, lines 1153-1192

```java
public static Expression transformAggregateExpression(Expression expr) {
    if (expr instanceof FunctionCall) {
        FunctionCall func = (FunctionCall) expr;
        String name = func.functionName().toLowerCase();
        if (name.equals("sum") || name.equals("sum_distinct")) {
            return new FunctionCall("spark_sum", func.arguments(), func.dataType(), func.nullable());
        }
        // ...
    }
    if (expr instanceof BinaryExpression) {
        BinaryExpression bin = (BinaryExpression) expr;
        Expression newLeft = transformAggregateExpression(bin.left());
        Expression newRight = transformAggregateExpression(bin.right());
        if (newLeft != bin.left() || newRight != bin.right()) {
            return new BinaryExpression(newLeft, bin.operator(), newRight);
        }
        return expr;
    }
    if (expr instanceof CastExpression) { ... }
    if (expr instanceof UnaryExpression) { ... }
    return expr;
}
```

**Problem**: This is a generic AST transformation that manually reconstructs nodes. With records, you get `with` patterns (or simple copy constructors) for free. With sealed types, the compiler ensures exhaustive handling.

### 2.4 TypeInferenceEngine -- Massive `instanceof` Dispatch

**File**: `/workspace/core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java`

The `resolveType()` method (lines 337-396) has 11 `instanceof` checks:

```java
public static DataType resolveType(Expression expr, StructType schema) {
    if (expr instanceof AliasExpression) { ... }
    if (expr instanceof UnresolvedColumn) { ... }
    if (expr instanceof WindowFunction) { ... }
    if (expr instanceof BinaryExpression) { ... }
    if (expr instanceof FunctionCall) { ... }
    if (expr instanceof CaseWhenExpression) { ... }
    if (expr instanceof ArrayLiteralExpression) { ... }
    if (expr instanceof MapLiteralExpression) { ... }
    if (expr instanceof StructLiteralExpression) { ... }
    if (expr instanceof InExpression) { ... }
    return expr.dataType();
}
```

The `resolveNullable()` method (lines 989-1031) has a parallel structure with 7 `instanceof` checks.

The `promoteNumericTypes()` method (lines 115-150) has 7 `instanceof` checks on `DataType`:
```java
if (left instanceof DoubleType || right instanceof DoubleType) return DoubleType.get();
if (left instanceof FloatType || right instanceof FloatType) return FloatType.get();
if (left instanceof DecimalType || right instanceof DecimalType) { ... }
if (left instanceof LongType || right instanceof LongType) return LongType.get();
// etc.
```

The `isNumericType()` method (lines 736-744) has 7 `instanceof` checks:
```java
private static boolean isNumericType(DataType type) {
    return type instanceof IntegerType ||
           type instanceof LongType ||
           type instanceof ShortType ||
           type instanceof ByteType ||
           type instanceof FloatType ||
           type instanceof DoubleType ||
           type instanceof DecimalType;
}
```

### 2.5 TypeMapper -- `instanceof` Chain + String Switch Duplication

**File**: `/workspace/core/src/main/java/com/thunderduck/types/TypeMapper.java`

`toDuckDBType()` (lines 63-99) uses a `HashMap<Class<? extends DataType>, String>` for primitives but falls into `instanceof` chains for complex types:

```java
if (sparkType instanceof DecimalType) {
    DecimalType decimal = (DecimalType) sparkType;
    return String.format("DECIMAL(%d,%d)", decimal.precision(), decimal.scale());
}
if (sparkType instanceof ArrayType) { ... }
if (sparkType instanceof MapType) { ... }
```

`toSparkType()` (lines 118-181) uses a `switch` on normalized string but can't leverage the type system.

### 2.6 Literal.toSQL() -- `instanceof` Chain on DataType

**File**: `/workspace/core/src/main/java/com/thunderduck/expression/Literal.java`, lines 76-109

```java
public String toSQL() {
    if (value == null) return "NULL";
    if (dataType instanceof StringType) { ... }
    if (dataType instanceof BooleanType) { ... }
    if (dataType instanceof DateType) { ... }
    if (dataType instanceof TimestampType) {
        if (value instanceof java.time.Instant) {
            java.time.Instant instant = (java.time.Instant) value;
            // ...
        }
        return "TIMESTAMP '" + value + "'";
    }
    return value.toString();
}
```

### 2.7 WindowFunction.dataType() -- String-Matched Switch

**File**: `/workspace/core/src/main/java/com/thunderduck/expression/WindowFunction.java`, lines 185-255

```java
public DataType dataType() {
    String funcUpper = function.toUpperCase();
    switch (funcUpper) {
        case "ROW_NUMBER":
        case "RANK":
        case "DENSE_RANK":
        case "NTILE":
            return IntegerType.get();
        case "COUNT":
            return LongType.get();
        case "LAG":
        case "LEAD":
        // ... 50+ more lines of case matching
    }
}
```

This same string-based dispatch is duplicated in `nullable()` (lines 258-283) and `wrapWithCastIfRankingFunction()` (lines 448-457), and then again in `TypeInferenceEngine.resolveWindowFunctionType()` (lines 417-493).

### 2.8 FunctionRegistry -- Three Parallel Dispatch Systems

**File**: `/workspace/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`

The registry maintains three separate `HashMap`s:
1. `DIRECT_MAPPINGS` (line 29): `Map<String, String>` -- 100+ entries
2. `CUSTOM_TRANSLATORS` (line 30): `Map<String, FunctionTranslator>` -- 40+ entries
3. `FUNCTION_METADATA` (line 31): `Map<String, FunctionMetadata>` -- 40+ entries

These are populated in separate `initializeXxx()` methods but represent the same function in three different registries. For example, `locate` appears in both `CUSTOM_TRANSLATORS` (line 394) and `FUNCTION_METADATA` (line 1046) with duplicated translation logic.

### 2.9 Expression Subclasses as Boilerplate-Heavy Classes

Every `Expression` subclass follows the same pattern:
- Private final fields
- Constructor with validation
- Accessor methods
- `dataType()`, `nullable()`, `toSQL()`
- `equals()`, `hashCode()`, `toString()`

For example, `CastExpression` (88 lines) and `AliasExpression` (89 lines) are essentially:
```
record CastExpression(Expression expression, DataType targetType) { ... }
record AliasExpression(Expression expression, String alias) { ... }
```

### 2.10 ExpressionConverter -- Regex-Based Function Return Type Inference

**File**: `/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`, lines 310-500

The `inferFunctionReturnType()` method uses regex matching on function names:
```java
if (lower.matches("sqrt|log|ln|log10|log2|exp|expm1|...")) {
    return DoubleType.get();
}
if (lower.matches("ceil|ceiling|floor")) {
    return LongType.get();
}
```

This is fragile, hard to maintain, and duplicates logic that also exists in `FunctionMetadata`, `FunctionCategories`, and `TypeInferenceEngine`.

---

## 3. Java 21 Opportunities

### 3.1 Sealed Interfaces + Pattern Matching for LogicalPlan

**Current** (lines 139-188 of SQLGenerator):
```java
private void visit(LogicalPlan plan) {
    if (plan instanceof Project) {
        visitProject((Project) plan);
    } else if (plan instanceof Filter) {
        visitFilter((Filter) plan);
    } else if (plan instanceof TableScan) {
        visitTableScan((TableScan) plan);
    }
    // ... 17 more branches
    else {
        throw new UnsupportedOperationException(...);
    }
}
```

**Proposed**:
```java
public sealed interface LogicalPlan
    permits Project, Filter, TableScan, Sort, Limit, Aggregate,
            Join, Union, Intersect, Except, InMemoryRelation,
            LocalRelation, LocalDataRelation, SQLRelation,
            AliasedRelation, Distinct, RangeRelation, Tail,
            Sample, WithColumns, ToDF, SingleRowRelation {

    StructType schema();
    List<LogicalPlan> children();
    OptionalLong planId();
    // No toSQL() -- SQL generation is in the generator
}
```

```java
// In SQLGenerator:
private void visit(LogicalPlan plan) {
    switch (plan) {
        case Project p       -> visitProject(p);
        case Filter f        -> visitFilter(f);
        case TableScan t     -> visitTableScan(t);
        case Sort s          -> visitSort(s);
        case Limit l         -> visitLimit(l);
        case Aggregate a     -> visitAggregate(a);
        case Join j          -> visitJoin(j);
        case Union u         -> visitUnion(u);
        case Intersect i     -> visitIntersect(i);
        case Except e        -> visitExcept(e);
        case InMemoryRelation m -> visitInMemoryRelation(m);
        case LocalRelation l -> visitLocalRelation(l);
        case LocalDataRelation l -> visitLocalDataRelation(l);
        case SQLRelation s   -> visitSQLRelation(s);
        case AliasedRelation a -> visitAliasedRelation(a);
        case Distinct d      -> visitDistinct(d);
        case RangeRelation r -> visitRangeRelation(r);
        case Tail t          -> visitTail(t);
        case Sample s        -> visitSample(s);
        case WithColumns w   -> visitWithColumns(w);
        case ToDF t          -> visitToDF(t);
        case SingleRowRelation s -> visitSingleRowRelation(s);
        // No default needed -- compiler enforces exhaustiveness
    }
}
```

**Impact**: Compile-time exhaustiveness checking. Adding a new `LogicalPlan` subclass forces updating ALL switch expressions across the codebase. No more silent runtime failures.

### 3.2 Sealed Interfaces + Pattern Matching for Expression

**Current** (`qualifyCondition`, lines 1686-1785; `resolveType`, lines 337-396; `transformAggregateExpression`, lines 1153-1192):

Multiple 7-11 branch `instanceof` chains that all dispatch on the same `Expression` type.

**Proposed**:
```java
public sealed interface Expression
    permits Literal, ColumnReference, UnresolvedColumn,
            BinaryExpr, UnaryExpr, FunctionCall,
            CastExpr, AliasExpr, CaseWhenExpr,
            WindowFunction, InExpr, InSubquery,
            ExistsSubquery, ScalarSubquery, SubqueryExpr,
            StarExpr, RegexColumnExpr, IntervalExpr,
            LambdaExpr, LambdaVariableExpr,
            ArrayLiteralExpr, MapLiteralExpr, StructLiteralExpr,
            ExtractValueExpr, UpdateFieldsExpr,
            RawSQLExpr {

    DataType dataType();
    boolean nullable();
    String toSQL();
}
```

```java
// In TypeInferenceEngine.resolveType():
public static DataType resolveType(Expression expr, StructType schema) {
    return switch (expr) {
        case AliasExpr a             -> resolveType(a.expression(), schema);
        case UnresolvedColumn col    -> resolveColumnType(col, schema);
        case WindowFunction wf       -> resolveWindowFunctionType(wf, schema);
        case BinaryExpr bin          -> resolveBinaryExpressionType(bin, schema);
        case FunctionCall func       -> resolveFunctionCallType(func, schema);
        case CaseWhenExpr cw         -> resolveCaseWhenType(cw, schema);
        case ArrayLiteralExpr arr    -> resolveArrayLiteralType(arr, schema);
        case MapLiteralExpr map      -> resolveMapLiteralType(map, schema);
        case StructLiteralExpr st    -> resolveStructLiteralType(st, schema);
        case InExpr _               -> BooleanType.get();
        case Literal _,
             ColumnReference _,
             CastExpr _,
             UnaryExpr _,
             StarExpr _,
             // ... other leaf types
             RawSQLExpr _            -> expr.dataType();
    };
}
```

### 3.3 Sealed Interfaces + Pattern Matching for DataType

**Current** (`promoteNumericTypes`, lines 115-150; `isNumericType`, lines 736-744; `Literal.toSQL()`, lines 76-109):

```java
// TypeInferenceEngine
if (left instanceof DoubleType || right instanceof DoubleType) return DoubleType.get();
if (left instanceof FloatType || right instanceof FloatType) return FloatType.get();
// ...
```

**Proposed**:
```java
public sealed interface DataType
    permits BooleanType, ByteType, ShortType, IntegerType,
            LongType, FloatType, DoubleType, DecimalType,
            StringType, DateType, TimestampType, BinaryType,
            ArrayType, MapType, StructType, UnresolvedType {

    String typeName();
    int defaultSize();
}

// Singleton types become records:
public record BooleanType() implements DataType {
    public static final BooleanType INSTANCE = new BooleanType();
    @Override public String typeName() { return "boolean"; }
    @Override public int defaultSize() { return 1; }
}
```

```java
// TypeMapper.toDuckDBType becomes:
public static String toDuckDBType(DataType type) {
    return switch (type) {
        case BooleanType _   -> "BOOLEAN";
        case ByteType _      -> "TINYINT";
        case ShortType _     -> "SMALLINT";
        case IntegerType _   -> "INTEGER";
        case LongType _      -> "BIGINT";
        case FloatType _     -> "FLOAT";
        case DoubleType _    -> "DOUBLE";
        case StringType _    -> "VARCHAR";
        case DateType _      -> "DATE";
        case TimestampType _ -> "TIMESTAMP";
        case BinaryType _    -> "BLOB";
        case DecimalType d   -> "DECIMAL(%d,%d)".formatted(d.precision(), d.scale());
        case ArrayType a     -> toDuckDBType(a.elementType()) + "[]";
        case MapType m       -> "MAP(%s, %s)".formatted(
                                    toDuckDBType(m.keyType()), toDuckDBType(m.valueType()));
        case StructType _    -> throw new UnsupportedOperationException("StructType as column type");
        case UnresolvedType _ -> "VARCHAR";  // fallback
    };
}
```

```java
// Literal.toSQL becomes:
public String toSQL() {
    if (value == null) return "NULL";
    return switch (dataType) {
        case StringType _    -> "'" + value.toString().replace("'", "''") + "'";
        case BooleanType _   -> value.toString().toUpperCase();
        case DateType _      -> "DATE '" + value + "'";
        case TimestampType _ -> formatTimestamp(value);
        default              -> value.toString();
    };
}
```

```java
// isNumericType becomes a sealed interface method or:
private static boolean isNumericType(DataType type) {
    return switch (type) {
        case IntegerType _, LongType _, ShortType _, ByteType _,
             FloatType _, DoubleType _, DecimalType _ -> true;
        default -> false;
    };
}
```

### 3.4 Records for Immutable Data Carriers

Many expression and plan classes are pure data carriers where `equals()`, `hashCode()`, `toString()`, and accessor methods are boilerplate.

**Current `CastExpression`** (88 lines, `/workspace/core/src/main/java/com/thunderduck/expression/CastExpression.java`):
```java
public class CastExpression extends Expression {
    private final Expression expression;
    private final DataType targetType;

    public CastExpression(Expression expression, DataType targetType) {
        this.expression = Objects.requireNonNull(expression, "expression must not be null");
        this.targetType = Objects.requireNonNull(targetType, "targetType must not be null");
    }

    public Expression expression() { return expression; }
    public DataType targetType() { return targetType; }

    @Override public DataType dataType() { return targetType; }
    @Override public boolean nullable() { return expression.nullable(); }
    @Override public String toSQL() {
        return String.format("CAST(%s AS %s)", expression, targetType.typeName());
    }
    @Override public boolean equals(Object obj) { ... }  // 5 lines
    @Override public int hashCode() { ... }               // 3 lines
}
```

**Proposed** (~12 lines):
```java
public record CastExpr(Expression expression, DataType targetType) implements Expression {
    public CastExpr {
        Objects.requireNonNull(expression, "expression must not be null");
        Objects.requireNonNull(targetType, "targetType must not be null");
    }

    @Override public DataType dataType() { return targetType; }
    @Override public boolean nullable() { return expression.nullable(); }
    @Override public String toSQL() {
        return "CAST(%s AS %s)".formatted(expression.toSQL(), targetType.typeName());
    }
}
```

**Candidates for records** (with line savings estimate):

| Current Class | Lines | As Record | Savings |
|---|---|---|---|
| `CastExpression` | 88 | ~12 | 76 |
| `AliasExpression` | 89 | ~10 | 79 |
| `ColumnReference` | 180 | ~20 | 160 |
| `UnaryExpression` | 194 | ~25 | 169 |
| `Literal` | 201 | ~30 | 171 |
| `InExpression` | ~100 | ~15 | 85 |
| `StarExpression` | ~30 | ~5 | 25 |
| `ScalarSubquery` | ~60 | ~10 | 50 |
| `JoinPart` (inner class) | 10 | 1 | 9 |
| `Sort.SortOrder` | ~30 | ~5 | 25 |
| `StructField` | ~60 | ~10 | 50 |
| `DecimalType` | 86 | ~15 | 71 |
| `ArrayType` | ~60 | ~10 | 50 |
| `MapType` | ~70 | ~12 | 58 |

**Total estimated savings**: ~1,000+ lines of boilerplate eliminated.

**Note on BinaryExpression and FunctionCall**: These have significant logic in `dataType()` and `toSQL()`, so they would remain full classes (sealed interface permits) rather than simple records.

### 3.5 Records for the SQLGenerator JoinPart Inner Class

**Current** (lines 515-525 of SQLGenerator):
```java
private static class JoinPart {
    final LogicalPlan right;
    final Join.JoinType joinType;
    final Expression condition;

    JoinPart(LogicalPlan right, Join.JoinType joinType, Expression condition) {
        this.right = right;
        this.joinType = joinType;
        this.condition = condition;
    }
}
```

**Proposed**:
```java
private record JoinPart(LogicalPlan right, Join.JoinType joinType, Expression condition) {}
```

### 3.6 Text Blocks for SQL Templates

Several methods build SQL using repeated `sql.append()` calls that are hard to read.

**Current** (`visitRangeRelation`, lines 745-755):
```java
private void visitRangeRelation(RangeRelation plan) {
    sql.append("SELECT range AS \"id\" FROM range(")
       .append(plan.start())
       .append(", ")
       .append(plan.end())
       .append(", ")
       .append(plan.step())
       .append(")");
}
```

**Proposed**:
```java
private void visitRangeRelation(RangeRelation plan) {
    sql.append("SELECT range AS \"id\" FROM range(%d, %d, %d)"
        .formatted(plan.start(), plan.end(), plan.step()));
}
```

**Current** (`visitSample`, lines 896-914):
```java
private void visitSample(Sample plan) {
    sql.append("SELECT * FROM (");
    subqueryDepth++;
    visit(plan.child());
    subqueryDepth--;
    sql.append(") AS ").append(generateSubqueryAlias());
    double percentage = plan.fraction() * 100.0;
    if (plan.seed().isPresent()) {
        sql.append(String.format(" USING SAMPLE %.6f%% (bernoulli, %d)",
            percentage, plan.seed().getAsLong()));
    } else {
        sql.append(String.format(" USING SAMPLE %.6f%% (bernoulli)",
            percentage));
    }
}
```

The `String.format()` calls here already use formatting; `formatted()` is the instance-method version available since Java 15 and more idiomatic.

### 3.7 Enhanced Switch Expressions for Join Type and Table Format

**Current** (`getJoinKeyword`, lines 530-541):
```java
private String getJoinKeyword(Join.JoinType joinType) {
    switch (joinType) {
        case INNER: return "INNER JOIN";
        case LEFT: return "LEFT OUTER JOIN";
        case RIGHT: return "RIGHT OUTER JOIN";
        case FULL: return "FULL OUTER JOIN";
        case CROSS: return "CROSS JOIN";
        case LEFT_SEMI: return "SEMI JOIN";
        case LEFT_ANTI: return "ANTI JOIN";
        default: throw new UnsupportedOperationException("Unknown join type: " + joinType);
    }
}
```

**Proposed**:
```java
private String getJoinKeyword(Join.JoinType joinType) {
    return switch (joinType) {
        case INNER     -> "INNER JOIN";
        case LEFT      -> "LEFT OUTER JOIN";
        case RIGHT     -> "RIGHT OUTER JOIN";
        case FULL      -> "FULL OUTER JOIN";
        case CROSS     -> "CROSS JOIN";
        case LEFT_SEMI -> "SEMI JOIN";
        case LEFT_ANTI -> "ANTI JOIN";
    };
}
```

**Note**: This is already available in Java 17 switch expressions, but sealed types make the exhaustiveness compiler-checked.

**Current** (`visitTableScan`, lines 760-796):
```java
switch (format) {
    case TABLE:
        sql.append("SELECT * FROM ");
        sql.append(quoteIdentifier(source));
        break;
    case PARQUET:
        sql.append("SELECT * FROM read_parquet(");
        sql.append(quoteFilePath(source));
        sql.append(")");
        break;
    // ...
}
```

**Proposed**:
```java
String sourceSQL = switch (format) {
    case TABLE   -> quoteIdentifier(source);
    case PARQUET -> "read_parquet(%s)".formatted(quoteFilePath(source));
    case DELTA   -> "delta_scan(%s)".formatted(quoteFilePath(source));
    case ICEBERG -> "iceberg_scan(%s)".formatted(quoteFilePath(source));
};
sql.append("SELECT * FROM ").append(sourceSQL);
```

### 3.8 FunctionRegistry Unification with Sealed Types

The current three-map design (`DIRECT_MAPPINGS`, `CUSTOM_TRANSLATORS`, `FUNCTION_METADATA`) leads to inconsistencies. A function may be in one map but not another.

**Proposed**: Replace the three maps with a single registry of `FunctionSpec` sealed types:

```java
public sealed interface FunctionSpec {

    record DirectMapping(String sparkName, String duckdbName) implements FunctionSpec {}

    record CustomTranslation(String sparkName, FunctionTranslator translator) implements FunctionSpec {}

    record FullSpec(
        String sparkName,
        String duckdbName,
        FunctionTranslator translator,   // nullable for direct mappings
        FunctionMetadata.ReturnTypeResolver returnType,
        FunctionMetadata.NullableResolver nullable
    ) implements FunctionSpec {}
}
```

```java
// Registration becomes:
private static final Map<String, FunctionSpec> FUNCTIONS = new HashMap<>();
static {
    register(new DirectMapping("upper", "upper"));
    register(new CustomTranslation("locate", args -> { /* ... */ }));
    register(new FullSpec("year", "year",
        args -> "CAST(year(" + args[0] + ") AS INTEGER)",
        FunctionMetadata.constantType(IntegerType.get()),
        FunctionMetadata.anyArgNullable()));
}
```

This eliminates the problem where `locate` has duplicated translation logic in both `CUSTOM_TRANSLATORS` and `FUNCTION_METADATA`.

### 3.9 Window Function Enum Instead of String Dispatch

**Current**: `WindowFunction.dataType()` switches on `function.toUpperCase()` string (lines 188-255). This is duplicated in `TypeInferenceEngine.resolveWindowFunctionType()` (lines 417-493).

**Proposed**:
```java
public enum WindowFunctionKind {
    // Ranking
    ROW_NUMBER(IntegerType.get(), false),
    RANK(IntegerType.get(), false),
    DENSE_RANK(IntegerType.get(), false),
    NTILE(IntegerType.get(), false),

    // Distribution
    PERCENT_RANK(DoubleType.get(), false),
    CUME_DIST(DoubleType.get(), false),

    // Aggregate (type depends on argument)
    COUNT(LongType.get(), false),
    SUM(null, true),   // resolved from argument
    AVG(null, true),   // resolved from argument
    MIN(null, true),
    MAX(null, true),

    // Analytic (type depends on argument)
    LAG(null, true),
    LEAD(null, true),
    FIRST_VALUE(null, true),
    LAST_VALUE(null, true),
    NTH_VALUE(null, true),

    // Statistical
    STDDEV(DoubleType.get(), true),
    STDDEV_POP(DoubleType.get(), true),
    VAR_POP(DoubleType.get(), true),
    VAR_SAMP(DoubleType.get(), true);

    private final DataType fixedReturnType;  // null if argument-dependent
    private final boolean defaultNullable;

    // Parse from string once, use enum everywhere
    public static WindowFunctionKind parse(String name) {
        return switch (name.toUpperCase()) {
            case "ROW_NUMBER" -> ROW_NUMBER;
            case "RANK" -> RANK;
            // ...
            default -> throw new UnsupportedOperationException("Unknown window function: " + name);
        };
    }
}
```

This eliminates three separate string-matching blocks for the same information.

### 3.10 Optional Improvements with Java 21 APIs

**Stream.toList()** (available since Java 16): Replace `Collectors.toList()` calls.

```java
// Current (ExpressionConverter, line 231):
List<Expression> arguments = func.getArgumentsList().stream()
    .map(this::convert)
    .collect(Collectors.toList());

// Proposed:
List<Expression> arguments = func.getArgumentsList().stream()
    .map(this::convert)
    .toList();
```

**SequencedCollection**: The `LogicalPlan.children()` return type could become `SequencedCollection<LogicalPlan>` to express ordered semantics, but this is low-priority since `List` already satisfies this contract.

---

## 4. Proposed Architecture

### 4.1 Package Structure (Unchanged)

The existing package structure is clean and should remain:
```
com.thunderduck.expression/  -- sealed Expression interface + record implementations
com.thunderduck.logical/     -- sealed LogicalPlan interface + record/class implementations
com.thunderduck.types/        -- sealed DataType interface + record implementations
com.thunderduck.functions/    -- unified FunctionSpec registry
com.thunderduck.generator/    -- SQLGenerator with pattern-matched visit()
com.thunderduck.runtime/      -- execution (no changes needed)
```

### 4.2 Type System Layer

```java
// DataType.java -- sealed interface
public sealed interface DataType
    permits BooleanType, ByteType, ShortType, IntegerType, LongType,
            FloatType, DoubleType, DecimalType, StringType,
            DateType, TimestampType, BinaryType,
            ArrayType, MapType, StructType, UnresolvedType {
    String typeName();
    default int defaultSize() { return -1; }
}

// Singleton types as records:
public record BooleanType()  implements DataType { ... }
public record IntegerType()  implements DataType { ... }
// etc.

// Parameterized types as records:
public record DecimalType(int precision, int scale) implements DataType { ... }
public record ArrayType(DataType elementType, boolean containsNull) implements DataType { ... }
public record MapType(DataType keyType, DataType valueType, boolean valueContainsNull) implements DataType { ... }
```

### 4.3 Expression Layer

```java
// Expression.java -- sealed interface
public sealed interface Expression
    permits Literal, ColumnRef, UnresolvedColumn, BinaryExpr, UnaryExpr,
            FunctionCall, CastExpr, AliasExpr, CaseWhenExpr,
            WindowFunction, InExpr, InSubquery, ExistsSubquery,
            ScalarSubquery, SubqueryExpr, StarExpr, RegexColumnExpr,
            IntervalExpr, LambdaExpr, LambdaVariableExpr,
            ArrayLiteralExpr, MapLiteralExpr, StructLiteralExpr,
            ExtractValueExpr, UpdateFieldsExpr, RawSQLExpr {

    DataType dataType();
    boolean nullable();
    String toSQL();
}

// Simple expressions as records:
public record CastExpr(Expression expression, DataType targetType) implements Expression { ... }
public record AliasExpr(Expression expression, String alias) implements Expression { ... }
public record Literal(Object value, DataType dataType) implements Expression { ... }

// Complex expressions remain as classes:
public final class WindowFunction implements Expression { ... }
public final class BinaryExpr implements Expression { ... }
```

### 4.4 Logical Plan Layer

```java
// LogicalPlan.java -- sealed interface
public sealed interface LogicalPlan
    permits Project, Filter, TableScan, Sort, Limit, Aggregate,
            Join, Union, Intersect, Except,
            InMemoryRelation, LocalRelation, LocalDataRelation,
            SQLRelation, AliasedRelation, Distinct,
            RangeRelation, Tail, Sample, WithColumns, ToDF,
            SingleRowRelation {

    StructType schema();
    List<LogicalPlan> children();
    OptionalLong planId();
}
```

### 4.5 SQL Generator

The SQLGenerator remains a stateful class (it uses `StringBuilder` and `aliasCounter`), but its `visit()` method and all dispatch points become pattern-matching `switch` expressions.

### 4.6 Function Registry

Single-map registry with `FunctionSpec` sealed interface:
```java
public sealed interface FunctionSpec
    permits DirectMapping, CustomTranslation, FullSpec { ... }
```

---

## 5. Migration Strategy

### Phase 1: Foundation (Low Risk, High Impact) -- ~1 week

**Goal**: Seal the three core type hierarchies and enable pattern matching.

1. **Seal `DataType`** -- Lowest risk because types are rarely extended.
   - Convert singleton types (BooleanType, IntegerType, etc.) to records
   - Keep parameterized types (DecimalType, ArrayType, MapType) as records
   - Add `sealed` + `permits` to DataType
   - Convert all `instanceof` chains on DataType to pattern-matched switches
   - **Files changed**: All 14 type classes, `TypeMapper`, `TypeInferenceEngine`, `Literal.toSQL()`

2. **Update `maven.compiler.release` to `21`** in `pom.xml`.

3. **Replace boilerplate-only Expression subclasses with records**:
   - CastExpression, AliasExpression, StarExpression, RawSQLExpression, ScalarSubquery
   - These have no complex logic -- purely data + `toSQL()`

### Phase 2: Expression Hierarchy (Medium Risk, High Impact) -- ~1 week

**Goal**: Seal the Expression hierarchy and eliminate `instanceof` chains.

1. **Seal `Expression`** with `permits` clause listing all 24+ subtypes
2. **Convert remaining simple expressions to records**: InExpression, ColumnReference, IntervalExpression, etc.
3. **Refactor `qualifyCondition()`** to use `switch (expr) { case X x -> ... }`
4. **Refactor `transformAggregateExpression()`** to use pattern matching
5. **Refactor `resolveType()` and `resolveNullable()`** in TypeInferenceEngine

### Phase 3: Logical Plan Hierarchy (Medium Risk, Medium Impact) -- ~1 week

**Goal**: Seal the LogicalPlan hierarchy and make `visit()` exhaustive.

1. **Seal `LogicalPlan`** with `permits` clause
2. **Convert `visit()` in SQLGenerator** to pattern-matched switch
3. **Convert `convertInternal()` in RelationConverter** -- this already uses a protobuf enum switch, but the result type could benefit
4. **Convert simple leaf plans to records**: RangeRelation, Tail, Sample, etc.

### Phase 4: Function Registry Unification (Medium Risk, High Maintenance Impact) -- ~1 week

**Goal**: Unify the three-map function registry into a single source of truth.

1. **Define `FunctionSpec` sealed interface** with DirectMapping, CustomTranslation, FullSpec variants
2. **Migrate all 200+ function registrations** to the unified registry
3. **Remove duplicated translation logic** between `CUSTOM_TRANSLATORS` and `FUNCTION_METADATA`
4. **Add WindowFunctionKind enum** to eliminate string-based window function dispatch

### Phase 5: Cleanup and Polish (Low Risk) -- ~3 days

1. **Replace `Collectors.toList()` with `.toList()`** across all files
2. **Replace `String.format()` with `formatted()`** where readability improves
3. **Convert `JoinPart` inner class to record**
4. **Add text blocks** where multi-line SQL templates improve readability
5. **Review and remove commented-out code** (e.g., `contextStack` in SQLGenerator)

### Risk Mitigation

- Each phase can be independently compiled and tested
- The existing differential test suite (TPC-H Q1-Q22 both SQL and DataFrame) provides comprehensive regression coverage
- Records preserve behavioral semantics -- they just eliminate boilerplate
- Sealed types with pattern matching are a MECHANICAL refactoring -- the logic doesn't change, only the dispatch mechanism

### What NOT to Change

- **SQLGenerator's stateful design** (StringBuilder + aliasCounter) -- works well for incremental SQL building
- **FunctionTranslator's `@FunctionalInterface`** -- lambda-based custom translators are elegant
- **The connect-server converter layer** -- the protobuf switch statements are already well-structured
- **The runtime/execution layer** -- Arrow interop and DuckDB JDBC are orthogonal to the translation pipeline

---

## 6. Quantitative Impact Summary

| Metric | Current | After Refactoring |
|---|---|---|
| `instanceof` chains | 15+ locations, ~300 lines | 0 (replaced by pattern matching) |
| Boilerplate lines (equals/hashCode/accessors) | ~1,000+ lines | ~0 (records) |
| Silent fall-through risk | Every `instanceof` chain | Compile-time enforced exhaustiveness |
| Function registry maps | 3 separate maps | 1 unified map |
| String-based window function dispatch | 3 duplicate locations | 1 enum |
| Duplicate type inference logic | ExpressionConverter + TypeInferenceEngine | Single source of truth |

**Total estimated line reduction**: ~1,500-2,000 lines of boilerplate eliminated while simultaneously improving type safety through compile-time exhaustiveness checking.
