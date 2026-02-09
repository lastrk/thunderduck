# Eliminating SQL String Manipulation — Architecture Plan

**Status**: Planning
**Created**: 2026-02-09
**Last Updated**: 2026-02-09

## Architectural Principles

These are non-negotiable constraints that govern all design decisions:

1. **All SQL and expression snippets MUST be parsed into a typed AST.** No string manipulation on SQL text — ever.
2. **Zero pre/post-processing of SQL strings.** All transformations happen on the AST.
3. **SparkSQL data flow**: Spark SQL string → ANTLR parse tree → Thunderduck expression tree (DuckDB constructs + extensions) → generate SQL string for execution.
4. **DataFrame data flow**: Spark Connect protobuf → Thunderduck expression tree (DuckDB constructs + extensions) → generate SQL string for execution.
5. **Relaxed mode**: Find the best performance mapping to vanilla DuckDB constructs that produces a value-equivalent result (type equivalence is not required).
6. **Strict mode**: Match Apache Spark behavior exactly via (a) CASTs at top-level SELECT projection, or (b) DuckDB extension functions (e.g., `spark_decimal_div`). No casts on intermediate values if avoidable.
7. **Minimal result-set adjustments**: No or minimal type/nullability adjustments when retrieving streaming results in strict mode.
8. **Zero result copying**: In relaxed mode, no type matching required. In strict mode, 100% type matching is achieved at SQL generation time using extension functions. No Arrow vector copying or rewriting.

## Current State — What Violates These Principles

### Violation 1: `preprocessSQL()` — 14 regex passes over SQL strings

Both the SparkSQL path and the DataFrame path run generated SQL through `preprocessSQL()`, a ~170-line function that applies regex-based transformations to SQL text. This violates principles 1, 2, 3, and 4.

**Location**: `SparkConnectServiceImpl.java:836-1005`

| Line | What it does | Which principle it violates |
|------|-------------|---------------------------|
| 838 | Backtick → double-quote | 1 (should be handled at parse time) |
| 844-852 | `count(*)` → `count(*) AS "count(1)"` | 2 (should be in SQLGenerator) |
| 856 | Fix `returns` keyword as alias | 1 (parser should handle) |
| 859-860 | `+` → `\|\|` for string concat | 1 (BinaryExpression should emit correct operator) |
| 863 | Remove `at` before `cross join` | 1 (parser bug) |
| 871 | `year()` → `CAST(year() AS INTEGER)` | 1 (parser already handles this via EXTRACT mapping) |
| 881-882 | `CAST(x AS INTEGER)` → `CAST(TRUNC(x) AS INTEGER)` | 1 (CastExpression should handle) |
| 890-891 | `SUM(` → `spark_sum(`, `AVG(` → `spark_avg(` | 1, 6 (SQLGenerator already does this for DataFrame path) |
| 897 | `fixDivisionDecimalCasts()` — 130 lines of SQL parsing | 1, 2, 6 (should be in expression tree) |
| 902-903 | Force-uppercase DESC/ASC | 1 (Sort already emits uppercase) |
| 908-999 | ROLLUP NULLS FIRST — 90 lines of char scanning | 1, 2 (should be in SQLGenerator) |
| 1002-1243 | `translateSparkFunctions()` + 4 sub-methods | 1, 3 (parser should produce correct AST) |

### Violation 2: `SchemaCorrectedBatchIterator` — Arrow vector copying

After DuckDB returns results, `SchemaCorrectedBatchIterator` copies every Arrow batch to correct nullable flags, decimal precision, and column names. This violates principles 7 and 8.

**Location**: `core/.../runtime/SchemaCorrectedBatchIterator.java:49-333`

| What it does | Principle violated | Why it exists today |
|-------------|-------------------|---------------------|
| Corrects nullable flags on every batch | 7, 8 | DuckDB defaults all columns to nullable |
| Renames `spark_sum(...)` → `sum(...)` columns | 8 | Extension functions change column names |
| Promotes DECIMAL precision (e.g., 28,4 → 38,4) | 8 | DuckDB intermediate precision differs from Spark |
| `splitAndTransfer()` copies batch data | 8 | Creates new VectorSchemaRoot with corrected schema |

### Violation 3: Post-hoc schema detection via SQL string parsing

Multiple methods in `SparkConnectServiceImpl` parse the generated SQL string to detect what expressions are present (COUNT, SUM/AVG, division patterns), then patch the schema accordingly. This violates principles 1 and 2.

| Method | Lines | What it does |
|--------|-------|-------------|
| `fixCountNullable()` | 2960-3078 | Regex-detects COUNT columns, fixes nullable |
| `fixDecimalPrecisionForComplexAggregates()` | 2314-2433 | Regex-detects arithmetic around aggregates |
| `fixDecimalToDoubleCasts()` | 2833-2941 | Compares logical schema vs DuckDB output, injects CASTs into SQL |
| `fixDivisionDecimalCasts()` | 2621-2750 | Wraps division expressions in DECIMAL casts |
| `detectCountColumns()` | 2978-3020 | Parses SELECT list for COUNT patterns |
| `detectComplexAggregateColumns()` | 2367-2433 | Scans for SUM/AVG with arithmetic |
| `mergeNullableOnly()` | 2242-2275 | Merges nullable flags from logical schema |
| `normalizeAggregateColumnName()` | 2284-2287 | String-replace `spark_sum` → `sum` |

### Violation 4: `transformSparkSQL` regex fallback

When the SparkSQL parser fails, `transformSparkSQL()` falls back to `preprocessSQL()` instead of failing. This masks parser gaps and keeps the regex hacks alive.

**Location**: `SparkConnectServiceImpl.java:816-834`

## Target Architecture

### Data Flow

```
SparkSQL path:
  spark.sql("SELECT ...")
    → ANTLR parse tree (SparkSQLParser)
    → Thunderduck expression tree (typed AST with DuckDB constructs)
    → SQLGenerator.generate()
    → SQL string for DuckDB execution

DataFrame path:
  df.select(...) / df.filter(...) / etc.
    → Spark Connect protobuf (ExpressionConverter + RelationConverter)
    → Thunderduck expression tree (typed AST with DuckDB constructs)
    → SQLGenerator.generate()
    → SQL string for DuckDB execution
```

Both paths converge on the **Thunderduck expression tree** — the single source of truth. No transformation happens after `SQLGenerator.generate()` returns a string. The string goes directly to DuckDB.

### Strict Mode Strategy

Strict mode achieves exact Spark type matching at SQL generation time via two mechanisms:

1. **Extension functions**: `spark_sum`, `spark_avg`, `spark_decimal_div` — produce Spark-correct types and rounding natively inside DuckDB. The expression tree emits these function names instead of vanilla DuckDB equivalents. Column naming uses DuckDB's `AS` aliases to match Spark names (e.g., `spark_sum(x) AS "sum(x)"`), eliminating the need for post-hoc column renaming.

2. **Top-level CASTs**: When an expression's DuckDB return type differs from Spark's expected type, the SQLGenerator wraps the outermost SELECT projection in `CAST(expr AS target_type)`. No intermediate casts — only at the final projection boundary.

This means:
- DuckDB returns results with correct types, correct nullability, correct column names
- No `SchemaCorrectedBatchIterator` needed
- No post-hoc schema merging, no SQL string re-parsing
- Arrow batches flow directly from DuckDB → gRPC serialization

### Relaxed Mode Strategy

Relaxed mode uses vanilla DuckDB constructs with no extension functions and no type-matching CASTs. Values are equivalent but types may differ (e.g., DuckDB returns HUGEINT where Spark returns BIGINT). No result copying — the client receives DuckDB's native types.

## Inventory of Violations and Their Resolution

### Category 1: Transformations that belong in the SparkSQL Parser

These are needed only for the `spark.sql("...")` path. The parser should produce the correct AST so no string fixup is needed.

| ID | Current hack | Resolution |
|----|-------------|------------|
| P1 | Backtick → double-quote (line 838) | Parser uses ANTLR lexer; SQLQuoting handles identifier emission |
| P2 | `returns` keyword alias (line 856) | Parser should emit `AS` before all aliases |
| P3 | String concat `+` → `\|\|` (lines 859-860) | Parser should detect string context and emit `\|\|` operator in BinaryExpression |
| P4 | `at cross join` removal (line 863) | Fix parser's join handling for this edge case |
| P5 | `year()` → `CAST(year() AS INTEGER)` (line 871) | Already done — EXTRACT maps to FunctionCall via parser (commit 28c8618) |
| P6 | `translateNamedStruct()` (lines 1046-1100) | Parser should emit `StructLiteralExpression` (partially done) |
| P7 | `translateMapFunction()` (lines 1105-1168) | Parser should emit `MapLiteralExpression` or `FunctionCall` |
| P8 | `translateStructFunction()` (lines 1173-1201) | Parser should map `struct()` → `row()` via FunctionRegistry |
| P9 | `translateArrayFunction()` (lines 1206-1243) | Parser should map `array()` → `list_value()` via FunctionRegistry |
| P10 | DESC/ASC uppercase (lines 902-903) | Parser produces `Sort.SortOrder` with correct direction enum |

### Category 2: Transformations that belong in the SQLGenerator

These affect both paths. The expression tree is correct, but the SQL generator doesn't emit the right DuckDB SQL.

| ID | Current hack | Resolution |
|----|-------------|------------|
| G1 | `count(*)` auto-aliasing as `"count(1)"` (lines 844-852) | SQLGenerator should emit `count(*) AS "count(1)"` when generating unaliased count(*) in a SELECT |
| G2 | `CAST(TRUNC(x) AS INTEGER)` (lines 881-882) | CastExpression.toSQL() should emit `TRUNC` when target is INTEGER and source is floating-point/decimal |
| G3 | `SUM` → `spark_sum` / `AVG` → `spark_avg` regex (lines 890-891) | SQLGenerator.visitAggregate() already handles this for DataFrame path. Extend to cover all plan shapes (WithColumns, subqueries). Remove regex. |
| G4 | `fixDivisionDecimalCasts()` (line 897, ~130 lines) | In strict mode, SQLGenerator wraps division-of-aggregate expressions in `CAST(... AS DECIMAL(38,6))` at the top-level SELECT projection |
| G5 | ROLLUP NULLS FIRST (lines 908-999, ~90 lines) | SQLGenerator detects Sort above Aggregate-with-ROLLUP and emits NULLS FIRST on all ORDER BY columns |
| G6 | `spark_sum(...)` → `sum(...)` column renaming | SQLGenerator emits `spark_sum(x) AS "sum(x)"` in strict mode, so DuckDB returns the correct column name. Eliminates `normalizeAggregateColumnName()`. |

### Category 3: Schema/type matching that belongs at SQL generation time

These post-hoc schema fixes violate principles 6, 7, 8. In the target architecture, the SQLGenerator produces SQL that makes DuckDB return the exact types Spark expects.

| ID | Current hack | Resolution |
|----|-------------|------------|
| T1 | `fixCountNullable()` — regex-detect COUNT, fix nullable | **Relaxed mode**: not needed (principle 5). **Strict mode**: DuckDB's COUNT already returns non-nullable. If schema reporting is wrong, fix the schema inference query, not the result. The expression tree knows count() is non-nullable — use that directly. |
| T2 | `fixDecimalPrecisionForComplexAggregates()` — promote DECIMAL(28,4) → DECIMAL(38,4) | **Relaxed mode**: not needed. **Strict mode**: `spark_sum`/`spark_avg` extension functions already return Spark-correct precision. If intermediate expressions still have wrong precision, wrap the top-level SELECT projection in `CAST(expr AS DECIMAL(38,s))`. |
| T3 | `fixDecimalToDoubleCasts()` — inject CAST(... AS DOUBLE) into SQL | **Relaxed mode**: not needed. **Strict mode**: SQLGenerator should emit `CAST(expr AS DOUBLE)` at the top-level projection when the expression tree's `dataType()` says DOUBLE but the DuckDB expression would return DECIMAL. This check lives in the expression tree, not in SQL string parsing. |
| T4 | `mergeNullableOnly()` — merge logical schema nullable onto DuckDB schema | Expression tree's `nullable()` method already knows the correct answer. SQLGenerator should produce SQL whose result schema matches. For `analyzePlan`, use the expression tree's schema directly instead of executing SQL and patching. |

### Category 4: Result-set processing to eliminate

| ID | Current code | Resolution |
|----|-------------|------------|
| R1 | `SchemaCorrectedBatchIterator` — copies every Arrow batch | **Eliminate entirely.** With G6 (column naming), T1-T4 (type/nullable correctness at SQL generation time), DuckDB returns Arrow batches with correct schema. Pass them through directly. |
| R2 | `normalizeAggregateColumnName()` | Eliminated by G6 — aliases in generated SQL |
| R3 | `inferSchemaFromDuckDB()` for schema analysis | For DataFrame plans, use the expression tree's `inferSchema()` directly. Only fall back to DuckDB execution for raw SQL where no AST is available. |

## Execution Plan

### Phase 1: Stop calling `preprocessSQL` on the DataFrame path

The DataFrame path already has a typed AST. Audit which `preprocessSQL` transformations are still needed for DataFrame-generated SQL and move each to the SQLGenerator. Target: `preprocessSQL` is only called for the raw SQL (`spark.sql(...)`) path.

Work items:
- [ ] G1: `count(*)` aliasing → SQLGenerator
- [ ] G2: `CAST(TRUNC)` → CastExpression
- [ ] G3: Verify `spark_sum`/`spark_avg` coverage in SQLGenerator for all plan shapes
- [ ] G5: ROLLUP NULLS FIRST → SQLGenerator
- [ ] Remove `preprocessSQL()` call at line 294 (DataFrame path)
- [ ] Verify full differential test suite passes without it

### Phase 2: Eliminate result-set copying

Make DuckDB return Arrow batches with the correct schema so `SchemaCorrectedBatchIterator` is unnecessary.

Work items:
- [ ] G6: Emit `AS "sum(...)"` aliases for extension functions in strict mode
- [ ] T1: Trust expression tree for COUNT nullability; stop regex detection
- [ ] T2: Ensure extension functions return correct DECIMAL precision; add top-level CAST in SQLGenerator for remaining mismatches
- [ ] T3: Emit CAST(... AS DOUBLE) in SQLGenerator when expression tree says DOUBLE
- [ ] T4: Use expression tree schema for `analyzePlan` instead of DuckDB execution + patching
- [ ] R1: Remove `SchemaCorrectedBatchIterator` (or reduce to a thin passthrough with no copying)

### Phase 3: Complete the SparkSQL parser

Make the SparkSQL parser handle all Spark SQL constructs so the `transformSparkSQL()` fallback to `preprocessSQL()` is never triggered.

Work items:
- [ ] P3: String concatenation operator `+` → `||`
- [ ] P6: `NAMED_STRUCT` → `StructLiteralExpression`
- [ ] P7: `MAP()` → `MapLiteralExpression`
- [ ] P8: `struct()` → `row()` via FunctionRegistry
- [ ] P9: `array()` → `list_value()` via FunctionRegistry
- [ ] P2, P4: Minor parser fixes
- [ ] G4: Division-of-aggregates decimal casting → SQLGenerator top-level projection CAST
- [ ] Remove `transformSparkSQL()` fallback — parser failure = query failure (with clear error)

### Phase 4: Delete dead code

Once phases 1-3 are complete:
- [ ] Delete `preprocessSQL()` entirely
- [ ] Delete `translateSparkFunctions()` and its 4 sub-methods
- [ ] Delete `fixCountNullable()`, `detectCountColumns()`, `collectCountAliases()`
- [ ] Delete `fixDecimalPrecisionForComplexAggregates()`, `detectComplexAggregateColumns()`
- [ ] Delete `fixDecimalToDoubleCasts()`, `applyDoubleCastsToSelectItems()`
- [ ] Delete `fixDivisionDecimalCasts()` and its helpers
- [ ] Delete `mergeNullableOnly()`, `normalizeAggregateColumnName()`
- [ ] Delete or gut `SchemaCorrectedBatchIterator`
- [ ] Delete ~20 helper methods (`findOutermostKeyword`, `splitSelectList`, `findMatchingParen`, etc.)

Estimated deletion: **~1200 lines** across `SparkConnectServiceImpl.java` and `SchemaCorrectedBatchIterator.java`.

## Extension Function Coverage Gaps

The strict-mode strategy depends on DuckDB extension functions producing Spark-correct types. Current gaps:

| Function | Gap | Impact |
|----------|-----|--------|
| `spark_avg` | Only supports DECIMAL input. No BIGINT/INTEGER overload. | AVG on integer columns may return wrong type in strict mode |
| `spark_sum` | Covers DECIMAL + all integer types. No DOUBLE overload. | SUM on DOUBLE may return wrong type |
| Column naming | Extension functions produce column names like `spark_sum(x)` | Need `AS "sum(x)"` aliases in generated SQL (G6) |

These gaps should be addressed by either adding overloads to the extension or by adding top-level CASTs in the SQLGenerator for the missing input types.

## Bugs Caused by String Hacks (evidence of fragility)

1. **2026-02-09**: `count(*) AS "count(1)"` regex applied BEFORE `OVER` clause, producing `count(*) AS "count(1)" OVER (...)` — invalid SQL. Root cause: regex can't understand SQL semantics.
2. **Ongoing risk**: `SUM(`/`AVG(` regex (line 890-891) matches inside string literals, subqueries, and column names containing "sum" or "avg".
3. **Ongoing risk**: `CAST(TRUNC)` regex (line 881) can't distinguish intentional casts from those needing truncation semantics.
4. **Known bug**: `Aggregate.toSQL()` applies `spark_sum`/`spark_avg` replacement without strict mode guard.

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-09 | Adopt 8 architectural principles | Eliminate entire class of string-manipulation bugs; achieve predictable type behavior |
| 2026-02-09 | Phase 1 = DataFrame path first | Highest impact (most queries), lowest risk (AST already available) |
| 2026-02-09 | Phase 2 = eliminate result copying before parser completion | Result copying affects performance on every query; parser gaps only affect raw SQL edge cases |
| 2026-02-09 | Extension functions + top-level CASTs for strict mode | Matches principle 6 — no intermediate casts; high-performance native DuckDB execution |
| 2026-02-09 | Relaxed mode = no CASTs, no extensions, value-equivalent only | Matches principle 5 — best performance mapping to vanilla DuckDB |

## Related Files

| File | Role | Lines affected |
|------|------|---------------|
| `connect-server/.../service/SparkConnectServiceImpl.java` | preprocessSQL, schema fixes, transformSparkSQL | ~800 lines to delete |
| `core/.../runtime/SchemaCorrectedBatchIterator.java` | Arrow batch copying | ~280 lines to delete/gut |
| `core/.../generator/SQLGenerator.java` | SQL generation — gains strict-mode CASTs, aliases, ROLLUP handling | Moderate additions |
| `core/.../expression/CastExpression.java` | CAST generation — gains TRUNC for INTEGER target | Small addition |
| `core/.../parser/SparkSQLAstBuilder.java` | SparkSQL parser — gains MAP, ARRAY, STRUCT, string concat | Medium additions |
| `core/.../functions/FunctionRegistry.java` | Function name mapping — gains struct, array, map entries | Small additions |
| `core/.../types/TypeInferenceEngine.java` | Type inference — may need DuckDB decimal precision modeling | Medium additions |
| `thunderduck-duckdb-extension/src/include/spark_aggregates.hpp` | Extension functions — may need new overloads | Small additions |
