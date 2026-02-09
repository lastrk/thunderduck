# SparkSQL Parser Design for Thunderduck

**Date:** 2026-02-09
**Author:** Architecture Team
**Status:** Design Proposal
**Tracking:** Replaces regex-based `preprocessSQL()` in `SparkConnectServiceImpl`

---

## 1. Problem Statement

Thunderduck currently passes `spark.sql()` queries directly to DuckDB with minimal regex-based rewriting (`preprocessSQL()`). This approach has three fundamental problems:

1. **SparkSQL-specific syntax is unsupported.** Four differential tests are skipped because `spark.sql()` queries with SparkSQL syntax (e.g., `DATE '2025-01-15'`, `CAST(NULL AS INT)`) fail when sent to DuckDB verbatim. The test file `tests/integration/differential/test_type_casting_differential.py` documents these at lines 131, 225, 241, and 304.

2. **Regex rewriting is fragile and unmaintainable.** The `preprocessSQL()` method in `SparkConnectServiceImpl.java` (line 814) has grown to ~160 lines of regex hacks covering backtick-to-quote translation, `count(*)` column renaming, `year()` wrapping, integer truncation, `NAMED_STRUCT`/`MAP`/`STRUCT`/`ARRAY` translation, and decimal division cast fixups. Each regex risks false positives, and they interact in order-dependent ways.

3. **No participation in the analysis framework.** Raw SQL queries bypass the LogicalPlan/Expression AST, type inference, schema correction, and all the Spark-parity logic that the DataFrame path benefits from.

### What the Skipped Tests Need

| Test | Line | SparkSQL Feature | SQL Used |
|------|------|-----------------|----------|
| `test_decimal_scale_change` | 131 | `CAST(123.456 AS DECIMAL(10,3))` | Inline literal CAST in SELECT |
| `test_date_to_string` | 225 | `DATE '2025-01-15'` | Typed date literal |
| `test_timestamp_to_date` | 241 | `TIMESTAMP '2025-01-15 10:30:45'` | Typed timestamp literal |
| `test_cast_null_literal` | 304 | `CAST(NULL AS INT)`, etc. | NULL literal casts |

These are standard SQL constructs that DuckDB also supports -- the issue is that Thunderduck's `preprocessSQL()` pipeline does not handle them correctly, and the raw-SQL path lacks schema correction. A proper parser would make these trivially supportable.

---

## 2. ANTLR Grammar Import

### 2.1 Grammar Source

The SparkSQL ANTLR4 grammar files have been imported from the Apache Spark v4.0.0 release:

- **Source repository:** `apache/spark` on GitHub
- **Tag:** `v4.0.0` (released 2025)
- **Source path:** `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/`
- **Files imported:**
  - `SqlBaseParser.g4` -- 2,298 lines (parser grammar)
  - `SqlBaseLexer.g4` -- 640 lines (lexer grammar)
- **Local path:** `core/src/main/antlr4/org/apache/spark/sql/catalyst/parser/`

### 2.2 Why v4.0.0 (Not master)

The Thunderduck POM specifies `spark.version=4.1.1`, but the grammar at `v4.0.0` is the stable baseline for the 4.x series. The parser grammar is additive between minor versions -- new syntax rules get added but existing ones are not removed. Using v4.0.0 gives us:

- A stable, well-tested grammar for the features we need today
- Forward compatibility: anything that parses under v4.0.0 will parse under v4.1.x
- A clear upgrade path: diff grammar files between v4.0.0 and v4.1.1 when ready

If new SparkSQL syntax from 4.1.x is needed (e.g., pipe syntax, new DDL), the grammar can be point-updated by cherry-picking specific rules.

### 2.3 Grammar Structure

The grammar is split into two files following ANTLR4 best practices (per SPARK-38378):

**SqlBaseLexer.g4** defines:
- 350+ SQL keyword tokens (SELECT, FROM, WHERE, LATERAL, PIVOT, ...)
- Operators and punctuation tokens
- Literal patterns (strings, numbers, identifiers)
- Lexer modes for complex type handling (ARRAY, MAP, STRUCT brackets)

**SqlBaseParser.g4** defines:
- `statement` -- top-level dispatch to query, DDL, DML, utility
- `query` -- SELECT with CTEs, set operations, ORDER BY, LIMIT
- `queryTerm` / `queryPrimary` -- UNION/INTERSECT/EXCEPT, subqueries
- `selectClause` / `fromClause` / `whereClause` -- query components
- `expression` / `booleanExpression` / `valueExpression` / `primaryExpression` -- expression hierarchy with precedence
- `lateralView`, `pivotClause`, `unpivotClause` -- Spark-specific relation transforms
- `functionCall`, `windowSpec`, `tableValuedFunction` -- function syntax
- `dataType` -- type definitions including complex types
- DDL/DML rules (CREATE, ALTER, DROP, INSERT, etc.)

### 2.4 License Compliance

Both files are licensed under Apache License 2.0, same as Thunderduck. No additional license obligations.

---

## 3. Test Coverage Analysis

### 3.1 TPC-H Queries (22 queries, all passing)

The TPC-H SQL queries at `tests/integration/sql/tpch_queries/q*.sql` use the following SQL features:

| Feature | Queries Using It | DuckDB Compatible? |
|---------|-----------------|-------------------|
| Basic SELECT/FROM/WHERE/GROUP BY/ORDER BY | All 22 | Yes |
| Subqueries (scalar, IN, EXISTS, NOT EXISTS) | Q2, Q4, Q11, Q15, Q17, Q18, Q20, Q21, Q22 | Yes |
| CTEs (WITH clause) | Q15 | Yes |
| CASE WHEN expressions | Q8, Q12 | Yes |
| `year()` function | Q7, Q8, Q9 | Needs CAST to INTEGER |
| `date 'YYYY-MM-DD'` typed literal | Q4, Q5, Q7, Q8, Q10, Q14, Q15, Q20 | Yes |
| `interval '3' month` arithmetic | Q4, Q14, Q15, Q20 | Mostly compatible |
| `CAST(x AS date)` | Q5, Q10, Q12 | Yes |
| `substring()` function | Q22 | Yes |
| `BETWEEN` predicate | Q7, Q8 | Yes |
| LEFT OUTER JOIN | Q13 | Yes |
| `count(*)`, `SUM`, `AVG`, `MIN`, `MAX` | Most queries | Needs type fixups |
| Implicit comma-join (FROM t1, t2) | Q2, Q7, Q8, Q9, Q21 | Yes |
| Derived table aliases | Q7, Q8, Q13, Q22 | Yes |
| Column alias in ORDER BY | Multiple | Yes |

**Key finding:** TPC-H queries are essentially ANSI SQL with minor Spark function calls (`year()`). They already pass through the regex preprocessor. The parser must not regress these.

### 3.2 TPC-DS Queries (91 queries)

The TPC-DS SQL queries at `tests/integration/sql/tpcds_queries/q*.sql` exercise significantly more SparkSQL features:

| Feature | Queries Using It | Notes |
|---------|-----------------|-------|
| CTEs (WITH clause) | Q1, Q4, Q5, Q11, Q14a/b, Q23a/b, Q24a/b, Q30, Q47, Q51, Q57, Q58, Q59, Q74, Q75, Q78, Q80, Q95, Q97 | Heavy use, multiple CTEs per query |
| ROLLUP | Q5, Q14a, Q18, Q22, Q27, Q36, Q67, Q70, Q77, Q80, Q86 | `GROUP BY ROLLUP(a,b,c)` |
| `GROUPING()` function | Q27, Q36, Q70, Q86 | Used with ROLLUP |
| `rank() OVER (...)` window functions | Q36, Q47, Q51, Q57, Q67, Q70, Q86 | With PARTITION BY and ORDER BY |
| `INTERSECT` set operation | Q14a, Q38, Q87 | Between SELECT statements |
| `interval 'N' day` arithmetic | Q5, Q12, Q16, Q20, Q21, Q32, Q37, Q72, Q80, Q82, Q92, Q94, Q98 | `CAST(date) + interval '30' day` |
| `CAST(x AS DECIMAL(p,s))` | Q5, Q80 | Explicit decimal precision |
| `FULL OUTER JOIN` | Q51 | Less common join type |
| `UNION ALL` | Q5, Q14a, Q75, Q77, Q80, Q97 | Within CTEs |
| Correlated subqueries | Q1, Q6, Q30, Q35, Q69, Q70 | Reference outer query |
| `HAVING` clause | Q14a, Q56, Q60 | Post-aggregation filter |
| `coalesce()` | Q67 | Standard function |
| `sum(sum(x)) OVER (...)` | Q51 | Nested aggregate in window |
| `concat()` | Q5 | String function |
| `abs()` | Q47 | Math function |

**Key finding:** TPC-DS queries are the real stress test. They use CTEs extensively, ROLLUP with GROUPING(), window functions, INTERSECT, and correlated subqueries. All of these are standard SQL that DuckDB supports -- but they exercise more parser rules than TPC-H.

### 3.3 Current preprocessSQL() Regex Hacks

The following transformations are currently applied by `preprocessSQL()` in `SparkConnectServiceImpl.java`:

| # | Regex Hack | Purpose | Risk Level |
|---|-----------|---------|-----------|
| 1 | Backtick to double-quote | SparkSQL uses backticks for identifiers | Low (simple) |
| 2 | `count(*)` to `count(*) AS "count(1)"` | Match Spark column naming | High (complex regex, context-dependent) |
| 3 | `returns` keyword to `AS returns` | Fix DuckDB keyword conflict | Medium |
| 4 | `+` to `\|\|` for string concat | SparkSQL overloads + for strings | High (fragile pattern matching) |
| 5 | Remove `at` before `cross join` | Q90-specific fix | Low (targeted) |
| 6 | `year(x)` to `CAST(year(x) AS INTEGER)` | DuckDB returns BIGINT for year() | Medium (must avoid nested) |
| 7 | `CAST(x AS INTEGER)` to `CAST(TRUNC(x) AS INTEGER)` | Spark truncates, DuckDB rounds | Medium (interacts with #6) |
| 8 | `SUM(` to `spark_sum(` (strict mode) | Extension-based Spark-compatible aggregates | Low (straight replace) |
| 9 | `AVG(` to `spark_avg(` (strict mode) | Extension-based Spark-compatible aggregates | Low (straight replace) |
| 10 | `fixDivisionDecimalCasts()` (strict mode) | Fix decimal division precision | High (200+ lines) |
| 11 | ROLLUP ORDER BY NULLS FIRST injection | Ensure correct NULL ordering | High (manual SQL parsing) |
| 12 | `NAMED_STRUCT` to `struct_pack` | Spark function translation | Medium (arg pairing) |
| 13 | `MAP(k1,v1,...)` to `MAP([keys],[vals])` | Different MAP constructor syntax | Medium (arg restructuring) |
| 14 | `STRUCT(...)` to `row(...)` | Simple name mapping | Low |
| 15 | `ARRAY(...)` to `list_value(...)` | Simple name mapping | Low |
| 16 | `FunctionRegistry.rewriteSQL()` | Bulk Spark-to-DuckDB function name mapping | Medium (regex-based) |

**Total: 16 regex-based transformations, at least 5 with "high" fragility risk.**

---

## 4. Prioritized Feature Roadmap

### P0 -- Critical (TPC-H/TPC-DS correctness)

These features are needed for the existing query suites to work correctly through the parser:

1. **Basic DQL parsing** -- SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
2. **Subqueries** -- scalar, IN, EXISTS, NOT EXISTS, derived tables
3. **CTEs** -- WITH clause (single and multiple)
4. **Set operations** -- UNION ALL, UNION, INTERSECT, EXCEPT
5. **Join types** -- INNER, LEFT/RIGHT/FULL OUTER, CROSS, SEMI, ANTI
6. **Window functions** -- OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
7. **ROLLUP / CUBE / GROUPING SETS** -- with `GROUPING()` function
8. **Expression types** -- arithmetic, comparison, logical, CASE WHEN, CAST, BETWEEN, IN, LIKE, IS NULL
9. **Date/interval arithmetic** -- `date '...' + interval 'N' day/month/year`
10. **Typed literals** -- `DATE 'x'`, `TIMESTAMP 'x'`, `DECIMAL(p,s)`
11. **Aggregate functions** -- SUM, AVG, COUNT, MIN, MAX, COUNT DISTINCT
12. **Identifier quoting** -- backtick-to-double-quote translation

### P1 -- High (Skipped tests + common user patterns)

1. **Inline CAST expressions** -- `CAST(123.456 AS DECIMAL(10,3))` (test_decimal_scale_change)
2. **Typed date/timestamp literals** -- `DATE '2025-01-15'` (test_date_to_string)
3. **NULL literal CAST** -- `CAST(NULL AS INT)` (test_cast_null_literal)
4. **Spark function name mapping** -- route through FunctionRegistry during AST build
5. **`year()` / `month()` / `day()` return type correction** -- CAST to INTEGER
6. **Integer division truncation semantics** -- TRUNC wrapping
7. **String concatenation operator** -- `+` on strings becomes `||`
8. **Aggregate column naming** -- `count(*)` aliased as `count(1)`

### P2 -- Medium (Common SparkSQL patterns)

1. **LATERAL VIEW** -- `LATERAL VIEW explode(array) t AS col`
2. **PIVOT / UNPIVOT** -- table transformation operators
3. **TABLESAMPLE** -- `TABLESAMPLE (10 PERCENT)`
4. **Complex type constructors** -- `NAMED_STRUCT`, `MAP()`, `STRUCT()`, `ARRAY()`
5. **Lambda expressions** -- `transform(array, x -> x + 1)`
6. **Type coercion rules** -- implicit Spark-style type promotion
7. **CLUSTER BY / DISTRIBUTE BY / SORT BY** -- Spark distribution hints
8. **Table-valued functions** -- `range()`, `explode()`, `inline()`
9. **CREATE TEMP VIEW** via SQL -- `CREATE OR REPLACE TEMP VIEW v AS ...`

### P3 -- Low (Rare / advanced)

1. **DDL statements** -- CREATE TABLE, ALTER TABLE, DROP TABLE
2. **DML statements** -- INSERT INTO, DELETE, UPDATE, MERGE
3. **Query hints** -- `/*+ BROADCAST(t) */`, `/*+ SHUFFLE_HASH(t) */`
4. **Compound statements** -- `BEGIN ... END` blocks
5. **TRANSFORM clause** -- Hive TRANSFORM (pipe to external process)
6. **Multiple statement execution** -- semicolon-separated statements
7. **EXPLAIN** -- query plan introspection
8. **Collation expressions** -- `COLLATE 'utf8_binary'`

---

## 5. Architecture Design

### 5.1 Recommended Approach: Parse -> Thunderduck AST -> DuckDB SQL

**Decision: Parse SparkSQL into our existing LogicalPlan/Expression AST, then use SQLGenerator to emit DuckDB SQL.**

This is preferred over direct SparkSQL-to-DuckDB transpilation for the following reasons:

1. **Reuses all existing infrastructure.** The SQLGenerator, FunctionRegistry, type inference engine, and schema correction logic are already battle-tested for the DataFrame path. Sending SQL through the same AST means SQL queries automatically benefit from all the Spark-parity work.

2. **Single code path.** The CLAUDE.md architecture notes already warn about the "dual SQL generation paths" bug pattern. Adding a third path (direct transpilation) would triple the maintenance burden. By funneling both DataFrame API and `spark.sql()` through the same AST, we eliminate this class of bugs.

3. **Enables optimization.** With a proper AST, we can apply optimizations (predicate pushdown, join reordering, constant folding) that are impossible with string rewriting.

4. **Gradual migration.** We can convert `preprocessSQL()` hacks one at a time. Each hack we remove from the regex pipeline is replaced by a proper AST transformation.

### 5.2 System Architecture

```
                        spark.sql("SELECT ...")
                                |
                                v
                    +-------------------+
                    | SparkSQL Parser   |  <-- NEW COMPONENT
                    | (ANTLR4-based)    |
                    +-------------------+
                                |
                                v
                    +-------------------+
                    | SparkSQL Visitor  |  <-- NEW COMPONENT
                    | (AST Builder)     |  Converts ANTLR ParseTree -> Thunderduck LogicalPlan/Expression
                    +-------------------+
                                |
                                v
              +----------------------------------+
              |     Thunderduck LogicalPlan       |  <-- EXISTING (shared with DataFrame path)
              | (Project, Filter, Aggregate, ...) |
              +----------------------------------+
                                |
                                v
              +----------------------------------+
              |         SQLGenerator             |  <-- EXISTING
              | (generates DuckDB SQL)           |
              +----------------------------------+
                                |
                                v
              +----------------------------------+
              |         preprocessSQL()          |  <-- EXISTING (shrinks over time)
              | (remaining regex fixups)         |
              +----------------------------------+
                                |
                                v
                          DuckDB Execution
```

For comparison, the DataFrame API path:

```
              DataFrame API (PySpark)
                        |
                        v
              Spark Connect Protobuf
                        |
                        v
              +-------------------+
              | PlanConverter     |  <-- EXISTING
              | RelationConverter |
              | ExpressionConverter |
              +-------------------+
                        |
                        v
              Thunderduck LogicalPlan  <<<  SAME AST  >>>
                        |
                        v
                  SQLGenerator -> DuckDB
```

Both paths converge at the LogicalPlan/Expression AST layer.

### 5.3 Visitor vs Listener Pattern

**Decision: Use the Visitor pattern (not Listener).**

Rationale:
- Visitors return values, which is essential for AST construction. Each `visitExpression()` call returns an `Expression` node, each `visitQuery()` returns a `LogicalPlan` node.
- Listener pattern requires external state (stack-based) for return values, which is error-prone.
- Spark's own `AstBuilder.scala` uses the Visitor pattern for the same reason.
- The generated `SqlBaseParserBaseVisitor<T>` class provides type-safe dispatch.

### 5.4 New Classes

| Class | Package | Responsibility |
|-------|---------|---------------|
| `SparkSQLParser` | `com.thunderduck.parser` | Entry point. Manages ANTLR lexer/parser lifecycle, SLL/LL fallback. |
| `SparkSQLAstBuilder` | `com.thunderduck.parser` | ANTLR Visitor that builds `LogicalPlan` and `Expression` nodes. |
| `SparkSQLTypeConverter` | `com.thunderduck.parser` | Converts SparkSQL type syntax to `DataType` objects. |
| `SparkSQLErrorListener` | `com.thunderduck.parser` | Custom ANTLR error listener for user-friendly error messages. |

### 5.5 Integration Point

In `SparkConnectServiceImpl.java`, the current flow for `spark.sql()` is:

```java
// Current (line 196):
sql = query;
// ... later ...
sql = preprocessSQL(sql);
executeSQL(sql, session, responseObserver);
```

After integration:

```java
// New:
SparkSQLParser parser = getOrCreateParser();  // cached per-service instance
LogicalPlan plan = parser.parse(query);
String duckdbSQL = sqlGenerator.generate(plan);
duckdbSQL = preprocessSQL(duckdbSQL);  // remaining fixups (shrinks over time)
executeSQLStreaming(duckdbSQL, plan.schema(), session, responseObserver, -1, timing);
```

Key changes:
- SQL queries now get a `LogicalPlan` and thus a `schema()`, enabling `SchemaCorrectedBatchIterator` for Spark-parity type corrections.
- The `preprocessSQL()` pipeline remains as a safety net but shrinks as the AST builder subsumes each regex hack.
- The schema analysis path (`analyzePlan` with `hasSql()`) also benefits from proper parsing.

### 5.6 Incremental Adoption Strategy

The parser can be adopted incrementally without a big-bang rewrite:

**Phase A -- Parallel execution (canary mode):**
Parse the SQL and generate DuckDB SQL via the AST path. Compare the AST-generated SQL with the regex-preprocessed SQL. Log differences but execute the original (regex) path. This catches regressions with zero risk.

**Phase B -- AST-first with fallback:**
Try the AST path first. If it succeeds, use it. If the parser throws `UnsupportedOperationException` (unimplemented SQL feature), fall back to the regex path. Track fallback rates in metrics.

**Phase C -- Full cutover:**
Remove the regex path when fallback rate reaches zero for the test suite. Keep `preprocessSQL()` as a no-op for debugging.

### 5.7 Pass-Through Optimization

**Decision: Do not implement a "detect DuckDB-compatible SQL and skip parsing" optimization.**

Rationale:
1. Determining DuckDB compatibility without parsing is itself a parsing problem.
2. The ANTLR parser with SLL mode is fast enough (see Section 6) that the overhead is negligible for typical query sizes.
3. A pass-through mode creates two code paths with different behavior, which is exactly what we are trying to eliminate.
4. We tried the "send to DuckDB first, retry on failure" approach in the early days and it created confusing error messages and doubled latency for failing queries.

The one exception: if the SQL is a simple `SELECT 1` or `SHOW TABLES` type command, the parser should recognize it quickly and generate minimal AST.

### 5.8 Error Handling

```java
public class SparkSQLParseException extends RuntimeException {
    private final int line;
    private final int charPositionInLine;
    private final String offendingToken;
    private final String suggestion;  // e.g., "Did you mean TIMESTAMP?"
}
```

Error messages should:
- Report line and column position within the original SQL
- Show the offending token and expected alternatives
- Be returned as gRPC `INVALID_ARGUMENT` status (not `INTERNAL`)
- Match Spark's error message format where possible (for client compatibility)

---

## 6. Performance Analysis

### 6.1 ANTLR4 Performance Characteristics

| Metric | Cold (First Parse) | Warm (Subsequent) |
|--------|-------------------|-------------------|
| Lexer/Parser class loading | ~5ms | 0ms (loaded) |
| DFA construction (SLL mode) | 50-200ms | 0ms (cached) |
| DFA construction (LL mode) | 2-8 seconds | 0ms (cached) |
| Parse time (simple SELECT) | ~1ms | ~0.3ms |
| Parse time (TPC-H Q7, ~40 lines) | ~3ms | ~1ms |
| Parse time (TPC-DS Q14a, ~80 lines) | ~5ms | ~2ms |
| Memory (DFA cache, full grammar) | 10-50MB | Stable |

### 6.2 SLL/LL Two-Phase Parsing Strategy

ANTLR4 supports two prediction modes:

- **SLL (Strong LL)**: Faster but may reject valid input that requires full LL lookahead.
- **LL (Adaptive LL(*))**: Always correct but builds DFA states lazily, causing cold-start latency.

**Strategy: Use SLL first, fall back to LL on SLL failure.**

```java
parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
try {
    return parser.singleStatement();
} catch (Exception e) {
    tokens.seek(0);  // reset token stream
    parser.reset();
    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
    return parser.singleStatement();
}
```

This is exactly what Spark itself does (see `AbstractSqlParser.scala`). For >95% of queries, SLL succeeds on the first attempt. The LL fallback handles ambiguous grammars (e.g., deeply nested expressions that SLL cannot resolve).

### 6.3 Parser Instance Management

**Decision: One parser instance per `SparkConnectServiceImpl`, not per-query.**

The ANTLR4 lexer and parser instances are lightweight (a few KB each), but they carry DFA caches that are expensive to rebuild. By reusing instances:

```java
public class SparkSQLParser {
    private final SqlBaseLexer lexer;
    private final CommonTokenStream tokens;
    private final SqlBaseParser parser;

    public SparkSQLParser() {
        // Create once, reuse for all queries
        this.lexer = new SqlBaseLexer(null);  // input set per parse
        this.tokens = new CommonTokenStream(lexer);
        this.parser = new SqlBaseParser(tokens);

        // Configure for performance
        parser.removeErrorListeners();
        parser.addErrorListener(new SparkSQLErrorListener());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
    }

    public synchronized LogicalPlan parse(String sql) {
        lexer.setInputStream(CharStreams.fromString(sql));
        tokens.setTokenSource(lexer);
        parser.setTokenStream(tokens);
        // ... parse and build AST
    }
}
```

Note the `synchronized`: ANTLR4 parsers are **not thread-safe**. Since `SparkConnectServiceImpl` handles concurrent gRPC requests, the parser must be synchronized or pooled.

**Recommendation: Thread-local parser pool.** Each thread gets its own parser instance. This avoids lock contention while sharing DFA caches within a thread.

```java
private static final ThreadLocal<SparkSQLParser> PARSER_POOL =
    ThreadLocal.withInitial(SparkSQLParser::new);
```

### 6.4 DFA Cache Management

Spark itself encountered memory issues with ANTLR DFA caches (SPARK-47404). Our grammar is the same, so we face the same risk. Mitigation:

1. **Set `atn.clearDFA()` periodically** -- After processing N queries (e.g., 10,000), clear the DFA cache. This causes a brief latency spike on the next parse but prevents unbounded memory growth.

2. **Monitor cache size** -- The `ATN` object can report DFA state counts. Log warnings if cache exceeds a threshold.

3. **SLL mode helps** -- SLL builds fewer DFA states than LL, so the cache grows more slowly.

### 6.5 Latency Budget

Current `preprocessSQL()` operates in microsecond range (regex on a string). The ANTLR parser will add 0.3-5ms per query. For context:

- DuckDB query execution: 1ms-10,000ms (depending on data size)
- gRPC serialization: ~0.1ms
- Arrow batch construction: ~0.5ms

Parser overhead of 1-2ms is well within budget -- it is <1% of total query latency for any non-trivial query.

### 6.6 Parse Tree Caching

**Decision: Do not cache parsed query plans.**

Rationale:
- Query strings are rarely repeated verbatim in analytics workloads (different parameters, different date ranges).
- The parse time (1-2ms) is negligible compared to execution time.
- Caching adds memory pressure and cache invalidation complexity.
- If needed later, a simple `Map<String, LogicalPlan>` LRU cache can be added with minimal code changes.

### 6.7 Comparison: ANTLR4 vs Current Regex

| Metric | Regex (`preprocessSQL`) | ANTLR4 Parser |
|--------|------------------------|---------------|
| Latency (simple query) | ~10us | ~300us |
| Latency (complex TPC-DS) | ~50us | ~2ms |
| Correctness | Fragile (false positives) | Grammar-defined |
| Maintainability | Low (regex soup) | High (grammar rules) |
| Error messages | None (silent corruption) | Line/column/token |
| Feature coverage | ~16 specific hacks | Full SparkSQL grammar |
| Memory | Negligible | 10-50MB DFA cache |

The 30x latency increase is acceptable because:
- It is constant overhead, not scaling with data
- Parse time is amortized over query execution (which is 100x-10000x longer)
- We gain correctness, maintainability, and feature completeness

---

## 7. Implementation Roadmap

### Phase 1: Infrastructure (1-2 weeks)

**Goal:** ANTLR4 integrated into the build, grammar compiles, basic smoke test.

Tasks:
- [ ] Add `antlr4-runtime` dependency to `core/pom.xml` (version 4.13.x)
- [ ] Add `antlr4-maven-plugin` to `core/pom.xml` for grammar compilation
- [ ] Configure plugin to generate Java visitor classes from `SqlBaseParser.g4` and `SqlBaseLexer.g4`
- [ ] Verify generated classes compile (resolve any `@header` / `@members` issues)
- [ ] Create `SparkSQLParser` entry point class with SLL/LL fallback
- [ ] Create `SparkSQLErrorListener` with user-friendly error messages
- [ ] Write unit tests: parse simple SELECT, parse TPC-H Q1, parse invalid SQL (error case)
- [ ] Verify grammar `@members` blocks compile with Java (they reference `ArrayDeque`, `Deque`)

**Maven plugin configuration:**

```xml
<plugin>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-maven-plugin</artifactId>
    <version>4.13.2</version>
    <executions>
        <execution>
            <goals>
                <goal>antlr4</goal>
            </goals>
            <configuration>
                <visitor>true</visitor>
                <listener>false</listener>
                <sourceDirectory>${basedir}/src/main/antlr4</sourceDirectory>
                <outputDirectory>${project.build.directory}/generated-sources/antlr4</outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>
```

**Milestone:** `new SparkSQLParser().parse("SELECT 1")` returns a `LogicalPlan`.

### Phase 2: Core SELECT Parsing (2-3 weeks)

**Goal:** Parse and translate the 80% case -- SELECT queries with the features needed for TPC-H.

Tasks:
- [ ] Implement `SparkSQLAstBuilder` visitor with:
  - `visitSingleStatement` -- entry point
  - `visitQuery` -- WITH, queryTerm, ORDER BY, LIMIT
  - `visitQueryPrimary` -- SELECT ... FROM ... WHERE ... GROUP BY ... HAVING
  - `visitSelectClause` -- projection expressions
  - `visitFromClause` -- table references, joins, subqueries
  - `visitExpression` hierarchy -- arithmetic, comparison, logical, CASE, CAST, function calls
  - `visitTableName` -- resolve to `TableScan` node
  - `visitSubquery` -- wrap in subquery `SQLRelation` or inline
  - `visitSetOperation` -- UNION/INTERSECT/EXCEPT to `Union`/`Intersect`/`Except` nodes
- [ ] Implement `SparkSQLTypeConverter`:
  - `STRING` -> `DataType.STRING`
  - `INT/INTEGER` -> `DataType.INTEGER`
  - `BIGINT/LONG` -> `DataType.LONG`
  - `DOUBLE` -> `DataType.DOUBLE`
  - `DECIMAL(p,s)` -> `DataType.decimal(p,s)`
  - `DATE` -> `DataType.DATE`
  - `TIMESTAMP` -> `DataType.TIMESTAMP`
  - `BOOLEAN` -> `DataType.BOOLEAN`
- [ ] Map function names through `FunctionRegistry.translate()` during AST build
- [ ] Handle SparkSQL typed literals (`DATE '...'`, `TIMESTAMP '...'`) by generating DuckDB-compatible CAST expressions
- [ ] Handle backtick-quoted identifiers by emitting double-quoted identifiers
- [ ] Run all 22 TPC-H queries through AST path in parallel mode (canary), compare output with regex path
- [ ] Fix the 4 skipped type-casting tests

**Milestone:** All 22 TPC-H queries produce identical DuckDB SQL via both regex and AST paths. 4 skipped tests now pass.

### Phase 3: Advanced SQL Features (2-3 weeks)

**Goal:** Support TPC-DS query features and common SparkSQL patterns.

Tasks:
- [ ] CTEs (WITH clause) -- emit DuckDB-compatible CTEs directly
- [ ] Window functions -- `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` with frame specs
- [ ] ROLLUP / CUBE / GROUPING SETS -- translate to DuckDB `GROUP BY ROLLUP(...)`
- [ ] `GROUPING()` function -- pass through (DuckDB supports it)
- [ ] Correlated subqueries -- detect and translate correctly
- [ ] `INTERVAL` arithmetic -- `date + interval 'N' unit` to DuckDB syntax
- [ ] `IN` subqueries -- `WHERE x IN (SELECT ...)`
- [ ] `EXISTS` / `NOT EXISTS` subqueries
- [ ] Nested aggregates in window functions -- `sum(sum(x)) OVER (...)`
- [ ] Column aliases in ORDER BY (resolve to expression)
- [ ] Ordinal references in ORDER BY (`ORDER BY 3` meaning 3rd column)
- [ ] Run all 91 TPC-DS queries through AST path

**Milestone:** All TPC-DS queries produce identical output via AST path.

### Phase 4: Replace preprocessSQL() Regex Hacks (1-2 weeks)

**Goal:** Systematically eliminate each regex hack by handling it in the AST builder.

| Regex Hack | AST Replacement |
|-----------|----------------|
| Backtick to double-quote | Handled in identifier visitor |
| `count(*)` aliasing | Handled in aggregate expression builder |
| `year()` CAST wrapping | Handled in function call visitor (FunctionRegistry) |
| Integer truncation | Handled in CAST expression visitor |
| String concat `+` to `\|\|` | Handled in binary expression visitor |
| `SUM`/`AVG` to `spark_sum`/`spark_avg` | Handled in function call visitor (strict mode) |
| `NAMED_STRUCT`/`MAP`/`STRUCT`/`ARRAY` | Handled in function call visitor |
| ROLLUP NULL ordering | Handled in ORDER BY visitor |
| Division decimal casts | Handled in expression type inference |
| `FunctionRegistry.rewriteSQL()` | No longer needed -- function mapping happens at AST level |

Tasks:
- [ ] Remove each regex hack one at a time, with test verification after each removal
- [ ] Run full TPC-H + TPC-DS + differential test suites after each removal
- [ ] Keep `preprocessSQL()` method but make it a thin passthrough (for debugging)
- [ ] Remove `translateNamedStruct`, `translateMapFunction`, `translateStructFunction`, `translateArrayFunction` methods

**Milestone:** `preprocessSQL()` is an empty method. All SQL goes through AST.

### Phase 5: SparkSQL Extensions and DDL (2-4 weeks, lower priority)

**Goal:** Support LATERAL VIEW, PIVOT, TABLESAMPLE, and DDL for full SparkSQL coverage.

Tasks:
- [ ] LATERAL VIEW EXPLODE -- translate to DuckDB `UNNEST` or lateral join
- [ ] PIVOT / UNPIVOT -- translate to DuckDB PIVOT or manual CASE aggregation
- [ ] TABLESAMPLE -- translate to DuckDB `TABLESAMPLE`
- [ ] Complex type constructors in SQL -- NAMED_STRUCT, MAP, STRUCT, ARRAY
- [ ] Lambda expressions -- `transform(arr, x -> x + 1)` to DuckDB `list_transform`
- [ ] CREATE/DROP TEMP VIEW via SQL
- [ ] INSERT INTO / CTAS
- [ ] EXPLAIN

**Milestone:** Feature parity with SparkSQL for common analytics patterns.

---

## 8. Alternatives Considered

### 8.1 Use Spark's Own Parser Directly (CatalystSqlParser)

**Approach:** Import `spark-catalyst_2.13` (already in POM as `provided` scope) and call `CatalystSqlParser.parsePlan(sql)` to get a Catalyst `LogicalPlan`, then convert that to Thunderduck's `LogicalPlan`.

**Why rejected:**
- **Scala runtime dependency.** `spark-catalyst` pulls in the Scala 2.13 standard library (~20MB), Spark's internal IR classes, and dozens of transitive dependencies. Thunderduck currently uses `provided` scope specifically to avoid this weight at runtime.
- **API instability.** Spark's internal Catalyst `LogicalPlan` types change between minor versions. We would need to write a converter from Catalyst's `LogicalPlan` to our `LogicalPlan`, and maintain it across Spark version upgrades.
- **Version coupling.** Thunderduck would be tied to a specific Spark version for parser behavior. By owning the grammar, we can decouple parser evolution from Spark releases.
- **Two conversion steps.** Instead of `ANTLR ParseTree -> Thunderduck AST`, we would have `ANTLR ParseTree -> Catalyst LogicalPlan -> Thunderduck AST`. The intermediate Catalyst representation adds complexity without benefit.
- **Circular dependency risk.** The Spark Connect server already depends on Spark jars for protobuf definitions. Adding runtime dependency on Catalyst creates tight coupling.

**Verdict:** The grammar files (.g4) are the right level of abstraction to borrow from Spark. We take the grammar, not the implementation.

### 8.2 Extend the Regex Approach

**Approach:** Keep adding regex patterns to `preprocessSQL()` for each new SparkSQL feature.

**Why rejected:**
- **Quadratic complexity.** Each new regex must be tested against all existing regexes for interaction effects. With 16 patterns already, adding one more requires 16 interaction tests.
- **Correctness is impossible.** Regexes cannot correctly parse nested expressions, respect string quoting, or handle context-dependent keywords. Example: the `count(*)` regex already has a 200-character negative lookahead to avoid false positives.
- **Error messages are absent.** When a regex fails to match, the SQL passes through unchanged and DuckDB throws an opaque error. Users get no indication that their SparkSQL syntax was not handled.
- **Technical debt compounding.** The `fixDivisionDecimalCasts` method alone is 200+ lines of manual SQL parsing code. This pattern does not scale.

**Verdict:** The regex approach has reached its limits. It was an appropriate bootstrap strategy but must be replaced with a proper parser.

### 8.3 Apache Calcite

**Approach:** Use Calcite's SQL parser with `SparkSqlDialect` conformance to parse SparkSQL and convert to DuckDB SQL.

**Why rejected:**
- **Incomplete Spark dialect.** Calcite's `SparkSqlDialect` is primarily for SQL generation (Calcite -> SparkSQL), not parsing. It does not support all SparkSQL syntax (LATERAL VIEW, TRANSFORM, Spark-specific function names, typed literals).
- **Heavy dependency.** Calcite adds ~30MB of jars and pulls in dozens of transitive dependencies (Avatica, Janino, etc.).
- **Different IR.** Calcite uses its own `RelNode` / `RexNode` IR, requiring a conversion layer to Thunderduck's LogicalPlan. This is the same double-conversion problem as the Catalyst approach.
- **Overkill.** Calcite's value proposition is its optimizer (hundreds of rules, cost-based planning). Thunderduck delegates optimization to DuckDB, so Calcite's optimizer is wasted.

**Verdict:** Calcite is the right tool if you need a query planner. Thunderduck needs a parser and transpiler -- Spark's ANTLR grammar is more appropriate.

### 8.4 Hand-Written Recursive Descent Parser

**Approach:** Write a parser from scratch in Java that handles the subset of SparkSQL we need.

**Why rejected for initial implementation:**
- **High implementation effort.** Even a subset of SparkSQL has complex expression precedence, subquery handling, and dozens of statement types. The ANTLR grammar captures all of this in 2,300 lines; hand-writing it would be 5,000-10,000 lines.
- **Maintenance burden.** As SparkSQL evolves (new syntax in Spark 4.x, 5.x), we would need to manually add support. With the ANTLR grammar, we update two files.
- **Risk of subtle incompatibilities.** The ANTLR grammar is the canonical definition of SparkSQL syntax. A hand-written parser might accept or reject different inputs.

**Verdict:** A hand-written parser could be considered later for hot-path optimization (e.g., a fast-path for `SELECT ... FROM ... WHERE ...` patterns), but ANTLR is the right starting point.

### 8.5 Direct SparkSQL-to-DuckDB Transpilation (Without AST)

**Approach:** Write an ANTLR visitor that emits DuckDB SQL directly from the parse tree, without constructing a LogicalPlan.

**Why rejected:**
- **Misses the type inference layer.** Without a LogicalPlan, there is no `inferSchema()`, no `SchemaCorrectedBatchIterator`, and no type-aware SQL generation. This is exactly the gap that causes the 4 skipped tests.
- **Creates a third code path.** The CLAUDE.md already warns about dual SQL generation paths. A transpiler would be a third path that must be kept in sync with the other two.
- **No optimization opportunity.** String-to-string transpilation cannot benefit from LogicalPlan-level optimizations.
- **Faster to implement but harder to maintain.** Initial development might be faster, but every SparkSQL-to-DuckDB difference must be handled with per-rule logic, rather than leveraging the existing SQLGenerator.

**Verdict:** Going through the AST costs ~1 week of additional implementation but pays back in maintainability and correctness permanently.

---

## 9. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| ANTLR4 DFA cold start adds latency spike on first query | High | Medium | Pre-warm parser on server startup by parsing a canonical query |
| Grammar generates code that conflicts with existing packages | Low | High | Put generated code in distinct package (`com.thunderduck.parser.generated`) |
| Some SparkSQL syntax not translatable to DuckDB | Medium | Medium | Return `UnsupportedOperationException` with clear message; fall back to regex path |
| Thread safety issues with shared parser | Medium | High | ThreadLocal parser pool; add unit tests for concurrent parsing |
| Memory growth from DFA cache | Low | Medium | Periodic `atn.clearDFA()` with metrics monitoring |
| Build time increase from ANTLR code generation | Low | Low | ANTLR4 codegen takes <5 seconds; incremental builds cache generated code |

---

## 10. Testing Strategy

### 10.1 Unit Tests

- Parse validity tests: every TPC-H and TPC-DS query string should parse without errors
- AST structure tests: verify parse tree -> LogicalPlan conversion for canonical patterns
- Error handling tests: malformed SQL produces `SparkSQLParseException` with line/column
- Function mapping tests: SparkSQL function names map to DuckDB equivalents

### 10.2 Differential Tests (AST vs Regex)

During the parallel execution phase (Phase A), for every `spark.sql()` call:
1. Generate DuckDB SQL via AST path
2. Generate DuckDB SQL via regex path
3. Compare results (allowing for whitespace/formatting differences)
4. Log discrepancies as warnings

### 10.3 Integration Tests

- All 22 TPC-H queries via `spark.sql()` must produce identical results to Spark reference
- All 91 TPC-DS queries via `spark.sql()` must produce identical results
- All 4 currently-skipped type casting tests must pass
- Mixed workload: DataFrame API + spark.sql() in same session

### 10.4 Performance Tests

- Benchmark parse time for all TPC-H queries (target: < 5ms each)
- Benchmark parse time for all TPC-DS queries (target: < 10ms each)
- Measure cold start time (first query) -- target: < 500ms with SLL
- Measure memory usage after 1000 unique query parses -- target: < 100MB DFA cache

---

## 11. Appendix: SparkSQL vs DuckDB Syntax Differences

This table catalogs known syntax differences that the parser/AST builder must handle:

| SparkSQL Syntax | DuckDB Equivalent | Transformation |
|----------------|-------------------|----------------|
| `` `identifier` `` (backticks) | `"identifier"` (double quotes) | Lexer-level or visitor-level |
| `DATE '2025-01-15'` | `DATE '2025-01-15'` | Pass through (DuckDB supports typed literals) |
| `TIMESTAMP '2025-01-15 10:30:00'` | `TIMESTAMP '2025-01-15 10:30:00'` | Pass through |
| `CAST(x AS STRING)` | `CAST(x AS VARCHAR)` | Type name mapping |
| `CAST(x AS INT)` | `CAST(TRUNC(x) AS INTEGER)` | Truncation semantics |
| `interval '3' month` | `INTERVAL 3 MONTH` | Syntax transformation |
| `interval '1' year` | `INTERVAL 1 YEAR` | Syntax transformation |
| `year(date_col)` | `CAST(year(date_col) AS INTEGER)` | Return type correction |
| `NAMED_STRUCT('a', 1, 'b', 2)` | `struct_pack(a := 1, b := 2)` | Function restructuring |
| `MAP('k1', v1, 'k2', v2)` | `MAP(['k1','k2'], [v1,v2])` | Argument restructuring |
| `ARRAY(1, 2, 3)` | `list_value(1, 2, 3)` | Function rename |
| `STRUCT(1, 2, 3)` | `row(1, 2, 3)` | Function rename |
| `collect_list(col)` | `list(col) FILTER (WHERE col IS NOT NULL)` | Semantic transformation |
| `collect_set(col)` | `list(DISTINCT col) FILTER (WHERE col IS NOT NULL)` | Semantic transformation |
| `size(array_col)` | `len(array_col)` | Function rename |
| `SUM(x)` (strict mode) | `spark_sum(x)` | Extension function (strict) |
| `AVG(x)` (strict mode) | `spark_avg(x)` | Extension function (strict) |
| `LATERAL VIEW explode(arr) t AS c` | DuckDB `UNNEST` or lateral join | Complex transformation |
| `x + y` (string operands) | `x \|\| y` | Operator overload resolution |
| `LEFT SEMI JOIN` | `SEMI JOIN` | Remove `LEFT` prefix |
| `LEFT ANTI JOIN` | `ANTI JOIN` | Remove `LEFT` prefix |
| `RLIKE 'pattern'` | `regexp_matches(col, 'pattern')` | Function transformation |
| `NVL(a, b)` | `COALESCE(a, b)` | Function rename |
| `IF(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` | Control flow transformation |

---

## 12. Decision Log

| # | Decision | Rationale | Date |
|---|----------|-----------|------|
| 1 | Parse to LogicalPlan AST, not direct transpilation | Single code path, type inference, schema correction | 2026-02-09 |
| 2 | Use ANTLR4 Visitor pattern, not Listener | Return values needed for AST construction | 2026-02-09 |
| 3 | Import grammar from Spark v4.0.0, not master | Stable baseline, forward compatible | 2026-02-09 |
| 4 | ThreadLocal parser pool, not shared instance | Thread safety without lock contention | 2026-02-09 |
| 5 | SLL-first with LL fallback | Fast path for 95% of queries | 2026-02-09 |
| 6 | No pass-through optimization | Simpler architecture, negligible overhead | 2026-02-09 |
| 7 | No parse tree caching | Analytics workloads rarely repeat verbatim | 2026-02-09 |
| 8 | Incremental adoption with parallel/canary mode | Zero-risk migration path | 2026-02-09 |
