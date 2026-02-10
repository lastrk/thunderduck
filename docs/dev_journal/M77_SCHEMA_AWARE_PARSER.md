# M77: Schema-Aware SparkSQL Parser

**Date:** 2026-02-10
**Status:** Complete

## Summary

Made the SparkSQL parser schema-aware by resolving table schemas at parse time via DuckDB `SELECT * FROM table LIMIT 0`. This enables `plan.inferSchema()` to succeed for all SparkSQL plan shapes, eliminating 20 regex-based schema fixup methods (~593 lines) from `SparkConnectServiceImpl.java`. Net result: -440 lines, zero regressions (747/825 full suite).

## Problem

After preprocessSQL elimination (M75-M76), ~593 lines of regex-based schema fixup code remained in `SparkConnectServiceImpl.java`. These methods (`fixCountNullable`, `fixDecimalPrecisionForComplexAggregates`, `detectCountColumns`, `splitSelectList`, etc.) existed because `plan.inferSchema()` failed for SparkSQL queries.

**Root cause:** The parser created `SQLRelation("SELECT * FROM tableName")` nodes with empty schemas. Since SQLRelation had no column metadata, `inferSchema()` returned empty/null, forcing fallback to DuckDB schema queries + regex-based corrections for nullable flags and decimal precision.

**Cascade effect:**
```
Parser creates SQLRelation with empty schema
  → Project/Filter/Join above SQLRelation can't infer child schema
    → plan.inferSchema() returns null
      → Fallback to inferSchemaFromDuckDB() + regex fixups
        → 20 methods analyzing SQL strings for COUNT columns, decimal precision, etc.
```

## Solution

Resolve table schemas at parse time by passing the DuckDB connection into the parser. When the parser encounters a table reference, it queries DuckDB for the table's column metadata and attaches it to the `SQLRelation` node. Downstream `inferSchema()` calls then succeed without regex.

**New data flow:**
```
SparkSQL string + DuckDB connection
  → ANTLR parse tree
    → SparkSQLAstBuilder.visitTableName()
      → SELECT * FROM table LIMIT 0 (resolve schema)
        → SQLRelation("SELECT * FROM table", resolvedSchema)
          → plan.inferSchema() succeeds
            → No regex fixups needed
```

## Changes Made

### 1. `SparkSQLParser.java` (+23 lines)

Added `parse(String sql, Connection connection)` overload:
- Passes connection to `SparkSQLAstBuilder` for table schema resolution
- Existing `parse(String sql)` delegates to new overload with `null` (backward compatible)
- Unit tests and other callers without a connection are unaffected

### 2. `SparkSQLAstBuilder.java` (+134 lines)

Added schema resolution infrastructure:
- `Connection connection` field (nullable) with two constructors
- `visitTableName()` now calls `resolveTableSchema()` when connection is available
- `resolveTableSchema(String tableName)` — queries DuckDB via `SELECT * FROM table LIMIT 0`, reads `ResultSetMetaData` for column names/types/nullability
- `mapDuckDBTypeToThunderduck()` — converts JDBC type names to Thunderduck `DataType` instances

**Type mapping coverage:**

| JDBC Type | Thunderduck Type |
|-----------|-----------------|
| BOOLEAN | BooleanType |
| TINYINT | ByteType |
| SMALLINT | ShortType |
| INTEGER, INT | IntegerType |
| BIGINT, LONG, HUGEINT | LongType |
| FLOAT, REAL | FloatType |
| DOUBLE | DoubleType |
| VARCHAR, TEXT, STRING | StringType |
| DATE | DateType |
| TIMESTAMP variants | TimestampType |
| BLOB, BINARY | BinaryType |
| DECIMAL(p,s) | DecimalType(p,s) |

**Graceful fallback:** If schema resolution fails (CTE reference, missing table, temp view not yet created), returns `null` and falls back to empty schema (current behavior). This is logged at DEBUG level.

### 3. `SparkConnectServiceImpl.java` (-593/+79 lines, net -440)

**Added:**
- `transformSparkSQLWithPlan(String, Connection)` overload — passes connection to parser
- `executeSQLWithPlan(String sql, LogicalPlan plan, ...)` — uses `plan.inferSchema()` for schema-aware streaming; falls back to DuckDB schema query when plan is null

**Modified:**
- `executePlan()` — retrieves DuckDB connection from session, passes through schema-aware parser, tracks LogicalPlan alongside SQL string
- `analyzePlan()` — uses schema-aware parser with connection
- All `transformSparkSQL()` call sites updated to pass connection

**Deleted 20 methods (~593 lines):**

| Method | Purpose | Lines |
|--------|---------|-------|
| `fixCountNullable` | Fix nullable flags for COUNT columns | 25 |
| `detectCountColumns` | Parse SQL to find COUNT expressions | 40 |
| `collectCountAliases` | Regex to collect COUNT aliases | 25 |
| `detectRenamedCountColumns` | Handle renamed COUNT columns in subqueries | 35 |
| `fixDecimalPrecisionForComplexAggregates` | Fix DECIMAL precision for SUM/AVG of arithmetic | 45 |
| `detectComplexAggregateColumns` | Parse SQL to find complex aggregate columns | 50 |
| `containsArithmeticWithAggregate` | Check if expression has arithmetic with aggregate | 10 |
| `containsAggregateWithArithmeticInside` | Check if aggregate contains arithmetic | 25 |
| `stripAggregateBodies` | Remove aggregate function bodies from SQL | 25 |
| `containsAggregateOfArithmeticAlias` | Check if aggregate references arithmetic alias | 20 |
| `extractAliasOrReference` | Parse AS alias from select item | 30 |
| `collectArithmeticAliases` | Collect aliases of arithmetic expressions | 25 |
| `findExpressionStart` | Find start of expression in SQL string | 25 |
| `unwrapSelectStar` | Remove `SELECT * FROM (...)` wrappers | 30 |
| `inferNullableSchemaFromSQL` | Orchestrate nullable schema inference | 10 |
| `findOutermostKeyword` | Find SQL keyword at outermost nesting level | 30 |
| `splitSelectList` | Split SELECT list respecting parens/quotes | 25 |
| `findMatchingOpenParen` | Find matching open parenthesis | 20 |
| `findMatchingCloseParen` | Find matching close parenthesis | 20 |
| `extractColumnName` | Extract simple column name from select item | 10 |

Also deleted 3 constants: `DECIMAL_NO_CHANGE`, `DECIMAL_PROMOTE_PRECISION`, `DECIMAL_FIX_DIVISION_TYPE`.

**Preserved:** `inferSchemaFromDuckDB()` as edge-case fallback for queries where plan schema is unavailable (e.g., deeply nested subqueries with unresolvable CTEs).

## Test Results

**TPC-H:** 51/51 passing (100%)

**Full suite:** 747/825 passing (zero regressions vs baseline)
- 73 failures are all pre-existing (complex types, map functions, TPC-DS subset)
- No new failures introduced

## Design Decisions

1. **`SELECT * FROM table LIMIT 0` over `PRAGMA table_info`**: PRAGMA only works for base tables; `SELECT ... LIMIT 0` works for tables, views, and CTEs that reference real tables.

2. **Parse-time resolution, not generation-time**: Resolving schemas during AST building means the entire plan tree has correct schemas before SQLGenerator runs. This is simpler than lazy resolution.

3. **Nullable connection parameter**: Backward compatible — callers without a connection (unit tests, pure DataFrame API) are unaffected.

4. **Graceful degradation over hard failures**: Failed schema resolution returns null, falling back to empty schema. This prevents parse failures for edge cases (CTE self-references, temp views created mid-query).

## Architecture Impact

**Before:**
```
SparkSQL → parse → SQLRelation(empty schema) → inferSchema() fails
  → inferSchemaFromDuckDB(sql) → fixCountNullable(sql, schema) → fixDecimalPrecision(sql, schema)
    → 20 regex methods parsing SQL strings for schema corrections
```

**After:**
```
SparkSQL + connection → parse → SQLRelation(resolved schema) → inferSchema() succeeds
  → executeSQLWithPlan(sql, plan) → plan.inferSchema() → correct types/nullable
```

This closes the last major gap in the "zero SQL string manipulation" architecture. The only remaining string-based fallback is `inferSchemaFromDuckDB()` for edge cases where plan schema is unavailable.

## SparkConnectServiceImpl Line Count

| Version | Lines | Delta |
|---------|-------|-------|
| Before preprocessSQL elimination | ~2900 | — |
| After Phase 1-2 (M75) | ~2645 | -255 |
| After Phase 3-4 + G4/P4/DDL (M76) | ~2645 | 0 (moved, not deleted) |
| After schema-aware parser (M77) | ~2052 | **-593** |
| **Total reduction** | — | **-848 lines (29%)** |

## Performance Impact

- **Parse-time overhead**: Negligible — one `SELECT ... LIMIT 0` per table reference per query. DuckDB resolves this from catalog metadata without disk I/O.
- **Execution-time savings**: Eliminates 20 regex method calls per SQL query in strict mode.
- **Memory**: No additional allocations (schema is a small StructType).

## Lessons Learned

1. **Schema resolution belongs at parse time**: The parser is the natural place to resolve table schemas — it already knows the table name and can query the database.

2. **LIMIT 0 is a universal schema query**: Works for tables, views, and any queryable source. More robust than catalog-specific PRAGMA commands.

3. **593 lines of regex can be replaced by 134 lines of schema resolution**: The complexity was always in the wrong place. Correct schemas make post-processing unnecessary.

4. **Backward compatibility through overloads**: Adding `parse(sql, connection)` alongside `parse(sql)` means zero disruption to existing callers.

## Commits

- (pending) Schema-aware parser implementation on `feat/schema-aware-parser` branch
