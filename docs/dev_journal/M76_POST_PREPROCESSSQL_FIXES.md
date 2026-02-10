# M76: Post-preprocessSQL Bug Fixes and DDL/DML Support

**Date:** 2026-02-10
**Status:** Complete

## Summary

This milestone resolved three parser gaps discovered after `preprocessSQL()` elimination (G4: division type dispatch, P4: CROSS JOIN handling, P2: RETURNS keyword), added full DDL/DML parser support with Spark-to-DuckDB type mapping, and closed out the preprocessSQL elimination effort with 51/51 TPC-H queries passing through the pure AST path.

## Work Completed

### 1. Type-Aware Division Dispatch (G4)

**Commit:** `236993e`

**Problem:** After `preprocessSQL()` removal, division expressions like `SUM(a) / SUM(b)` produced incorrect types. The old `fixDivisionDecimalCasts()` regex had been applying `spark_decimal_div()` to DECIMAL/DECIMAL divisions, but the AST path was using native DuckDB division.

**Solution:** `BinaryExpression.toSQL()` now inspects operand `dataType()` to choose division strategy in strict mode:

| Left Type | Right Type | Strategy | SQL Generated |
|-----------|-----------|----------|---------------|
| DECIMAL | DECIMAL | Extension function | `spark_decimal_div(left, right)` |
| INTEGER/BIGINT | INTEGER/BIGINT | Cast to DOUBLE | `CAST(left AS DOUBLE) / right` |
| DECIMAL | INTEGER | Promote integer | `spark_decimal_div(left, CAST(right AS DECIMAL(...)))` |
| FLOAT/DOUBLE | Any | Native division | `left / right` |
| Unknown | Any | Native fallback | `left / right` |

**Key insight:** Type dispatch belongs in the expression tree, not in regex post-processing.

**Impact:** TPC-H Q1 and other division-heavy queries now produce Spark-correct DECIMAL types in strict mode.

**Files Modified:**
| File | Change |
|------|--------|
| `core/.../expression/BinaryExpression.java` | Add type-aware division dispatch in `toSQL()` |
| `core/.../types/TypeInferenceEngine.java` | Add division type inference rules |

### 2. CROSS JOIN Null Condition Fix (P4)

**Commit:** `bf7b8b7`

**Problem 1:** The ANTLR grammar already treated `AT` as a non-reserved keyword, so `SELECT * FROM foo at CROSS JOIN bar` parsed correctly with `at` as table alias. But the AST builder set `condition = Literal.of(true)` for CROSS joins, violating the `Join` constructor's invariant (CROSS joins must have null condition).

**Problem 2:** Unaliased table references in comma-separated FROM clauses (implicit CROSS joins) lost their original table names when wrapped in subqueries, causing `table.column` references in WHERE clauses to fail.

**Solution:**
- Part 1: Fix AST builder to pass `null` condition for CROSS joins (not `Literal.of(true)`)
- Part 2: Add implicit `AliasedRelation` wrapping for unaliased tables in comma-separated FROM clauses, preserving table name for qualified column references

**Test:** Added TPC-DS Q90 test (Thunderduck-only; Spark cannot run due to DIVIDE_BY_ZERO error). Q90 uses multiple CROSS joins with `AT` aliases.

**Result:** P4 resolved. CROSS JOIN handling is fully correct.

**Files Modified:**
| File | Change |
|------|--------|
| `core/.../parser/SparkSQLAstBuilder.java` | Fix CROSS JOIN condition + implicit aliasing |
| `tests/.../differential/test_tpcds_differential.py` | Add Q90 test (Thunderduck-only) |

### 3. RETURNS Keyword Resolution (P2)

**Commit:** `0326930`

**Status:** Resolved with no code change needed.

**Finding:** `RETURNS` is already in the ANTLR grammar's `nonReserved` rule (line 2192) and `ansiNonReserved` rule (line 1809). The parser handles it correctly as a column alias.

**Evidence:** TPC-H queries using `RETURNS` as an alias parse without errors.

**Decision:** Mark P2 as resolved. No implementation work required.

### 4. DDL/DML Parser Support

**Commit:** `371f8c9`

**Problem:** After `preprocessSQL()` removal, DDL/DML statements (CREATE TABLE, INSERT INTO, etc.) had no AST representation. They fell back to... nothing (preprocessSQL was deleted).

**Solution:** Added 7 DDL/DML visitor methods to `SparkSQLAstBuilder` with Spark-to-DuckDB type mapping:

**Supported statements:**
- `CREATE TABLE` (with column definitions + data types)
- `CREATE TABLE AS SELECT` (CTAS)
- `DROP TABLE`
- `TRUNCATE TABLE`
- `CREATE VIEW` / `DROP VIEW`
- `INSERT INTO ... VALUES`
- `INSERT INTO ... SELECT`
- `ALTER TABLE ADD COLUMN`

**Type mapping:**

| Spark Type | DuckDB Type | Preserves Precision/Scale |
|------------|-------------|---------------------------|
| `STRING` | `VARCHAR` | N/A |
| `LONG` | `BIGINT` | Yes |
| `SHORT` | `SMALLINT` | Yes |
| `BYTE` | `TINYINT` | Yes |
| `INT` / `INTEGER` | `INTEGER` | Yes |
| `FLOAT` | `FLOAT` | Yes |
| `DOUBLE` | `DOUBLE` | Yes |
| `BOOLEAN` | `BOOLEAN` | Yes |
| `DATE` | `DATE` | Yes |
| `TIMESTAMP` | `TIMESTAMP` | Yes |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Yes |
| `ARRAY<T>` | `T[]` | Recursive |
| `MAP<K,V>` | `MAP(K,V)` | Recursive |
| `STRUCT<...>` | `STRUCT(...)` | Recursive |

**Architecture:**

```
SparkSQL DDL string
  → ANTLR parse tree (DDL-specific visitor methods)
  → RawDDLStatement LogicalPlan node (contains transformed SQL)
  → SQLGenerator.visitRawDDLStatement()
  → DuckDB-compatible SQL string
```

**Key decisions:**
- DDL/DML statements are **pass-through** (minimal AST representation)
- Type mapping happens at parse time (not SQL generation time)
- Inner SELECT clauses go through full AST parsing for type safety
- Identifiers are quoted to preserve case sensitivity

**Files Modified:**
| File | Change |
|------|--------|
| `core/.../parser/SparkSQLAstBuilder.java` | Add 7 DDL/DML visitor methods (350+ lines) |
| `core/.../logical/RawDDLStatement.java` | **NEW** - LogicalPlan node for DDL pass-through |
| `core/.../generator/SQLGenerator.java` | Add `visitRawDDLStatement()` |
| `core/.../types/TypeMapper.java` | **NEW** - Spark-to-DuckDB type mapping |
| `tests/.../test_ddl_parsing.py` | **NEW** - 13 integration tests |

**Test coverage:**
- CREATE TABLE with all data types (primitives + complex)
- CREATE TABLE AS SELECT (CTAS)
- CREATE/DROP VIEW
- INSERT INTO ... VALUES / SELECT
- ALTER TABLE ADD COLUMN
- TRUNCATE TABLE
- DROP TABLE

**Result:** 13/13 DDL tests pass. 51/51 TPC-H pass (zero regressions). DDL/DML statements no longer require special handling.

## Test Results

**TPC-H:** 51/51 passing (100%)

**Full suite:** 747/815 passing (91.7%)
- 0 regressions from G4, P4, P2, DDL work
- All preprocessSQL-related work complete

## Key Insights

1. **Type dispatch is an expression concern**: Division type dispatch belongs in `BinaryExpression.toSQL()`, not in regex post-processing. The expression tree knows the operand types; use that information.

2. **Grammar bugs are rare; AST builder bugs are common**: The ANTLR grammar was 99% correct. Most bugs were in the visitor (wrong condition for CROSS joins, missing implicit aliases).

3. **DDL/DML is simpler than SELECT**: DDL statements have fixed structure. No need for complex AST nodes — just parse structure, map types, and pass through.

4. **Type mapping is finite**: 15 Spark types map to 15 DuckDB types. Complex types (ARRAY, MAP, STRUCT) are recursive but straightforward.

5. **Zero-regression validation is critical**: After every fix, run 51 TPC-H queries + full suite. Catch regressions immediately, not 3 commits later.

## Performance Impact

- **Division expressions**: No overhead (type dispatch is compile-time in JIT)
- **DDL/DML**: Same performance as before (pass-through strategy)

## Lessons Learned

Added to `tasks/lessons.md`:
- "When adding type-aware behavior, inspect operand types in the expression tree — never regex the generated SQL."
- "Grammar non-reserved keywords are often sufficient. Check the grammar before adding special cases."
- "DDL/DML pass-through is simpler than building full AST nodes. Map types at parse time, emit directly."

## Documentation Updates

**Commit:** `e40ab43`

Updated `CURRENT_FOCUS_PREPROCESSSQL_ELIMINATION.md` to reflect:
- Phase 3-4 complete (preprocessSQL deleted)
- G4 resolved (division dispatch)
- P4 resolved (CROSS JOIN)
- P2 resolved (RETURNS keyword)
- DDL/DML support added
- 51/51 TPC-H achieved

## Community Contributions

None (internal development).

## Commits (chronological)

- `0326930` - Mark P2 (returns keyword) as resolved — no code change needed
- `236993e` - Add type-aware division dispatch in strict mode (G4)
- `bf7b8b7` - Fix CROSS JOIN handling and add TPC-DS Q90 test (P4)
- `371f8c9` - Add DDL/DML parser support with Spark-to-DuckDB type mapping
- `a75c7e6` - Merge branch 'feat/ddl-dml-support'
