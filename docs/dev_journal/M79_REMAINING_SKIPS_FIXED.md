# M79: Final Test Suite Fixes — Q14b, months_between, next_day, CAST Truncation

**Date**: 2026-02-11
**Scope**: Fix last failure (Q14b) and 3 of 5 skipped tests
**Result**: 734/0/5 → **737/0/2** (737 passed, 0 failed, 2 skipped)

## Q14b Fix: DuckDB SELECT * Column Deduplication

**Problem**: TPC-DS Q14b does `SELECT * FROM (subquery) this_year, (subquery) last_year` where both subqueries produce identical column names. DuckDB auto-deduplicates with `:1` suffixes; Spark follows the SQL standard and keeps duplicates.

**Fix** (two parts):
1. **Expand `SELECT *` to explicit qualified columns**: Added `hasOverlappingColumns()` and `buildExplicitSelectForJoinChain()` helpers in `SQLGenerator.java`. When a flat join chain has overlapping column names across sources, `SELECT *` is replaced with `SELECT "this_year"."channel", ..., "last_year"."channel", ...`. DuckDB does NOT auto-rename explicitly listed columns.

2. **Extend `canAppendClause()` for Sort**: `visitLimit()` was wrapping the query in `SELECT * FROM (...) AS subquery_1 LIMIT N`, triggering a second DuckDB dedup layer. Adding Sort to `canAppendClause()` allows `ORDER BY ... LIMIT N` to be appended directly.

**Files**: `core/.../generator/SQLGenerator.java`

## months_between: Spark-Exact Algorithm

**Problem**: Previous implementation used `datediff('day', d2, d1) / 31.0` — a rough approximation that diverges from Spark's exact fractional month calculation.

**Fix**: Replaced with Spark-exact CASE expression in `FunctionRegistry`:
- If `day(d1) == day(d2)` OR both are last day of their month → exact integer month difference as DOUBLE
- Otherwise → `ROUND(monthDiff + (day1 - day2) / 31.0, 8)`

**Files**: `core/.../functions/FunctionRegistry.java`

## next_day: Pure SQL Implementation

**Problem**: FunctionRegistry had `DIRECT_MAPPINGS.put("next_day", "next_day")` but DuckDB has no native `next_day` function — calls failed at runtime.

**Fix**: Replaced with CUSTOM_TRANSLATOR using dayofweek arithmetic:
1. Map day-of-week strings (SU/SUN/SUNDAY, MO/MON/MONDAY, etc.) to DuckDB `dayofweek` integers (0-6)
2. Compute `((targetDow - currentDow + 7) % 7)`, use 7 when result is 0 (strictly after)
3. Cast days-to-add to INTEGER (required: DuckDB's `DATE + BIGINT` is not supported, only `DATE + INTEGER`)

**Files**: `core/.../functions/FunctionRegistry.java`

## CAST Truncation: Three-Mode TruncMode Strategy

**Problem**: DuckDB's `CAST(x AS INTEGER)` rounds floating-point values (PostgreSQL semantics), while Spark truncates toward zero (Java semantics). Existing TRUNC-wrapping code was dead due to three bugs:
1. `needsTrunc()` only matched known float types; `UnresolvedColumn.dataType()` returns `StringType` placeholder
2. Target type check only covered INTEGER, missing BIGINT/SMALLINT/TINYINT
3. `SQLGenerator.qualifyCondition()` bypassed `CastExpression.toSQL()`

**Fix**: Introduced `TruncMode` enum:
| Mode | When | SQL Generated |
|------|------|--------------|
| `NONE` | Source is integer, boolean, date, timestamp, binary | `CAST(x AS INT)` |
| `DIRECT` | Source is known DOUBLE/FLOAT/DECIMAL | `CAST(TRUNC(x) AS INT)` |
| `VIA_DOUBLE` | Source type unknown (UnresolvedColumn) | `CAST(TRUNC(CAST(x AS DOUBLE)) AS INT)` |

Extracted `CastExpression.generateCastSQL()` as shared public helper used by both `toSQL()` and `SQLGenerator.qualifyCondition()`.

**Files**: `core/.../expression/CastExpression.java`, `core/.../generator/SQLGenerator.java`

## Remaining 2 Skipped Tests

Both are relaxed mode divergences for negative array indices — DuckDB returns elements from the end, Spark throws `ArrayIndexOutOfBoundsException`. Marked as `skip_relaxed` to run in strict mode when the DuckDB extension handles them.

## Worktree Workflow

All fixes used git worktrees for parallel development:
- `feat/array-index-oob` → merged to main (skip markers)
- `feat/datetime-extension` → merged to main (months_between + next_day)
- `feat/cast-truncation` → merged to main (CAST truncation)
