# Full Differential Test Suite Baseline (Relaxed Mode)

**Date**: 2026-02-11
**Mode**: `THUNDERDUCK_COMPAT_MODE=relaxed`
**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **733 passed, 1 failed, 5 skipped** (739 total, 232s)

**Previous baselines**: 646/88/5 (2026-02-09) → 708/26/5 (2026-02-10) → 718/16/5 (2026-02-11 AM) → 733/1/5 (2026-02-11 PM)

**TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
**TPC-DS**: 98/99 (98.9%) — Q14b is the sole remaining failure

## Progress Summary

From the original 88 failures, 87 have been fixed across 6 worktrees plus direct main commits:

| Worktree | Scope | Fixed | Remaining |
|----------|-------|-------|-----------|
| **WT1**: Subquery alias scoping | 35 | 35 | 0 |
| **WT2**: Test infra cleanup | 17 | 17 | 0 |
| **WT3**: Quick SQL generator wins | 16 | 16 | 0 |
| **WT4**: Type system + complex types | 12 | 12 | 0 |
| **WT5**: Decimal precision + Q77 | 6 | 5 | 1 (Q14b) |
| **WT6**: Semi/anti join chain | 1 | 1 | 0 |
| Direct fixes (main) | 3 | 3 | 0 |
| **Total** | **90** | **89** | **1** |

---

## Remaining 1 Failure

### TPC-DS Q14b: Cross-Join Column Ordering

Originally classified as decimal precision — actually a Spark-vs-DuckDB semantics difference in how `SELECT *` from a cross join with identically-named subquery aliases orders columns.

- TPC-DS: Q14b

**Component**: SQL Generator (cross-join column deduplication/ordering)
**Effort**: Medium (investigation needed to understand column ordering difference)

---

## Skipped Tests (5)

| Test | Reason |
|------|--------|
| test_array_negative_index_last | Spark throws on negative index; DuckDB supports it |
| test_array_negative_index_second_last | Same as above |
| test_months_between | Complex fractional month calculation — pending |
| test_next_day | DuckDB lacks next_day function |
| test_double_to_int | DuckDB rounds (-3.7 → -4), Spark truncates (-3.7 → -3) |

---

## What Was Fixed (87 of 88 failures)

### WT1: Subquery Alias Scoping (35 fixed)
- Two-pass FROM-first approach for join alias mapping (fixes alias prediction errors)
- Qualified aggregate SELECT/GROUP BY/HAVING with table aliases on join paths
- Merged `generateAggregateOnJoin`/`generateAggregateOnFilterJoin` into unified method
- Added `generateProjectOnFilterAliased()` for Project(Filter(AliasedRelation)) pattern
- Extended `canAppendClause()` for Filter(Join) to preserve aliases in ORDER BY

### WT2: Test Infrastructure (17 fixed)
- Cleaned spark-warehouse/ before tests, used unique table names

### WT3: Quick SQL Generator Wins (16 fixed)
- Fixed aggregate column naming (lowercase sum/avg/count)
- Added multi-word alias quoting in SparkSQL parser
- Fixed selectExpr alias leaking full expression
- Fixed grouping_id() bit ordering

### WT4: Type System + Complex Types (12 fixed)
- MAP-aware polymorphic dispatch: `size(MAP)` → `cardinality()`, `element_at(MAP)` → bracket notation, `explode(MAP)` → unnest(map_keys/values)
- Recursive polymorphic function resolution through BinaryExpression/CastExpression
- Filter condition polymorphic resolution (`resolveFilterConditionSQL()`)
- MAP explode schema inference in `Project.inferSchema()` (key + value columns)
- Interval DATE→TIMESTAMP: CAST result back to DATE when input is DATE
- Overflow detection: CAST SUM to BIGINT to trigger overflow
- Complex type literals: empty array syntax, struct literal without named fields

### WT5: Decimal Precision + Q77 (5 fixed)
- AVG DECIMAL precision: cast to DOUBLE in relaxed mode for full precision
- TypeInferenceEngine: correct AVG return type DECIMAL(min(p+4,38), min(s+4,p+4))
- Q77 deterministic sort: tiebreaker columns for ROLLUP ties

### WT6: Semi/Anti Join Chain Breaking (1 fixed)
- `containsSemiOrAntiJoin()` guard on all flat-join-chain callers
- `collectJoinParts()` stops recursion at SEMI/ANTI boundaries
- Q21 DataFrame alias "l1" now correctly scoped

### Direct Main Fixes (3 fixed)
- Empty schema fallback (`schema == null || schema.size() == 0`)
- StarExpression expansion in `Project.inferSchema()`
- GROUP BY column preservation for columns not in SELECT
- VALUES/InlineTable support via `visitInlineTableDefault1/2`
- Catalog `listFunctions` AIOOBE fix (`setSafe()`)
- Pre-compiled regex patterns, SLF4J logger, code quality improvements
