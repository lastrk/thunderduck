# Full Differential Test Suite Baseline (Relaxed Mode)

**Date**: 2026-02-11
**Mode**: `THUNDERDUCK_COMPAT_MODE=relaxed`
**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **737 passed, 0 failed, 2 skipped** (739 total, 154s)

**Previous baselines**: 646/88/5 (2026-02-09) → 708/26/5 (2026-02-10) → 718/16/5 (2026-02-11 AM) → 733/1/5 (2026-02-11 PM) → **737/0/2** (2026-02-11 evening)

**TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
**TPC-DS**: 99/99 (100%) — all SQL queries passing

## Progress Summary

From the original 88 failures + 5 skips, all failures are fixed and 3 of 5 skips are resolved:

| Worktree | Scope | Fixed | Remaining |
|----------|-------|-------|-----------|
| **WT1**: Subquery alias scoping | 35 | 35 | 0 |
| **WT2**: Test infra cleanup | 17 | 17 | 0 |
| **WT3**: Quick SQL generator wins | 16 | 16 | 0 |
| **WT4**: Type system + complex types | 12 | 12 | 0 |
| **WT5**: Decimal precision + Q77 | 6 | 6 | 0 |
| **WT6**: Semi/anti join chain | 1 | 1 | 0 |
| Direct fixes (main) | 3 | 3 | 0 |
| Q14b cross-join column dedup | 1 | 1 | 0 |
| Skipped → passing | 3 | 3 | 0 |
| **Total** | **94** | **94** | **0** |

---

## Remaining 2 Skipped Tests (Relaxed Mode Divergences)

| Test | Reason | Resolution |
|------|--------|------------|
| test_array_negative_index_last | DuckDB supports `arr[-1]`; Spark throws ArrayIndexOutOfBoundsException | Strict mode via DuckDB extension |
| test_array_negative_index_second_last | DuckDB supports `arr[-2]`; Spark throws ArrayIndexOutOfBoundsException | Strict mode via DuckDB extension |

These are marked `@pytest.mark.skip_relaxed` — they will run automatically in strict mode.

---

## Recent Fixes (2026-02-11 evening session)

### Q14b: Cross-Join Column Deduplication
- DuckDB auto-renames duplicate column names with `:1` suffixes in `SELECT *`
- Fix: Expand `SELECT *` to explicit qualified column list when join sources have overlapping names
- Also extended `canAppendClause()` for Sort to prevent double `SELECT *` wrapping

### months_between: Spark-Exact Algorithm
- Replaced approximate `datediff/31.0` with exact CASE expression matching Spark's algorithm
- Handles same-day, both-last-day-of-month, and fractional cases with 8-decimal rounding

### next_day: Pure SQL Implementation
- DuckDB has no native `next_day` — implemented via dayofweek arithmetic CASE expression
- Handles all Spark abbreviations (SU/SUN/SUNDAY, MO/MON/MONDAY, etc.)

### CAST Truncation: Three-Mode TruncMode
- Fixed 3 bugs in dead TRUNC-wrapping code: exclusion-based type checking, all integral targets, unified code path
- `TruncMode.VIA_DOUBLE` handles UnresolvedColumn safely via `TRUNC(CAST(x AS DOUBLE))`

---

## What Was Fixed (all 88 original failures + 3 skips)

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

### WT5: Decimal Precision + Q77 (6 fixed, including Q14b)
- AVG DECIMAL precision: cast to DOUBLE in relaxed mode for full precision
- TypeInferenceEngine: correct AVG return type DECIMAL(min(p+4,38), min(s+4,p+4))
- Q77 deterministic sort: tiebreaker columns for ROLLUP ties
- Q14b: cross-join SELECT * column deduplication

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

### Skipped → Passing (3 unskipped)
- months_between: Spark-exact fractional month CASE expression
- next_day: Pure SQL dayofweek arithmetic implementation
- CAST double-to-int: Three-mode TRUNC strategy (NONE/DIRECT/VIA_DOUBLE)
