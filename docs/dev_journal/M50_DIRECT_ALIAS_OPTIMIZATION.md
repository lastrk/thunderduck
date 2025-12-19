# M50: Direct Alias Optimization for Join Sources

**Date**: 2025-12-19
**Status**: Complete

## Summary

Implemented an optimization to eliminate unnecessary subquery wrapping for simple table sources in joins. This produces cleaner, more readable SQL output.

## Problem Statement

Previously, joins generated verbose SQL with redundant subquery wrapping:

```sql
-- Before (verbose)
SELECT * FROM (SELECT * FROM "users") AS subquery_1
INNER JOIN (SELECT * FROM "orders") AS subquery_2
ON (subquery_1."id" = subquery_2."id")
```

This extra wrapping was unnecessary for simple table sources and made the SQL harder to read and debug.

## Solution

Added a `getDirectlyAliasableSource()` helper method that identifies `TableScan` nodes which can be directly aliased without subquery wrapping. Updated `visitJoin()` to use direct aliasing for:

- TABLE format: `"tablename" AS alias`
- PARQUET format: `read_parquet('file.parquet') AS alias`
- DELTA format: `delta_scan('/path') AS alias`
- ICEBERG format: `iceberg_scan('/path') AS alias`

Complex sources (Filter, Project, Aggregate, etc.) continue to be wrapped as before.

## Result

```sql
-- After (clean)
SELECT * FROM "users" AS subquery_1
INNER JOIN "orders" AS subquery_2
ON (subquery_1."id" = subquery_2."id")
```

## Files Modified

| File | Changes |
|------|---------|
| `core/.../generator/SQLGenerator.java` | Added `getDirectlyAliasableSource()`, updated `visitJoin()` for regular and SEMI/ANTI joins |
| `tests/.../generator/SQLGeneratorDirectAliasTest.java` | New test file with 13 unit tests |

## Test Results

- 831 Java unit tests: All passing
- 13 new direct alias tests: All passing
- E2E tests: Working correctly

## Design Decisions

1. **Only optimize TableScan**: Other leaf nodes (RangeRelation, LocalDataRelation) produce SELECT statements that inherently need wrapping.

2. **Preserve plan_id qualification**: Column qualification in ON clauses continues to work unchanged.

3. **Targeted scope**: Only modified join handling, no changes to other visitor methods.

## Related Work

This optimization was suggested in PR #5 discussion, following the project principle to "keep the smarts out of Thunderduck and delegate to DuckDB" by generating simpler SQL.
