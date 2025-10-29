# TPC-DS Q36 & Q86 Root Cause Analysis

## Executive Summary

After exploring DuckDB's official TPC-DS implementation:
- **Q36**: DuckDB rewrote it to avoid `GROUPING()` in `PARTITION BY` (doesn't work in DuckDB)
- **Q86**: DuckDB uses the same ROLLUP/GROUPING approach (should work)

## Critical Discoveries

### 1. DuckDB Has Two Different Approaches

| Query | Original TPC-DS | DuckDB Version | Reason |
|-------|----------------|----------------|---------|
| Q36 | GROUP BY ROLLUP | Rewritten with UNION ALL | GROUPING() in PARTITION BY unsupported |
| Q86 | GROUP BY ROLLUP | Kept ROLLUP (with NULLS FIRST) | Basic ROLLUP works |

### 2. The GROUPING() in PARTITION BY Problem

DuckDB **cannot** handle:
```sql
RANK() OVER (PARTITION BY GROUPING(column) + ...)
```

This is why they rewrote Q36 but kept Q86 - both use the same pattern, but DuckDB found a workaround only for Q36.

### 3. Q86 Should Work But Doesn't

Q86 in DuckDB's version:
- Uses the same ROLLUP/GROUPING() as ours
- Adds explicit `NULLS FIRST` to all ORDER BY expressions
- Has minor syntax differences

Our Q86 still fails despite these being minor differences.

## Root Causes Identified

### For Q36:
- **Primary Issue**: GROUPING() in window function PARTITION BY is unsupported in DuckDB
- **Solution**: Must rewrite like DuckDB did (UNION ALL approach)

### For Q86:
- **Possible Issues**:
  1. Missing NULLS FIRST in some ORDER BY expressions
  2. CASE expression in ORDER BY might need adjustment
  3. Data mismatch or test comparison issue

## Why Other ROLLUP Queries Pass

Queries Q14a, Q18, Q22, Q67 pass because they:
- Don't use GROUPING() in PARTITION BY
- Use simpler ROLLUP patterns
- Our NULLS FIRST fix handles them correctly

## Recommended Actions

### 1. Immediate Fix for Q36
Rewrite Q36 to match DuckDB's UNION ALL approach (no other option).

### 2. Debug Q86
Since DuckDB's version should work:
- Check if our NULLS FIRST is being applied correctly
- Verify the CASE expression in ORDER BY
- Check actual vs expected data

### 3. Long-term Solution
Add detection for `PARTITION BY GROUPING(` pattern and either:
- Throw informative error
- Auto-rewrite to UNION ALL approach
- Document as known limitation

## Test to Confirm

```sql
-- This will FAIL in DuckDB (GROUPING in PARTITION BY):
SELECT RANK() OVER (PARTITION BY GROUPING(cat))
FROM (VALUES ('A')) t(cat)
GROUP BY ROLLUP(cat);

-- This will WORK in DuckDB (GROUPING not in PARTITION BY):
SELECT GROUPING(cat) as g, RANK() OVER (ORDER BY cat)
FROM (VALUES ('A')) t(cat)
GROUP BY ROLLUP(cat);
```

## Impact Summary

- **2 queries affected**: Q36 (needs rewrite), Q86 (needs investigation)
- **14 other failing queries**: Unrelated to ROLLUP/GROUPING
- **84% pass rate**: Could reach 86% with these fixes

## Conclusion

DuckDB's own TPC-DS implementation confirms that:
1. GROUPING() in PARTITION BY doesn't work (they rewrote Q36)
2. Basic ROLLUP/GROUPING() does work (they kept Q86)
3. We need query-specific handling for these edge cases