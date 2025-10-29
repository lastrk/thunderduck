# Critical Discovery: DuckDB's TPC-DS Implementation

## Key Finding
DuckDB's official TPC-DS implementation reveals **they rewrote Q36 to avoid ROLLUP/GROUPING()** while keeping Q86 with ROLLUP.

## Evidence from DuckDB Repository

### Q36: DuckDB AVOIDS ROLLUP
- **Our version**: Uses `GROUP BY ROLLUP(i_category, i_class)` with `GROUPING()` functions
- **DuckDB's version**: Completely rewritten using UNION ALL to avoid ROLLUP
- **URL**: https://github.com/duckdb/duckdb/blob/main/extension/tpcds/dsdgen/queries/36.sql

### Q86: DuckDB KEEPS ROLLUP
- **Our version**: Uses `GROUP BY ROLLUP(i_category, i_class)` with `GROUPING()` functions
- **DuckDB's version**: Also uses ROLLUP and GROUPING() - identical approach
- **URL**: https://github.com/duckdb/duckdb/blob/main/extension/tpcds/dsdgen/queries/86.sql

## Why This Matters

### Q36 Rewrite Pattern (DuckDB's Solution)
Instead of:
```sql
GROUP BY ROLLUP(i_category, i_class)
```

DuckDB uses:
```sql
-- Three separate aggregations combined with UNION ALL
SELECT ... GROUP BY i_category, i_class  -- Detail level
UNION ALL
SELECT ... GROUP BY i_category          -- Category level
UNION ALL
SELECT ... GROUP BY ()                  -- Grand total
```

### Q86 Still Uses ROLLUP
DuckDB kept the original ROLLUP syntax for Q86, suggesting:
1. Basic ROLLUP works in DuckDB
2. Q36 has something specific that breaks

## The Real Problem

### What's Different About Q36?
Q36 uses GROUPING() in more complex contexts than Q86:
```sql
-- Q36: GROUPING in window function PARTITION BY
rank() over (
    partition by grouping(i_category)+grouping(i_class),
    case when grouping(i_class) = 0 then i_category end
    order by ...
)
```

### DuckDB's Limitation
The evidence suggests DuckDB **cannot handle GROUPING() in PARTITION BY clauses** of window functions, which is why they rewrote Q36 but not Q86.

## Our Options

### Option 1: Follow DuckDB's Lead (Rewrite Q36)
Transform Q36 to use UNION ALL instead of ROLLUP, matching DuckDB's approach.

### Option 2: Fix GROUPING() in Window Functions
Try to precompute GROUPING() values before the window function.

### Option 3: Special Case Q36
Detect Q36 pattern and apply specific transformation.

## Recommended Solution

**Rewrite Q36 queries that use GROUPING() in PARTITION BY** to match DuckDB's approach:

1. Detect pattern: `PARTITION BY GROUPING(`
2. If found, rewrite the entire query using UNION ALL
3. Leave other ROLLUP queries unchanged (like Q86)

## Test Verification

To confirm this is the issue:
```sql
-- This should FAIL in DuckDB:
SELECT *, RANK() OVER (PARTITION BY GROUPING(cat))
FROM (VALUES ('A'), ('B')) t(cat)
GROUP BY ROLLUP(cat);

-- This should WORK in DuckDB:
SELECT *, GROUPING(cat) as g
FROM (VALUES ('A'), ('B')) t(cat)
GROUP BY ROLLUP(cat);
```

## Impact on ThunderDuck

- **Q36**: Needs rewriting (GROUPING in PARTITION BY doesn't work)
- **Q86**: Should work as-is (but currently failing - investigate why)
- **Other ROLLUP queries**: Already working (Q14a, Q18, Q22, Q67)

## Next Steps

1. Implement Q36 rewrite to use UNION ALL
2. Debug why Q86 still fails despite DuckDB supporting it
3. Document this limitation for future reference