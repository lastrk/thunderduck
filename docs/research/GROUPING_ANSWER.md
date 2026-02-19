# Is GROUPING() a Standard SQL Function?

## Yes, GROUPING() is Part of the SQL Standard

### Standard Definition
- **Introduced**: SQL:1999 (SQL99)
- **Current**: ISO/IEC 9075-2:2011
- **Feature ID**: T433 (for multi-argument version)

### Precise Semantics (SQL Standard)

The GROUPING() function has **precisely defined semantics** in the SQL standard:

```sql
GROUPING(column_reference)
```

**Returns:**
- `1` - When the column is NULL because it's part of a super-aggregate row (created by ROLLUP/CUBE)
- `0` - When the column contains its actual value (including actual NULL values from data)

**Return type:** INTEGER (always 0 or 1)

### Example Demonstrating Standard Semantics

```sql
-- Data: Two groups (A, B) with values
WITH data AS (
    SELECT 'A' as category, 10 as amount
    UNION ALL SELECT 'A', 20
    UNION ALL SELECT 'B', 30
    UNION ALL SELECT NULL, 40  -- Actual NULL in data
)
SELECT
    category,
    SUM(amount) as total,
    GROUPING(category) as g
FROM data
GROUP BY ROLLUP(category)
ORDER BY category NULLS FIRST;
```

**Standard-Compliant Result:**
```
category | total | g
---------|-------|---
NULL     | 100   | 1  <- NULL from ROLLUP: GROUPING = 1
NULL     | 40    | 0  <- NULL from data: GROUPING = 0
A        | 30    | 0  <- Actual value: GROUPING = 0
B        | 30    | 0  <- Actual value: GROUPING = 0
```

### Universal Implementation

All major databases implement GROUPING() with identical semantics:

| Database | GROUPING(aggregated_null) | GROUPING(actual_value) | Standard? |
|----------|---------------------------|------------------------|-----------|
| Oracle | 1 | 0 | ✅ |
| SQL Server | 1 | 0 | ✅ |
| PostgreSQL | 1 | 0 | ✅ |
| Spark SQL | 1 | 0 | ✅ |
| DB2 | 1 | 0 | ✅ |
| Snowflake | 1 | 0 | ✅ |
| MySQL 8.0+ | 1 | 0 | ✅ |
| **DuckDB** | **1** | **0** | **✅** |

### DuckDB Specific Notes

DuckDB (v1.4.4.0 used in this project) **does implement standard GROUPING() semantics**:
- Returns 1 for aggregated NULLs
- Returns 0 for actual values
- Supports GROUPING() with ROLLUP and CUBE

### Why Are Q36 and Q86 Failing?

Given that DuckDB implements standard semantics, the issue is likely:

1. **Context limitations**: GROUPING() might not work in all contexts:
   ```sql
   -- This might not be supported:
   RANK() OVER (PARTITION BY GROUPING(col) + GROUPING(col2))
   ```

2. **Expression evaluation timing**: GROUPING() in complex expressions might be evaluated at the wrong phase:
   ```sql
   -- Complex expression that might fail:
   GROUPING(cat) + GROUPING(subcat) as hierarchy_level
   ```

3. **Window function incompatibility**: Using GROUPING() inside window function clauses might not be supported in DuckDB

### The Real Issue

The problem is **not** that GROUPING() has different semantics, but rather:
- **Limited context support**: DuckDB might not support GROUPING() in PARTITION BY clauses
- **Expression restrictions**: Complex expressions with GROUPING() might not evaluate correctly
- **Implementation gaps**: While the basic function works, advanced usage patterns might be unsupported

### Verification Test

To confirm DuckDB follows the standard:
```sql
-- This MUST return a row with grouping_flag = 1 per standard
SELECT GROUPING(x) as grouping_flag
FROM (VALUES (1)) t(x)
GROUP BY ROLLUP(x)
HAVING GROUPING(x) = 1;
```

If this returns 1, DuckDB is standard-compliant for basic GROUPING() usage.

### Conclusion

- **GROUPING() is standardized** with precise, unambiguous semantics
- **All databases agree**: Return 1 for aggregated NULLs, 0 for actual values
- **DuckDB follows the standard** for basic usage
- **The issue is likely** context/expression support, not semantic differences

The fix needed is probably not to change GROUPING() values, but to handle unsupported contexts where GROUPING() is used in complex expressions or window functions.