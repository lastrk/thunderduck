# GROUPING() Function: SQL Standard and Semantics

## SQL Standard Status

### ISO/IEC SQL Standard
The GROUPING() function **IS** part of the SQL standard, specifically introduced in:
- **SQL:1999** (SQL99) - First introduced with ROLLUP and CUBE operations
- **SQL:2003** - Further refined
- **SQL:2011** - Current specification

### Standard Definition (SQL:2011)

According to ISO/IEC 9075-2:2011, the GROUPING() function:

```sql
GROUPING ( <column reference> )
```

**Returns:**
- `1` (one) if the argument is a null value created by a ROLLUP or CUBE operation (i.e., represents a super-aggregate row)
- `0` (zero) if the argument represents a detail row or contains an actual NULL from the data

### Key Semantic Points from the Standard

1. **Return Type**: INTEGER (specifically 0 or 1)

2. **Context**: Can only be used in:
   - SELECT clause
   - HAVING clause
   - ORDER BY clause
   - When GROUP BY with ROLLUP, CUBE, or GROUPING SETS is present

3. **Purpose**: Distinguishes between:
   - NULL values in data (returns 0)
   - NULL values from aggregation (returns 1)

## Implementation Comparison

### Compliant with Standard (Return 1 for aggregated NULLs)

| Database | GROUPING() Returns | Standard Compliant |
|----------|-------------------|-------------------|
| **Oracle** | 1 for aggregated NULL, 0 for actual value | ✅ YES |
| **SQL Server** | 1 for aggregated NULL, 0 for actual value | ✅ YES |
| **PostgreSQL** | 1 for aggregated NULL, 0 for actual value | ✅ YES |
| **Spark SQL** | 1 for aggregated NULL, 0 for actual value | ✅ YES |
| **DB2** | 1 for aggregated NULL, 0 for actual value | ✅ YES |
| **Snowflake** | 1 for aggregated NULL, 0 for actual value | ✅ YES |

### DuckDB Implementation

Let's check DuckDB's documentation and behavior:

```sql
-- Standard test case
WITH data AS (
    SELECT 'A' as grp, 1 as val
    UNION ALL
    SELECT 'B', 2
)
SELECT
    grp,
    SUM(val),
    GROUPING(grp) as grouping_flag
FROM data
GROUP BY ROLLUP(grp)
ORDER BY grp NULLS FIRST;
```

**Expected Standard Result:**
```
grp  | SUM(val) | grouping_flag
-----|----------|---------------
NULL | 3        | 1  <- Aggregated NULL, GROUPING returns 1
A    | 1        | 0  <- Actual value, GROUPING returns 0
B    | 2        | 0  <- Actual value, GROUPING returns 0
```

## DuckDB Specifics (Research Findings)

### DuckDB Documentation Check

According to DuckDB v0.8+ documentation:
- DuckDB **does** support GROUPING() function
- DuckDB claims SQL standard compliance
- Should return 1 for aggregated NULLs, 0 for actual values

### Potential Issues

1. **Version Differences**:
   - Older DuckDB versions might have bugs
   - Implementation might have been fixed in newer versions

2. **Context Limitations**:
   - DuckDB might not support GROUPING() in all contexts (e.g., window functions)
   - PARTITION BY GROUPING() might not be supported

3. **Expression Evaluation**:
   - Complex expressions like `GROUPING(a) + GROUPING(b)` might evaluate differently
   - Order of operations could differ

## Testing for Compliance

### Minimal Compliance Test
```sql
-- This query MUST return 1 for the aggregated row per SQL standard
SELECT GROUPING(col)
FROM (VALUES (1)) t(col)
GROUP BY ROLLUP(col)
HAVING GROUPING(col) = 1;
```

### Complex Expression Test (Like Q36/Q86)
```sql
-- Test GROUPING in calculated expressions and window functions
WITH data AS (SELECT 'A' cat, 'X' cls, 1 val UNION ALL SELECT 'B', 'Y', 2)
SELECT
    cat, cls,
    GROUPING(cat) + GROUPING(cls) as hierarchy_level,
    RANK() OVER (
        PARTITION BY GROUPING(cat) + GROUPING(cls)
        ORDER BY SUM(val)
    ) as rnk
FROM data
GROUP BY ROLLUP(cat, cls);
```

## Implications for ThunderDuck

### If DuckDB is Standard-Compliant
The issue might be:
1. **Window Function Context**: GROUPING() in PARTITION BY might not work
2. **Expression Evaluation**: Complex expressions might need parentheses
3. **NULL Ordering**: Even with NULLS FIRST, the GROUPING() values might affect sort

### If DuckDB is Non-Compliant
We need to:
1. **Invert Values**: Transform `GROUPING(x)` to `(1 - GROUPING(x))`
2. **Use Alternative**: Find DuckDB-specific equivalent
3. **Rewrite Queries**: Avoid GROUPING() where possible

## Recommendation

1. **Verify DuckDB Version**: Check which version is being used
2. **Run Compliance Test**: Execute the minimal test above
3. **Check Error Logs**: GROUPING() might be throwing errors silently
4. **Test in Isolation**: Run GROUPING() queries directly in DuckDB CLI

## SQL Standard References

- ISO/IEC 9075-2:2011, Section 6.9 "Set function specification"
- ISO/IEC 9075-2:2011, Section 7.9 "GROUP BY clause"
- Feature T433 "Multiargument GROUPING function"

## Conclusion

The GROUPING() function **IS** standardized with precise semantics:
- **Standard behavior**: Returns 1 for aggregated NULLs, 0 for actual values
- **All major databases** follow this standard
- **DuckDB claims compliance** but might have implementation gaps

The issue is likely not about different interpretations of the standard, but rather:
1. Implementation bugs in specific contexts
2. Incomplete support for GROUPING() in complex expressions
3. Version-specific issues