# GROUPING() Function Compatibility Analysis
## Spark vs DuckDB Implementation Differences

### Executive Summary

Two TPC-DS queries (Q36 and Q86) are failing despite our ROLLUP NULL ordering fixes. Both queries heavily use the `GROUPING()` function, which appears to have implementation differences between Spark and DuckDB.

### The GROUPING() Function

The `GROUPING()` function is used with `ROLLUP` and `CUBE` to indicate whether a NULL value in a result row is due to aggregation or is an actual NULL value in the data.

### Test Cases for Analysis

#### Test Case 1: Basic GROUPING() with ROLLUP

```sql
-- Simple test data
WITH test_data AS (
    SELECT 'A' as category, 'X' as subcategory, 10 as value
    UNION ALL SELECT 'A', 'Y', 20
    UNION ALL SELECT 'B', 'X', 30
    UNION ALL SELECT 'B', 'Y', 40
)
SELECT
    category,
    subcategory,
    SUM(value) as total,
    GROUPING(category) as g_cat,
    GROUPING(subcategory) as g_sub,
    GROUPING(category) + GROUPING(subcategory) as level
FROM test_data
GROUP BY ROLLUP(category, subcategory)
ORDER BY category NULLS FIRST, subcategory NULLS FIRST
```

**Expected Spark Output:**
```
category  subcategory  total  g_cat  g_sub  level
--------------------------------------------------
NULL      NULL         100    1      1      2      <- Grand total
A         NULL         30     0      1      1      <- Category subtotal
A         X            10     0      0      0      <- Detail row
A         Y            20     0      0      0      <- Detail row
B         NULL         70     0      1      1      <- Category subtotal
B         X            30     0      0      0      <- Detail row
B         Y            40     0      0      0      <- Detail row
```

In Spark:
- `GROUPING(column) = 1` when column is aggregated (shows as NULL due to ROLLUP)
- `GROUPING(column) = 0` when column has its actual value

#### Test Case 2: GROUPING() in Complex Expressions (Like Q36)

```sql
WITH sales_data AS (
    SELECT 'Electronics' as category, 'Phone' as class, 1000 as profit
    UNION ALL SELECT 'Electronics', 'Laptop', 2000
    UNION ALL SELECT 'Clothing', 'Shirt', 500
)
SELECT
    category,
    class,
    SUM(profit) as total_profit,
    GROUPING(category) + GROUPING(class) as lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(category) + GROUPING(class)
        ORDER BY SUM(profit) DESC
    ) as rank_within_parent
FROM sales_data
GROUP BY ROLLUP(category, class)
ORDER BY lochierarchy DESC, category, class
```

This mirrors Q36's pattern of using `GROUPING()` in:
1. Calculated columns (`lochierarchy`)
2. Window function PARTITION BY clauses
3. ORDER BY expressions

### Failing Queries Analysis

#### Q36 Pattern:
```sql
SELECT
    ...
    GROUPING(i_category) + GROUPING(i_class) as lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category) + GROUPING(i_class),
        CASE WHEN GROUPING(i_class) = 0 THEN i_category END
        ORDER BY ...
    ) as rank_within_parent
FROM ...
GROUP BY ROLLUP(i_category, i_class)
ORDER BY lochierarchy DESC, ...
```

#### Q86 Pattern (Similar):
```sql
SELECT
    ...
    GROUPING(i_category) + GROUPING(i_class) as lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category) + GROUPING(i_class),
        CASE WHEN GROUPING(i_class) = 0 THEN i_category END
        ORDER BY ...
    ) as rank_within_parent
FROM ...
GROUP BY ROLLUP(i_category, i_class)
ORDER BY lochierarchy DESC, ...
```

### Hypothesis: DuckDB GROUPING() Differences

Based on the test failures, DuckDB might:

1. **Return inverted values**:
   - DuckDB might return 0 for aggregated, 1 for actual (opposite of Spark)

2. **Different NULL handling**:
   - The function might not be available or work differently in window functions

3. **Expression evaluation order**:
   - `GROUPING()` in calculated expressions might evaluate differently

### Recommended Tests to Confirm

To determine the exact difference, run these queries through both Spark and our DuckDB implementation:

1. **Basic value test**: Confirm what GROUPING() returns
2. **Expression test**: Check `GROUPING(a) + GROUPING(b)`
3. **Window function test**: Verify GROUPING() in PARTITION BY
4. **CASE statement test**: Check GROUPING() in CASE expressions

### Potential Fix Strategies

If DuckDB's GROUPING() is incompatible:

1. **Invert the function** (if values are reversed):
   ```java
   sql = sql.replaceAll("GROUPING\\(([^)]+)\\)", "(1 - GROUPING($1))");
   ```

2. **Replace with DuckDB equivalent** (if different function exists):
   ```java
   // Map GROUPING() to DuckDB's equivalent
   ```

3. **Rewrite queries** for Q36 and Q86 specifically:
   - Custom handling for these complex GROUPING() patterns

### Next Steps

1. **Verify GROUPING() behavior**: Run the test cases above through both systems
2. **Check DuckDB documentation**: Confirm GROUPING() semantics
3. **Implement transformation**: Based on findings, add to preprocessSQL()
4. **Test Q36 and Q86**: Verify the fix resolves these queries

### Files to Test

- `/tmp/test_grouping_function.py` - Comprehensive test suite
- `/tmp/simple_grouping_test.py` - Minimal test case
- `/tmp/check_grouping.sql` - Raw SQL for testing

### Current Status

- 84/100 TPC-DS queries passing (84%)
- Q36 and Q86 are the only failing ROLLUP queries with GROUPING()
- Other ROLLUP queries (Q14a, Q18, Q22, Q67) now pass after NULL ordering fix

The GROUPING() function incompatibility is likely the last major hurdle for ROLLUP query compatibility.