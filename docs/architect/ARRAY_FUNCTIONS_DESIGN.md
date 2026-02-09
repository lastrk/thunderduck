# Array Functions Design: Gap Analysis and Implementation Plan

## 1. Test Results Summary

### test_array_functions_differential.py

| # | Test | Status | Root Cause |
|---|------|--------|------------|
| 1 | test_split_function | PASS | |
| 2 | test_startswith_function | PASS | |
| 3 | test_collect_list_function | **FAIL** | `FILTER` keyword rewritten to `list_filter` by `rewriteSQL()` |
| 4 | test_collect_set_function | **FAIL** | Same as above: `FILTER` -> `list_filter` |
| 5 | test_size_function | **FAIL** | `TRUNC` function collision: `trunc` direct mapping applies to integer literals, produces `TRUNC(1 AS BIGINT)` |
| 6 | test_explode_function | PASS | |
| 7 | test_array_contains_function | PASS | |
| 8 | test_size_in_select_expr | PASS | |
| 9 | test_collect_list_with_groupby | PASS | |
| 10 | test_size_in_sql_with_filter | PASS | |
| 11 | test_case_insensitive_functions | **FAIL** | Same `TRUNC`/`size` issue |

**Result: 7 passed, 4 failed, 0 skipped**

### test_complex_types_differential.py

| # | Test | Status | Root Cause |
|---|------|--------|------------|
| 1 | test_struct_field_dot_notation | PASS | |
| 2 | test_struct_field_bracket_notation | PASS | |
| 3 | test_nested_struct_access | **FAIL** | Nested `NAMED_STRUCT` inside `struct_pack()` not rewritten |
| 4 | test_array_first_element | PASS | |
| 5 | test_array_middle_element | PASS | |
| 6 | test_array_negative_index_last | **SKIP** | Spark throws OOB; DuckDB supports negative indexing |
| 7 | test_array_negative_index_second_last | **SKIP** | Same |
| 8 | test_map_string_key | PASS | |
| 9 | test_map_missing_key | PASS | |
| 10 | test_with_field_add_new | PASS | |
| 11 | test_with_field_add_multiple | PASS | |
| 12 | test_array_of_structs | PASS | |
| 13 | test_struct_with_array | PASS | |
| 14 | test_struct_access_multiple_rows | PASS | |
| 15 | test_array_index_multiple_rows | PASS | |

**Result: 12 passed, 1 failed, 2 skipped**

### test_lambda_differential.py

| # | Test | Status | Root Cause |
|---|------|--------|------------|
| 1 | test_transform_add_one | PASS | |
| 2 | test_transform_multiply | PASS | |
| 3 | test_transform_from_subquery | PASS | |
| 4 | test_filter_greater_than | PASS | |
| 5 | test_filter_even_numbers | PASS | |
| 6 | test_filter_all_pass | PASS | |
| 7 | test_filter_none_pass | PASS | |
| 8 | test_exists_true | PASS | |
| 9 | test_exists_false | PASS | |
| 10 | test_forall_true | PASS | |
| 11 | test_forall_false | PASS | |
| 12 | test_aggregate_sum | PASS | |
| 13 | test_aggregate_product | PASS | |
| 14 | test_aggregate_with_init | PASS | |
| 15 | test_nested_transform | PASS | |
| 16 | test_transform_then_filter | PASS | |
| 17 | test_transform_multiple_rows | PASS | |
| 18 | test_filter_in_where | PASS | |

**Result: 18 passed, 0 failed, 0 skipped**

### test_dataframe_functions.py (Array Functions subset)

| # | Test | Status | Root Cause |
|---|------|--------|------------|
| 1 | test_array_contains | PASS | |
| 2 | test_array_size | PASS | |
| 3 | test_array_sort | PASS | |
| 4 | test_array_sort_desc | PASS | |
| 5 | test_array_distinct | **FAIL** | Element order mismatch: DuckDB reverses order, Spark preserves insertion order |
| 6 | test_array_union | **FAIL** | `list_union` does not exist in DuckDB |
| 7 | test_array_intersect | **FAIL** | Element order mismatch (DuckDB vs Spark) |
| 8 | test_array_except | **FAIL** | `list_except` does not exist in DuckDB |
| 9 | test_arrays_overlap | PASS | |
| 10 | test_array_position | **FAIL** | Spark returns 0 for not-found; DuckDB `list_position` returns 0 for not-found but NULL for NULL array |
| 11 | test_element_at | PASS | |
| 12 | test_explode | PASS | |
| 13 | test_explode_outer | **FAIL** | `explode_outer` not mapped; DuckDB has no direct equivalent |
| 14 | test_flatten | PASS | |
| 15 | test_reverse_array | **FAIL** | DuckDB `reverse()` only works on strings; arrays need `list_reverse()` |
| 16 | test_slice | PASS | |
| 17 | test_array_join | **FAIL** | `array_join` not mapped in FunctionRegistry (for DataFrame API path) |

**Result: 9 passed, 8 failed, 0 skipped**

### Aggregate Totals (Array/Complex Type Tests Only)

| Suite | Passed | Failed | Skipped | Total |
|-------|--------|--------|---------|-------|
| test_array_functions_differential | 7 | 4 | 0 | 11 |
| test_complex_types_differential | 12 | 1 | 2 | 15 |
| test_lambda_differential | 18 | 0 | 0 | 18 |
| test_dataframe_functions (Array) | 9 | 8 | 0 | 17 |
| **TOTAL** | **46** | **13** | **2** | **61** |

---

## 2. Complete Function Inventory

### 2.1 Array Functions

| Spark Function | DuckDB Equivalent | FunctionRegistry Status | Category | Notes |
|---|---|---|---|---|
| `array()` | `list_value()` | Mapped | Works | Direct mapping |
| `array_contains()` | `list_contains()` | Mapped | Works | Direct mapping |
| `array_distinct()` | `list_distinct()` | Mapped | **Semantic Mismatch** | DuckDB reverses element order; Spark preserves insertion order |
| `array_except()` | *(none)* | Mapped to `list_except` | **Missing in DuckDB** | `list_except` does not exist; needs SQL rewrite or extension |
| `array_intersect()` | `list_intersect()` | Mapped | **Semantic Mismatch** | Element order differs between Spark and DuckDB |
| `array_join()` | `array_to_string()` or `list_aggr()` | **Not Mapped** | **Name Mismatch** | DuckDB uses `array_to_string(arr, sep)` |
| `array_max()` | `list_max()` | Mapped | Works | Direct mapping |
| `array_min()` | `list_min()` | Mapped | Works | Direct mapping |
| `array_position()` | `list_position()` | Mapped | **Semantic Mismatch** | Spark returns 0 for not-found; DuckDB `list_position` returns NULL for not-found. Both return NULL for NULL array. |
| `array_remove()` | `list_filter(arr, x -> x != val)` | **Not Mapped** | **Complex Translation** | Needs lambda-based SQL rewrite |
| `array_repeat()` | `[val] * n` or custom | **Not Mapped** | **Missing** | No direct DuckDB equivalent |
| `array_reverse()` | `list_reverse()` | Mapped | Works | Direct mapping (but `reverse()` on arrays needs remap) |
| `array_sort()` / `sort_array()` | `list_sort()` | Mapped (custom) | Works | Boolean-to-string direction conversion |
| `array_union()` | *(none)* | Mapped to `list_union` | **Missing in DuckDB** | `list_union` does not exist; needs SQL rewrite or extension |
| `array_zip()` / `arrays_zip()` | *(none)* | **Not Mapped** | **Missing in DuckDB** | No direct equivalent |
| `arrays_overlap()` | `list_has_any()` | Mapped | Works | Direct mapping |
| `collect_list()` | `list() FILTER (WHERE ...)` | Mapped (custom) | **Bug** | `rewriteSQL()` corrupts `FILTER` keyword to `list_filter` |
| `collect_set()` | `list_distinct(list() FILTER ...)` | Mapped (custom) | **Bug** | Same `FILTER` -> `list_filter` corruption |
| `element_at()` | `list_extract()` | Mapped | **Semantic Mismatch** | Spark: 1-based, negative counts from end, throws on OOB. DuckDB: 1-based, negative counts from end, returns NULL on OOB. Close but not identical on error behavior. |
| `explode()` | `unnest()` | Mapped | Works | Direct mapping |
| `explode_outer()` | *(none)* | **Not Mapped** | **Missing in DuckDB** | Spark preserves NULL rows with NULL exploded value; DuckDB `unnest` drops them |
| `flatten()` | `flatten()` | Mapped | Works | Direct mapping |
| `posexplode()` | *(none)* | **Not Mapped** | **Complex Translation** | Returns (position, element) tuples |
| `posexplode_outer()` | *(none)* | **Not Mapped** | **Complex Translation** | Same but preserves NULLs |
| `reverse()` (on arrays) | `list_reverse()` | **Collision** | **Name Mismatch** | `reverse` is mapped to string `reverse()`; array variant needs `list_reverse()` |
| `sequence()` | `generate_series()` or `range()` | **Not Mapped** | **Name Mismatch** | DuckDB `generate_series(start, stop, step)` |
| `shuffle()` | `list_sort(arr, 'random')` | **Not Mapped** | **Complex Translation** | No direct equivalent |
| `size()` | `CAST(len(arr) AS INTEGER)` | Mapped (custom) | **Bug** | Works via custom translator, but `rewriteSQL()` has collision with `trunc` mapping |
| `slice()` | `list_slice()` | Mapped | Works | Direct mapping |
| `sort_array()` | `list_sort()` | Mapped (custom) | Works | Boolean-to-string conversion |
| `transform()` | `list_transform()` | Mapped | Works | Direct mapping; lambda handling in ExpressionConverter |
| `filter()` | `list_filter()` | Mapped | Works | Direct mapping; lambda handling in ExpressionConverter |
| `exists()` | `list_any()` via wrapper | Mapped | Works | Handled in ExpressionConverter |
| `forall()` | `list_all()` via wrapper | Mapped | Works | Handled in ExpressionConverter |
| `aggregate()` | `list_reduce()` | Mapped | Works | Direct mapping |
| `zip_with()` | *(none)* | **Not Mapped** | **Missing in DuckDB** | No direct equivalent |

### 2.2 Map Functions (for context)

| Spark Function | DuckDB Equivalent | Status | Notes |
|---|---|---|---|
| `map()` | `MAP([keys], [values])` | Mapped (custom) | Works |
| `map_from_arrays()` | `MAP(keys, values)` | Mapped (custom) | Works |
| `map_keys()` | `map_keys()` | **Not Mapped** | Missing from FunctionRegistry |
| `map_values()` | `map_values()` | **Not Mapped** | Missing from FunctionRegistry |
| `map_entries()` | `map_entries()` | **Not Mapped** | Missing from FunctionRegistry |
| `element_at()` on maps | `element_at()` | Mapped to `list_extract` | Wrong mapping for maps |

---

## 3. Root Cause Analysis

### 3.1 BUG: `rewriteSQL()` FILTER Keyword Corruption

**Impact: collect_list, collect_set broken in raw SQL path**

The `rewriteSQL()` method applies `DIRECT_MAPPINGS` replacements using regex. The mapping `filter -> list_filter` causes the SQL keyword `FILTER` (in `FILTER (WHERE ...)` aggregate filter clauses) to be incorrectly rewritten to `list_filter`.

**Example:**
```sql
-- Generated by custom translator:
list(name) FILTER (WHERE name IS NOT NULL)
-- After rewriteSQL applies DIRECT_MAPPINGS:
list(name) list_filter (WHERE name IS NOT NULL)   -- BROKEN!
```

**Fix:** The `rewriteSQL()` regex pattern `\bfilter(?=\s*\()` matches `FILTER (WHERE ...)` because `FILTER` is followed by `(`. The fix must either:
- Skip the `filter -> list_filter` mapping in `rewriteSQL()` (it only applies to DataFrame API lambda expressions, not SQL), or
- Make the regex smarter to exclude `FILTER (WHERE` patterns, or
- Process custom translators BEFORE direct mappings so their output is not re-processed.

### 3.2 BUG: `trunc` Direct Mapping Collision

**Impact: createDataFrame with certain numeric types fails**

The direct mapping `trunc -> trunc` is benign on its own, but the `date_trunc` custom translator also registers `trunc`. The real issue is that `CAST(TRUNC(1.5 AS DOUBLE))` appears in generated SQL because `trunc` is being applied to numeric literals during DataFrame creation. This is a separate issue from array functions but surfaces in array tests because `createDataFrame` with array data triggers it.

### 3.3 Missing DuckDB Functions

Several functions mapped in FunctionRegistry do not actually exist in DuckDB:

| Mapped Name | Actual DuckDB Function | Status |
|---|---|---|
| `list_union` | Does not exist | Need `array_union` SQL macro or extension function |
| `list_except` | Does not exist | Need `array_except` SQL macro or extension function |
| `list_intersect` | `list_intersect` | Exists but has ordering differences |
| `list_distinct` | `list_distinct` | Exists but has ordering differences |

### 3.4 Semantic Mismatches: Element Ordering

DuckDB's `list_distinct()` and `list_intersect()` do not guarantee the same element ordering as Spark:
- **Spark**: Preserves insertion order (first occurrence)
- **DuckDB**: May reverse or use hash-based ordering

This is a fundamental semantic difference that cannot be fixed with a simple name mapping. Options:
1. Post-process with `list_sort()` (changes semantics for unsorted inputs)
2. Implement custom extension functions that preserve insertion order
3. Accept the difference (some applications don't depend on element order within arrays)

### 3.5 `reverse()` Dispatch Issue

`reverse` is mapped directly to DuckDB's `reverse()`, which only works on strings. For arrays, DuckDB requires `list_reverse()`. The FunctionRegistry maps `array_reverse -> list_reverse` but does not handle the case where `F.reverse()` is called on an array column via the DataFrame API (which generates a `reverse()` function call, not `array_reverse()`).

**Fix approach:** Type-aware dispatch in ExpressionConverter or SQLGenerator -- if the argument is an array type, emit `list_reverse()` instead of `reverse()`.

---

## 4. DuckDB Extension Function Designs

### 4.1 Existing Extension Architecture

The current extension (`thdck_spark_funcs`) implements:
- `spark_decimal_div` -- Spark-compatible DECIMAL division with ROUND_HALF_UP
- `/` operator overload for DECIMAL types
- `spark_sum` -- Spark-compatible SUM with correct return types
- `spark_avg` -- Spark-compatible AVG with correct return types

The extension uses DuckDB's `ScalarFunction` and `AggregateFunction` APIs, with bind functions for type resolution.

### 4.2 Proposed Extension Functions

#### 4.2.1 `spark_array_position(array, element)` -> BIGINT

**Spark semantics:**
- Returns 1-based position of first occurrence of `element` in `array`
- Returns 0 (not NULL) if element is not found
- Returns NULL if array is NULL

**DuckDB behavior:**
- `list_position(arr, elem)` returns 1-based position, 0 if not found, NULL if arr is NULL
- Actually matches Spark semantics. The test failure was for NULL array input where both return NULL.

**Confirmed semantic difference:** DuckDB `list_position` returns NULL when element is not found. Spark `array_position` returns 0.

Test data confirms: `arr1 = [4, 5, 6]` searching for value `2` (not in array). Spark returns `0`, DuckDB returns `NULL`. Same for `arr1 = [7, 8, 9]` searching for `2`.

**Fix (SQL macro or custom translator):**
```sql
-- SQL macro:
CREATE MACRO spark_array_position(arr, elem) AS
  COALESCE(list_position(arr, elem), 0);
```

**FunctionRegistry fix:**
```java
CUSTOM_TRANSLATORS.put("array_position", args ->
    "COALESCE(list_position(" + args[0] + ", " + args[1] + "), 0)");
```

This wraps `list_position` with `COALESCE(..., 0)` to convert NULL (not-found) to 0, matching Spark semantics. When the array itself is NULL, `list_position` returns NULL and `COALESCE` still returns 0, but Spark also returns NULL for NULL arrays. So the correct fix is:
```java
CUSTOM_TRANSLATORS.put("array_position", args ->
    "CASE WHEN " + args[0] + " IS NULL THEN NULL " +
    "ELSE COALESCE(list_position(" + args[0] + ", " + args[1] + "), 0) END");
```

#### 4.2.2 `spark_element_at(array, index)` -> element_type

**Spark semantics:**
- 1-based indexing
- Negative indices count from end (-1 = last element)
- Throws `SparkArrayIndexOutOfBoundsException` for out-of-bounds (in ANSI mode, default in Spark 4.x)
- Returns NULL for NULL array

**DuckDB `list_extract` behavior:**
- 1-based indexing
- Negative indices count from end
- Returns NULL for out-of-bounds (does not throw)

**Semantic difference:** DuckDB returns NULL on OOB; Spark throws. For a drop-in replacement, we should match Spark's error behavior in strict mode.

**Implementation approach (SQL macro for relaxed mode):**
```sql
-- Relaxed mode: just use list_extract (close enough)
CREATE MACRO spark_element_at(arr, idx) AS list_extract(arr, idx);
```

**Implementation approach (C++ extension for strict mode):**
```cpp
// In strict mode, validate bounds and throw
static void SparkElementAtExec(DataChunk &args, ExpressionState &state, Vector &result) {
    // For each row: check if index is in bounds
    // If not: throw InvalidInputException matching Spark's error message
    // If yes: extract element
}
```

**Recommendation:** Use `list_extract` for now (already mapped). Add strict-mode extension function later if needed.

#### 4.2.3 `spark_size(array_or_map)` -> INTEGER

**Spark semantics:**
- Returns number of elements in array or number of entries in map
- Returns -1 for NULL input (legacy behavior, default in Spark 3.x)
- Returns NULL for NULL input in Spark 4.x with ANSI mode

**DuckDB behavior:**
- `len(arr)` returns number of elements, NULL for NULL input

**Current mapping:** `size -> CAST(len(arr) AS INTEGER)` -- works for non-null arrays but returns NULL (not -1) for NULL arrays.

**Recommendation:** For Spark 4.x compatibility (ANSI mode), the current mapping is correct. For Spark 3.x compatibility, implement as:
```sql
CREATE MACRO spark_size(arr) AS
  CASE WHEN arr IS NULL THEN -1 ELSE CAST(len(arr) AS INTEGER) END;
```

**Note:** The current test failures for `size()` are NOT due to NULL handling. They are caused by the `trunc` mapping collision in `rewriteSQL()`.

#### 4.2.4 `spark_array_union(array1, array2)` -> array

**Spark semantics:**
- Returns union of two arrays with duplicates removed
- Preserves order: elements from array1 first, then unique elements from array2
- NULL elements are treated as a value (one NULL in result if either array has NULL)

**DuckDB:** No `list_union` function exists.

**Implementation approach (SQL macro):**
```sql
CREATE MACRO spark_array_union(arr1, arr2) AS (
  SELECT list(DISTINCT elem)
  FROM (
    SELECT unnest(arr1) AS elem
    UNION ALL
    SELECT unnest(arr2) AS elem
  )
);
```

**Better approach -- implement in C++ extension:**
```cpp
// Iterate arr1, add all unique elements (preserving order)
// Iterate arr2, add elements not already seen
// Return combined list
```

**Recommendation:** Implement as SQL rewrite first:
```sql
-- In FunctionRegistry custom translator:
array_union(arr1, arr2) ->
  list_distinct(list_concat(arr1, arr2))
```

Note: `list_concat` exists in DuckDB and concatenates two lists. `list_distinct` removes duplicates. However, ordering will differ from Spark.

#### 4.2.5 `spark_array_except(array1, array2)` -> array

**Spark semantics:**
- Returns elements in array1 but not in array2
- Removes duplicates from result
- Preserves order from array1

**DuckDB:** No `list_except` function exists.

**Implementation approach (SQL rewrite):**
```sql
-- FunctionRegistry custom translator:
array_except(arr1, arr2) ->
  list_filter(list_distinct(arr1), x -> NOT list_contains(arr2, x))
```

This is expressible as a SQL rewrite using DuckDB's lambda support.

#### 4.2.6 `spark_array_join(array, delimiter, [null_replacement])` -> VARCHAR

**Spark semantics:**
- Concatenates array elements with delimiter
- NULL elements are omitted (or replaced with null_replacement if provided)

**DuckDB equivalent:** `array_to_string(arr, delimiter)` -- concatenates array elements with delimiter, skipping NULLs.

**Fix:** Add mapping to FunctionRegistry:
```java
DIRECT_MAPPINGS.put("array_join", "array_to_string");
```

For the 3-argument form with null replacement:
```java
CUSTOM_TRANSLATORS.put("array_join", args -> {
    if (args.length == 2) {
        return "array_to_string(" + args[0] + ", " + args[1] + ")";
    } else if (args.length == 3) {
        return "array_to_string(" + args[0] + ", " + args[1] + ", " + args[2] + ")";
    }
    throw new IllegalArgumentException("array_join requires 2 or 3 arguments");
});
```

#### 4.2.7 `explode_outer()` -> table function

**Spark semantics:**
- Like `explode()` but preserves rows where the array/map is NULL
- For NULL arrays: produces one row with NULL exploded value

**DuckDB:** `unnest()` drops NULL arrays entirely.

**Implementation approach:** This requires SQL rewrite at the logical plan level, not just a function mapping. The SQLGenerator must detect `explode_outer` and generate:
```sql
-- Instead of: SELECT id, unnest(arr) FROM t
-- Generate:
SELECT id, CASE WHEN arr IS NULL THEN NULL ELSE u.elem END AS elem
FROM t
LEFT JOIN LATERAL (SELECT unnest(arr) AS elem) u ON true
```

Or use DuckDB's `UNNEST` with `recursive := true` option.

**Recommendation:** Handle in SQLGenerator/ExpressionConverter as a plan-level transformation.

---

## 5. FunctionRegistry Additions Needed

### 5.1 Missing Direct Mappings

| Spark Function | DuckDB Function | Priority |
|---|---|---|
| `array_join` | `array_to_string` | P2 |
| `map_keys` | `map_keys` | P2 |
| `map_values` | `map_values` | P2 |
| `map_entries` | `map_entries` | P2 |
| `sequence` | `generate_series` | P3 |
| `array_repeat` | *(custom)* | P3 |
| `array_remove` | *(custom with lambda)* | P3 |
| `array_compact` | `list_filter(arr, x -> x IS NOT NULL)` | P3 |

### 5.2 Existing Mappings Needing Fixes

| Spark Function | Current Mapping | Issue | Fix |
|---|---|---|---|
| `array_union` | `list_union` | Does not exist in DuckDB | Rewrite to `list_distinct(list_concat(a, b))` |
| `array_except` | `list_except` | Does not exist in DuckDB | Rewrite to `list_filter(list_distinct(a), x -> NOT list_contains(b, x))` |
| `reverse` (arrays) | `reverse` | Only works for strings | Type-aware dispatch: `list_reverse` for arrays |
| `collect_list` / `collect_set` | Custom translator | `FILTER` keyword corrupted by `rewriteSQL()` | Fix `rewriteSQL()` to skip `filter` mapping |

### 5.3 Functions Needing New Custom Translators

| Spark Function | DuckDB Translation | Priority |
|---|---|---|
| `explode_outer` | Plan-level LEFT JOIN LATERAL + UNNEST | P2 |
| `posexplode` | `unnest(arr) WITH ORDINALITY` or `UNNEST` + `generate_subscripts` | P3 |
| `posexplode_outer` | Combination of above two approaches | P3 |
| `array_remove` | `list_filter(arr, x -> x != val)` | P3 |
| `array_repeat` | `[val] || ... ` or custom | P3 |
| `shuffle` | `list_sort(arr, 'random')` | P3 |
| `zip_with` | No direct equivalent; needs extension | P3 |
| `nvl2` | `CASE WHEN arg1 IS NOT NULL THEN arg2 ELSE arg3 END` | P2 |
| `nanvl` | `CASE WHEN isnan(arg1) THEN arg2 ELSE arg1 END` | P2 |

---

## 6. Implementation Priority

### P0: Critical Bugs (Block existing tests)

1. **Fix `rewriteSQL()` FILTER keyword corruption** -- Causes `collect_list` and `collect_set` to fail in raw SQL path.
2. **Fix `trunc` mapping collision** -- Causes `createDataFrame` failures when data includes certain numeric types (affects `size()` and `case_insensitive` tests).

### P1: Missing Functions Used by Skipped/Failing Tests

3. **Fix `array_union` mapping** -- `list_union` does not exist. Replace with `list_distinct(list_concat(a, b))`.
4. **Fix `array_except` mapping** -- `list_except` does not exist. Replace with lambda-based `list_filter`.
5. **Fix `reverse` array dispatch** -- Map to `list_reverse` when argument is an array.
6. **Add `array_join` mapping** -- Map to `array_to_string`.
7. **Add `explode_outer` support** -- Plan-level transformation.
8. **Fix `array_position` NULL handling** -- Verify and fix if DuckDB returns NULL instead of 0 for not-found.

### P2: Functions Needed by test_dataframe_functions.py

9. **Add `map_keys`, `map_values`, `map_entries` mappings**
10. **Add `nvl2` custom translator** -- `CASE WHEN ... IS NOT NULL THEN ... ELSE ... END`
11. **Add `nanvl` custom translator** -- `CASE WHEN isnan(...) THEN ... ELSE ... END`
12. **Fix `array_distinct` ordering** -- Post-process with `list_sort` or accept ordering difference
13. **Fix `array_intersect` ordering** -- Same approach as `array_distinct`
14. **Fix nested `NAMED_STRUCT` rewriting** -- Recursive SQL rewriting for nested struct constructors

### P3: Remaining Spark Array Functions

15. **Add `sequence` mapping** -- `generate_series`
16. **Add `array_repeat` translator**
17. **Add `array_remove` translator**
18. **Add `posexplode` support**
19. **Add `posexplode_outer` support**
20. **Add `arrays_zip` / `array_zip` support**
21. **Add `zip_with` support**
22. **Add `shuffle` support**
23. **Add `array_compact` mapping**

---

## 7. Implementation Roadmap

### Phase 1: Fix Critical Bugs (P0)

**Goal:** Make existing passing tests stop regressing and fix the 4 test failures in test_array_functions_differential.py that are caused by infrastructure bugs.

1. **Fix `rewriteSQL()` FILTER corruption:**
   - In `initializeArrayFunctions()`, the mapping `"filter" -> "list_filter"` causes `FILTER (WHERE ...)` aggregate clauses to be corrupted.
   - **Solution A** (preferred): Remove `filter -> list_filter` from `DIRECT_MAPPINGS`. The lambda `filter()` function is handled by ExpressionConverter which uses the `translate()` method (not `rewriteSQL()`), so the direct mapping is only needed for raw SQL. For raw SQL, Spark's `filter()` HOF uses different syntax anyway.
   - **Solution B**: Make `rewriteSQL()` regex exclude `FILTER (WHERE` patterns by using negative lookahead: `\\bfilter(?=\\s*\\()(?!\\s*\\(\\s*WHERE)`.

2. **Fix `trunc` mapping collision:**
   - The `trunc` direct mapping is being applied inside `CAST(TRUNC(literal AS TYPE))` patterns.
   - Root cause needs deeper investigation -- this may be a SQL generation bug where `TRUNC` is being inserted into CAST expressions inappropriately.

### Phase 2: Fix Broken Mappings (P1)

**Goal:** Fix the 8 test failures in test_dataframe_functions.py ArrayFunctions.

3. **Replace `array_union` mapping:**
   ```java
   CUSTOM_TRANSLATORS.put("array_union", args ->
       "list_distinct(list_concat(" + args[0] + ", " + args[1] + "))");
   ```

4. **Replace `array_except` mapping:**
   ```java
   CUSTOM_TRANSLATORS.put("array_except", args ->
       "list_filter(list_distinct(" + args[0] + "), x -> NOT list_contains(" + args[1] + ", x))");
   ```

5. **Fix `reverse` for arrays:**
   - In ExpressionConverter or SQLGenerator, detect when `reverse()` argument is array type and emit `list_reverse()`.
   - Alternatively, add type-aware dispatch in FunctionRegistry (would need access to argument types).

6. **Add `array_join` mapping:**
   ```java
   CUSTOM_TRANSLATORS.put("array_join", args -> {
       if (args.length == 2) return "array_to_string(" + args[0] + ", " + args[1] + ")";
       if (args.length == 3) return "array_to_string(" + args[0] + ", " + args[1] + ", " + args[2] + ")";
       throw new IllegalArgumentException("array_join requires 2 or 3 arguments");
   });
   ```

7. **Add `explode_outer` support:**
   - Handle at plan level in SQLGenerator/ExpressionConverter.

8. **Fix `array_position` return value for not-found:**
   - Investigate actual DuckDB behavior and add COALESCE wrapper if needed.

### Phase 3: Element Ordering and Additional Functions (P2)

**Goal:** Achieve exact Spark parity for array operations including element ordering.

9. **Fix `array_distinct` ordering:** Either wrap with `list_sort` or implement C++ extension that preserves insertion order.

10. **Fix `array_intersect` ordering:** Same approach.

11. **Add map function mappings** (map_keys, map_values, map_entries).

12. **Add `nvl2` and `nanvl` translators.**

### Phase 4: Extension Functions for Strict Mode (P3)

**Goal:** Full Spark parity including error behavior.

13. **Implement `spark_element_at` in C++ extension** -- throws on OOB in strict mode.
14. **Implement `spark_size` in C++ extension** -- returns -1 for NULL in legacy mode.
15. **Add remaining array functions** (sequence, array_repeat, array_remove, etc.).

---

## 8. C++ Extension Implementation Notes

### Architecture Pattern

All new extension scalar functions should follow the existing pattern:
1. Define a bind function that resolves types and selects the execution function
2. Define an execution function template parameterized by result type
3. Register in `LoadInternal()` using `loader.RegisterFunction()`

### SQL Macro Alternative

For functions that can be expressed as SQL, DuckDB supports SQL macros:
```sql
CREATE MACRO spark_array_union(arr1, arr2) AS
  list_distinct(list_concat(arr1, arr2));
```

Macros can be registered in the extension via:
```cpp
auto macro = make_uniq<ScalarMacroFunction>(
    make_uniq<FunctionExpression>("list_distinct", ...));
loader.RegisterFunction(macro);
```

**Recommendation:** Prefer SQL macros over C++ functions when possible. They are easier to maintain and debug. Use C++ functions only when:
- Custom error handling is needed (e.g., strict mode OOB errors)
- Performance-critical operations on large arrays
- Complex logic that cannot be expressed in SQL (e.g., preserving insertion order)

### Functions Suitable for SQL Macros

| Function | SQL Macro Expression |
|---|---|
| `spark_array_union` | `list_distinct(list_concat(arr1, arr2))` |
| `spark_array_except` | `list_filter(list_distinct(arr1), x -> NOT list_contains(arr2, x))` |
| `spark_array_remove` | `list_filter(arr, x -> x != val)` |
| `spark_array_compact` | `list_filter(arr, x -> x IS NOT NULL)` |
| `spark_array_position` | `COALESCE(list_position(arr, elem), 0)` |
| `spark_nvl2` | `CASE WHEN arg1 IS NOT NULL THEN arg2 ELSE arg3 END` |

### Functions Requiring C++ Implementation

| Function | Reason |
|---|---|
| `spark_element_at` (strict) | Must throw on OOB |
| `spark_size` (legacy) | Must return -1 for NULL (custom NULL handling) |
| `spark_array_distinct` | Must preserve insertion order (hash set with ordered iteration) |
| `spark_array_intersect` | Must preserve order from first array |

---

## 9. Key Files Reference

| File | Purpose |
|---|---|
| `/home/vscode/worktrees/array-expressions/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java` | Function name mapping and SQL translation |
| `/workspace/thunderduck-duckdb-extension/src/thdck_spark_funcs_extension.cpp` | DuckDB extension entry point |
| `/workspace/thunderduck-duckdb-extension/src/include/spark_aggregates.hpp` | Aggregate function implementations |
| `/workspace/thunderduck-duckdb-extension/src/include/spark_precision.hpp` | Decimal precision calculation |
| `/workspace/tests/integration/differential/test_array_functions_differential.py` | Array function tests |
| `/workspace/tests/integration/differential/test_complex_types_differential.py` | Complex type tests |
| `/workspace/tests/integration/differential/test_lambda_differential.py` | Lambda/HOF tests |
| `/workspace/tests/integration/differential/test_dataframe_functions.py` | DataFrame API function tests |
