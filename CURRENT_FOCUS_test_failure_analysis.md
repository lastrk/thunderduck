# Current Focus: Test Failure Root Cause Analysis

## Objective

Analyze the differential test failures from `run-differential-tests-v2.sh` to:
1. Cluster failures by root cause
2. Identify the most impactful fixes
3. Address root causes systematically

## Current State (as of 2025-12-20)

- **Failed**: 417
- **Passed**: 155
- **Skipped**: 2
- **Pass Rate**: ~27% (up from ~21%)

### Recent Improvements

- **Window Function Types**: Fixed ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) to return IntegerType instead of LongType (+7 tests passing)
- **Nullable Inference**: Added `SchemaCorrectedBatchIterator` and `WithColumns` logical plan to properly infer nullable flags from expressions
- **WithColumns Plan**: Replaced raw SQLRelation with proper WithColumns plan class for correct schema inference

## Root Cause Analysis (Updated)

### Issue #1: GroupBy/Aggregate Schema Loss (~200+ tests) - PARTIALLY FIXED

**Symptoms**:
- Column names become `group_0`, `group_1`, `group_2` instead of actual names
- All types become `StringType()`
- Affects TPC-DS, TPC-H DataFrame tests

**Status**: WithColumns schema inference fixed, but groupBy operations may still have issues.

**Files to Check**: `Aggregate.java`, `SQLGenerator.java`, schema propagation through groupBy

---

### Issue #2: Window Functions (~35 tests) - RANKING FIXED, OTHERS PENDING

**Symptoms**:
- Ranking functions now return correct IntegerType
- LAG, LEAD, FIRST, LAST, NTH_VALUE still have type mismatches
- Frame specifications (ROWS BETWEEN, RANGE BETWEEN) may have issues

**Status**: Ranking functions (row_number, rank, dense_rank, ntile) FIXED. Analytic functions pending.

**Files to Fix**: `ExpressionConverter.java` - LAG/LEAD/FIRST/LAST type inference

---

### Issue #3: USING Join Column Deduplication (~10 tests)

**Symptoms**:
- Column count mismatch (Test has extra columns)
- USING join columns appear twice instead of once

**Status**: Not yet addressed.

**Files to Fix**: `Join.java` or SQL generation for USING joins

---

### Issue #4: Missing SQL Syntax Translation (~70 tests)

**Symptoms**:
- `Parser Error: syntax error at or near "10"` for `ARRAY(10, 20, 30)`
- `Scalar Function with name named_struct does not exist`

**Functions needing translation**:
| Spark SQL | DuckDB Equivalent |
|-----------|------------------|
| `ARRAY(1, 2, 3)` | `[1, 2, 3]` or `list_value(1, 2, 3)` |
| `MAP('a', 1)` | `MAP(['a'], [1])` |
| `NAMED_STRUCT('x', 1)` | `{'x': 1}` or `struct_pack(x := 1)` |

**Status**: Not yet addressed.

---

### Issue #5: Missing DuckDB Functions (~30 tests)

| Function | Count | DuckDB Alternative |
|----------|-------|-------------------|
| `named_struct` | 22 | struct literal syntax |
| `list_union` | 2 | Need custom implementation |
| `list_except` | 2 | Need custom implementation |
| `explode_outer` | 2 | Need LATERAL JOIN pattern |
| `array_join` | 2 | `list_string_agg` |
| `initcap` | 2 | Custom UDF or implementation |

**Status**: Not yet addressed.

---

### Issue #6: Type Width Mismatches (~50 tests)

| Pattern | Count | Fix |
|---------|-------|-----|
| IntegerType vs LongType | ~40 | Partially fixed for window functions |
| LongType vs DecimalType | 20 | Map decimal types correctly |
| ByteType vs LongType | 12 | Small int handling |

**Status**: Window function types fixed. Other type mismatches pending.

---

### Issue #7: Lambda Function Tests (~18 tests)

All lambda tests fail - likely `transform`, `filter`, `aggregate` function translation issues.

**Status**: Not yet addressed.

---

### Issue #8: Nullable Inference - PARTIALLY FIXED

**Symptoms**:
- Many tests fail only due to nullable mismatches
- Thunderduck marks columns as `nullable=True` when Spark says `nullable=False`

**Status**: Fixed for:
- Ranking window functions (ROW_NUMBER, RANK, etc.)
- COUNT aggregates
- WithColumns expressions

Still pending for other operations.

**Files Modified**:
- `WindowFunction.java` - `nullable()` returns false for ranking functions
- `Aggregate.AggregateExpression.java` - `nullable()` returns false for COUNT
- `SchemaCorrectedBatchIterator.java` - NEW - corrects Arrow schema nullable flags
- `WithColumns.java` - NEW - proper schema inference for withColumn operations

---

## Priority Fix Order (by Impact)

| Priority | Issue | Est. Tests | Status |
|----------|-------|------------|--------|
| 1 | GroupBy Schema Loss | ~200 | Partially Fixed |
| 2 | SQL Syntax (ARRAY, MAP, STRUCT) | ~70 | Not Started |
| 3 | Window Analytic Functions (LAG, LEAD) | ~20 | Not Started |
| 4 | Missing Functions | ~30 | Not Started |
| 5 | Lambda Functions | ~18 | Not Started |
| 6 | USING Join Dedup | ~10 | Not Started |
| 7 | Type Width (remaining) | ~30 | Partially Fixed |

## Next Steps

1. [ ] **Fix GroupBy schema loss** - check if remaining issues in Aggregate/Project
   - Debug why groupBy loses column names/types in some cases

2. [ ] **Add ARRAY/MAP/STRUCT literal translation**
   - Update `ExpressionConverter` for complex type literals

3. [ ] **Fix window analytic function types**
   - LAG, LEAD, FIRST, LAST, NTH_VALUE type inference

4. [ ] **Implement missing functions**
   - Add `list_union`, `list_except`, `explode_outer` translations

## Investigation Commands

```bash
# Run specific test with verbose output
python3 -m pytest differential/test_tpcds_differential.py::TestTPCDS_DataFrame_Differential::test_q3_dataframe -v -s

# Check generated SQL
grep "Generated SQL" /tmp/server.log

# Run window function tests only
python3 -m pytest differential/test_window_functions.py -v

# Run all differential tests
/workspace/tests/scripts/run-differential-tests-v2.sh
```

## Test Results History

| Date | Passed | Failed | Pass Rate | Notes |
|------|--------|--------|-----------|-------|
| 2025-12-20 | 155 | 417 | 27% | Nullable fix, WithColumns plan |
| Previous | 121 | 451 | 21% | Baseline |
