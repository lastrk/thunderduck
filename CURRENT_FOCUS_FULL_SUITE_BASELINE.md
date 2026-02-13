# Full Differential Test Suite Baseline

**Date**: 2026-02-13 (updated)

## Relaxed Mode (Complete)

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **746 passed, 0 failed, 2 skipped** (748 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL queries passing
- **Lambda HOFs**: 27/27 (100%) — transform, filter, exists, forall, aggregate
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → 737/0/2 → **746/0/2** (+9 from merged lambda tests)

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **665 passed, 83 failed, 0 skipped** (748 total)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → 658/81 → **665/83**

### What Changed (658/81 → 665/83)

Two categories of change in this update:

#### 1. Merged `feat-lambda-expressions` branch (+9 new tests, 12 new failures)

| Test File | New Tests | Pass | Fail | Root Cause |
|-----------|-----------|------|------|------------|
| `test_lambda_differential.py` | +13 new | +2 | +11 | N2: DuckDB DESCRIBE marks temp view columns as nullable |
| `test_tpcds_dataframe_differential.py` | (new file, -4 moved) | -4 | 0 | Tests moved from test_tpcds_differential.py |

Note: `test_lambda_differential.py` grew from 14 to 27 tests. The 13 new tests include transform, filter, aggregate, and SQL lambda tests. The +2 SQL aggregate tests pass because they go through the SQL parser path.

#### 2. Type inference improvements (15 existing tests fixed)

| Change | What it fixed | Tests fixed |
|--------|--------------|-------------|
| SchemaInferrer: parse MAP types | MAP(k,v) no longer falls back to StringType | ~2 |
| SchemaInferrer: parse STRUCT types | STRUCT(...) no longer falls back to StringType | ~3 |
| SchemaInferrer: LIST containsNull=false | Arrays from SQL match Spark's containsNull=false default | ~5 |
| TypeInferenceEngine: grouping/grouping_id | ByteType/LongType instead of missing, non-nullable | ~3 |
| TypeInferenceEngine: split, map_entries | Correct return types instead of fallback | ~2 |
| TypeInferenceEngine: UpdateFieldsExpression | Correct struct type resolution for ADD/DROP | ~1 |
| ExpressionConverter: aggregate/reduce type | Fixed hardcoded StringType → initArg.dataType() | 3 |
| TypeInferenceEngine: aggregate nullable | Fixed to always-nullable (Spark semantics) | 2 |

**Net movement**: 81 → 83 failures (+2), 658 → 665 passed (+7). Adjusted for 9 new tests: existing test failures dropped from 81 → 71 (-10).

---

### Root Cause Clustering (83 failures)

| # | Root Cause | Count | Fix Strategy |
|---|-----------|-------|-------------|
| **N2** | Nullable over-broadening (DuckDB DESCRIBE) | **~22** | DuckDB DESCRIBE marks all computed columns nullable; needs per-expression nullable narrowing |
| **T2** | Decimal precision/scale mismatch | **~15** | SUM precision formula, scale propagation |
| **T3** | DOUBLE ↔ DECIMAL confusion | **~10** | AVG/division return type mapping |
| **S1** | StringType fallback (reduced from ~15) | **~5** | Remaining: struct field access, sql_transform_with_table |
| **D1** | spark_decimal_div BinaryExpression path | **4** | Re-land integer-to-DECIMAL promotion fix |
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |
| **R1** | Misc (toSchema, VALUES types) | **~2** | Individual fixes |

### N2: Nullable Over-broadening (~22 tests)

DuckDB DESCRIBE returns `null=YES` for all computed expression columns (e.g., `SELECT ARRAY(1,2,3) AS arr` → arr is nullable). Spark correctly infers these as non-nullable. This propagates through HOFs (transform, filter) and complex type access.

| Sub-group | Tests | Pattern |
|-----------|-------|---------|
| **Lambda functions** | 11 | Lambda results nullable because source arrays are nullable from DESCRIBE |
| **Complex types** | ~5 | Struct/map access — nullable propagates through extraction |
| **TPC-DS SQL** | ~4 | Nullable mismatches stacking with existing decimal issues |
| **Other** | ~2 | VALUES clause, decimal_scale_change |

### S1: StringType Fallback (~5 tests) — reduced from ~15

Most S1 issues fixed by SchemaInferrer MAP/STRUCT parsing and TypeInferenceEngine additions. Remaining:

| Function/Pattern | Expected Type | Tests |
|-----------------|--------------|-------|
| Struct field access (strict mode) | Struct field type | ~3 |
| `sql_transform_with_table` | ArrayType from SQL path | 1 |
| `toSchema` coercion | Target schema type | 1 |

### T2: Decimal Precision/Scale (~15 tests) — persistent

`DecimalType(37,2)` vs `DecimalType(38,2)` — SUM precision off by 1. Also wrong scale in division results.

### T3: DOUBLE ↔ DECIMAL Confusion (~10 tests) — persistent

AVG over DECIMAL returns DECIMAL in DuckDB but DOUBLE in Spark. Division type inference doesn't match Spark's widening rules.

### D1: spark_decimal_div BinaryExpression Path (4 tests) — persistent, fix reverted

Q23a/Q23b fail through `BinaryExpression.generateStrictModeDivision()`. Q39a/Q39b fail with raw SQL StringType issues.

### X2: Overflow Behavior (2 tests) — persistent

DuckDB's `spark_sum` extension doesn't throw on integer overflow. Spark throws `ArithmeticException`.

---

### Failures by Test File

| File | Failures | Delta (from 81) | Primary Root Cause |
|------|----------|------------------|--------------------|
| `test_tpcds_differential.py` (SQL) | 30 | -9 | T2 + T3 + D1 |
| `test_lambda_differential.py` | 12 | -2 (was 14, +11 new, -3 fixed) | N2 |
| `test_complex_types_differential.py` | 10 | 0 | N2 + S1 |
| `test_tpcds_dataframe_differential.py` | 3 | +3 (moved from tpcds_differential) | T3 |
| `test_dataframe_functions.py` | 4 | -2 | S1 (map functions fixed, some remain) |
| `test_differential_v2.py` (TPC-H SQL) | 4 | +1 | T2 + T3 |
| `test_multidim_aggregations.py` | 4 | 0 | N2 + S1 (grouping type fixed, nullable remains) |
| `test_overflow_differential.py` | 2 | 0 | X2 |
| `test_simple_sql.py` | 1 | 0 | N2 |
| `test_type_casting_differential.py` | 1 | 0 | N2 |
| `test_to_schema_differential.py` | 1 | 0 | S1 |
| `test_tpcds_differential.py` (DataFrame) | 3 | +3 (moved) | T3 |
| **TOTAL** | **83** | **+2** | |

Zero-failure test files (all passing):
`test_tpch_differential.py`, `test_window_functions.py`, `test_joins_differential.py`, `test_datetime_functions_differential.py`, `test_array_functions_differential.py`, `test_sql_expressions_differential.py`, `test_string_functions_differential.py`, `test_math_functions_differential.py`, `test_column_operations_differential.py`, `test_null_handling_differential.py`, `test_subquery_differential.py`, `test_ddl_parser_differential.py`, `test_dataframe_ops_differential.py`, `test_conditional_differential.py`, `test_type_literals_differential.py`, `test_using_joins_differential.py`

---

### Prioritized Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | N2: Nullable over-broadening | ~22 | Hard | DuckDB DESCRIBE limitation; needs per-expression nullable narrowing or DESCRIBE bypass |
| **P2** | T2: Decimal precision | ~15 | Low | Fix SUM precision formula (38 not 37), scale propagation |
| **P3** | T3: DOUBLE ↔ DECIMAL | ~10 | Medium | AVG-over-DECIMAL → DOUBLE, division widening rules |
| **P4** | S1: StringType fallback | ~5 | Low | Struct field access, sql_transform type resolution |
| **P5** | D1: decimal-div BinaryExpr | 4 | Low | Re-land reverted fix (with extension rebuild) |
| **P6** | X2: Overflow | 2 | Low | Add overflow detection to spark_sum extension |

**Note on N2**: The remaining nullable over-broadening issues are fundamentally caused by DuckDB DESCRIBE returning `null=YES` for all computed expression columns. This affects temp views created with array/struct/map constructors. Fix options: (a) bypass DESCRIBE for views where we know the defining SQL, (b) infer nullable from the view's defining expression, or (c) accept as a known limitation.

---

### Architecture Goal: Zero-Copy Strict Mode

**Two invariants that must hold simultaneously:**

```
Apache Spark 4.1 types (authoritative truth)
  <- must match -> DuckDB output (shaped by SQL generation + extension functions)
  <- must match -> inferSchema() (type inference in logical plan)
```

1. **Spark is the authority.** The target types, precision, scale, and nullability are defined by what Apache Spark 4.1 returns. We don't approximate -- we match exactly.

2. **DuckDB output must match Spark at the engine level.** This is achieved through SQL generation (CASTs, `AS` aliases, function rewrites) and DuckDB extension functions (`spark_avg`, `spark_sum`, `spark_decimal_div`). No post-hoc Arrow rewriting.

3. **`inferSchema()` must match DuckDB output.** The logical plan's type inference must return exactly the same types that the generated SQL will produce when executed by DuckDB.

**`SchemaCorrectedBatchIterator` has been removed.** DuckDB Arrow batches flow through with no schema patching. All type correctness is achieved at SQL generation time. Zero Arrow vector copying. Zero runtime type conversion.

---

## Performance Optimization: `__int128` Accumulators (2026-02-12)

Changed `SparkSumDecimalState` and `SparkAvgDecimalState` accumulators from `hugeint_t` to `__int128` in `spark_aggregates.hpp`.

| Operation | Before | After |
|-----------|--------|-------|
| SUM/AVG per-row accumulation | `hugeint_t::operator+=` (non-inline library call) | `__int128 +=` (inline ADD/ADC, 2 instructions) |
| SUM/AVG ConstantOperation | `Hugeint::Convert` + `hugeint_t::operator*` (2 non-inline calls) | `__int128 *` (inline MUL, 1 instruction) |
| SUM/AVG Combine | `hugeint_t::operator+=` (non-inline) | `__int128 +=` (inline) |
| SUM Finalize | `HugeintToInt128` conversion + write | Direct write from `__int128` state |
| AVG Finalize | `HugeintToInt128` conversion + division + write | Division + write (skip conversion) |

Input arrives as `hugeint_t` (DuckDB's type), converted once per row via `HugeintToInt128()` (already inline in `wide_integer.hpp`). All arithmetic stays in `__int128` until finalize, where `WriteAggResult` converts back to the target physical type.
