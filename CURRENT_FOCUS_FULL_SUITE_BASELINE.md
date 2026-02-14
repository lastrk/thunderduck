# Full Differential Test Suite Baseline

**Date**: 2026-02-14 (updated)

## Relaxed Mode

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **746 passed, 0 failed, 2 skipped** (748 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL + DataFrame passing
- **Lambda HOFs**: 27/27 (100%) — transform, filter, exists, forall, aggregate
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)
- **0 regressions**: No new failures from N2/T1/G1/D1 fixes

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → 737/0/2 → 746/0/2 → 739/7/2 → 746/0/2 → **746/0/2**

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **744 passed, 2 failed, 2 skipped** (748 total)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → 658/81 → 665/83 → 684/64 → 685/63 → 712/36 → 727/21 → 743/3 → **744/2**

### What Changed (727/21 → 743/3)

#### N2 fix: Nullable over-broadening

| Component | Change |
|-----------|--------|
| `TypeInferenceEngine.java` | Fixed nullable inference for struct field access, grouping functions, VALUES clause, lambda expressions |
| `SchemaInferrer.java` | Corrected containsNull propagation |

**Impact**: Fixed ~8 tests across complex types, multidim aggregations, lambda, and simple SQL.

#### T1 fix: stddev/variance type mismatch

| Component | Change |
|-----------|--------|
| `TypeInferenceEngine.java` | stddev/variance now correctly returns DoubleType instead of LongType |

**Impact**: Fixed ~2 tests (Q17, test_multiple_aggregations_same_column).

#### G1 fix: grouping/grouping_id type inference

| Component | Change |
|-----------|--------|
| `TypeInferenceEngine.java` | grouping() returns ByteType, grouping_id returns correct type |

**Impact**: Fixed ~3 tests (Q27, Q70, Q86).

### What Changed (743/3 → 744/2)

#### D1 fix: map_keys containsNull mismatch

| Component | Change |
|-----------|--------|
| `TypeInferenceEngine.java` | Changed `map_keys` return type from `ArrayType(keyType, false)` to `ArrayType(keyType, true)` at lines ~1024 and ~1213 to match Spark's conservative nullable marking |

**Impact**: Fixed `test_dataframe_functions.py::TestMapFunctions::test_map_keys` in strict mode. Spark conservatively marks `map_keys()` output elements as nullable (`containsNull=true`) even though map keys cannot actually be null.

### What Changed (712/36 → 727/21)

#### S1 fix: Complex type return type inference

| Component | Change |
|-----------|--------|
| `TypeInferenceEngine.java` | Added return type handlers for `map_keys`, `map_values`, `flatten`, `split`, `map_entries`, `map_from_arrays`, `map`/`create_map` in ArrayType and MapType branches |

**Impact**: Fixed 3 `test_dataframe_functions` tests (flatten, map_values, map_from_arrays) and improved schema inference for map/array operations.

#### SQL-path decimal dispatch: Schema-aware expression transformation

| Component | Change |
|-----------|--------|
| `SQLGenerator.java` | Added `transformExpressionForStrictMode()` — recursive expression walker that resolves column types from child schema and rewrites DIVIDE to `spark_decimal_div()`, wraps SUM/AVG on DECIMAL with `CAST(sum/avg(...) AS DECIMAL(p,s))` |
| `SQLGenerator.java` | Changed `renderAggregateExpression()` from `spark_sum`/`spark_avg` extension functions to `CAST(sum/avg(...) AS DECIMAL(p,s))` to avoid DuckDB optimizer crash in UNION ALL CTEs |
| `SQLGenerator.java` | Enhanced `qualifyCondition()` with schema parameter for type-aware division dispatch in join paths |
| `BinaryExpression.java` | Added `toSparkSQL()` method for column naming without DuckDB-specific rewrites |
| `Project.java` | Updated `buildSparkColumnName()` to use `toSparkSQL()` to prevent `spark_decimal_div` leaking into column names |
| `TypeInferenceEngine.java` | Added DECIMAL-preserving type inference for `round`/`bround`; MapType STRUCT_FIELD fallback |

**Impact**: Fixed 12 TPC-DS SQL failures caused by unresolved column types in the SQL path preventing correct dispatch to extension functions:
- Q2, Q59: Decimal division in CTEs
- Q4, Q11, Q66, Q74: CTE decimal cascade with UNION ALL (worked around DuckDB `spark_sum` optimizer crash)
- Q9, Q28: AVG(DECIMAL) in scalar subqueries / derived tables
- Q61: Column name leakage fix

**Key design decision**: Replaced `spark_sum`/`spark_avg` extension aggregate functions with `CAST(sum/avg(...) AS DECIMAL(p,s))`. DuckDB's native `sum()` preserves DECIMAL precision; the CAST adjusts precision to match Spark's formula. This avoids a DuckDB `CompressedMaterialization::CompressAggregate` optimizer crash when `spark_sum` is used inside UNION ALL CTEs.

---

### Root Cause Clustering (2 remaining failures)

| # | Root Cause | Count | Status |
|---|-----------|-------|--------|
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |
| ~~N2~~ | ~~Nullable over-broadening~~ | ~~0~~ | **RESOLVED** (743/3 commit) |
| ~~T1~~ | ~~stddev/variance type mismatch~~ | ~~0~~ | **RESOLVED** (743/3 commit) |
| ~~G1~~ | ~~grouping/grouping_id type~~ | ~~0~~ | **RESOLVED** (743/3 commit) |
| ~~S1~~ | ~~StringType fallback~~ | ~~0~~ | **RESOLVED** (727/21 commit) |
| ~~D1~~ | ~~map_keys containsNull~~ | ~~0~~ | **RESOLVED** (this commit) |

### Failures by Test File

| File | Failures | Root Cause |
|------|----------|------------|
| `test_overflow_differential.py` | 2 | X2 (overflow behavior) |
| **TOTAL** | **2** | |

Zero-failure test files (all passing): all 35 other test files including `test_tpch_differential.py`, `test_tpcds_differential.py`, `test_tpcds_dataframe_differential.py`, `test_complex_types_differential.py`, `test_multidim_aggregations.py`, `test_dataframe_functions.py`, `test_lambda_differential.py`, `test_simple_sql.py`, etc.

---

### Remaining Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | X2: Overflow | 2 | Low | Add overflow detection to spark_sum extension |

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
