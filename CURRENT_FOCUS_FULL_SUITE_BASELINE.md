# Full Differential Test Suite Baseline

**Date**: 2026-02-12

## Relaxed Mode (Complete)

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **737 passed, 0 failed, 2 skipped** (739 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL queries passing
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → **737/0/2**

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **623 passed, 116 failed** (739 total)

**Previous baselines**: 541/198 → **623/116**

### What Changed (541→623): `__int128` Accumulator + Prior Fixes

Performance optimization in `spark_aggregates.hpp`: switched SUM/AVG decimal state accumulators from `hugeint_t` to native `__int128`. All per-row `+=` operations now compile to inline ADD/ADC instruction pairs instead of non-inline `hugeint_t::operator+=` library calls. No correctness changes — same arithmetic, faster execution.

The 541→623 improvement reflects cumulative fixes since the previous baseline snapshot (strict mode type inference, extension function emission guards, schema correction improvements).

### Root Cause Clustering (116 failures)

| # | Root Cause | Tests | Fix Area |
|---|-----------|-------|----------|
| **C1** | Nullable flag mismatches | **~60** | Nullable inference across plan nodes |
| **C2** | Unsupported features (lambda, complex types, unpivot, cube/rollup) | **~45** | Feature implementation |
| **C3** | Extension function missing type overloads | **~8** | DuckDB C++ extension or SQL gen guards |
| **C4** | Type mismatches (precision off by 1, DECIMAL vs DOUBLE) | **~3** | TypeInferenceEngine / SQL generation |

---

### C1: Nullable Flag Mismatches (~60 tests)

The dominant failure mode. Thunderduck returns `nullable=False` where Spark returns `nullable=True` (or vice versa). Affects TPC-H (Q12, Q13, Q15, Q17), TPC-DS (~30 queries), and several feature tests.

These are cross-cutting: they appear in tests that otherwise produce correct values, types, and column names. The nullable inference in `inferSchema()` doesn't match Spark's nullable propagation rules for aggregations, joins, and subqueries.

### C2: Unsupported Features (~45 tests)

| Feature | Test File | Failures |
|---------|-----------|----------|
| Lambda functions | `test_lambda_differential.py` | 18 |
| Complex types (struct/map) | `test_complex_types_differential.py` | 13 |
| Multidim aggregations (cube/rollup/unpivot) | `test_multidim_aggregations.py` | 12 |
| DataFrame functions (array, map, null, string, math) | `test_dataframe_functions.py` | ~12 |

These are unimplemented or partially implemented features — not regressions.

### C3: Extension Function Overloads (~8 tests)

`spark_decimal_div` receives non-DECIMAL inputs (DOUBLE or INTEGER) when it expects DECIMAL. Affects TPC-DS Q4, Q11, Q23a/b, Q74, and a few others. Fix: either add non-DECIMAL overloads to the C++ extension, or guard the SQL generator to only emit extension functions for confirmed DECIMAL inputs.

### C4: Type Precision Mismatches (~3 tests)

- `DecimalType(38,2)` vs `DecimalType(37,2)` — precision off by 1
- `DecimalType(27,2)` vs `DoubleType()` — AVG/division returning DOUBLE instead of DECIMAL

---

### Failures by Test File

| File | Failures | Primary Root Cause |
|------|----------|--------------------|
| `test_tpcds_differential.py` | 40 | C1 (nullable) + C3 (overloads) |
| `test_lambda_differential.py` | 18 | C2 (unsupported) |
| `test_complex_types_differential.py` | 13 | C2 (unsupported) |
| `test_multidim_aggregations.py` | 12 | C2 (unsupported) |
| `test_dataframe_functions.py` | 12 | C2 (unsupported) |
| `test_differential_v2.py` (TPC-H SQL) | 4 | C1 (nullable) |
| `test_array_functions_differential.py` | 3 | C2 (unsupported) |
| `test_sql_expressions_differential.py` | 3 | C2 (unsupported) |
| `test_overflow_differential.py` | 2 | C3 (overloads) |
| `test_range_operations_differential.py` | 2 | C2 (unsupported) |
| Others (5 files) | 7 | Mixed |

---

### Architecture Goal: Zero-Copy Strict Mode

**Three invariants that must hold simultaneously:**

```
Apache Spark 4.1 types (authoritative truth)
  ← must match → DuckDB output (shaped by SQL generation + extension functions)
  ← must match → inferSchema() (type inference in logical plan)
```

1. **Spark is the authority.** The target types, precision, scale, and nullability are defined by what Apache Spark 4.1 returns. We don't approximate — we match exactly.

2. **DuckDB output must match Spark at the engine level.** This is achieved through a combination of SQL generation (CASTs, `AS` aliases, function rewrites) and DuckDB extension functions (`spark_avg`, `spark_sum`, `spark_decimal_div`). The extension functions exist to produce Spark-correct types where vanilla DuckDB diverges. No post-hoc Arrow vector rewriting — types must be correct in the Arrow data DuckDB produces.

3. **`inferSchema()` must match DuckDB output.** The logical plan's type inference must return exactly the same types that the generated SQL will produce when executed by DuckDB. If these disagree, `AnalyzePlan` responses (used by PySpark for `df.schema`) will report different types than the actual Arrow data.

**`SchemaCorrectedBatchIterator` should not be needed in strict mode.** If it's still required, it means either the SQL generation isn't producing Spark-correct types at the DuckDB level, or `inferSchema()` doesn't match what DuckDB returns. Both are bugs to fix, not to paper over with post-hoc correction.

**No performance compromise.** All type correctness is achieved at query planning / SQL generation time. Zero Arrow vector copying. Zero runtime type conversion. The extension functions run inside DuckDB's execution engine at native speed.

### Priority Fix Order

1. **C1 (Nullable)** — ~60 tests. Nullable inference doesn't match Spark's propagation rules for aggregations, joins, and subqueries. Fix: audit and correct nullable propagation in `inferSchema()` across plan nodes.
2. **C2 (Unsupported features)** — ~45 tests. Lambda functions, complex types, cube/rollup/unpivot. Fix: implement missing features incrementally.
3. **C3 (Extension overloads)** — ~8 tests. Extension only has DECIMAL overloads. Fix: add guards in SQL gen to only emit extension functions for confirmed DECIMAL inputs, or add overloads.
4. **C4 (Type precision)** — ~3 tests. Precision calculations off by 1. Fix: match Spark 4.1 precision rules exactly.

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
