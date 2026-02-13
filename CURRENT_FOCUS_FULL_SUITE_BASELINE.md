# Full Differential Test Suite Baseline

**Date**: 2026-02-13 (updated)

## Relaxed Mode (Complete)

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **746 passed, 0 failed, 2 skipped** (748 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 99/99 (100%) — all SQL queries passing
- **Lambda HOFs**: 27/27 (100%) — transform, filter, exists, forall, aggregate
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → 737/0/2 → **746/0/2**

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **684 passed, 64 failed, 0 skipped** (748 total)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → 658/81 → 665/83 → **684/64**

### What Changed (665/83 → 684/64)

Two fixes combined to eliminate 19 failures:

#### 1. SQL-path view schema caching (N2 fix)

The previous view schema caching (from `Session.registerTempView()`) only covered the DataFrame API path (`df.createOrReplaceTempView()`). The SQL path (`spark.sql("CREATE OR REPLACE TEMP VIEW ...")`) went through `SparkSQLAstBuilder.visitCreateView()` → `RawDDLStatement` → direct DuckDB DDL execution, bypassing `session.registerTempView()` entirely.

Fix: Thread view name and inner query `LogicalPlan` through `RawDDLStatement`, then cache `inferSchema()` result in `SparkConnectServiceImpl.executeSQLWithPlan()` after DDL execution.

| Component | Change |
|-----------|--------|
| `RawDDLStatement.java` | Added optional `viewName` and `viewQueryPlan` fields |
| `SparkSQLAstBuilder.java` | `visitCreateView()` attaches query plan metadata for temp views |
| `Session.java` | Added `cacheViewSchema()` for SQL-created views |
| `SparkConnectServiceImpl.java` | After DDL execution, cache view schema from query plan |

**Impact**: -14 failures (lambda 12→1, complex_types 10→0, update_fields tests fixed)

#### 2. Decimal precision/scale formulas (T2 fix)

Matched Spark's `DecimalPrecision` rules for arithmetic operators.

| Change | Detail |
|--------|--------|
| `adjustPrecisionScale()` | Shared utility for precision overflow handling |
| Multiplication | Use `adjustPrecisionScale` instead of naive `min()` cap |
| Addition/Subtraction | `promoteDecimalAddition()` with +1 carry digit |
| Modulo | `promoteDecimalModulo()` using `min(intDigits)` (Spark's remainder formula) |
| Division | Refactored to use shared `adjustPrecisionScale` |
| Integer promotion | DIVIDE promotes integer operands to DECIMAL when other is DECIMAL |
| LongType precision | Fixed from DECIMAL(19,0) to DECIMAL(20,0) |

**Impact**: -5 failures (TPC-H Q14/Q19 fixed, TPC-DS decimal queries improved)

---

### Root Cause Clustering (64 failures)

| # | Root Cause | Count | Fix Strategy |
|---|-----------|-------|-------------|
| **T2** | Decimal precision/scale mismatch | **~20** | Remaining precision edge cases in complex expressions |
| **T3** | DOUBLE ↔ DECIMAL confusion | **~10** | AVG/division return type mapping |
| **D1** | spark_decimal_div / gRPC errors | **~8** | Q4, Q11, Q66, Q74 gRPC errors; Q39a/Q39b decimal-div |
| **N2** | Nullable over-broadening (residual) | **~6** | Patterns not covered by view caching (createDataFrame, VALUES, inline) |
| **S1** | StringType fallback | **~3** | sql_transform_with_table, toSchema |
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |

### Failures by Test File

| File | Failures | Delta (from 83) | Primary Root Cause |
|------|----------|------------------|--------------------|
| `test_tpcds_differential.py` (SQL) | ~44 | +14 | T2 + T3 + D1 (some gRPC errors new) |
| `test_tpcds_differential.py` (DataFrame) | 3 | 0 | T3 (Q12, Q20, Q98) |
| `test_multidim_aggregations.py` | 4 | 0 | N2 (grouping nullable) |
| `test_tpcds_dataframe_differential.py` | 3 | 0 | T3 (Q12, Q20, Q98) |
| `test_differential_v2.py` (TPC-H SQL) | 3 | -1 | T2 + T3 (Q14, Q15, Q17) |
| `test_overflow_differential.py` | 2 | 0 | X2 |
| `test_type_casting_differential.py` | 2 | +1 | N2 + T2 |
| `test_lambda_differential.py` | 1 | -11 | S1 (sql_transform_with_table) |
| `test_simple_sql.py` | 1 | 0 | N2 (VALUES clause) |
| `test_to_schema_differential.py` | 1 | 0 | S1 |
| `test_complex_types_differential.py` | **0** | **-10** | All fixed by SQL-path view schema caching |
| `test_dataframe_functions.py` | **0** | **-4** | All fixed |
| **TOTAL** | **64** | **-19** | |

Zero-failure test files (all passing):
`test_tpch_differential.py`, `test_complex_types_differential.py`, `test_dataframe_functions.py`, `test_window_functions.py`, `test_joins_differential.py`, `test_datetime_functions_differential.py`, `test_array_functions_differential.py`, `test_sql_expressions_differential.py`, `test_string_functions_differential.py`, `test_math_functions_differential.py`, `test_column_operations_differential.py`, `test_null_handling_differential.py`, `test_subquery_differential.py`, `test_ddl_parser_differential.py`, `test_dataframe_ops_differential.py`, `test_conditional_differential.py`, `test_type_literals_differential.py`, `test_using_joins_differential.py`

---

### Prioritized Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | T2: Decimal precision | ~20 | Medium | Remaining precision edge cases in complex multi-operator expressions |
| **P2** | T3: DOUBLE ↔ DECIMAL | ~10 | Medium | AVG-over-DECIMAL → DOUBLE, division widening rules |
| **P3** | D1: gRPC / decimal-div | ~8 | Medium | Debug Q4/Q11/Q66/Q74 gRPC errors; Q39a/Q39b decimal-div fix |
| **P4** | N2: Nullable residual | ~6 | Hard | createDataFrame, VALUES, inline subquery nullable patterns |
| **P5** | S1: StringType fallback | ~3 | Low | sql_transform_with_table, toSchema |
| **P6** | X2: Overflow | 2 | Low | Add overflow detection to spark_sum extension |

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
