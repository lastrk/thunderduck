# Full Differential Test Suite Baseline

**Date**: 2026-02-13 (updated)

## Relaxed Mode

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **739 passed, 7 failed, 2 skipped** (748 total)

- **TPC-H**: 51/51 (100%) — 29 SQL + 22 DataFrame
- **TPC-DS**: 94/99 — Q4, Q11, Q61, Q66, Q74 failing (spark_sum/spark_decimal_div issues)
- **Lambda HOFs**: 27/27 (100%) — transform, filter, exists, forall, aggregate
- **2 skipped**: negative array index tests (`skip_relaxed` — DuckDB supports `arr[-1]`, Spark throws)
- **5 relaxed regressions**: From decimal precision merge — extension functions emitted in relaxed mode for some TPC-DS queries

**Previous baselines**: 646/88/5 → 708/26/5 → 718/16/5 → 733/1/5 → 737/0/2 → 746/0/2 → **739/7/2**

---

## Strict Mode Baseline

**Command**: `cd /workspace/tests/integration && THUNDERDUCK_COMPAT_MODE=strict THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`
**Result**: **685 passed, 63 failed, 0 skipped** (748 total)

**Previous baselines**: 541/198 → 623/116 → 636/103 → 638/88 → 658/81 → 665/83 → 684/64 → **685/63**

### What Changed (684/64 → 685/63)

#### S1 fix: ToSchema schema preservation + SQL-path array type resolution

| Component | Change |
|-----------|--------|
| `RelationConverter.java` | `convertToSchema()` builds target StructType and passes to `SQLRelation(sql, targetSchema)` |
| `ExpressionConverter.java` | Made `convertDataType()` package-private for RelationConverter access |
| `SparkSQLAstBuilder.java` | `mapDuckDBTypeToThunderduck()` handles ARRAY/LIST types via `SchemaInferrer.mapDuckDBType()` |
| `TypeInferenceEngine.java` | Added `LambdaVariableExpression` handling in `resolveType()`; transform handler resolves lambda body types |

**Impact**: -1 failure (`test_to_schema_then_select` fixed). `test_sql_transform_with_table` type corrected from StringType to ArrayType (reclassified from S1 to N2 — nullable mismatch only).

**Baseline correction**: Previous 684/64 count was optimistic — complex_types (7) and dataframe_functions (4) failures were masked by test ordering effects. These are pre-existing N2/S1 issues confirmed to exist at the baseline commit without S1 changes.

---

### Root Cause Clustering (63 failures)

| # | Root Cause | Count | Fix Strategy |
|---|-----------|-------|-------------|
| **T2** | Decimal precision/scale mismatch | **~20** | Remaining precision edge cases in complex expressions |
| **T3** | DOUBLE ↔ DECIMAL confusion | **~10** | AVG/division return type mapping |
| **N2** | Nullable over-broadening (residual) | **~12** | createDataFrame, VALUES, struct/map field access, view schema gaps |
| **D1** | spark_decimal_div / gRPC errors | **~8** | Q4, Q11, Q66, Q74 gRPC errors; Q39a/Q39b decimal-div |
| **S1** | StringType fallback (residual) | **~7** | map value access, flatten element type, chained extraction |
| **X2** | Overflow behavior mismatch | **2** | DuckDB silently promotes; Spark throws |

### Failures by Test File

| File | Failures | Primary Root Cause |
|------|----------|-------------------|
| `test_tpcds_differential.py` (SQL) | 33 | T2 + T3 + D1 |
| `test_complex_types_differential.py` | 7 | N2 (nullable) + S1 (map/struct type) |
| `test_tpcds_differential.py` (DataFrame) | 3 | T3 (Q12, Q20, Q98) |
| `test_tpcds_dataframe_differential.py` | 3 | T3 (Q12, Q20, Q98) |
| `test_differential_v2.py` (TPC-H SQL) | 4 | T2 + T3 (Q13, Q14, Q15, Q17) |
| `test_dataframe_functions.py` | 4 | S1 (flatten, map_keys, map_values, map_from_arrays) |
| `test_multidim_aggregations.py` | 4 | N2 (grouping nullable) |
| `test_overflow_differential.py` | 2 | X2 |
| `test_lambda_differential.py` | 1 | N2 (sql_transform_with_table nullable) |
| `test_simple_sql.py` | 1 | N2 (VALUES clause) |
| `test_type_casting_differential.py` | 1 | T2 |
| **TOTAL** | **63** | |

Zero-failure test files (all passing):
`test_tpch_differential.py`, `test_to_schema_differential.py`, `test_window_functions.py`, `test_joins_differential.py`, `test_datetime_functions_differential.py`, `test_array_functions_differential.py`, `test_sql_expressions_differential.py`, `test_string_functions_differential.py`, `test_math_functions_differential.py`, `test_column_operations_differential.py`, `test_null_handling_differential.py`, `test_subquery_differential.py`, `test_ddl_parser_differential.py`, `test_dataframe_ops_differential.py`, `test_conditional_differential.py`, `test_type_literals_differential.py`, `test_using_joins_differential.py`

---

### Prioritized Fix Plan

| Priority | Cluster | Tests | Effort | Strategy |
|----------|---------|-------|--------|----------|
| **P1** | T2: Decimal precision | ~20 | Medium | Remaining precision edge cases in complex multi-operator expressions |
| **P2** | T3: DOUBLE ↔ DECIMAL | ~10 | Medium | AVG-over-DECIMAL → DOUBLE, division widening rules |
| **P3** | N2: Nullable residual | ~12 | Hard | struct/map field access, createDataFrame, VALUES, inline subquery |
| **P4** | D1: gRPC / decimal-div | ~8 | Medium | Debug Q4/Q11/Q66/Q74 gRPC errors; Q39a/Q39b decimal-div fix |
| **P5** | S1: StringType residual | ~7 | Medium | MAP/STRUCT type parsing in mapDuckDBTypeToThunderduck, function return types |
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
