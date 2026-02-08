# Relaxed Mode Differential Test Report

**Date**: 2026-02-08
**Mode**: `THUNDERDUCK_COMPAT_MODE=relaxed`
**Extension loaded**: No (vanilla DuckDB)
**Test files**: `test_tpch_differential.py`, `test_differential_v2.py`
**Test runner**: pytest with `--tb=short`, `COLLECT_TIMEOUT=30`, `CONTINUE_ON_ERROR=true`

## Summary

| Metric | Count | Percentage |
|--------|-------|-----------|
| **Total** | 51 | 100% |
| **PASSED** | 51 | 100.0% |
| **FAILED** | 0 | 0.0% |
| **ERRORED** | 0 | 0.0% |
| **SKIPPED** | 0 | 0.0% |

**Result: ALL 51 TESTS PASSED in relaxed mode.**

Total execution time: 27.50 seconds.

## Breakdown by Test Class

### DataFrame Tests (`test_tpch_differential.py` -- TestTPCHDifferential)

| Test | Status |
|------|--------|
| `test_q01_dataframe` | PASSED |
| `test_q02_dataframe` | PASSED |
| `test_q03_dataframe` | PASSED |
| `test_q04_dataframe` | PASSED |
| `test_q05_dataframe` | PASSED |
| `test_q06_dataframe` | PASSED |
| `test_q07_dataframe` | PASSED |
| `test_q08_dataframe` | PASSED |
| `test_q09_dataframe` | PASSED |
| `test_q10_dataframe` | PASSED |
| `test_q11_dataframe` | PASSED |
| `test_q12_dataframe` | PASSED |
| `test_q13_dataframe` | PASSED |
| `test_q14_dataframe` | PASSED |
| `test_q15_dataframe` | PASSED |
| `test_q16_dataframe` | PASSED |
| `test_q17_dataframe` | PASSED |
| `test_q18_dataframe` | PASSED |
| `test_q19_dataframe` | PASSED |
| `test_q20_dataframe` | PASSED |
| `test_q21_dataframe` | PASSED |
| `test_q22_dataframe` | PASSED |

**DataFrame: 22/22 passed (100%)**

### SQL TPC-H Tests (`test_differential_v2.py`)

#### Dedicated SQL Test Classes

| Test | Status |
|------|--------|
| `TestTPCH_Q1_Differential::test_q1_differential` | PASSED |
| `TestTPCH_Q3_Differential::test_q3_differential` | PASSED |
| `TestTPCH_Q6_Differential::test_q6_differential` | PASSED |

#### Parameterized SQL Tests (TestTPCH_AllQueries_Differential)

| Query | Status |
|-------|--------|
| Q2 | PASSED |
| Q4 | PASSED |
| Q5 | PASSED |
| Q7 | PASSED |
| Q8 | PASSED |
| Q9 | PASSED |
| Q10 | PASSED |
| Q11 | PASSED |
| Q12 | PASSED |
| Q13 | PASSED |
| Q14 | PASSED |
| Q15 | PASSED |
| Q16 | PASSED |
| Q17 | PASSED |
| Q18 | PASSED |
| Q19 | PASSED |
| Q20 | PASSED |
| Q21 | PASSED |
| Q22 | PASSED |

**SQL TPC-H: 22/22 passed (100%)**

### Sanity Tests (`test_differential_v2.py` -- TestDifferential_Sanity)

| Test | Status |
|------|--------|
| `test_simple_select` | PASSED |

**Sanity: 1/1 passed (100%)**

### Basic Operations (`test_differential_v2.py` -- TestBasicOperations_Differential)

| Test | Status |
|------|--------|
| `test_simple_filter` | PASSED |
| `test_simple_select` | PASSED |
| `test_simple_aggregate` | PASSED |
| `test_simple_groupby` | PASSED |
| `test_simple_orderby` | PASSED |
| `test_simple_join` | PASSED |

**Basic Operations: 6/6 passed (100%)**

## Failing Tests

None. All 51 tests passed.

## Error Patterns Observed

No errors observed. All tests passed cleanly.

## Warnings

7 deprecation warnings from PySpark's logger module (`warn` method deprecated in favor of `warning`). These are PySpark internal warnings and do not affect test correctness.

## Notes

- Relaxed mode ignores schema type and nullable mismatches, using cross-type numeric comparison
- This means tests pass even when Thunderduck returns different types than Spark (e.g., `DecimalType(38,2)` vs `DecimalType(25,2)`)
- The 100% pass rate reflects value correctness, not type-level Spark compatibility
- For type-level parity assessment, see the strict mode report
