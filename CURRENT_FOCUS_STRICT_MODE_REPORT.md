# Strict Mode Differential Test Report

**Date**: 2026-02-08
**Mode**: `THUNDERDUCK_COMPAT_MODE=strict`
**Extension loaded**: Yes (thdck_spark_funcs DuckDB extension)
**Test files**: `test_tpch_differential.py`, `test_differential_v2.py`
**Test runner**: pytest with `--tb=short`, `COLLECT_TIMEOUT=30`, `CONTINUE_ON_ERROR=true`

## Summary

| Metric | Count | Percentage |
|--------|-------|-----------|
| **Total** | 51 | 100% |
| **PASSED** | 26 | 51.0% |
| **FAILED** | 25 | 49.0% |
| **ERRORED** | 0 | 0.0% |
| **SKIPPED** | 0 | 0.0% |

**Result: 26/51 PASSED, 25/51 FAILED in strict mode.**

Total execution time: 22.45 seconds.

## Breakdown by Test Class

### DataFrame Tests (`test_tpch_differential.py` -- TestTPCHDifferential)

| Test | Status | Failure Reason |
|------|--------|----------------|
| `test_q01_dataframe` | FAILED | Schema: `sum_qty` DecimalType(38,4) vs (25,4); `sum_base_price` DecimalType(38,4) vs (25,4); `sum_disc_price`/`sum_charge` DecimalType(38,4) vs (38,6)/(38,8); `avg_qty`/`avg_price`/`avg_disc` DecimalType(38,8) vs DoubleType |
| `test_q02_dataframe` | PASSED | |
| `test_q03_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q04_dataframe` | PASSED | |
| `test_q05_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q06_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q07_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q08_dataframe` | FAILED | Schema: `mkt_share` DecimalType(38,12) vs DoubleType |
| `test_q09_dataframe` | FAILED | Schema: `sum_profit` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q10_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q11_dataframe` | PASSED | |
| `test_q12_dataframe` | PASSED | |
| `test_q13_dataframe` | PASSED | |
| `test_q14_dataframe` | FAILED | Schema: `promo_revenue` DecimalType(38,7) vs DoubleType |
| `test_q15_dataframe` | FAILED | Schema: `total_revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q16_dataframe` | PASSED | |
| `test_q17_dataframe` | FAILED | Schema: `avg_yearly` DecimalType(30,6) vs DoubleType |
| `test_q18_dataframe` | PASSED | |
| `test_q19_dataframe` | FAILED | Schema: `revenue` DecimalType(28,4) vs DecimalType(38,4) |
| `test_q20_dataframe` | PASSED | |
| `test_q21_dataframe` | PASSED | |
| `test_q22_dataframe` | PASSED | |

**DataFrame: 12/22 passed (54.5%)**

### SQL TPC-H Tests (`test_differential_v2.py`)

#### Dedicated SQL Test Classes

| Test | Status | Failure Reason |
|------|--------|----------------|
| `TestTPCH_Q1_Differential::test_q1_differential` | FAILED | Schema: `sum_qty` DecimalType(38,2) vs (25,2); `sum_base_price` DecimalType(38,2) vs (25,2); `avg_qty`/`avg_price`/`avg_disc` DoubleType vs DecimalType(19,6); `count_order` nullable mismatch |
| `TestTPCH_Q3_Differential::test_q3_differential` | PASSED | |
| `TestTPCH_Q6_Differential::test_q6_differential` | PASSED | |

#### Parameterized SQL Tests (TestTPCH_AllQueries_Differential)

| Query | Status | Failure Reason |
|-------|--------|----------------|
| Q2 | PASSED | |
| Q4 | FAILED | `order_count` nullable mismatch (Reference=False, Test=True) |
| Q5 | PASSED | |
| Q7 | FAILED | `l_year` IntegerType vs LongType |
| Q8 | FAILED | `o_year` IntegerType vs LongType |
| Q9 | FAILED | `o_year` IntegerType vs LongType |
| Q10 | PASSED | |
| Q11 | PASSED | |
| Q12 | FAILED | `high_line_count`/`low_line_count` DecimalType(38,0) vs LongType |
| Q13 | FAILED | `c_count`/`custdist` nullable mismatch (Reference=False, Test=True) |
| Q14 | PASSED | |
| Q15 | PASSED | |
| Q16 | FAILED | `supplier_cnt` nullable mismatch (Reference=False, Test=True) |
| Q17 | FAILED | `avg_yearly` DecimalType(38,6) vs DecimalType(30,6) |
| Q18 | FAILED | `sum(l_quantity)` DecimalType(38,2) vs DecimalType(25,2) |
| Q19 | PASSED | |
| Q20 | PASSED | |
| Q21 | FAILED | `numwait` nullable mismatch (Reference=False, Test=True) |
| Q22 | FAILED | `numcust` nullable mismatch; `totacctbal` DecimalType(38,2) vs DecimalType(25,2) |

**SQL TPC-H: 10/22 passed (45.5%)**

### Sanity Tests (`test_differential_v2.py` -- TestDifferential_Sanity)

| Test | Status | Failure Reason |
|------|--------|----------------|
| `test_simple_select` | FAILED | `cnt` nullable mismatch (Reference=False, Test=True) |

**Sanity: 0/1 passed (0%)**

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

## Complete List of Failing Tests (25)

### Category 1: Decimal Precision/Scale Mismatches (14 tests)

These tests fail because Thunderduck produces a different decimal precision or scale than Spark. The values are correct, but the type metadata differs.

| # | Test | Column(s) | Thunderduck Type | Spark Type |
|---|------|-----------|-----------------|------------|
| 1 | `test_q01_dataframe` | `sum_qty`, `sum_base_price`, etc. | DecimalType(38,4), DecimalType(38,8) | DecimalType(25,4), DoubleType |
| 2 | `test_q03_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 3 | `test_q05_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 4 | `test_q06_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 5 | `test_q07_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 6 | `test_q08_dataframe` | `mkt_share` | DecimalType(38,12) | DoubleType |
| 7 | `test_q09_dataframe` | `sum_profit` | DecimalType(28,4) | DecimalType(38,4) |
| 8 | `test_q10_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 9 | `test_q14_dataframe` | `promo_revenue` | DecimalType(38,7) | DoubleType |
| 10 | `test_q15_dataframe` | `total_revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 11 | `test_q17_dataframe` | `avg_yearly` | DecimalType(30,6) | DoubleType |
| 12 | `test_q19_dataframe` | `revenue` | DecimalType(28,4) | DecimalType(38,4) |
| 13 | `test_q1_differential` (SQL) | `sum_qty`, `avg_qty`, etc. | DecimalType(38,2), DoubleType | DecimalType(25,2), DecimalType(19,6) |
| 14 | `test_query_differential[17]` (SQL) | `avg_yearly` | DecimalType(38,6) | DecimalType(30,6) |

### Category 2: Nullable Mismatches (7 tests)

These tests fail because Thunderduck marks columns as nullable when Spark marks them as non-nullable. COUNT(*) and COUNT(DISTINCT) are the primary offenders.

| # | Test | Column(s) | Issue |
|---|------|-----------|-------|
| 1 | `test_simple_select` (Sanity) | `cnt` | nullable should be False |
| 2 | `test_query_differential[4]` | `order_count` | nullable should be False |
| 3 | `test_query_differential[13]` | `c_count`, `custdist` | nullable should be False |
| 4 | `test_query_differential[16]` | `supplier_cnt` | nullable should be False |
| 5 | `test_query_differential[18]` | `sum(l_quantity)` | DecimalType(38,2) vs (25,2) + type mismatch |
| 6 | `test_query_differential[21]` | `numwait` | nullable should be False |
| 7 | `test_query_differential[22]` | `numcust` nullable + `totacctbal` type | nullable + DecimalType(38,2) vs (25,2) |

### Category 3: Integer/Long Type Mismatches (3 tests)

These tests fail because `EXTRACT(YEAR FROM ...)` returns LongType in Thunderduck but IntegerType in Spark.

| # | Test | Column | Thunderduck | Spark |
|---|------|--------|------------|-------|
| 1 | `test_query_differential[7]` | `l_year` | LongType | IntegerType |
| 2 | `test_query_differential[8]` | `o_year` | LongType | IntegerType |
| 3 | `test_query_differential[9]` | `o_year` | LongType | IntegerType |

### Category 4: Aggregation Return Type Mismatches (1 test)

| # | Test | Column(s) | Thunderduck | Spark |
|---|------|-----------|------------|-------|
| 1 | `test_query_differential[12]` | `high_line_count`, `low_line_count` | DecimalType(38,0) | LongType |

## Error Pattern Analysis

All 25 failures are **schema mismatches** -- no value mismatches, no gRPC errors, no timeouts. The data values produced are correct in every case. The failures fall into four distinct patterns:

### Pattern 1: Decimal Precision Mismatch (14 failures)
Thunderduck uses DuckDB's native decimal precision rules. For example, `SUM` of a `DECIMAL(15,2)` column yields `DECIMAL(38,2)` in DuckDB but `DECIMAL(25,2)` in Spark. Similarly, multiplication/division produce different precision/scale. The DataFrame API (which constructs expressions programmatically) hits this more often because the type inference flows through more intermediate expressions.

### Pattern 2: Nullable Flag (7 failures)
`COUNT(*)`, `COUNT(DISTINCT ...)`, and similar aggregations are inherently non-nullable (they always produce a value, never NULL). Spark correctly marks these as `nullable=False`. Thunderduck marks all columns as `nullable=True`. This is a metadata-only issue.

### Pattern 3: EXTRACT Returns Long Instead of Int (3 failures)
DuckDB's `EXTRACT(YEAR FROM date)` returns BIGINT (LongType). Spark returns INTEGER (IntegerType). This affects TPC-H Q7, Q8, Q9 which extract year from date columns.

### Pattern 4: Conditional SUM Returns Decimal Instead of Long (1 failure)
TPC-H Q12 uses `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` which Spark returns as LongType but DuckDB returns as DecimalType(38,0).

## Comparison: Relaxed vs Strict Mode

### Overall

| Mode | Passed | Failed | Pass Rate |
|------|--------|--------|-----------|
| Relaxed | 51 | 0 | 100.0% |
| Strict | 26 | 25 | 51.0% |
| **Delta** | **-25** | **+25** | **-49.0pp** |

### Tests that PASS in relaxed but FAIL in strict (25 tests)

All 25 strict-mode failures pass in relaxed mode. This means:
- All queries produce **correct values** (relaxed mode validates values)
- The **type metadata** (precision, scale, nullable) does not match Spark exactly

### Tests that PASS in strict but FAIL in relaxed

None. There are no tests that only pass in strict mode.

### By Test Class

| Test Class | Relaxed | Strict | Delta |
|------------|---------|--------|-------|
| DataFrame Q1-Q22 | 22/22 (100%) | 12/22 (54.5%) | -10 |
| SQL Q1 (dedicated) | 1/1 | 0/1 | -1 |
| SQL Q3 (dedicated) | 1/1 | 1/1 | 0 |
| SQL Q6 (dedicated) | 1/1 | 1/1 | 0 |
| SQL Q2-Q22 (parameterized) | 19/19 | 7/19 | -12 |
| Sanity | 1/1 | 0/1 | -1 |
| Basic Operations | 6/6 | 6/6 | 0 |

### Extension Loading

The DuckDB extension (`thdck_spark_funcs`) loaded successfully in strict mode. The extension provides `spark_decimal_div` for exact Spark decimal division semantics. However, the extension does not address:
- Decimal precision/scale for SUM, AVG, and arithmetic expressions
- Nullable metadata for aggregation columns
- EXTRACT return types (BIGINT vs INTEGER)
- Conditional SUM return types

## Development Priorities for Strict Mode

### Priority 1: Nullable Metadata (7 tests, low complexity)
Mark aggregation results (`COUNT`, `COUNT DISTINCT`) as non-nullable. This is purely metadata -- no value changes needed.

### Priority 2: Decimal Precision/Scale Rules (14 tests, high complexity)
Implement Spark's decimal precision/scale propagation rules:
- `SUM(DECIMAL(p,s))` -> `DECIMAL(p+10, s)` in Spark (not `DECIMAL(38,s)`)
- `AVG(DECIMAL(p,s))` -> Spark returns `DECIMAL(p+4, s+4)` (not `DOUBLE`)
- Multiplication/division precision rules differ between Spark and DuckDB

### Priority 3: EXTRACT Return Type (3 tests, low complexity)
Cast `EXTRACT(YEAR FROM ...)` results to INTEGER to match Spark's behavior.

### Priority 4: Conditional SUM Type (1 test, medium complexity)
Ensure `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` returns LongType (not DecimalType).
