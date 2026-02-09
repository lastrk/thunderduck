# Full Differential Test Suite Baseline

**Date**: 2026-02-09
**Command**: `cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 python3 -m pytest differential/ -v --tb=short`

## Summary

```
825 collected | 755 passed | 65 failed | 5 skipped | 5m 19s
Pass rate: 91.5%
```

## Failed Tests by Category (65 total)

| Category | Count | Root Cause |
|----------|-------|------------|
| Literal types (BYTE, SHORT, BINARY, DECIMAL, ARRAY, MAP, TIMESTAMP, STRUCT) | 14 | Unsupported literal types in expression converter |
| Set operations (union/intersect/except) | 8 | Row count mismatches — operations not working correctly |
| TPC-H DataFrame (q04, q11, q13, q16, q20, q22) | 6 | Type mismatches, unresolved functions, semi-join issues |
| Window functions (row_number, rank, sum/count OVER) | 5 | Unresolved functions, alias-before-OVER SQL bug |
| NA operations (fillna, dropna) | 5 | Type mismatches, NULL handling differences |
| Date functions (date_format, months_between) | 3 | Format pattern translation, return type |
| String functions (concat_ws, format_string, regexp_replace) | 3 | SQL generation, missing functions, regex behavior |
| Math functions (log, bin, conv) | 3 | Type mismatches, missing functions |
| Catalog operations (dropTempView, listFunctions) | 3 | Return value differences |
| Column operations (between, eqNullSafe, metadata) | 3 | Expression conversion errors |
| createOrReplaceTempView | 3 | "Relation range is not supported" |
| Type coercion (integer division, boolean cast) | 2 | Return type differences |
| Pivot | 2 | Not supported |
| TPC-H SQL (Q13, Q17) | 2 | DOUBLE vs DECIMAL type mismatches |
| Hash functions (md5, sha2) | 2 | Expression conversion errors |
| Cross join | 1 | Not supported |

## All 65 Failed Tests (detail)

### test_catalog_operations.py (3)

| Test | Error |
|------|-------|
| `TestDropTempView::test_drop_nonexistent_temp_view` | Thunderduck returns True; Spark returns False |
| `TestDropTempView::test_drop_existing_temp_view` | Thunderduck returns True; Spark returns None |
| `TestListFunctions::test_list_functions_includes_common_functions` | Function "substring" not found in Thunderduck function list |

### test_column_operations_differential.py (3)

| Test | Error |
|------|-------|
| `TestColumnAliasDifferential::test_alias_with_metadata` | INTERNAL error on metadata column alias |
| `TestColumnBetweenDifferential::test_between_with_expressions` | INTERNAL error on BETWEEN with column expressions |
| `TestColumnEqNullSafeDifferential::test_eqnullsafe_basic` | INTERNAL error on null-safe equal (`<=>`) |

### test_createorreplacetempview_differential.py (3)

| Test | Error |
|------|-------|
| `TestBasicTempView::test_replace_temp_view` | "Relation range is not supported" |
| `TestSQLOnTempViews::test_sql_on_temp_view` | "Relation range is not supported" |
| `TestTempViewFromTransformations::test_temp_view_from_filter` | "Relation range is not supported" |

### test_dataframe_advanced_differential.py (5)

| Test | Error |
|------|-------|
| `TestNAFillDifferential::test_fillna_all_columns` | Type mismatch: Spark DOUBLE vs Thunderduck BIGINT for "age" |
| `TestNADropDifferential::test_dropna_all` | Row count: Spark 2 vs Thunderduck 3 |
| `TestNADropDifferential::test_dropna_any` | Row count: Spark 3 vs Thunderduck 4 |
| `TestNADropDifferential::test_dropna_subset` | Row count: Spark 3 vs Thunderduck 4 |
| `TestNADropDifferential::test_dropna_thresh` | Row count: Spark 3 vs Thunderduck 4 |

### test_dataframe_operations_differential.py (8)

| Test | Error |
|------|-------|
| `TestUnionDifferential::test_union_basic` | Row count: Spark 6 vs Thunderduck 3 |
| `TestUnionDifferential::test_union_all` | Row count: Spark 6 vs Thunderduck 3 |
| `TestUnionByNameDifferential::test_union_by_name` | Row count: Spark 6 vs Thunderduck 3 |
| `TestUnionByNameDifferential::test_union_by_name_different_order` | Row count: Spark 6 vs Thunderduck 3 |
| `TestUnionByNameDifferential::test_union_by_name_allow_missing` | Thunderduck raised unexpected error |
| `TestIntersectDifferential::test_intersect_basic` | Row count: Spark 2 vs Thunderduck 3 |
| `TestExceptDifferential::test_except_basic` | Row count: Spark 1 vs Thunderduck 0 |
| `TestExceptDifferential::test_except_all` | Row count: Spark 1 vs Thunderduck 0 |

### test_date_functions_differential.py (3)

| Test | Error |
|------|-------|
| `TestDateFormatDifferential::test_date_format_custom` | Value: Spark "2024-01-15" vs Thunderduck "01/15/2024" |
| `TestDateFormatDifferential::test_date_format_time` | Value: Spark "14:30:00" vs Thunderduck "02:30:00 PM" |
| `TestMonthsBetweenDifferential::test_months_between` | Type: Spark DOUBLE vs Thunderduck VARCHAR |

### test_differential_v2.py (2)

| Test | Error |
|------|-------|
| `TestTPCH_AllQueries_Differential[13]` | Type: Spark DOUBLE vs Thunderduck DECIMAL(38,6) for "c_acctbal" |
| `TestTPCH_AllQueries_Differential[17]` | Type: Spark DOUBLE vs Thunderduck DECIMAL(38,4) for "avg_yearly" |

### test_groupby_differential.py (2)

| Test | Error |
|------|-------|
| `TestPivotDifferential::test_pivot_basic` | "Pivot operation is not supported" |
| `TestPivotDifferential::test_pivot_with_agg` | "Pivot operation is not supported" |

### test_join_differential.py (1)

| Test | Error |
|------|-------|
| `TestCrossJoinDifferential::test_cross_join` | INTERNAL error on crossJoin |

### test_math_functions_differential.py (3)

| Test | Error |
|------|-------|
| `TestLogDifferential::test_log_default` | Type: Spark DOUBLE vs Thunderduck FLOAT for "log_val" |
| `TestBinDifferential::test_bin` | Scalar Function "bin" does not exist |
| `TestConvDifferential::test_conv_decimal_to_hex` | Scalar Function "conv" does not exist |

### test_misc_functions_differential.py (2)

| Test | Error |
|------|-------|
| `TestMD5Differential::test_md5` | INTERNAL error converting MD5 expression |
| `TestSha2Differential::test_sha2_256` | INTERNAL error converting SHA2 expression |

### test_string_functions_differential.py (3)

| Test | Error |
|------|-------|
| `TestConcatWsDifferential::test_concat_ws_basic` | Parser Error: syntax error near ',' in concat_ws SQL |
| `TestFormatStringDifferential::test_format_string` | Scalar Function "format_string" does not exist |
| `TestRegexpReplaceDifferential::test_regexp_replace` | Value mismatch: regex not replacing all occurrences |

### test_tpch_differential.py (6)

| Test | Error |
|------|-------|
| `TestTPCHDifferential::test_q04_dataframe` | Type: Spark STRING vs Thunderduck VARCHAR for "o_orderpriority" |
| `TestTPCHDifferential::test_q11_dataframe` | Type: Spark DOUBLE vs Thunderduck DECIMAL(38,2) for "value" |
| `TestTPCHDifferential::test_q13_dataframe` | INTERNAL error: unresolved function: count |
| `TestTPCHDifferential::test_q16_dataframe` | Type: Spark STRING vs Thunderduck VARCHAR for "p_brand" |
| `TestTPCHDifferential::test_q20_dataframe` | INTERNAL error: semi join conversion issue |
| `TestTPCHDifferential::test_q22_dataframe` | INTERNAL error: CASE WHEN conversion issue |

### test_type_coercion_differential.py (2)

| Test | Error |
|------|-------|
| `TestIntegerDivisionDifferential::test_integer_division` | Type: Spark BIGINT vs Thunderduck DOUBLE |
| `TestBooleanCastDifferential::test_boolean_to_int` | Type: Spark INT vs Thunderduck BOOLEAN |

### test_type_literals_differential.py (14)

| Test | Error |
|------|-------|
| `TestIntegerLiterals_Differential::test_byte_literal` | Literal type BYTE not supported |
| `TestIntegerLiterals_Differential::test_short_literal` | Literal type SHORT not supported |
| `TestStringLiterals_Differential::test_binary_literal` | Literal type BINARY not supported |
| `TestDecimalLiterals_Differential::test_decimal_literal` | Literal type DECIMAL not supported |
| `TestTimestampLiterals_Differential::test_timestamp_literal` | Datetime becomes string instead of TIMESTAMP |
| `TestTimestampLiterals_Differential::test_timestamp_ntz_literal` | Datetime becomes string instead of TIMESTAMP_NTZ |
| `TestArrayLiterals_Differential::test_integer_array_literal` | Literal type ARRAY not supported |
| `TestArrayLiterals_Differential::test_string_array_literal` | Literal type ARRAY not supported |
| `TestArrayLiterals_Differential::test_nested_array_literal` | Literal type ARRAY not supported |
| `TestMapLiterals_Differential::test_string_map_literal` | Literal type MAP not supported |
| `TestMapLiterals_Differential::test_integer_map_literal` | Literal type MAP not supported |
| `TestStructLiterals_Differential::test_named_struct_literal` | Function "named_struct" does not exist |
| `TestStructLiterals_Differential::test_struct_pyspark_literal` | Parser error on `row(99 AS id, ...)` syntax |
| `TestComplexNestedTypes_Differential::test_map_with_array_values` | Binder error: cannot deduce template type |
| `TestComplexNestedTypes_Differential::test_deeply_nested` | Function "named_struct" does not exist |

### test_window_functions.py (5)

| Test | Error |
|------|-------|
| `TestBasicWindowFunctions::test_row_number_over_window` | Unresolved function: row_number |
| `TestBasicWindowFunctions::test_rank_over_window` | Unresolved function: rank |
| `TestAggregateWindowFunctions::test_sum_over_window` | Alias before OVER clause in generated SQL |
| `TestAggregateWindowFunctions::test_count_over_window` | Alias before OVER clause in generated SQL |

### test_tpcds_differential.py (5 unique queries, 9 test entries)

| Test | Error |
|------|-------|
| `TestTPCDS_Differential::test_query_differential[14b]` | TPC-DS query failure |
| `TestTPCDS_Differential::test_query_differential[39a]` | TPC-DS query failure |
| `TestTPCDS_Differential::test_query_differential[39b]` | TPC-DS query failure |
| `TestTPCDS_Differential::test_query_differential[64]` | TPC-DS query failure |
| `TestTPCDS_Differential::test_query_differential[77]` | TPC-DS query failure |
| `TestTPCDS_Batch2::test_batch2[14b]` | (duplicate of above in batch runner) |
| `TestTPCDS_Batch4::test_batch4[39a]` | (duplicate of above in batch runner) |
| `TestTPCDS_Batch4::test_batch4[39b]` | (duplicate of above in batch runner) |
| `TestTPCDS_Batch7::test_batch7[64]` | (duplicate of above in batch runner) |
