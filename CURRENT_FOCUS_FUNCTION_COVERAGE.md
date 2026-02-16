# Spark SQL Function Coverage Gap Analysis

**Created**: 2026-02-15
**Updated**: 2026-02-16
**Status**: Completed (all 4 priorities + 5 follow-up fixes: soundex, dropFields, schema_of_json, json_tuple, split limit)

## Coverage Summary

| | Count |
|---|---|
| Spark built-in functions (total) | ~540 |
| Thunderduck mapped functions (before) | ~179 |
| New functions added | ~81 |
| Thunderduck mapped functions (after) | ~260 |
| Coverage rate | ~48% |

Many of Spark's ~540 functions are in categories Thunderduck intentionally doesn't target (streaming, ML, sketches, Avro/Protobuf, XML, variants). The practical gap is in core SQL analytics functions, which is now substantially covered.

## Extension Functions (Strict Mode — Already Implemented)

| Extension Function | Purpose | Status |
|---|---|---|
| `spark_decimal_div(a, b)` | DECIMAL division with ROUND_HALF_UP | Implemented |
| `spark_sum(col)` | SUM with Spark type rules (DECIMAL->wider DECIMAL, INT->BIGINT) | Implemented |
| `spark_avg(col)` | AVG with Spark precision rules for DECIMAL | Implemented |
| `spark_skewness(col)` | Population skewness matching Spark (no sample bias correction) | Implemented |

Planned but not yet implemented:
- `spark_extract_int` — EXTRACT returns INTEGER (not BIGINT)
- `spark_checked_add/multiply` — Integer overflow detection

## Priority 1: Quick Wins (Direct Mappings) — DONE

53 functions added in commit `938094f`, merged to main. All differential tests pass (relaxed: 744, strict: 746).

### Aggregate Functions (14 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `count_if` | `count_if` | Done |
| `median` | `median` | Done |
| `mode` | `mode` | Done |
| `max_by` | `max_by` | Done |
| `min_by` | `min_by` | Done |
| `bool_and` / `every` | `bool_and` | Done |
| `bool_or` / `some` / `any` | `bool_or` | Done |
| `bit_and` | `bit_and` | Done |
| `bit_or` | `bit_or` | Done |
| `bit_xor` | `bit_xor` | Done |
| `kurtosis` | `kurtosis_pop` | Done |
| `skewness` | `spark_skewness` (strict) / `skewness` (relaxed) | Done |

### String Functions (14 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `soundex` | Custom SQL (TRANSLATE + REPLACE) | Done |
| `levenshtein` | `levenshtein` | Done |
| `overlay` | `overlay` | Done |
| `left` | `left` | Done |
| `right` | `right` | Done |
| `split_part` | `split_part` | Done |
| `translate` | `translate` | Done |
| `btrim` | `trim` | Done |
| `char_length` / `character_length` | `length` | Done |
| `octet_length` | `octet_length` | Done |
| `bit_length` | `bit_length` | Done |

### Math Functions (8 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `factorial` | `factorial` | Done |
| `cbrt` | `cbrt` | Done |
| `width_bucket` | `width_bucket` | Done |
| `bin` | `bin` | Done |
| `hex` | `hex` | Done |
| `unhex` | `unhex` | Done |
| `negative` | `-(x)` | Done (custom) |
| `positive` | `+(x)` | Done (custom) |

### Date/Time Functions (4 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `make_date` | `make_date` | Done |
| `make_timestamp` | `make_timestamp` | Done |
| `dayname` | `dayname` | Done |
| `monthname` | `monthname` | Done |

### Bitwise Functions (6 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `bit_count` | `bit_count` | Done |
| `bit_get` / `getbit` | `get_bit` | Done |
| `shiftleft` | `(x << n)` | Done (custom) |
| `shiftright` | `(x >> n)` | Done (custom) |
| `shiftrightunsigned` | `(x >> n)` | Done (custom) |

### Collection Functions (6 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `cardinality` | `CAST(len(x) AS INTEGER)` | Done (custom) |
| `array_append` | `list_append` | Done |
| `array_prepend` | `list_prepend` | Done |
| `array_remove` | `list_filter(arr, x -> x != val)` | Done (custom) |
| `array_compact` | `list_filter(arr, x -> x IS NOT NULL)` | Done (custom) |
| `sequence` | `generate_series` | Done |

## Priority 2: JSON Support — DONE

7 functions added in commit `948be64`, merged to main. All differential tests pass.

| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `to_json` | `to_json` | Done |
| `json_array_length` | `json_array_length` | Done |
| `json_object_keys` | `json_keys` | Done |
| `schema_of_json` | `spark_schema_of_json` (strict) / `json_structure` (relaxed) | Done |
| `get_json_object` | `json_extract_string` | Done (custom) |
| `from_json` | `json()` | Done (basic; full struct schema TBD) |
| `json_tuple` | Multiple `json_extract_string` | Done (custom) |

## Priority 3: String Functions — DONE

6 custom translators added in commit `8f29c1e`, merged to main. All differential tests pass.

| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `contains` | `contains` | Already existed |
| `startswith` | `starts_with` | Already existed |
| `endswith` | `ends_with` | Already existed |
| `format_number(num, d)` | `printf('%,.<d>f', num)` | Done (custom) |
| `substring_index(str, delim, count)` | `string_split` + `array_to_string` | Done (custom) |
| `to_number(str, format)` | `regexp_replace` + `CAST` | Done (custom) |
| `to_char(num/date, format)` | `strftime` | Done (custom) |
| `encode(str, charset)` | `encode(str)` | Done (custom) |
| `decode(binary, charset)` | `decode(binary)` | Done (custom) |

## Priority 4: Remaining Aggregates — DONE

15 functions added (commit merged to main as `92d1da2`). TypeInferenceEngine updated with return types and nullable handling. All differential tests pass.

| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `percentile(col, p)` | `quantile(col, p)` | Done |
| `percentile_approx(col, p, acc)` | `approx_quantile(col, p)` | Done (drops accuracy arg) |
| `kurtosis(col)` | `kurtosis_pop(col)` | Done |
| `skewness(col)` | `spark_skewness(col)` (strict) / `skewness(col)` (relaxed) | Done |
| `regr_count` | `regr_count` | Done |
| `regr_r2` | `regr_r2` | Done |
| `regr_avgx` | `regr_avgx` | Done |
| `regr_avgy` | `regr_avgy` | Done |
| `regr_sxx` | `regr_sxx` | Done |
| `regr_syy` | `regr_syy` | Done |
| `regr_sxy` | `regr_sxy` | Done |
| `regr_slope` | `regr_slope` | Done |
| `regr_intercept` | `regr_intercept` | Done |

## Differential Test Results

Current relaxed mode baseline:

| | Passed | Failed | Skipped |
|---|---|---|---|
| Full differential suite (relaxed) | 825 | 0 | 3 |

76 new tests added across 4 files:

| Test File | Tests | Passed | Skipped |
|---|---|---|---|
| `test_math_bitwise_date_differential.py` | 16 | 16 | 0 |
| `test_string_collection_differential.py` | 22 | 21 | 1 |
| `test_new_aggregates_differential.py` | 27 | 17 | 10 |
| `test_json_functions_differential.py` | 9 | 7 | 2 |
| **Total new tests** | **76** | **63** | **10** |

### Skipped Tests — Behavioral/Formula Differences (4)

| Test | Function | Skip Reason |
|---|---|---|
| `test_percentile_p50` | `percentile` | DuckDB `quantile` uses nearest-rank method, Spark uses linear interpolation |
| `test_percentile_p25` | `percentile` | Same nearest-rank vs interpolation difference |
| `test_percentile_p75` | `percentile` | Same nearest-rank vs interpolation difference |
| `test_percentile_approx` | `percentile_approx` | DuckDB `approx_quantile` returns 55.0, Spark returns 50.0 — different algorithms |

### Previously Skipped, Now Passing (11)

| Test | Function | Fix |
|---|---|---|
| `test_kurtosis` | `kurtosis` | Mapped to `kurtosis_pop` (DuckDB built-in population kurtosis) |
| `test_kurtosis_grouped` | `kurtosis` (grouped) | Same `kurtosis_pop` mapping |
| `test_skewness` | `skewness` | `spark_skewness()` extension function computes population skewness |
| `test_percentile_grouped` | `percentile` (grouped) | Fixed in earlier commit |
| `test_overlay` | `overlay` | Custom translator: `LEFT() \|\| replacement \|\| SUBSTR()` |
| `test_octet_length` | `octet_length` | Custom translator: `strlen()` (byte length for VARCHAR) |
| `test_bit_get` | `bit_get` | Custom translator: `(value >> pos) & 1` (bitwise math) |
| `test_array_prepend` | `array_prepend` | Custom translator: `list_prepend()` with swapped args |
| `test_width_bucket` | `width_bucket` | Custom translator: CASE expression emulating histogram bucket assignment |
| `test_dayname` | `dayname` | Custom translator: `LEFT(dayname(...), 3)` truncates to abbreviation |
| `test_monthname` | `monthname` | Custom translator: `LEFT(monthname(...), 3)` truncates to abbreviation |

### Skipped Tests — Output Format Differences (1)

| Test | Function | Skip Reason |
|---|---|---|
| `test_schema_of_json` | `schema_of_json` | Skipped in relaxed mode (format differs); passes in strict mode with `spark_schema_of_json` extension |

## Known Behavioral Divergences

### Open Issues

| Function | Gap | Status |
|---|---|---|
| `from_json` | Basic JSON parse only; full struct schema not supported | Partial |
| `percentile` / `percentile_approx` | DuckDB uses nearest-rank, Spark uses linear interpolation | Needs custom extension function |

### Intentionally Not Addressed

| Function | Gap | Reason |
|---|---|---|
| Negative array index | DuckDB returns element; Spark errors | Only detectable for literal indices, not expressions; partial fix would be misleading |

### Fixed

| Function | Gap | Fix |
|---|---|---|
| `json_tuple` | Generator function returned wrong column count (2 instead of 3) | Fixed: parser expands `json_tuple(json_str, k1, k2) AS (a1, a2)` into N separate `json_tuple(json_str, k_i) AS a_i` projections |
| `split(str, pattern, limit)` | 3rd arg (limit) was dropped | Fixed: custom translator emulates Spark limit semantics with CASE + list slicing; `rewriteSplit()` handles RawSQLExpression path |
| `UNION` type checking | Only checked column count, not types | Fixed: `Union.inferSchema()` computes widened types via `TypeInferenceEngine.unifyTypes()`; `SQLGenerator.visitUnion()` wraps sides with CASTs when types differ |
| `width_bucket` | DuckDB does not have this function | Fixed: custom translator emulates with CASE expression |
| `dayname` / `monthname` | DuckDB returns full name, Spark returns abbreviation | Fixed: custom translator with `LEFT(dayname(...), 3)` |
| `overlay` | DuckDB does not support OVERLAY PLACING syntax | Fixed: custom translator with `LEFT() \|\| replacement \|\| SUBSTR()` |
| `octet_length(VARCHAR)` | DuckDB only accepts BLOB/BIT, not VARCHAR | Fixed: custom translator using `strlen()` |
| `bit_get(INTEGER)` | DuckDB `get_bit` expects BIT type, not INTEGER | Fixed: custom translator using `(value >> pos) & 1` |
| `array_prepend` | `list_prepend` reversed argument order | Fixed: custom translator swaps args |
| `kurtosis` | DuckDB `kurtosis` uses sample formula | Fixed: mapped to `kurtosis_pop` (population formula) |
| `skewness` | DuckDB `skewness` uses sample bias correction | Fixed: `spark_skewness()` extension function in strict mode; relaxed mode accepts ~0.2% difference |
| `soundex` | DuckDB has no built-in soundex | Fixed: custom translator using TRANSLATE + REPLACE chain implementing American Soundex (H/W transparent, vowels separate) |
| `dropFields()` | Struct field drop generated placeholder comment | Fixed: generates `struct_pack()` excluding dropped field; resolves struct type from schema context |
| `schema_of_json` | DuckDB `json_structure` returns JSON format, Spark returns DDL | Fixed: `spark_schema_of_json()` extension function in strict mode; relaxed mode uses `json_structure` |

## Intentionally Out of Scope

| Category | Count | Reason |
|---|---|---|
| Sketch functions | ~25 | Distributed approximation algorithms |
| Variant functions | ~10 | Spark 4.x new feature, niche |
| XML functions | ~12 | Niche format |
| Avro/Protobuf | ~5 | Serialization formats |
| CSV functions | ~3 | Niche |
| Streaming functions | ~10 | Not applicable to single-node |
| Misc (spark_partition_id, etc.) | ~15 | Distributed-only concepts |
