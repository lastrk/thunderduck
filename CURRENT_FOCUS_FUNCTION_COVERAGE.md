# Spark SQL Function Coverage Gap Analysis

**Created**: 2026-02-15
**Updated**: 2026-02-15
**Status**: Completed (all 4 priorities implemented)

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
| `kurtosis` | `kurtosis` | Done |
| `skewness` | `skewness` | Done |

### String Functions (14 entries)
| Spark Function | DuckDB Equivalent | Status |
|---|---|---|
| `soundex` | `soundex` | Done |
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
| `schema_of_json` | `json_structure` | Done |
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
| `kurtosis(col)` | `kurtosis(col)` | Done |
| `skewness(col)` | `skewness(col)` | Done |
| `regr_count` | `regr_count` | Done |
| `regr_r2` | `regr_r2` | Done |
| `regr_avgx` | `regr_avgx` | Done |
| `regr_avgy` | `regr_avgy` | Done |
| `regr_sxx` | `regr_sxx` | Done |
| `regr_syy` | `regr_syy` | Done |
| `regr_sxy` | `regr_sxy` | Done |
| `regr_slope` | `regr_slope` | Done |
| `regr_intercept` | `regr_intercept` | Done |

## Test Results After All Merges

| Mode | Passed | Failed | Skipped |
|---|---|---|---|
| Relaxed | 744 | 0 | 2 |
| Strict | 746 | 0 | 0 |

Unit tests: 1840/1865 passed (25 pre-existing failures unrelated to function coverage work).

## Known Behavioral Divergences

| Function | Gap | Status |
|---|---|---|
| `split(str, pattern, limit)` | 3rd arg (limit) dropped | Open |
| Negative array index | DuckDB returns element; Spark errors | Planned fix |
| `UNION` type checking | Only checks column count, not types | TODO in code |
| `dropFields()` on structs | Generates placeholder comment | Unsupported |
| `from_json` | Basic JSON parse only; full struct schema not supported | Partial |

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
