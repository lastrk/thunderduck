# M72: DuckDB Extension Integration and Spark Compatibility Modes

**Date:** 2026-02-07 to 2026-02-08
**Status:** Complete

## Summary

This milestone integrated a custom DuckDB extension (`thdck_spark_funcs`) into the Maven build system and introduced strict/relaxed compatibility modes. The extension provides Spark-compatible aggregate functions (`spark_sum`, `spark_avg`, `spark_decimal_div`) that match Spark's exact return types and rounding behavior, enabling 100% type parity in strict mode while maintaining high-performance vanilla DuckDB execution in relaxed mode.

## Work Completed

### 1. DuckDB Extension as Submodule

**Commit:** `5625455`

Converted the standalone `duckdb_ext` directory into a Git submodule at `thunderduck-duckdb-extension`, pulling in the full DuckDB source tree. This enables building the extension against the exact DuckDB version used by JDBC.

**Architecture:**
- Submodule: `thunderduck-duckdb-extension/duckdb` (DuckDB v1.1.3)
- Extension config: `thunderduck-duckdb-extension/extension_config.cmake`
- Extension source: `thunderduck-duckdb-extension/src/` (C++ aggregate functions)

### 2. Maven-Integrated Extension Build

**Commits:** `e4959a5`, `54465b3`, `758b2eb`

Added Maven profile `build-extension` that invokes CMake directly from `pom.xml`:

```bash
mvn clean package -DskipTests -Pbuild-extension
```

**Key decisions:**
- CMake invoked directly (not via `make`) for better control
- Build cache at `thunderduck-duckdb-extension/build/release/` persists across `mvn clean`
- Ninja job limit (`-j4`) to avoid OOM on 15GB machines
- First build: ~5-10 minutes; incremental: seconds

**Files Modified:**
| File | Change |
|------|--------|
| `pom.xml` | Add `build-extension` profile with cmake execution |
| `thunderduck-duckdb-extension/extension_config.cmake` | Extension metadata and versioning |

### 3. Spark Compatibility Mode System

**Commits:** `1b813e3`, `a34be39`

Introduced `SparkCompatMode` enum with three modes:

| Mode | Behavior | Performance | Type Parity |
|------|----------|-------------|-------------|
| **RELAXED** | Vanilla DuckDB (no extension) | Highest | ~85% |
| **STRICT** | DuckDB + extension functions | High | ~100% |
| **AUTO** | Detect from env/config | Variable | Variable |

**Detection logic:**
1. Check `THUNDERDUCK_COMPAT_MODE` env var (strict/relaxed/auto)
2. If auto: detect extension availability at runtime
3. Fall back to relaxed if extension not loaded

**Extension functions:**
- `spark_sum(DECIMAL)` → Returns DECIMAL(38,scale) matching Spark
- `spark_avg(DECIMAL)` → Returns DECIMAL(38,scale) with Spark rounding
- `spark_decimal_div(DECIMAL, DECIMAL)` → Spark-compatible division with scale adjustment

### 4. Mode-Aware Type Architecture

**Commit:** `896bd74`

Eliminated the generic "schema correction layer" in favor of mode-aware type generation at SQL generation time:

- **Relaxed mode**: SQLGenerator emits vanilla DuckDB SQL (SUM, AVG, `/`)
- **Strict mode**: SQLGenerator emits extension functions (`spark_sum`, `spark_avg`, `spark_decimal_div`)

This pushed type correctness upstream from result processing to SQL generation, enabling a zero-copy result path.

**Files Modified:**
| File | Change |
|------|--------|
| `core/.../runtime/SparkCompatMode.java` | **NEW** - Mode detection and enum |
| `core/.../runtime/DuckDBRuntime.java` | Extension loading logic |
| `core/.../generator/SQLGenerator.java` | Mode-aware function emission |
| `core/.../expression/BinaryExpression.java` | Division operator dispatch |

### 5. Test Infrastructure Enhancements

**Commits:** `89aaa1a`, `bfabeb8`

- **Configurable ports**: `THUNDERDUCK_PORT` and `SPARK_PORT` env vars enable parallel test runs
- **Health checks**: Added server health checks between test classes
- **Session cleanup**: Fixed session leak in test orchestrator
- **Deduplication**: Eliminated redundant TPC-H tests

### 6. Semi/Anti Join Syntax Fix

**Commit:** `529a914`

DuckDB uses `SEMI JOIN` / `ANTI JOIN` (not `LEFT SEMI JOIN` / `LEFT ANTI JOIN`). Updated SQLGenerator to emit correct syntax.

**Bug:** Previous syntax caused DuckDB parser errors on TPC-H Q21.

### 7. Composite Aggregate Expressions

**Commit:** `c578f1c`

Extended `AggregateExpression` to support composite expressions like `SUM(a) / SUM(b)`:

- **Simple aggregate**: `sum(revenue)` → single FunctionCall
- **Composite aggregate**: `SUM(a) / SUM(b)` → BinaryExpression wrapping two FunctionCalls

**Impact:** Enables complex aggregate expressions in HAVING and SELECT clauses without string manipulation.

## Key Insights

1. **Extension build complexity**: DuckDB extensions MUST statically link DuckDB (`EXTENSION_STATIC_BUILD=1`) due to `RTLD_LOCAL` in JDBC native library loading. Standalone builds fail with unresolved RTTI symbols.

2. **Memory-hungry builds**: DuckDB unity build units consume 2-3GB RAM each. Ninja defaults to `nproc` jobs → OOM on 10-core/15GB machines. Must limit to `-j4`.

3. **Type parity via extension functions**: Using DuckDB extension functions for aggregates eliminates 90% of post-hoc type fixups. The remaining 10% are top-level CAST wrappers for edge cases.

4. **Mode detection trade-offs**: AUTO mode is convenient but adds runtime overhead. STRICT/RELAXED explicit modes are faster and more predictable for production.

5. **Composite aggregates enable DataFrame parity**: Without composite aggregate support, expressions like `SUM(a) / SUM(b)` required string concatenation or multiple queries. Now handled as AST nodes.

## Performance Impact

- **Relaxed mode**: No overhead (vanilla DuckDB)
- **Strict mode**: Extension function overhead is negligible (<1% on TPC-H queries)
- **Result processing**: Eliminated Arrow batch copying for type corrections (moved to SQL generation)

## Community Contributions

None (internal development).

## Commits (chronological)

- `a3f97df` - Rename DuckDB extension from spark_decimal_div to thdck_spark_funcs
- `e4959a5` - Add Maven-integrated DuckDB extension build with runtime loading
- `54465b3` - Fix Maven extension profile paths for multi-module build
- `758b2eb` - Update Maven build-extension profile to use cmake directly
- `5625455` - Add DuckDB extension as submodule, rename duckdb_ext to thunderduck-duckdb-extension
- `0c72870` - Fix spark_decimal_div bypass in qualifyCondition and override / operator for DECIMAL
- `1b813e3` - Add Spark compatibility modes with extension aggregate functions
- `896bd74` - Eliminate schema correction layer in favor of mode-aware type architecture
- `be07e35` - Fix like() SQL syntax, join alias scoping, and IS_LOCAL analyze handler
- `23e93ed` - Fix nested semi/anti join alias scoping for Q21 DataFrame queries
- `c578f1c` - Support composite aggregate expressions and fix FunctionCall argument rendering
- `529a914` - Use DuckDB-native SEMI JOIN / ANTI JOIN syntax instead of LEFT prefix
- `bfabeb8` - Fix test infrastructure: health checks, session cleanup, deduplication
- `89aaa1a` - Make server ports fully configurable via env vars for parallel test runs
