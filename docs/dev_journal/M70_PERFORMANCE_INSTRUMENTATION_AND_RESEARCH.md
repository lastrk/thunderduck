# M70: Performance Instrumentation and Research Documentation

**Date:** 2025-12-30
**Status:** Complete

## Summary

Added comprehensive query timing instrumentation to understand performance characteristics, created an interactive playground for benchmarking, and restructured research documentation for UDF support and SparkSQL parsing.

## Work Completed

### 1. Query Timing Instrumentation

Added detailed timing breakdown for query execution phases to enable data-driven optimization decisions.

**New Class:** `QueryTimingStats.java`
- Tracks 5 execution phases: `plan_convert`, `sql_generate`, `duckdb_execute`, `result_stream`, `total`
- Also tracks batch count and row count for context
- Outputs log-friendly format: `plan_convert=0.7ms, sql_generate=0.6ms, ...`

**Instrumented:** `SparkConnectServiceImpl.java`
- Timing around Protobuf → LogicalPlan conversion
- Timing around LogicalPlan → SQL generation
- Timing around DuckDB query execution (to first batch)
- Timing around Arrow batch streaming to gRPC

**Sample Output:**
```
[op-12345] Query timing: plan_convert=15.2ms, sql_generate=0.1ms, duckdb_execute=1613.6ms, result_stream=1.5ms, total=1630.4ms (1 batches, 7 rows)
```

### 2. Benchmark Results (TPC-H SF20.0)

Ran timing analysis on ~6.5GB TPC-H dataset:

| Query | DuckDB Exec | Plan Convert | SQL Gen | Result Stream | Overhead % |
|-------|-------------|--------------|---------|---------------|------------|
| Q1 (aggregation) | 1667.7ms | 15.9ms | 0.1ms | 1.6ms | 1.1% |
| Q2 (2-way join) | 357.5ms | 6.5ms | 0.1ms | 1.2ms | 2.2% |
| Q3 (3-way join) | 1361.6ms | 19.6ms | 0.1ms | 1.0ms | 1.5% |
| Q4 (filter+group) | 13.0ms | 1.4ms | 0.1ms | 0.9ms | 15.6% |

**Key Findings:**
- For large queries: Overhead is only 1-2%
- For small queries (13ms): Overhead is ~15%
- DuckDB execution dominates total time (96-99% for large queries)
- SQL generation is negligible (~0.1ms)
- Result streaming is very fast (~1ms)

### 3. ArrowStreamingExecutor Allocation Fix

**Commit:** `16724cf`

Fixed per-query allocation overhead by caching `ArrowStreamingExecutor` per session instead of creating new instances for each query. This reduced overhead by 2-5ms per query.

### 4. Interactive Playground

**Commit:** `b215b83`

Added Marimo notebook-based playground for interactive benchmarking:
- TPC-H data generation at configurable scale factors
- Side-by-side comparison queries for Thunderduck vs Spark
- Located at `playground/thunderduck_playground.py`

### 5. Research Documentation Updates

#### SparkSQL Parser Research
**File:** `docs/architect/SPARKSQL_PARSER_RESEARCH.md`

New research document evaluating parser implementation options:
- ANTLR4 with optimizations (recommended for initial implementation)
- JavaCC, PEG (Parboiled2) alternatives
- Two-stage hybrid approach analysis

#### UDF Support Research Restructure
**File:** `docs/architect/UDF_SUPPORT_RESEARCH.md`

Major restructure to focus on Arrow-optimized Python UDFs:
- Primary focus: Spark Connect Client API for Python UDFs
- Elevated Java FFM (Panama) as primary DuckDB integration approach
- GraalPy for Python execution (runs on standard JVM)
- Moved Java/Scala UDFs, Arrow Flight, JNI to "Other Ways" section
- Reduced from 945 to 567 lines while preserving technical details

## Files Modified/Added

| File | Change |
|------|--------|
| `connect-server/.../service/QueryTimingStats.java` | **NEW** - Timing collector |
| `connect-server/.../service/SparkConnectServiceImpl.java` | Added timing instrumentation |
| `connect-server/src/main/resources/logback.xml` | Added timing logger config |
| `docs/architect/SPARKSQL_PARSER_RESEARCH.md` | **NEW** - Parser research |
| `docs/architect/UDF_SUPPORT_RESEARCH.md` | Restructured for Python UDFs |
| `docs/architect/DUCKDB_QUERY_PLAN_ANALYSIS.md` | **NEW** - Query plan analysis |
| `playground/thunderduck_playground.py` | Updated with benchmarking |
| `.gitignore` | Added playground/data/, thunderduck_sessions/ |

## Key Insights

1. **Performance parity with Spark explained**: Both Thunderduck and Spark share identical Spark Connect protocol overhead (gRPC + Arrow IPC). Thunderduck's plan conversion adds only 1-2% overhead for real workloads.

2. **Architecture validation**: Confirmed that Thunderduck already uses DuckDB's native `arrowExportStream()` for zero-copy Arrow streaming - no architectural changes needed.

3. **Java FFM (Panama) for UDFs**: Identified as the optimal approach for Arrow-optimized Python UDF integration due to:
   - Arrow zero-copy via MemorySegment
   - Pure Java implementation (no native compilation)
   - 2-5x faster than JNI

## Commits

- `16724cf` - Fix per-query ArrowStreamingExecutor allocation overhead
- `ea9c77e` - Improve playground TPC-H generation and document date_sub gap
- `b215b83` - Add interactive playground with Marimo notebook
- `bd43f76` - Add query timing instrumentation for performance analysis
- `1da74ec` - Add SparkSQL parser research documentation
- `341f814` - Restructure UDF research to focus on Arrow-optimized Python UDFs
