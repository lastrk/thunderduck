# Thunderduck Reimplementation Analysis: Go vs Rust

**Date**: 2026-02-18
**Status**: Research Complete

## Executive Summary

We analyzed the feasibility of reimplementing Thunderduck in **Go** and **Rust** to address the ~50% PySpark Spark Connect protocol overhead identified in our [performance analysis](THUNDERDUCK_PERFORMANCE_ANALYSIS.md). Both languages have mature ecosystems for every major dependency. The key finding: **Arrow IPC serialization is mandatory** per the Spark Connect protocol — it cannot be bypassed regardless of implementation language. However, the server-side data copy chain can be reduced from **4 copies (Java) to 1 copy (Rust) or 1-2 copies (Go)**, and the PySpark client overhead (~50% of total time) remains unchanged since it's in the client, not the server.

**UPDATE**: Subsequent investigation revealed the ~2x overhead was NOT due to protocol overhead but to a **double query execution bug** — Thunderduck does not implement `SqlCommandResult`, causing PySpark to discard the first execution's results and re-execute. See [THUNDERDUCK_PERFORMANCE_ANALYSIS.md](THUNDERDUCK_PERFORMANCE_ANALYSIS.md) for the full root cause analysis. After fixing that bug, the actual protocol overhead is <20ms — making a server-side rewrite unnecessary for performance reasons alone.

---

## Current Architecture: Data Copy Analysis

### Java Path (Current — 4 copies after DuckDB)

```
DuckDB C engine (native Arrow buffers)
  → [Copy 1] JNI: DuckDB C → Java Arrow (arrowExportStream via JDBC)
  → [Copy 2] ArrowStreamWriter serializes to ByteArrayOutputStream (Java heap)
  → [Copy 3] out.toByteArray() creates a new byte[]
  → [Copy 4] ByteString.copyFrom(arrowData) copies into protobuf ByteString
  → gRPC sends protobuf bytes over HTTP/2
```

Plus Java GC overhead on all transient byte arrays.

Source: `connect-server/.../StreamingResultHandler.java` lines 128-159.

### Rust Path (Proposed — 1 mandatory copy)

```
DuckDB C engine (native Arrow buffers)
  → [Zero-copy] Arrow C Data Interface: FFI_ArrowArray → Rust RecordBatch (pointer transfer)
  → [Copy 1] arrow-ipc StreamWriter serializes RecordBatch to Vec<u8> / BytesMut
  → [Potentially zero-copy] prost bytes::Bytes wraps the buffer (ref-counted)
  → gRPC/tonic sends the bytes
```

### Go Path (Proposed — 1-2 copies)

```
DuckDB C engine (native Arrow buffers)
  → [Zero-copy] Arrow C Data Interface via CGO → Go arrow.Record (pointer transfer)
  → [Copy 1] Arrow IPC serialize to []byte (mandatory per protocol)
  → [Copy 2?] Set as protobuf bytes field (gRPC-Go has buffer pooling)
  → gRPC-Go sends buffer
```

### Why Arrow IPC Cannot Be Eliminated

The Spark Connect protocol defines `ExecutePlanResponse.ArrowBatch.data` as a `bytes` field containing Arrow IPC serialized data. PySpark's client deserializes this with `pyarrow.ipc.RecordBatchStreamReader`. There is no alternative encoding, and Arrow Flight is a separate protocol not supported by PySpark's Spark Connect client. The IPC format is close to in-memory Arrow layout (column buffers + FlatBuffers metadata header), so "serialization" is mostly memcpy.

---

## Language Comparison

### 1. DuckDB Bindings

| Feature | Go (`duckdb/duckdb-go` v2.5+) | Rust (`duckdb-rs` v1.4.4) |
|---------|------|------|
| Maintainer | Official DuckDB team | Official DuckDB project |
| Arrow C Data Interface | Yes (`NewArrowFromConn`) | Yes (`query_arrow`, `stream_arrow`) |
| Zero-copy Arrow export | Yes (via CGO, pointer transfer) | Yes (via FFI, pointer transfer) |
| Bundled build | `CGO_ENABLED=1` | `bundled` feature flag |
| Concurrent connections | Not safe (single conn) | Not safe (single conn) |
| Known issues | CGO build complexity | DECIMAL type narrowing (fix in progress) |

**Verdict**: Both are production-ready with equivalent Arrow access. Rust has slightly cleaner FFI (no CGO overhead).

### 2. gRPC + Protobuf

| Feature | Go (`grpc-go` + `protobuf`) | Rust (`tonic` + `prost`) |
|---------|------|------|
| Maturity | Production (Google-maintained) | Production (widely adopted) |
| Memory usage | ~50-80 MB | ~10-30 MB |
| Latency | Competitive | Best-in-class |
| JIT warm-up | None | None |
| Server streaming | Yes | Yes (async via tokio) |
| Custom marshaller | Possible (custom codec) | Possible (`Config::bytes`) |
| Spark Connect protos | Working codegen (`spark-connect-go`) | Working codegen (`spark-connect-rs`) |

**Verdict**: Both are excellent. Rust has lower memory footprint. Neither has an existing Spark Connect **server** implementation — Thunderduck would be the first in either language.

### 3. Apache Arrow

| Feature | Go (`arrow-go` v18.5) | Rust (`arrow-rs` v58.x) |
|---------|------|------|
| Maintainer | Apache | Apache |
| IPC reader/writer | Yes | Yes (`arrow-ipc` crate) |
| C Data Interface (FFI) | Yes | Yes (`arrow::ffi`) |
| Arrow Flight | Yes | Yes (`arrow-flight` crate) |
| Memory management | Reference counting | Reference counting + ownership |

**Verdict**: Both are mature. Rust's ownership model provides stronger memory safety guarantees.

### 4. SQL Parsing (Highest Risk Component)

| Feature | Go | Rust |
|---------|------|------|
| **Best option** | ANTLR4 Go target (buggy) | `sqlparser-rs` with Databricks dialect |
| SparkSQL coverage | ~60% (needs significant porting) | ~80-90% (Databricks ≈ SparkSQL) |
| Maturity | ANTLR4 Go has memory issues, EOF bugs | `sqlparser-rs` is Apache-governed, production |
| Alternative | Hand-written recursive descent | `antlr4rust` (less maintained) |
| Risk level | **HIGH** | **MEDIUM** |

**Go ANTLR4 issues**:
- Parsing ~20MB files consumes >2GB memory (issue #4816)
- EOF parsing bugs (issue #4813)
- Complex grammars that work in Java may not work in Go
- No existing SparkSQL parser in Go

**Rust sqlparser-rs advantages**:
- Has a `DatabricksDialect` (closest to SparkSQL)
- Has `HiveDialect` (SparkSQL inherits from HiveQL)
- `LateralView` is a first-class AST node
- Extensible dialect system designed for exactly this use case

**Verdict**: Rust has a significantly better parser story. The SQL parser is the highest-risk component of any rewrite, and `sqlparser-rs` with the Databricks dialect provides 80-90% coverage out of the box. Go would require porting the ANTLR4 grammar with a known-buggy target.

---

## Performance Impact Assessment

### What a Server-Side Rewrite Can Fix

| Optimization | Java → Rust Improvement | Java → Go Improvement |
|-------------|------------------------|----------------------|
| Data copies (4 → 1) | ~3 copies eliminated | ~2-3 copies eliminated |
| GC pauses | Eliminated | Reduced (Go GC is simpler) |
| JVM warm-up | Eliminated | Eliminated |
| Memory footprint | ~10-30 MB (from ~200 MB) | ~50-80 MB (from ~200 MB) |
| Server-side overhead | <20ms → <5ms | <20ms → <10ms |

### What It Cannot Fix

| Component | Time | Fixable by rewrite? |
|-----------|------|-------------------|
| DuckDB execution | ~1,880ms | No (same engine) |
| PySpark client overhead | ~1,900ms | **No** (client-side) |
| Arrow IPC serialization | ~2ms | No (protocol requirement) |

**The ~1,900ms PySpark overhead is the dominant bottleneck, and a server rewrite does not address it.**

### Estimated End-to-End Impact

```
Current:  DuckDB (1,880ms) + Server overhead (20ms) + Client overhead (1,900ms) = 3,800ms
Rust:     DuckDB (1,880ms) + Server overhead (5ms)  + Client overhead (1,900ms) = 3,785ms
Go:       DuckDB (1,880ms) + Server overhead (10ms) + Client overhead (1,900ms) = 3,790ms
```

**Net improvement: <1% end-to-end.** The server-side overhead is already negligible.

---

## Dependency Mapping (Full Codebase Analysis)

### Thunderduck Codebase Statistics

| Metric | Count |
|--------|-------|
| Java source files | 124 |
| ANTLR4 grammar lines | ~3,000 |
| Spark function mappings | 254 |
| LogicalPlan node types | 28 (sealed) |
| Expression types | 37 |
| Protobuf message types | 500+ |
| gRPC endpoints | 10 |

### Go/Rust Equivalents for All Dependencies

| Java Dependency | Version | Go Equivalent | Rust Equivalent |
|----------------|---------|---------------|-----------------|
| DuckDB JDBC | 1.4.4.0 | `duckdb/duckdb-go` v2.5 | `duckdb-rs` v1.4.4 |
| Apache Arrow | 18.3.0 | `arrow-go` v18.5 | `arrow-rs` v58.x |
| gRPC (netty) | 1.76.0 | `grpc-go` | `tonic` v0.14 |
| Protobuf | 4.33.0 | `google.golang.org/protobuf` | `prost` + `prost-build` |
| ANTLR4 | 4.13.2 | `antlr4-go/antlr` (buggy) | `sqlparser-rs` v0.54+ |
| SLF4J/Logback | 2.0.9 | `log/slog` (stdlib) | `tracing` + `tracing-subscriber` |
| JUnit | 5.x | `testing` (stdlib) | `cargo test` (built-in) |
| Maven | - | Go modules | Cargo |

---

## Effort Estimates

### Rust Implementation

| Component | Effort | Complexity |
|-----------|--------|------------|
| Proto codegen + gRPC server scaffold | 1-2 weeks | Low |
| Plan deserializer (protobuf → LogicalPlan) | 3-4 weeks | Medium |
| SQL generator (LogicalPlan → DuckDB SQL) | 2-3 weeks | Medium |
| SparkSQL parser (extend sqlparser-rs) | 4-6 weeks | High |
| DuckDB execution + Arrow streaming | 1-2 weeks | Low |
| Session management | 1 week | Low |
| Function registry (254 mappings) | 2-3 weeks | Medium |
| Type inference engine | 2-3 weeks | Medium |
| Integration testing + PySpark compatibility | 3-4 weeks | Medium |
| **Total** | **19-28 weeks** | Single developer |

### Go Implementation

| Component | Effort | Complexity |
|-----------|--------|------------|
| Proto codegen + gRPC server scaffold | 1 week | Low |
| Plan deserializer (protobuf → LogicalPlan) | 3-4 weeks | Medium |
| SQL generator (LogicalPlan → DuckDB SQL) | 2-3 weeks | Medium |
| SparkSQL parser (ANTLR4 Go or hand-written) | **6-10 weeks** | **Very High** |
| DuckDB execution + Arrow streaming | 1-2 weeks | Low |
| Session management | 1 week | Low |
| Function registry (254 mappings) | 2-3 weeks | Medium |
| Type inference engine | 2-3 weeks | Medium |
| Integration testing + PySpark compatibility | 3-4 weeks | Medium |
| **Total** | **21-32 weeks** | Single developer |

---

## Risk Assessment

| Risk | Go | Rust |
|------|-----|------|
| SQL parser coverage | **HIGH** — ANTLR4 Go target is buggy | **MEDIUM** — sqlparser-rs Databricks dialect |
| Feature parity regression | HIGH — same for both | HIGH — same for both |
| Build complexity | MEDIUM — CGO cross-compilation | LOW — Cargo handles everything |
| Team expertise | Variable | Variable |
| PySpark compatibility | MEDIUM — same for both | MEDIUM — same for both |
| No existing reference server | MEDIUM — same for both | MEDIUM — same for both |

---

## Recommendations

### 1. Do NOT Rewrite for Performance Alone

The server-side rewrite would yield <1% end-to-end improvement. The dominant bottleneck (~1,900ms) is in the PySpark Spark Connect client, which a server rewrite cannot address.

### 2. If Rewriting, Choose Rust

Rust has clear advantages:
- **Parser**: `sqlparser-rs` with Databricks dialect provides 80-90% SparkSQL coverage out of the box
- **Memory safety**: Rust's ownership model eliminates an entire class of bugs (memory leaks, use-after-free)
- **Data copies**: 1 mandatory copy vs Go's 1-2
- **Memory footprint**: ~10-30 MB vs Go's ~50-80 MB
- **Sealed types**: Rust `enum` is a perfect match for Java sealed classes (exhaustive `match`)

### 3. Highest-Impact Optimizations (No Rewrite Needed)

Before considering a rewrite, investigate these Java-only optimizations:

| Optimization | Effort | Impact |
|-------------|--------|--------|
| Use `UnsafeByteOperations.unsafeWrap()` instead of `ByteString.copyFrom()` | 1 line | Eliminates 1 copy |
| Use `RecyclableByteArrayOutputStream` | 5 lines | Reduces GC pressure |
| Profile PySpark client overhead root cause | 1-2 days | Identifies true bottleneck |
| Build native Python gRPC client | 2-4 weeks | **Eliminates ~1,900ms overhead** |

### 4. The Real Win: Native Python Client

Build a lightweight Python client that speaks raw Spark Connect gRPC (using `grpcio` + `pyarrow`), bypassing PySpark's heavy client library. This would:
- Eliminate the reattachable execution protocol overhead
- Eliminate PySpark's internal state management
- Go directly from gRPC bytes → `pyarrow.RecordBatchStreamReader` → Pandas
- Estimated improvement: **3-5x faster for short-to-medium queries**

This is a 2-4 week project that delivers more impact than a 6-month server rewrite.

---

## Architecture Sketch: Rust Thunderduck

```
PySpark Client
     |
     | (gRPC / Spark Connect protocol)
     v
+------------------------------------------+
| Rust Thunderduck Server                  |
|                                          |
|  tonic gRPC server                       |
|    |                                     |
|    v                                     |
|  SparkConnectService (trait impl)        |
|    |                                     |
|    +→ Plan deserializer (prost)          |
|    |     |                               |
|    |     v                               |
|    |   LogicalPlan (Rust enum)           |
|    |     |                               |
|    +→ SQL parser (sqlparser-rs)          |
|    |     |                               |
|    |     v                               |
|    |   LogicalPlan (same type)           |
|    |                                     |
|    v                                     |
|  SQL Generator                           |
|    |                                     |
|    v                                     |
|  DuckDB SQL string                       |
|    |                                     |
|    v                                     |
|  duckdb-rs (query_arrow / stream_arrow)  |
|    |                                     |
|    | [zero-copy via C Data Interface]    |
|    v                                     |
|  arrow-rs RecordBatch                    |
|    |                                     |
|    | [1 copy: IPC serialization]         |
|    v                                     |
|  Arrow IPC bytes → prost bytes::Bytes    |
|    |                                     |
|    | [zero-copy: Bytes is ref-counted]   |
|    v                                     |
|  tonic gRPC streaming response           |
+------------------------------------------+
```

---

## Appendix: Spark Connect Wire Format

```protobuf
message ExecutePlanResponse {
  oneof response_type {
    ArrowBatch arrow_batch = 2;
  }
  message ArrowBatch {
    int64 row_count = 1;
    bytes data = 2;  // Arrow IPC serialized bytes — MANDATORY
  }
}
```

PySpark reads `data` with `pyarrow.ipc.RecordBatchStreamReader`. No alternative encoding is supported. Arrow Flight is a separate protocol not compatible with PySpark's Spark Connect client.

Spark 4.1 added Arrow batch chunking (SPARK-53525) for batches exceeding the 128MB gRPC message limit. Any reimplementation must handle this.
