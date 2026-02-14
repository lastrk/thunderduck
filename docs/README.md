# Thunderduck Documentation

This directory contains documentation for the Thunderduck project.

---

## Core Documentation

### Architecture & Design
- **[PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)** - Multi-architecture support (x86_64, ARM64)
- **[OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)** - Why we use DuckDB's optimizer
- **[SUPPORTED_OPERATIONS.md](SUPPORTED_OPERATIONS.md)** - Quick reference of supported Spark operations
- **[SPARK_CONNECT_GAP_ANALYSIS.md](SPARK_CONNECT_GAP_ANALYSIS.md)** - Comprehensive Spark Connect protocol coverage analysis
- **[SQL_PARSER_RESEARCH.md](SQL_PARSER_RESEARCH.md)** - Research on SQL parser options for SparkSQL support
- **[TPC_H_BENCHMARK.md](TPC_H_BENCHMARK.md)** - TPC-H data generation and benchmark guide

### Architecture Documents (architect/)
- **[SPARK_COMPAT_EXTENSION.md](architect/SPARK_COMPAT_EXTENSION.md)** - Spark compatibility extension (strict/relaxed modes)
- **[DIFFERENTIAL_TESTING_ARCHITECTURE.md](architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)** - Differential testing framework
- **[SESSION_MANAGEMENT_ARCHITECTURE.md](architect/SESSION_MANAGEMENT_ARCHITECTURE.md)** - Session management design
- **[ARROW_STREAMING_ARCHITECTURE.md](architect/ARROW_STREAMING_ARCHITECTURE.md)** - Arrow data streaming
- **[SPARK_CONNECT_QUICK_REFERENCE.md](architect/SPARK_CONNECT_QUICK_REFERENCE.md)** - Quick reference guide
- **[SPARK_CONNECT_PROTOCOL_COMPLIANCE.md](architect/SPARK_CONNECT_PROTOCOL_COMPLIANCE.md)** - Protocol compliance
- **[CATALOG_OPERATIONS.md](architect/CATALOG_OPERATIONS.md)** - Catalog operations implementation
- **[TYPE_MAPPING.md](architect/TYPE_MAPPING.md)** - DuckDB to Spark type mapping
- **[SPARKSQL_PARSER_DESIGN.md](architect/SPARKSQL_PARSER_DESIGN.md)** - SparkSQL ANTLR4 parser design (implemented)

### Research (research/)
Historical technical investigations: TPC-DS discovery, GROUPING() function analysis, Q36 DuckDB limitation.

### Pending Design (pending_design/)
Future feature specifications: Delta Lake integration, AWS credential chain, S3 throughput optimization.

### Development Journal (dev_journal/)
Milestone completion reports documenting the project's development progress.
Reports are prefixed with M[X]_ indicating chronological order (M1 through M71+).

Key milestones:
- **M1-M16**: Core infrastructure, TPC-H/TPC-DS, Spark Connect server
- **M17-M38**: Session management, DataFrame operations, window functions
- **M39-M44**: Differential testing framework, catalog operations
- **M45-M49**: Statistics, lambda functions, complex types, type literals
- **M50-M64**: Direct alias optimization, type inference, joins, sorting
- **M68-M71**: Decimal precision, datetime fixes, performance instrumentation
- **M72-M79**: SparkSQL parser, schema-aware dispatch, extension functions
- **M80-M83**: Strict mode convergence (744/2), test suite optimization

---

## Quick Links

**Getting Started**:
- Start with [../README.md](../README.md) - Project overview
- Then read [SUPPORTED_OPERATIONS.md](SUPPORTED_OPERATIONS.md) - What Spark operations are supported
- For testing: [architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md](architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md)

**For Developers**:
- Compatibility modes: [architect/SPARK_COMPAT_EXTENSION.md](architect/SPARK_COMPAT_EXTENSION.md)
- Gap analysis: [SPARK_CONNECT_GAP_ANALYSIS.md](SPARK_CONNECT_GAP_ANALYSIS.md)
- Optimization: [OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)

**For Operations**:
- Platform support: [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)
- Session management: [architect/SESSION_MANAGEMENT_ARCHITECTURE.md](architect/SESSION_MANAGEMENT_ARCHITECTURE.md)

---

**Last Updated**: 2026-02-14
