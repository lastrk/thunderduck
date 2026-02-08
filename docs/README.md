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
- **[LAMBDA_AND_CALL_FUNCTION_SPEC.md](architect/LAMBDA_AND_CALL_FUNCTION_SPEC.md)** - Lambda functions and higher-order functions
- **[STATISTICS_OPERATIONS_SPEC.md](architect/STATISTICS_OPERATIONS_SPEC.md)** - Statistics operations
- **[UDF_SUPPORT_RESEARCH.md](architect/UDF_SUPPORT_RESEARCH.md)** - UDF support research

### Research & Analysis (research/)
Deep technical investigations conducted during development:

- **[DUCKDB_TPCDS_DISCOVERY.md](research/DUCKDB_TPCDS_DISCOVERY.md)** - Discovery of DuckDB's TPC-DS implementation patterns
- **[TPCDS_ROOT_CAUSE_ANALYSIS.md](research/TPCDS_ROOT_CAUSE_ANALYSIS.md)** - Root cause analysis of Q36 & Q86 failures
- **[GROUPING_FUNCTION_ANALYSIS.md](research/GROUPING_FUNCTION_ANALYSIS.md)** - Analysis of GROUPING() function behavior

### Pending Design (pending_design/)
Future feature specifications:

- **[DELTA_LAKE_INTEGRATION_SPEC.md](pending_design/DELTA_LAKE_INTEGRATION_SPEC.md)** - Delta Lake integration
- **[AWS_CREDENTIAL_CHAIN_SPEC.md](pending_design/AWS_CREDENTIAL_CHAIN_SPEC.md)** - AWS credential chain support
- **[S3_THROUGHPUT_OPTIMIZATION_RESEARCH.md](pending_design/S3_THROUGHPUT_OPTIMIZATION_RESEARCH.md)** - S3 throughput optimization

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

**Last Updated**: 2026-02-08
