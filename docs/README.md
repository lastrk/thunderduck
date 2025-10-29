# Thunderduck Documentation

This directory contains comprehensive documentation for the thunderduck project.

---

## Core Documentation

### Architecture & Design
- **[PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)** - Multi-architecture support (x86_64, ARM64)
- **[OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)** - Why we use DuckDB's optimizer
- **[SPARK_CONNECT_PROTOCOL_SPEC.md](SPARK_CONNECT_PROTOCOL_SPEC.md)** - Spark Connect protocol details
- **[Testing_Strategy.md](Testing_Strategy.md)** - BDD and differential testing approach

### Spark Connect Architecture (architect/)
- **[SPARK_CONNECT_ARCHITECTURE.md](architect/SPARK_CONNECT_ARCHITECTURE.md)** - Server architecture
- **[SINGLE_SESSION_ARCHITECTURE.md](architect/SINGLE_SESSION_ARCHITECTURE.md)** - Session management design
- **[SPARK_CONNECT_QUICK_REFERENCE.md](architect/SPARK_CONNECT_QUICK_REFERENCE.md)** - Quick reference guide

### Build & Infrastructure (coder/)
- **[01_Build_Infrastructure_Design.md](coder/01_Build_Infrastructure_Design.md)** - Maven build system
- **[02_Testing_Infrastructure_Design.md](coder/02_Testing_Infrastructure_Design.md)** - Test framework
- **[03_Module_Organization_Design.md](coder/03_Module_Organization_Design.md)** - Module structure
- **[04_CI_CD_Integration_Design.md](coder/04_CI_CD_Integration_Design.md)** - CI/CD pipeline
- **[05_Data_Generation_Pipeline_Design.md](coder/05_Data_Generation_Pipeline_Design.md)** - Test data generation

### Research & Analysis (research/)
Deep technical investigations and root cause analyses conducted during development:

- **[DUCKDB_TPCDS_DISCOVERY.md](research/DUCKDB_TPCDS_DISCOVERY.md)** - Discovery of DuckDB's TPC-DS implementation patterns
- **[TPCDS_ROOT_CAUSE_ANALYSIS.md](research/TPCDS_ROOT_CAUSE_ANALYSIS.md)** - Root cause analysis of Q36 & Q86 failures
- **[Q36_DUCKDB_LIMITATION.md](research/Q36_DUCKDB_LIMITATION.md)** - Documentation of Q36's GROUPING() in PARTITION BY limitation
- **[GROUPING_FUNCTION_ANALYSIS.md](research/GROUPING_FUNCTION_ANALYSIS.md)** - Analysis of GROUPING() function behavior
- **[GROUPING_STANDARD_RESEARCH.md](research/GROUPING_STANDARD_RESEARCH.md)** - Research on SQL standard GROUPING() semantics
- **[GROUPING_ANSWER.md](research/GROUPING_ANSWER.md)** - Definitive answer on GROUPING() standardization

---

## Quick Links

**Getting Started**:
- Start with [../README.md](../README.md) - Project overview
- Then read [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md) - Platform selection guide
- For testing: [Testing_Strategy.md](Testing_Strategy.md)

**For Developers**:
- Architecture: [architect/SPARK_CONNECT_ARCHITECTURE.md](architect/SPARK_CONNECT_ARCHITECTURE.md)
- Build system: [coder/01_Build_Infrastructure_Design.md](coder/01_Build_Infrastructure_Design.md)
- Optimization: [OPTIMIZATION_STRATEGY.md](OPTIMIZATION_STRATEGY.md)

**For Operations**:
- Platform support: [PLATFORM_SUPPORT.md](PLATFORM_SUPPORT.md)
- Spark Connect: [architect/SINGLE_SESSION_ARCHITECTURE.md](architect/SINGLE_SESSION_ARCHITECTURE.md)

---

**Last Updated**: 2025-10-29
