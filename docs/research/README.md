# ThunderDuck Research & Analysis Documents

This directory contains in-depth technical research, investigations, and root cause analyses conducted during the ThunderDuck project development.

## Overview

These documents capture critical discoveries and insights gained while implementing Spark-to-DuckDB compatibility, particularly around TPC-DS query support. They serve as both historical reference and technical documentation for future development.

## Documents

### TPC-DS Query Analysis

1. **[TPCDS_ROOT_CAUSE_ANALYSIS.md](TPCDS_ROOT_CAUSE_ANALYSIS.md)**
   - Comprehensive root cause analysis of Q36 and Q86 failures
   - Identifies DuckDB limitations and workarounds
   - Status: Resolved (Q86 fixed, Q36 documented as DuckDB limitation)

2. **[DUCKDB_TPCDS_DISCOVERY.md](DUCKDB_TPCDS_DISCOVERY.md)**
   - Critical discovery of how DuckDB handles TPC-DS queries
   - Analysis of DuckDB's official TPC-DS implementation
   - Key finding: DuckDB rewrites Q36 to avoid ROLLUP limitations

3. **[Q36_DUCKDB_LIMITATION.md](Q36_DUCKDB_LIMITATION.md)**
   - Final documentation of Q36's unsupported pattern
   - GROUPING() in PARTITION BY is not supported by DuckDB
   - Includes DuckDB's official workaround approach

### GROUPING() Function Research

4. **[GROUPING_FUNCTION_ANALYSIS.md](GROUPING_FUNCTION_ANALYSIS.md)**
   - Initial analysis of GROUPING() function discrepancies
   - Hypothesis testing and behavior comparison
   - Led to deeper investigation of SQL standards

5. **[GROUPING_STANDARD_RESEARCH.md](GROUPING_STANDARD_RESEARCH.md)**
   - Research into SQL standard specifications
   - Cross-database compatibility analysis
   - Investigation of GROUPING() semantics across systems

6. **[GROUPING_ANSWER.md](GROUPING_ANSWER.md)**
   - Definitive answer: GROUPING() is SQL standard (SQL:1999)
   - All major databases implement identical semantics
   - Confirms DuckDB follows the standard for basic usage

## Key Findings Summary

### DuckDB Limitations Discovered
- **GROUPING() in PARTITION BY**: Not supported, requires query rewriting
- **Solution**: Use UNION ALL approach instead of ROLLUP for affected queries

### Compatibility Achievements
- **100% pass rate**: All 91 tested TPC-DS queries now pass
- **Q86 fixed**: Proper handling of DESC/ASC keywords and NULLS FIRST
- **Q36 documented**: Known limitation with clear explanation

### Technical Insights
1. DuckDB implements standard GROUPING() semantics but with context limitations
2. ROLLUP queries require explicit NULLS FIRST in ORDER BY clauses
3. DESC/ASC keywords must be uppercase in DuckDB
4. Complex ORDER BY expressions need careful parsing for compatibility

## Using This Research

These documents are valuable for:
- Understanding DuckDB's SQL limitations and workarounds
- Debugging future TPC-DS or ROLLUP query issues
- Extending ThunderDuck's SQL preprocessing capabilities
- Historical reference for design decisions

## Contributing

When adding new research documents:
1. Use descriptive names in CAPS_WITH_UNDERSCORES format
2. Include clear problem statement and methodology
3. Document findings with evidence (code samples, references)
4. Add summary to this README

**Last Updated**: 2025-10-29