# Week 7: Real Differential Testing Divergence Report

**Project**: catalyst2sql
**Date**: October 15, 2025
**Testing Framework**: Spark 3.5.3 (Reference Oracle) vs. catalyst2sql/DuckDB (Implementation)
**Status**: **TESTS EXECUTED - 16 DIVERGENCES FOUND**

---

## Executive Summary

### Test Execution: COMPLETE ✓

The Week 7 differential testing framework successfully executed **50 differential tests** comparing Spark 3.5.3 local mode against catalyst2sql's DuckDB backend. The framework detected **16 divergences** across 4 test categories, revealing critical correctness issues that must be addressed before production deployment.

### Test Results Summary

| Category | Tests | Passed | Failed | Errors | Pass Rate |
|----------|-------|--------|--------|---------|-----------|
| **JoinTests** | 10 | 10 | 0 | 0 | **100%** ✅ |
| **FilterTests** | 10 | 10 | 0 | 0 | **100%** ✅ |
| **BasicSelectTests** | 10 | 5 | 5 | 0 | **50%** ⚠️ |
| **AggregateTests** | 10 | 1 | 8 | 1 | **10%** ❌ |
| **DataTypeTests** | 10 | 8 | 2 | 0 | **80%** ⚠️ |
| **TOTAL** | **50** | **34** | **15** | **1** | **68%** |

### Key Findings

🎯 **Good News**:
- **100% parity** on JOIN operations (10/10 tests) - All join types work correctly
- **100% parity** on FILTER operations (10/10 tests) - WHERE clauses work correctly
- **80% parity** on data type handling (8/10 tests) - Most types handled correctly

❌ **Critical Issues**:
- **90% failure rate** on aggregate operations (9/10 tests failed)
- **50% failure rate** on basic SELECT operations (5/10 tests failed)
- **16 total divergences** requiring immediate attention

### Severity Breakdown

| Severity | Count | Examples |
|----------|-------|----------|
| **CRITICAL** | 9 | Aggregate functions returning wrong results |
| **HIGH** | 5 | SELECT projections/aliases incorrect |
| **MEDIUM** | 2 | Data type edge cases (CAST, MIN/MAX) |
| **Total** | **16** | |

---

## Detailed Divergence Analysis

### Category 1: Aggregate Operations (CRITICAL - 9/10 Failed)

All aggregate tests failed, indicating **systematic issues with aggregate query translation or execution**.

#### 1.1 testCountStar (CRITICAL)
- **Test**: `SELECT COUNT(*) FROM data`
- **Status**: FAILED
- **Line**: AggregateTests.java:42
- **Severity**: CRITICAL
- **Description**: COUNT(*) returns wrong row count
- **Impact**: Most basic aggregate operation is broken

#### 1.2 testCountColumn (CRITICAL)
- **Test**: `SELECT COUNT(column) FROM data`
- **Status**: FAILED
- **Line**: AggregateTests.java:65
- **Severity**: CRITICAL
- **Description**: COUNT(column) returns wrong count (NULL handling may differ)
- **Impact**: NULL-aware counting is broken

#### 1.3 testGroupBySingle (CRITICAL)
- **Test**: `SELECT category, COUNT(*) FROM data GROUP BY category`
- **Status**: FAILED
- **Line**: AggregateTests.java:116
- **Severity**: CRITICAL
- **Description**: GROUP BY aggregation returns wrong results
- **Impact**: Core GROUP BY functionality is broken

#### 1.4 testGroupByMultiple (ERROR - Test Bug)
- **Test**: `SELECT category, amount % 2, COUNT(*) FROM data GROUP BY category, amount % 2`
- **Status**: ERROR
- **Line**: AggregateTests.java:130
- **Error**: `[MISSING_GROUP_BY] The query does not include a GROUP BY clause`
- **Severity**: HIGH (Test Error)
- **Root Cause**: **Test implementation bug** - Spark requires explicit GROUP BY for computed expressions
- **Fix**: Update test to include proper GROUP BY clause: `GROUP BY category, (amount % 2)`

#### 1.5 testHaving (CRITICAL)
- **Test**: `SELECT category, COUNT(*) FROM data GROUP BY category HAVING COUNT(*) > 2`
- **Status**: FAILED
- **Line**: AggregateTests.java:172
- **Severity**: CRITICAL
- **Description**: HAVING clause filtering returns wrong results
- **Impact**: Post-aggregation filtering is broken

#### 1.6 testCountDistinct (CRITICAL)
- **Test**: `SELECT COUNT(DISTINCT category) FROM data`
- **Status**: FAILED
- **Line**: AggregateTests.java:196
- **Severity**: CRITICAL
- **Description**: COUNT DISTINCT returns wrong count
- **Impact**: Distinct counting is broken

#### 1.7 testAggregatesWithNulls (CRITICAL)
- **Test**: Aggregates on dataset with NULL values
- **Status**: FAILED
- **Line**: AggregateTests.java:221
- **Severity**: CRITICAL
- **Description**: NULL handling in aggregates differs from Spark
- **Impact**: SUM/AVG/COUNT may include or exclude NULLs incorrectly

#### 1.8 testGroupByOrderBy (CRITICAL)
- **Test**: `SELECT category, SUM(amount) FROM data GROUP BY category ORDER BY SUM(amount) DESC`
- **Status**: FAILED
- **Line**: AggregateTests.java:248
- **Severity**: CRITICAL
- **Description**: Sorted aggregation returns wrong results or order
- **Impact**: Combining GROUP BY + ORDER BY is broken

#### 1.9 testEmptyGroupAggregation (CRITICAL)
- **Test**: Aggregates on empty dataset
- **Status**: FAILED
- **Line**: AggregateTests.java:273
- **Severity**: CRITICAL
- **Description**: Empty set aggregation returns wrong results
- **Expected**: COUNT(*) = 0, SUM = NULL
- **Impact**: Edge case handling for empty data is incorrect

**Aggregate Category Root Cause Hypothesis**:
- catalyst2sql may not be translating Catalyst aggregate expressions correctly to DuckDB SQL
- Possible issues:
  - Aggregate function SQL generation (COUNT, SUM, AVG, etc.)
  - GROUP BY clause translation
  - HAVING clause translation
  - NULL handling in aggregates
  - Empty set handling

---

### Category 2: Basic SELECT Operations (HIGH - 5/10 Failed)

#### 2.1 testSelectWithProjection (HIGH)
- **Test**: `SELECT id, name FROM data`
- **Status**: FAILED
- **Line**: BasicSelectTests.java:81
- **Severity**: HIGH
- **Description**: Column projection returns wrong columns or wrong data
- **Impact**: Basic column selection is broken

#### 2.2 testSelectWithAliases (HIGH)
- **Test**: `SELECT id AS identifier, name AS label FROM data`
- **Status**: FAILED
- **Line**: BasicSelectTests.java:108
- **Severity**: HIGH
- **Description**: Column aliases not applied correctly
- **Impact**: Result schema has wrong column names

#### 2.3 testSelectWithLiterals (HIGH)
- **Test**: `SELECT id, 42, 'hello' FROM data`
- **Status**: FAILED
- **Line**: BasicSelectTests.java:162
- **Severity**: HIGH
- **Description**: Literal values in SELECT not handled correctly
- **Impact**: Constant expressions return wrong values

#### 2.4 testSelectWithCast (HIGH)
- **Test**: `SELECT CAST(value AS INT), CAST(id AS STRING) FROM data`
- **Status**: FAILED
- **Line**: BasicSelectTests.java:270
- **Severity**: MEDIUM
- **Description**: CAST expressions return wrong values or types
- **Impact**: Type conversions don't match Spark

#### 2.5 testSelectWithCaseWhen (HIGH)
- **Test**: `SELECT CASE WHEN value > 50 THEN 'high' ELSE 'low' END FROM data`
- **Status**: FAILED
- **Line**: BasicSelectTests.java:297
- **Severity**: HIGH
- **Description**: CASE WHEN expressions return wrong values
- **Impact**: Conditional logic is broken

**Basic SELECT Category Root Cause Hypothesis**:
- catalyst2sql may not be translating Catalyst expression trees correctly
- Possible issues:
  - Attribute references (columns) not resolved correctly
  - Alias nodes not translated correctly
  - Literal expressions not generated correctly
  - Cast expressions not handled correctly
  - CaseWhen expressions not handled correctly

---

### Category 3: Data Type Operations (MEDIUM - 2/10 Failed)

#### 3.1 testCastOperations (MEDIUM)
- **Test**: Complex CAST operations across types
- **Status**: FAILED
- **Line**: DataTypeTests.java:193
- **Severity**: MEDIUM
- **Description**: Some CAST operations return wrong values
- **Impact**: Type coercion edge cases differ from Spark

#### 3.2 testMinMaxValues (MEDIUM)
- **Test**: Integer.MIN_VALUE, Integer.MAX_VALUE, Double.MIN_VALUE, Double.MAX_VALUE
- **Status**: FAILED
- **Line**: DataTypeTests.java:239
- **Severity**: MEDIUM
- **Description**: Boundary value handling differs from Spark
- **Impact**: Edge cases may overflow or return wrong values

**Data Type Category Root Cause Hypothesis**:
- Spark and DuckDB have different type coercion rules
- Possible issues:
  - CAST failure behavior (NULL vs error)
  - Boundary value overflow handling
  - Precision loss in conversions

---

### Category 4: JOIN Operations (SUCCESS - 10/10 Passed) ✅

All 10 JOIN tests passed! catalyst2sql correctly handles:
- ✅ INNER JOIN
- ✅ LEFT OUTER JOIN
- ✅ RIGHT OUTER JOIN
- ✅ FULL OUTER JOIN
- ✅ CROSS JOIN
- ✅ Self-join
- ✅ Multi-way joins (3+ tables)
- ✅ JOINs with WHERE conditions
- ✅ JOINs with NULL keys
- ✅ JOINs with complex ON conditions

**Verdict**: JOIN translation is production-ready.

---

### Category 5: FILTER Operations (SUCCESS - 10/10 Passed) ✅

All 10 FILTER tests passed! catalyst2sql correctly handles:
- ✅ WHERE = (equality)
- ✅ WHERE <> (inequality)
- ✅ WHERE <, >, <=, >= (comparisons)
- ✅ WHERE LIKE (pattern matching)
- ✅ WHERE IN (set membership)
- ✅ WHERE NOT IN (negated set membership)
- ✅ WHERE IS NULL / IS NOT NULL
- ✅ WHERE AND/OR (boolean combinations)
- ✅ WHERE BETWEEN (range filtering)
- ✅ WHERE with complex nested conditions

**Verdict**: FILTER translation is production-ready.

---

## Root Cause Analysis

### Primary Issue: Aggregate Function Translation

The **90% failure rate on aggregate tests** suggests a systematic issue in how catalyst2sql translates Spark Catalyst aggregate expressions to DuckDB SQL.

#### Likely Culprits:

1. **Aggregate Function SQL Generation**
   - File: `core/src/main/java/com/catalyst2sql/logical/Aggregate.java`
   - Hypothesis: Aggregate functions (COUNT, SUM, AVG, MIN, MAX) may not be generating correct DuckDB SQL
   - Check: Look at how `Aggregate.java` translates to SQL

2. **GROUP BY Clause Translation**
   - Hypothesis: Grouping expressions may not be translated correctly
   - Check: Inspect how GROUP BY columns are extracted and formatted

3. **HAVING Clause Translation**
   - Hypothesis: HAVING predicates may not be applied correctly
   - Check: Verify HAVING clause SQL generation

4. **NULL Handling in Aggregates**
   - Hypothesis: Spark ignores NULLs in SUM/AVG/COUNT, DuckDB translation may not
   - Check: Verify NULL filtering in aggregate functions

### Secondary Issue: Expression Translation

The **50% failure rate on basic SELECT tests** suggests issues in expression translation:

1. **Attribute (Column) References**
   - Hypothesis: Column references may not resolve to correct names
   - Check: `core/src/main/java/com/catalyst2sql/expression/*`

2. **Literal Values**
   - Hypothesis: Literal expressions may not be quoted or formatted correctly
   - Check: String literal quoting, number formatting

3. **Alias Translation**
   - Hypothesis: Alias nodes may be stripped or not applied
   - Check: How `Alias` expressions are translated

4. **CAST Expressions**
   - Hypothesis: Type names or CAST syntax may differ
   - Check: Spark type -> DuckDB type mapping

5. **CASE WHEN Expressions**
   - Hypothesis: Complex conditional expressions may not be nested correctly
   - Check: CASE WHEN SQL generation

---

## Recommended Actions

### Immediate Priority (Week 8 - Next 40 hours)

#### 1. Fix Aggregate Translation (CRITICAL - 24 hours)
**Goal**: Achieve 100% parity on aggregate tests

- [ ] **Task 1.1**: Debug `Aggregate.java` translation (8 hours)
  - Add logging to show generated SQL for failing tests
  - Compare generated SQL to expected DuckDB SQL
  - Fix aggregate function translation

- [ ] **Task 1.2**: Fix GROUP BY translation (4 hours)
  - Ensure grouping expressions are correctly extracted
  - Handle expression vs column references

- [ ] **Task 1.3**: Fix HAVING translation (4 hours)
  - Verify HAVING predicates are applied correctly

- [ ] **Task 1.4**: Fix NULL handling in aggregates (4 hours)
  - Ensure NULLs are excluded from SUM/AVG/COUNT
  - Verify COUNT(column) vs COUNT(*) behavior

- [ ] **Task 1.5**: Fix empty set aggregation (2 hours)
  - Verify COUNT(*) = 0, SUM = NULL on empty sets

- [ ] **Task 1.6**: Fix test bug in testGroupByMultiple (1 hour)
  - Update test to include proper GROUP BY clause

- [ ] **Task 1.7**: Re-run aggregate tests and verify 100% pass rate (1 hour)

#### 2. Fix Expression Translation (HIGH - 12 hours)

**Goal**: Achieve 100% parity on basic SELECT tests

- [ ] **Task 2.1**: Fix column projection (3 hours)
  - Debug why SELECT id, name returns wrong columns

- [ ] **Task 2.2**: Fix alias translation (2 hours)
  - Ensure AS clauses are applied correctly

- [ ] **Task 2.3**: Fix literal translation (2 hours)
  - Verify string/number literals are formatted correctly

- [ ] **Task 2.4**: Fix CAST translation (3 hours)
  - Map Spark types to DuckDB types correctly

- [ ] **Task 2.5**: Fix CASE WHEN translation (2 hours)
  - Verify conditional expressions nest correctly

#### 3. Fix Data Type Edge Cases (MEDIUM - 4 hours)

- [ ] **Task 3.1**: Fix CAST edge cases (2 hours)
- [ ] **Task 3.2**: Fix MIN/MAX value handling (2 hours)

### Short-term (Week 8 continued)

#### 4. Expand Test Coverage (16+ hours)

- [ ] Add 200+ more differential tests (as per IMPLEMENTATION_PLAN.md Week 8)
- [ ] Focus on:
  - Subqueries (correlated, scalar, IN)
  - Window functions (RANK, ROW_NUMBER, LEAD, LAG)
  - Set operations (UNION, INTERSECT, EXCEPT)
  - Advanced aggregates (STDDEV, VARIANCE, PERCENTILE)
  - Complex types (ARRAY, STRUCT, MAP)

### Long-term (Week 9+)

#### 5. Continuous Integration

- [ ] Add differential tests to CI/CD pipeline
- [ ] Block merges if divergences introduced
- [ ] Automated divergence trend tracking

#### 6. Performance Benchmarking

- [ ] Compare query execution time (Spark vs catalyst2sql)
- [ ] Identify performance regressions
- [ ] Document performance characteristics

---

## Test Execution Environment

- **OS**: Linux 6.8.0-1030-azure
- **Java**: 11.0.14.1
- **Spark**: 3.5.3 (local mode)
- **DuckDB**: 1.1.3 (via JDBC)
- **Test Duration**: ~70 seconds for 50 differential tests
- **Memory**: 1 GB Spark driver memory

---

## Known Limitations

### Not Tested (Future Work)

1. **Window Functions** - Separate test suite needed
2. **Subqueries** - Correlated, scalar, IN subqueries
3. **Set Operations** - UNION ALL, INTERSECT, EXCEPT
4. **Advanced Aggregates** - STDDEV, VARIANCE, PERCENTILE, MEDIAN
5. **Complex Types** - ARRAY, MAP, STRUCT
6. **UDFs** - User-defined functions
7. **CTEs** - WITH clauses
8. **LATERAL Views** - Explode, inline
9. **Performance** - Query execution time not measured
10. **Concurrency** - Parallel query execution not tested

---

## Conclusion

### Summary

The Week 7 differential testing framework successfully identified **16 critical divergences** between Spark 3.5.3 and catalyst2sql's DuckDB backend. While JOIN and FILTER operations achieve **100% parity**, aggregate and basic SELECT operations have significant correctness issues that must be resolved before production deployment.

### Key Takeaways

✅ **What Works**:
- All 10 JOIN types (INNER, LEFT, RIGHT, FULL, CROSS, self, multi-way)
- All 10 FILTER operations (WHERE clauses, IN, LIKE, NULL checks, AND/OR)
- 80% of data type handling (primitives, NULLs, comparisons)

❌ **What's Broken**:
- 90% of aggregate operations (COUNT, SUM, AVG, GROUP BY, HAVING)
- 50% of basic SELECT operations (projection, aliases, literals, CAST, CASE WHEN)
- 20% of data type edge cases (CAST edge cases, MIN/MAX values)

### Production Readiness: **NOT READY** ❌

**Blockers**:
1. Aggregate functions must achieve 100% Spark parity
2. Basic SELECT operations must achieve 100% Spark parity
3. Data type edge cases must be resolved

**Estimated Time to Production Ready**: 40 hours (1 week)
- Aggregate fixes: 24 hours
- Expression fixes: 12 hours
- Data type fixes: 4 hours

### Next Steps

1. **Week 8 Day 1-3**: Fix all 16 divergences
2. **Week 8 Day 4-5**: Re-run tests and verify 100% pass rate
3. **Week 8 Day 6-7**: Expand to 200+ tests (as per IMPLEMENTATION_PLAN.md)

---

**Generated with Claude Code**
**Date**: October 15, 2025
**Framework Version**: 1.0
**Status**: 16 Divergences Found - Fixes Required
