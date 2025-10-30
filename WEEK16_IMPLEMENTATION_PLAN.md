# Week 16 Implementation Plan: DataFrame API Testing & Translation Validation

**Duration**: 5 days
**Goal**: Implement comprehensive DataFrame API testing using TPC-H queries to validate ThunderDuck's translation layer
**Priority**: HIGH - Critical for production readiness
**Prerequisites**: ✅ 100% TPC-H SQL coverage (Week 14) | ✅ 99% TPC-DS SQL coverage (Week 15)

---

## Executive Summary

Week 16 addresses a critical testing gap: the current TPC-H/TPC-DS tests use SQL passthrough, bypassing ThunderDuck's DataFrame-to-SQL translation layer. This week implements TPC-H queries using the DataFrame API to ensure comprehensive testing of the complete translation stack.

**Key Insight**: Most Spark users use DataFrame API, not raw SQL. Testing only SQL passthrough leaves the primary usage path unvalidated.

---

## Problem Statement

### Current Testing Gap

```python
# Current tests - bypass translation
spark.sql("SELECT * FROM lineitem WHERE l_quantity > 40")
# Path: SQL string → Direct DuckDB execution
```

### What We Need to Test

```python
# DataFrame API - exercises full stack
df.filter(df.l_quantity > 40).groupBy(...).agg(...)
# Path: DataFrame ops → RelationConverter → LogicalPlan → SQLGenerator → DuckDB
```

---

## Week 16 Daily Plan

### Day 1: Foundation & Basic Queries (Monday)

**Goal**: Set up DataFrame test framework and implement Q1-Q3

**Tasks**:
1. **Create test structure** (1 hour)
   - Create `tests/integration/test_tpch_dataframe.py`
   - Set up result validation helpers
   - Create reference data loader

2. **Implement Q1** (1 hour)
   - Pricing summary report
   - Tests: Filter, GroupBy, Aggregate, OrderBy
   - Complex expressions in aggregations

3. **Implement Q2** (1 hour)
   - Minimum cost supplier
   - Tests: Subquery pattern, multi-table join
   - Nested conditions

4. **Implement Q3** (1 hour)
   - Shipping priority
   - Tests: 3-way join with filters
   - GroupBy after join

**Validation**:
- Compare results with SQL reference data
- Ensure exact value matches
- Verify execution time is reasonable

**Success Criteria**: Q1-Q3 passing with exact match to SQL results

---

### Day 2: Join-Heavy Queries (Tuesday)

**Goal**: Implement Q4-Q7 focusing on complex joins

**Tasks**:
1. **Q4: Order Priority** (1 hour)
   - EXISTS subquery pattern
   - Semi-join implementation

2. **Q5: Local Supplier Volume** (1.5 hours)
   - 6-way join
   - Complex predicates across tables
   - Regional filtering

3. **Q6: Forecasting Revenue** (0.5 hours)
   - Simple but multiple filters
   - Aggregate with expressions

4. **Q7: Volume Shipping** (1 hour)
   - Nation-to-nation shipping
   - Date range filters
   - Multi-condition joins

**Validation**:
- Test join ordering optimization
- Verify predicate pushdown
- Check memory usage

**Success Criteria**: Q4-Q7 passing, join performance acceptable

---

### Day 3: Aggregations & Analytics (Wednesday)

**Goal**: Implement Q8-Q14 focusing on complex aggregations

**Tasks**:
1. **Q8-Q10** (2 hours)
   - Market share calculations
   - Returned item reporting
   - Nation aggregations

2. **Q11-Q12** (1.5 hours)
   - Important stock identification
   - Shipping mode analysis
   - HAVING clause patterns

3. **Q13-Q14** (1.5 hours)
   - Customer distribution
   - Promotion effect
   - Percentage calculations

**Key Patterns**:
- Nested aggregations
- Conditional aggregations
- Group by with complex expressions
- HAVING clause equivalents

**Success Criteria**: Q8-Q14 passing, complex aggregations working

---

### Day 4: Advanced Patterns (Thursday)

**Goal**: Complete Q15-Q22 with focus on subqueries and complex patterns

**Tasks**:
1. **Q15-Q17** (2 hours)
   - Top supplier query (CTE pattern)
   - Parts/supplier relationship
   - Small quantity order revenue

2. **Q18-Q20** (2 hours)
   - Large volume customers
   - Discount revenue
   - Supplier nation volume

3. **Q21-Q22** (1 hour)
   - Suppliers who kept orders waiting
   - Global sales opportunity

**Complex Patterns**:
- Correlated subqueries
- NOT EXISTS patterns
- Multiple aggregation levels
- Window functions (if supported)

**Success Criteria**: All 22 TPC-H queries implemented and passing

---

### Day 5: Optimization & Documentation (Friday)

**Goal**: Optimize, document, and create comprehensive test report

**Tasks**:
1. **Performance Analysis** (2 hours)
   - Compare DataFrame vs SQL performance
   - Identify translation overhead
   - Document bottlenecks

2. **Coverage Documentation** (1 hour)
   - Create DataFrame API compatibility matrix
   - Document supported/unsupported operations
   - Note any limitations found

3. **Test Enhancement** (1 hour)
   - Add edge case tests
   - Test error handling
   - Validate type conversions

4. **Final Report** (1 hour)
   - Create Week 16 completion report
   - Document all findings
   - Recommend next steps

**Deliverables**:
- Complete TPC-H DataFrame test suite
- Performance comparison report
- API compatibility documentation
- Week 16 completion report

---

## Implementation Details

### Test Structure

```python
class TestTPCHDataFrame:
    """TPC-H queries using DataFrame API"""

    def load_reference(self, query_num):
        """Load SQL test reference data"""

    def compare_results(self, df_results, sql_reference):
        """Compare DataFrame results with SQL reference"""

    def test_q1_pricing_summary(self):
        """Q1 implementation using DataFrame API"""
        # Full implementation with validation
```

### Key DataFrame Operations to Test

**Basic Operations**:
- ✅ filter() / where()
- ✅ select() / selectExpr()
- ✅ groupBy() / agg()
- ✅ orderBy() / sort()
- ✅ limit()

**Join Operations**:
- ✅ join() - inner, left, right, full
- ✅ Semi-joins (EXISTS pattern)
- ✅ Anti-joins (NOT EXISTS pattern)

**Expressions**:
- ✅ Column arithmetic
- ✅ Conditional (when/otherwise)
- ✅ String operations
- ✅ Date operations
- ✅ Type casting

**Advanced** (verify support):
- ⚠️ Window functions
- ⚠️ Pivot operations
- ⚠️ ROLLUP/CUBE
- ⚠️ Complex subqueries

---

## Success Metrics

### Functional Metrics
- ✅ 22/22 TPC-H queries implemented using DataFrame API
- ✅ 100% result accuracy vs SQL references
- ✅ All major DataFrame operations tested
- ✅ Translation layer fully validated

### Performance Metrics
- Translation overhead < 100ms per query
- DataFrame execution within 2x of SQL execution
- Memory usage comparable to SQL tests
- No memory leaks or resource issues

### Coverage Metrics
- Branch coverage of RelationConverter > 80%
- All supported DataFrame operations documented
- Edge cases identified and tested
- Error handling validated

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|---------|------------|
| Unsupported operations | Medium | Document and implement missing ops |
| Performance regression | Low | Accept overhead, optimize critical paths |
| Complex query translation | Medium | Start simple, build complexity |
| Test maintenance | Low | Share reference data with SQL tests |

---

## Expected Outcomes

By end of Week 16:
1. **Complete test coverage** of DataFrame-to-SQL translation
2. **Production confidence** in DataFrame API support
3. **Performance baseline** for translation overhead
4. **Documentation** of supported operations
5. **Foundation** for TPC-DS DataFrame tests (future)

---

## Notes

- Start with simple queries, progressively increase complexity
- Reuse SQL test reference data for validation
- Document any missing DataFrame operations discovered
- Consider creating helper functions for common patterns
- Focus on correctness first, optimization second

---

*Plan Created: October 29, 2025*
*Status: Ready for Implementation*
*Estimated Effort: 5 days (40 hours)*