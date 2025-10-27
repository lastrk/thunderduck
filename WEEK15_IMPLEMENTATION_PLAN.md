# Week 15 Implementation Plan: TPC-DS Benchmark Validation (99 Queries)

**Duration**: Multi-week effort (estimated 40-60 hours total)
**Goal**: Achieve comprehensive TPC-DS coverage and validate Spark parity across decision support workloads
**Status**: PLANNED

---

## Executive Summary

Week 15 focuses on expanding validation coverage to the **TPC-DS benchmark suite**, which contains **99 queries** testing complex decision support scenarios. This is significantly more comprehensive than TPC-H (22 queries) and includes advanced SQL features like window functions, rollup/cube, and complex analytical patterns.

**Challenge**: TPC-DS is 4.5x larger than TPC-H with more complex SQL
**Approach**: Phased validation, prioritize high-value queries first
**Target**: Progressive coverage goals (50% → 75% → 90% → 100%)

---

## TPC-DS vs TPC-H Comparison

| Aspect | TPC-H | TPC-DS |
|--------|-------|---------|
| **Queries** | 22 | 99 |
| **Tables** | 8 | 24 |
| **Complexity** | Moderate | High |
| **SQL Features** | Standard | Advanced (window functions, ROLLUP, CUBE) |
| **Use Case** | Ad-hoc queries | Decision support + BI |

**Key Difference**: TPC-DS includes window functions, ROLLUP/CUBE, and more complex analytical patterns that TPC-H doesn't cover.

---

## Prerequisites ✅

From Weeks 13-14:
- ✅ 100% TPC-H coverage (22/22 queries)
- ✅ Server working perfectly (DATE fix, protobuf fix)
- ✅ Testing framework proven (reference-based validation)
- ✅ Infrastructure ready (data pipeline, validation tools)

---

## Week 15 Phased Approach

### Phase 1: Infrastructure Setup (Day 1-2, 8-12 hours)

**Task 1.1**: Generate TPC-DS Data (4-6 hours)
- Use Spark's TPC-DS data generator
- Scale factor: 1 GB (small enough for fast testing)
- Generate all 24 tables
- Convert to Parquet format
- Store in `/workspace/data/tpcds_sf1/`

**Task 1.2**: Download TPC-DS Queries (2-3 hours)
- Source: databricks/spark-sql-perf or similar
- Get all 99 query SQL files
- Store in `/workspace/benchmarks/tpcds_queries/`
- Categorize by complexity

**Task 1.3**: Initial Compatibility Assessment (2-3 hours)
- Run smoke test on all 99 queries
- Identify which work immediately
- Categorize failures by missing features
- Create feature gap analysis

**Deliverable**: TPC-DS infrastructure ready, gap analysis complete

---

### Phase 2: Tier 1 Validation (Day 3-5, 12-18 hours)

**Target**: 30-40 simpler TPC-DS queries (queries without window functions)

**Task 2.1**: Generate Reference Data (6-8 hours)
- Select 30-40 queries that work with current features
- Generate Spark reference files
- Validate data quality

**Task 2.2**: Create Validation Tests (4-6 hours)
- Add test methods for Tier 1 queries
- Follow same pattern as TPC-H tests
- Organize by query groups

**Task 2.3**: Run Validation (2-4 hours)
- Execute all Tier 1 tests
- Document pass/fail
- Fix any issues found

**Deliverable**: 30-40 TPC-DS queries validated (30-40% coverage)

---

### Phase 3: Window Functions Implementation (Day 6-10, 20-30 hours)

**Many TPC-DS queries require window functions** (ROW_NUMBER, RANK, DENSE_RANK, etc.)

**Task 3.1**: Implement Window Function Support (12-16 hours)
- ROW_NUMBER(), RANK(), DENSE_RANK()
- LAG(), LEAD()
- SUM/AVG OVER (...)
- PARTITION BY, ORDER BY in window specs
- Frame specifications (ROWS BETWEEN, RANGE BETWEEN)

**Task 3.2**: Test Window Functions (4-6 hours)
- Unit tests for each window function
- Integration tests with TPC-DS queries
- Validate against Spark results

**Task 3.3**: Tier 2 Validation (4-8 hours)
- Queries using window functions
- Generate references, create tests
- Run validation

**Deliverable**: Window functions working, 50-60 queries validated (50-60% coverage)

---

### Phase 4: Advanced Features & Full Coverage (Day 11-15, 20-30 hours)

**Task 4.1**: Implement ROLLUP/CUBE (if needed, 8-12 hours)
- GROUP BY ROLLUP
- GROUP BY CUBE
- GROUPING SETS

**Task 4.2**: Remaining Query Validation (8-12 hours)
- Complex analytical queries
- Multi-level aggregations
- Advanced window patterns

**Task 4.3**: Final Push to 100% (4-6 hours)
- Fix remaining failures
- Edge case handling
- Complete validation

**Deliverable**: 90-100% TPC-DS coverage

---

## Realistic Milestones

### Minimum Success (Week 15 Phase 1-2)
- ✅ TPC-DS data generated (24 tables)
- ✅ All 99 queries downloaded
- ✅ 30-40 queries validated (30-40% coverage)
- ✅ Gap analysis complete

### Target Success (Week 15 Complete)
- ✅ Window functions implemented
- ✅ 50-60 queries validated (50-60% coverage)
- ✅ Clear roadmap for remaining queries

### Stretch Goal (Week 15-16)
- ✅ ROLLUP/CUBE implemented
- ✅ 90+ queries validated (90%+ coverage)
- ✅ Path to 100% clear

### Ultimate Goal (Multi-Week)
- ✅ 99/99 queries validated (100% TPC-DS coverage)
- ✅ Complete Spark parity across both benchmarks
- ✅ Production-ready for all decision support workloads

---

## Week 15 Focus: Progressive Validation

### Day 1-2: Setup & Assessment
**Goal**: Infrastructure ready, know what works today
**Output**: Gap analysis showing which queries work vs need features

### Day 3-5: Low-Hanging Fruit
**Goal**: Validate 30-40 queries that work with current features
**Output**: 30-40% TPC-DS coverage without new features

### Day 6-10: Window Functions (if needed)
**Goal**: Implement window function support
**Output**: Enable 50-60% coverage

### Day 11-15: Advanced Features
**Goal**: Push towards 90%+ coverage
**Output**: Most TPC-DS queries validated

---

## TPC-DS Query Categories

Based on research, TPC-DS queries fall into these categories:

**Basic Queries** (~20-25 queries):
- Simple scans, filters, joins
- No window functions
- Should work with current implementation

**Window Function Queries** (~40-50 queries):
- ROW_NUMBER, RANK patterns
- Moving averages, running totals
- Requires window function implementation

**Advanced Analytical** (~20-30 queries):
- ROLLUP, CUBE, GROUPING SETS
- Complex multi-level aggregations
- May require additional features

**Very Complex** (~5-10 queries):
- Multiple window functions
- Complex CTEs with windows
- Most challenging queries

---

## Resource Requirements

### Data Generation
- TPC-DS data generator (included in Spark)
- Scale factor 1 (1 GB) for fast testing
- 24 tables (vs 8 in TPC-H)
- More complex schema relationships

### Development Time Estimate
- Phase 1 (Setup): 8-12 hours
- Phase 2 (Basic queries): 12-18 hours
- Phase 3 (Window functions): 20-30 hours
- Phase 4 (Advanced): 20-30 hours
- **Total**: 60-90 hours (2-3 weeks of focused work)

### Success Criteria
- **Minimum**: 30% coverage (good progress)
- **Target**: 50% coverage (solid achievement)
- **Excellent**: 75% coverage (very good)
- **Outstanding**: 90%+ coverage (exceptional)
- **Perfect**: 100% coverage (ultimate goal)

---

## Risk Assessment

### High Probability Challenges

1. **Window Functions** (if not implemented)
   - Impact: ~40-50 queries blocked
   - Effort: 20-30 hours to implement
   - Mitigation: Implement incrementally, test frequently

2. **ROLLUP/CUBE** (if not implemented)
   - Impact: ~20-30 queries affected
   - Effort: 8-12 hours to implement
   - Mitigation: May defer to later if needed

3. **Query Complexity**
   - Some queries are very complex
   - May expose edge cases
   - Mitigation: Incremental fixes, good logging

### Low Probability Challenges

4. **Data Generation**
   - TPC-DS generator should work
   - Mitigation: Use Spark's built-in tooling

5. **Performance Issues**
   - Larger queries may be slower
   - Mitigation: Correctness first, optimize later

---

## Week 15 Day 1 Immediate Tasks

**Today's Goals** (4-6 hours):

1. **Generate TPC-DS data** (2-3 hours)
   - Use Spark's TPC-DS data generator
   - SF=1 (1 GB scale)
   - Save to Parquet

2. **Download all 99 queries** (1-2 hours)
   - Source: databricks/spark-sql-perf
   - Verify SQL syntax
   - Organize by query number

3. **Run smoke test** (1-2 hours)
   - Execute all 99 queries
   - Count successes/failures
   - Categorize failure types

**Expected Output**:
- TPC-DS data ready
- All 99 queries available
- Initial pass/fail assessment
- Gap analysis complete

---

## Success Definition

Week 15 is successful if we achieve **ANY** of these:

**Option A**: 30-40 queries validated
- Proves TPC-DS feasibility
- Identifies feature gaps clearly
- Good foundation for future work

**Option B**: Window functions implemented + 50+ queries
- Major feature addition
- Significant coverage increase
- Production value high

**Option C**: 75+ queries validated
- Exceptional achievement
- Most TPC-DS patterns covered
- Near production-complete

**Option D**: 99/99 queries (stretch goal)
- Perfect score
- Complete decision support validation
- Ultimate benchmark achievement

---

## Relationship to Original Plan

**Original Week 15-16 goals** → Moved to **Backlog/Future Work**:
- Performance optimization
- Production hardening
- Monitoring & observability
- Multi-session support
- Docker/Kubernetes deployment

**New Week 15 goal**: TPC-DS validation (more valuable for proving capabilities)

**Rationale**:
- Correctness validation > optimization at this stage
- TPC-DS coverage proves feature completeness
- Can optimize later with more data

---

**Plan Created**: 2025-10-27
**Status**: Ready to start
**First Task**: Generate TPC-DS SF=1 data (24 tables, 1 GB)
**Expected Duration**: 2-3 weeks for comprehensive coverage
**Immediate Goal**: Validate 30-40 queries without new features
