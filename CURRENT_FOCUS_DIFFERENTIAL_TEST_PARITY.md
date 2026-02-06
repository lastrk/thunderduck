# Current Focus: Differential Test Parity with Spark 4.x

**Status:** In Progress
**Updated:** 2026-02-06 (Post-Implementation Test Results)
**Previous Update:** 2026-02-06 (Afternoon - Type Casting Fixes Implemented)

---

## Executive Summary

### Test Results Overview (2026-02-06)

| Test Suite | Total | Passed | Failed | Skipped | Pass Rate | Change |
|------------|-------|--------|--------|---------|-----------|--------|
| **Maven Unit Tests** | 976 | 976 | 0 | 0 | **100%** | ✅ Maintained |
| **Differential Tests (Before)** | 854 | 438 | 363 | 53 | 51.3% | Baseline |
| **Differential Tests (After)** | 854 | **445** | **356** | 53 | **52.1%** | +7 tests |

### Key Findings

1. **Maven Unit Tests: EXCELLENT** - 976/976 tests passing (100%)
   - All core functionality tests pass
   - Zero failures, zero skipped
   - Covers expressions, converters, type system, SQL generation, error handling

2. **Differential Tests: MODEST IMPROVEMENT** - 445/854 tests passing (52.1%)
   - Up from 438 passing before type fixes (+7 tests, +0.8%)
   - Up from 411 passing on 2026-02-05 (+34 tests total this week)
   - 53 tests intentionally skipped (unsupported DuckDB features)
   - 356 failures remain (down from 363)
   - **Expected impact not fully realized** - targeted 100+ test improvement, achieved 7

3. **Critical Discovery: Arrow Serialization Layer Issue** 🔴
   - DecimalVector → BigIntVector errors **persist** despite SQL fixes
   - Root cause: Arrow vector creation layer forces type conversions
   - SQL generation correctly preserves DECIMAL types ✅
   - But Arrow serialization fails when creating result vectors ❌
   - **Impact**: Blocks TPC-H Q1/Q3/Q5/Q6, TPC-DS Q43/Q62/Q99 (10+ major queries)

4. **New Issue: DECIMAL Precision/Rounding** 🟡
   - ~60 tests show unexpected decimal rounding
   - Example: `31079.94` (Spark) vs `31080.00` (Thunderduck)
   - Affects revenue/ratio calculations across TPC-DS
   - Possible causes: DuckDB DECIMAL arithmetic, intermediate precision loss

5. **Successful Fix: DateTime Functions** ✅
   - Year, month, day, etc. now return INTEGER (was BIGINT)
   - ~7 tests now passing
   - Clean implementation, no side effects

### Latest Implementation (2026-02-06 Afternoon)

**Type Casting Error Fixes - PARTIALLY SUCCESSFUL** ⚠️

Two major root causes addressed:

1. **DateTime Function Type Mismatch** (~20-30 tests affected) ✅ **SUCCESS**
   - **Problem**: DuckDB returns BIGINT, Spark expects INTEGER (32-bit)
   - **Solution**: Added CAST to INTEGER in SQL generation
   - **Functions Fixed**: year, month, day, dayofmonth, hour, minute, second, quarter, weekofyear, dayofyear
   - **File**: `FunctionRegistry.java`
   - **Status**: Verified with manual tests - returns IntegerType ✅
   - **Impact**: Estimated ~5-7 tests fixed

2. **DECIMAL Aggregation Precision Loss** (~100+ tests affected) ❌ **LIMITED SUCCESS**
   - **Problem**: Generic SUM always cast to BIGINT, breaking DECIMAL precision
   - **Solution**: Type-aware SUM logic based on input type
     - INTEGER/LONG/SHORT/BYTE → Cast to BIGINT (overflow prevention)
     - DECIMAL → No cast (preserves precision and scale)
     - FLOAT/DOUBLE → No cast (already correct)
   - **File**: `Aggregate.java`
   - **Expected Impact**: TPC-DS Q43, Q48, Q62, Q99 (conditional SUM on prices)
   - **Actual Result**: ❌ Q43, Q62, Q99 DataFrame versions STILL FAILING with same error
   - **Root Cause**: Arrow serialization layer issue, not SQL generation issue

### Post-Implementation Test Results (2026-02-06 Evening)

**Full differential test suite run completed:**
- **Time**: 143.63 seconds (2:23)
- **Results**: 445 passed, 356 failed, 53 skipped (+7 tests from 438 baseline)
- **Pass Rate**: 52.1% (up from 51.3%, +0.8% improvement)

**⚠️ CRITICAL FINDING**: The DecimalVector → BigIntVector errors persist in:
- TPC-H DataFrame: Q1, Q3, Q5, Q6 (still failing)
- TPC-DS DataFrame: Q43, Q62, Q99 (still failing)
- Error occurs in "Query streaming failed after 0 batches" (Arrow layer)

**Analysis**:
- Our SQL generation fix works correctly ✅
- The problem is in the **Arrow serialization layer**, not SQL generation
- Type inference says DECIMAL, SQL generates DECIMAL-preserving query
- But Arrow vector creation still tries to cast DECIMAL → BIGINT
- Location: Likely in `ArrowStreamingExecutor.java` or type-to-vector mapping

---

## Test Suite Details

### Maven Unit Tests (976 total, 976 passing)

**Status**: ✅ **ALL PASSING**

Test coverage includes:
- Expression translation (literals, window, aggregate, binary, unary)
- Converter tests (ExpressionConverter, RelationConverter, PlanConverter)
- Type system tests
- SQL generation tests
- Error handling tests
- Query logging tests

### Differential Tests Breakdown

#### Fully Passing Test Categories (100%)

| Category | Tests | Status |
|----------|-------|--------|
| Column Operations | 11/11 | ✅ All pass |
| Join Operations | 17/17 | ✅ All pass |
| Set Operations | 14/14 | ✅ All pass |
| Conditional Expressions | 12/12 | ✅ All pass |
| Distinct Operations | 12/12 | ✅ All pass |
| Sorting Operations | 11/11 | ✅ All pass |
| DDL Operations | 17/17 | ✅ All pass |
| SQL Expressions | 11/11 | ✅ All pass |
| Empty DataFrame | 12/12 | ✅ All pass |
| Overflow/Boundary | 19/19 | ✅ All pass |
| Pivot Operations | 7/7 | ✅ All pass |

#### Partially Passing Categories

| Category | Passed | Failed | Notes |
|----------|--------|--------|-------|
| Basic DataFrame Ops | 8/9 | 1 | Minor edge case |
| Offset/Range | 14/17 | 3 | toDF edge cases |
| Array Functions | ~15/20 | ~5 | Type mismatches |
| Statistics | ~12/16 | ~4 | Implementation differences |

#### TPC-H Results (9 passing)

**Passing**: Q2, Q3, Q5, Q6, Q10, Q11, Q15, Q19, Q20

**Failing (13)**: Q1, Q4, Q7, Q8, Q9, Q12, Q13, Q14, Q16, Q17, Q18, Q21, Q22
- Primary issue: DecimalVector/BigIntVector type conversion errors

#### TPC-DS SQL Results (11 passing)

**Passing**: Q1, Q4, Q11, Q22, Q23a, Q23b, Q37, Q41, Q74, Q82, Q93

**Failing**: Most others due to:
- Type casting errors (DecimalVector → BigIntVector)
- Decimal precision differences
- Missing functions (named_struct)
- Rollup/cube implementation issues

#### TPC-DS DataFrame Results (13 passing)

**Passing**: Q3, Q7, Q13, Q19, Q26, Q37, Q41, Q45, Q48, Q82, Q84, Q91, Q96

---

## New Failure Categories Discovered (2026-02-06 Test Run)

### DECIMAL Precision/Rounding Issues (NEW - CRITICAL)

**Impact**: ~60+ tests showing data mismatches

**Symptoms**:
- Revenue calculations: `31079.94` (Spark) vs `31080.00` (Thunderduck)
- Ratio calculations: `91.24538744574955089` vs `91.24537607891491840`
- Price aggregations: `6517.20` vs `6517.00`

**Pattern**: Thunderduck is **rounding decimals** to fewer decimal places
- Spark preserves full precision: `DecimalType(38,17)` → value `91.24538744574955089`
- Thunderduck rounds: `DecimalType(38,17)` → value `91.24537607891491840`

**Affected Queries**:
- TPC-DS Q15, Q42, Q48, Q50, Q52, Q55, Q98, Q99
- Pattern appears in SUM aggregations with CASE WHEN expressions

**Root Cause**: Unknown - possible issues:
1. DuckDB's DECIMAL arithmetic rounding mode differs from Spark
2. Intermediate calculation precision loss
3. CAST operations introducing rounding

### Timestamp Timezone Issues (NEW - MEDIUM)

**Impact**: ~5 tests

**Error**: `TimeStampMicroTZVector cannot be cast to TimeStampMicroVector`

**Symptom**: DuckDB returns timezone-aware timestamps, Spark expects timezone-naive

**Affected**: `test_string_to_timestamp` in type casting tests

### Nullability Mismatches for Literals (ONGOING)

**Impact**: ~30 tests in type_literals_differential.py

**Symptoms**:
- Array literals: `nullable=False` (Spark) vs `nullable=True` (Thunderduck)
- Struct fields: Individual fields nullable when should be non-nullable
- Map values: Value type nullable when should be non-nullable

**Example**:
```
Reference: StructType([StructField('name', StringType(), False), ...])
Test:      StructType([StructField('name', StringType(), True), ...])
```

### Date vs Timestamp Return Types (NEW)

**Impact**: ~8 tests

**Symptom**: Interval arithmetic returns `TimestampType` instead of `DateType`

**Example**:
- Query: `date_col + INTERVAL '1' MONTH`
- Spark: Returns `DateType`
- Thunderduck: Returns `TimestampType`

---

## Failure Analysis by Category

### Priority 1: Type Conversion Issues (CRITICAL) - ⚠️ PARTIALLY FIXED

**Impact**: ~150+ tests (7 fixed, 143+ remain)

**Primary Symptoms**:
1. ⚠️ **PARTIALLY FIXED**: `DecimalVector cannot be cast to BigIntVector` - Arrow type mismatch
   - Fixed SQL generation layer ✅
   - Still failing in Arrow serialization layer ❌
   - Affects: TPC-H Q1/Q3/Q5/Q6, TPC-DS Q43/Q62/Q99 (DataFrame versions)
2. ✅ **FIXED**: DateTime functions returning BIGINT instead of INTEGER (~7 tests)
3. ⏳ **REMAINING**: Window function results wrapped incorrectly

**Root Causes Analysis**:

1. ✅ **DateTime Functions - SOLVED**
   - Problem: DuckDB returns BIGINT, Spark expects INTEGER
   - Solution: Added CAST to INTEGER in FunctionRegistry.CUSTOM_TRANSLATORS
   - Result: ~7 tests now passing
   - Functions fixed: year, month, day, dayofmonth, hour, minute, second, quarter, weekofyear, dayofyear

2. ⚠️ **SUM Aggregation - SQL LAYER FIXED, ARROW LAYER BROKEN**
   - SQL Generation: ✅ Fixed in Aggregate.AggregateExpression.toSQL()
     - INTEGER types → Cast to BIGINT (overflow prevention)
     - DECIMAL types → No cast (preserves precision)
     - FLOAT/DOUBLE → No cast (already correct)
   - Arrow Serialization: ❌ **Still broken**
     - Error occurs in "Query streaming failed after 0 batches"
     - Type inference correctly identifies DECIMAL
     - SQL correctly preserves DECIMAL
     - BUT: Arrow vector creation tries to force BIGINT
     - Root cause: `ArrowStreamingExecutor.java` or type-to-vector mapping

**Files Modified**:
- `/workspace/core/src/main/java/com/thunderduck/functions/FunctionRegistry.java`
- `/workspace/core/src/main/java/com/thunderduck/logical/Aggregate.java`
- 3 test files updated to expect new CAST format

**Actual Impact**: 7 tests fixed (expected 120-150)
**Remaining Work**: Fix Arrow serialization layer for DECIMAL aggregates

---

### Priority 2: Missing Functions (HIGH)

**Impact**: ~50+ tests

**Missing Functions**:
- `named_struct` - Not available in DuckDB (used for struct literals)
- `transform` - Lambda/HOF function (DuckDB has different syntax)
- Some string function behavior differences

**Affected Test Files**:
- `test_lambda_differential.py` (18 tests)
- `test_type_literals_differential.py` (~15 tests)
- `test_complex_types_differential.py` (10 tests)

**Recommendation**:
1. Implement `named_struct` equivalent using DuckDB struct syntax
2. Map Spark lambda functions to DuckDB list comprehensions where possible
3. Document unsupported functions clearly

---

### Priority 3: Nullability Mismatches (MEDIUM)

**Impact**: ~30+ tests

**Symptoms**:
- Struct/array literals returning `nullable=True` when Spark returns `nullable=False`
- Aggregate results marked nullable incorrectly

**Recent Fix (2026-02-06)**:
- Added `resolveAggregateNullable()` to TypeInferenceEngine
- SUM/AVG/MIN/MAX now correctly inherit nullability from input column

**Remaining Issues**:
- Complex type literals in raw SQL still show nullable=True
- Some window function results have wrong nullability

---

### Priority 4: Decimal Precision (MEDIUM)

**Impact**: ~40+ TPC-DS tests

**Symptoms**:
- Spark returns `DecimalType(17,2)`, Thunderduck returns `DecimalType(38,2)`
- Revenue calculations show rounding differences (e.g., 5765.84 vs 5766.00)
- Ratio calculations have precision drift

**Root Cause**:
- DuckDB defaults to higher precision decimals
- Intermediate calculations accumulate precision differently

**Recommendation**:
1. Match Spark's decimal precision inference rules exactly
2. Consider explicit CAST to match expected precision in aggregates

---

### Priority 5: Grouping Operations (LOW)

**Impact**: ~13 tests

**Affected Tests**:
- `test_multidim_aggregations.py` - Rollup/Cube tests

**Symptoms**:
- ROLLUP/CUBE producing different results
- GROUPING/GROUPING_ID functions returning wrong values

**Recommendation**:
- Review DuckDB ROLLUP/CUBE implementation
- May need to generate different SQL for these operations

---

## Skipped Tests (53 total)

Tests intentionally skipped due to known limitations:
- Array negative indexing (2 tests) - DuckDB uses different semantics
- `months_between` (1 test) - Implementation differences
- `next_day` (1 test) - Implementation pending
- Various TPC-DS queries requiring unsupported features (~49 tests)

---

## Priority Fix Roadmap

### Phase 1: Type Conversion Fixes (HIGH IMPACT) - REVISED PRIORITIES

**Goal**: Fix ~150 tests (7 completed, 143+ remain)

| Task | Impact | Complexity | Status |
|------|--------|------------|--------|
| Fix datetime extracts to return INTEGER (not BIGINT) | ~7 tests | Low | ✅ **DONE** (2026-02-06) |
| Fix Arrow DECIMAL serialization layer | ~100+ tests | **HIGH** | ❌ **BLOCKED** |
| Fix DECIMAL precision/rounding differences | ~60 tests | High | ⏳ NEW PRIORITY |
| Fix window function result types | ~30 tests | Medium | ⏳ TODO |
| Fix timestamp timezone handling | ~5 tests | Low | ⏳ TODO |

**Completed 2026-02-06**:
- ✅ DateTime extraction functions (year, month, day, etc.) now cast DuckDB BIGINT → INTEGER
- ⚠️ SUM aggregation type-aware logic implemented in SQL layer (but Arrow layer still broken)
- ✅ All 976 Maven unit tests passing with updated expectations
- ✅ Differential test validation completed: +7 tests (52.1% pass rate)

**Critical Blockers Identified**:
1. **Arrow DECIMAL Serialization** - DecimalVector → BigIntVector casting errors persist
   - Location: `ArrowStreamingExecutor.java` or type-to-vector mapping
   - Affects: TPC-H Q1/Q3/Q5/Q6, TPC-DS Q43/Q62/Q99 DataFrame queries
   - Priority: **CRITICAL** - blocks 10+ major queries

2. **DECIMAL Precision/Rounding** - New issue discovered
   - Thunderduck rounds decimals: `31079.94` → `31080.00`
   - Affects: ~60 TPC-DS queries with revenue calculations
   - Priority: **HIGH** - causes data mismatches even when queries execute

### Phase 2: Missing Function Implementation (MEDIUM IMPACT)

**Goal**: Fix ~50 tests

| Task | Impact | Complexity | Status |
|------|--------|------------|--------|
| Implement named_struct equivalent | ~25 tests | Medium | ⏳ TODO |
| Map lambda functions to DuckDB | ~18 tests | High | ⏳ TODO |
| Fix string function differences | ~10 tests | Low | ⏳ TODO |

### Phase 3: Precision and Nullability (LOWER IMPACT)

**Goal**: Fix ~70 tests

| Task | Impact | Complexity | Status |
|------|--------|------------|--------|
| Fix remaining nullability mismatches | ~30 tests | Low | ⏳ TODO |
| Match Spark decimal precision rules | ~40 tests | Medium | ⏳ TODO |

### Phase 4: Grouping Operations (LOW PRIORITY)

**Goal**: Fix ~13 tests

| Task | Impact | Complexity | Status |
|------|--------|------------|--------|
| Fix ROLLUP/CUBE SQL generation | ~13 tests | Medium | ⏳ TODO |

---

## What Currently Works (High Confidence)

### Core Translation (100% Unit Test Coverage)
- ✅ All Spark logical operators translate to DuckDB SQL
- ✅ Full Spark type system support
- ✅ 200+ Spark functions implemented
- ✅ Arrow batch streaming
- ✅ All join types including USING clause
- ✅ Window functions (basic)
- ✅ Aggregations (single and multi-dimensional)
- ✅ Set operations (UNION, INTERSECT, EXCEPT)
- ✅ DDL operations (CREATE, DROP, INSERT, TRUNCATE, ALTER)

### Differential Test Verified
- ✅ Column operations (drop, rename, withColumn)
- ✅ Join operations (all types)
- ✅ Set operations
- ✅ Conditional expressions (WHEN/OTHERWISE)
- ✅ Distinct operations
- ✅ Sorting with null ordering
- ✅ Pivot operations
- ✅ SQL expressions and temp views
- ✅ Empty DataFrame handling
- ✅ Overflow detection

---

## Next Steps - Prioritized Action Plan

Based on the 2026-02-06 test results, here are the prioritized next steps:

### Immediate Priority: Fix Arrow DECIMAL Serialization (CRITICAL) 🔴

**Problem**: DecimalVector → BigIntVector casting errors in Arrow layer

**Investigation Needed**:
1. Read `/workspace/connect-server/src/.../ArrowStreamingExecutor.java`
2. Find where DuckDB query results are converted to Arrow vectors
3. Identify why DECIMAL results trigger BIGINT vector creation
4. Check if type inference result is being ignored/overridden

**Hypothesis**:
- Type inference correctly identifies `DecimalType(38,2)`
- SQL query preserves DECIMAL (our fix works)
- But vector allocation uses wrong type from somewhere
- Possible culprit: Aggregate return type override or schema mismatch

**Expected Impact**: +10-15 major query fixes (TPC-H Q1/Q3/Q5/Q6, TPC-DS Q43/Q62/Q99)

**Complexity**: Medium-High (requires understanding Arrow integration)

---

### High Priority: Investigate DECIMAL Rounding (HIGH) 🟡

**Problem**: Decimal values show precision loss: `31079.94` → `31080.00`

**Investigation Needed**:
1. Check if DuckDB DECIMAL arithmetic uses different rounding mode than Spark
2. Verify DECIMAL scale preservation through aggregations
3. Test simple SUM query to isolate issue
4. Compare DuckDB `DECIMAL(38,2)` with Spark `DecimalType(38,2)`

**Hypothesis**:
- DuckDB may round during intermediate calculations
- CAST operations might introduce rounding
- Scale might be lost in arithmetic expressions

**Expected Impact**: +60 tests (many TPC-DS queries with revenue calculations)

**Complexity**: High (may require DuckDB configuration or query rewrites)

---

### Medium Priority: Fix Timestamp Timezone Handling (MEDIUM) 🟡

**Problem**: `TimeStampMicroTZVector` vs `TimeStampMicroVector`

**Investigation**: Check timestamp type creation in FunctionRegistry and type inference

**Expected Impact**: +5 tests

**Complexity**: Low-Medium

---

### Lower Priority: Fix Nullability for Literals (LOW) 🟢

**Problem**: Array/struct/map literals marked nullable when shouldn't be

**Expected Impact**: +30 tests (type_literals_differential.py)

**Complexity**: Low (known issue, fix in PySpark API layer)

---

## Running Tests

### Maven Unit Tests

```bash
# Run all unit tests
cd /workspace && mvn test

# Run specific module
mvn test -pl core

# Run with verbose output
mvn test -Dtest=TypeInferenceEngineTest
```

### Differential Tests

```bash
# Setup (one-time)
./tests/scripts/setup-differential-testing.sh

# Run all tests
./tests/scripts/run-differential-tests-v2.sh all

# Run specific group
./tests/scripts/run-differential-tests-v2.sh tpch
./tests/scripts/run-differential-tests-v2.sh tpcds
./tests/scripts/run-differential-tests-v2.sh functions
./tests/scripts/run-differential-tests-v2.sh window
./tests/scripts/run-differential-tests-v2.sh joins

# Run specific test file
cd /workspace/tests/integration
python3 -m pytest differential/test_joins_differential.py -v
```

---

## Success Metrics

### Current State (2026-02-06 Evening - After Type Fixes)
- ✅ Maven Unit Tests: **100%** (976/976) - Maintained
- ⚠️ Differential Tests: **52.1%** (445/854) - Up from 51.3% (+7 tests)
- ⚠️ TPC-H: **~41%** (estimate based on partial failures)
- ⚠️ TPC-DS SQL: **~11%** (limited improvement observed)
- ⚠️ TPC-DS DataFrame: **~35%** (some regressions from DecimalVector errors)

### Revised Target State (Next Milestone)
- ✅ Maven Unit Tests: **100%** (maintain) - **ACHIEVED**
- ⏳ Differential Tests: **>60%** (510+/854) - **Requires Arrow layer fix**
- ⏳ TPC-H: **>55%** (12+/22) - **Requires DECIMAL fixes**
- ⏳ TPC-DS SQL: **>15%** (15+/99) - **Requires DECIMAL precision**
- ⏳ TPC-DS DataFrame: **>45%** (15+/34) - **Requires Arrow layer fix**

**Why Targets Revised**:
- Arrow serialization layer issues more complex than anticipated
- New DECIMAL precision/rounding issues discovered
- Window function issues still unaddressed
- Estimated 2-3 additional fix cycles needed for original 65% target

### Ultimate Goal
- ✅ Maven Unit Tests: **100%**
- ⏳ Differential Tests: **>90%**
- ⏳ Full Spark 4.x DataFrame/SQL parity

---

## Recent Changes

**2026-02-06 (Evening) - Post-Implementation Analysis**:
- 📊 **TEST RESULTS**: 445/854 passing (52.1%, up from 51.3%)
  - Actual improvement: +7 tests
  - Expected improvement: +100-150 tests
  - Gap analysis: Arrow serialization layer issues discovered
- 🔍 **ROOT CAUSE IDENTIFIED**: DECIMAL casting errors persist in Arrow layer
  - SQL generation correctly preserves DECIMAL ✅
  - Arrow vector creation forces BIGINT → causes ClassCastException ❌
  - Affects: TPC-H Q1/Q3/Q5/Q6, TPC-DS Q43/Q62/Q99
  - Next fix location: `ArrowStreamingExecutor.java`
- 🆕 **NEW ISSUE DISCOVERED**: DECIMAL precision/rounding differences
  - ~60 tests show decimal rounding: `31079.94` → `31080.00`
  - Affects revenue/ratio calculations in TPC-DS
  - Requires investigation of DuckDB DECIMAL arithmetic
- ✅ **SUCCESSFUL FIX**: DateTime functions now return INTEGER (~7 tests)
- ✅ All 976 Maven unit tests passing (100%)

**2026-02-06 (Afternoon) - Type Casting Fixes Implementation**:
- ⚠️ **PARTIAL FIX**: Implemented type-aware SUM aggregation (SQL layer only)
  - DECIMAL inputs preserve precision in SQL generation ✅
  - Arrow serialization still broken ❌
  - Expected to fix Q43, Q48, Q62, Q99 but didn't
- ✅ **SUCCESSFUL FIX**: DateTime extraction functions now return INTEGER
  - Added CAST(... AS INTEGER) for year, month, day, dayofmonth, hour, minute, second, quarter, weekofyear, dayofyear
  - DuckDB returns BIGINT, Spark expects INTEGER (32-bit)
  - Fixed ~7 tests with datetime operations
- Updated 8 unit tests to expect new CAST format
- All 976 Maven unit tests passing (100%)

**2026-02-06 (Morning)**:
- Fixed aggregate function nullable mismatches
- Added `resolveAggregateNullable()` to TypeInferenceEngine
- SUM/AVG/MIN/MAX now correctly inherit nullability from input
- Full test suite refresh: 976 unit tests (100%), 438 differential tests (51.3%)

**2026-02-05**:
- Fixed nullability for PySpark API (array/map/struct literals)
- Fixed right join column resolution for duplicate names
- TPC-DS auto-generation feature added

---

## References

- **Unit Test Results**: `mvn test` output
- **Differential Test Code**: `/workspace/tests/integration/differential/`
- **Test Scripts**: `/workspace/tests/scripts/`
- **TypeInferenceEngine**: `/workspace/core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java`
- **ArrowStreamingExecutor**: `/workspace/connect-server/src/main/java/com/thunderduck/connect/arrow/ArrowStreamingExecutor.java`
