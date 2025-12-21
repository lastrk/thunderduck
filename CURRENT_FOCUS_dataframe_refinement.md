# Current Focus: DataFrame API Refinement and Cleanup

## Status: In Progress

## Overview

With SparkSQL pass-through removed, focus shifts to refining the DataFrame API implementation:
1. Remove accumulated cruft from iterative test-fixing
2. Establish principled schema inference and nullability handling
3. Fix remaining DataFrame test failures
4. Update unit tests for consistency

---

## Priority 1: Refactoring - COMPLETED

### TypeInferenceEngine Created

`TypeInferenceEngine.java` (598 lines) consolidates type inference logic:
- `resolveType(expr, schema)` - Main type resolution
- `resolveNullable(expr, schema)` - Nullability resolution
- `promoteNumericTypes(left, right)` - Numeric type promotion
- `promoteDecimalDivision(dividend, divisor)` - Decimal division per Spark rules
- `resolveAggregateReturnType(function, argType)` - Aggregate type inference

All logical plan nodes (WithColumns, Project, Aggregate) now delegate to this engine.

---

## Priority 2: Type Preservation Fixes

### Decimal Division - FIXED ✓

**Commits**:
- `dc59d72` - Initial Decimal division type preservation
- (pending) - Fix Q98 Decimal scale calculation

**What was fixed**:
- Added `promoteDecimalDivision()` using Spark's two-step formula with precision loss adjustment
- Added `promoteDecimalMultiplication()` for proper Decimal * Integer arithmetic
- Division of Decimal/Decimal now returns correct Decimal type
- Added CAST wrapper in SQL generation for divisions to force Spark-compatible types
- **Q98 now passes**

**Technical details**:
- Spark's division formula has two steps:
  1. Calculate initial: `scale = max(6, s1 + p2 + 1)`, `precision = p1 - s1 + s2 + scale`
  2. If precision > 38, apply precision loss adjustment: `scale = max(6, 38 - intDigits)`
- Integer literals promoted to Decimal based on actual value (100 → Decimal(3,0))
- CAST wrapper added in SQLGenerator.visitWithColumns() for division expressions

### CASE WHEN Type Preservation - FIXED ✓

**Changes made**:
- Created `CaseWhenExpression` class to preserve branch structure for schema-aware type resolution
- Updated `ExpressionConverter.convertCaseWhen()` to return `CaseWhenExpression` instead of `RawSQLExpression`
- Added `TypeInferenceEngine.resolveCaseWhenType()` for schema-aware type resolution
- Fixed Decimal type unification in `promoteNumericTypes()`:
  - Integer → Decimal(10,0) for unification
  - Proper LUB (Least Upper Bound) calculation: precision = max(intDigits1, intDigits2) + scale
- Fixed GROUP BY alias SQL generation in both `Aggregate.toSQL()` and `SQLGenerator.visitAggregate()`

**Result**:
- Q99 now passes! ✓
- Q62 now passes! ✓
- All other CASE WHEN queries work correctly with Decimal type preservation

---

## Priority 3: Differential Test Results (2025-12-21)

### Summary (Updated 2025-12-21)
- **TPC-DS DataFrame**: 24 passed, 0 failed ✓
- **SQL Tests**: 203 skipped (not implemented yet)

### TPC-DS DataFrame Status

| Query | Status | Issue |
|-------|--------|-------|
| Q3, Q7, Q12, Q13, Q15, Q19, Q20, Q26, Q37, Q41, Q42, Q43, Q45, Q48, Q50, Q52, Q55, **Q62**, Q82, **Q84**, Q91, Q96, **Q98**, **Q99** | PASS | - |

### Fixed Issues

1. ~~**Decimal precision/scale** (Q98)~~ - **FIXED**

2. ~~**CASE WHEN type inference** (Q99, Q62)~~ - **FIXED**
   - Created `CaseWhenExpression` class
   - Added schema-aware type resolution in `TypeInferenceEngine`
   - Fixed Decimal + Integer type unification

3. ~~**Q84 nullable mismatch** (customer_name)~~ - **FIXED**
   - `concat_ws` nullability now correctly depends only on separator argument
   - Added special case in `ExpressionConverter.inferFunctionNullable()`

4. **Remaining issues**
   - SQL tests not yet implemented

---

## Priority 4: Next Steps

### Completed
1. ~~**Fix Decimal division scale**~~ - **DONE** (Q98 passes)
2. ~~**Schema-aware CASE WHEN**~~ - **DONE** (Q99, Q62 pass)
3. ~~**Fix Q84 concat_ws nullability**~~ - **DONE** (Q84 passes, all 24 TPC-DS DataFrame tests pass)
4. ~~**Pivot operations**~~ - **DONE** (all 6 pivot tests pass)

### Remaining
- SQL test implementation
- Unpivot/cube/rollup nullable fixes (same issue as pivot, needs schema-aware SQLRelation)

---

## Priority 5: RawSQLExpression Elimination - COMPLETED ✓

### Problem
`RawSQLExpression` was used as a catch-all for expressions without typed representations, causing:
- Type information loss (TypeInferenceEngine defaults to StringType)
- Nullable analysis failure (always returns true)
- No schema-aware resolution

### Solution: Created Typed Expression Classes

| Expression Class | Replaces | Purpose |
|-----------------|----------|---------|
| `ArrayLiteralExpression` | Array literals | Preserves element list for type inference |
| `MapLiteralExpression` | Map literals | Preserves key/value lists for type inference |
| `StructLiteralExpression` | Struct literals | Preserves field names/values for type inference |
| `InExpression` | ISIN function | Preserves test expr and values, returns BooleanType |
| `IntervalExpression` | Interval literals | Preserves interval components for SQL generation |

### RawSQLExpression Completely Eliminated (2025-12-21)

`RawSQLExpression.java` has been **deleted** from the codebase:
- `convertExpressionString()` now throws `UnsupportedOperationException` with clear error message
- Reasoning: Thunderduck explicitly does NOT support Spark SQL (`spark.sql()`, `expr()`, `selectExpr()`)
- When Spark SQL is added, a proper SQL parser will generate typed AST nodes - RawSQLExpression would still not be needed
- Users attempting to use SQL expression strings receive clear guidance to use DataFrame API instead

---

## Files Modified (All Sessions)

| File | Changes |
|------|---------|
| `core/.../types/TypeInferenceEngine.java` | Added `promoteDecimalDivision()`, `promoteDecimalMultiplication()`, `resolveCaseWhenType()`, `unifyTypes()` (public), `toDecimalForUnification()`, `unifyDecimalTypes()`, `resolveArrayLiteralType()`, `resolveMapLiteralType()`, `resolveStructLiteralType()` |
| `core/.../expression/CaseWhenExpression.java` | NEW - Expression class preserving CASE WHEN branch structure |
| `core/.../expression/ArrayLiteralExpression.java` | NEW - Array literal with element list |
| `core/.../expression/MapLiteralExpression.java` | NEW - Map literal with key/value lists |
| `core/.../expression/StructLiteralExpression.java` | NEW - Struct literal with field names/values |
| `core/.../expression/InExpression.java` | NEW - IN clause with test expr and values |
| `core/.../expression/IntervalExpression.java` | NEW - Interval literals (YEAR_MONTH, DAY_TIME, CALENDAR) |
| `core/.../expression/RawSQLExpression.java` | DELETED - No longer needed |
| `core/.../generator/SQLGenerator.java` | Added CAST wrapper for decimal divisions, fixed GROUP BY alias unwrapping |
| `core/.../logical/Aggregate.java` | Fixed GROUP BY alias unwrapping |
| `connect-server/.../ExpressionConverter.java` | Updated to use typed expression classes |
| `tests/.../types/TypeInferenceEngineTest.java` | 15 unit tests |
| `tests/.../expression/CaseWhenExpressionTest.java` | NEW - 19 unit tests |
| `tests/.../expression/RawSQLExpressionTest.java` | DELETED - No longer needed |
| `tests/.../expression/ArrayLiteralExpressionTest.java` | NEW - 14 unit tests |
| `tests/.../expression/MapLiteralExpressionTest.java` | NEW - 13 unit tests |
| `tests/.../expression/StructLiteralExpressionTest.java` | NEW - 11 unit tests |
| `tests/.../expression/InExpressionTest.java` | NEW - 14 unit tests |
| `tests/.../expression/IntervalExpressionTest.java` | NEW - 21 unit tests |

---

## Architecture Notes

### Type Resolution Flow

```
Spark Connect Protocol
    ↓
ExpressionConverter (no schema access)
    ↓
Logical Plan Nodes (WithColumns, Project, Aggregate)
    ↓
TypeInferenceEngine.resolveType(expr, schema) ← Schema available here
    ↓
Schema Inference
```

### CASE WHEN Solution (FIXED)

```
F.when(condition, column_ref).otherwise(0)
    ↓
ExpressionConverter.convertCaseWhen()
    ↓
CaseWhenExpression(conditions, thenBranches, elseBranch)  ← Preserves structure
    ↓
TypeInferenceEngine.resolveCaseWhenType(caseWhen, schema)  ← Schema-aware
    ↓
Branch type resolution: cs_sales_price → Decimal(7,2)
    ↓
Type unification: Decimal(7,2) + Integer → Decimal(12,2)
    ↓
SUM(Decimal(12,2)) → Decimal(22,2)  ← Correct!
```
