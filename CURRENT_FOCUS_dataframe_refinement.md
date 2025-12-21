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

### Decimal Division - FIXED

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

### CASE WHEN Type Preservation - FIXED

**Changes made**:
- Created `CaseWhenExpression` class to preserve branch structure for schema-aware type resolution
- Updated `ExpressionConverter.convertCaseWhen()` to return `CaseWhenExpression` instead of `RawSQLExpression`
- Added `TypeInferenceEngine.resolveCaseWhenType()` for schema-aware type resolution
- Fixed Decimal type unification in `promoteNumericTypes()`:
  - Integer → Decimal(10,0) for unification
  - Proper LUB (Least Upper Bound) calculation: precision = max(intDigits1, intDigits2) + scale
- Fixed GROUP BY alias SQL generation in both `Aggregate.toSQL()` and `SQLGenerator.visitAggregate()`

**Result**:
- Q99 now passes!
- Q62 now passes!
- All other CASE WHEN queries work correctly with Decimal type preservation

---

## Priority 3: Join Alias Visibility - FIXED (2025-12-21)

### Problem
TPC-DS queries Q17, Q25, Q29 were failing with SQL binding errors:
- `d1['d_date_sk']` - bracket notation instead of qualified column syntax
- `Referenced table "d1" not found` - aliases buried in nested subqueries

### Solution

1. **Created `AliasedRelation` logical plan node** - Preserves user-provided aliases from `DataFrame.alias()` operations
2. **Fixed `ExpressionConverter.convertUnresolvedAttribute()`** - Two-part column references (`d1.d_date_sk`) now use qualified column syntax instead of struct field access
3. **Modified `Join.java`** - Added `generateJoinSide()` to preserve user-provided aliases
4. **Enhanced `SQLGenerator.java`**:
   - Added `visitAliasedRelation()` handler
   - Added `generateProjectOnFilterJoin()` for flat SQL generation
   - Added `generateFlatJoinChain()` and `collectJoinParts()` to flatten join chains
   - Added `generateJoinSource()` for individual join sources

**Result**: All 33 TPC-DS DataFrame tests now pass (was 24)

---

## Priority 4: SQL Generation Optimization - COMPLETED (2025-12-21)

### Problem
SQL generation was creating excessive nested subqueries even for simple table references:
```sql
SELECT col1, col2 FROM (
    SELECT * FROM (
        SELECT * FROM "users"
    ) AS subquery_1
    WHERE id > 100
) AS subquery_2
```

### Solution
Extended the `getDirectlyAliasableSource()` pattern to 7 visit methods:

| Method | Optimization |
|--------|--------------|
| `visitAliasedRelation()` | Direct table ref: `SELECT * FROM "table" AS alias` |
| `visitFilter()` | Skip subquery for TableScan children |
| `visitProject()` | Direct FROM clause for TableScan children |
| `visitSort()` | Skip subquery for TableScan children |
| `visitLimit()` | Skip subquery for TableScan children |
| `visitDistinct()` | Skip subquery for TableScan children |
| `visitAggregate()` | Direct FROM clause for TableScan children |

**Result**: Generated SQL is cleaner and more readable:
```sql
SELECT col1, col2 FROM "users"
WHERE id > 100
ORDER BY col1
```

---

## Priority 5: Differential Test Results (2025-12-21)

### Summary
- **TPC-DS DataFrame**: 33 passed, 0 failed
- **SQL Tests**: 203 skipped (not implemented yet)

### TPC-DS DataFrame Status - ALL PASSING

| Query | Status |
|-------|--------|
| Q3, Q7, Q9, Q12, Q13, Q15, Q17, Q19, Q20, Q25, Q26, Q29, Q32, Q37, Q40, Q41, Q42, Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q71, Q82, Q84, Q85, Q91, Q92, Q96, Q98, Q99 | PASS |

### Fixed Issues

1. **Decimal precision/scale** (Q98) - FIXED
2. **CASE WHEN type inference** (Q99, Q62) - FIXED
3. **Q84 nullable mismatch** (customer_name) - FIXED
4. **Q17, Q25, Q29 join alias visibility** - FIXED
5. **Q91 ORDER BY tie-breaking** - FIXED (added secondary sort keys)

---

## Priority 6: RawSQLExpression Elimination - COMPLETED

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

## Next Steps

### Completed
1. **Fix Decimal division scale** - DONE (Q98 passes)
2. **Schema-aware CASE WHEN** - DONE (Q99, Q62 pass)
3. **Fix Q84 concat_ws nullability** - DONE (Q84 passes)
4. **Pivot operations** - DONE (all 6 pivot tests pass)
5. **Fix join alias visibility** - DONE (Q17, Q25, Q29 pass)
6. **SQL generation optimization** - DONE (reduced unnecessary subquery wrapping)

### Remaining
- SQL test implementation
- Unpivot/cube/rollup nullable fixes (same issue as pivot, needs schema-aware SQLRelation)

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
| `core/.../logical/AliasedRelation.java` | NEW - Preserves user-provided aliases for joins |
| `core/.../logical/Join.java` | Added `generateJoinSide()` for alias preservation |
| `core/.../generator/SQLGenerator.java` | Added CAST wrapper for decimal divisions, fixed GROUP BY alias unwrapping, added `visitAliasedRelation()`, `generateProjectOnFilterJoin()`, `generateFlatJoinChain()`, direct source optimization for 7 visit methods |
| `core/.../logical/Aggregate.java` | Fixed GROUP BY alias unwrapping |
| `connect-server/.../ExpressionConverter.java` | Updated to use typed expression classes, fixed two-part column references |
| `connect-server/.../RelationConverter.java` | Uses `AliasedRelation` instead of `SQLRelation` for aliases |
| `tests/.../tpcds_dataframe/tpcds_dataframe_queries.py` | Q91 orderBy fix with secondary sort keys |
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

### Join Alias Visibility Solution (FIXED)

```
df.alias("d1").join(df.alias("d2"), col("d1.id") == col("d2.id"))
    ↓
RelationConverter.convertSubqueryAlias()
    ↓
AliasedRelation(child, "d1")  ← Preserves user alias
    ↓
SQLGenerator.visitJoin() / generateFlatJoinChain()
    ↓
SELECT * FROM "table" AS "d1" INNER JOIN "table" AS "d2" ON "d1"."id" = "d2"."id"
```
