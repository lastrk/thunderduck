# M52: Type Inference Engine and Spark Type Parity

**Date**: 2025-12-20 to 2025-12-21
**Status**: Complete

## Summary

Major refactoring of the type inference system into a centralized `TypeInferenceEngine`, achieving Spark type parity for window functions, decimal operations, CASE WHEN expressions, and complex types. This work fixed schema mismatches that were causing TPC-DS DataFrame differential test failures.

## Key Changes

### 1. Centralized TypeInferenceEngine

Created `/core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java` - a unified type inference system that consolidates type resolution logic previously scattered across multiple classes:

- **Function return type inference**: Maps function names to their return types
- **Expression type inference**: Resolves types for arithmetic, comparison, and logical expressions
- **Aggregation type inference**: Handles AVG, SUM, COUNT, etc. with proper Decimal handling
- **Window function type inference**: Returns correct types for ranking and analytic functions

### 2. Window Function Type Fixes

Fixed window ranking functions to match Spark's return types:

| Function | Before (Wrong) | After (Correct) |
|----------|----------------|-----------------|
| `rank()` | BIGINT | INTEGER |
| `dense_rank()` | BIGINT | INTEGER |
| `row_number()` | BIGINT | INTEGER |
| `ntile(n)` | BIGINT | INTEGER |
| `percent_rank()` | DOUBLE | DOUBLE (non-nullable) |
| `cume_dist()` | DOUBLE | DOUBLE (non-nullable) |

Added CAST wrapper in SQL generation to convert DuckDB's BIGINT results to INTEGER:
```java
// DuckDB returns BIGINT, Spark expects INTEGER
sql.append("CAST(").append(functionCall).append(" AS INTEGER)");
```

### 3. Decimal Division Parity

Fixed Decimal division to match Spark's precision/scale rules:

**Spark Rule**: `result_scale = max(6, s1 + p2 + 1)`

```java
// Example: Decimal(10,2) / Decimal(10,2)
// Spark: scale = max(6, 2 + 10 + 1) = 13 → Decimal(38,13)
// Before: Thunderduck was using different scale
```

### 4. CASE WHEN Type Inference

Implemented `CaseWhenExpression` class for schema-aware CASE WHEN type inference:

- Analyzes all THEN/ELSE branches to determine result type
- Handles type promotion (e.g., INT + DOUBLE → DOUBLE)
- Supports nullable inference based on branch coverage

### 5. Eliminated RawSQLExpression

Replaced `RawSQLExpression` (untyped SQL strings) with properly typed expression classes:

- `CaseWhenExpression` - CASE WHEN statements
- `AliasedExpression` - Column aliases with type preservation
- `TypedExpression` - Base class for all typed expressions

This ensures schema information is preserved through SQL generation.

### 6. AVG and Complex Type Fixes

- **AVG on Decimal**: Returns `Decimal(38, max(scale+4, 6))` to match Spark
- **ARRAY/MAP/STRUCT**: Added SQL function translations for complex type operations

## Commits Covered

| Commit | Description |
|--------|-------------|
| `6f8ccf0` | Fix schema type resolution for DataFrame API operations |
| `a874b94` | Refactor function type inference with shared constants and metadata |
| `6697f95` | Fix AVG DecimalType and add ARRAY/MAP/STRUCT SQL function translation |
| `3e854b7` | Fix window ranking functions to return IntegerType instead of LongType |
| `0fdeef0` | Fix nullable inference and window function types |
| `0ae795b` | Fix window analytic functions and add ToDF support |
| `1faf2a9` | Refactor type inference into centralized TypeInferenceEngine |
| `856db32` | Fix WindowFunctionTest expectations for CAST wrapper on ranking functions |
| `dc59d72` | Fix Decimal division type preservation for Spark parity |
| `91db75b` | Add CASE WHEN type inference support (partial fix for Q99) |
| `88f0c48` | Fix Q98 Decimal division scale calculation for Spark parity |
| `1ff48be` | Add CaseWhenExpression for schema-aware CASE WHEN type inference |
| `e7353df` | Eliminate RawSQLExpression with typed expression classes |
| `0a34356` | Remove SparkSQL pass-through until SQL parser integration |

## Files Modified

| File | Changes |
|------|---------|
| `core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java` | New - centralized type inference |
| `core/src/main/java/com/thunderduck/expression/CaseWhenExpression.java` | New - typed CASE WHEN |
| `core/src/main/java/com/thunderduck/expression/window/*.java` | Fixed return types |
| `core/src/main/java/com/thunderduck/generator/SQLGenerator.java` | CAST wrapper for ranking functions |
| `core/src/main/java/com/thunderduck/functions/FunctionRegistry.java` | Updated function mappings |

## Test Results

After these changes:
- **TPC-DS DataFrame tests**: 33/33 passing (was ~25 before)
- **Window function tests**: Schema types match Spark exactly
- **Decimal tests**: Precision/scale matches Spark rules

## Key Learnings

1. **Type parity is non-negotiable**: Spark clients expect exact types. Returning BIGINT when INTEGER is expected breaks compatibility even if values are correct.

2. **Centralized type inference**: Having a single `TypeInferenceEngine` makes it easier to maintain consistency and add new functions.

3. **DuckDB vs Spark type differences**: DuckDB often uses larger types (BIGINT for counts/ranks). Explicit CAST is needed for parity.

4. **Decimal arithmetic**: Spark has specific rules for Decimal precision/scale that must be followed exactly.
