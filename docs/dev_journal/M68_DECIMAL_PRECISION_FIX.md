# M68: Fix Decimal Precision Mismatch

**Date:** 2025-12-22
**Status:** Complete

## Summary

Fixed decimal precision mismatch in `test_complex_chain` where Spark returns `DecimalType(31,4)` but Thunderduck was returning `DecimalType(18,4)` for `l_extendedprice * l_quantity`.

## Problem

When computing `col("l_extendedprice") * col("l_quantity")`:
- Both columns are `DECIMAL(15, 2)` in the parquet file
- Spark's decimal multiplication rule: `precision = p1 + p2 + 1 = 31`, `scale = s1 + s2 = 4`
- Expected result type: `DecimalType(31, 4)`
- Actual Thunderduck result: `DecimalType(18, 4)` (DuckDB's default)

### Root Cause

Two issues discovered:

1. **Schema Merging**: `SparkConnectServiceImpl.mergeSchemas()` always used DuckDB's inferred type instead of the logical plan's computed type, which was correct for Spark's decimal rules.

2. **Integer Multiplication**: `TypeInferenceEngine.resolveBinaryExpressionType()` was converting ALL multiplication to decimal arithmetic, even `IntegerType * IntegerType`, which should stay as `IntegerType`.

## Solution

### Fix 1: Schema Merging (`SparkConnectServiceImpl.java`)

Modified `mergeSchemas()` to prefer the logical plan's type for DecimalType fields:

```java
// For DecimalType: prefer logical plan's type (computed using Spark rules)
// For other types: use DuckDB's type (handles aggregates, etc.)
if (logicalField.dataType() instanceof DecimalType) {
    finalType = logicalField.dataType();
} else {
    finalType = duckField.dataType();
}
```

### Fix 2: Integer Multiplication (`TypeInferenceEngine.java`)

Modified multiplication type inference to only apply decimal rules when at least one operand is already a DecimalType:

```java
if (op == BinaryExpression.Operator.MULTIPLY) {
    boolean leftIsDecimal = leftType instanceof DecimalType;
    boolean rightIsDecimal = rightType instanceof DecimalType;

    // Only apply decimal rules if at least one operand is already decimal
    if (leftIsDecimal || rightIsDecimal) {
        DecimalType leftDec = toDecimalType(leftType, binExpr.left());
        DecimalType rightDec = toDecimalType(rightType, binExpr.right());
        if (leftDec != null && rightDec != null) {
            return promoteDecimalMultiplication(leftDec, rightDec);
        }
    }
    // Otherwise fall through to numeric type promotion
}
```

## Files Modified

1. `/workspace/connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`
   - `mergeSchemas()`: For DecimalType, use logical plan's type

2. `/workspace/core/src/main/java/com/thunderduck/types/TypeInferenceEngine.java`
   - `resolveBinaryExpressionType()`: Only promote to decimal when at least one operand is decimal

## Test Results

All 25 DataFrame ops differential tests pass:
- `test_complex_chain` - Fixed (decimal precision now matches Spark)
- `test_with_column_new` - Fixed (integer multiplication stays integer)

## Key Insights

1. **Spark's Decimal Rules**: Multiplication precision = p1 + p2 + 1, scale = s1 + s2
2. **DuckDB's Decimal Rules**: Different from Spark, uses its own defaults
3. **Type Merging Strategy**: Use logical plan's DecimalType (Spark rules), DuckDB's type for others (aggregates)
4. **Integer Arithmetic**: Don't promote to decimal unless at least one operand is decimal

## Lessons Learned

- Schema merging between DuckDB execution results and logical plan inference requires careful type-specific handling
- Decimal arithmetic rules differ significantly between Spark and DuckDB
- Integer arithmetic should not be promoted to decimal unless explicitly involving decimal types
