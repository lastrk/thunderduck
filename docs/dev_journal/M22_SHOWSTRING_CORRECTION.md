# M22: ShowString Implementation Correction

**Date:** 2025-12-12
**Status:** Documentation fix (no new code)

## Summary

Discovered that ShowString (`df.show()`) was already fully implemented but incorrectly marked as "partial" in the gap analysis. Corrected the documentation and added a fail-fast guard.

## Key Discovery

ShowString implementation exists in `SparkConnectServiceImpl.java`:
- **Detection**: Lines 112-136 check for ShowString at root, extract parameters
- **Execution**: `executeShowString()` method (lines 772-835)
- **Formatting**: `formatAsTextTable()` with horizontal/vertical modes (lines 840-928)
- **Response**: Returns single-column `show_string` Arrow batch with ASCII table

## Why It Was Marked Partial

The `RelationConverter.java` had a passthrough case:
```java
case SHOW_STRING:
    return convert(relation.getShowString().getInput());
```

This looked incomplete, but ShowString is handled at root level in `SparkConnectServiceImpl` before `RelationConverter` is ever called - similar to how `Tail` works.

## Changes Made

### 1. Gap Analysis (v1.6)
- ShowString: Partial → Implemented
- Coverage: 19 relations implemented, 1 partial (SubqueryAlias)

### 2. RelationConverter.java
Replaced silent passthrough with explicit error:
```java
case SHOW_STRING:
    throw new PlanConversionException(
        "ShowString should be handled at root level, not in RelationConverter");
```

This follows the principle: if code shouldn't be reached, make it fail loudly.

## ShowString Protocol Details

From `relations.proto`:
```protobuf
message ShowString {
  Relation input = 1;     // Inner relation to execute
  int32 num_rows = 2;     // Max rows (default: 20)
  int32 truncate = 3;     // Max column width (0 = no truncation)
  bool vertical = 4;      // Vertical format if true
}
```

Response: Single-row DataFrame with column `show_string` containing formatted ASCII table.

## Lessons Learned

1. **Check service layer first**: Action-like operations (ShowString, Tail) are often handled at the service layer before plan conversion
2. **Gap analysis accuracy**: Always verify "partial" claims by tracing actual execution paths
3. **Fail-fast guards**: Replace dead code with explicit errors to catch unexpected states

## References

- Commit: `cc80b3a` - Correct ShowString status, add fail-fast guard
- Files: `SparkConnectServiceImpl.java:112-136, 772-928`, `RelationConverter.java:70-74`
