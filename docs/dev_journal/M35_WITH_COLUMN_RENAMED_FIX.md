# M35: WithColumnRenamed PySpark 4.0 Compatibility Fix

**Date:** 2025-12-16
**Status:** Complete

## Summary

Fixed `df.withColumnRenamed("old", "new")` to work with PySpark 4.0+. The operation was silently doing nothing because the server code only checked the deprecated `rename_columns_map` protobuf field, while PySpark 4.0 sends renames via the new `renames` list field.

## Problem

When calling `df.withColumnRenamed("n_name", "nation_name")`, the column was NOT being renamed. The server logs showed:

```
WithColumnsRenamed: no columns to rename, returning input as-is
```

This happened because:
1. PySpark 4.0 changed the protobuf format for `WithColumnsRenamed`
2. The old format used `rename_columns_map` (field 2) - now deprecated
3. The new format uses `renames` (field 3) - a repeated `Rename` message

## Root Cause

In `RelationConverter.java`, the `convertWithColumnsRenamed()` method only checked the deprecated map:

```java
// OLD CODE - only checked deprecated field
Map<String, String> renameMap = withColumnsRenamed.getRenameColumnsMapMap();
if (renameMap.isEmpty()) {
    logger.debug("WithColumnsRenamed: no columns to rename, returning input as-is");
    return input;  // BUG: missed the new renames field!
}
```

The protobuf definition in `relations.proto` shows both fields:

```protobuf
message WithColumnsRenamed {
  Relation input = 1;
  map<string, string> rename_columns_map = 2 [deprecated=true];
  repeated Rename renames = 3;  // NEW: PySpark 4.0+ uses this

  message Rename {
    string col_name = 1;
    string new_col_name = 2;
  }
}
```

## Solution

Updated `convertWithColumnsRenamed()` to check both fields for backward compatibility:

```java
private LogicalPlan convertWithColumnsRenamed(WithColumnsRenamed withColumnsRenamed) {
    LogicalPlan input = convert(withColumnsRenamed.getInput());

    // Build rename map from both the deprecated map and the new renames list
    // PySpark 4.0+ uses the renames list, older versions use rename_columns_map
    Map<String, String> renameMap = new java.util.LinkedHashMap<>();

    // Add from deprecated map (for backward compatibility with older clients)
    renameMap.putAll(withColumnsRenamed.getRenameColumnsMapMap());

    // Add from new renames list (PySpark 4.0+)
    for (WithColumnsRenamed.Rename rename : withColumnsRenamed.getRenamesList()) {
        renameMap.put(rename.getColName(), rename.getNewColName());
    }

    if (renameMap.isEmpty()) {
        logger.debug("WithColumnsRenamed: no columns to rename, returning input as-is");
        return input;
    }

    // ... rest of method generates SQL
}
```

## Test Results

**Before fix:**
```python
>>> df.columns
['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
>>> result = df.withColumnRenamed("n_name", "nation_name")
>>> result.columns
['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']  # NOT renamed!
```

**After fix:**
```python
>>> df.columns
['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
>>> result = df.withColumnRenamed("n_name", "nation_name")
>>> result.columns
['n_nationkey', 'n_regionkey', 'n_comment', 'nation_name']  # RENAMED!
```

E2E test `test_with_column_renamed` now passes (previously XFAIL).

## Files Modified

- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java`
  - Updated `convertWithColumnsRenamed()` to check both `getRenameColumnsMapMap()` and `getRenamesList()`

- `tests/integration/test_dataframe_operations.py`
  - Removed `@pytest.mark.xfail` decorator from `test_with_column_renamed`

- `CURRENT_FOCUS_E2E_TEST_GAPS.md`
  - Updated `WithColumnRenamed` status from XFAIL to **PASS**
  - Updated test results count (21 passed, 7 xfailed)

## Key Learnings

1. **Always check protobuf deprecation notes** - When a field is deprecated, there's usually a replacement field that newer clients use
2. **Build backward-compatible handlers** - Support both old and new protobuf fields to work with different client versions
3. **Server logs are invaluable** - The "no columns to rename" log message immediately pointed to the issue

## Build Note

The build produces a deprecation warning for `getRenameColumnsMapMap()`:
```
[WARNING] RelationConverter.java: getRenameColumnsMapMap() in WithColumnsRenamed has been deprecated
```

This is expected - we intentionally call the deprecated method for backward compatibility with older clients. The warning is informational only.
