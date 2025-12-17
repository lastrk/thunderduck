# M47: Complex Type Expression Implementation

**Date:** 2025-12-17
**Status:** Complete
**Expression Coverage:** 75% → 94% (15/16 expressions)

## Summary

Implemented UnresolvedExtractValue, UnresolvedRegex, and UpdateFields expressions to enable complex type operations in Thunderduck. This brings expression coverage from 75% to 94%.

## Implementation Details

### New Expression Classes

1. **ExtractValueExpression** (`core/src/main/java/com/thunderduck/expression/ExtractValueExpression.java`)
   - Extracts values from structs, arrays, and maps
   - Three extraction types: STRUCT_FIELD, ARRAY_INDEX, MAP_KEY
   - Handles 0-based to 1-based index conversion for arrays
   - Negative index support: `arr[-1]` → `arr[length(arr)]`
   - Uses bracket notation (`['field']`) for universal struct/map access

2. **RegexColumnExpression** (`core/src/main/java/com/thunderduck/expression/RegexColumnExpression.java`)
   - Represents Spark's `df.colRegex()` for regex column selection
   - Translates to DuckDB's `COLUMNS('pattern')` expression
   - Strips backticks from Spark pattern format

3. **UpdateFieldsExpression** (`core/src/main/java/com/thunderduck/expression/UpdateFieldsExpression.java`)
   - Add/replace struct fields using DuckDB `struct_insert()`
   - Drop field operation has limited support (requires schema introspection)

### ExpressionConverter Changes

Added to `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`:

- **UNRESOLVED_EXTRACT_VALUE case**: Converts struct.field, arr[index], map[key] access
- **UNRESOLVED_REGEX case**: Converts colRegex patterns to DuckDB COLUMNS()
- **UPDATE_FIELDS case**: Converts withField/dropFields to struct_insert

### Key Technical Decisions

#### Index Conversion (Critical)
PySpark uses 0-based indexing, DuckDB uses 1-based:
- Positive: `arr[0]` → `arr[1]` (add 1)
- Negative: `arr[-1]` → `arr[length(arr)]`
- Dynamic: `arr[expr]` → `arr[(expr) + 1]`

#### Universal Bracket Notation
Changed from `struct_extract(struct, 'field')` to `struct['field']` because:
- Works for both structs AND maps
- DuckDB supports this syntax universally
- Avoids type detection issues at conversion time

#### Nested Struct Access
`col("person.address.city")` is converted to chained ExtractValue calls:
```java
ExtractValue(ExtractValue(ExtractValue(person, "address"), "city"))
```

### Known Limitations

1. **withField Replace**: DuckDB `struct_insert` cannot replace existing fields (causes "Duplicate struct entry name" error). Only adding new fields is supported.

2. **dropFields**: Requires schema introspection to rebuild struct without the dropped field. Currently outputs a placeholder comment.

3. **colRegex Table Qualification**: Plan ID resolution for table-qualified regex not yet implemented.

## Test Results

15 E2E tests created in `tests/integration/test_complex_types.py`:

| Test Class | Tests | Status |
|------------|-------|--------|
| TestStructFieldAccess | 3 | All Pass |
| TestArrayIndexing | 4 | All Pass |
| TestMapKeyAccess | 2 | All Pass |
| TestUpdateFields | 2 | All Pass |
| TestChainedExtraction | 2 | All Pass |
| TestMultipleRows | 2 | All Pass |

## Files Changed

### Created
- `core/src/main/java/com/thunderduck/expression/ExtractValueExpression.java`
- `core/src/main/java/com/thunderduck/expression/RegexColumnExpression.java`
- `core/src/main/java/com/thunderduck/expression/UpdateFieldsExpression.java`
- `tests/integration/test_complex_types.py`

### Modified
- `connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`

## Metrics

- **Lines of Code**: ~650 (expressions + converter + tests)
- **Expression Coverage**: 75% → 94%
- **E2E Tests Added**: 15

## Next Steps

Only one expression remains unimplemented:
1. **CommonInlineUserDefinedFunction** - Python/Scala UDF support (future)

Potential enhancements:
- Full dropFields support with schema introspection
- withField replace support via struct reconstruction
- colRegex table qualification support
