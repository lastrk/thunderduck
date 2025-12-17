# M49: ToSchema Operation Implementation

**Date:** 2025-12-17
**Status:** Complete
**Relation Coverage:** 37/40 → 38/40

## Summary

Implemented the `ToSchema` relation operation which reconciles a DataFrame to match a specified schema. This enables `df.to(schema)` in PySpark to work with Thunderduck.

## Implementation Details

### ToSchema Operation

**PySpark API:** `DataFrame.to(schema: StructType) -> DataFrame`

**Proto Definition:** `relations.proto` lines 968-976
```protobuf
message ToSchema {
  Relation input = 1;
  DataType schema = 2;  // StructType
}
```

### Functionality

ToSchema performs three key operations:
1. **Column Reordering** - Columns are selected in target schema order
2. **Column Projection** - Columns not in target schema are dropped
3. **Type Casting** - Columns are CAST to match target data types

### Implementation

Added to `RelationConverter.java`:

**Switch case (line 133-134):**
```java
case TO_SCHEMA:
    return convertToSchema(relation.getToSchema());
```

**convertToSchema() method (lines 1422-1472):**
```java
private LogicalPlan convertToSchema(ToSchema toSchema) {
    LogicalPlan input = convert(toSchema.getInput());

    // Extract target schema fields
    DataType.Struct targetStruct = toSchema.getSchema().getStruct();

    // Build SELECT list with CASTs
    List<String> selectItems = new ArrayList<>();
    for (DataType.StructField field : targetStruct.getFieldsList()) {
        String colName = field.getName();
        String duckdbType = SparkDataTypeConverter.toDuckDBType(field.getDataType());
        selectItems.add(String.format("CAST(%s AS %s) AS %s",
            quoteIdentifier(colName), duckdbType, quoteIdentifier(colName)));
    }

    // Generate SQL
    return new SQLRelation(String.format("SELECT %s FROM (%s)",
        String.join(", ", selectItems), inputSQL));
}
```

### Generated SQL Example

```sql
-- df.to(StructType([StructField("b", StringType), StructField("a", LongType)]))
-- Input has columns [a, b, c]

SELECT CAST("b" AS VARCHAR) AS "b", CAST("a" AS BIGINT) AS "a"
FROM (SELECT * FROM input_table)
```

## Test Results

13 E2E tests created in `tests/integration/test_to_schema.py`:

| Test Class | Tests | Status |
|------------|-------|--------|
| TestToSchemaBasic | 4 | All Pass |
| TestToSchemaTypeCasting | 4 | All Pass |
| TestToSchemaErrors | 1 | Pass |
| TestToSchemaMultipleRows | 2 | All Pass |
| TestToSchemaChaining | 2 | All Pass |

**Total:** 13 passed

## Files Changed

### Modified
- `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java`
  - Added `TO_SCHEMA` case (line 133-134)
  - Added `convertToSchema()` method (lines 1422-1472)

### Created
- `tests/integration/test_to_schema.py` (13 tests)
- `docs/dev_journal/M49_TO_SCHEMA_IMPLEMENTATION.md` (this file)

## Metrics

- **Lines of Code Added:** ~60 (converter) + ~200 (tests)
- **E2E Tests Added:** 13
- **Relation Coverage:** 37/40 → 38/40 (95%)

## Known Limitations

1. **Nullability not validated** - DuckDB doesn't enforce NOT NULL constraints. Spark's strict nullability checking is not replicated.

2. **Nested struct reordering not supported** - Nested struct fields keep their original order. Full nested struct reordering would require STRUCT_PACK generation.

3. **Type compatibility more permissive** - DuckDB allows more casts than Spark (e.g., INT→VARCHAR). This is a superset of Spark's behavior.

## Test Cases

| Test | Description | Expected |
|------|-------------|----------|
| Column reorder | `[a,b,c]` → `[c,a,b]` | Columns reordered |
| Column projection | `[a,b,c,d]` → `[a,c]` | b,d dropped |
| INT → BIGINT | Widening cast | Success |
| BIGINT → INT | Narrowing cast | Success |
| FLOAT ↔ DOUBLE | Precision change | Success |
| Missing column | Target has col not in input | Error |
| Empty DataFrame | 0 rows | Schema changed, 0 rows |
| With nulls | NULL values preserved | Nulls preserved |
| Chaining | filter → to_schema → select | Works |

## Next Steps

Remaining unimplemented relations (2/40):
1. **Parse** - CSV/JSON parsing (low priority)
2. **CollectMetrics** - Metrics collection (low priority)

The spec document `docs/pending_design/TO_SCHEMA_OPERATION_SPEC.md` can now be moved to `docs/architect/`.
