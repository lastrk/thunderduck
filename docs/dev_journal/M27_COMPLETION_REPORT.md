# M27 Completion Report: Unpivot Operation

**Date Completed**: 2025-12-12
**Status**: COMPLETE - Unpivot Implemented with Schema Inference and Tests

---

## Executive Summary

M27 implements the Unpivot DataFrame operation (`df.unpivot()`), which transforms data from wide format to long format (inverse of pivot). This is a key data reshaping operation used in data analysis workflows.

**Key Achievement**: Full implementation using DuckDB's native UNPIVOT syntax with schema inference for automatic value column detection.

---

## Delivered Features

### 1. Unpivot Relation Converter

**File**: `connect-server/src/main/java/com/thunderduck/connect/converter/RelationConverter.java`

**Implemented**:
- Added `UNPIVOT` case to the relation switch statement (line 126-127)
- Implemented `convertUnpivot()` method (lines 1143-1250)
- Implemented `extractColumnName()` helper method (lines 1252-1266)

**Spark Connect Protocol Support**:
```protobuf
message Unpivot {
  Relation input = 1;                    // Required: input DataFrame
  repeated Expression ids = 2;           // Required: ID columns to keep
  optional Values values = 3;            // Optional: columns to unpivot
  string variable_column_name = 4;       // Required: name for "variable" column
  string value_column_name = 5;          // Required: name for "value" column
}
```

### 2. SQL Generation

**DuckDB Native UNPIVOT Syntax**:
```sql
SELECT * FROM (input_sql) UNPIVOT (
    value_column FOR variable_column IN (col1, col2, col3)
)
```

**Example Transformation**:
```
Input (wide):  id | q1_sales | q2_sales | q3_sales
               1  | 100      | 150      | 200

Output (long): id | quarter  | sales
               1  | q1_sales | 100
               1  | q2_sales | 150
               1  | q3_sales | 200
```

### 3. Schema Inference for values=None

When the `values` parameter is not specified (None in Python), the implementation automatically infers which columns to unpivot:

- Uses `SchemaInferrer` to query DuckDB schema
- Identifies all columns that are NOT in the `ids` list
- These become the value columns to unpivot

**Example**:
```python
# Explicit values
df.unpivot(["id"], ["col1", "col2"], "variable", "value")

# Auto-infer values (unpivot all non-id columns)
df.unpivot(["id"], None, "variable", "value")
```

---

## Test Coverage

### UnpivotTest (11 tests)

| Test Category | Tests | Description |
|--------------|-------|-------------|
| BasicUnpivotTests | 3 | SQL generation, execution, value correctness |
| SchemaInferenceTests | 2 | Inferred value columns, multiple ID columns |
| ColumnNamingTests | 2 | Custom names, default names |
| SalesDataUnpivotTests | 2 | Quarterly sales, partial column selection |
| EdgeCaseTests | 2 | Single value column, no ID columns |

**Total: 11 new tests, all passing**

**Combined with M26**: 59 tests total (SchemaInferrer: 30, NAFunctions: 18, Unpivot: 11)

---

## Files Changed

### Modified Files
| File | Changes |
|------|---------|
| `connect-server/.../RelationConverter.java` | Added UNPIVOT case and convertUnpivot() method |
| `CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md` | Updated coverage to 27/40 relations (67.5-70%) |

### New Files
| File | Purpose |
|------|---------|
| `tests/src/test/java/com/thunderduck/logical/UnpivotTest.java` | Comprehensive Unpivot unit tests |

---

## Technical Implementation

### 1. Column Name Extraction

The `extractColumnName()` helper extracts column names from Spark Connect Expression messages:

```java
private String extractColumnName(Expression expr) {
    if (expr.hasUnresolvedAttribute()) {
        return expr.getUnresolvedAttribute().getUnparsedIdentifier();
    }
    return null;
}
```

### 2. Schema Inference Integration

When `values` is empty, schema inference is used:

```java
if (valueCols.isEmpty()) {
    StructType schema = schemaInferrer.inferSchema(inputSql);
    for (StructField field : schema.fields()) {
        String colName = field.name();
        if (!idCols.contains(colName)) {
            valueCols.add(colName);
        }
    }
}
```

### 3. SQL Generation

The final SQL uses DuckDB's SQL Standard UNPIVOT syntax:

```java
String sql = String.format(
    "SELECT * FROM (%s) UNPIVOT (%s FOR %s IN (%s))",
    inputSql,
    SQLQuoting.quoteIdentifier(valueColumnName),
    SQLQuoting.quoteIdentifier(variableColumnName),
    valueColList.toString()
);
```

---

## Gap Analysis Update

**Relations Coverage**: 27/40 (67.5-70%)

**Phase 2 Status**: Mostly complete
- NADrop, NAFill, NAReplace (M26)
- Hint, Repartition, RepartitionByExpression (M25)
- Unpivot (M27) - this milestone
- Remaining: SubqueryAlias, ToSchema

---

## API Examples

```python
# PySpark API (what users write)
df.unpivot(
    ids=["id", "name"],           # Columns to keep as identifiers
    values=["q1", "q2", "q3"],    # Columns to unpivot
    variableColumnName="quarter",  # Name for the "variable" column
    valueColumnName="sales"        # Name for the "value" column
)

# With auto-inference (unpivot all non-id columns)
df.unpivot(
    ids=["id"],
    values=None,                   # Infer from schema
    variableColumnName="metric",
    valueColumnName="value"
)
```

**Generated DuckDB SQL**:
```sql
SELECT * FROM (SELECT * FROM sales_data)
UNPIVOT ("sales" FOR "quarter" IN ("q1", "q2", "q3"))
```

---

## Lessons Learned

1. **DuckDB's Native UNPIVOT**: Using DuckDB's built-in UNPIVOT syntax is simpler and more efficient than manually generating equivalent SQL with UNION ALL

2. **Schema Inference Reuse**: The SchemaInferrer from M26 proves valuable for automatically determining unpivot columns

3. **Column Quoting**: Proper identifier quoting via `SQLQuoting.quoteIdentifier()` ensures column names with special characters work correctly

---

## Next Steps

Recommended next milestones from gap analysis:
- **M28**: SubqueryAlias - Proper alias handling for complex queries
- **M29**: Statistical functions (StatDescribe, StatSummary)
- **M30**: Catalog operations (ListTables, DropTempView)
