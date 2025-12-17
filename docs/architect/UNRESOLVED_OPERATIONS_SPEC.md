# Unresolved Operations Specification

**Version:** 1.0
**Date:** 2025-12-16
**Status:** Research Complete
**Target Milestone:** Phase 3 (Complex Types & Expressions)

---

## Executive Summary

This document provides comprehensive specifications for three Spark Connect expression operations that enable advanced column selection and complex type manipulation:

1. **UnresolvedRegex** - Regex-based column selection patterns
2. **UpdateFields** - Struct field manipulation (add, replace, drop)
3. **UnresolvedExtractValue** - Extract values from complex types (structs, arrays, maps)

These operations are currently unimplemented in Thunderduck (as of M44) and are categorized as **MEDIUM priority** in the gap analysis. They enable powerful data manipulation patterns common in production Spark workloads.

**Proto Definition Location**: `/workspace/connect-server/src/main/proto/spark/connect/expressions.proto`

---

## 1. UnresolvedRegex

### 1.1 Overview

UnresolvedRegex enables selecting columns using regular expression patterns instead of explicit column names. This is particularly useful when working with wide tables or datasets with evolving schemas.

**Priority**: 🟡 MEDIUM
**Proto Field**: `unresolved_regex` (field 8 in Expression)
**Spark Version**: Introduced in Spark 2.3.0

### 1.2 Proto Definition

```protobuf
// Represents all of the input attributes to a given relational operator, for example in
// "SELECT `(id)?+.+` FROM ...".
message UnresolvedRegex {
  // (Required) The column name used to extract column with regex.
  string col_name = 1;

  // (Optional) The id of corresponding connect plan.
  optional int64 plan_id = 2;
}
```

### 1.3 PySpark API

**Configuration Required**:
```python
# MUST be enabled before using regex column selection
spark.conf.set("spark.sql.parser.quotedRegexColumnNames", True)
```

**DataFrame API**:
```python
# Using colRegex() method
df.select(df.colRegex("`col[123]`"))        # Select col1, col2, col3
df.select(df.colRegex("`^dim_.*`"))         # Select columns starting with dim_
df.select(df.colRegex("`.*_id$`"))          # Select columns ending with _id
df.select(df.colRegex("`.*name.*`"))        # Select columns containing 'name'

# Backticks are REQUIRED - without them, PySpark throws an error
df.select(df.colRegex("`col.*`"))           # ✅ Correct
df.select(df.colRegex("col.*"))             # ❌ Error
```

**SQL API**:
```sql
-- With configuration enabled
SET spark.sql.parser.quotedRegexColumnNames=true;

-- Select columns matching regex pattern
SELECT `col.*` FROM table_name;
SELECT `^dim_.*` FROM fact_table;
SELECT `(id)?+.+` FROM table_name;
```

### 1.4 Behavior Semantics

**Pattern Matching**:
- Uses Java regex syntax (via `java.util.regex.Pattern`)
- Case-sensitive by default
- Matches against column names, not table-qualified names
- If pattern matches zero columns, returns empty result (no error)

**Resolution Process**:
1. Parse regex pattern from backtick-quoted string
2. Get all available columns from input relation
3. Apply regex filter to column names
4. Expand matching columns in place (like `*` expansion)
5. If optional `plan_id` is set, only match columns from that specific relation

**With Qualifiers**:
```python
# When table alias is specified
df.alias("t").select(df.colRegex("`t.col.*`"))  # Matches columns from table "t"
```

**Type System**:
- Input: Regex pattern string (in backticks)
- Output: Multiple columns with their original types
- No type coercion or transformation occurs

### 1.5 Error Handling

| Error Condition | Behavior |
|----------------|----------|
| Invalid regex syntax | `AnalysisException` at parse time |
| Pattern matches no columns | Returns empty result (no error) |
| Configuration not enabled | Pattern treated as literal column name, fails with column not found |
| Backticks missing | Pattern treated as literal column name |
| Ambiguous column reference | Same as regular `*` ambiguity handling |

### 1.6 Edge Cases

**Empty Pattern**:
```python
df.colRegex("``")  # Matches no columns, returns empty result
```

**Special Characters**:
```python
df.colRegex("`col\..*`")     # Literal dot requires escaping
df.colRegex("`col[0-9]+`")   # Character classes work
df.colRegex("`(col|dim)_.*`") # Alternation works
```

**Case Sensitivity**:
```python
# Regex is case-sensitive by default
df.colRegex("`col.*`")   # Matches 'col1' but not 'Col1'
df.colRegex("`(?i)col.*`")  # Case-insensitive with (?i) flag
```

**Conflicting Names After Expansion**:
```python
# If multiple tables have matching columns
df1.alias("a").join(df2.alias("b"), "id") \
   .select(df1.colRegex("`val.*`"))  # Only selects from df1 context
```

### 1.7 Implementation Approach for DuckDB

**DuckDB Native Solution: COLUMNS Expression**

DuckDB provides the `COLUMNS()` expression which supports regex patterns:

```sql
-- Direct regex matching
SELECT COLUMNS('^dim_.*') FROM fact_table;
SELECT COLUMNS('test_(\w+)') AS '\1' FROM table;  -- With capture groups

-- With lambda functions
SELECT COLUMNS(c -> c LIKE '%_id') FROM table;
```

**Implementation Strategy**:

1. **Parse UnresolvedRegex proto**:
   ```java
   String regexPattern = unresolvedRegex.getColName();
   Optional<Long> planId = unresolvedRegex.hasPlanId()
       ? Optional.of(unresolvedRegex.getPlanId())
       : Optional.empty();
   ```

2. **Convert to DuckDB COLUMNS()**:
   ```java
   // Simple case: direct pattern
   String duckdbExpr = String.format("COLUMNS('%s')", regexPattern);

   // With table qualifier (if planId specified)
   if (planId.isPresent()) {
       String tableAlias = resolveTableAlias(planId.get());
       duckdbExpr = String.format("%s.COLUMNS('%s')", tableAlias, regexPattern);
   }
   ```

3. **Handle Java -> DuckDB Regex Differences**:
   - DuckDB uses RE2 syntax (similar to Java but with differences)
   - May need pattern translation for edge cases
   - Test common patterns: `^`, `$`, `.*`, `\w`, `\d`, character classes

4. **Fallback Strategy**:
   - Query DuckDB for available columns: `PRAGMA table_info('table_name')`
   - Apply Java regex filter in Java code
   - Generate explicit column list: `SELECT col1, col2, col3 FROM ...`
   - Less efficient but handles all Java regex features

**Recommended Approach**: Start with DuckDB COLUMNS() for common patterns, fallback to explicit expansion for edge cases.

**Testing Considerations**:
- Test regex compatibility between Java and DuckDB
- Validate empty result handling
- Test with qualified table names
- Test with multiple joins and ambiguous columns

---

## 2. UpdateFields

### 2.1 Overview

UpdateFields provides struct field manipulation operations: adding new fields, replacing existing fields, or dropping fields by name. Introduced in Spark 3.1.0 to simplify working with deeply nested data structures.

**Priority**: 🟢 LOW
**Proto Field**: `update_fields` (field 13 in Expression)
**Spark Version**: Introduced in Spark 3.1.0

### 2.2 Proto Definition

```protobuf
// Add, replace or drop a field of `StructType` expression by name.
message UpdateFields {
  // (Required) The struct expression.
  Expression struct_expression = 1;

  // (Required) The field name.
  string field_name = 2;

  // (Optional) The expression to add or replace.
  //
  // When not set, it means this field will be dropped.
  Expression value_expression = 3;
}
```

### 2.3 PySpark API

**Column Methods**:

```python
from pyspark.sql import functions as F

# Add or replace a field
df.withColumn("person",
    F.col("person").withField("age", F.lit(30)))

# Drop a field
df.withColumn("person",
    F.col("person").dropFields("city"))

# Chain multiple operations
df.withColumn("person",
    F.col("person")
        .withField("age", F.lit(30))
        .withField("gender", F.lit("M"))
        .dropFields("city"))

# Nested field manipulation
df.withColumn("data",
    F.col("data").withField("address.zip", F.lit("12345")))
```

**Semantics**:
- `withField(name, value)` - Add new field or replace existing field
- `dropFields(*fieldNames)` - Drop one or more fields (no-op if field doesn't exist)
- Both return a new struct Column

### 2.4 Behavior Semantics

**Add Operation** (value_expression is set):
- If field exists: Replace with new value
- If field doesn't exist: Add new field at the end
- Preserves order of existing fields
- New fields appended to the end

**Drop Operation** (value_expression is null/unset):
- If field exists: Remove field
- If field doesn't exist: No-op (no error)
- Preserves order of remaining fields

**Nested Field Access**:
```python
# Dot notation for nested fields
col("struct").withField("outer.inner.field", value)

# Equivalent to:
col("struct").withField("outer",
    col("struct.outer").withField("inner",
        col("struct.outer.inner").withField("field", value)))
```

**Type System**:
- Input: StructType expression + field name + optional value expression
- Output: Modified StructType with same or different schema
- Field name must be string literal (not expression)
- Value expression can be any type

### 2.5 Error Handling

| Error Condition | Behavior |
|----------------|----------|
| Non-struct input | `AnalysisException`: "struct_expression must be StructType" |
| Null struct value (runtime) | Propagates null (doesn't evaluate field operations) |
| Invalid field name syntax | `AnalysisException`: "Invalid field name" |
| Nested field with non-struct parent | `AnalysisException`: "Cannot resolve nested field" |
| Type mismatch on replace | No error - field replaced with new type |
| Drop non-existent field | No error - no-op |

### 2.6 Edge Cases

**Null Handling**:
```python
# If struct is null, entire expression returns null
df.withColumn("result",
    F.when(F.col("person").isNull(), None)
    .otherwise(F.col("person").withField("age", F.lit(30))))
```

**Multiple Operations on Same Field**:
```python
# Last operation wins
col("struct").withField("x", lit(1)).withField("x", lit(2))  # x=2
col("struct").withField("x", lit(1)).dropFields("x")         # x dropped
col("struct").dropFields("x").withField("x", lit(1))         # x=1
```

**Schema Evolution**:
```python
# Can change field types
col("struct").withField("count", col("struct.count").cast("long"))

# Can change from simple to nested
col("struct").withField("metadata", struct(lit("key"), lit("value")))
```

**Performance Optimization**:
```python
# Less efficient - multiple struct rebuilds
df.select(
    col("s").withField("a", lit(1)).alias("s1"),
    col("s").withField("b", lit(2)).alias("s2")
)

# More efficient - extract struct first
df.select(
    col("s").withField("a", lit(1)).withField("b", lit(2)).alias("s")
)
```

### 2.7 Implementation Approach for DuckDB

**DuckDB Native Functions**:

DuckDB provides several struct manipulation functions:

```sql
-- struct_insert - adds fields but NOT update existing
SELECT struct_insert(my_struct, field := value) FROM table;

-- struct_extract - get field value
SELECT struct_extract(my_struct, 'field_name') FROM table;

-- Rebuild struct with selected fields (for drop)
SELECT struct_pack(
    field1 := my_struct.field1,
    field3 := my_struct.field3
) FROM table;  -- Drops field2
```

**Implementation Strategy**:

1. **Parse UpdateFields proto**:
   ```java
   Expression structExpr = updateFields.getStructExpression();
   String fieldName = updateFields.getFieldName();
   boolean isDrop = !updateFields.hasValueExpression();
   Expression valueExpr = isDrop ? null : updateFields.getValueExpression();
   ```

2. **Handle Nested Field Names**:
   ```java
   String[] fieldPath = fieldName.split("\\.");
   if (fieldPath.length > 1) {
       // Recursive nested field handling
       // Build nested struct_insert/struct_pack calls
   }
   ```

3. **Generate DuckDB SQL**:

   **For Add/Replace**:
   ```java
   // Approach 1: Use struct_insert (if supported for updates in DuckDB)
   String duckdbSql = String.format(
       "struct_insert(%s, %s := %s)",
       convertExpression(structExpr),
       fieldName,
       convertExpression(valueExpr)
   );

   // Approach 2: Rebuild struct with struct_pack
   // Query existing schema, add/replace field, rebuild
   List<String> fields = getStructFields(structExpr);
   String packExprs = fields.stream()
       .map(f -> f.equals(fieldName)
           ? fieldName + " := " + convertExpression(valueExpr)
           : f + " := " + structExpr + "." + f)
       .collect(Collectors.joining(", "));
   String duckdbSql = String.format("struct_pack(%s)", packExprs);
   ```

   **For Drop**:
   ```java
   // Must rebuild struct without dropped field
   List<String> fields = getStructFields(structExpr);
   String packExprs = fields.stream()
       .filter(f -> !f.equals(fieldName))
       .map(f -> f + " := " + structExpr + "." + f)
       .collect(Collectors.joining(", "));
   String duckdbSql = String.format("struct_pack(%s)", packExprs);
   ```

4. **Schema Inference**:
   ```java
   // Need to track struct schema through query plan
   // Use DuckDB DESCRIBE or schema introspection
   // Cache schema information during conversion
   ```

**Challenges**:
- DuckDB's `struct_insert` may not support updating existing fields (need to verify)
- Nested field manipulation requires recursive traversal
- Schema tracking needed to know which fields to preserve/drop
- Performance: rebuilding structs can be expensive for wide schemas

**Recommended Approach**:
- For simple add/replace: Use `struct_insert` if DuckDB supports updates
- For drop or complex cases: Use `struct_pack` with explicit field list
- Cache struct schemas during plan conversion to avoid repeated introspection
- Consider generating CTEs for complex nested operations

**Testing Considerations**:
- Test add, replace, drop operations separately
- Test nested field manipulation (dot notation)
- Test with null struct values
- Test schema evolution (type changes)
- Performance test with wide structs (50+ fields)

---

## 3. UnresolvedExtractValue

### 3.1 Overview

UnresolvedExtractValue extracts values from complex types (structs, arrays, maps) using subscript notation. It generalizes field access, array indexing, and map key lookup into a single operation.

**Priority**: 🟡 MEDIUM
**Proto Field**: `unresolved_extract_value` (field 12 in Expression)
**Spark Version**: Core feature since early Spark SQL

### 3.2 Proto Definition

```protobuf
// Extracts a value or values from an Expression
message UnresolvedExtractValue {
  // (Required) The expression to extract value from, can be
  // Map, Array, Struct or array of Structs.
  Expression child = 1;

  // (Required) The expression to describe the extraction, can be
  // key of Map, index of Array, field name of Struct.
  Expression extraction = 2;
}
```

### 3.3 PySpark API

**Struct Field Access**:
```python
# Dot notation (preferred)
df.select(df.person.name)
df.select(col("person").name)

# Bracket notation (alternative)
df.select(df.person["name"])
df.select(col("person")["name"])

# getField() method (explicit)
df.select(col("person").getField("name"))

# Nested access
df.select(df.person.address.city)
df.select(col("person.address.city"))
```

**Array Indexing**:
```python
# Bracket notation (0-based in Python, but 1-based in SQL!)
df.select(df.items[0])           # First element
df.select(df.items[-1])          # Last element (negative indexing)
df.select(col("items")[2])       # Third element

# getItem() method (explicit)
df.select(col("items").getItem(0))

# Note: In PySpark API, indexing is 0-based
# But in Spark SQL expressions, it's 1-based!
```

**Map Key Access**:
```python
# Bracket notation
df.select(df.properties["color"])
df.select(col("properties")["key_name"])

# getItem() method (explicit)
df.select(col("properties").getItem("color"))
```

**Array of Structs**:
```python
# Chain operations
df.select(df.employees[0].name)
df.select(col("employees")[0].getField("salary"))
```

### 3.4 Behavior Semantics

**Type-Dependent Behavior**:

| Child Type | Extraction Type | Behavior | SQL Function |
|-----------|----------------|----------|--------------|
| StructType | String literal | Get field by name | `struct.field` or `struct_extract(struct, 'field')` |
| ArrayType | Integer | Get element by index (1-based) | `list[index]` |
| MapType | Key expression | Get value by key | `map[key]` or `element_at(map, key)` |

**Indexing Conventions**:
- **PySpark API**: 0-based indexing (Python convention)
- **Spark SQL**: 1-based indexing (SQL convention)
- **DuckDB**: 1-based indexing for lists, 0-based for JSON
- **Conversion needed**: PySpark `arr[0]` → Spark SQL `arr[1]`

**Null Handling**:

| Scenario | Behavior |
|----------|----------|
| Null struct | Returns null (doesn't evaluate field access) |
| Null array | Returns null |
| Null map | Returns null |
| Array index out of bounds | Returns null (no error!) |
| Map key not found | Returns null (no error!) |
| Struct field doesn't exist | `AnalysisException` at analysis time |

**Type Coercion**:
- Map keys must match the map's key type exactly (no automatic coercion)
- Array indices must be integer types (byte, short, int, long)
- Struct field names must be string literals (not expressions)

### 3.5 Error Handling

| Error Condition | Behavior |
|----------------|----------|
| Non-complex child type | `AnalysisException`: "Cannot extract value from primitive type" |
| Struct with non-literal field name | `AnalysisException` in Spark < 3.0, supported in 3.0+ |
| Array with non-integer index | `AnalysisException`: "Index must be integer type" |
| Map with wrong key type | `AnalysisException`: "Key type mismatch" |
| Array out of bounds (runtime) | Returns null |
| Map key not found (runtime) | Returns null |
| Null child (runtime) | Returns null |

### 3.6 Edge Cases

**Negative Array Indexing**:
```python
# PySpark supports negative indexing (Python convention)
df.select(col("items")[-1])  # Last element
df.select(col("items")[-2])  # Second to last

# Converts to: size(items) - 1, size(items) - 2, etc.
```

**Dynamic Field Access** (Spark 3.0+):
```python
# Field name as expression (not just literal)
df.select(col("struct")[col("field_name_column")])

# This is DIFFERENT from literal field name
```

**Nested Extraction**:
```python
# Chained extractions create nested UnresolvedExtractValue nodes
col("data")["person"]["address"]["city"]

# Proto tree:
# UnresolvedExtractValue(
#   child=UnresolvedExtractValue(
#     child=UnresolvedExtractValue(
#       child=col("data"),
#       extraction=lit("person")),
#     extraction=lit("address")),
#   extraction=lit("city"))
```

**Map with Complex Keys**:
```python
# Map keys can be structs or arrays
df.select(col("map_of_structs")[struct(lit("a"), lit(1))])
```

**Array of Arrays**:
```python
# Nested array access
df.select(col("matrix")[0][1])  # matrix[0][1]
```

### 3.7 Implementation Approach for DuckDB

**DuckDB Native Syntax**:

```sql
-- Struct field access (dot notation)
SELECT person.name FROM table;
SELECT person['name'] FROM table;  -- Also works

-- Array indexing (1-based!)
SELECT items[1] FROM table;  -- First element
SELECT items[-1] FROM table; -- Last element (DuckDB supports negative indexing)

-- Map element access
SELECT element_at(properties, 'key') FROM table;

-- List element access with null handling
SELECT list_element(items, 1) FROM table;
```

**Implementation Strategy**:

1. **Parse UnresolvedExtractValue proto**:
   ```java
   Expression child = unresolvedExtractValue.getChild();
   Expression extraction = unresolvedExtractValue.getExtraction();
   ```

2. **Determine child type**:
   ```java
   DataType childType = inferType(child);

   if (childType instanceof StructType) {
       return handleStructExtraction(child, extraction);
   } else if (childType instanceof ArrayType) {
       return handleArrayExtraction(child, extraction);
   } else if (childType instanceof MapType) {
       return handleMapExtraction(child, extraction);
   } else {
       throw new PlanConversionException("Cannot extract from type: " + childType);
   }
   ```

3. **Struct Field Extraction**:
   ```java
   private String handleStructExtraction(Expression child, Expression extraction) {
       String structExpr = convertExpression(child);

       if (extraction.hasLiteral() && extraction.getLiteral().hasString()) {
           String fieldName = extraction.getLiteral().getString();
           // DuckDB dot notation
           return String.format("%s.%s", structExpr, escapeIdentifier(fieldName));
       } else {
           // Dynamic field access (Spark 3.0+)
           String fieldNameExpr = convertExpression(extraction);
           return String.format("struct_extract(%s, %s)", structExpr, fieldNameExpr);
       }
   }
   ```

4. **Array Element Extraction**:
   ```java
   private String handleArrayExtraction(Expression child, Expression extraction) {
       String arrayExpr = convertExpression(child);
       String indexExpr = convertExpression(extraction);

       // CRITICAL: Handle 0-based (PySpark) to 1-based (DuckDB) conversion
       // Check if extraction is a literal integer
       if (extraction.hasLiteral()) {
           if (extraction.getLiteral().hasInteger()) {
               int pyIndex = extraction.getLiteral().getInteger();
               int sqlIndex = pyIndex + 1;  // Convert 0-based to 1-based
               return String.format("%s[%d]", arrayExpr, sqlIndex);
           } else if (extraction.getLiteral().hasLong()) {
               long pyIndex = extraction.getLiteral().getLong();
               long sqlIndex = pyIndex + 1;
               return String.format("%s[%d]", arrayExpr, sqlIndex);
           }
       }

       // Dynamic index - add 1 at runtime
       return String.format("%s[%s + 1]", arrayExpr, indexExpr);
   }
   ```

5. **Map Element Extraction**:
   ```java
   private String handleMapExtraction(Expression child, Expression extraction) {
       String mapExpr = convertExpression(child);
       String keyExpr = convertExpression(extraction);

       // DuckDB element_at function
       return String.format("element_at(%s, %s)", mapExpr, keyExpr);
   }
   ```

6. **Handle Negative Indexing**:
   ```java
   // For negative indices in arrays
   if (indexValue < 0) {
       // arr[-1] → arr[length(arr) + (-1) + 1] → arr[length(arr)]
       return String.format("%s[length(%s) + %d + 1]",
           arrayExpr, arrayExpr, indexValue);
   }
   ```

**Challenges**:
- **Index conversion**: PySpark 0-based → Spark SQL 1-based → DuckDB 1-based
  - Need to determine if proto contains original Python index or converted SQL index
  - May need protocol-level flag or analysis of call context
- **Type inference**: Need to know child type to choose extraction strategy
- **Dynamic access**: Struct field access with expression (not literal) requires `struct_extract()`
- **Nested extraction**: May produce deeply nested expressions

**Recommended Approach**:
- Cache type information during plan analysis
- Use DuckDB native syntax (dot notation, brackets) for literals
- Use functions (`struct_extract`, `element_at`) for dynamic access
- Add helper method to determine if index needs conversion (check context/metadata)

**Testing Considerations**:
- Test struct field access with literals and expressions
- Test array indexing: 0, positive, negative, out of bounds
- Test map key access with various key types
- Test null propagation at each level
- Test nested extraction chains
- Critical: Verify index conversion (0-based vs 1-based)

---

## 4. Implementation Priority & Dependencies

### 4.1 Recommended Implementation Order

1. **UnresolvedExtractValue** (Highest Impact)
   - Most commonly used in production workloads
   - Foundational for working with complex types
   - Required for: `col.field`, `arr[0]`, `map["key"]`
   - Estimated effort: 1 week

2. **UnresolvedRegex** (High Value for Wide Tables)
   - Enables schema-agnostic column selection
   - Particularly useful for evolving schemas
   - Less critical but high user demand
   - Estimated effort: 3-4 days

3. **UpdateFields** (Advanced Use Cases)
   - Lower priority - less frequently used
   - Can be worked around with SQL reconstruct
   - Nice-to-have for nested data manipulation
   - Estimated effort: 1 week

### 4.2 Dependencies

**Type System Enhancement**:
All three operations require robust type inference:
- Track StructType schemas through query plan
- Infer ArrayType element types
- Infer MapType key/value types

Recommendation: Build type inference subsystem first (3-4 days)

**Schema Introspection**:
Need ability to query DuckDB for schema information:
- `PRAGMA table_info('table')`
- `DESCRIBE SELECT ...`
- Runtime schema caching

**Complex Literal Support**:
UnresolvedExtractValue with complex map keys requires:
- Array literals (currently unimplemented)
- Struct literals (currently unimplemented)
- Map literals (currently unimplemented)

See: Section 2.3 "Literal Type Support" in gap analysis

### 4.3 Testing Strategy

**Differential Testing**:
Create differential tests for each operation:
- Compare Thunderduck vs Spark 4.0.1 results
- Test all documented edge cases
- Test error conditions
- Validate null handling

**Unit Testing**:
- Test proto parsing
- Test SQL generation
- Test type inference
- Test index conversion logic

**Integration Testing**:
- Test with real datasets
- Test performance with wide tables (UnresolvedRegex)
- Test with deeply nested data (UpdateFields, UnresolvedExtractValue)
- Test with various DuckDB data sources

**Edge Case Testing**:
- Empty results
- Null propagation
- Out of bounds access
- Type mismatches
- Schema evolution

---

## 5. Compatibility Notes

### 5.1 Spark Version Differences

| Feature | Spark 2.x | Spark 3.0+ | Spark 3.1+ | Notes |
|---------|-----------|------------|------------|-------|
| UnresolvedRegex | ✓ (2.3+) | ✓ | ✓ | Requires config flag |
| UnresolvedExtractValue (literal) | ✓ | ✓ | ✓ | Core feature |
| UnresolvedExtractValue (expr) | ✗ | ✓ | ✓ | Dynamic field access |
| UpdateFields (withField) | ✗ | ✗ | ✓ (3.1.0) | New in 3.1.0 |
| UpdateFields (dropFields) | ✗ | ✗ | ✓ (3.1.0) | New in 3.1.0 |

### 5.2 DuckDB Compatibility

| Feature | DuckDB Equivalent | Notes |
|---------|------------------|-------|
| Regex column selection | `COLUMNS('regex')` | Similar but RE2 syntax vs Java regex |
| Struct field access | `struct.field` or `struct['field']` | Native support |
| Array indexing | `list[index]` | 1-based indexing (Spark SQL compatible) |
| Map element | `element_at(map, key)` | Returns null if key not found |
| Add struct field | `struct_insert(struct, field := value)` | May not update existing fields |
| Drop struct field | `struct_pack(field1 := ...)` | Rebuild without field |
| Negative array indexing | `list[-1]` | Native support |

### 5.3 Known Limitations

**UnresolvedRegex**:
- Regex syntax differences between Java and DuckDB RE2
- May need fallback to explicit expansion for complex patterns
- Performance impact with very wide tables (100+ columns)

**UpdateFields**:
- Rebuilding structs can be expensive (O(n) where n = field count)
- No native "update field" in DuckDB (must rebuild entire struct)
- Nested field updates require recursive reconstruction

**UnresolvedExtractValue**:
- Index conversion complexity (0-based vs 1-based)
- Type inference required for correct SQL generation
- Dynamic field access less efficient than literal access

---

## 6. References

### 6.1 Spark Documentation

- [PySpark DataFrame.colRegex Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.colRegex.html)
- [PySpark Column.withField Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.withField.html)
- [PySpark Column.dropFields Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.dropFields.html)
- [PySpark Column.getField Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.getField.html)
- [PySpark Column.getItem Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.getItem.html)

### 6.2 Spark Source Code

- [SPARK-12139: REGEX Column Specification PR](https://github.com/apache/spark/pull/18023)
- [SPARK-31317: Add withField method PR](https://github.com/apache/spark/pull/27066)
- [SPARK-7133: Struct/Array/Map field accessor PR](https://github.com/apache/spark/pull/5744)

### 6.3 DuckDB Documentation

- [DuckDB COLUMNS Expression](https://duckdb.org/docs/stable/sql/expressions/star)
- [DuckDB Struct Functions](https://duckdb.org/docs/stable/sql/functions/struct)
- [DuckDB List Functions](https://duckdb.org/docs/stable/sql/functions/list)
- [DuckDB Map Functions](https://duckdb.org/docs/stable/sql/functions/map)
- [DuckDB Indexing](https://duckdb.org/docs/stable/sql/dialect/indexing)
- [DuckDB Struct Data Type](https://duckdb.org/docs/stable/sql/data_types/struct)
- [DuckDB JSON Arrow Operators](https://duckdb.org/docs/stable/data/json/json_functions)

### 6.4 Community Resources

- [Manipulating Nested Data in Spark 3.1.1 (Medium)](https://medium.com/@fqaiser94/manipulating-nested-data-just-got-easier-in-apache-spark-3-1-1-f88bc9003827)
- [Spark 3 Nested Fields (Towards Data Science)](https://towardsdatascience.com/spark-3-nested-fields-not-so-nested-anymore-9b8d34b00b95/)
- [Working with Structs in PySpark (TechBrothersIT)](https://www.techbrothersit.com/2025/06/working-with-structs-and-nested-fields.html)
- [DuckDB STRUCT: A Practical Guide (MotherDuck)](https://motherduck.com/learn-more/duckdb-struct-nested-data/)
- [Understanding Struct, Map, and Array in PySpark (Medium)](https://medium.com/@sachan.pratiksha/understanding-struct-map-and-array-in-pyspark-without-confusion-5674cc1cc16d)

### 6.5 Related Thunderduck Documentation

- `/workspace/CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md` - Section 2.2: Not Implemented Expressions
- `/workspace/connect-server/src/main/proto/spark/connect/expressions.proto` - Proto definitions
- `/workspace/docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md` - Testing strategy

---

## 7. Appendix: Example Use Cases

### 7.1 UnresolvedRegex Examples

**Wide Table Column Selection**:
```python
# Dataset has 100+ columns with patterns
# Select all dimension columns: dim_customer_id, dim_product_id, etc.
df.select(df.colRegex("`^dim_.*`"), "measure_value")

# Select all metric columns: metric_revenue, metric_profit, etc.
df.select("id", df.colRegex("`^metric_.*`"))
```

**Schema Evolution Resilience**:
```python
# New columns added over time, select all that match
df.select(df.colRegex("`^feature_v[0-9]+_.*`"))  # feature_v1_x, feature_v2_y, ...
```

**Exclusion Patterns**:
```python
# Select all columns except internal ones
df.select(df.colRegex("`^(?!_internal).*`"))  # Negative lookahead
```

### 7.2 UpdateFields Examples

**Data Quality Fixes**:
```python
# Fix incorrect nested value
df.withColumn("event",
    col("event").withField("metadata.corrected_flag", lit(True)))
```

**Schema Normalization**:
```python
# Remove PII fields from nested structure
df.withColumn("user",
    col("user")
        .dropFields("ssn", "credit_card", "address.full_address"))
```

**Feature Engineering**:
```python
# Add computed fields to nested structure
df.withColumn("order",
    col("order")
        .withField("total_with_tax", col("order.total") * 1.08)
        .withField("discount_pct", col("order.discount") / col("order.total")))
```

### 7.3 UnresolvedExtractValue Examples

**JSON Flattening**:
```python
# Extract nested fields from JSON-derived struct
df.select(
    col("id"),
    col("data")["user"]["profile"]["name"].alias("user_name"),
    col("data")["user"]["profile"]["email"].alias("user_email"),
    col("data")["timestamp"]
)
```

**Array Processing**:
```python
# Get first and last items from array
df.select(
    col("id"),
    col("items")[0].alias("first_item"),
    col("items")[-1].alias("last_item"),
    size(col("items")).alias("item_count")
)
```

**Map Key Extraction**:
```python
# Extract specific configuration values
df.select(
    col("id"),
    col("config")["timeout"].alias("timeout_ms"),
    col("config")["retries"].alias("max_retries"),
    col("config")["endpoint"].alias("api_endpoint")
)
```

**Complex Nested Access**:
```python
# Navigate deeply nested structure
df.select(
    col("events")[0]["metadata"]["source"]["ip_address"]
)
```

---

**Document Status**: Research Complete, Ready for Implementation
**Next Steps**:
1. Implement type inference subsystem
2. Implement UnresolvedExtractValue (highest priority)
3. Add differential tests for complex type operations
4. Implement UnresolvedRegex (medium priority)
5. Implement UpdateFields (lower priority)
