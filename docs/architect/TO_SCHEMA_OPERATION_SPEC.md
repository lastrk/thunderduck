# Spark ToSchema Operation Specification

**Author**: Claude (Research Agent)
**Date**: 2025-12-16
**Status**: Research Document for Implementation Planning

## Overview

This document specifies the behavior and implementation details for Spark's `DataFrame.to(schema)` operation. This operation is exposed through the DataFrame API and defined in the Spark Connect protocol as the `ToSchema` relation type.

The `to()` method returns a new DataFrame where each row is reconciled to match a specified schema. It performs column reordering, type casting, projection, and nullability validation.

---

## 1. ToSchema Operation

### PySpark API

```python
DataFrame.to(schema: StructType) -> DataFrame
```

**Parameters**:
- `schema` (required): A `pyspark.sql.types.StructType` object defining the target schema

**Available Since**: PySpark 3.4.0

**Supports**: Spark Connect

### Return Type

Returns a **DataFrame** with schema matching the specified `StructType`.

### Purpose

The `to()` method reconciles a DataFrame to match a specified schema by:
1. **Reordering columns** by name to match the target schema
2. **Projecting away columns** not needed by the target schema
3. **Casting columns** to match target data types (if compatible)
4. **Validating nullability** constraints
5. **Carrying over metadata** from the target schema

### Schema Reconciliation Example

Input DataFrame:
```python
df = spark.createDataFrame([("a", 1)], ["i", "j"])
# Schema: StructType([
#   StructField('i', StringType(), True),
#   StructField('j', LongType(), True)
# ])
```

Target Schema:
```python
from pyspark.sql.types import StructField, StringType, StructType

schema = StructType([
    StructField("j", StringType()),  # Reordered and type changed
    StructField("i", StringType())
])
```

Reconciled DataFrame:
```python
df2 = df.to(schema)
# Schema: StructType([
#   StructField('j', StringType(), True),
#   StructField('i', StringType(), True)
# ])

df2.show()
# +---+---+
# |  j|  i|
# +---+---+
# |  1|  a|
# +---+---+
```

---

## 2. Expected Behavior

### 2.1 Column Reordering

**Behavior**: Columns are reordered by name to match the order specified in the target schema.

**Example**:
```python
# Input: columns [a, b, c]
# Target: columns [c, a, b]
# Result: columns [c, a, b]
```

**Important**: Column matching is by **name**, not position.

### 2.2 Column Projection

**Behavior**: Columns present in the input DataFrame but NOT in the target schema are **projected away** (dropped).

**Example**:
```python
# Input: columns [a, b, c, d]
# Target: columns [a, c]
# Result: columns [a, c]  (b and d are dropped)
```

### 2.3 Missing Columns

**Behavior**: Columns present in the target schema but NOT in the input DataFrame will cause the operation to **FAIL**.

**Example**:
```python
# Input: columns [a, b]
# Target: columns [a, b, c]
# Result: ERROR - column 'c' is missing
```

**Error Type**: `AnalysisException` with message indicating missing column(s).

**Rationale**: Missing columns cannot be filled with default values - Spark requires explicit handling.

### 2.4 Type Casting

**Behavior**: Columns are cast to match target data types if the cast is **compatible**.

**Compatible Casts**:
- Numeric to numeric (with overflow checking)
  - INT → BIGINT ✅
  - DOUBLE → FLOAT ✅ (with precision loss warning)
  - BIGINT → INT ✅ (fails on overflow)
- String to string ✅
- Date/Timestamp conversions ✅
- Widening conversions ✅

**Incompatible Casts**:
- String to numeric ❌
- Numeric to string ❌
- String to date/timestamp ❌ (without explicit format)
- Complex type changes ❌

**Example**:
```python
# Input: column 'age' is LongType
# Target: column 'age' is StringType
# Result: ERROR - incompatible cast from Long to String

# Input: column 'value' is IntegerType
# Target: column 'value' is LongType
# Result: SUCCESS - widening cast from Int to Long
```

**Error Type**: `AnalysisException` with message about incompatible cast.

### 2.5 Nullability Validation

**Behavior**: Nullability constraints in the target schema are **validated** (strict mode).

**Rules**:
1. **Nullable → Non-nullable**: FAILS if the column can contain nulls
2. **Non-nullable → Nullable**: SUCCEEDS (relaxing constraint)
3. **Nullable → Nullable**: SUCCEEDS
4. **Non-nullable → Non-nullable**: SUCCEEDS

**Example**:
```python
# Input: column 'name' is StringType(nullable=True)
# Target: column 'name' is StringType(nullable=False)
# Result: ERROR - cannot make nullable column non-nullable

# Input: column 'id' is IntegerType(nullable=False)
# Target: column 'id' is IntegerType(nullable=True)
# Result: SUCCESS - relaxing nullability constraint
```

**Error Type**: `AnalysisException` with message about nullability constraint violation.

**Important**: Spark's nullability validation is **static** based on schema metadata, not runtime data. Even if a nullable column contains no nulls at runtime, attempting to make it non-nullable will fail.

### 2.6 Nested Struct Support

**Behavior**: The `to()` method supports nested struct fields and applies the same reconciliation rules recursively.

**Example**:
```python
# Input schema:
# StructType([
#   StructField('id', IntegerType()),
#   StructField('address', StructType([
#     StructField('street', StringType()),
#     StructField('city', StringType()),
#     StructField('zip', IntegerType())
#   ]))
# ])

# Target schema (reorder nested fields, drop 'zip'):
# StructType([
#   StructField('id', IntegerType()),
#   StructField('address', StructType([
#     StructField('city', StringType()),
#     StructField('street', StringType())
#   ]))
# ])

# Result: Nested fields reordered, 'zip' field dropped
```

**Nested Field Rules**:
- Missing nested fields cause failure
- Nested field type casts follow same compatibility rules
- Nested field nullability is validated

### 2.7 Metadata Preservation

**Behavior**: The target schema's metadata is carried over to the result DataFrame. Column/field metadata from the input is preserved only if not overwritten by the target schema.

**Precedence**: Target schema metadata > Input schema metadata

### 2.8 Edge Cases

| Scenario | Behavior |
|----------|----------|
| **Empty DataFrame** | Schema is reconciled; zero rows returned |
| **All nulls in column** | Schema is reconciled; null values preserved |
| **Target schema is identical** | No-op; returns DataFrame with same schema |
| **Target schema is empty** | Invalid; StructType must have at least one field |
| **Column name case mismatch** | Depends on Spark's case sensitivity setting (`spark.sql.caseSensitive`) |

---

## 3. Protocol Definition

From `relations.proto` (lines 968-976):

```protobuf
message ToSchema {
  // (Required) The input relation.
  Relation input = 1;

  // (Required) The user provided schema.
  //
  // The Server side will update the dataframe with this schema.
  DataType schema = 2;
}
```

### Protocol Notes

- The `schema` field is a `DataType` message (typically a `StructType`)
- The server performs all validation and reconciliation
- The operation is a **transformation** (lazy, returns DataFrame)

---

## 4. DuckDB Implementation Strategy

### 4.1 SQL Generation Approach

The ToSchema operation can be implemented using standard SQL SELECT with:
1. Column reordering via explicit SELECT list
2. Type casting via CAST expressions
3. Column projection (omitting unwanted columns)

### 4.2 Implementation Pseudocode

```java
public String generateToSchemaSQL(Relation input, StructType targetSchema) {
    // 1. Get input schema
    StructType inputSchema = getInputSchema(input);

    // 2. Validate: Check for missing columns
    for (StructField targetField : targetSchema.fields()) {
        if (!inputSchema.hasField(targetField.name())) {
            throw new AnalysisException(
                "Column '" + targetField.name() + "' does not exist in input DataFrame"
            );
        }
    }

    // 3. Validate: Check nullability constraints
    for (StructField targetField : targetSchema.fields()) {
        StructField inputField = inputSchema.getField(targetField.name());
        if (!targetField.nullable() && inputField.nullable()) {
            throw new AnalysisException(
                "Cannot change nullable column '" + targetField.name() +
                "' to non-nullable"
            );
        }
    }

    // 4. Generate SELECT with casts and reordering
    List<String> selectList = new ArrayList<>();
    for (StructField targetField : targetSchema.fields()) {
        StructField inputField = inputSchema.getField(targetField.name());

        // Check if cast is needed
        if (!typesMatch(inputField.dataType(), targetField.dataType())) {
            // Validate cast compatibility
            if (!isCastCompatible(inputField.dataType(), targetField.dataType())) {
                throw new AnalysisException(
                    "Cannot cast column '" + targetField.name() +
                    "' from " + inputField.dataType() +
                    " to " + targetField.dataType()
                );
            }

            // Generate CAST expression
            String duckdbType = mapToDuckDBType(targetField.dataType());
            selectList.add(
                "CAST(" + quoteIdentifier(targetField.name()) +
                " AS " + duckdbType + ") AS " +
                quoteIdentifier(targetField.name())
            );
        } else {
            // No cast needed
            selectList.add(quoteIdentifier(targetField.name()));
        }
    }

    // 5. Generate final SQL
    return "SELECT " + String.join(", ", selectList) +
           " FROM (" + convertInputRelation(input) + ")";
}
```

### 4.3 DuckDB SQL Examples

**Example 1: Column reordering**
```sql
-- Input: columns [a, b, c]
-- Target: columns [c, a, b]

SELECT c, a, b
FROM input_table;
```

**Example 2: Type casting**
```sql
-- Input: column 'age' is BIGINT
-- Target: column 'age' is VARCHAR

-- This would FAIL in Spark (incompatible cast)
-- In DuckDB: CAST(age AS VARCHAR) works, but Thunderduck should reject it

SELECT CAST(age AS VARCHAR) AS age, name
FROM input_table;
```

**Example 3: Column projection + reordering**
```sql
-- Input: columns [a, b, c, d]
-- Target: columns [c, a]

SELECT c, a
FROM input_table;
```

**Example 4: Nested struct reordering**
```sql
-- Input: struct with fields [street, city, zip]
-- Target: struct with fields [city, street]

SELECT
    id,
    STRUCT_PACK(
        city := address.city,
        street := address.street
    ) AS address
FROM input_table;
```

### 4.4 Type Compatibility Matrix

| From Type | To Type | Compatible? | DuckDB Behavior |
|-----------|---------|-------------|-----------------|
| TINYINT | SMALLINT/INTEGER/BIGINT | ✅ Yes | Widening cast |
| INTEGER | BIGINT | ✅ Yes | Widening cast |
| BIGINT | INTEGER | ⚠️ Check overflow | Narrowing cast, can overflow |
| FLOAT | DOUBLE | ✅ Yes | Widening cast |
| DOUBLE | FLOAT | ⚠️ Precision loss | Narrowing cast |
| INTEGER | VARCHAR | ❌ No | Not allowed by Spark |
| VARCHAR | INTEGER | ❌ No | Not allowed by Spark |
| DATE | TIMESTAMP | ✅ Yes | Conversion |
| TIMESTAMP | DATE | ✅ Yes | Truncation |
| STRUCT | STRUCT | ✅ Yes (if fields match) | Recursive check |

**Implementation Note**: Thunderduck must replicate Spark's type compatibility rules, not DuckDB's more permissive casting.

### 4.5 Nullability Handling

**Challenge**: DuckDB does NOT enforce nullability constraints in the type system. All columns are effectively nullable in DuckDB.

**Solution**:
1. Perform nullability validation in Java before generating SQL
2. Throw `AnalysisException` if validation fails
3. DuckDB query execution proceeds only after validation passes

**Implementation**:
```java
private void validateNullability(StructField inputField, StructField targetField) {
    if (!targetField.nullable() && inputField.nullable()) {
        throw new AnalysisException(
            "Cannot enforce non-nullable constraint on column '" +
            targetField.name() + "' which is nullable in input DataFrame. " +
            "Nullability can only be relaxed (non-nullable -> nullable), " +
            "not restricted (nullable -> non-nullable)."
        );
    }
}
```

---

## 5. Error Messages

Thunderduck should provide clear, Spark-compatible error messages:

### Missing Column Error
```
[MISSING_COLUMN] Column 'column_name' does not exist in the input DataFrame.
Available columns: [col1, col2, col3]
```

### Incompatible Cast Error
```
[INCOMPATIBLE_CAST] Cannot cast column 'column_name' from LongType to StringType.
Compatible casts include: numeric to numeric, date/timestamp conversions.
```

### Nullability Constraint Error
```
[NULLABILITY_CONSTRAINT_VIOLATION] Cannot change nullable column 'column_name' to non-nullable.
Nullability can only be relaxed (non-nullable -> nullable), not restricted.
```

### Nested Field Error
```
[MISSING_NESTED_FIELD] Nested field 'struct_name.field_name' does not exist in the input DataFrame.
```

---

## 6. Testing Strategy

### 6.1 Differential Tests

Create PySpark tests and compare against Thunderduck:

```python
# Test 1: Column reordering
def test_to_schema_reorder():
    df = spark.createDataFrame([(1, "a", 3.0)], ["x", "y", "z"])
    schema = StructType([
        StructField("z", DoubleType()),
        StructField("x", IntegerType()),
        StructField("y", StringType())
    ])
    result = df.to(schema)
    assert result.columns == ["z", "x", "y"]

# Test 2: Type casting (widening)
def test_to_schema_widening_cast():
    df = spark.createDataFrame([(1,)], ["value"])
    schema = StructType([StructField("value", LongType())])
    result = df.to(schema)
    assert result.schema["value"].dataType == LongType()

# Test 3: Missing column (should fail)
def test_to_schema_missing_column():
    df = spark.createDataFrame([(1,)], ["x"])
    schema = StructType([
        StructField("x", IntegerType()),
        StructField("y", StringType())
    ])
    with pytest.raises(AnalysisException):
        df.to(schema).collect()

# Test 4: Nullability constraint (should fail)
def test_to_schema_nullability_fail():
    df = spark.createDataFrame([(1,)], ["x"])
    df = df.withColumn("x", col("x").cast("int"))  # nullable=True
    schema = StructType([StructField("x", IntegerType(), nullable=False)])
    with pytest.raises(AnalysisException):
        df.to(schema).collect()

# Test 5: Column projection
def test_to_schema_projection():
    df = spark.createDataFrame([(1, "a", 3.0)], ["x", "y", "z"])
    schema = StructType([
        StructField("x", IntegerType()),
        StructField("z", DoubleType())
    ])
    result = df.to(schema)
    assert result.columns == ["x", "z"]
    assert "y" not in result.columns

# Test 6: Nested struct
def test_to_schema_nested_struct():
    schema_in = StructType([
        StructField("id", IntegerType()),
        StructField("address", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("zip", IntegerType())
        ]))
    ])
    df = spark.createDataFrame(
        [(1, ("123 Main St", "Boston", 12345))],
        schema_in
    )

    schema_out = StructType([
        StructField("id", IntegerType()),
        StructField("address", StructType([
            StructField("city", StringType()),
            StructField("street", StringType())
        ]))
    ])

    result = df.to(schema_out)
    assert result.schema["address"].dataType.fieldNames() == ["city", "street"]
```

### 6.2 Edge Case Tests

```python
# Test 7: Empty DataFrame
def test_to_schema_empty_dataframe():
    df = spark.createDataFrame([], StructType([StructField("x", IntegerType())]))
    schema = StructType([StructField("x", LongType())])
    result = df.to(schema)
    assert result.count() == 0
    assert result.schema["x"].dataType == LongType()

# Test 8: Identical schema (no-op)
def test_to_schema_identical():
    schema = StructType([StructField("x", IntegerType())])
    df = spark.createDataFrame([(1,)], schema)
    result = df.to(schema)
    assert result.schema == schema
```

### 6.3 Type Compatibility Tests

```python
# Test 9: Incompatible cast (should fail)
def test_to_schema_incompatible_cast():
    df = spark.createDataFrame([(1,)], ["x"])
    schema = StructType([StructField("x", StringType())])
    with pytest.raises(AnalysisException):
        df.to(schema).collect()

# Test 10: Narrowing cast with overflow
def test_to_schema_narrowing_overflow():
    df = spark.createDataFrame([(2147483648,)], ["x"])  # Exceeds INT max
    schema = StructType([StructField("x", IntegerType())])
    with pytest.raises(Exception):  # Overflow error
        df.to(schema).collect()
```

---

## 7. Implementation Checklist

### Phase 1: Basic Implementation
- [ ] Parse ToSchema message from protocol
- [ ] Extract target schema from DataType message
- [ ] Implement column reordering logic
- [ ] Implement column projection logic
- [ ] Generate DuckDB SELECT with explicit column list

### Phase 2: Type Casting
- [ ] Implement type compatibility checking
- [ ] Map Spark types to DuckDB types
- [ ] Generate CAST expressions for compatible types
- [ ] Reject incompatible casts with clear error messages

### Phase 3: Validation
- [ ] Implement missing column detection
- [ ] Implement nullability constraint validation
- [ ] Implement error message generation

### Phase 4: Nested Structs
- [ ] Implement recursive schema reconciliation
- [ ] Generate DuckDB STRUCT_PACK for nested reordering
- [ ] Validate nested field compatibility

### Phase 5: Testing
- [ ] Create differential tests (10+ test cases)
- [ ] Test edge cases (empty DataFrame, identical schema)
- [ ] Test error cases (missing columns, incompatible casts, nullability violations)
- [ ] Validate against PySpark 4.0.x behavior

---

## 8. Related Operations

| Operation | Relation to ToSchema |
|-----------|---------------------|
| **Cast** | ToSchema performs casting as part of reconciliation |
| **WithColumns** | Adds/replaces columns; ToSchema enforces schema |
| **Select** | Projects columns; ToSchema also reorders and casts |
| **ToDF** | Renames columns; ToSchema does full schema reconciliation |

---

## 9. References

### PySpark Documentation
- [DataFrame.to()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.to.html) - Official API documentation
- [DataFrame.schema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html) - Schema property

### Academic References
- Spark Connect Protocol: `relations.proto` (lines 968-976)
- [Spark SQL StructType Documentation](https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/)

### Related Thunderduck Documentation
- [docs/architect/STATISTICS_OPERATIONS_SPEC.md](STATISTICS_OPERATIONS_SPEC.md) - Similar specification document
- [CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md](/workspace/CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md) - Gap analysis showing ToSchema as unimplemented

---

## 10. Key Design Decisions

### 10.1 Strict vs Lenient Mode

**Decision**: Implement STRICT mode (Spark default behavior)
- Missing columns → FAIL
- Incompatible casts → FAIL
- Nullability violations → FAIL

**Rationale**: Match Spark behavior exactly (Thunderduck requirement)

### 10.2 Type Compatibility

**Decision**: Use Spark's type compatibility rules, NOT DuckDB's
- Numeric to String → FAIL (even though DuckDB allows it)
- String to Numeric → FAIL (even though DuckDB allows it)

**Rationale**: Spark is more restrictive; Thunderduck must match

### 10.3 Nullability Validation

**Decision**: Validate nullability at plan conversion time (before SQL execution)

**Rationale**: DuckDB doesn't enforce nullability, so we must validate in Java layer

### 10.4 Nested Struct Support

**Decision**: Implement full nested struct support using DuckDB's STRUCT_PACK

**Rationale**: Spark supports nested schemas; users expect parity

---

## 11. Performance Considerations

### 11.1 Zero-Copy Optimization

When the target schema is identical to the input schema:
- Detect this case early
- Return input relation unchanged (no SQL generation needed)

### 11.2 Type Cast Overhead

Type casting can be expensive for large DataFrames:
- Widening casts (INT → BIGINT) are cheap
- Narrowing casts (BIGINT → INT) require overflow checking
- Complex type casts (nested structs) are more expensive

**Recommendation**: Users should minimize type casting by defining correct schemas upfront

### 11.3 Column Projection

Projecting away columns is essentially free in columnar storage (DuckDB):
- DuckDB only reads columns that are selected
- No performance penalty for having extra columns in input

---

## 12. Spark Compatibility Notes

### 12.1 Case Sensitivity

Spark's column name matching is controlled by `spark.sql.caseSensitive` config:
- `false` (default): Case-insensitive matching
- `true`: Case-sensitive matching

**Thunderduck Implementation**: Must respect this config setting when matching column names.

### 12.2 Spark 3.4+ Feature

The `to()` method was added in Spark 3.4. Earlier versions do not support this operation.

**Thunderduck**: This is acceptable since Thunderduck targets Spark Connect 4.0.x.

### 12.3 Spark Connect Protocol

ToSchema is a **relation**, not a command:
- It's a transformation (lazy)
- Returns a DataFrame
- Chainable with other operations

---

## Appendix A: Complete Type Mapping

| Spark Type | DuckDB Type | Cast Allowed? |
|------------|-------------|---------------|
| BooleanType | BOOLEAN | Identity |
| ByteType | TINYINT | Identity |
| ShortType | SMALLINT | Identity |
| IntegerType | INTEGER | Identity |
| LongType | BIGINT | Identity |
| FloatType | FLOAT | Identity |
| DoubleType | DOUBLE | Identity |
| DecimalType(p,s) | DECIMAL(p,s) | Identity |
| StringType | VARCHAR | Identity |
| BinaryType | BLOB | Identity |
| DateType | DATE | Identity |
| TimestampType | TIMESTAMP | Identity |
| ArrayType(T) | T[] | Recursive |
| MapType(K,V) | MAP(K,V) | Recursive |
| StructType | STRUCT | Recursive |

---

## Appendix B: Implementation Complexity

| Feature | Complexity | Estimated Effort |
|---------|-----------|------------------|
| Column reordering | Low | 1 day |
| Column projection | Low | 1 day |
| Missing column validation | Low | 1 day |
| Type casting | Medium | 2-3 days |
| Nullability validation | Medium | 1-2 days |
| Nested struct support | High | 3-4 days |
| Error messages | Low | 1 day |
| Testing | Medium | 2-3 days |
| **Total** | | **12-16 days** |

---

**End of Specification**
