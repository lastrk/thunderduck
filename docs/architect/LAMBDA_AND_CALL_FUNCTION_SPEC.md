# LambdaFunction and CallFunction Expression Specification

**Version:** 1.0
**Date:** 2025-12-16
**Status:** Specification
**Related:** [Spark Connect Gap Analysis](../../CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md)

---

## Table of Contents

1. [Overview](#overview)
2. [LambdaFunction Expression](#lambdafunction-expression)
3. [CallFunction Expression](#callfunction-expression)
4. [DuckDB Mapping](#duckdb-mapping)
5. [Implementation Strategy](#implementation-strategy)
6. [Testing Requirements](#testing-requirements)
7. [References](#references)

---

## Overview

This document specifies the behavior, semantics, and DuckDB mapping for two critical Spark Connect expression types that enable higher-order functions and dynamic function invocation:

- **LambdaFunction**: Represents anonymous functions (lambdas) used in higher-order array/map operations
- **CallFunction**: Represents dynamic function calls, primarily for user-defined functions (UDFs)

### Key Distinctions

| Expression | Purpose | Resolution | Typical Use Case |
|------------|---------|------------|------------------|
| **UnresolvedFunction** | Built-in or catalog functions | Server-side name resolution | `sum()`, `max()`, `concat()` |
| **CallFunction** | Dynamic/runtime function calls | Direct invocation by name | UDFs, dynamic `call_function()` |
| **LambdaFunction** | Anonymous inline functions | Variable scoping within parent function | `transform(arr, x -> x + 1)` |

---

## LambdaFunction Expression

### 1.1 What is LambdaFunction?

A **LambdaFunction** represents an anonymous function (lambda) used in higher-order functions that operate on arrays, maps, and other complex data types. Lambda functions allow inline transformation logic without requiring UDF registration.

**Introduced in:** Spark 2.4.0
**Protocol Location:** `spark.connect.Expression.LambdaFunction` (line 350-359 in expressions.proto)

### 1.2 Protobuf Definition

```protobuf
message LambdaFunction {
  // (Required) The lambda function.
  //
  // The function body should use 'UnresolvedAttribute' as arguments, the sever side will
  // replace 'UnresolvedAttribute' with 'UnresolvedNamedLambdaVariable'.
  Expression function = 1;

  // (Required) Function variables. Must contains 1 ~ 3 variables.
  repeated Expression.UnresolvedNamedLambdaVariable arguments = 2;
}

message UnresolvedNamedLambdaVariable {
  // (Required) a list of name parts for the variable. Must not be empty.
  repeated string name_parts = 1;
}
```

**Key Fields:**
- `function`: The lambda body expression (e.g., `x + 1`)
- `arguments`: Lambda parameter names (e.g., `[x]` or `[acc, x]`), 1-3 variables

### 1.3 Lambda Syntax

**Spark SQL Syntax:**
```sql
-- Single parameter (parentheses optional)
SELECT transform(array(1, 2, 3), x -> x * x);

-- Multiple parameters
SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x);

-- With index parameter
SELECT transform(array(10, 20, 30), (x, i) -> x + i);
```

**PySpark DataFrame API:**
```python
from pyspark.sql.functions import transform, col

# Lambda with expr()
df.withColumn("doubled", expr("transform(values, x -> x * 2)"))

# Lambda with Python function (PySpark 3.1+)
df.select(transform(col("values"), lambda x: x * 2))
```

### 1.4 Higher-Order Functions Using Lambdas

| Function | Signature | Description | Example |
|----------|-----------|-------------|---------|
| **transform** | `transform(array, lambda)` | Apply function to each element | `transform(arr, x -> x + 1)` |
| **filter** | `filter(array, lambda)` | Keep elements matching condition | `filter(arr, x -> x > 0)` |
| **aggregate** | `aggregate(array, init, merge[, finish])` | Reduce array to single value | `aggregate(arr, 0, (acc, x) -> acc + x)` |
| **exists** | `exists(array, lambda)` | Check if any element matches | `exists(arr, x -> x > 10)` |
| **forall** | `forall(array, lambda)` | Check if all elements match | `forall(arr, x -> x > 0)` |
| **zip_with** | `zip_with(arr1, arr2, lambda)` | Merge two arrays element-wise | `zip_with(a, b, (x, y) -> x + y)` |
| **map_filter** | `map_filter(map, lambda)` | Filter map entries | `map_filter(m, (k, v) -> v > 0)` |
| **map_zip_with** | `map_zip_with(m1, m2, lambda)` | Merge two maps | `map_zip_with(m1, m2, (k, v1, v2) -> v1 + v2)` |
| **transform_keys** | `transform_keys(map, lambda)` | Transform map keys | `transform_keys(m, (k, v) -> upper(k))` |
| **transform_values** | `transform_values(map, lambda)` | Transform map values | `transform_values(m, (k, v) -> v * 2)` |

**Source:** [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html), [Databricks Higher-Order Functions](https://www.databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html)

### 1.5 Lambda Scoping and Variable References

**Client-Side Processing:**
1. Client sends lambda with `UnresolvedAttribute` for parameters
2. Example: `transform(arr, x -> x + 1)` where `x` is `UnresolvedAttribute("x")`

**Server-Side Processing:**
1. Server replaces `UnresolvedAttribute` with `UnresolvedNamedLambdaVariable`
2. Variables are scoped to the lambda body
3. Nested lambdas create nested scopes

**Example with nested lambdas:**
```sql
SELECT transform(
  list_of_arrays,
  arr -> transform(arr, x -> x * 2)
)
-- 'arr' scoped to outer lambda
-- 'x' scoped to inner lambda
```

### 1.6 Type Inference

Lambda parameter types are inferred from:
1. **Input array/map element type** (for single-argument lambdas)
2. **Accumulator type** (for aggregate's merge function)
3. **Context** (for zip_with, both array element types)

**Example:**
```sql
-- arr: ARRAY<INT>
transform(arr, x -> x + 1)
-- x inferred as INT
-- Result: ARRAY<INT>

-- arr: ARRAY<DOUBLE>
transform(arr, x -> x * 2.5)
-- x inferred as DOUBLE
-- Result: ARRAY<DOUBLE>
```

---

## CallFunction Expression

### 2.1 What is CallFunction?

**CallFunction** represents a dynamic function call where the function is invoked directly by name without going through Spark's function resolution mechanism. This is primarily used for:

1. **User-Defined Functions (UDFs)** - Custom functions registered at runtime
2. **Dynamic function invocation** - Using `call_function()` API in PySpark
3. **Resolved functions** - Functions already resolved by the client

**Protocol Location:** `spark.connect.Expression.CallFunction` (line 432-438 in expressions.proto)

### 2.2 Protobuf Definition

```protobuf
message CallFunction {
  // (Required) Unparsed name of the SQL function.
  string function_name = 1;

  // (Optional) Function arguments. Empty arguments are allowed.
  repeated Expression arguments = 2;
}
```

**Key Differences from UnresolvedFunction:**

| Field | UnresolvedFunction | CallFunction |
|-------|-------------------|--------------|
| Resolution | Server resolves via catalog | Direct invocation by name |
| `is_distinct` | Yes | No (must be in function name) |
| `is_user_defined_function` | Yes | No (assumed) |
| `is_internal` | Yes | No |
| Use Case | Built-in/catalog functions | UDFs, dynamic calls |

### 2.3 Use Cases

#### Use Case 1: Dynamic Function Calls (PySpark)

```python
from pyspark.sql.functions import call_function, col

# Call function by name variable
func_name = "upper"
df.select(call_function(func_name, col("name")))

# Configurable transformations
def apply_transform(df, func, col_name):
    return df.withColumn(col_name, call_function(func, col(col_name)))
```

**Source:** [TechBrothersIT PySpark call_function()](https://www.techbrothersit.com/2025/07/how-to-use-callfunction-in-pyspark.html)

#### Use Case 2: User-Defined Functions

```python
# Register UDF
spark.udf.register("my_custom_func", lambda x: x * 2, IntegerType())

# Call via CallFunction (after client resolution)
df.selectExpr("my_custom_func(value)")
```

#### Use Case 3: Pre-resolved Functions

When the client already knows the exact function to call (e.g., after catalog lookup), it can send `CallFunction` directly instead of `UnresolvedFunction`.

### 2.4 CallFunction vs UnresolvedFunction

**When to use UnresolvedFunction:**
- Standard SQL functions: `sum()`, `count()`, `concat()`
- Built-in DataFrame functions: `col()`, `lit()`, `when()`
- Functions requiring catalog resolution

**When to use CallFunction:**
- Dynamic function names from variables
- UDFs with known names
- Client-side resolved functions
- Functions requiring runtime dispatch

---

## DuckDB Mapping

### 3.1 DuckDB Lambda Support

DuckDB supports lambda functions natively with **Python-style syntax** (as of DuckDB 1.3.0):

**New Syntax (DuckDB 1.3.0+):**
```sql
-- Python-style lambda (PREFERRED)
SELECT list_transform([1, 2, 3], lambda x: x + 1);

-- Multiple parameters
SELECT list_filter([1, 2, 3, 4], lambda x, i: x > i);
```

**Old Syntax (deprecated, removed in DuckDB 1.7.0):**
```sql
-- Arrow syntax (DEPRECATED)
SELECT list_transform([1, 2, 3], x -> x + 1);
```

**Configuration:**
```sql
-- Control lambda syntax (DuckDB 1.3.0 - 1.6.x)
SET lambda_syntax = 'DEFAULT';  -- Python-style only
SET lambda_syntax = 'ENABLE_SINGLE_ARROW';  -- Allow both
SET lambda_syntax = 'DISABLE_SINGLE_ARROW';  -- Python-style only (future default)
```

**Source:** [DuckDB 1.3.0 Release](https://duckdb.org/2025/05/21/announcing-duckdb-130), [DuckDB Lambda Functions](https://duckdb.org/docs/stable/sql/functions/lambda)

### 3.2 DuckDB Higher-Order Functions

| Spark Function | DuckDB Equivalent | Syntax | Notes |
|----------------|-------------------|--------|-------|
| `transform` | `list_transform` | `list_transform(list, lambda x: expr)` | Also aliased as `apply` |
| `filter` | `list_filter` | `list_filter(list, lambda x: predicate)` | Returns matching elements |
| `aggregate` | `list_reduce` | `list_reduce(list, lambda acc, x: expr)` | Binary reduction |
| `aggregate` | `list_aggregate` | `list_aggregate(list, 'sum')` | Named aggregate functions |
| `exists` | ❌ Not native | `list_any(list_transform(...))` | Emulate with `list_any` |
| `forall` | ❌ Not native | `list_all(list_transform(...))` | Emulate with `list_all` |
| `zip_with` | ❌ Not native | Nested `list_transform` | Complex emulation |
| `map_filter` | ❌ Not native | Convert to/from lists | No native map lambdas |
| `transform_keys` | ❌ Not native | Convert to/from lists | No native map lambdas |
| `transform_values` | ❌ Not native | Convert to/from lists | No native map lambdas |

**Source:** [DuckDB List Functions](https://duckdb.org/docs/stable/sql/functions/list), [Friendly Lists and Lambdas](https://duckdb.org/2024/08/08/friendly-lists-and-their-buddies-the-lambdas)

### 3.3 Syntax Translation Strategy

#### Option A: Convert Spark Lambda to DuckDB Python-Style

```java
// Spark: transform(arr, x -> x + 1)
// DuckDB: list_transform(arr, lambda x: x + 1)

String sparkLambda = "x -> x + 1";
String duckdbLambda = convertLambdaToDuckDB(sparkLambda);
// Result: "lambda x: x + 1"
```

**Implementation:**
1. Parse Spark lambda: `<params> -> <body>`
2. Generate DuckDB lambda: `lambda <params>: <body>`
3. Handle parentheses: `(x, y) -> expr` becomes `lambda x, y: expr`

#### Option B: Use DuckDB Arrow Syntax (if enabled)

```sql
-- Requires: SET lambda_syntax = 'ENABLE_SINGLE_ARROW';
SELECT list_transform([1, 2, 3], x -> x + 1);
```

**Recommendation:** Use Option A (Python-style) for future-proofing, as arrow syntax will be removed in DuckDB 1.7.0.

### 3.4 List Comprehension Alternative

DuckDB supports list comprehensions as syntactic sugar:

```sql
-- Lambda
SELECT list_transform(list_filter([1, 2, 3, 4], lambda x: x % 2 = 0), lambda x: x * x);

-- List comprehension (equivalent)
SELECT [x * x FOR x IN [1, 2, 3, 4] IF x % 2 = 0];
```

**Translation:**
- `transform(filter(arr, p), f)` → `[f(x) FOR x IN arr IF p(x)]`
- More concise but less flexible (no aggregate, zip_with, etc.)

### 3.5 Emulating Missing Functions

#### Emulate `exists`:
```sql
-- Spark: exists(arr, x -> x > 10)
-- DuckDB:
SELECT list_any(list_transform(arr, lambda x: x > 10))
```

#### Emulate `forall`:
```sql
-- Spark: forall(arr, x -> x > 0)
-- DuckDB:
SELECT list_all(list_transform(arr, lambda x: x > 0))
```

#### Emulate `zip_with`:
```sql
-- Spark: zip_with(a, b, (x, y) -> x + y)
-- DuckDB:
SELECT list_transform(
  range(1, len(a) + 1),
  lambda i: a[i] + b[i]
)
```

**Note:** Index-based emulation requires both arrays to have the same length.

### 3.6 DuckDB Function Registry

For `CallFunction`, DuckDB provides a function registry queryable via:

```sql
SELECT * FROM duckdb_functions() WHERE function_name = 'my_func';
```

**Function Types:**
- **Scalar functions**: `sum`, `avg`, `concat`, etc.
- **Aggregate functions**: `count`, `max`, `list`, etc.
- **Table functions**: `read_csv`, `read_parquet`, etc.
- **Macro functions**: User-defined SQL macros

**UDF Support:**
- **Scalar UDFs**: Not directly supported (use macros)
- **Aggregate UDFs**: Limited support via CREATE MACRO AGGREGATE
- **Python UDFs**: Via DuckDB Python API (not gRPC-compatible)

---

## Implementation Strategy

### 4.1 LambdaFunction Implementation

#### Phase 1: Basic Lambda Support

**Components:**
1. **LambdaExpression class** (new)
   - Fields: `List<String> parameters`, `Expression body`
   - Methods: `toSQL()`, `validate()`

2. **LambdaVariableExpression class** (new)
   - Represents lambda parameters in body
   - Replaces `UnresolvedNamedLambdaVariable` after conversion

3. **ExpressionConverter updates**
   ```java
   case LAMBDA_FUNCTION:
       return convertLambdaFunction(expr.getLambdaFunction());
   ```

4. **convertLambdaFunction implementation**
   ```java
   private Expression convertLambdaFunction(LambdaFunction lambda) {
       // Extract parameter names
       List<String> params = lambda.getArgumentsList().stream()
           .map(arg -> String.join(".", arg.getNamePartsList()))
           .collect(Collectors.toList());

       // Convert body (replace UnresolvedAttribute with LambdaVariableExpression)
       Expression body = convertLambdaBody(lambda.getFunction(), params);

       return new LambdaExpression(params, body);
   }
   ```

5. **SQL Generation**
   ```java
   @Override
   public String toSQL() {
       // Generate DuckDB Python-style lambda
       String paramList = String.join(", ", parameters);
       return String.format("lambda %s: %s", paramList, body.toSQL());
   }
   ```

#### Phase 2: Higher-Order Function Mapping

**FunctionRegistry additions:**
```java
// Spark -> DuckDB function name mapping
private static final Map<String, String> HIGHER_ORDER_FUNCTIONS = Map.of(
    "transform", "list_transform",
    "filter", "list_filter",
    "aggregate", "list_reduce",
    "exists", "list_any_transform",  // Custom wrapper
    "forall", "list_all_transform",  // Custom wrapper
    "zip_with", "zip_with_emulated"  // Custom emulation
);
```

**Example conversion:**
```java
// Input: transform(array(1, 2, 3), x -> x + 1)
// Output: list_transform(array(1, 2, 3), lambda x: x + 1)

private Expression convertHigherOrderFunction(String sparkFunc,
                                               List<Expression> args) {
    String duckdbFunc = HIGHER_ORDER_FUNCTIONS.get(sparkFunc);

    // Transform arguments (convert last arg if lambda)
    List<Expression> duckdbArgs = new ArrayList<>();
    for (int i = 0; i < args.size(); i++) {
        Expression arg = args.get(i);
        if (i == args.size() - 1 && arg instanceof LambdaExpression) {
            // Lambda already converted to DuckDB syntax
            duckdbArgs.add(arg);
        } else {
            duckdbArgs.add(arg);
        }
    }

    return new FunctionCall(duckdbFunc, duckdbArgs, inferReturnType(sparkFunc));
}
```

#### Phase 3: Nested Lambda Support

**Variable Scoping:**
```java
class LambdaScope {
    private Map<String, LambdaVariableExpression> variables;
    private LambdaScope parent;

    public LambdaVariableExpression resolve(String name) {
        if (variables.containsKey(name)) {
            return variables.get(name);
        } else if (parent != null) {
            return parent.resolve(name);
        } else {
            throw new PlanConversionException("Unresolved lambda variable: " + name);
        }
    }
}
```

### 4.2 CallFunction Implementation

#### Phase 1: Basic CallFunction Support

**ExpressionConverter updates:**
```java
case CALL_FUNCTION:
    return convertCallFunction(expr.getCallFunction());
```

**Implementation:**
```java
private Expression convertCallFunction(CallFunction callFunc) {
    String functionName = callFunc.getFunctionName();
    List<Expression> arguments = callFunc.getArgumentsList().stream()
        .map(this::convert)
        .collect(Collectors.toList());

    // Check if function exists in DuckDB
    if (!functionExists(functionName)) {
        throw new PlanConversionException(
            "Function not found in DuckDB: " + functionName);
    }

    // Map function name if needed
    String duckdbName = mapFunctionName(functionName);

    return new FunctionCall(duckdbName, arguments, inferReturnType(duckdbName));
}
```

#### Phase 2: UDF Support via Macros

DuckDB macros can emulate simple UDFs:

```sql
-- Create macro (equivalent to registering UDF)
CREATE MACRO double_value(x) AS x * 2;

-- Call macro
SELECT double_value(10);  -- Returns 20
```

**Session-level UDF registry:**
```java
class Session {
    private Map<String, String> udfMacros = new HashMap<>();

    public void registerUDF(String name, String definition) {
        // Store UDF as macro definition
        udfMacros.put(name, definition);

        // Create macro in DuckDB
        String createMacro = String.format(
            "CREATE MACRO %s AS %s",
            name,
            definition
        );
        executor.execute(createMacro);
    }

    public boolean isUDF(String name) {
        return udfMacros.containsKey(name);
    }
}
```

**Limitations:**
- ❌ Python UDFs not supported (requires Python runtime)
- ❌ Scala UDFs not supported (requires JVM serialization)
- ❌ Java UDFs not supported (DuckDB has no JVM interface)
- ✅ SQL-based UDFs supported via macros
- ✅ Built-in function calls fully supported

#### Phase 3: Function Existence Validation

```java
private boolean functionExists(String functionName) {
    String query = String.format(
        "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = '%s'",
        escapeSql(functionName)
    );

    try (VectorSchemaRoot result = executor.executeQuery(query)) {
        if (result.getRowCount() > 0) {
            BigIntVector vec = (BigIntVector) result.getVector(0);
            return vec.get(0) > 0;
        }
    }

    return false;
}
```

### 4.3 Error Handling

**Common Errors:**

| Error | Cause | Solution |
|-------|-------|----------|
| `Unresolved lambda variable: x` | Variable used outside lambda scope | Fix client-side lambda generation |
| `Function not found: my_udf` | UDF not registered | Register UDF or use macro |
| `Lambda parameter count mismatch` | Wrong number of params for function | Check higher-order function signature |
| `Nested lambda scope error` | Variable shadowing issue | Use unique variable names |

**Error Messages:**
```java
throw new PlanConversionException(
    String.format(
        "LambdaFunction requires %d parameters for %s, got %d",
        expectedParams, functionName, actualParams
    )
);
```

---

## Testing Requirements

### 5.1 Unit Tests

**Test: Basic Lambda Conversion**
```java
@Test
public void testLambdaConversion() {
    // Spark: transform(array(1, 2, 3), x -> x + 1)
    Expression lambda = createLambda("x", add(var("x"), lit(1)));
    Expression transform = createFunction("transform",
        createArray(1, 2, 3), lambda);

    String sql = transform.toSQL();
    assertEquals("list_transform([1, 2, 3], lambda x: x + 1)", sql);
}
```

**Test: Nested Lambda**
```java
@Test
public void testNestedLambda() {
    // Spark: transform(arrays, arr -> transform(arr, x -> x * 2))
    Expression innerLambda = createLambda("x", multiply(var("x"), lit(2)));
    Expression innerTransform = createFunction("transform", var("arr"), innerLambda);
    Expression outerLambda = createLambda("arr", innerTransform);
    Expression outerTransform = createFunction("transform", col("arrays"), outerLambda);

    String sql = outerTransform.toSQL();
    assertContains(sql, "lambda arr:");
    assertContains(sql, "lambda x:");
}
```

**Test: CallFunction**
```java
@Test
public void testCallFunction() {
    CallFunction callFunc = CallFunction.newBuilder()
        .setFunctionName("upper")
        .addArguments(createLiteral("hello"))
        .build();

    Expression expr = converter.convert(
        Expression.newBuilder().setCallFunction(callFunc).build()
    );

    assertEquals("upper('hello')", expr.toSQL());
}
```

### 5.2 Integration Tests

**Test: E2E Higher-Order Functions**
```python
def test_transform_lambda():
    df = spark.createDataFrame([(1, [1, 2, 3])], ("id", "values"))
    result = df.select(
        transform(col("values"), lambda x: x * 2).alias("doubled")
    ).collect()

    assert result[0]["doubled"] == [2, 4, 6]

def test_filter_lambda():
    df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("id", "values"))
    result = df.select(
        filter(col("values"), lambda x: x % 2 == 0).alias("evens")
    ).collect()

    assert result[0]["evens"] == [2, 4]

def test_aggregate_lambda():
    df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("id", "values"))
    result = df.select(
        aggregate(
            col("values"),
            lit(0),
            lambda acc, x: acc + x
        ).alias("sum")
    ).collect()

    assert result[0]["sum"] == 10
```

**Test: CallFunction with UDF**
```python
def test_call_function_udf():
    spark.udf.register("double", lambda x: x * 2, IntegerType())

    df = spark.createDataFrame([(1, 10), (2, 20)], ("id", "value"))
    result = df.selectExpr("double(value) as doubled").collect()

    assert result[0]["doubled"] == 20
    assert result[1]["doubled"] == 40
```

### 5.3 Differential Tests

**Test Matrix:**

| Spark Function | Test Query | Expected Result | DuckDB Mapping |
|----------------|------------|----------------|----------------|
| `transform` | `transform([1,2,3], x -> x+1)` | `[2, 3, 4]` | `list_transform` |
| `filter` | `filter([1,2,3,4], x -> x>2)` | `[3, 4]` | `list_filter` |
| `aggregate` | `aggregate([1,2,3], 0, (a,x)->a+x)` | `6` | `list_reduce` |
| `exists` | `exists([1,2,3], x -> x>2)` | `true` | `list_any + list_transform` |
| `forall` | `forall([1,2,3], x -> x>0)` | `true` | `list_all + list_transform` |

**Differential Test Implementation:**
```python
# tests/integration/test_lambda_functions.py

def test_transform_differential():
    query = """
    SELECT id, transform(values, x -> x * 2) as doubled
    FROM (SELECT 1 as id, array(1, 2, 3) as values)
    """

    spark_result = execute_spark(query)
    thunderduck_result = execute_thunderduck(query)

    assert_results_equal(spark_result, thunderduck_result)
```

---

## References

### Spark Documentation
- [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)
- [Introducing New Built-in and Higher-Order Functions](https://www.databricks.com/blog/2018/11/16/introducing-new-built-in-functions-and-higher-order-functions-for-complex-data-types-in-apache-spark.html)
- [Higher-Order Functions (Databricks)](https://docs.databricks.com/aws/en/semi-structured/higher-order-functions)
- [Spark Higher-Order Functions Blog](https://blog.genuine.com/2021/02/spark-higher-order-functions/)
- [PySpark transform() API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html)
- [TechBrothersIT: call_function() in PySpark](https://www.techbrothersit.com/2025/07/how-to-use-callfunction-in-pyspark.html)

### DuckDB Documentation
- [DuckDB Lambda Functions](https://duckdb.org/docs/stable/sql/functions/lambda)
- [DuckDB List Functions](https://duckdb.org/docs/stable/sql/functions/list)
- [Friendly Lists and Their Buddies, the Lambdas](https://duckdb.org/2024/08/08/friendly-lists-and-their-buddies-the-lambdas)
- [DuckDB 1.3.0 Release Notes](https://duckdb.org/2025/05/21/announcing-duckdb-130)
- [Lambda Functions in DuckDB (Medium)](https://medium.com/@thomas_reid/lambda-functions-in-duckdb-7ed8542e3c0b)
- [Using Higher-Order Functions in DuckDB (Medium)](https://medium.com/@marvin_data/using-higher-order-functions-in-duckdb-5c6103cf6ca5)

### Spark Internals
- [UnresolvedFunction (Spark Catalyst)](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-Expression-UnresolvedFunction.html)
- [ResolveFunctions Analyzer Rule](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-Analyzer-ResolveFunctions.html)
- [Spark Higher-Order Functions Test Suite](https://github.com/apache/spark/blob/master/sql/core/src/test/resources/sql-tests/inputs/higher-order-functions.sql)

### Thunderduck Internal Docs
- [Spark Connect Gap Analysis](../../CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md)
- [ExpressionConverter.java](../../connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java)
- [expressions.proto](../../connect-server/src/main/proto/spark/connect/expressions.proto)

---

## Appendix: DuckDB Lambda Syntax Evolution

### Timeline

| Version | Lambda Syntax | Status |
|---------|---------------|--------|
| DuckDB < 1.3.0 | `x -> x + 1` (arrow) | Only option |
| DuckDB 1.3.0 - 1.6.x | `lambda x: x + 1` (Python) | Preferred, arrow deprecated |
| DuckDB 1.7.0+ | `lambda x: x + 1` (Python) | Only option (arrow removed) |

### Migration Guide

**Old Code (deprecated):**
```sql
SELECT list_transform([1, 2, 3], x -> x + 1);
```

**New Code (recommended):**
```sql
SELECT list_transform([1, 2, 3], lambda x: x + 1);
```

**Compatibility Layer (DuckDB 1.3-1.6):**
```sql
SET lambda_syntax = 'ENABLE_SINGLE_ARROW';  -- Allow both syntaxes
```

**Thunderduck Strategy:**
- Target DuckDB Python-style syntax for all lambda generation
- Ensures forward compatibility with DuckDB 1.7.0+
- No need to maintain two syntax generators

---

**End of Specification**
