# User-Defined Functions (UDFs) in DuckDB and Spark Connect

**Date:** 2025-12-16
**Status:** Research Complete
**Purpose:** Evaluate options for UDF support in Thunderduck

---

## Executive Summary

DuckDB natively supports Python UDFs with excellent performance via Arrow batching. For Thunderduck to support Spark UDFs, the recommended approach is a phased implementation: SQL macros first, then Arrow-based Python UDFs, and finally a UDF microservice for full compatibility.

---

## 1. Python UDFs in DuckDB

**Support Status**: Full native support

### Registration Methods

```python
import duckdb

# Native Python UDF (slower)
def add_one(x):
    return x + 1

con = duckdb.connect()
con.create_function("add_one", add_one, [duckdb.int64], duckdb.int64)

# Arrow UDF (15x faster - recommended)
def vectorized_add(x):  # x is a PyArrow array
    return x + 1

con.create_function("vec_add", vectorized_add, [duckdb.int64], duckdb.int64, type="arrow")
```

### UDF Types and Performance

| Type | Description | Performance (10M ops) | Use Cases |
|------|-------------|----------------------|-----------|
| **Native Python** | Process one row at a time | 5.37 seconds | General-purpose, Python libraries |
| **Arrow/PyArrow** | Process batches (up to 2,048 rows) | **0.35 seconds (15x faster)** | Production workloads |

### Key Characteristics

- Available from **SQL queries only** (registered via Python API, called via SQL)
- Support for NULL handling configuration
- Exception behavior control
- Optional side effects flag for optimization hints
- Vectorized chunk processing reduces serialization cost

---

## 2. Alternative UDF Options

### A. SQL-Based UDFs (CREATE MACRO)

```sql
-- Scalar macro
CREATE MACRO add_x(a, b) AS a + b;

-- Table macro
CREATE MACRO get_data(limit_val) AS TABLE
  SELECT * FROM my_table LIMIT limit_val;

-- Overloaded macros
CREATE MACRO add_x(a, b) AS a + b;
CREATE MACRO add_x(a, b, c) AS a + b + c;
```

**Advantages:**
- No serialization overhead
- Pure SQL execution
- Composable and overloadable

**Limitations:**
- Limited to SQL expressions
- Cannot call external libraries

### B. WebAssembly (WASM) UDFs

**Status**: Experimental/Emerging

| Aspect | Details |
|--------|---------|
| DuckDB Core | WASM support developing |
| DuckDB-Wasm (browser) | UDFs not currently supported |
| MotherDuck | Provides WASM-powered UDF support |

**Advantages:**
- Secure, sandboxed execution
- Portable across platforms
- Compiled code performance

**Limitations:**
- Requires compilation workflow
- Limited debugging
- Smaller ecosystem than Python

### C. Custom Extensions (C++/Rust)

**Status**: Production ready, but overkill for UDFs

**When to use:**
- Adding new file formats
- Domain-specific operations (geospatial, ML)
- New table functions with complex logic

**Why not for UDFs:**
- Requires C++ compilation
- Version-specific binaries
- Complex build pipeline

---

## 3. Spark Connect UDF Compatibility

### Spark UDF Registration

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

@spark.udf.register("add_one", returnType=IntegerType())
def my_udf(x):
    return x + 1

result = spark.sql("SELECT add_one(age) FROM people")
```

### Spark Connect Protocol Limitations

| Limitation | Impact |
|------------|--------|
| No direct JVM access | Cannot inspect Py4J objects |
| No RDD support | Must use DataFrames |
| Requires class upload (Scala) | Must explicitly upload classfiles |

### Python UDF Serialization in Spark

1. **Scalar Python UDFs**: Serialized via pickle, sent per batch
2. **Pandas UDFs** (Vectorized): Arrow serialization, zero-copy transfer, 10-15x faster

---

## 4. Implementation Options for Thunderduck

### Option A: Direct Python UDF Pass-Through

**Verdict**: ❌ **Not viable**

- Spark uses pickle serialization - requires full CPython runtime
- Double serialization overhead (Spark→pickle + DuckDB→Python)
- Function closures cannot reliably cross boundaries

### Option B: SQL Function Translation

**Verdict**: ✅ **Limited applicability**

Works for:
- Pure SQL expressions
- Simple arithmetic, string ops, date functions

Fails for:
- Python UDFs with libraries (numpy, pandas, ML)
- UDFs with side effects

```python
# Spark
spark.udf.register("double_it", lambda x: x * 2, returnType=IntegerType())

# Translates to DuckDB
CREATE MACRO double_it(x) AS x * 2;
```

### Option C: Arrow-Based Batch Processing

**Verdict**: ✅ **Theoretically possible, high cost**

Requirements:
- Embedded Python interpreter (Jython or GraalVM)
- Arrow serialization layer
- UDF registry and lifecycle management

Disadvantages:
- Complex implementation
- Security considerations
- GraalVM Python has limitations

### Option D: UDF Microservice (RECOMMENDED)

**Verdict**: ✅ **Most practical**

```
┌─────────────────────────────────────────────────────┐
│ Spark Client                                         │
└─────────────────────────┬───────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │ Thunderduck Spark Connect Server    │
        │ (intercepts UDF calls)              │
        └────────────┬────────────────────────┘
                     │
        ┌────────────▼────────────────────────┐
        │ UDF Execution Microservice          │
        │ (Python-based, handles UDFs)        │
        │ - Receives serialized functions     │
        │ - Executes batches via Arrow        │
        │ - Returns results                   │
        └──────────────────────────────────────┘
```

**Advantages:**
- Clean separation of concerns
- Minimal changes to Thunderduck core
- Leverages existing Python/Arrow infrastructure
- Can scale independently
- Easier debugging and testing

---

## 5. Recommended Implementation Strategy

### Phase 5A: Limited UDF Support (2-3 weeks)

1. **SQL Macros**
   - Map `spark.udf.register()` to `CREATE MACRO` for simple cases
   - Support scalar and table macros

2. **Simple Python UDFs**
   - Support "pure" Python functions (no external deps)
   - Use DuckDB's native Arrow UDF support
   - Restrictions: no pandas/numpy/ML libraries

### Phase 6: Full UDF Support (6-8 weeks)

1. **Phase 6a**: UDF Microservice POC
   - Standalone Python service with FastAPI
   - Receives serialized UDFs via Arrow
   - Deploy as separate Docker container

2. **Phase 6b**: Thunderduck Integration
   - Detect UDF registration in Spark Connect protocol
   - Route UDF calls to microservice
   - Handle serialization/deserialization

3. **Phase 6c**: Production Hardening
   - Security (sandboxing, resource limits)
   - Performance optimization
   - Monitoring and documentation

---

## 6. Summary Comparison

| Approach | Viability | Effort | Performance | When to Use |
|----------|-----------|--------|-------------|-------------|
| **SQL Macro Translation** | ✅ Medium | Low | Excellent | Simple functions |
| **Simple Python UDFs** | ✅ Medium | Low-Medium | Good | Pure Python, no deps |
| **Arrow-based Batch UDFs** | ✅ Medium | High | Excellent | Full Python support |
| **UDF Microservice** | ✅ High | Medium | Very Good | Production, scalable |
| **DuckDB Extensions** | ❌ Low | Very High | Excellent | New formats/types only |
| **Direct Pass-Through** | ❌ Very Low | High | Poor | Never |

---

## 7. Key Takeaways

1. **DuckDB has excellent native UDF support** - Arrow UDFs are 15x faster than native Python
2. **SQL macros work for simple cases** - No serialization overhead, pure SQL
3. **Full Spark compatibility requires a microservice** - Separate Python process for arbitrary UDFs
4. **WASM UDFs are not ready** - Experimental, limited tooling
5. **Extensions are overkill** - Better for new data formats, not UDFs

---

## Sources

- [DuckDB Python Function API](https://duckdb.org/docs/stable/clients/python/function)
- [DuckDB Python UDF Blog Post](https://duckdb.org/2023/07/07/python-udf)
- [DuckDB CREATE MACRO](https://duckdb.org/docs/stable/sql/statements/create_macro)
- [DuckDB Extensions Overview](https://duckdb.org/docs/stable/extensions/overview)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [PySpark UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
