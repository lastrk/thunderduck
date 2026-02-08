# ThunderDuck Supported Operations

A quick reference of Spark DataFrame API operations supported by ThunderDuck.

## DataFrame Transformations

| Operation | Status | Notes |
|-----------|--------|-------|
| `select()` | ✅ | Column selection and expressions |
| `filter()` / `where()` | ✅ | Row filtering |
| `distinct()` / `dropDuplicates()` | ✅ | Deduplication |
| `groupBy()` | ✅ | Grouping for aggregations |
| `agg()` | ✅ | Aggregation functions |
| `join()` | ✅ | INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI |
| `union()` / `unionAll()` | ✅ | Combining DataFrames |
| `orderBy()` / `sort()` | ✅ | Sorting |
| `limit()` | ✅ | Row limiting |
| `withColumn()` | ✅ | Adding/replacing columns |

## Window Functions

| Function | Status |
|----------|--------|
| `row_number()` | ✅ |
| `rank()` | ✅ |
| `dense_rank()` | ✅ |
| `percent_rank()` | ✅ |
| `lag()` / `lead()` | ✅ |
| `first_value()` / `last_value()` | ✅ |
| Window specs: `partitionBy()`, `orderBy()`, `rowsBetween()`, `rangeBetween()` | ✅ |

## Aggregate Functions

| Function | Status |
|----------|--------|
| `count()`, `sum()`, `avg()`, `min()`, `max()` | ✅ |
| `stddev()`, `variance()` | ✅ |
| `collect_list()`, `collect_set()` | ✅ |
| `first()`, `last()` | ✅ |
| Conditional aggregations with `when().otherwise()` | ✅ |

## String Functions

| Function | Status | DuckDB Equivalent |
|----------|--------|-------------------|
| `substring()`, `concat()`, `length()` | ✅ | Same |
| `upper()`, `lower()`, `trim()` | ✅ | Same |
| `startswith()` | ✅ | `starts_with()` |
| `endswith()` | ✅ | `ends_with()` |
| `contains()` | ✅ | `contains()` |
| `rlike()` | ✅ | `regexp_matches()` |
| `regexp_replace()`, `regexp_extract()` | ✅ | Same |

## Date/Time Functions

| Function | Status |
|----------|--------|
| `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()` | ✅ |
| `date_add()`, `date_sub()`, `datediff()` | ✅ |
| `to_date()`, `to_timestamp()` | ✅ |
| `current_date()`, `current_timestamp()` | ✅ |

## Advanced Transformations

| Operation | Status | Notes |
|-----------|--------|-------|
| `pivot()` | ✅ | With sum, avg, max/min, multiple aggregations |
| `unpivot()` / `melt()` | ✅ | Wide to long format via DuckDB native UNPIVOT |
| `cube()` | ✅ | With `grouping()`, `grouping_id()` |
| `rollup()` | ✅ | Hierarchical aggregation with `grouping()` |
| `sample()` | ✅ | Bernoulli sampling via DuckDB USING SAMPLE |
| `na.drop()` / `na.fill()` / `na.replace()` | ✅ | Null handling operations |
| `alias()` | ✅ | DataFrame aliasing for self-joins |
| `to(schema)` | ✅ | Column reordering, projection, type casting |
| `like()` | ✅ | Infix LIKE syntax |

## Known Limitations

| Feature | Status | Notes |
|---------|--------|-------|
| UDFs (User Defined Functions) | ❌ | Use SQL expressions instead |
| Spark ML Integration | ❌ | Use separate ML framework |
| Streaming Operations | ❌ | Batch-only by design |
| `sample(withReplacement=True)` | ❌ | DuckDB has no Poisson sampling |

## Migration from Spark

```python
# Spark (original)
spark = SparkSession.builder \
    .master("spark://master:7077") \
    .getOrCreate()

# ThunderDuck (drop-in replacement)
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

# Your DataFrame code works unchanged
df = spark.read.parquet("data.parquet")
result = df.filter(col("status") == "active") \
           .groupBy("category") \
           .agg(count("*").alias("count"))
```

---

*See [SPARK_CONNECT_GAP_ANALYSIS.md](SPARK_CONNECT_GAP_ANALYSIS.md) for detailed coverage gaps.*
