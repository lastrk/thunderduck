# DataFrame API Test Strategy for ThunderDuck

## Executive Summary

The current TPC-H and TPC-DS test suites use SQL passthrough (`spark.sql()`), which bypasses ThunderDuck's critical DataFrame-to-SQL translation layer. This document proposes implementing TPC-H/TPC-DS queries using the Spark DataFrame API to achieve comprehensive testing of the translation component.

## Current Testing Gap

### What We Test Now
```python
# Current approach - SQL passthrough
result = spark.sql("SELECT * FROM lineitem WHERE l_quantity > 40")
```
**Path**: Client → gRPC → SQL Relation → Direct DuckDB execution

### What We Need to Test
```python
# DataFrame API approach
df = spark.read.parquet("lineitem.parquet")
result = df.filter(df.l_quantity > 40)
```
**Path**: Client → gRPC → Filter(Read) → RelationConverter → LogicalPlan → SQLGenerator → DuckDB

## Viability Analysis

### ✅ Advantages of DataFrame API for TPC-H/TPC-DS

1. **Complete Coverage**: Tests the entire translation stack
2. **Real-world Usage**: Most Spark users prefer DataFrame API over SQL
3. **Type Safety**: DataFrame API provides compile-time type checking
4. **Optimization Testing**: Exercises query plan optimization
5. **Reference Implementation**: Can compare against existing SQL tests

### ⚠️ Challenges

1. **Complex Queries**: Some TPC-DS queries are very complex to express in DataFrame API
2. **Window Functions**: Need careful translation of SQL window specs
3. **Subqueries**: Correlated subqueries require special handling
4. **ROLLUP/CUBE**: Group by extensions need specific API calls

## Implementation Strategy

### Phase 1: TPC-H DataFrame Implementation (22 queries)

Start with TPC-H as it's simpler and we have 100% SQL coverage for reference.

#### Example: TPC-H Q1 in DataFrame API

**SQL Version**:
```sql
SELECT l_returnflag, l_linestatus,
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity) as avg_qty,
       avg(l_extendedprice) as avg_price,
       avg(l_discount) as avg_disc,
       count(*) as count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

**DataFrame API Version**:
```python
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Load data
lineitem = spark.read.parquet("/path/to/lineitem.parquet")

# Calculate date filter
ship_date_limit = datetime(1998, 12, 1) - timedelta(days=90)

# Apply transformations
result = (lineitem
    .filter(F.col("l_shipdate") <= ship_date_limit)
    .groupBy("l_returnflag", "l_linestatus")
    .agg(
        F.sum("l_quantity").alias("sum_qty"),
        F.sum("l_extendedprice").alias("sum_base_price"),
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")))
            .alias("sum_disc_price"),
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax")))
            .alias("sum_charge"),
        F.avg("l_quantity").alias("avg_qty"),
        F.avg("l_extendedprice").alias("avg_price"),
        F.avg("l_discount").alias("avg_disc"),
        F.count("*").alias("count_order")
    )
    .orderBy("l_returnflag", "l_linestatus")
)
```

#### Example: TPC-H Q3 (Multi-table Join)

**DataFrame API Version**:
```python
# Load tables
customer = spark.read.parquet("/path/to/customer.parquet")
orders = spark.read.parquet("/path/to/orders.parquet")
lineitem = spark.read.parquet("/path/to/lineitem.parquet")

# Apply transformations
result = (customer
    .filter(F.col("c_mktsegment") == "BUILDING")
    .join(orders, customer.c_custkey == orders.o_custkey)
    .filter(F.col("o_orderdate") < "1995-03-15")
    .join(lineitem, orders.o_orderkey == lineitem.l_orderkey)
    .filter(F.col("l_shipdate") > "1995-03-15")
    .select(
        F.col("l_orderkey"),
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")))
            .alias("revenue"),
        F.col("o_orderdate"),
        F.col("o_shippriority")
    )
    .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
    .agg(F.sum("revenue").alias("revenue"))
    .orderBy(F.desc("revenue"), F.col("o_orderdate"))
    .limit(10)
)
```

### Phase 2: TPC-DS DataFrame Implementation (Selected Queries)

Focus on queries that exercise different DataFrame API features:

#### Categories to Test

1. **Basic Operations** (Q1, Q6, Q14)
   - Filter, project, aggregate
   - Tests: Filter, Project, Aggregate relations

2. **Complex Joins** (Q2, Q3, Q5, Q7)
   - Multi-way joins with filters
   - Tests: Join relation with different types

3. **Window Functions** (Q12, Q20, Q36)
   - RANK(), ROW_NUMBER(), analytical functions
   - Tests: Window relation handling

4. **Subqueries** (Q4, Q11, Q15, Q16)
   - Scalar and exists subqueries
   - Tests: Subquery transformation

5. **ROLLUP/CUBE** (Q27, Q36, Q67, Q86)
   - Grouping sets operations
   - Tests: GroupingSets relation

6. **Set Operations** (Q38, Q87)
   - UNION, INTERSECT, EXCEPT
   - Tests: SetOp relation

## Required DataFrame API Operations

### Currently Implemented ✅
- `read.parquet()`
- `filter()` / `where()`
- `select()` / `selectExpr()`
- `groupBy()` + `agg()`
- `orderBy()` / `sort()`
- `limit()`
- `join()` (inner, left, right, full, semi, anti)
- `union()`, `intersect()`, `except()`
- Basic column operations

### Need to Verify/Implement ⚠️
- Window functions (`over()`, `partitionBy()`, `orderBy()`)
- Complex aggregations (multiple agg functions)
- Date/time operations
- String functions
- Math functions
- Conditional expressions (`when()`, `otherwise()`)
- `rollup()`, `cube()`, `groupingSets()`
- Subquery support (scalar, exists, in)

## Test Implementation Plan

### 1. Create DataFrame Test Module
```
tests/integration/dataframe/
├── test_tpch_dataframe.py      # TPC-H queries in DataFrame API
├── test_tpcds_dataframe.py     # Selected TPC-DS queries
├── test_api_coverage.py        # Systematic API testing
└── reference_generator.py      # Generate expected results
```

### 2. Progressive Implementation

**Week 1**: Basic TPC-H Queries
- Q1: Basic aggregation
- Q6: Simple filter + aggregate
- Q14: Percentage calculation

**Week 2**: Join Queries
- Q3: 3-way join
- Q5: 6-way join
- Q7: Complex predicates

**Week 3**: Subqueries
- Q4: EXISTS subquery
- Q11: HAVING with subquery
- Q15: WITH clause (CTE)

**Week 4**: Advanced Features
- Window functions (if supported)
- ROLLUP/CUBE operations
- Complex expressions

### 3. Validation Strategy

Each DataFrame query should:
1. **Match SQL results exactly** (same as current SQL tests)
2. **Generate correct LogicalPlan**
3. **Produce optimal DuckDB SQL**
4. **Execute efficiently**

## Example Test Structure

```python
class TestTPCHDataFrame:
    """TPC-H queries implemented using DataFrame API"""

    def test_q1_pricing_summary(self, spark):
        """Q1: Pricing Summary Report using DataFrame API"""
        # Load data
        lineitem = spark.read.parquet(self.lineitem_path)

        # DataFrame implementation
        result_df = (lineitem
            .filter(F.col("l_shipdate") <= "1998-09-02")
            .groupBy("l_returnflag", "l_linestatus")
            .agg(
                F.sum("l_quantity").alias("sum_qty"),
                # ... more aggregations
            )
            .orderBy("l_returnflag", "l_linestatus")
        )

        # Get results
        df_results = result_df.collect()

        # Load SQL reference
        sql_reference = self.load_reference("q1")

        # Compare
        assert self.compare_results(df_results, sql_reference)
```

## Benefits of This Approach

1. **Complete Testing**: Exercises the entire ThunderDuck stack
2. **Real-world Validation**: Tests actual user patterns
3. **Regression Prevention**: Ensures translation layer remains correct
4. **Performance Benchmarking**: Can compare DataFrame vs SQL performance
5. **Documentation**: Serves as examples for users

## Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Complex query translation | Start with simple queries, progressively increase complexity |
| Missing API support | Document unsupported operations, implement as needed |
| Maintenance burden | Automate test generation where possible |
| Performance overhead | Expected - DataFrame API has translation overhead |

## Success Criteria

1. **Coverage**: 100% of TPC-H queries have DataFrame implementations
2. **Correctness**: All DataFrame tests produce identical results to SQL
3. **Performance**: Translation overhead < 100ms for most queries
4. **Completeness**: All major DataFrame API operations tested

## Recommendation

**YES - Implementing TPC-H/TPC-DS queries using DataFrame API is highly viable and recommended.**

Reasons:
1. Fills critical testing gap in translation layer
2. Matches real-world Spark usage patterns
3. Provides comprehensive API coverage validation
4. Can be implemented incrementally
5. Builds on existing SQL test infrastructure

## Next Steps

1. **Prototype**: Implement Q1, Q3, Q6 in DataFrame API
2. **Validate**: Compare results with SQL versions
3. **Expand**: Systematically cover all TPC-H queries
4. **Document**: Create DataFrame API compatibility matrix
5. **Optimize**: Identify and fix translation inefficiencies

---

*This strategy ensures ThunderDuck is tested exactly as users will use it - through the DataFrame API, not just SQL passthrough.*