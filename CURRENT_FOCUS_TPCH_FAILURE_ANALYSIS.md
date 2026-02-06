# TPC-H Failing Query Analysis

**Date:** 2026-02-06
**Queries Analyzed:** 13 failing TPC-H queries (Q1, Q4, Q7, Q8, Q9, Q12, Q13, Q14, Q16, Q17, Q18, Q21, Q22)
**Test Run:** 13 failed, 0 passed (of selected), completed in 12.26s

---

## Key Finding

**All 13 failures are schema mismatches (type and/or nullable differences), NOT value/data differences.** Every query executes successfully on both Spark and Thunderduck, and the actual result data is correct. The failures are caused by Thunderduck returning wrong column types or nullable metadata.

---

## Per-Query Failure Details

### Q1 - Pricing Summary Report
**Tables:** lineitem | **Features:** SUM, AVG, COUNT, GROUP BY

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `sum_qty` | `DecimalType(25,2)` | `DecimalType(38,2)` | Precision too wide |
| `sum_base_price` | `DecimalType(25,2)` | `DecimalType(38,2)` | Precision too wide |
| `avg_qty` | `DecimalType(19,6)` | `DoubleType()` | Wrong type |
| `avg_price` | `DecimalType(19,6)` | `DoubleType()` | Wrong type |
| `avg_disc` | `DecimalType(19,6)` | `DoubleType()` | Wrong type |
| `count_order` | nullable=`False` | nullable=`True` | Nullable mismatch |

**Root Causes:** SUM decimal precision, AVG returns Double, COUNT nullable

---

### Q4 - Order Priority Checking
**Tables:** orders, lineitem | **Features:** EXISTS subquery, COUNT, GROUP BY

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `order_count` | nullable=`False` | nullable=`True` | Nullable mismatch |

**Root Cause:** COUNT nullable

---

### Q7 - Volume Shipping
**Tables:** supplier, lineitem, orders, customer, nation (x2) | **Features:** EXTRACT(YEAR), subquery, 6-table join

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `l_year` | `IntegerType()` | `LongType()` | Wrong type |

**Root Cause:** EXTRACT(YEAR) returns LongType instead of IntegerType

---

### Q8 - National Market Share
**Tables:** part, supplier, lineitem, orders, customer, nation (x2), region | **Features:** CASE, division, EXTRACT(YEAR), 8-table join

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `o_year` | `IntegerType()` | `LongType()` | Wrong type |
| `mkt_share` | `DecimalType(38,6)` | `DoubleType()` | Wrong type |

**Root Causes:** EXTRACT(YEAR) returns Long, decimal division returns Double

---

### Q9 - Product Type Profit Measure
**Tables:** part, supplier, lineitem, partsupp, orders, nation | **Features:** EXTRACT(YEAR), arithmetic, LIKE, subquery

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `o_year` | `IntegerType()` | `LongType()` | Wrong type |

**Root Cause:** EXTRACT(YEAR) returns LongType instead of IntegerType

---

### Q12 - Shipping Modes and Order Priority
**Tables:** orders, lineitem | **Features:** SUM(CASE WHEN ... THEN 1 ELSE 0), IN, GROUP BY

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `high_line_count` | `LongType()` | `DecimalType(38,0)` | Wrong type |
| `low_line_count` | `LongType()` | `DecimalType(38,0)` | Wrong type |

**Root Cause:** SUM of integer CASE expression returns Decimal(38,0) instead of LongType

---

### Q13 - Customer Distribution
**Tables:** customer, orders | **Features:** LEFT OUTER JOIN, COUNT, GROUP BY, HAVING

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `c_count` | nullable=`False` | nullable=`True` | Nullable mismatch |
| `custdist` | nullable=`False` | nullable=`True` | Nullable mismatch |

**Root Cause:** COUNT nullable

---

### Q14 - Promotion Effect
**Tables:** lineitem, part | **Features:** CASE, SUM, decimal division

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `promo_revenue` | `DecimalType(38,6)` | `DoubleType()` | Wrong type |

**Root Cause:** Decimal division returns DoubleType instead of DecimalType

---

### Q16 - Parts/Supplier Relationship
**Tables:** partsupp, part, supplier | **Features:** NOT IN subquery, COUNT DISTINCT, GROUP BY

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `supplier_cnt` | nullable=`False` | nullable=`True` | Nullable mismatch |

**Root Cause:** COUNT nullable

---

### Q17 - Small-Quantity-Order Revenue
**Tables:** lineitem, part | **Features:** Correlated subquery, AVG, scalar division

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `avg_yearly` | `DecimalType(30,6)` | `DoubleType()` | Wrong type |

**Root Cause:** Decimal division (SUM/7.0) returns DoubleType instead of DecimalType

---

### Q18 - Large Volume Customer
**Tables:** customer, orders, lineitem | **Features:** IN subquery with HAVING, SUM, GROUP BY, LIMIT

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `sum(l_quantity)` | `DecimalType(25,2)` | `DecimalType(38,2)` | Precision too wide |

**Root Cause:** SUM decimal precision

---

### Q21 - Suppliers Who Kept Orders Waiting
**Tables:** supplier, lineitem (x3), orders, nation | **Features:** EXISTS, NOT EXISTS, self-joins

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `numwait` | nullable=`False` | nullable=`True` | Nullable mismatch |

**Root Cause:** COUNT nullable

---

### Q22 - Global Sales Opportunity
**Tables:** customer, orders | **Features:** SUBSTRING, NOT EXISTS, correlated subquery, aggregates

| Column | Spark (Expected) | Thunderduck (Actual) | Issue |
|--------|------------------|---------------------|-------|
| `numcust` | nullable=`False` | nullable=`True` | Nullable mismatch |
| `totacctbal` | `DecimalType(25,2)` | `DecimalType(38,2)` | Precision too wide |

**Root Causes:** COUNT nullable, SUM decimal precision

---

## Root Cause Consolidation

### 5 Distinct Root Causes

| # | Root Cause | Queries Affected | Total Column Mismatches | Fix Complexity |
|---|-----------|-----------------|------------------------|----------------|
| **RC1** | **COUNT nullable**: COUNT(*) / COUNT(col) marked nullable=True, should be False | Q1, Q4, Q13, Q16, Q21, Q22 | 7 columns | Low |
| **RC2** | **SUM decimal precision**: SUM on decimal columns returns Decimal(38,N) instead of computing proper precision (e.g., Decimal(25,2)) | Q1, Q18, Q22 | 4 columns | Medium |
| **RC3** | **AVG/division returns Double**: AVG on decimal columns and decimal-by-decimal division return DoubleType instead of DecimalType | Q1, Q8, Q14, Q17 | 6 columns | Medium |
| **RC4** | **EXTRACT returns Long**: EXTRACT(YEAR FROM ...) in raw SQL returns LongType instead of IntegerType | Q7, Q8, Q9 | 3 columns | Low |
| **RC5** | **SUM(CASE int) returns Decimal**: SUM(CASE WHEN ... THEN 1 ELSE 0 END) returns DecimalType(38,0) instead of LongType | Q12 | 2 columns | Medium |

### Fix Priority (by impact)

1. **RC1 - COUNT nullable** (6 queries) - Fix `TypeInferenceEngine` to mark COUNT aggregates as non-nullable. Would fix Q4, Q13, Q16, Q21 completely and partially fix Q1, Q22.

2. **RC3 - AVG/Division returns Double** (4 queries) - Fix AVG aggregate and decimal division to return DecimalType with proper precision/scale. Would fix Q14, Q17 completely and partially fix Q1, Q8.

3. **RC4 - EXTRACT returns Long** (3 queries) - The `FunctionRegistry` fix for datetime extracts was applied to DataFrame API but NOT to raw SQL `EXTRACT(YEAR FROM ...)` syntax. Would fix Q7, Q9 completely and partially fix Q8.

4. **RC2 - SUM decimal precision** (3 queries) - Match Spark's decimal precision inference for SUM: `precision = min(38, input_precision + 10)`. Would partially fix Q1, Q18, Q22.

5. **RC5 - SUM(CASE int) type** (1 query) - When SUM is applied to integer CASE expression, result should be LongType not DecimalType(38,0). Would fix Q12 completely.

### Queries Fixed Per Root Cause Resolution

| Queries | RC1 (COUNT) | RC2 (SUM prec) | RC3 (AVG/div) | RC4 (EXTRACT) | RC5 (SUM CASE) | All Fixed? |
|---------|:-----------:|:--------------:|:-------------:|:-------------:|:--------------:|:----------:|
| **Q1**  | Partial | Partial | Partial | - | - | Needs RC1+RC2+RC3 |
| **Q4**  | **FIXED** | - | - | - | - | Yes |
| **Q7**  | - | - | - | **FIXED** | - | Yes |
| **Q8**  | - | - | Partial | Partial | - | Needs RC3+RC4 |
| **Q9**  | - | - | - | **FIXED** | - | Yes |
| **Q12** | - | - | - | - | **FIXED** | Yes |
| **Q13** | **FIXED** | - | - | - | - | Yes |
| **Q14** | - | - | **FIXED** | - | - | Yes |
| **Q16** | **FIXED** | - | - | - | - | Yes |
| **Q17** | - | - | **FIXED** | - | - | Yes |
| **Q18** | - | Partial | - | - | - | Needs RC2 |
| **Q21** | **FIXED** | - | - | - | - | Yes |
| **Q22** | Partial | Partial | - | - | - | Needs RC1+RC2 |

### Estimated Fix Impact

- **Fixing RC1 alone:** 4 queries fully fixed (Q4, Q13, Q16, Q21)
- **Fixing RC1+RC4:** 7 queries fully fixed (+Q7, Q9, Q12... wait, Q12 needs RC5)
- **Fixing RC1+RC3+RC4:** 9 queries fully fixed (+Q14, Q17)
- **Fixing all 5 RCs:** All 13 queries fixed

### Where to Fix

| Root Cause | File(s) to Modify |
|-----------|-------------------|
| RC1 (COUNT nullable) | `TypeInferenceEngine.java` - `resolveAggregateNullable()` or aggregate type resolution |
| RC2 (SUM decimal precision) | `TypeInferenceEngine.java` - SUM return type precision calculation |
| RC3 (AVG/div returns Double) | `TypeInferenceEngine.java` - AVG return type; `ExpressionConverter.java` - division type |
| RC4 (EXTRACT returns Long) | `ExpressionConverter.java` or `FunctionRegistry.java` - SQL EXTRACT handling (note: DataFrame API already fixed) |
| RC5 (SUM CASE int type) | `TypeInferenceEngine.java` - SUM on integer expression type inference |

---

## Appendix: Full Schema Comparison Per Query

### Q1 Full Schema
```
Reference: [l_returnflag: string, l_linestatus: string, sum_qty: decimal(25,2),
            sum_base_price: decimal(25,2), sum_disc_price: decimal(38,4),
            sum_charge: decimal(38,6), avg_qty: decimal(19,6),
            avg_price: decimal(19,6), avg_disc: decimal(19,6), count_order: bigint]

Thunderduck: [l_returnflag: string, l_linestatus: string, sum_qty: decimal(38,2),
              sum_base_price: decimal(38,2), sum_disc_price: decimal(38,4),
              sum_charge: decimal(38,6), avg_qty: double,
              avg_price: double, avg_disc: double, count_order: bigint]
```

### Q12 Full Schema
```
Reference: [l_shipmode: string, high_line_count: bigint, low_line_count: bigint]
Thunderduck: [l_shipmode: string, high_line_count: decimal(38,0), low_line_count: decimal(38,0)]
```
