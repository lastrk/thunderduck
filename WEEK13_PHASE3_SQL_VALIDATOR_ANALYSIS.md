# Week 13 Phase 3: SQL Validator Issue Analysis

## Problem Statement

**Symptom**: SQL tests fail with "Schema mismatch: Missing columns" but DataFrame API tests pass

**Test Results**:
- DataFrame API tests: ✅ 12/12 passing
- SQL tests: ❌ 6/6 failing with schema validation errors
- **Pattern**: Only SQL tests fail, equivalent DataFrame tests pass

**Error Message**:
```
AssertionError: Schema mismatch: Missing columns: {'count_order', 'l_returnflag',
'l_linestatus', 'sum_qty', 'avg_qty', 'sum_disc_price', 'sum_base_price',
'avg_price', 'sum_charge', 'avg_disc'}
```

**Observation**: ALL expected columns are reported as "missing"

---

## Investigation

### What We Know

1. **SQL Execution Succeeds**:
   - Server logs show: "✓ Query executed: 4 rows returned"
   - No SQL execution errors
   - Temp views created successfully

2. **DataFrame API Works**:
   - Same queries via DataFrame API pass validation
   - Example: `test_q1_dataframe_api` ✅ PASSES
   - Example: `test_q1_sql` ❌ FAILS

3. **Validator Code** (`result_validator.py:60`):
   ```python
   actual_columns = df.columns
   if set(actual_columns) != set(expected_columns):
       missing = set(expected_columns) - set(actual_columns)
       extra = set(actual_columns) - set(expected_columns)
       msg = []
       if missing:
           msg.append(f"Missing columns: {missing}")
   ```

4. **All columns reported as missing** suggests:
   - `df.columns` returns empty list `[]`, OR
   - `df.columns` returns `None`, OR
   - `df.columns` triggers an error that's swallowed

---

## Hypothesis

### Most Likely Cause: Schema Access Triggers Additional Query

When `validate_schema()` is called:
1. Test calls: `validator.validate_schema(result, expected_columns)`
2. Validator accesses: `df.columns`
3. PySpark may trigger: **Additional execution to infer schema**
4. That execution fails (different error path than collect())

### Why DataFrame API Works But SQL Doesn't

**DataFrame API Path**:
```python
result = lineitem.filter(...).groupBy(...).agg(...)
# Schema is known from plan - no additional query needed
validator.validate_schema(result, cols)  # Uses plan schema
```

**SQL Path**:
```python
result = spark.sql(query)
# Schema must be inferred from SQL string
validator.validate_schema(result, cols)  # May trigger COUNT query
```

### Supporting Evidence

Looking at earlier test output:
```
tests/integration/test_tpch_queries.py:42: in test_q1_sql
    validator.validate_schema(result, expected_columns)
tests/integration/utils/result_validator.py:69: in validate_schema
    raise AssertionError(f"Schema mismatch: {', '.join(msg)}")
```

The error happens **before** `result.collect()` is called in the test (line 31).

---

##Possible Root Causes

### 1. AnalyzePlan Not Returning Proper Schema

When PySpark accesses `df.columns`, it may call `analyzePlan` RPC.

**Check**: Does our `analyzePlan()` implementation handle SQL queries properly?

**Current code** (SparkConnectServiceImpl.java:269):
```java
public void analyzePlan(AnalyzePlanRequest request, ...) {
    if (request.hasSchema()) {
        Plan plan = request.getSchema().getPlan();
        // Deserialize plan
        LogicalPlan logicalPlan = planConverter.convert(plan);
        // Get schema...
    }
}
```

**Issue**: If plan is SQL, we need to handle it differently!

### 2. SQL Plan Conversion Issues

When SQL is passed to `analyzePlan`, it may fail to convert because SQL is not a normal plan structure.

**Fix**: Add special handling for SQL in analyzePlan()

---

## Solution Approach

### Option 1: Fix analyzePlan() for SQL Queries

```java
public void analyzePlan(AnalyzePlanRequest request, ...) {
    if (request.hasSchema()) {
        Plan plan = request.getSchema().getPlan();

        // Handle SQL queries specially
        if (plan.hasRoot() && plan.getRoot().hasSql()) {
            String sql = plan.getRoot().getSql().getQuery();
            // Infer schema from SQL using LIMIT 0
            String schemaSQL = sql + " LIMIT 0";
            schema = inferSchemaFromDuckDB(schemaSQL);
        } else {
            // Normal plan deserialization
            LogicalPlan logicalPlan = planConverter.convert(plan);
            schema = logicalPlan.schema();
        }
    }
}
```

### Option 2: Fix Test Validator

Don't call `validate_schema()` before `collect()`:

```python
# Instead of:
validator.validate_schema(result, expected_columns)  # Fails
rows = result.collect()

# Do:
rows = result.collect()  # Get data first
validator.validate_schema_from_rows(rows, expected_columns)  # Validate after
```

### Option 3: Make validate_schema() More Robust

```python
def validate_schema(self, df, expected_columns):
    try:
        actual_columns = df.columns
    except Exception as e:
        # Fall back to collecting and getting columns from rows
        rows = df.collect()
        if rows:
            actual_columns = list(rows[0].asDict().keys())
        else:
            actual_columns = []
    # ... rest of validation
```

---

## Recommended Fix

**Short-term (QUICK FIX)**:
- Update validator to be defensive and handle SQL queries
- Fall back to collecting if `.columns` fails

**Long-term (PROPER FIX)**:
- Fix `analyzePlan()` to handle SQL queries properly
- Ensure schema inference works for SQL strings

---

## Action Items

1. ✅ Document the issue
2. ⏳ Test hypothesis with focused debug script
3. ⏳ Implement quick fix in validator
4. ⏳ Test all SQL tests again
5. ⏳ Implement proper fix in analyzePlan() if needed

---

**Analysis Date**: 2025-10-26
**Status**: Investigation in progress
**Impact**: Medium (functionality works, validation fails)
**Priority**: High (blocks full TPC-H testing)
