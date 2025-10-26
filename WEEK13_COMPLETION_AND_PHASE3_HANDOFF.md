# Week 13 Phase 2 Complete + Phase 3 Ready to Start

## 🎉 Phase 2 Complete - Summary

### ✅ All Objectives Achieved

**1. BUILD ISSUES RESOLVED**
- Fixed Java version mismatch (11 → 17)
- All modules compile cleanly
- Server JAR: 105 MB, properly compiled
- Commit: `dc3155a`

**2. TPC-H DATA GENERATED**
- Scale Factor: 0.01 (2.87 MB Parquet)
- 8 tables, 86,705 total rows
- Location: `/workspace/data/tpch_sf001/`

**3. DIFFERENTIAL TEST FRAMEWORK CREATED**
- `test_differential_tpch.py` - All 22 TPC-H queries structured
- `test_differential_simple.py` - 3 basic differential tests
- `DifferentialComparator` class for result validation
- Schema + data comparison with epsilon tolerance

**4. INTEGRATION TESTS: 10/10 PASSING** ✅
- Simple SQL: 3/3
- Basic DataFrame operations: 7/7
- Server functional and tested

---

## 📋 Phase 3 Ready to Start

### Current Status
- ✅ Server operational
- ✅ Basic queries working
- ✅ TPC-H data ready
- ✅ Test framework ready
- ❌ COMMAND plan type not yet implemented (blocks temp views)

### Phase 3 Goal
**Implement COMMAND plan type support** to enable:
- `createOrReplaceTempView()` functionality
- Full TPC-H query execution
- Complete differential testing (all 22 queries)

---

## 🔍 Analysis Complete - Ready to Code

### COMMAND Plan Type Structure (Analyzed)

**Protobuf Structure**:
```protobuf
Plan {
  oneof op_type {
    Relation root = 1;        // Query operations
    Command command = 2;       // Command operations
  }
}

Command {
  oneof command_type {
    SqlCommand sql_command = 1;
    CreateDataFrameViewCommand create_dataframe_view = 4;
    // ... other commands
  }
}

CreateDataFrameViewCommand {
  Relation input = 1;    // The DataFrame plan
  string name = 2;        // View name (e.g., "lineitem")
  bool is_temp = 3;      // Always true for temp views
  bool replace = 4;      // True for createOrReplaceTemp

View
}
```

**Generated Java Classes** (Found):
- `/workspace/connect-server/target/generated-sources/protobuf/java/org/apache/spark/connect/proto/Command.java`
- `/workspace/connect-server/target/generated-sources/protobuf/java/org/apache/spark/connect/proto/CreateDataFrameViewCommand.java`

**Current Code Location**:
- File: `SparkConnectServiceImpl.java`
- Method: `executePlan()`
- Lines: 125-129 - Handles SQL commands only
- Line 164-170 - Returns UNIMPLEMENTED for unknown plan types

---

## 🚀 Implementation Roadmap

### Step 1: Add COMMAND Handling to executePlan()
**File**: `SparkConnectServiceImpl.java`
**Location**: Lines 125-170

**Current Code** (lines 125-129):
```java
} else if (plan.hasCommand() && plan.getCommand().hasSqlCommand()) {
    // SQL command (e.g., spark.sql(...))
    sql = plan.getCommand().getSqlCommand().getSql();
    logger.debug("Found SQL command: {}", sql);
}
```

**Needed Addition** (after line 129, before line 131):
```java
} else if (plan.hasCommand()) {
    // Handle non-SQL commands (CreateTempView, etc.)
    Command command = plan.getCommand();
    logger.debug("Handling COMMAND: {}", command.getCommandTypeCase());

    executeCommand(command, sessionId, responseObserver);
    return; // Command handling is complete
```

### Step 2: Create executeCommand() Method
**File**: `SparkConnectServiceImpl.java`
**Add after executePlan() method**:

```java
/**
 * Execute a Command (non-query operation)
 *
 * Handles:
 * - CreateDataFrameViewCommand (createOrReplaceTempView)
 * - Other commands as needed
 */
private void executeCommand(Command command, String sessionId,
                           StreamObserver<ExecutePlanResponse> responseObserver) {

    if (command.hasCreateDataframeView()) {
        // Handle createOrReplaceTempView
        CreateDataFrameViewCommand viewCmd = command.getCreateDataframeView();
        String viewName = viewCmd.getName();
        Relation input = viewCmd.getInput();
        boolean replace = viewCmd.getReplace();

        logger.info("Creating temp view: {} (replace={})", viewName, replace);

        try {
            // Convert input relation to LogicalPlan
            LogicalPlan logicalPlan = planConverter.convertRelation(input);

            // Register view in session
            sessionManager.getSession(sessionId).registerTempView(viewName, logicalPlan);

            // Return success response (empty result set)
            ExecutePlanResponse response = ExecutePlanResponse.newBuilder()
                .setSessionId(sessionId)
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.info("Temp view registered: {}", viewName);

        } catch (Exception e) {
            logger.error("Failed to create temp view: " + viewName, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription("Failed to create temp view: " + e.getMessage())
                .asRuntimeException());
        }
    } else {
        // Unsupported command type
        responseObserver.onError(Status.UNIMPLEMENTED
            .withDescription("Unsupported command type: " + command.getCommandTypeCase())
            .asRuntimeException());
    }
}
```

### Step 3: Add View Registry to Session Class
**File**: `Session.java` (in `com.thunderduck.connect.session` package)

**Add field**:
```java
private final Map<String, LogicalPlan> tempViews = new ConcurrentHashMap<>();
```

**Add methods**:
```java
public void registerTempView(String name, LogicalPlan plan) {
    tempViews.put(name, plan);
    logger.debug("Registered temp view: {}", name);
}

public Optional<LogicalPlan> getTempView(String name) {
    return Optional.ofNullable(tempViews.get(name));
}

public void dropTempView(String name) {
    tempViews.remove(name);
    logger.debug("Dropped temp view: {}", name);
}

public Set<String> getTempViewNames() {
    return tempViews.keySet();
}
```

### Step 4: Modify SQL Generation to Resolve Views
**File**: `SQLGenerator.java`

When generating SQL from a plan that references a table name, check if it's a temp view first:

```java
// In table scan generation
private String generateTableScan(TableScan scan) {
    String tableName = scan.getTableName();

    // Check if this is a temp view
    Optional<LogicalPlan> tempView = getCurrentSession().getTempView(tableName);
    if (tempView.isPresent()) {
        // Generate SQL from the view's plan
        return "(" + generate(tempView.get()) + ")";
    }

    // Regular table scan
    return tableName;
}
```

**Note**: This requires passing session context through the generator. May need to add session parameter to SQLGenerator methods.

---

## 🧪 Testing Plan

### Test 1: Basic Temp View
```python
def test_create_temp_view_basic():
    df = spark.read.parquet("lineitem.parquet")
    df.createOrReplaceTempView("lineitem_view")

    result = spark.sql("SELECT COUNT(*) FROM lineitem_view")
    assert result.collect()[0][0] == 60175
```

### Test 2: Temp View with Filter
```python
def test_temp_view_with_filter():
    df = spark.read.parquet("lineitem.parquet")
    filtered = df.filter("l_quantity > 40")
    filtered.createOrReplaceTempView("filtered_view")

    result = spark.sql("SELECT COUNT(*) FROM filtered_view")
    assert result.collect()[0][0] > 0
```

### Test 3: Multiple Temp Views with Join
```python
def test_multiple_temp_views_join():
    orders = spark.read.parquet("orders.parquet")
    lineitem = spark.read.parquet("lineitem.parquet")

    orders.createOrReplaceTempView("orders")
    lineitem.createOrReplaceTempView("lineitem")

    result = spark.sql("""
        SELECT COUNT(*)
        FROM orders o
        JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    """)

    count = result.collect()[0][0]
    assert count == 60175  # All lineitems should match
```

### Test 4: TPC-H Q1 with Temp View
```python
def test_tpch_q1_with_temp_view():
    # This is the key test - if this works, all TPC-H queries should work
    lineitem = spark.read.parquet("lineitem.parquet")
    lineitem.createOrReplaceTempView("lineitem")

    result = spark.sql("""
        SELECT
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        FROM lineitem
        WHERE l_shipdate <= date '1998-12-01'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """)

    rows = result.collect()
    assert len(rows) == 4  # Expected result count
```

---

## 📁 Files to Modify

### New Files to Create:
1. `tests/integration/test_temp_views.py` - Temp view tests

### Existing Files to Modify:
1. `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`
   - Add COMMAND handling in executePlan() (line ~125)
   - Add executeCommand() method

2. `connect-server/src/main/java/com/thunderduck/connect/session/Session.java`
   - Add tempViews map field
   - Add registerTempView(), getTempView(), dropTempView() methods

3. `connect-server/src/main/java/com/thunderduck/generator/SQLGenerator.java`
   - Modify table scan generation to resolve temp views
   - Add session context parameter (if needed)

---

## ⏱️ Estimated Time

| Task | Duration | Priority |
|------|----------|----------|
| 1. Add COMMAND handling | 1-2 hours | HIGH |
| 2. Add view registry | 1 hour | HIGH |
| 3. Test temp views | 1 hour | HIGH |
| 4. Run TPC-H Q1 | 30 min | HIGH |
| 5. Run all 22 TPC-H queries | 2-3 hours | HIGH |
| 6. Fix discovered issues | 2-6 hours | MEDIUM |
| **TOTAL** | **7-13 hours** | |

---

## 🎯 Success Criteria

### Phase 3 Complete When:
- ✅ createOrReplaceTempView() works
- ✅ Temp views queryable with SQL
- ✅ At least 15/22 TPC-H queries passing (68%+)
- ✅ Differential tests running successfully
- ✅ Performance metrics documented

### Stretch Goals:
- 🎯 20/22 queries passing (91%+)
- 🎯 Average 3x+ speedup vs Spark
- 🎯 100% pass rate on all TPC-H queries

---

## 🔄 Next Immediate Steps

1. ✅ Phase 2 committed (commit `dc3155a`)
2. ⏳ Open `SparkConnectServiceImpl.java` at line 125
3. ⏳ Add COMMAND handling code
4. ⏳ Modify `Session.java` to add view registry
5. ⏳ Test with simple temp view
6. ⏳ Run TPC-H Q1
7. ⏳ Iterate on remaining queries

---

## 📊 Current State

```
Week 13 Progress:
├── Phase 1: Architecture & Planning      [✅ COMPLETE]
├── Phase 2: Build + Data + Tests         [✅ COMPLETE - commit dc3155a]
└── Phase 3: COMMAND + TPC-H Testing      [🟡 READY TO START]
    ├── Analysis                          [✅ DONE]
    ├── Implementation Plan               [✅ DONE]
    ├── Code Changes                      [⏳ NEXT]
    └── Testing                           [⏳ PENDING]
```

---

## 📝 Documentation Created

1. `WEEK13_PHASE2_COMPLETION_SUMMARY.md` - Comprehensive Phase 2 report
2. `WEEK13_PHASE3_PLAN.md` - Detailed Phase 3 roadmap
3. `WEEK13_COMPLETION_AND_PHASE3_HANDOFF.md` - This document

---

**Status**: ✅ **PHASE 2 COMPLETE** | 🟡 **PHASE 3 READY TO START**

**Next Session**: Open `SparkConnectServiceImpl.java` and add COMMAND handling at line 125

**Estimated Remaining Work**: 7-13 hours to complete Phase 3

---

*Last Updated: 2025-10-26*
*Phase 2 Commit: dc3155a*
