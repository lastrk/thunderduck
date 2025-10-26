# Week 13 Phase 3: COMMAND Plan Type + Full TPC-H Testing

## Objective
Implement COMMAND plan type support to enable full TPC-H query testing with temporary view registration.

## Current Status
- ✅ Server compiles and runs
- ✅ Basic operations working (read, filter, select, aggregate, groupBy, orderBy, join)
- ✅ TPC-H data generated (SF=0.01, 2.87 MB)
- ✅ Differential test framework created
- ❌ COMMAND plan type not implemented (blocks createOrReplaceTempView)

## Phase 3 Goals

### 1. Implement COMMAND Plan Type Support
**Priority**: HIGH (blocks full TPC-H testing)

**What is COMMAND?**
- Special plan type for non-query operations
- Used by: createOrReplaceTempView, dropTempView, config commands
- Current error: "Unsupported plan type. Plan type: COMMAND"

**Implementation Steps**:
1. Add COMMAND case to SparkConnectServiceImpl.executePlan()
2. Create CommandHandler class to route commands
3. Implement CreateTempViewCommand handler
4. Store view registry in SessionManager

**Files to Modify**:
- `SparkConnectServiceImpl.java` - Add COMMAND handling
- `SessionManager.java` - Add view registry (Map<String, LogicalPlan>)
- `CommandHandler.java` (new) - Command routing and execution
- `PlanConverter.java` - Extract command details from protobuf

**Estimated Time**: 4-6 hours

---

### 2. Test Temporary View Support
**Priority**: HIGH

**Test Cases**:
1. Create temp view from DataFrame
2. Query temp view with SQL
3. Multiple temp views with joins
4. Drop temp view
5. View lifecycle across queries

**Files to Create**:
- `test_temp_views.py` - Comprehensive temp view tests

**Success Criteria**:
- ✅ createOrReplaceTempView() succeeds
- ✅ SQL queries can reference temp views
- ✅ Views persist within session
- ✅ All temp view tests pass

**Estimated Time**: 2 hours

---

### 3. Run Full TPC-H Differential Test Suite
**Priority**: HIGH

**Approach**:
1. Update differential tests to use temp views
2. Run all 22 TPC-H queries sequentially
3. Compare results with Spark local mode
4. Document any failures with root cause

**Test Execution**:
```bash
# Run all TPC-H differential tests
python3 -m pytest tests/integration/test_differential_tpch.py -v

# Run specific query
python3 -m pytest tests/integration/test_differential_tpch.py::TestTPCH_Q1_Differential -v
```

**Expected Results**:
- Goal: 22/22 queries passing
- Minimum: 15/22 queries passing (68% coverage)
- Track failures for Phase 4 fixes

**Estimated Time**: 3-4 hours (includes investigation)

---

### 4. Fix Discovered Issues
**Priority**: MEDIUM (depends on findings)

**Common Issues to Expect**:
1. **Missing SQL functions** - Implement in FunctionMapper
2. **Type conversion issues** - Fix in TypeMapper
3. **Subquery support gaps** - Extend RelationConverter
4. **Window function issues** - Check WindowConverter
5. **Performance bottlenecks** - Profile and optimize

**Debugging Process**:
1. Identify failing query
2. Extract minimal reproducer
3. Add unit test
4. Fix issue
5. Verify differential test passes

**Estimated Time**: Variable (2-8 hours depending on issues)

---

### 5. Document Results
**Priority**: LOW

**Deliverables**:
1. **Phase 3 Completion Report**:
   - Implementation summary
   - Test results (pass/fail for all 22 queries)
   - Performance metrics
   - Known limitations

2. **TPC-H Query Coverage Matrix**:
   ```
   | Query | Status | Spark Time | Thunderduck Time | Speedup | Notes |
   |-------|--------|------------|------------------|---------|-------|
   | Q1    | ✅ PASS | 2.5s       | 0.8s             | 3.1x    |       |
   | Q2    | ✅ PASS | 3.2s       | 1.1s             | 2.9x    |       |
   | ...   |        |            |                  |         |       |
   ```

3. **Issues Log**:
   - Document any failing queries
   - Root cause analysis
   - Workarounds or fixes needed

**Estimated Time**: 2 hours

---

## Implementation Details

### COMMAND Plan Type Architecture

```
Client (PySpark)
  ↓
  df.createOrReplaceTempView("table_name")
  ↓
Spark Connect Protocol
  ↓
  ExecutePlanRequest (plan.opType = COMMAND)
  ↓
SparkConnectServiceImpl.executePlan()
  ↓
  if (plan.hasCommand()) {
    CommandHandler.handle(command, session)
  }
  ↓
SessionManager.registerTempView(name, logicalPlan)
  ↓
Later: spark.sql("SELECT * FROM table_name")
  ↓
SessionManager.resolveTempView(name) → LogicalPlan
  ↓
SQLGenerator.generate(logicalPlan) → DuckDB SQL
  ↓
QueryExecutor.execute(sql) → Results
```

### View Registry Design

```java
class SessionManager {
    private final Map<String, ViewRegistration> viewRegistry = new ConcurrentHashMap<>();

    static class ViewRegistration {
        String viewName;
        LogicalPlan plan;
        long createdAt;

        // For SQL views (alternative to LogicalPlan)
        String sqlDefinition;
    }

    public void registerTempView(String name, LogicalPlan plan) {
        viewRegistry.put(name, new ViewRegistration(name, plan, System.currentTimeMillis()));
    }

    public Optional<ViewRegistration> getTempView(String name) {
        return Optional.ofNullable(viewRegistry.get(name));
    }

    public void dropTempView(String name) {
        viewRegistry.remove(name);
    }
}
```

---

## Success Criteria

### Phase 3 Complete When:
- ✅ COMMAND plan type implemented
- ✅ createOrReplaceTempView() working
- ✅ At least 15/22 TPC-H queries passing (68%+)
- ✅ Differential tests running successfully
- ✅ Performance metrics documented
- ✅ Known issues documented with root causes

### Stretch Goals:
- 🎯 20/22 queries passing (91%+)
- 🎯 Average 3x+ speedup vs Spark
- 🎯 All discovered issues fixed (100% pass rate)

---

## Timeline

| Task | Duration | Status |
|------|----------|--------|
| 1. Analyze COMMAND requirements | 1 hour | ⏳ Not started |
| 2. Implement COMMAND handler | 3-4 hours | ⏳ Not started |
| 3. Test temp view support | 2 hours | ⏳ Not started |
| 4. Run TPC-H differential suite | 3-4 hours | ⏳ Not started |
| 5. Fix discovered issues | 2-8 hours | ⏳ Not started |
| 6. Document results | 2 hours | ⏳ Not started |

**Total Estimated Time**: 13-21 hours (2-3 days)

---

## Risk Assessment

### HIGH RISK:
- **View resolution complexity** - May need query rewriting
- **Multiple issues discovered** - Could extend timeline significantly

### MEDIUM RISK:
- **Missing SQL features** - May require extensive function mapping
- **Performance issues** - May need optimization work

### LOW RISK:
- **COMMAND implementation** - Well-defined protobuf structure
- **Temp view storage** - Simple map-based registry

---

## Next Actions

1. ✅ Commit Phase 2 work
2. ⏳ Analyze COMMAND protobuf structure
3. ⏳ Implement CommandHandler class
4. ⏳ Add view registry to SessionManager
5. ⏳ Test with simple temp view
6. ⏳ Run first TPC-H query (Q1)
7. ⏳ Iterate on remaining queries

---

**Phase 3 Start Date**: 2025-10-26
**Target Completion**: 2025-10-27 or 2025-10-28
**Status**: 🟡 IN PROGRESS - Starting COMMAND analysis
