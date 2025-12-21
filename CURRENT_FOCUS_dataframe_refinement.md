# Current Focus: DataFrame API Refinement and Cleanup

## Status: In Progress

## Overview

With SparkSQL pass-through removed, focus shifts to refining the DataFrame API implementation:
1. Remove accumulated cruft from iterative test-fixing
2. Establish principled schema inference and nullability handling
3. Fix remaining DataFrame test failures
4. Update unit tests for consistency

---

## Priority 1: Refactoring Opportunities

### Goal
Remove code that was added reactively while fixing differential tests. Consolidate duplicated logic and simplify the codebase.

### Duplication Analysis (Completed)

**Code audited:** WithColumns.java, Project.java, Aggregate.java

| Code Pattern | WithColumns | Project | Aggregate | Lines Duplicated |
|-------------|-------------|---------|-----------|------------------|
| Window function type resolution | lines 156-242 | lines 170-258 | N/A | ~80 lines |
| Binary expression type resolution | lines 244-275 | lines 259-289 | N/A | ~40 lines |
| `promoteNumericTypes()` | lines 297-338 | lines 486-534 | N/A | ~40 lines |
| Column lookup from schema | lines 277-287 | lines 164-169 | lines 284-289 | ~15 lines |
| Nullable resolution | lines 347-374 | lines 395-428 | lines 303-320 | ~30 lines |

**Total duplicated code: ~205 lines across 3 files**

### Specific Duplication Details

#### 1. Window Function Type Resolution (~80 lines)

Both WithColumns and Project have nearly identical logic for:
- Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE) → IntegerType
- Distribution functions (PERCENT_RANK, CUME_DIST) → DoubleType
- COUNT → LongType
- Analytic functions (LAG, LEAD, FIRST, LAST, NTH_VALUE) → argument type
- MIN, MAX → argument type
- SUM → promoted type (Integer/Long→Long, Float/Double→Double, Decimal→Decimal(p+10,s))
- AVG → DoubleType for numeric, Decimal for Decimal input
- STDDEV, VARIANCE → DoubleType

#### 2. Binary Expression Type Resolution (~40 lines)

Both files have identical logic for:
- Comparison/logical operators → BooleanType
- Division → DoubleType (always)
- Other arithmetic → promoteNumericTypes()
- String concatenation → StringType

#### 3. Numeric Type Promotion (~40 lines)

Both files implement identical `promoteNumericTypes()`:
- Double > Float > Decimal > Long > Integer > Short > Byte
- Decimal + Decimal → max(precision), max(scale)

#### 4. Column Lookup (~15 lines per file)

All three files iterate over schema fields to find column by name (case-insensitive).

### Recommended Refactoring

Create `TypeInferenceEngine.java` in `com.thunderduck.logical` with:

```java
public final class TypeInferenceEngine {
    // Type resolution
    public static DataType resolveType(Expression expr, StructType schema);

    // Nullability resolution
    public static boolean resolveNullable(Expression expr, StructType schema);

    // Aggregate type inference
    public static DataType inferAggregateType(String function, DataType argType);

    // Numeric type promotion
    public static DataType promoteNumericTypes(DataType left, DataType right);
}
```

**Benefits:**
- Single source of truth for type inference rules
- Easier to maintain Spark compatibility
- Reduces code in logical plan nodes by ~60%
- Makes adding new expression types easier

### Implementation Steps

- [ ] Create `TypeInferenceEngine.java` with consolidated logic
- [ ] Update `WithColumns.java` to delegate to TypeInferenceEngine
- [ ] Update `Project.java` to delegate to TypeInferenceEngine
- [ ] Update `Aggregate.java` to delegate to TypeInferenceEngine
- [ ] Remove duplicate private methods from all three files
- [ ] Add unit tests for TypeInferenceEngine

---

## Priority 2: Principled Schema Inference & Nullability

### Goal
Define a consistent, Spark-conformant approach to schema and type inference.

### Spark Schema Semantics

**Schema inference** answers: "What are the column names and types of this operation's output?"

**Nullability** answers: "Can this column/expression contain NULL values?"

### Spark's Nullability Rules (to match)

| Expression Type | Nullable |
|-----------------|----------|
| Literal (non-null) | false |
| Column reference | inherits from source schema |
| Arithmetic on nullable | true if any operand nullable |
| Aggregate (COUNT) | false |
| Aggregate (SUM, AVG, MIN, MAX) | true (empty group → null) |
| Window ranking (ROW_NUMBER, RANK) | false |
| Window analytic (LAG, LEAD) | true (unless default provided AND column non-nullable) |
| COALESCE | true only if ALL args nullable |
| CASE WHEN | true if any THEN/ELSE branch nullable |

### Spark's Type Promotion Rules (to match)

**Division**: `Decimal / Decimal` → `Decimal` (not Double!)
- Precision: `p1 - s1 + s2 + max(6, s1 + p2 + 1)`
- Scale: `max(6, s1 + p2 + 1)`

**Note**: Current implementation returns Double for all division. This is a known deviation.

**Multiplication**: `Decimal * Decimal` → `Decimal`
- Precision: `p1 + p2 + 1`
- Scale: `s1 + s2`

**Addition/Subtraction**: Widen to larger precision/scale

### Architecture After Refactoring

```
┌─────────────────────────────────────────┐
│         TypeInferenceEngine             │
│  - resolveType(expr, schema)            │
│  - resolveNullable(expr, schema)        │
│  - promoteNumericTypes(left, right)     │
│  - inferAggregateType(func, argType)    │
└─────────────────────────────────────────┘
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
Project      WithColumns      Aggregate
(delegates)   (delegates)    (delegates)
```

---

## Priority 3: Remaining DataFrame Test Failures

### Current Status
- **TPC-DS DataFrame**: 20/33 passing (13 failing)
- **Window tests**: 35/35 passing (100%)

### Known Failure Categories

1. **Decimal type preservation** (Q98, Q99)
   - Division returns Double instead of Decimal
   - CASE WHEN loses Decimal type (RawSQLExpression returns StringType)

2. **Data/ordering mismatches** (Q84, others)
   - Need investigation

3. **Other type mismatches**
   - Need investigation

### Tests to Investigate
- [ ] Q9, Q12, Q17, Q20, Q25, Q29, Q40, Q43, Q62, Q84, Q85, Q98, Q99

---

## Priority 4: Unit Test Updates

### Areas to Check

- [ ] Type inference tests - do they cover new Decimal division logic?
- [ ] Nullability tests - do they cover the rules above?
- [ ] Window function tests - verify IntegerType for ranking functions
- [ ] Schema inference tests - verify logical plan nodes

---

## Files to Modify

| File | Action |
|------|---------|
| `core/src/main/java/com/thunderduck/logical/TypeInferenceEngine.java` | NEW - consolidated type inference |
| `core/src/main/java/com/thunderduck/logical/WithColumns.java` | Remove ~150 lines, delegate to engine |
| `core/src/main/java/com/thunderduck/logical/Project.java` | Remove ~200 lines, delegate to engine |
| `core/src/main/java/com/thunderduck/logical/Aggregate.java` | Remove ~40 lines, delegate to engine |
| `connect-server/.../ExpressionConverter.java` | CASE WHEN type tracking |
| `core/src/main/java/com/thunderduck/types/*.java` | Type promotion utilities |

---

## Next Steps

1. **Create TypeInferenceEngine** - Consolidate all type inference into single class
2. **Migrate logical plan nodes** - Update WithColumns, Project, Aggregate to use engine
3. **Fix Decimal division type** - Address Q98 (return Decimal not Double)
4. **Fix CASE WHEN type preservation** - Address Q99 (RawSQLExpression type tracking)
5. **Test** - Verify no regressions with full DataFrame test suite
