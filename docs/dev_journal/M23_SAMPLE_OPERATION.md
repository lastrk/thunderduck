# M23: Sample Operation Implementation

**Date:** 2025-12-12
**Status:** Complete

## Summary

Implemented `df.sample(fraction, seed)` support using DuckDB's native Bernoulli sampling to match Spark's per-row probability algorithm.

## Implementation Details

### Files Created
- `core/src/main/java/com/thunderduck/logical/Sample.java` - LogicalPlan class
- `tests/src/test/java/com/thunderduck/logical/SampleTest.java` - 18 unit tests

### Files Modified
- `connect-server/.../RelationConverter.java` - Added `convertSample()` method
- `core/.../generator/SQLGenerator.java` - Added `visitSample()` method
- `CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md` - Updated to v1.7

## Spark Connect Protocol

From `relations.proto:420-439`:
```protobuf
message Sample {
  Relation input = 1;           // Child relation
  double lower_bound = 2;       // Usually 0.0
  double upper_bound = 3;       // The fraction (e.g., 0.1 for 10%)
  optional bool with_replacement = 4;
  optional int64 seed = 5;
  bool deterministic_order = 6; // For randomSplit()
}
```

## DuckDB SQL Generation

### Basic sampling
```sql
SELECT * FROM (child_sql) AS subquery_1 USING SAMPLE 10.000000% (bernoulli)
```

### With seed
```sql
SELECT * FROM (child_sql) AS subquery_1 USING SAMPLE 10.000000% (bernoulli, 42)
```

## Key Design Decisions

### 1. Bernoulli vs System Sampling
- **Chose Bernoulli** to match Spark's per-row probability algorithm
- DuckDB's System sampling operates at block granularity (~1024 rows)
- System sampling would give wrong results for small datasets

### 2. withReplacement Error Handling
- Spark's withReplacement=true uses Poisson sampling (row can appear 0, 1, 2+ times)
- DuckDB has no equivalent for Poisson sampling
- **Decision:** Return `PlanConversionException` for fail-fast behavior (user requested)

### 3. Sample is a Transformation
- Returns DataFrame (not List[Row] like actions)
- Lazy evaluation - no immediate execution
- Can be chained: `df.sample(0.1).filter(...).select(...)`

## Test Coverage

18 tests covering:
- Constructor validation (fraction bounds 0.0-1.0, seed handling)
- Schema inference (unchanged from child)
- SQL generation with Bernoulli sampling
- Composability (Filter → Sample, Sample → Limit, chained Samples)

## Coverage Update

Gap analysis updated to v1.7:
- Relations: 20 implemented + 1 partial = **50-52.5%** coverage
- Sample moved from "Not Implemented" to "Implemented"

## Lessons Learned

1. **Algorithm alignment matters**: Using SYSTEM sampling would have been faster but semantically wrong for Spark compatibility
2. **Spark sampling is probabilistic**: Unlike RESERVOIR which gives exact counts, Spark uses Bernoulli/Poisson which gives approximate row counts based on probability
