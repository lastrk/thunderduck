# Claude Code Project Rules

This file contains project-specific rules and guidelines for working with thunderduck.

## Documentation Structure Rules

**Permanent Rule**: The thunderduck project follows a strict documentation structure pattern:

1. **ONE high-level plan** (`IMPLEMENTATION_PLAN.md`)
   - Contains the 16-week development roadmap
   - Describes all phases and major milestones
   - This is the single source of truth for project timeline

2. **ONE detailed plan per active milestone** (e.g., `WEEK11_IMPLEMENTATION_PLAN.md`)
   - Contains the detailed 5-day implementation plan for the current week
   - Includes specific tasks, test coverage, and success criteria
   - Only one detailed plan should exist at a time for the current milestone
   - Previous week detailed plans should be removed or consolidated

3. **ONE completion report per finished milestone** (e.g., `WEEK*_COMPLETION_REPORT.md`)
   - Historical record of completed work
   - Documents achievements, test results, and lessons learned
   - Should be preserved for project history

4. **Everything else consolidated or removed**
   - No duplicate high-level plans
   - No obsolete planning documents
   - Architecture documentation goes in `docs/architect/`
   - Protocol specifications go in `docs/`
   - Testing documentation goes in `docs/Testing_Strategy.md`

### Enforcement

When creating new documentation:
- **DO NOT** create additional high-level implementation plans
- **DO NOT** create duplicate milestone plans
- **DO** consolidate temporary planning documents into the appropriate permanent location
- **DO** remove obsolete planning documents after content is extracted
- **DO** create completion reports when milestones are finished
- **DO** archive or remove the detailed plan after the completion report is created

### Current Structure

```
thunderduck/
├── IMPLEMENTATION_PLAN.md              # ONE high-level plan
├── README.md                            # Project overview
├── WEEK11_IMPLEMENTATION_PLAN.md        # Current milestone (active)
├── WEEK*_COMPLETION_REPORT.md           # Historical completion reports
├── docs/
│   ├── SPARK_CONNECT_PROTOCOL_SPEC.md  # Protocol reference
│   ├── Testing_Strategy.md              # Testing approach
│   ├── architect/                       # Architecture documentation
│   └── coder/                           # Build/CI documentation
```

**Last Updated**: 2025-10-27
**Cleanup Report**: See `DOCUMENTATION_CLEANUP_PLAN.md` for the rationale behind this structure

## Spark Parity Requirements

**Critical Rule**: Thunderduck must match Spark EXACTLY, not just produce equivalent results.

### Numeric Type Compatibility

Thunderduck must match Spark's:
- **Return types**: If Spark returns DOUBLE, Thunderduck must return DOUBLE (not BIGINT)
- **Rounding conventions**: Must match Spark's rounding behavior
- **Arithmetic properties**: Integer division, modulo, overflow behavior must match
- **Type coercion**: Implicit casts must follow Spark's rules
- **NULL handling**: Must match Spark's null propagation

### Examples of WRONG Behavior

❌ **Wrong**: Spark returns `64.0` (DOUBLE), Thunderduck returns `64` (BIGINT)
- Even though 64.0 == 64 numerically, the TYPE mismatch breaks compatibility
- Client code expecting DOUBLE will fail with BIGINT

❌ **Wrong**: Spark returns `java.sql.Date`, Thunderduck returns `null`
- Even if other columns are correct, missing a column value is wrong

❌ **Wrong**: Spark rounds `3.5` to `4`, Thunderduck rounds to `3`
- Numerical precision matters for reproducibility

### What This Means

When validating correctness:
1. **Row-by-row value comparison** ✅ (what we do now)
2. **Type-by-type comparison** ✅ (what we need to add)
3. **Precision/rounding validation** ✅ (required)

If Thunderduck produces "close enough" results, **that's not good enough**.
If types don't match exactly, **that's a bug that must be fixed**.

### Testing Standard

Differential tests must validate:
- ✓ Same number of rows
- ✓ Same column names
- ✓ **Same column TYPES** (not just convertible types)
- ✓ Same values (with appropriate epsilon for floats)
- ✓ Same null handling
- ✓ Same sort order (with exceptions noted below)

### Sort Order and Tie-Breaking

**Important**: When ORDER BY results in ties (multiple rows with equal sort keys), the order of tied rows is **non-deterministic** in SQL. This is expected behavior in both Spark and Thunderduck.

**Examples**:
- Query: `ORDER BY cnt` where multiple states have same count
- Result: States with same count may appear in any order
- Status: **CORRECT** - this is SQL standard behavior

**Testing Approach**:
When comparing results with potential ties:
1. **Option A**: Sort both result sets by ALL columns before comparing (order-independent)
2. **Option B**: Note that specific tie-breaking order doesn't matter (values are correct)
3. **Option C**: Add secondary sort keys to make ORDER BY deterministic

**Goal**: Drop-in replacement for Spark, not "Spark-like" behavior.

**Last Updated**: 2025-10-27
