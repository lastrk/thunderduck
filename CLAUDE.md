# Claude Code Project Rules

This file contains project-specific rules and guidelines for working with thunderduck.

## Documentation Structure Rules

**Permanent Rule**: The thunderduck project follows a focused documentation structure:

1. **Current focus documents** (in workspace root, prefixed with `CURRENT_FOCUS_`)
   - Active work items and immediate priorities
   - Only documents related to the current milestone/focus area
   - Removed or archived when focus shifts

2. **Permanent documentation** (in `docs/`)
   - `docs/MVP_IMPLEMENTATION_PLAN.md` - Original 16-week MVP roadmap (archived)
   - `docs/architect/` - Architecture decisions and designs
   - `docs/dev_journal/` - Completion reports and historical records
   - `docs/SPARK_CONNECT_PROTOCOL_SPEC.md` - Protocol reference
   - `docs/Testing_Strategy.md` - Testing approach

3. **Test infrastructure** (in `tests/scripts/`)
   - Test runner scripts and utilities

### Enforcement

When creating new documentation:
- **DO** use `CURRENT_FOCUS_` prefix for active work documents in workspace root
- **DO** move completed focus documents to `docs/` or `docs/dev_journal/`
- **DO** keep workspace root clean - only README.md, CLAUDE.md, and CURRENT_FOCUS_* files
- **DO NOT** create planning documents without the `CURRENT_FOCUS_` prefix
- **DO NOT** leave stale focus documents - archive or remove when done

### Current Structure

```
thunderduck/
├── README.md                            # Project overview
├── CLAUDE.md                            # Project rules (this file)
├── CURRENT_FOCUS_*.md                   # Active work items
├── docs/
│   ├── MVP_IMPLEMENTATION_PLAN.md       # Archived MVP roadmap
│   ├── SPARK_CONNECT_PROTOCOL_SPEC.md   # Protocol reference
│   ├── Testing_Strategy.md              # Testing approach
│   ├── architect/                       # Architecture documentation
│   └── dev_journal/                     # Completion reports
└── tests/
    └── scripts/                         # Test runner scripts
```

**Last Updated**: 2025-12-10

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

## Spark Connect Server Configuration

**Critical**: The following configuration is required for the ThunderDuck Spark Connect Server to work correctly.

### Protobuf Dependency Configuration

**Issue**: Spark Connect includes pre-compiled protobuf classes that can cause `VerifyError` at runtime if version mismatch occurs.

**Solution**: Use `provided` scope for `spark-connect_2.13` dependency:
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-connect_2.13</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>  <!-- CRITICAL: Must be 'provided' not 'compile' -->
</dependency>
```

**Reason**: This prevents bundling Spark's pre-compiled protobuf classes which were compiled with a different protobuf version, avoiding runtime `VerifyError`.

### Apache Arrow on ARM64 Platforms

**Issue**: Apache Arrow 17.0.0 requires special JVM flags on ARM64 platforms (AWS Graviton, Apple Silicon) to access internal Java NIO classes.

**Required JVM Flags**:
```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
```

**How to Run Server**:
```bash
# Option 1: Direct JAR execution (recommended for production)
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar connect-server/target/thunderduck-connect-server-*.jar

# Option 2: Using Maven exec plugin
export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"
mvn exec:java -pl connect-server \
    -Dexec.mainClass="com.thunderduck.connect.server.SparkConnectServer"

# Option 3: Using start-server.sh script (already configured)
./tests/scripts/start-server.sh
```

**Error if Missing**:
```
java.lang.RuntimeException: Failed to initialize MemoryUtil.
You must start Java with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
```

### Key Learnings

1. **Always use clean builds** when diagnosing server issues: `mvn clean compile` or `mvn clean package`
2. **Dependency scoping matters**: `compile` vs `provided` scope can cause runtime class conflicts
3. **Platform-specific requirements**: ARM64 platforms have special requirements for Apache Arrow
4. **Test with actual client**: Always test with PySpark client after server changes

**Last Updated**: 2025-11-05
**Fix Applied**: See `/workspace/docs/PROTOBUF_FIX_REPORT.md` for detailed resolution history
