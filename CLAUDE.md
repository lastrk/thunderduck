# Claude Code Project Rules

This file contains project-specific rules and guidelines for working with thunderduck.

## Project Vision and Value Proposition

**Critical Context**: Understand this before working on Thunderduck.

### The Core Insight

Analysis of hundreds of thousands of real-world Spark workloads reveals that **most don't actually need distributed computing**. They could run faster and cheaper on a single large server node.

### Why This Matters Now

The economics of computing have fundamentally shifted:
- **Past**: 2x hardware = 4x+ cost → distributed computing made economic sense
- **Today**: Linear pricing at cloud providers → 200 CPU / 1TB RAM machines are cost-effective
- **Result**: Single-node compute eliminates shuffles, network bottlenecks, and coordination overhead

### The Problem

Organizations have massive investments in Spark codebases but:
- Their workloads don't need distributed compute
- Spark local mode is slow (JVM overhead, row-based processing, poor utilization)
- Rewriting to a different system is prohibitively expensive

### Thunderduck's Value Proposition

**Keep your Spark API, get single-node DuckDB performance.**

Thunderduck is a migration path for the "post-Big Data" era:
- Drop-in Spark Connect server (works with existing PySpark/Scala code)
- Translates Spark operations to DuckDB SQL
- 5-10x faster than Spark local mode, 6-8x better memory efficiency
- Zero code changes required for compatible workloads

### Target Audience

Organizations that:
1. Have existing Spark codebases (significant investment)
2. Discovered their workloads fit on a single node
3. Want better performance without rewriting everything

**This is NOT for**: Workloads that genuinely require distributed compute (100TB+ datasets, streaming at scale).

**Last Updated**: 2025-12-17

## Documentation Structure Rules

**Permanent Rule**: The thunderduck project follows a focused documentation structure:

1. **Current focus documents** (in workspace root, prefixed with `CURRENT_FOCUS_`)
   - Active work items and immediate priorities
   - Only documents related to the current milestone/focus area
   - Removed or archived when focus shifts

2. **Permanent documentation** (in `docs/`)
   - `docs/MVP_IMPLEMENTATION_PLAN.md` - Original 16-week MVP roadmap (archived)
   - `docs/architect/` - Architecture decisions and designs
   - `docs/dev_journal/` - Developer journal with milestone completion reports
   - `docs/SPARK_CONNECT_PROTOCOL_SPEC.md` - Protocol reference
   - `docs/Testing_Strategy.md` - Testing approach

3. **Developer journal** (`docs/dev_journal/`)
   - Completion reports after each milestone, prefixed with `M[X]_` (e.g., `M1_`, `M2_`)
   - X is a monotonically increasing number indicating chronological order
   - Each report documents achievements, decisions, and lessons learned

4. **Test infrastructure** (in `tests/scripts/`)
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

### Apache Arrow JVM Requirements (Spark 4.0.x)

**Issue**: Apache Arrow requires special JVM flags on ALL platforms to access internal Java NIO classes. As of Spark 4.0.x, these flags are required everywhere (not just ARM64).

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
3. **JVM flags required on ALL platforms**: As of Spark 4.0.x, `--add-opens` flags are needed everywhere
4. **Test with actual client**: Always test with PySpark 4.0.x client after server changes

### Server Process Cleanup (IMPORTANT)

**Critical Rule**: After running E2E tests or any tests that start the Thunderduck server, ALWAYS kill the server process when done.

**Why**: Background server processes can accumulate and cause port conflicts, resource exhaustion, and confusing test failures.

**How to cleanup**:
```bash
# Kill all thunderduck server processes
pkill -9 -f thunderduck-connect-server

# Or kill all java processes (more aggressive)
pkill -9 -f java
```

**Best Practice**: When running tests:
1. Kill any existing server before starting a new one
2. After tests complete, kill the server
3. Periodically check for dangling processes: `ps aux | grep thunderduck`

**Last Updated**: 2025-12-16
**Fix Applied**: See `/workspace/docs/PROTOBUF_FIX_REPORT.md` for detailed resolution history
- Always do a full clean and rebuild before testing, you keep making the mistake to test with old build and be surprised that code changes have had no effect

## Git Commit Workflow

**Critical Rule**: NEVER commit code without user review first.

### Commit Process

1. **Show changes first**: Always show the user a summary of changes before committing
2. **Wait for approval**: Do NOT commit until the user explicitly approves
3. **Only commit when asked**: The user must explicitly request a commit (e.g., "commit this", "let's commit")

### What This Means

- After implementing features or making changes, STOP and summarize what was done
- Let the user review the code/changes before committing
- If the user says "commit" or asks to commit, THEN proceed with the commit
- Do NOT assume that completing a task means you should commit

### Examples

**WRONG**:
```
User: "Add function X"
Claude: *implements function X*
Claude: *immediately commits without asking*
```

**CORRECT**:
```
User: "Add function X"
Claude: *implements function X*
Claude: "I've added function X. Here's what changed: [summary]. Let me know when you'd like to commit."
User: "looks good, commit it"
Claude: *commits the changes*
```

**Last Updated**: 2025-12-16