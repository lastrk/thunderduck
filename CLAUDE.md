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

## Workflow Orchestration

### 1. Plan Mode Default
Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)If something goes sideways, STOP and re-plan immediately - don't keep pushingUse plan mode for verification steps, not just buildingWrite detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy to keep main context window clean
Offload research, exploration, and parallel analysis to subagentsFor complex problems, throw more compute at it via subagentsOne task per subagent for focused execution.
**Use subagent for running compile tasks**: every time you need to recompile the project by running maven, use a subagent and let it return success if the compilation succeeded or the focused error message
**Use subagent for running test suites**: every time you need to run unit or integration tests, use a subagent and let it return a summary of number of tests succeeded, failed and which tests specifically failed, further analysis of failing tests can be done by running subagents for singular tests and they should return a summary of the output.

### 3. Self-Improvement Loop
After ANY correction from the user: update 'tasks/lessons.md' with the patternWrite rules for yourself that prevent the same mistakeRuthlessly iterate on these lessons until mistake rate dropsReview lessons at session start for relevant project

### 4. Verification Before Done
Never mark a task complete without proving it worksDiff behavior between main and your changes when relevantAsk yourself: "Would a staff engineer approve this?"Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
For non-trivial changes: pause and ask "is there a more elegant way?"If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"Skip this for simple, obvious fixes - don't over-engineerChallenge your own work before presenting it

### 6. Autonomous Bug Fixing
When given a bug report: just fix it. Don't ask for hand-holdingPoint at logs, errors, failing tests -> then resolve themZero context switching required from the userGo fix failing CI tests without being told how

## Task Management
**Plan First**: Write plan to 'tasks/todo.md' with checkable items
**Verify Plan**: Check in before starting implementation
**Track Progress**: Mark items complete as you go
**Explain Changes**: High-level summary at each step
**Document Results**: Add review to 'tasks/todo.md'
**Capture Lessons**: Update 'tasks/lessons.md' after corrections

## Core Principles
**Simplicity First**: Make every change as simple as possible. Impact minimal code.
**No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
**Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

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
├── core/                                # Core translation engine (Java)
├── connect-server/                      # Spark Connect gRPC server (Java)
├── thunderduck-duckdb-extension/                          # Optional DuckDB C extension (C++)
├── tests/                               # Integration & E2E tests
│   └── scripts/                         # Test runner scripts
└── docs/
    ├── MVP_IMPLEMENTATION_PLAN.md       # Archived MVP roadmap
    ├── SPARK_CONNECT_PROTOCOL_SPEC.md   # Protocol reference
    ├── Testing_Strategy.md              # Testing approach
    ├── architect/                       # Architecture documentation
    │   └── SPARK_COMPAT_EXTENSION.md    # Extension architecture
    └── dev_journal/                     # Completion reports
```

**Last Updated**: 2026-02-07

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

## Spark Compatibility Extension (`thunderduck-duckdb-extension/`)

**Architectural Decision**: Some Spark operations cannot be faithfully replicated using vanilla DuckDB functions (e.g., decimal division always casts to DOUBLE, losing precision). Thunderduck uses an optional DuckDB C extension to implement these operations with exact Spark semantics.

### Two Compatibility Modes

| Mode | Extension | Compatibility | Use Case |
|------|-----------|--------------|----------|
| **Relaxed** (default) | Not loaded | ~85% Spark compat | General analytics, development |
| **Strict** | Loaded | ~100% Spark compat | Production parity, numeric-sensitive workloads |

### How It Works

1. Extension is built separately via CMake (`cd thunderduck-duckdb-extension && GEN=ninja make release`)
2. Compiled `.duckdb_extension` binary is placed in `core/src/main/resources/extensions/<platform>/`
3. `DuckDBRuntime` auto-detects and loads the extension at connection creation
4. `FunctionRegistry` checks extension availability and maps to extension functions when loaded
5. If extension is absent or fails to load, server continues with vanilla DuckDB functions

### Key Rules

- Extension build is **NOT** part of the default Maven lifecycle — it's a separate CMake step
- Extension DuckDB version **must exactly match** the `duckdb_jdbc` Maven dependency version
- Extension is **unsigned** — requires `allow_unsigned_extensions=true` connection property
- Extension loading is **per-connection** — each `DuckDBRuntime` instance loads independently

### Current Extension Functions

| Function | Replaces | Purpose |
|----------|----------|---------|
| `spark_decimal_div(a, b)` | `a / b` (decimal) | Spark 4.1 decimal division with ROUND_HALF_UP |

### Building & Bundling

**Maven Profile** (Recommended):
```bash
# Without extension (relaxed mode, default)
mvn clean package -DskipTests

# With extension (strict mode) - builds and bundles automatically
mvn clean package -DskipTests -Pbuild-extension

# The extension will be built and bundled for the current platform only
# For multi-platform builds (CI/CD), build on each platform separately
```

**Build performance**: The Maven profile uses CMake with Ninja directly (bypassing `make`).
The build is **incremental** — `thunderduck-duckdb-extension/build/release/` persists across `mvn clean`
(Maven only cleans `target/` dirs). First build compiles DuckDB core (~2-5 min),
subsequent builds only recompile changed extension files (seconds).

**Manual Build** (for extension development/verification):
```bash
# Full DuckDB + extension build (equivalent to what Maven does)
cd thunderduck-duckdb-extension && GEN=ninja make release && cd ..

# Bundle for current platform
PLATFORM=linux_amd64  # or osx_arm64, linux_arm64, etc.
mkdir -p core/src/main/resources/extensions/$PLATFORM
cp thunderduck-duckdb-extension/build/release/extension/thdck_spark_funcs/thdck_spark_funcs.duckdb_extension \
   core/src/main/resources/extensions/$PLATFORM/

# Rebuild JAR with extension included
mvn clean package -DskipTests
```

**Prerequisites**: CMake, Ninja, and a C++ compiler (GCC 9+ or Clang 10+)

**Important**: The extension is statically linked against DuckDB (required because JDBC loads
its native library with `RTLD_LOCAL`, making DuckDB symbols unavailable for dynamic resolution).
This is why a full DuckDB compilation is needed, but incremental caching makes it fast after
the initial build.

> Full architecture details: [docs/architect/SPARK_COMPAT_EXTENSION.md](docs/architect/SPARK_COMPAT_EXTENSION.md)

**Last Updated**: 2026-02-07

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

## Development Cheatsheet

### Build

```bash
# Full clean build (always do this before testing)
mvn -f /workspace/pom.xml clean package -DskipTests -q

# Build WITH extension (strict Spark compatibility mode)
# First build compiles DuckDB core (~2-5 min), subsequent builds are incremental (seconds)
mvn -f /workspace/pom.xml clean package -DskipTests -q -Pbuild-extension

# Install core to local repo (REQUIRED before `mvn test -pl tests`)
mvn -f /workspace/pom.xml install -pl core -DskipTests -q

# Kill servers + rebuild (common combo)
pkill -9 -f java 2>/dev/null; sleep 2; mvn -f /workspace/pom.xml clean package -DskipTests -q
```

### Spark Compatibility Mode

```bash
# Strict mode (extension must be loaded, server fails if missing)
THUNDERDUCK_COMPAT_MODE=strict python3 -m pytest ...

# Relaxed mode (no extension, vanilla DuckDB)
THUNDERDUCK_COMPAT_MODE=relaxed python3 -m pytest ...

# Auto mode (default: loads extension if available)
python3 -m pytest ...

# Server CLI flags (alternative to env var)
java -jar connect-server.jar --strict
java -jar connect-server.jar --relaxed
```

### Integration Tests (pytest)

Run from `/workspace/tests/integration`. Fixtures auto-start both servers. Use `python3` (no `python`).

```bash
# Env vars prefix (always include)
ENV="THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30"

# ALL differential tests
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py -v --tb=short

# Single parameterized SQL query (e.g., Q7)
cd /workspace/tests/integration && $ENV python3 -m pytest "differential/test_differential_v2.py::TestTPCH_AllQueries_Differential[7]" -v --tb=long

# Single dedicated SQL test (Q1, Q3, Q6 have their own classes)
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py::TestTPCH_Q1_Differential -v --tb=long

# Single DataFrame test (zero-padded numbers)
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_tpch_differential.py::TestTPCHDifferential::test_q01_dataframe -v --tb=long

# Basic operations only
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py::TestBasicOperations_Differential -v --tb=long

# Sanity check only
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py::TestDifferential_Sanity -v --tb=long
```

**Test naming conventions:**
- Q1, Q3, Q6: dedicated classes `TestTPCH_Q1_Differential` etc.
- Q2, Q4, Q5, Q7-Q22: parameterized `TestTPCH_AllQueries_Differential[N]`
- DataFrame: `TestTPCHDifferential::test_q01_dataframe` (in `test_tpch_differential.py`)

### Java Unit Tests

```bash
mvn -f /workspace/pom.xml install -pl core -DskipTests -q  # install core first
mvn -f /workspace/pom.xml test -pl tests                    # run tests module
mvn -f /workspace/pom.xml test -pl tests -Dtest=TypeInferenceEngineTest  # single class
```

### Server Management

Ports: Thunderduck = `15002`, Spark Reference = `15003`. Pytest fixtures auto-manage servers.

```bash
pkill -9 -f java 2>/dev/null          # kill all servers
pkill -9 -f thunderduck-connect-server # kill Thunderduck only
```

### Change-and-Test Workflow

```bash
pkill -9 -f java 2>/dev/null; sleep 2
mvn -f /workspace/pom.xml clean package -DskipTests -q
cd /workspace/tests/integration && THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 \
  python3 -m pytest "differential/test_differential_v2.py::TestTPCH_AllQueries_Differential[7]" -v --tb=long
pkill -9 -f java 2>/dev/null
```

### Key Data & SQL Paths

| Resource | Path |
|----------|------|
| TPC-H parquet data | `tests/integration/tpch_sf001/*.parquet` |
| TPC-H SQL queries | `tests/integration/sql/tpch_queries/q{1-22}.sql` |
| Test conftest | `tests/integration/conftest.py` |
| DataFrame diff util | `tests/integration/utils/dataframe_diff.py` |

**Last Updated**: 2026-02-06