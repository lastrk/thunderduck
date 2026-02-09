# Claude Code Project Rules

This file contains project-specific rules and guidelines for working with thunderduck.

## Project Vision

**Keep your Spark API, get single-node DuckDB performance.** Thunderduck is a drop-in Spark Connect server backed by DuckDB for workloads that don't need distributed compute. See [docs/PROJECT_VISION.md](docs/PROJECT_VISION.md) for full context.

## Workflow Orchestration

### 1. Plan Mode Default
Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions). If something goes sideways, STOP and re-plan immediately. Write detailed specs upfront to reduce ambiguity.

### 2. Subagent Strategy
Offload research, exploration, and parallel analysis to subagents. One task per subagent for focused execution.
- **Compile tasks**: Use a subagent for Maven builds. Return success or the focused error message.
- **Test suites**: Use a subagent. Return: Total/Passed/Failed/Errors counts, list of failed tests with one-line error summaries, and the exact command used.

### 3. Self-Improvement Loop
After ANY correction from the user: update `tasks/lessons.md` with the pattern. Review lessons at session start.

### 4. Verification Before Done
Never mark a task complete without proving it works. Run tests, check logs, demonstrate correctness.

### 5. Demand Elegance (Balanced)
For non-trivial changes: pause and ask "is there a more elegant way?" Skip for simple, obvious fixes.

### 6. Autonomous Bug Fixing
When given a bug report: just fix it. Point at logs, errors, failing tests, then resolve them.

## Task Management
**Plan First**: Write plan to `tasks/todo.md` with checkable items. **Track Progress**: Mark items complete as you go. **Capture Lessons**: Update `tasks/lessons.md` after corrections.

## Core Principles
**Simplicity First**: Make every change as simple as possible. Impact minimal code.
**No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
**Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.

## Architecture Quick-Reference

### Key Classes and Their Responsibilities

| Layer | Class | Responsibility |
|-------|-------|---------------|
| **Converter** | `RelationConverter` | Converts Spark Connect protobuf to logical plan nodes |
| **Converter** | `ExpressionConverter` | Converts Spark expression protos to Expression AST |
| **Logical** | `Aggregate`, `Join`, `Filter`, `Project`, etc. | Logical plan nodes with `toSQL(SQLGenerator)` and `inferSchema()` |
| **Expression** | `FunctionCall`, `BinaryExpression`, `CastExpression`, etc. | Expression AST nodes with `toSQL()` and `dataType()` |
| **Generator** | `SQLGenerator` | Visits logical plan tree, generates DuckDB SQL |
| **Runtime** | `DuckDBRuntime` | Manages DuckDB connections, extension loading |
| **Runtime** | `SparkCompatMode` | Strict/relaxed mode detection |
| **Functions** | `FunctionRegistry` | Maps Spark function names to DuckDB equivalents |
| **Types** | `TypeInferenceEngine` | Resolves expression types, aggregate return types |

### CRITICAL: Dual SQL Generation Paths

Several components have TWO code paths for SQL generation. Changes must be applied to BOTH:

| Component | Path 1 (Plan Node) | Path 2 (SQLGenerator) | When Path 2 is Used |
|-----------|--------------------|-----------------------|---------------------|
| **Aggregate** | `Aggregate.toSQL()` | `SQLGenerator.visitAggregate()` | When SQLGenerator visits the plan tree |
| **Join** | `Join.toSQL()` via `visitJoin()` | `generateFlatJoinChainWithMapping()` | When join chain optimization is triggered by Project/Filter above Join |

**Rule:** When modifying SQL generation for aggregates or joins, ALWAYS check both paths.

### Expression Hierarchy

```
Expression (abstract)
  +-- FunctionCall         # func(args...) -- uses FunctionRegistry for translation
  +-- BinaryExpression     # left OP right
  +-- UnaryExpression      # OP operand
  +-- CastExpression       # CAST(expr AS type)
  +-- Literal              # constant values
  +-- UnresolvedColumn     # column references
  +-- AliasExpression      # expr AS alias
  +-- Aggregate.AggregateExpression  # AGG(arg) or composite (rawExpression)
```

### Raw SQL vs DataFrame API Code Paths

Raw SQL (`spark.sql("SELECT ...")`) goes directly to DuckDB -- no logical plan, no type inference. DataFrame API goes through converter -> logical plan -> SQLGenerator -> DuckDB with full type awareness.

**Implication:** Fixes to type inference or SQL rewriting in the logical plan layer do NOT affect raw SQL queries.

## Known Gotchas

1. **toSQL() vs toString()**: Expression rendering MUST use `toSQL()`, not `toString()`. The `toString()` method is for debug logging only. Bug example: `FunctionCall.toSQL()` was calling `Expression::toString` on arguments instead of `Expression::toSQL`.

2. **Composite aggregate expressions**: When adding new expression types that can appear inside aggregates (e.g., BinaryExpression wrapping FunctionCalls), ensure `RelationConverter.convertAggregate()` handles them. The default `else` branch previously silently dropped non-FunctionCall expressions.

3. **Semi/Anti join in flat join chains**: The join chain optimizer (`generateFlatJoinChainWithMapping`) does NOT convert semi/anti joins to EXISTS subqueries. Only `visitJoin()` does this correctly. If a semi/anti join appears in a chain, the chain must break at that point.

4. **DuckDB SEMI JOIN syntax**: DuckDB supports `SEMI JOIN` and `ANTI JOIN` (without the `LEFT` prefix). Using `LEFT SEMI JOIN` causes parser errors.

5. **Maven -q flag hides errors**: When using `mvn -q`, build failures may show exit code 1 but no error details. Remove `-q` when debugging build failures.

6. **Session-scoped test servers**: Test servers (port 15002/15003) are session-scoped -- started once, reused across all test classes. No automatic health checks or restarts between classes. If a server becomes unresponsive, subsequent test classes fail.

7. **Always clean build before testing**: Never test with a stale build. Always run `mvn clean package -DskipTests` before integration tests.

## Spark Parity Requirements

**Critical Rule**: Thunderduck must match Spark EXACTLY, not just produce equivalent results.

- **Return types**: If Spark returns DOUBLE, Thunderduck must return DOUBLE (not BIGINT)
- **Rounding conventions**: Must match Spark's rounding behavior
- **Type coercion**: Implicit casts must follow Spark's rules
- **NULL handling**: Must match Spark's null propagation

Differential tests must validate: same row count, same column names, **same column types**, same values (with epsilon for floats), same null handling, same sort order (ties are non-deterministic per SQL standard).

**Goal**: Drop-in replacement for Spark, not "Spark-like" behavior.

## Spark Compatibility Extension

Two modes: **Relaxed** (default, no extension, ~85% compat) and **Strict** (extension loaded, ~100% compat).

```bash
# Build WITHOUT extension (relaxed mode, default)
mvn clean package -DskipTests

# Build WITH extension (strict mode)
mvn clean package -DskipTests -Pbuild-extension
```

Key rules: Extension build is NOT part of default Maven lifecycle. Extension DuckDB version must exactly match `duckdb_jdbc` dependency. Extension is unsigned (requires `allow_unsigned_extensions=true`).

> Full details: [docs/architect/SPARK_COMPAT_EXTENSION.md](docs/architect/SPARK_COMPAT_EXTENSION.md)

## Spark Connect Server Configuration

**Protobuf**: Use `provided` scope for `spark-connect_2.13` dependency to avoid `VerifyError` from version mismatch.

**Arrow JVM Flags**: Required on ALL platforms: `--add-opens=java.base/java.nio=ALL-UNNAMED`

**Error if missing**: `java.lang.RuntimeException: Failed to initialize MemoryUtil.`

**Server cleanup**: Always kill server processes after tests: `pkill -9 -f java 2>/dev/null`

> Full details: [docs/architect/PROTOBUF_AND_ARROW_CONFIGURATION.md](docs/architect/PROTOBUF_AND_ARROW_CONFIGURATION.md)

## Documentation Structure Rules

1. **Current focus documents** (workspace root, `CURRENT_FOCUS_*` prefix) -- active work items
2. **Permanent documentation** (`docs/`) -- architecture decisions, dev journal, specs
3. **Developer journal** (`docs/dev_journal/`) -- milestone reports prefixed `M[X]_`
4. **Test infrastructure** (`tests/scripts/`) -- test runner scripts

Keep workspace root clean: only README.md, CLAUDE.md, and CURRENT_FOCUS_* files. Archive stale focus documents.

## Git Commit Workflow

**Critical Rule**: NEVER commit code without user review first. Show changes, wait for explicit approval, then commit.

## Development Cheatsheet

### Build

```bash
# Full clean build (always do this before testing)
mvn -f /workspace/pom.xml clean package -DskipTests -q

# Build WITH extension (strict Spark compatibility mode)
mvn -f /workspace/pom.xml clean package -DskipTests -q -Pbuild-extension

# Install core to local repo (REQUIRED before `mvn test -pl tests`)
mvn -f /workspace/pom.xml install -pl core -DskipTests -q

# Kill servers + rebuild (common combo)
pkill -9 -f java 2>/dev/null; sleep 2; mvn -f /workspace/pom.xml clean package -DskipTests -q

# Build WITHOUT quiet mode (for debugging build failures)
mvn -f /workspace/pom.xml clean package -DskipTests 2>&1 | tail -100

# Build single module only (faster iteration)
mvn -f /workspace/pom.xml compile -pl core -DskipTests
mvn -f /workspace/pom.xml compile -pl connect-server -DskipTests
```

### Spark Compatibility Mode

```bash
THUNDERDUCK_COMPAT_MODE=strict python3 -m pytest ...   # strict mode
THUNDERDUCK_COMPAT_MODE=relaxed python3 -m pytest ...  # relaxed mode
python3 -m pytest ...                                   # auto mode (default)
```

### Integration Tests (pytest)

Run from `/workspace/tests/integration`. Fixtures auto-start both servers. Use `python3` (not `python`).

```bash
# Env vars prefix (always include)
ENV="THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30"

# --- Full differential test suite (ALL tests across 36 files) ---
# This is the canonical "run everything" command
cd /workspace/tests/integration && $ENV python3 -m pytest differential/ -v --tb=short

# --- Quick check: TPC-H only (51 tests: 29 SQL + 22 DataFrame) ---
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py differential/test_tpch_differential.py -v --tb=short

# --- TPC-DS only (SQL + DataFrame) ---
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_tpcds_differential.py differential/test_tpcds_dataframe_differential.py -v --tb=short

# --- Single test file (e.g., joins, window functions, etc.) ---
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_joins_differential.py -v --tb=long

# --- Single parameterized SQL query (e.g., TPC-H Q7) ---
cd /workspace/tests/integration && $ENV python3 -m pytest "differential/test_differential_v2.py::TestTPCH_AllQueries_Differential[7]" -v --tb=long

# --- Single dedicated SQL test (Q1, Q3, Q6 have their own classes) ---
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_differential_v2.py::TestTPCH_Q1_Differential -v --tb=long

# --- Single DataFrame test (zero-padded numbers) ---
cd /workspace/tests/integration && $ENV python3 -m pytest differential/test_tpch_differential.py::TestTPCHDifferential::test_q01_dataframe -v --tb=long
```

**Test tiers:**
- **Full suite**: `pytest differential/` — runs ALL 36 test files (TPC-H, TPC-DS, joins, aggregations, window functions, array functions, datetime, type casting, etc.)
- **Quick check**: `test_differential_v2.py test_tpch_differential.py` — TPC-H only (51 tests)
- **TPC-DS**: `test_tpcds_differential.py test_tpcds_dataframe_differential.py`
- **Single file/test**: target specific test files or parameterized tests

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

Ports: Thunderduck = `15002`, Spark Reference = `15003` (configurable via `THUNDERDUCK_PORT` / `SPARK_PORT` env vars). Pytest fixtures auto-manage servers.

```bash
pkill -9 -f java 2>/dev/null          # kill all servers
pkill -9 -f thunderduck-connect-server # kill Thunderduck only
```

### Parallel Test Runs

Server ports are configurable via `THUNDERDUCK_PORT` and `SPARK_PORT` env vars (defaults: 15002/15003). This allows running multiple differential test suites in parallel on separate port pairs — useful for worktree-based development or testing different branches simultaneously.

```bash
# Terminal 1 — default ports (15002/15003)
cd /workspace/tests/integration && \
  THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 \
  python3 -m pytest differential/ -v --tb=short

# Terminal 2 — custom ports (15012/15013), different worktree
cd /workspace2/tests/integration && \
  THUNDERDUCK_PORT=15012 SPARK_PORT=15013 \
  THUNDERDUCK_TEST_SUITE_CONTINUE_ON_ERROR=true COLLECT_TIMEOUT=30 \
  python3 -m pytest differential/ -v --tb=short
```

**Rules**: Each parallel run needs a unique port pair. Both env vars must be set together to avoid port conflicts. Each worktree needs its own build (`mvn clean package`).

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

**Last Updated**: 2026-02-09
