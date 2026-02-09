# Lessons Learned

Review this file at session start to avoid repeating past mistakes.

## 2026-02-08: Composite Aggregates Silently Dropped

- **Pattern**: When aggregate expressions contain non-FunctionCall nodes (e.g., BinaryExpression wrapping FunctionCalls), they were silently dropped by `RelationConverter.convertAggregate()`. The default `else` branch did nothing.
- **Rule**: Both `Aggregate.toSQL()` and `SQLGenerator.visitAggregate()` must handle any new aggregate expression type.
- **Rule**: Always handle the `else` case explicitly -- at minimum, log a warning or throw an error for unsupported expression types.

## 2026-02-08: FunctionCall.toSQL() Used toString() on Arguments

- **Bug**: `FunctionCall.toSQL()` was calling `Expression::toString` instead of `Expression::toSQL` on arguments, causing incorrect SQL rendering for complex argument expressions (nested functions, casts, etc.).
- **Rule**: Any code that converts an Expression to a SQL string must use `toSQL()`, never `toString()`. The `toString()` method is for debug logging only.

## 2026-02-08: Semi/Anti Join Dual Path Bug

- **Bug**: `generateFlatJoinChainWithMapping()` emitted `LEFT SEMI JOIN` which DuckDB does not support. Only `visitJoin()` correctly rewrites semi/anti joins.
- **Rule**: When fixing join SQL generation, always check BOTH `visitJoin()` and `generateFlatJoinChainWithMapping()`.
- **Fix**: Changed to DuckDB-native `SEMI JOIN` / `ANTI JOIN` syntax (no `LEFT` prefix).

## 2026-02-08: Maven -q Flag Hides Build Errors

- **Issue**: `mvn -q` returns exit code 1 but shows no error output. This caused a subagent to incorrectly report build failure before discovering it was a process-kill timing issue.
- **Rule**: Use `-q` for routine builds only. Remove `-q` when investigating build failures to see full error output.

## 2026-02-09: preprocessSQL Regex Breaks Window Functions

- **Bug**: `preprocessSQL()` regex for `count(*) AS "count(1)"` aliasing didn't exclude `count(*) OVER (...)`, producing `count(*) AS "count(1)" OVER (PARTITION BY ...)` which DuckDB rejects.
- **Root cause**: String-based SQL manipulation can't understand semantic context. The regex sees `count(*)` but doesn't know it's part of a window function.
- **Rule**: Never add new regex hacks to `preprocessSQL()`. Fix the issue at the AST layer (parser, expression, or SQLGenerator) instead.
- **Tracking**: See `docs/architect/PREPROCESSSQL_ELIMINATION_PLAN.md` for the plan to remove all SQL string hacks.

## 2026-02-08: Always Clean Build Before Testing

- **Issue**: Repeatedly tested with stale builds, then was surprised that code changes had no effect.
- **Rule**: Always run `mvn clean package -DskipTests` before running integration tests. Never assume the previous build reflects the current source.
