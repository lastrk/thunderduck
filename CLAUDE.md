# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spark DataFrame API to SQL translation layer project that aims to translate Spark 3.5+ DataFrame operations to SQL via intermediate representations like Apache Calcite or Substrait. The project explores multiple implementation approaches including embedded API and Spark Connect server implementations.

## Key Architecture Decisions

### Translation Target
- Primary target: Apache Calcite RelNode trees (not SQL strings directly)
- Secondary target: Substrait for cross-engine portability
- This enables optimization and multi-engine support (DuckDB, PostgreSQL, etc.)

### Implementation Approaches
1. **Embedded Spark API Implementation** (Java 8): In-process translation with custom DataFrame API
2. **Spark Connect Server Implementation** (Java 21+): gRPC-based server translating Spark Connect protocol to SQL
3. **Hybrid Calcite-First Architecture**: Direct Calcite framework usage with Spark-compatible facade

### Critical Semantic Requirements
- **Numerical Correctness**: Java-style integer division (truncation towards zero, not floor division)
- **Overflow Behavior**: Silent wrap-around (Java semantics) vs SQL error handling
- **Null Handling**: Three-valued logic preservation (null == null returns null, not true)
- **Type Coercion**: Spark's automatic type promotion rules must be preserved

## Project Setup Commands

Since this is a design/analysis phase project without implementation yet, here are the commands for when implementation begins:

### For Maven-based project:
```bash
# Initialize project (when starting implementation)
mvn archetype:generate -DgroupId=com.spark.connect2sql -DartifactId=spark-connect2sql

# Build project
mvn clean compile

# Run tests
mvn test

# Run specific test class
mvn test -Dtest=DifferentialTestFramework

# Package
mvn package
```

### For Gradle-based project:
```bash
# Build project
./gradlew build

# Run tests
./gradlew test

# Run specific test
./gradlew test --tests "*DifferentialTestFramework*"
```

## Code Architecture

### Core Components Structure

#### Translation Layer
- `SQLTranslator`: Converts Spark operations to Calcite RelNode trees
- `TypeMapper`: Maps Spark data types to SQL types with numerical consistency
- `FunctionRegistry`: Maps Spark functions to SQL equivalents or UDFs
- `ExpressionTranslator`: Converts Spark expressions to Calcite RexNode

#### Testing Framework
- `SparkSQLDifferentialTestFramework`: Compares Spark vs SQL execution results
- `SemanticValidator`: Validates preservation of Spark semantics (null handling, numerics)
- `PropertyBasedDataGenerator`: Generates test data for property-based testing
- `MetamorphicTests`: Tests that transformations preserve expected relationships

#### For Spark Connect Server approach:
- `SparkConnectSQLServer`: gRPC server implementation
- `PlanTranslator`: Translates Spark Connect protocol messages to Calcite
- `SessionManager`: Manages client sessions and temporary views
- `SubstraitTranslator`: Converts between Calcite and Substrait representations

## Implementation Phases

The project follows a phased implementation approach as documented in `docs/Analysis_and_Design.md`:

1. **Phase 1**: Core foundation with basic select/filter/project operations
2. **Phase 2**: Expression support including arithmetic, comparison, string/date functions
3. **Phase 3**: Advanced operations (joins, aggregations, window functions)
4. **Phase 4**: Optimization rules and UDF framework

## Testing Strategy

The project emphasizes rigorous testing as documented in `docs/testing_correctness.md`:

- **Differential Testing**: Compare outputs between Spark and SQL engines
- **Property-Based Testing**: Verify algebraic properties hold across random inputs
- **Metamorphic Testing**: Validate transformation relationships
- **Regression Testing**: Prevent reintroduction of fixed bugs

## Key Files

- `docs/Analysis_and_Design.md`: Detailed architecture and implementation plans
- `docs/testing_correctness.md`: Comprehensive testing strategy for semantic preservation
- `initial_prompt.txt`: Original project requirements (ignore this file in normal development)

## Development Guidelines

### Type Safety
Always ensure consistent type mapping between Spark and SQL, especially for:
- Decimal precision/scale
- Integer division semantics
- Timestamp precision (microseconds in Spark)
- Null value handling

### Extension Points
When adding new functionality, consider:
- Custom function registration via `FunctionRegistry`
- SQL dialect adapters for engine-specific syntax
- Calcite optimization rules for Spark-specific patterns
- Test data generators for new data types

### Performance Considerations
- Use lazy evaluation to build complete plans before execution
- Implement predicate and projection pushdown
- Cache translated plans for repeated queries
- Use Apache Arrow for efficient data transfer