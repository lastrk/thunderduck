# Spark DataFrame to SQL Translation Layer

This project implements an embedded Spark DataFrame API replacement that translates operations to SQL and executes them on DuckDB. The implementation provides a drop-in replacement for Spark's DataFrame API while executing queries on an embedded SQL engine.

## Architecture

The project follows a layered architecture:

1. **Spark API Layer** - Drop-in replacement for Spark DataFrame API
2. **Plan Representation** - Internal logical plan structure
3. **Translation Layer** - Converts Spark plans to SQL (via Calcite in later phases)
4. **SQL Generation** - Generates optimized SQL
5. **Execution Engine** - Runs SQL on embedded DuckDB

## Project Status

**Phase 1: Foundation and Basic Operations** ✅ COMPLETE (Oct 9, 2025)

📊 For detailed implementation progress, see [PROGRESS.md](PROGRESS.md)

### Phase 1 Highlights

✅ **Core Infrastructure**
- Maven multi-module project structure (Java 8 compatible)
- Full Spark DataFrame API facade with 50+ expression types
- DuckDB 1.1.3 embedded SQL engine integration
- Clean separation of concerns across 6 modules

✅ **Testing Framework**
- Differential testing framework comparing against real Spark 3.5.3
- 100% passing tests for implemented operations
- Automated verification of schema and result equality

### Key Test Results

The differential testing framework runs identical operations on both:
- **Real Apache Spark 3.5.3** (in-memory)
- **Our DuckDB-based implementation**

Tests verify:
- Schema compatibility
- Result set equality
- Null handling semantics
- Numerical precision
- Integer division semantics (truncation vs floor)

## Building the Project

### Prerequisites
- Java 11 or later
- Maven 3.6+
- DuckDB JDBC driver (automatically downloaded)

### Build Commands
```bash
# Build all modules
mvn clean compile

# Run tests (requires Spark 3.5.3)
mvn test

# Package
mvn package
```

## Usage Example

```java
// Create a SparkSession (our implementation)
SparkSession spark = SparkSession.builder()
    .appName("MyApp")
    .getOrCreate();

// Create a DataFrame
Dataset<Row> df = spark.createDataFrame(data, schema);

// Perform operations
Dataset<Row> result = df
    .filter("age > 25")
    .select("name", "age")
    .orderBy("age");

// Execute and collect results
List<Row> rows = result.collect();
```

## Implementation Highlights

### Numerical Semantics
The implementation preserves Spark's Java-based numerical semantics:
- Integer division uses truncation (not floor division)
- Null propagation follows three-valued logic
- Overflow behavior matches Java semantics

### DuckDB Integration
- Embedded DuckDB 1.1.3 for SQL execution
- Custom UDFs for Spark-compatible functions
- PostgreSQL dialect for SQL generation

### Testing Strategy
Differential testing ensures correctness by comparing:
- Every operation against real Spark
- Schema and data type mappings
- Edge cases (nulls, division by zero, etc.)

## Next Steps

**Phase 2: Expression Support and Core Functions** (In Progress)
- JOIN operations (inner, outer, cross)
- GROUP BY and aggregation framework
- Window functions
- Extended function library
- See [PROGRESS.md](PROGRESS.md) for detailed roadmap

## Known Limitations

Current implementation is Phase 1 - basic operations only:
- No JOIN support yet
- No aggregation functions yet
- Simple SQL generation (no optimization)
- Limited function support

## Contributing

This is a proof-of-concept implementation following the design in `docs/Implementation_Plan_Embedded.md`.