# Spark DataFrame API to SQL Translation Layer - Analysis and Design

## Executive Summary

This document analyzes approaches for translating Spark DataFrame API operations to SQL, targeting Apache Calcite to leverage its powerful optimization capabilities and flexibility of targetting different SQL dialects and engines. The focus is on query operations with consistent numerical semantics, excluding streaming capabilities.

## Problem Statement

Build a translation layer that:
- Converts Spark 3.5+ DataFrame API calls to SQL operations
- Targets Apache Calcite for multi-engine support
- Ensures numerical consistency between Spark and SQL data types
- Maps unsupported Spark functions to SQL extension functions
- Supports common DataFrame transformations (not 100% coverage)

## Architectural Approaches

### Approach 1: Embedded Spark API Implementation (In-Process)

**Overview**: Implement a Spark-compatible DataFrame API that directly translates operations to SQL/Calcite within the client application process.

**Architecture**:
```
Scala/Java Application
    ↓
Custom DataFrame API (Spark-compatible)
    ↓
Translation Layer (in-process)
    ↓
Apache Calcite RelNode Tree
    ↓
SQL Engine (DuckDB, PostgreSQL, etc.)
```

**Advantages**:
- Low latency (no network overhead)
- Simple deployment (single JAR)
- Easy debugging and testing
- Direct control over translation logic
- No separate server infrastructure required
- Better for embedded/edge scenarios

**Disadvantages**:
- Requires reimplementation of DataFrame API surface
- Limited scalability (bound to single JVM)
- No natural separation between client and execution
- Harder to share computation resources
- More complex dependency management

### Approach 2: Spark Connect Server Implementation

**Overview**: Implement a custom Spark Connect server that receives DataFrame operations via gRPC and translates them to SQL.

**Architecture**:
```
Spark Client (standard Spark 3.5+)
    ↓ (gRPC/Protobuf)
Custom Spark Connect Server
    ↓
Translation Engine
    ↓
Apache Calcite/Substrait
    ↓
SQL Execution Engine
```

**Advantages**:
- Uses standard Spark client libraries (no custom API)
- Natural client-server separation
- Scalable architecture (multiple clients, server pooling)
- Protocol buffer-based communication (language agnostic)
- Reuses existing Spark Connect protocol
- Better for distributed/cloud deployments

**Disadvantages**:
- Network overhead for communication
- More complex deployment (server + client)
- Requires gRPC/protobuf infrastructure
- Additional serialization/deserialization cost
- Server lifecycle management complexity

### Approach 3: Hybrid Calcite-First Architecture

**Overview**: Build directly on Apache Calcite's framework, exposing both embedded and server modes with a Spark-compatible API facade.

**Architecture**:
```
Spark-Compatible API Layer
    ↓
Calcite Adapter Framework
    ↓
RelNode Builder & Optimizer
    ↓
Multiple SQL Dialects/Engines
```

**Advantages**:
- Leverages Calcite's mature optimization framework
- Built-in support for multiple SQL dialects
- Extensible through Calcite's adapter pattern
- Can support both embedded and server modes
- Rich set of optimization rules

**Disadvantages**:
- Steeper learning curve (Calcite internals)
- May require custom Calcite extensions
- Less straightforward mapping from Spark operations

## Data Type Mapping Strategy

### Critical Numerical Consistency Considerations

#### Integer Types
```java
// Spark -> SQL Type Mapping
SparkType.ByteType    -> SMALLINT    // SQL typically doesn't have TINYINT
SparkType.ShortType   -> SMALLINT
SparkType.IntegerType -> INTEGER
SparkType.LongType    -> BIGINT
```

#### Decimal/Floating Point
```java
// Precision-critical mappings
SparkType.FloatType     -> REAL/FLOAT4
SparkType.DoubleType    -> DOUBLE PRECISION
SparkType.DecimalType(p,s) -> DECIMAL(p,s)
```

#### Key Gotchas:
1. **Integer Division**: Spark uses Java semantics (truncation), SQL may vary
2. **Null Handling**: Spark's three-valued logic vs SQL variations
3. **Overflow Behavior**: Spark wraps, SQL may error or saturate
4. **Decimal Precision**: Ensure consistent scale/precision rules
5. **Timestamp Precision**: Microsecond (Spark) vs varying SQL precision

### Extension Function Mapping

Functions without direct SQL equivalents will be mapped to UDFs:
```java
// Example: Spark's array_distinct -> custom UDF
SPARK_ARRAY_DISTINCT(array_column)

// Example: Spark's regexp_extract with groups
SPARK_REGEXP_EXTRACT(string, pattern, group_idx)
```

## Implementation Plans

### Option 1: Embedded Implementation (Java 8)

#### Core Components

##### 1. DataFrame API Implementation
```java
public class SQLDataFrame implements DataFrame {
    private final CalciteConnection connection;
    private final RelNode relationalPlan;
    private final SQLTranslator translator;

    @Override
    public DataFrame select(Column... cols) {
        RelNode newPlan = translator.translateSelect(relationalPlan, cols);
        return new SQLDataFrame(connection, newPlan, translator);
    }

    @Override
    public DataFrame filter(Column condition) {
        RelNode newPlan = translator.translateFilter(relationalPlan, condition);
        return new SQLDataFrame(connection, newPlan, translator);
    }

    @Override
    public Dataset<Row> collect() {
        String sql = RelToSqlConverter.convert(relationalPlan);
        return executeSql(sql);
    }
}
```

##### 2. Translation Engine
```java
public class SQLTranslator {
    private final RelBuilder relBuilder;
    private final TypeMapper typeMapper;
    private final FunctionRegistry functionRegistry;

    public RelNode translateSelect(RelNode input, Column[] columns) {
        relBuilder.push(input);
        List<RexNode> projections = Arrays.stream(columns)
            .map(this::columnToRexNode)
            .collect(Collectors.toList());
        return relBuilder.project(projections).build();
    }

    private RexNode columnToRexNode(Column col) {
        // Convert Spark Column to Calcite RexNode
        if (col instanceof ColumnRef) {
            return relBuilder.field(col.getName());
        } else if (col instanceof Expression) {
            return translateExpression((Expression) col);
        }
        // ... handle other column types
    }
}
```

##### 3. Type Mapping System
```java
public class TypeMapper {
    private static final Map<DataType, RelDataType> TYPE_MAPPINGS;

    static {
        TYPE_MAPPINGS = new HashMap<>();
        TYPE_MAPPINGS.put(DataTypes.IntegerType, SqlTypeName.INTEGER);
        TYPE_MAPPINGS.put(DataTypes.LongType, SqlTypeName.BIGINT);
        TYPE_MAPPINGS.put(DataTypes.DoubleType, SqlTypeName.DOUBLE);
        // ... additional mappings
    }

    public RelDataType toCalciteType(DataType sparkType, RelDataTypeFactory factory) {
        if (sparkType instanceof DecimalType) {
            DecimalType dt = (DecimalType) sparkType;
            return factory.createSqlType(SqlTypeName.DECIMAL,
                dt.precision(), dt.scale());
        }
        return factory.createSqlType(TYPE_MAPPINGS.get(sparkType));
    }
}
```

##### 4. Function Registry
```java
public class FunctionRegistry {
    private final Map<String, SqlOperator> functionMappings;
    private final Map<String, UDFImplementation> customFunctions;

    public SqlOperator getOperator(String sparkFunction) {
        if (functionMappings.containsKey(sparkFunction)) {
            return functionMappings.get(sparkFunction);
        } else if (customFunctions.containsKey(sparkFunction)) {
            return createUDFOperator(customFunctions.get(sparkFunction));
        }
        throw new UnsupportedOperationException(
            "Function not supported: " + sparkFunction);
    }
}
```

### Option 2: Spark Connect Server Implementation (Java 21)

#### Core Components

##### 1. gRPC Server Implementation
```java
public class SparkConnectSQLServer extends SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    private final SessionManager sessionManager;
    private final SQLExecutor sqlExecutor;
    private final PlanTranslator planTranslator;

    @Override
    public void executePlan(ExecutePlanRequest request,
                           StreamObserver<ExecutePlanResponse> responseObserver) {
        var sessionId = request.getSessionId();
        var plan = request.getPlan();

        try {
            // Translate Spark plan to Calcite
            RelNode relNode = planTranslator.translate(plan);

            // Execute and stream results
            try (var resultStream = sqlExecutor.execute(relNode)) {
                resultStream.forEach(row -> {
                    var response = ExecutePlanResponse.newBuilder()
                        .setSessionId(sessionId)
                        .setResponseId(UUID.randomUUID().toString())
                        .setArrowBatch(rowToArrowBatch(row))
                        .build();
                    responseObserver.onNext(response);
                });
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getMessage())
                .asRuntimeException());
        }
    }
}
```

##### 2. Plan Translation Layer
```java
public class PlanTranslator {
    private final RelBuilder relBuilder;
    private final TypeSystem typeSystem;

    public RelNode translate(Plan sparkPlan) {
        return switch (sparkPlan.getRootCase()) {
            case RELATION -> translateRelation(sparkPlan.getRoot().getRelation());
            case COMMAND -> throw new UnsupportedOperationException("Commands not supported");
            default -> throw new IllegalArgumentException("Unknown plan type");
        };
    }

    private RelNode translateRelation(Relation relation) {
        return switch (relation.getRelTypeCase()) {
            case READ -> translateRead(relation.getRead());
            case FILTER -> translateFilter(relation.getFilter());
            case PROJECT -> translateProject(relation.getProject());
            case JOIN -> translateJoin(relation.getJoin());
            case AGGREGATE -> translateAggregate(relation.getAggregate());
            case SORT -> translateSort(relation.getSort());
            case LIMIT -> translateLimit(relation.getLimit());
            default -> throw new UnsupportedOperationException(
                "Relation type not supported: " + relation.getRelTypeCase());
        };
    }
}
```

##### 3. Expression Translation
```java
public class ExpressionTranslator {
    private final RexBuilder rexBuilder;
    private final FunctionRegistry functionRegistry;

    public RexNode translate(Expression expr) {
        return switch (expr.getExprTypeCase()) {
            case LITERAL -> translateLiteral(expr.getLiteral());
            case UNRESOLVED_ATTRIBUTE -> translateAttribute(expr.getUnresolvedAttribute());
            case UNRESOLVED_FUNCTION -> translateFunction(expr.getUnresolvedFunction());
            case ALIAS -> translateAlias(expr.getAlias());
            case CAST -> translateCast(expr.getCast());
            default -> throw new UnsupportedOperationException(
                "Expression type not supported: " + expr.getExprTypeCase());
        };
    }

    private RexNode translateFunction(UnresolvedFunction func) {
        String funcName = func.getFunctionName();
        List<RexNode> args = func.getArgumentsList().stream()
            .map(this::translate)
            .toList();

        SqlOperator operator = functionRegistry.getOperator(funcName);
        if (operator == null) {
            // Create UDF call
            operator = createUDF(funcName);
        }

        return rexBuilder.makeCall(operator, args);
    }
}
```

##### 4. Session and State Management
```java
public class SessionManager {
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    record Session(
        String sessionId,
        CalciteConnection connection,
        Map<String, RelNode> tempViews,
        Instant lastActivity
    ) {}

    public Session getOrCreateSession(String sessionId) {
        return sessions.computeIfAbsent(sessionId, id -> {
            var connection = createCalciteConnection();
            return new Session(id, connection, new ConcurrentHashMap<>(), Instant.now());
        });
    }

    public void registerTempView(String sessionId, String viewName, RelNode plan) {
        var session = sessions.get(sessionId);
        if (session != null) {
            session.tempViews().put(viewName, plan);
        }
    }
}
```

##### 5. Substrait Integration
```java
public class SubstraitTranslator {
    private final SubstraitRelConverter converter;

    public byte[] toSubstrait(RelNode relNode) {
        var substraitPlan = converter.convert(relNode);
        return substraitPlan.toByteArray();
    }

    public RelNode fromSubstrait(byte[] substraitPlan) {
        var plan = Plan.parseFrom(substraitPlan);
        return converter.convert(plan);
    }

    // Enable feeding to DuckDB directly
    public void executeDuckDB(RelNode relNode, DuckDBConnection conn) {
        byte[] substrait = toSubstrait(relNode);
        conn.executeSubstrait(substrait);
    }
}
```

## Testing Strategy

### Unit Tests
- Type mapping correctness
- Expression translation accuracy
- Function mapping validation
- Numerical precision tests

### Integration Tests
- End-to-end DataFrame operations
- Cross-engine SQL generation
- Performance benchmarks
- Compatibility tests with Spark examples

### Numerical Consistency Tests
```java
@Test
public void testIntegerDivision() {
    DataFrame df = createDataFrame(Arrays.asList(
        Row.of(10, 3),
        Row.of(-10, 3)
    ));

    DataFrame result = df.select(col("a").divide(col("b")));

    // Verify Spark semantics (truncation towards zero)
    assertEquals(3, result.first().getInt(0));  // 10/3 = 3
    assertEquals(-3, result.first().getInt(1)); // -10/3 = -3
}
```

## Performance Considerations

1. **Query Optimization**: Leverage Calcite's cost-based optimizer
2. **Lazy Evaluation**: Build complete plan before execution
3. **Pushdown**: Implement predicate and projection pushdown
4. **Caching**: Cache translated plans for repeated queries
5. **Vectorization**: Use Arrow format for data transfer

## Extensibility Points

1. **Custom Functions**: Plugin architecture for UDF registration
2. **SQL Dialects**: Adapter pattern for different SQL engines
3. **Type Extensions**: Support for complex/nested types
4. **Optimization Rules**: Custom Calcite rules for Spark-specific patterns

## Recommended Implementation Path

### Phase 1: Core Foundation (Weeks 1-4)
- Basic DataFrame API structure
- Calcite integration setup
- Simple select/filter/project operations
- Type mapping framework

### Phase 2: Expression Support (Weeks 5-8)
- Column expressions and literals
- Basic arithmetic and comparison operators
- String and date functions
- Null handling

### Phase 3: Advanced Operations (Weeks 9-12)
- Joins (inner, left, right, full)
- Aggregations (group by, window functions)
- Sorting and limiting
- Subqueries

### Phase 4: Optimization & Extensions (Weeks 13-16)
- Cost-based optimization rules
- UDF framework
- Substrait integration
- Performance tuning

## Conclusion

For maximum flexibility and adoption, implementing **both approaches** is recommended:

1. **Start with the Spark Connect Server** approach for immediate compatibility with existing Spark applications
2. **Add the embedded API** as a lightweight alternative for edge/embedded scenarios

Using Apache Calcite as the intermediate representation provides:
- Mature optimization framework
- Multi-engine support
- Extensibility for custom functions
- Well-tested SQL generation

The modular design allows sharing the core translation logic between both approaches, minimizing duplication while maximizing deployment flexibility.