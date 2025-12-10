# Comprehensive Testing Strategy for DuckDB Execution Mode

## Executive Summary

This document defines a multi-layer testing strategy for validating the Spark DataFrame to DuckDB translation layer. The strategy prioritizes:

1. **Correctness**: Numerical consistency with Spark using differential testing
2. **Performance**: Achieving 5-10x speedup over Spark local mode
3. **Maintainability**: Fast test execution and clear failure diagnostics
4. **Confidence**: Progressive validation from unit to integration to performance

**Key Metrics:**
- Target: 500+ passing differential tests
- Test execution time: <5 minutes for full suite
- Performance validation: TPC-H and TPC-DS at SF=0.01
- Code coverage: 80%+ on translation engine

## Testing Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 4: Comparative Analysis                              │
│  - Embedded DuckDB vs External Spark Cluster                │
│  - Performance profiling and bottleneck analysis            │
│  - Cost-benefit validation                                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Layer 3: Performance Benchmarks                            │
│  - TPC-H queries (SF=0.01, 22 queries)                      │
│  - TPC-DS queries (SF=0.01, 99 queries)                     │
│  - Memory profiling and resource utilization                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Layer 2: Integration Tests                                 │
│  - End-to-end transformation chains                         │
│  - Format readers (Parquet, Delta, Iceberg)                 │
│  - Multi-operation workflows                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Layer 1: BDD Unit Tests (Spark Local Mode as Oracle)      │
│  - Given-When-Then structure                                │
│  - Type mapping correctness                                 │
│  - Expression translation accuracy                          │
│  - Function mapping validation                              │
└─────────────────────────────────────────────────────────────┘
```

## Layer 1: BDD Unit Tests with Spark Local Mode as Reference Oracle

### Philosophy

Use Spark 3.5 local mode as the **reference oracle** for correctness validation. Every test executes the same operation on both systems and compares:
- Schema (data types, column names, nullability)
- Data (row-by-row comparison with tolerance for floating-point)
- Behavior (error handling, edge cases)

### BDD Test Structure

#### Given-When-Then Pattern

```java
public class BDDTestFramework {

    /**
     * Given: Test data setup
     * When: DataFrame operation execution
     * Then: Result validation against Spark
     */
    @Test
    public void testFilterOperation() {
        // GIVEN: A DataFrame with sample data
        DataFrame sparkDF = givenSparkDataFrame()
            .withColumn("id", DataTypes.IntegerType)
            .withColumn("name", DataTypes.StringType)
            .withColumn("age", DataTypes.IntegerType)
            .withRows(
                Row.of(1, "Alice", 25),
                Row.of(2, "Bob", 30),
                Row.of(3, "Charlie", 35)
            )
            .create();

        DataFrame duckDB = givenDuckDBDataFrame()
            .withSameData(sparkDF)
            .create();

        // WHEN: Applying filter operation
        Dataset<Row> sparkResult = whenApplyingToSpark(sparkDF)
            .filter("age > 25")
            .execute();

        Dataset<Row> duckDBResult = whenApplyingToDuckDB(duckDB)
            .filter("age > 25")
            .execute();

        // THEN: Results should match
        thenResultsShouldMatch(sparkResult, duckDBResult)
            .assertSchemaEquals()
            .assertDataEquals()
            .assertRowCount(2);
    }
}
```

#### Test Categories

**1. Type Mapping Tests**

```java
@Nested
@DisplayName("Type Mapping Correctness")
class TypeMappingTests {

    @Test
    @DisplayName("Given numeric types, when mapped, then DuckDB types match Spark semantics")
    public void testNumericTypeMapping() {
        // GIVEN: DataFrame with all numeric types
        given()
            .sparkDataFrame()
            .withColumns(
                col("byte_col", DataTypes.ByteType),
                col("short_col", DataTypes.ShortType),
                col("int_col", DataTypes.IntegerType),
                col("long_col", DataTypes.LongType),
                col("float_col", DataTypes.FloatType),
                col("double_col", DataTypes.DoubleType),
                col("decimal_col", DataTypes.createDecimalType(10, 2))
            )
            .withSampleData();

        // WHEN: Reading and writing through DuckDB
        when()
            .executeSelectAll();

        // THEN: Types should be preserved
        then()
            .assertTypeMapping("byte_col", "TINYINT")
            .assertTypeMapping("short_col", "SMALLINT")
            .assertTypeMapping("int_col", "INTEGER")
            .assertTypeMapping("long_col", "BIGINT")
            .assertTypeMapping("float_col", "FLOAT")
            .assertTypeMapping("double_col", "DOUBLE")
            .assertTypeMapping("decimal_col", "DECIMAL(10,2)")
            .assertNumericalConsistency();
    }

    @Test
    @DisplayName("Given complex types, when mapped, then nested structures preserved")
    public void testComplexTypeMapping() {
        // GIVEN: DataFrame with arrays, maps, structs
        given()
            .sparkDataFrame()
            .withColumns(
                col("array_col", DataTypes.createArrayType(DataTypes.IntegerType)),
                col("map_col", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)),
                col("struct_col", DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("field1", DataTypes.StringType, true),
                    DataTypes.createStructField("field2", DataTypes.IntegerType, true)
                )))
            )
            .withSampleData();

        // WHEN: Reading through DuckDB
        when()
            .executeSelectAll();

        // THEN: Complex types should be preserved
        then()
            .assertTypeMapping("array_col", "INTEGER[]")
            .assertTypeMapping("map_col", "MAP(VARCHAR, INTEGER)")
            .assertStructFieldsMatch("struct_col")
            .assertDataEquals();
    }

    @Test
    @DisplayName("Given timestamp types, when mapped, then microsecond precision preserved")
    public void testTimestampPrecision() {
        // GIVEN: DataFrame with timestamp data
        given()
            .sparkDataFrame()
            .withColumns(
                col("timestamp_col", DataTypes.TimestampType),
                col("date_col", DataTypes.DateType)
            )
            .withData(
                Row.of(Timestamp.valueOf("2024-01-15 10:30:45.123456"), Date.valueOf("2024-01-15")),
                Row.of(Timestamp.valueOf("2024-01-15 10:30:45.999999"), Date.valueOf("2024-01-16"))
            );

        // WHEN: Reading through DuckDB
        when()
            .executeSelectAll();

        // THEN: Microsecond precision should be preserved
        then()
            .assertTimestampPrecision(6) // microseconds
            .assertDataEquals();
    }
}
```

**2. Expression Translation Tests**

```java
@Nested
@DisplayName("Expression Translation Accuracy")
class ExpressionTranslationTests {

    @Test
    @DisplayName("Given arithmetic expressions, when translated, then numerical semantics match")
    public void testArithmeticExpressions() {
        // GIVEN: DataFrame with numeric columns
        given()
            .sparkDataFrame()
            .withColumns(col("a", IntegerType), col("b", IntegerType))
            .withData(
                Row.of(10, 3),
                Row.of(-10, 3),
                Row.of(10, -3),
                Row.of(Integer.MAX_VALUE, 2)
            );

        // WHEN: Applying arithmetic operations
        when()
            .select(
                expr("a + b").as("addition"),
                expr("a - b").as("subtraction"),
                expr("a * b").as("multiplication"),
                expr("a / b").as("division"),  // Integer division
                expr("a % b").as("modulo")
            )
            .execute();

        // THEN: Results should match Spark (Java semantics)
        then()
            .assertDataEquals()
            .assertIntegerDivisionTruncates() // Not floor division
            .assertModuloMatchesJavaSemantics()
            .assertOverflowWraps(); // Not error
    }

    @Test
    @DisplayName("Given comparison expressions, when translated, then null handling matches")
    public void testComparisonExpressions() {
        // GIVEN: DataFrame with nullable columns
        given()
            .sparkDataFrame()
            .withColumns(col("a", IntegerType, nullable=true))
            .withData(
                Row.of(10),
                Row.of(null),
                Row.of(20)
            );

        // WHEN: Applying comparison operations
        when()
            .select(
                expr("a > 15").as("gt"),
                expr("a IS NULL").as("is_null"),
                expr("a IS NOT NULL").as("is_not_null")
            )
            .execute();

        // THEN: Null handling should match three-valued logic
        then()
            .assertDataEquals()
            .assertNullPropagation()
            .assertThreeValuedLogic();
    }

    @Test
    @DisplayName("Given logical expressions, when translated, then short-circuit evaluation matches")
    public void testLogicalExpressions() {
        // GIVEN: DataFrame with boolean columns
        given()
            .sparkDataFrame()
            .withColumns(
                col("cond1", BooleanType),
                col("cond2", BooleanType)
            )
            .withData(
                Row.of(true, true),
                Row.of(true, false),
                Row.of(false, true),
                Row.of(false, false),
                Row.of(null, true),
                Row.of(true, null)
            );

        // WHEN: Applying logical operations
        when()
            .select(
                expr("cond1 AND cond2").as("and_result"),
                expr("cond1 OR cond2").as("or_result"),
                expr("NOT cond1").as("not_result")
            )
            .execute();

        // THEN: Null handling in boolean logic should match
        then()
            .assertDataEquals()
            .assertNullHandlingInAndOr();
    }
}
```

**3. Function Mapping Tests**

```java
@Nested
@DisplayName("Function Mapping Validation")
class FunctionMappingTests {

    @Test
    @DisplayName("Given string functions, when translated, then behavior matches")
    public void testStringFunctions() {
        // GIVEN: DataFrame with string data
        given()
            .sparkDataFrame()
            .withColumn("text", StringType)
            .withData(
                Row.of("  Hello World  "),
                Row.of("UPPERCASE"),
                Row.of("lowercase"),
                Row.of(null)
            );

        // WHEN: Applying string functions
        when()
            .select(
                upper(col("text")).as("upper"),
                lower(col("text")).as("lower"),
                trim(col("text")).as("trim"),
                substring(col("text"), 1, 5).as("substr"),
                length(col("text")).as("length")
            )
            .execute();

        // THEN: Results should match Spark
        then()
            .assertDataEquals()
            .assertNullPropagation();
    }

    @Test
    @DisplayName("Given date functions, when translated, then behavior matches")
    public void testDateFunctions() {
        // GIVEN: DataFrame with date/timestamp data
        given()
            .sparkDataFrame()
            .withColumn("dt", TimestampType)
            .withData(
                Row.of(Timestamp.valueOf("2024-01-15 10:30:45")),
                Row.of(Timestamp.valueOf("2024-02-29 23:59:59"))
            );

        // WHEN: Applying date functions
        when()
            .select(
                year(col("dt")).as("year"),
                month(col("dt")).as("month"),
                dayofmonth(col("dt")).as("day"),
                hour(col("dt")).as("hour"),
                date_add(col("dt"), 7).as("week_later")
            )
            .execute();

        // THEN: Results should match Spark
        then()
            .assertDataEquals()
            .assertDateArithmetic();
    }

    @Test
    @DisplayName("Given aggregate functions, when translated, then results match")
    public void testAggregateFunctions() {
        // GIVEN: DataFrame with numeric data
        given()
            .sparkDataFrame()
            .withColumns(
                col("category", StringType),
                col("value", DoubleType)
            )
            .withData(
                Row.of("A", 10.0),
                Row.of("A", 20.0),
                Row.of("B", 15.0),
                Row.of("B", null)
            );

        // WHEN: Applying aggregate functions
        when()
            .groupBy("category")
            .agg(
                sum(col("value")).as("sum"),
                avg(col("value")).as("avg"),
                min(col("value")).as("min"),
                max(col("value")).as("max"),
                count(col("value")).as("count"),
                stddev(col("value")).as("stddev")
            )
            .execute();

        // THEN: Results should match Spark
        then()
            .assertDataEquals()
            .assertNullHandlingInAggregates()
            .assertNumericPrecision(1e-10); // Tolerance for floating-point
    }

    @Test
    @DisplayName("Given window functions, when translated, then ordering and partitioning match")
    public void testWindowFunctions() {
        // GIVEN: DataFrame with time-series data
        given()
            .sparkDataFrame()
            .withColumns(
                col("id", IntegerType),
                col("category", StringType),
                col("value", IntegerType),
                col("timestamp", TimestampType)
            )
            .withSampleTimeSeriesData();

        // WHEN: Applying window functions
        Window windowSpec = Window.partitionBy("category").orderBy("timestamp");
        when()
            .select(
                col("*"),
                row_number().over(windowSpec).as("row_num"),
                rank().over(windowSpec).as("rank"),
                lag(col("value"), 1).over(windowSpec).as("prev_value"),
                lead(col("value"), 1).over(windowSpec).as("next_value")
            )
            .execute();

        // THEN: Results should match Spark
        then()
            .assertDataEquals()
            .assertWindowOrdering()
            .assertPartitionIsolation();
    }
}
```

**4. SQL Generation Tests**

```java
@Nested
@DisplayName("SQL Generation Correctness")
class SQLGenerationTests {

    @Test
    @DisplayName("Given simple select, when generating SQL, then syntax is valid")
    public void testSimpleSelect() {
        // GIVEN: Logical plan for simple select
        given()
            .logicalPlan()
            .tableScan("test.parquet")
            .project("id", "name");

        // WHEN: Generating SQL
        String sql = when()
            .generateSQL();

        // THEN: SQL should be valid DuckDB syntax
        then()
            .assertSQLValid(sql)
            .assertSQLMatches("SELECT id, name FROM read_parquet('test.parquet')");
    }

    @Test
    @DisplayName("Given complex query, when generating SQL, then subqueries are nested correctly")
    public void testComplexQuery() {
        // GIVEN: Logical plan with filter, aggregate, and join
        given()
            .logicalPlan()
            .tableScan("orders.parquet")
            .filter("order_date >= '2024-01-01'")
            .groupBy("customer_id")
            .agg(sum(col("amount")).as("total"))
            .join(
                tableScan("customers.parquet"),
                col("customer_id") === col("id")
            );

        // WHEN: Generating SQL
        String sql = when()
            .generateSQL();

        // THEN: SQL should have correct nesting
        then()
            .assertSQLValid(sql)
            .assertContainsSubquery()
            .assertJoinSyntaxCorrect();
    }
}
```

### Test Data Management

#### Strategy 1: In-Memory Test Data

```java
public class TestDataBuilder {

    public static DataFrame createSmallDataset() {
        return new TestDataFrameBuilder()
            .withSchema(
                StructField("id", IntegerType),
                StructField("name", StringType),
                StructField("age", IntegerType)
            )
            .withRows(
                Row.of(1, "Alice", 25),
                Row.of(2, "Bob", 30),
                Row.of(3, "Charlie", 35)
            )
            .build();
    }

    public static DataFrame createNullableDataset() {
        return new TestDataFrameBuilder()
            .withSchema(
                StructField("id", IntegerType, nullable=false),
                StructField("optional_value", IntegerType, nullable=true)
            )
            .withRows(
                Row.of(1, 100),
                Row.of(2, null),
                Row.of(3, 300)
            )
            .build();
    }

    public static DataFrame createEdgeCasesDataset() {
        return new TestDataFrameBuilder()
            .withSchema(
                StructField("int_col", IntegerType),
                StructField("float_col", FloatType),
                StructField("string_col", StringType)
            )
            .withRows(
                Row.of(Integer.MAX_VALUE, Float.MAX_VALUE, ""),
                Row.of(Integer.MIN_VALUE, Float.MIN_VALUE, null),
                Row.of(0, Float.NaN, "special chars: \n\t\"'"),
                Row.of(null, Float.POSITIVE_INFINITY, "unicode: \u00e9")
            )
            .build();
    }
}
```

#### Strategy 2: Generated Test Data

```java
public class TestDataGenerator {

    public static DataFrame generateRandomDataset(int rows) {
        Random random = new Random(42); // Fixed seed for reproducibility

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            data.add(Row.of(
                i,                                      // id
                "user_" + i,                           // name
                random.nextInt(80) + 18,               // age: 18-97
                random.nextDouble() * 1000,            // amount
                random.nextBoolean()                   // active
            ));
        }

        return new TestDataFrameBuilder()
            .withSchema(
                StructField("id", IntegerType),
                StructField("name", StringType),
                StructField("age", IntegerType),
                StructField("amount", DoubleType),
                StructField("active", BooleanType)
            )
            .withRows(data)
            .build();
    }
}
```

### Test Execution Framework

#### Parallel Test Execution

```java
@ExtendWith(ParallelTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ParallelBDDTests {

    private SparkSession sparkSession;
    private DuckDBSession duckDBSession;

    @BeforeAll
    public void setupSessions() {
        // Initialize Spark in local mode
        sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "false") // Deterministic plans
            .getOrCreate();

        // Initialize DuckDB session
        duckDBSession = DuckDBSession.builder()
            .inMemory()
            .enableHardwareOptimizations()
            .build();
    }

    @AfterAll
    public void tearDownSessions() {
        sparkSession.stop();
        duckDBSession.close();
    }

    // Tests run in parallel using JUnit 5 parallel execution
    @Test
    @Parallel
    public void testOperation1() { ... }

    @Test
    @Parallel
    public void testOperation2() { ... }
}
```

## Layer 2: Integration Tests

### End-to-End Transformation Chains

```java
@Nested
@DisplayName("End-to-End Transformation Chains")
class IntegrationTests {

    @Test
    @DisplayName("Given complex ETL pipeline, when executed, then results match Spark")
    public void testCompleteETLPipeline() {
        // GIVEN: Source data in Parquet
        given()
            .parquetFile("sales_data.parquet")
            .withSchema(salesSchema);

        // WHEN: Applying multi-step transformation
        DataFrame result = when()
            // Step 1: Load and filter
            .read().parquet("sales_data.parquet")
            .filter("sale_date >= '2024-01-01'")
            .filter("amount > 0")

            // Step 2: Join with dimension table
            .join(
                spark.read().parquet("products.parquet"),
                col("product_id") === col("id")
            )

            // Step 3: Aggregate
            .groupBy("category", "month(sale_date)")
            .agg(
                sum(col("amount")).as("total_sales"),
                count(col("*")).as("num_transactions"),
                avg(col("amount")).as("avg_transaction")
            )

            // Step 4: Window function
            .withColumn("rank",
                rank().over(Window.partitionBy("category").orderBy(desc("total_sales")))
            )

            // Step 5: Filter and sort
            .filter("rank <= 10")
            .orderBy("category", "rank")
            .execute();

        // THEN: Results should match Spark exactly
        then()
            .assertDataEquals(sparkResult, duckDBResult)
            .assertSchemaEquals()
            .assertRowCount()
            .assertExecutionTime(lessThan(sparkTime / 5)); // 5x faster
    }

    @Test
    @DisplayName("Given self-join pattern, when executed, then results match")
    public void testSelfJoinPattern() {
        // GIVEN: Time-series data
        given()
            .dataFrame("events")
            .withTimeSeriesData();

        // WHEN: Self-join for change detection
        DataFrame result = when()
            .alias("events", "e1")
            .join(
                spark.table("events").alias("e2"),
                col("e1.user_id") === col("e2.user_id")
                    && col("e1.timestamp") < col("e2.timestamp")
            )
            .select(
                col("e1.user_id"),
                col("e1.timestamp").as("from_time"),
                col("e2.timestamp").as("to_time"),
                datediff(col("e2.timestamp"), col("e1.timestamp")).as("days_diff")
            )
            .execute();

        // THEN: Results should match
        then()
            .assertDataEquals();
    }
}
```

### Format Reader Integration Tests

```java
@Nested
@DisplayName("Format Reader Integration")
class FormatReaderTests {

    @Test
    @DisplayName("Given Parquet file with nested schema, when reading, then structure preserved")
    public void testParquetNestedRead() {
        // GIVEN: Parquet file with complex nested schema
        given()
            .parquetFile("nested_data.parquet")
            .withNestedSchema();

        // WHEN: Reading through DuckDB
        DataFrame df = when()
            .read().parquet("nested_data.parquet")
            .execute();

        // THEN: Nested structure should be preserved
        then()
            .assertNestedFieldsAccessible()
            .assertArrayElementsAccessible()
            .assertMapKeysAccessible();
    }

    @Test
    @DisplayName("Given Delta Lake table with time travel, when reading version, then correct snapshot loaded")
    public void testDeltaLakeTimeTravel() {
        // GIVEN: Delta table with multiple versions
        given()
            .deltaTable("delta_table")
            .withVersions(0, 1, 2);

        // WHEN: Reading specific version
        DataFrame v1 = when()
            .read()
            .format("delta")
            .option("versionAsOf", "1")
            .load("delta_table")
            .execute();

        // THEN: Should match Spark's version 1
        then()
            .assertDataEquals(sparkV1, duckDBV1);
    }

    @Test
    @DisplayName("Given Iceberg table with partition pruning, when reading, then only relevant partitions scanned")
    public void testIcebergPartitionPruning() {
        // GIVEN: Iceberg table partitioned by date
        given()
            .icebergTable("iceberg_table")
            .partitionedBy("date");

        // WHEN: Reading with partition filter
        DataFrame df = when()
            .read()
            .format("iceberg")
            .load("iceberg_table")
            .filter("date = '2024-01-15'")
            .execute();

        // THEN: Should only scan relevant partition
        then()
            .assertPartitionPruningApplied()
            .assertDataEquals();
    }
}
```

## Layer 3: Performance Benchmarks

### TPC-H Benchmark Suite (SF=0.01)

```java
@Nested
@DisplayName("TPC-H Performance Benchmarks (SF=0.01)")
class TPCHBenchmarks {

    private static final double SCALE_FACTOR = 0.01;

    @BeforeAll
    public static void generateTPCHData() {
        TPCHDataGenerator.generate(SCALE_FACTOR, "tpch_data/");
    }

    @Test
    @DisplayName("Q1: Pricing Summary Report - Target 5x faster than Spark")
    public void testTPCH_Q1() {
        // Query: Scan + Aggregation
        String query = """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                AVG(l_quantity) as avg_qty,
                AVG(l_extendedprice) as avg_price,
                COUNT(*) as count_order
            FROM lineitem
            WHERE l_shipdate <= date '1998-12-01' - interval '90' day
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
            """;

        BenchmarkResult result = benchmark()
            .warmup(3)
            .iterations(10)
            .executeOnBoth(query);

        assertThat(result)
            .duckDBFasterThan(5.0, result.sparkMedianTime)
            .resultsMatch()
            .duckDBTimeUnder(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("Q3: Shipping Priority - Target 5x faster than Spark")
    public void testTPCH_Q3() {
        // Query: Join + Aggregation + Sort
        String query = """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) as revenue,
                o_orderdate,
                o_shippriority
            FROM customer, orders, lineitem
            WHERE c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND c_mktsegment = 'BUILDING'
                AND o_orderdate < date '1995-03-15'
                AND l_shipdate > date '1995-03-15'
            GROUP BY l_orderkey, o_orderdate, o_shippriority
            ORDER BY revenue DESC, o_orderdate
            LIMIT 10
            """;

        BenchmarkResult result = benchmark()
            .warmup(3)
            .iterations(10)
            .executeOnBoth(query);

        assertThat(result)
            .duckDBFasterThan(5.0, result.sparkMedianTime)
            .resultsMatch();
    }

    @Test
    @DisplayName("Q6: Forecast Revenue Change - Target 8x faster than Spark")
    public void testTPCH_Q6() {
        // Query: Highly selective filter
        String query = """
            SELECT
                SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE l_shipdate >= date '1994-01-01'
                AND l_shipdate < date '1994-01-01' + interval '1' year
                AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
                AND l_quantity < 24
            """;

        BenchmarkResult result = benchmark()
            .warmup(3)
            .iterations(10)
            .executeOnBoth(query);

        assertThat(result)
            .duckDBFasterThan(8.0, result.sparkMedianTime)
            .resultsMatch()
            .duckDBTimeUnder(Duration.ofMillis(200));
    }

    // ... All 22 TPC-H queries
}
```

### TPC-DS Benchmark Suite (SF=0.01)

```java
@Nested
@DisplayName("TPC-DS Performance Benchmarks (SF=0.01)")
class TPCDSBenchmarks {

    private static final double SCALE_FACTOR = 0.01;

    @BeforeAll
    public static void generateTPCDSData() {
        TPCDSDataGenerator.generate(SCALE_FACTOR, "tpcds_data/");
    }

    @Test
    @DisplayName("Q3: Complex multi-join query")
    public void testTPCDS_Q3() {
        // More complex query patterns than TPC-H
        BenchmarkResult result = benchmark()
            .warmup(3)
            .iterations(10)
            .executeTPCDSQuery(3);

        assertThat(result)
            .duckDBFasterThan(5.0, result.sparkMedianTime)
            .resultsMatch();
    }

    // ... Selected TPC-DS queries (99 total)
}
```

### Memory Profiling

```java
@Nested
@DisplayName("Memory Usage Analysis")
class MemoryProfilingTests {

    @Test
    @DisplayName("Given 1GB scan, when executing, then memory usage <500MB")
    public void testMemoryEfficiency_1GB() {
        // GIVEN: 1GB Parquet file
        given()
            .parquetFile("large_1gb.parquet");

        // WHEN: Scanning and counting
        MemoryProfile profile = when()
            .monitorMemory()
            .execute(() -> {
                long count = spark.read()
                    .parquet("large_1gb.parquet")
                    .count();
            });

        // THEN: Memory usage should be minimal
        assertThat(profile)
            .peakMemoryUsage(lessThan(500 * 1024 * 1024))  // <500MB
            .comparedToSpark(improvementFactor(8));         // 8x less
    }

    @Test
    @DisplayName("Given large join, when executing, then memory usage efficient")
    public void testMemoryEfficiency_Join() {
        // GIVEN: Two 500MB tables
        given()
            .parquetFiles("table1.parquet", "table2.parquet");

        // WHEN: Joining
        MemoryProfile profile = when()
            .monitorMemory()
            .execute(() -> {
                DataFrame result = spark.read()
                    .parquet("table1.parquet")
                    .join(
                        spark.read().parquet("table2.parquet"),
                        col("id") === col("id")
                    );
                result.count();
            });

        // THEN: Memory usage should be <2GB
        assertThat(profile)
            .peakMemoryUsage(lessThan(2L * 1024 * 1024 * 1024));
    }
}
```

## Layer 4: Comparative Analysis

### Embedded DuckDB vs External Spark Cluster

```java
@Nested
@DisplayName("Comparative Analysis: DuckDB vs Spark Cluster")
class ComparativeAnalysisTests {

    @Test
    @DisplayName("Given identical workload, when comparing costs, then DuckDB more cost-effective")
    public void testCostEffectiveness() {
        // GIVEN: Standard analytical workload
        Workload workload = standardAnalyticalWorkload();

        // WHEN: Running on both platforms
        CostAnalysis analysis = when()
            .runWorkload(workload)
            .onEmbeddedDuckDB(instanceType = "r8g.4xlarge")
            .onSparkCluster(clusterSize = "3 x m5.4xlarge")
            .measureCosts();

        // THEN: DuckDB should be more cost-effective
        assertThat(analysis)
            .duckDBCostSavings(greaterThan(60)) // >60% savings
            .performanceEquivalent()
            .reportSavingsBreakdown();
    }

    @Test
    @DisplayName("Given varying workload sizes, when analyzing performance crossover, then identify threshold")
    public void testPerformanceCrossoverPoint() {
        // GIVEN: Workloads of different sizes
        List<Workload> workloads = List.of(
            workloadWithData(1, "GB"),
            workloadWithData(10, "GB"),
            workloadWithData(100, "GB"),
            workloadWithData(500, "GB")
        );

        // WHEN: Running on both platforms
        CrossoverAnalysis analysis = when()
            .runWorkloads(workloads)
            .onBothPlatforms();

        // THEN: Identify crossover point
        assertThat(analysis)
            .crossoverPoint(approximately(200, "GB")) // DuckDB faster <200GB
            .reportRecommendations();
    }
}
```

## Command-Line Interface Design

### One-Liner Test Execution

```bash
# Layer 1: Run all BDD unit tests
./test.sh unit

# Layer 1: Run specific test category
./test.sh unit --category=type-mapping
./test.sh unit --category=expressions
./test.sh unit --category=functions

# Layer 2: Run integration tests
./test.sh integration

# Layer 2: Run format-specific tests
./test.sh integration --format=parquet
./test.sh integration --format=delta
./test.sh integration --format=iceberg

# Layer 3: Run performance benchmarks
./test.sh benchmark --suite=tpch
./test.sh benchmark --suite=tpcds
./test.sh benchmark --suite=all

# Layer 3: Run specific benchmark query
./test.sh benchmark --suite=tpch --query=1
./test.sh benchmark --suite=tpch --queries=1,3,6

# Layer 4: Run comparative analysis
./test.sh compare --workload=standard
./test.sh compare --workload=custom --config=workload.yaml

# Run all tests
./test.sh all

# Run with coverage report
./test.sh all --coverage

# Run in CI mode (fail fast, detailed logs)
./test.sh all --ci

# Run with profiling
./test.sh benchmark --suite=tpch --profile

# Generate performance report
./test.sh report --format=html --output=test_report.html
```

### Test Runner Implementation

```bash
#!/bin/bash
# test.sh - Unified test runner

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse command-line arguments
COMMAND=${1:-all}
shift

case "$COMMAND" in
    unit)
        echo "Running BDD unit tests..."
        mvn test -Dtest="*BDDTest" "$@"
        ;;

    integration)
        echo "Running integration tests..."
        mvn test -Dtest="*IntegrationTest" "$@"
        ;;

    benchmark)
        echo "Running performance benchmarks..."
        mvn test -Dtest="*BenchmarkTest" "$@"
        ;;

    compare)
        echo "Running comparative analysis..."
        mvn test -Dtest="*ComparativeTest" "$@"
        ;;

    all)
        echo "Running all tests..."
        mvn clean test "$@"
        ;;

    report)
        echo "Generating test report..."
        mvn surefire-report:report "$@"
        ;;

    *)
        echo "Usage: $0 {unit|integration|benchmark|compare|all|report} [options]"
        exit 1
        ;;
esac
```

## Success Criteria and Metrics

### Correctness Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Differential test pass rate | >95% | Unit tests vs Spark |
| Numerical consistency | 100% | Within floating-point tolerance |
| Type mapping accuracy | 100% | All Spark types supported |
| Function coverage | >90% | Common Spark functions |

### Performance Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| TPC-H speedup (SF=0.01) | 5-10x | Median query time |
| TPC-DS speedup (SF=0.01) | 5-10x | Median query time |
| Memory efficiency | 6-8x | Peak memory usage |
| Overhead vs native DuckDB | 10-20% | Additional latency |

### Maintainability Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Test execution time | <5 min | Full suite runtime |
| Code coverage | >80% | JaCoCo report |
| Test flakiness rate | <1% | Failed reruns |

## Test Infrastructure Requirements

### Hardware Requirements

**Development Environment:**
- CPU: 4+ cores
- RAM: 16GB minimum
- Storage: 50GB for test data

**CI/CD Environment:**
- CPU: 8+ cores
- RAM: 32GB minimum
- Storage: 100GB for test data + artifacts

### Software Dependencies

```xml
<dependencies>
    <!-- Testing frameworks -->
    <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>5.10.0</version>
        <scope>test</scope>
    </dependency>

    <!-- BDD support -->
    <dependency>
        <groupId>io.cucumber</groupId>
        <artifactId>cucumber-java</artifactId>
        <version>7.14.0</version>
        <scope>test</scope>
    </dependency>

    <!-- Benchmarking -->
    <dependency>
        <groupId>org.openjdk.jmh</groupId>
        <artifactId>jmh-core</artifactId>
        <version>1.37</version>
        <scope>test</scope>
    </dependency>

    <!-- Spark (for differential testing) -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>3.5.3</version>
        <scope>test</scope>
    </dependency>

    <!-- Assertions -->
    <dependency>
        <groupId>org.assertj</groupId>
        <artifactId>assertj-core</artifactId>
        <version>3.24.2</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### CI/CD Pipeline Configuration

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
      - name: Run unit tests
        run: ./test.sh unit --ci
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
      - name: Run integration tests
        run: ./test.sh integration --ci

  performance-benchmarks:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'
      - name: Run TPC-H benchmarks
        run: ./test.sh benchmark --suite=tpch
      - name: Upload benchmark results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: target/benchmark-results/
```

## Implementation Roadmap

### Phase 1: Foundation (Week 1)
- [ ] Set up BDD test framework
- [ ] Implement test data builders
- [ ] Create differential testing infrastructure
- [ ] Write 50+ type mapping tests
- [ ] Write 100+ expression translation tests

### Phase 2: Coverage (Week 2)
- [ ] Write 200+ function mapping tests
- [ ] Write 50+ SQL generation tests
- [ ] Implement integration test framework
- [ ] Write 30+ end-to-end pipeline tests

### Phase 3: Performance (Week 3)
- [ ] Generate TPC-H test data (SF=0.01)
- [ ] Implement all 22 TPC-H queries
- [ ] Generate TPC-DS test data (SF=0.01)
- [ ] Implement selected TPC-DS queries
- [ ] Set up memory profiling

### Phase 4: Analysis (Week 4)
- [ ] Implement comparative analysis framework
- [ ] Run cost-effectiveness studies
- [ ] Identify performance crossover points
- [ ] Generate comprehensive test reports

## Conclusion

This multi-layer testing strategy provides:

1. **Correctness Confidence**: Differential testing against Spark ensures numerical consistency
2. **Performance Validation**: TPC-H and TPC-DS benchmarks verify speedup targets
3. **Fast Feedback**: <5 minute test suite enables rapid iteration
4. **Clear Success Metrics**: Quantitative targets for all quality dimensions
5. **Maintainable Infrastructure**: BDD structure and one-liner commands

The strategy balances thoroughness with practicality, focusing test effort on high-value areas while maintaining fast execution times.
