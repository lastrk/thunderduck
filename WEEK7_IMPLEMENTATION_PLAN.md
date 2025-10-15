# Week 7: Spark Differential Testing Framework - Implementation Plan

**Project**: catalyst2sql
**Week**: 7 (Phase 3: Correctness & Production Readiness)
**Date**: October 15, 2025
**Goal**: Build comprehensive differential testing framework with Spark 3.5.3 as reference oracle

---

## Executive Summary

Week 7 focuses on establishing a robust differential testing framework that compares catalyst2sql execution results against Apache Spark 3.5.3 local mode. This framework will serve as the foundation for ensuring 100% correctness parity with Spark across all supported operations.

**Deliverables**:
- Automated differential testing harness
- Spark 3.5.3 local mode integration
- Schema validation framework
- Data comparison utilities (with numerical epsilon handling)
- Test data generation utilities
- 50+ differential test cases covering basic operations
- Comprehensive divergence report

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Implementation Tasks](#implementation-tasks)
3. [Test Categories](#test-categories)
4. [Success Criteria](#success-criteria)
5. [Deliverables Checklist](#deliverables-checklist)

---

## Architecture Overview

### Component Hierarchy

```
DifferentialTestHarness (abstract base class)
├── Schema Validation
│   ├── SchemaValidator
│   └── TypeComparator
├── Data Validation
│   ├── DataFrameComparator
│   ├── RowComparator
│   └── NumericalValidator (epsilon-based)
├── Test Data Generation
│   ├── SyntheticDataGenerator
│   ├── EdgeCaseGenerator
│   └── RealWorldPatternGenerator
└── Execution & Reporting
    ├── SparkExecutor (Spark 3.5.3 local mode)
    ├── Catalyst2SqlExecutor (DuckDB backend)
    └── DivergenceReporter

```

### Data Flow

```
1. Test Case Definition
   ↓
2. Generate Test Data (Parquet)
   ↓
3. Execute on Spark 3.5.3 → Result A
   ↓
4. Execute on catalyst2sql → Result B
   ↓
5. Compare Schema (A vs B)
   ↓
6. Compare Data (A vs B)
   ↓
7. Report Divergences
```

---

## Implementation Tasks

### Task 7.1: Set Up Spark 3.5.3 Dependency (30 min)

**Objective**: Add Apache Spark 3.5.3 to the tests module with proper scope and exclusions.

**Files to Modify**:
- `tests/pom.xml`

**Implementation**:
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.13</artifactId>
    <version>3.5.3</version>
    <scope>test</scope>
    <exclusions>
        <!-- Exclude conflicting dependencies -->
        <exclusion>
            <groupId>org.apache.arrow</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-catalyst_2.13</artifactId>
    <version>3.5.3</version>
    <scope>test</scope>
</dependency>
```

**Success Criteria**:
- ✅ Spark 3.5.3 dependency resolves successfully
- ✅ No dependency conflicts with existing DuckDB/Arrow dependencies
- ✅ Maven build succeeds with new dependencies

---

### Task 7.2: Implement DifferentialTestHarness Base Class (2 hours)

**Objective**: Create abstract base class for all differential tests with common setup/teardown.

**File**: `tests/src/test/java/com/catalyst2sql/differential/DifferentialTestHarness.java`

**Class Structure**:
```java
public abstract class DifferentialTestHarness extends TestBase {
    protected SparkSession sparkSession;
    protected Catalyst2SqlSession catalyst2sqlSession;
    protected SchemaValidator schemaValidator;
    protected DataFrameComparator dataComparator;
    protected DivergenceCollector divergenceCollector;

    @BeforeEach
    void setUpDifferentialTest() {
        // Initialize Spark 3.5.3 local mode
        sparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("DifferentialTest")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate();

        // Initialize catalyst2sql session
        catalyst2sqlSession = new Catalyst2SqlSession();

        // Initialize validators
        schemaValidator = new SchemaValidator();
        dataComparator = new DataFrameComparator();
        divergenceCollector = new DivergenceCollector();
    }

    @AfterEach
    void tearDownDifferentialTest() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
        if (catalyst2sqlSession != null) {
            catalyst2sqlSession.close();
        }
    }

    protected ComparisonResult executeAndCompare(
        String testName,
        Dataset<Row> sparkDF,
        Dataset<Row> catalyst2sqlDF
    ) {
        // 1. Validate schemas
        SchemaValidationResult schemaResult =
            schemaValidator.compare(sparkDF.schema(), catalyst2sqlDF.schema());

        // 2. Validate data
        DataValidationResult dataResult =
            dataComparator.compare(sparkDF, catalyst2sqlDF);

        // 3. Collect divergences
        ComparisonResult result = new ComparisonResult(testName, schemaResult, dataResult);
        divergenceCollector.record(result);

        return result;
    }
}
```

**Success Criteria**:
- ✅ Base class provides clean API for differential testing
- ✅ Proper resource cleanup in teardown
- ✅ Divergence collection mechanism in place

---

### Task 7.3: Implement Schema Validation Framework (2 hours)

**Objective**: Create utilities to compare Spark and catalyst2sql schemas field-by-field.

**Files**:
- `tests/src/test/java/com/catalyst2sql/differential/validation/SchemaValidator.java`
- `tests/src/test/java/com/catalyst2sql/differential/validation/SchemaValidationResult.java`
- `tests/src/test/java/com/catalyst2sql/differential/validation/TypeComparator.java`

**Key Features**:
```java
public class SchemaValidator {
    public SchemaValidationResult compare(StructType sparkSchema, StructType catalyst2sqlSchema) {
        List<SchemaDivergence> divergences = new ArrayList<>();

        // 1. Compare field count
        if (sparkSchema.fields().length != catalyst2sqlSchema.fields().length) {
            divergences.add(new FieldCountDivergence(...));
        }

        // 2. Compare field names
        // 3. Compare field types
        // 4. Compare nullability

        return new SchemaValidationResult(divergences);
    }
}

public class TypeComparator {
    public boolean areTypesCompatible(DataType sparkType, DataType catalyst2sqlType) {
        // Handle type mapping differences
        // E.g., Spark IntegerType → DuckDB INTEGER
    }
}
```

**Test Coverage**:
- Column count mismatch detection
- Column name mismatch detection
- Type incompatibility detection
- Nullability mismatch detection

**Success Criteria**:
- ✅ Accurate schema comparison for all primitive types
- ✅ Clear divergence reporting
- ✅ 15+ unit tests for schema validation

---

### Task 7.4: Implement Data Comparison Utilities (3 hours)

**Objective**: Build robust data comparison with numerical epsilon handling and null support.

**Files**:
- `tests/src/test/java/com/catalyst2sql/differential/validation/DataFrameComparator.java`
- `tests/src/test/java/com/catalyst2sql/differential/validation/RowComparator.java`
- `tests/src/test/java/com/catalyst2sql/differential/validation/NumericalValidator.java`
- `tests/src/test/java/com/catalyst2sql/differential/validation/DataValidationResult.java`

**Key Features**:
```java
public class DataFrameComparator {
    private static final double EPSILON = 1e-10;

    public DataValidationResult compare(Dataset<Row> sparkDF, Dataset<Row> catalyst2sqlDF) {
        List<DataDivergence> divergences = new ArrayList<>();

        // 1. Compare row counts
        long sparkCount = sparkDF.count();
        long catalyst2sqlCount = catalyst2sqlDF.count();
        if (sparkCount != catalyst2sqlCount) {
            divergences.add(new RowCountDivergence(sparkCount, catalyst2sqlCount));
        }

        // 2. Sort both DataFrames deterministically
        Dataset<Row> sparkSorted = sortDeterministically(sparkDF);
        Dataset<Row> catalyst2sqlSorted = sortDeterministically(catalyst2sqlDF);

        // 3. Compare row-by-row
        List<Row> sparkRows = sparkSorted.collectAsList();
        List<Row> catalyst2sqlRows = catalyst2sqlSorted.collectAsList();

        for (int i = 0; i < Math.min(sparkRows.size(), catalyst2sqlRows.size()); i++) {
            RowComparisonResult rowResult = rowComparator.compare(
                sparkRows.get(i),
                catalyst2sqlRows.get(i),
                sparkDF.schema()
            );

            if (!rowResult.isEqual()) {
                divergences.add(new RowDataDivergence(i, rowResult));
            }
        }

        return new DataValidationResult(divergences);
    }
}

public class NumericalValidator {
    public boolean areNumericallyEqual(Object sparkValue, Object catalyst2sqlValue, DataType type) {
        if (type instanceof IntegerType || type instanceof LongType) {
            return Objects.equals(sparkValue, catalyst2sqlValue);
        }

        if (type instanceof DoubleType || type instanceof FloatType) {
            double sparkDouble = ((Number) sparkValue).doubleValue();
            double catalyst2sqlDouble = ((Number) catalyst2sqlValue).doubleValue();

            // Handle special values
            if (Double.isNaN(sparkDouble) && Double.isNaN(catalyst2sqlDouble)) return true;
            if (Double.isInfinite(sparkDouble) && Double.isInfinite(catalyst2sqlDouble)) {
                return Double.compare(sparkDouble, catalyst2sqlDouble) == 0;
            }

            // Epsilon-based comparison
            return Math.abs(sparkDouble - catalyst2sqlDouble) < EPSILON;
        }

        return false;
    }
}
```

**Test Coverage**:
- Row count comparison
- Null value handling
- Integer exact match
- Floating point epsilon comparison
- NaN and Infinity handling
- Deterministic sorting

**Success Criteria**:
- ✅ Accurate data comparison for all data types
- ✅ Proper handling of nulls, NaN, Infinity
- ✅ Clear divergence reporting with row/column positions
- ✅ 20+ unit tests for data validation

---

### Task 7.5: Create Test Data Generation Utilities (2 hours)

**Objective**: Build utilities to generate synthetic test data with various patterns.

**Files**:
- `tests/src/test/java/com/catalyst2sql/differential/datagen/SyntheticDataGenerator.java`
- `tests/src/test/java/com/catalyst2sql/differential/datagen/EdgeCaseGenerator.java`
- `tests/src/test/java/com/catalyst2sql/differential/datagen/TestDataSpec.java`

**Key Features**:
```java
public class SyntheticDataGenerator {
    public Dataset<Row> generate(SparkSession spark, TestDataSpec spec) {
        // Generate data based on spec
        // - Row count
        // - Schema
        // - Value ranges
        // - Null percentage
        // - Distribution type (uniform, gaussian, zipf)
    }
}

public class EdgeCaseGenerator {
    public static List<Dataset<Row>> generateEdgeCases(SparkSession spark) {
        return Arrays.asList(
            emptyDataFrame(spark),
            singleRowDataFrame(spark),
            allNullsDataFrame(spark),
            maxIntDataFrame(spark),
            minIntDataFrame(spark),
            nanInfinityDataFrame(spark)
        );
    }
}

public class TestDataSpec {
    private int rowCount;
    private StructType schema;
    private Map<String, ValueGenerator> columnGenerators;
    private double nullPercentage;

    public static TestDataSpec simple(int rows) {
        return new TestDataSpec()
            .withRowCount(rows)
            .withColumn("id", IntegerType, sequential())
            .withColumn("name", StringType, randomString())
            .withColumn("value", DoubleType, randomDouble(0, 100));
    }
}
```

**Test Data Categories**:
- Simple tables (10-1000 rows)
- Edge cases (empty, single row, all nulls)
- Extreme values (MAX_INT, MIN_INT, NaN, Infinity)
- Real-world patterns (zipf distribution, correlated columns)

**Success Criteria**:
- ✅ Generate test data programmatically
- ✅ Support all primitive types
- ✅ Support null injection
- ✅ Deterministic generation (seeded random)

---

### Task 7.6: Write 50+ Differential Test Cases (4 hours)

**Objective**: Implement comprehensive test cases covering basic SQL operations.

**Test Categories** (50+ tests total):

#### Category 1: Basic SELECT Operations (10 tests)
- Simple SELECT *
- SELECT with column subset
- SELECT with expressions (arithmetic, string concat)
- SELECT with aliases
- SELECT with DISTINCT
- SELECT with LIMIT
- SELECT with ORDER BY (single column)
- SELECT with ORDER BY (multiple columns)
- SELECT with NULL values
- SELECT with extreme values

#### Category 2: Filter Operations (10 tests)
- WHERE with equality
- WHERE with inequality (<, >, <=, >=)
- WHERE with AND/OR
- WHERE with NOT
- WHERE with IN clause
- WHERE with NULL (IS NULL, IS NOT NULL)
- WHERE with LIKE
- WHERE with BETWEEN
- WHERE with complex predicates
- WHERE with nested conditions

#### Category 3: Aggregate Operations (10 tests)
- COUNT(*)
- COUNT(column)
- COUNT(DISTINCT column)
- SUM, AVG, MIN, MAX
- GROUP BY single column
- GROUP BY multiple columns
- GROUP BY with HAVING
- GROUP BY with NULL values
- Empty group aggregation
- Single row aggregation

#### Category 4: Join Operations (10 tests)
- INNER JOIN
- LEFT OUTER JOIN
- RIGHT OUTER JOIN
- FULL OUTER JOIN
- CROSS JOIN
- Join with NULL keys
- Join with multiple conditions
- Join with complex predicates
- Self-join
- Multi-table join

#### Category 5: Data Type Handling (10 tests)
- Integer types (INT, LONG)
- Floating point types (FLOAT, DOUBLE)
- String types
- Boolean types
- Null handling across types
- Type casting
- Decimal precision
- NaN and Infinity
- MIN_VALUE and MAX_VALUE
- Mixed types in expressions

**Example Test Case**:
```java
@Test
@DisplayName("Differential: Simple SELECT with WHERE clause")
void testSimpleSelectWithWhere() {
    // Given: Test data
    Dataset<Row> testData = generateTestData(100);
    String testDataPath = writeToParquet(testData);

    // When: Execute on both engines
    Dataset<Row> sparkResult = sparkSession.read()
        .parquet(testDataPath)
        .filter("value > 50")
        .select("id", "name", "value");

    Dataset<Row> catalyst2sqlResult = catalyst2sqlSession.read()
        .parquet(testDataPath)
        .filter(col("value").gt(50))
        .select("id", "name", "value");

    // Then: Compare results
    ComparisonResult result = executeAndCompare(
        "SimpleSelectWithWhere",
        sparkResult,
        catalyst2sqlResult
    );

    // Assert no divergences
    assertThat(result.hasDivergences()).isFalse();
}
```

**Success Criteria**:
- ✅ 50+ test cases implemented
- ✅ All basic SQL operations covered
- ✅ Tests execute successfully on both engines
- ✅ Clear divergence reporting when tests fail

---

### Task 7.7: Execute Tests and Collect Divergences (1 hour)

**Objective**: Run all differential tests and collect comprehensive divergence data.

**Process**:
1. Execute all 50+ tests
2. Collect divergences from failing tests
3. Categorize divergences by type
4. Analyze patterns in divergences

**Divergence Categories**:
- Schema divergences (type mismatches, nullability)
- Data divergences (incorrect results)
- Semantic divergences (different interpretation of SQL)
- Numerical divergences (precision issues)
- Null handling divergences

**Success Criteria**:
- ✅ All tests executed
- ✅ Divergences categorized
- ✅ Patterns identified

---

### Task 7.8: Generate Comprehensive Divergence Report (1 hour)

**Objective**: Create detailed report documenting all divergences found.

**Report Structure**:
```markdown
# Week 7: Differential Testing Divergence Report

## Executive Summary
- Total tests: 50+
- Passing tests: X
- Failing tests: Y
- Divergence categories: Z

## Divergence Categories

### Category 1: Schema Divergences
- Count: X
- Examples: ...

### Category 2: Data Divergences
- Count: X
- Examples: ...

## Detailed Divergence Listing

### Divergence 1: [Title]
- Test: [Test name]
- Category: [Category]
- Description: [What diverges]
- Spark Result: [...]
- catalyst2sql Result: [...]
- Root Cause: [Analysis]
- Severity: HIGH/MEDIUM/LOW
- Action: [Fix needed / Expected behavior]

## Recommendations
1. ...
2. ...
```

**Success Criteria**:
- ✅ Comprehensive report generated
- ✅ All divergences documented
- ✅ Root cause analysis included
- ✅ Actionable recommendations provided

---

## Test Categories Summary

| Category | Test Count | Focus Area |
|----------|-----------|------------|
| Basic SELECT | 10 | Column selection, projection, sorting |
| Filter Operations | 10 | WHERE clause, predicates, null handling |
| Aggregate Operations | 10 | GROUP BY, HAVING, aggregate functions |
| Join Operations | 10 | All join types, null keys, complex conditions |
| Data Type Handling | 10 | Type compatibility, casting, special values |
| **Total** | **50** | **Comprehensive basic SQL coverage** |

---

## Success Criteria

### Functional Criteria
- ✅ Spark 3.5.3 dependency integrated successfully
- ✅ Differential test harness implemented and tested
- ✅ Schema validation framework functional
- ✅ Data comparison utilities handle all data types
- ✅ Test data generation utilities working
- ✅ 50+ differential test cases implemented
- ✅ All tests execute without errors

### Quality Criteria
- ✅ Code coverage ≥ 85% for new test framework code
- ✅ Zero compilation errors
- ✅ Clear, maintainable code structure
- ✅ Comprehensive JavaDoc documentation

### Deliverable Criteria
- ✅ Divergence report generated
- ✅ All divergences categorized and analyzed
- ✅ Root cause analysis completed
- ✅ Recommendations documented

---

## Deliverables Checklist

### Code Deliverables
- [ ] `DifferentialTestHarness.java` - Base class for differential tests
- [ ] `SchemaValidator.java` - Schema comparison utility
- [ ] `DataFrameComparator.java` - Data comparison utility
- [ ] `RowComparator.java` - Row-level comparison
- [ ] `NumericalValidator.java` - Numerical comparison with epsilon
- [ ] `SyntheticDataGenerator.java` - Test data generation
- [ ] `EdgeCaseGenerator.java` - Edge case data generation
- [ ] `DivergenceCollector.java` - Divergence tracking
- [ ] `DivergenceReporter.java` - Report generation
- [ ] 50+ test cases in various test classes

### Documentation Deliverables
- [ ] `WEEK7_COMPLETION_REPORT.md` - Week 7 completion status
- [ ] `DIFFERENTIAL_TESTING_DIVERGENCE_REPORT.md` - Detailed divergence analysis
- [ ] JavaDoc for all public APIs

### Test Deliverables
- [ ] 10+ Basic SELECT tests
- [ ] 10+ Filter operation tests
- [ ] 10+ Aggregate operation tests
- [ ] 10+ Join operation tests
- [ ] 10+ Data type handling tests

---

## Timeline Estimate

| Task | Estimated Time | Priority |
|------|---------------|----------|
| 7.1: Spark dependency setup | 30 min | P0 |
| 7.2: Test harness base class | 2 hours | P0 |
| 7.3: Schema validation | 2 hours | P0 |
| 7.4: Data comparison | 3 hours | P0 |
| 7.5: Test data generation | 2 hours | P1 |
| 7.6: Write 50+ tests | 4 hours | P0 |
| 7.7: Execute and collect | 1 hour | P0 |
| 7.8: Generate report | 1 hour | P0 |
| **Total** | **~16 hours** | **Critical Path** |

---

## Risk Mitigation

### Risk 1: Spark/DuckDB Dependency Conflicts
**Mitigation**: Use dependency exclusions, test in isolated classloader if needed

### Risk 2: Large Data Comparison Performance
**Mitigation**: Limit test data size to 1000 rows, use sampling for large datasets

### Risk 3: Flaky Tests Due to Non-Determinism
**Mitigation**: Ensure deterministic sorting, use seeded random generation

### Risk 4: Too Many Divergences
**Mitigation**: Focus on documenting, not fixing in Week 7; fixes come in Week 8+

---

## Next Steps (Week 8)

After Week 7 completion:
1. Analyze divergence patterns
2. Prioritize critical divergences for fixing
3. Expand to 200+ tests covering edge cases
4. Implement fixes for discovered divergences

---

**Document Version**: 1.0
**Status**: Ready for Implementation
**Approval**: Approved

---

Generated with [Claude Code](https://claude.com/claude-code)
