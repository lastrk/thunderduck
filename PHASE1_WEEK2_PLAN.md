# PHASE 1 - WEEK 2: SQL GENERATION & DUCKDB EXECUTION
**Milestone-Focused Implementation & Test Plan**

---

## 🎯 WEEK 2 OBJECTIVES

**Primary Goal**: Implement SQL generation, DuckDB execution engine, and Parquet I/O

**Success Criteria**:
- ✅ SQL generation working for 5 core operators (Project, Filter, TableScan, Sort, Limit)
- ✅ DuckDB connection manager with hardware detection operational
- ✅ Arrow data interchange layer functional
- ✅ Parquet reader supporting files, globs, and partitions
- ✅ Parquet writer with compression options
- ✅ 150+ tests passing (179 existing + 100+ new expression translation tests)
- ✅ Can read/write Parquet files end-to-end
- ✅ Performance benchmarks showing 5-10x improvement over Spark

**Deliverables**:
1. `SQLGenerator.java` - Core SQL generation engine
2. `DuckDBConnectionManager.java` - Connection pooling with hardware detection
3. `ArrowInterchange.java` - Zero-copy Arrow data paths
4. `ParquetReader.java` - Multi-format Parquet reader
5. `ParquetWriter.java` - Parquet writer with compression
6. 100+ new expression translation tests
7. Integration tests for end-to-end workflows
8. Performance benchmarks

---

## 📋 PHASE BREAKDOWN

### **PHASE 2A: SQL GENERATION CORE** (Days 1-2)
**Goal**: Implement SQL generation for 5 core LogicalPlan operators

#### Tasks

##### Task 2A.1: Create SQLGenerator Core Infrastructure
**Priority**: CRITICAL
**Estimated Time**: 4 hours
**File**: `core/src/main/java/com/catalyst2sql/generator/SQLGenerator.java`

**Implementation Details**:
```java
package com.catalyst2sql.generator;

import com.catalyst2sql.logical.*;
import com.catalyst2sql.expression.*;
import java.util.*;

/**
 * SQL generator that converts Spark logical plans to DuckDB SQL.
 *
 * Implements visitor pattern for traversing logical plan trees and
 * generating optimized DuckDB SQL with proper quoting, escaping, and
 * operator precedence handling.
 */
public class SQLGenerator {

    private final StringBuilder sql;
    private final Stack<GenerationContext> contextStack;
    private int aliasCounter;

    public SQLGenerator() {
        this.sql = new StringBuilder();
        this.contextStack = new Stack<>();
        this.aliasCounter = 0;
    }

    /**
     * Generates SQL for a logical plan node.
     */
    public String generate(LogicalPlan plan) {
        sql.setLength(0); // Reset
        visit(plan);
        return sql.toString();
    }

    /**
     * Visitor dispatch method.
     */
    private void visit(LogicalPlan plan) {
        if (plan instanceof Project) {
            visitProject((Project) plan);
        } else if (plan instanceof Filter) {
            visitFilter((Filter) plan);
        } else if (plan instanceof TableScan) {
            visitTableScan((TableScan) plan);
        } else if (plan instanceof Sort) {
            visitSort((Sort) plan);
        } else if (plan instanceof Limit) {
            visitLimit((Limit) plan);
        } else if (plan instanceof Aggregate) {
            visitAggregate((Aggregate) plan);
        } else if (plan instanceof Join) {
            visitJoin((Join) plan);
        } else {
            throw new UnsupportedOperationException(
                "SQL generation not implemented for: " + plan.getClass().getSimpleName());
        }
    }

    // Visitor methods implemented below...
}
```

**Key Features**:
- Visitor pattern for type-safe plan traversal
- Context stack for nested query generation
- Automatic alias generation for subqueries
- Proper identifier quoting (backticks for DuckDB)
- Expression precedence handling

**Test Coverage**:
- Unit tests for each operator type
- Nested query generation tests
- Complex expression precedence tests
- Edge cases (empty projections, null filters, etc.)

##### Task 2A.2: Implement TableScan SQL Generation
**Priority**: CRITICAL
**Estimated Time**: 2 hours
**File**: `core/src/main/java/com/catalyst2sql/logical/TableScan.java`

**Implementation**:
```java
@Override
public String toSQL(SQLGenerator generator) {
    // For table scans, generate: FROM table_name or FROM 'file_path'
    if (tableName.endsWith(".parquet")) {
        // Parquet file scan
        return String.format("FROM '%s'", tableName);
    } else {
        // Regular table scan
        return String.format("FROM %s", quoteIdentifier(tableName));
    }
}

private String quoteIdentifier(String identifier) {
    // DuckDB uses double quotes for identifiers
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
}
```

**Test Cases**:
- Regular table scans
- Parquet file scans
- Tables with special characters in names
- Tables with schema qualifiers (schema.table)

##### Task 2A.3: Implement Project SQL Generation
**Priority**: CRITICAL
**Estimated Time**: 3 hours
**File**: `core/src/main/java/com/catalyst2sql/logical/Project.java`

**Implementation**:
```java
@Override
public String toSQL(SQLGenerator generator) {
    StringBuilder sql = new StringBuilder("SELECT ");

    // Generate projection list
    List<String> projections = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
        Expression expr = expressions.get(i);
        String exprSQL = expr.toSQL();

        // Add alias if provided
        if (i < aliases.size() && aliases.get(i) != null) {
            exprSQL += " AS " + quoteIdentifier(aliases.get(i));
        }

        projections.add(exprSQL);
    }

    sql.append(String.join(", ", projections));

    // Add FROM clause from child
    sql.append(" ");
    sql.append(child().toSQL(generator));

    return sql.toString();
}
```

**Test Cases**:
- Simple column projections
- Expression projections (a + b)
- Function call projections (upper(name))
- Aliased projections
- SELECT * (all columns)

##### Task 2A.4: Implement Filter SQL Generation
**Priority**: CRITICAL
**Estimated Time**: 2 hours
**File**: `core/src/main/java/com/catalyst2sql/logical/Filter.java`

**Implementation**:
```java
@Override
public String toSQL(SQLGenerator generator) {
    String childSQL = child().toSQL(generator);
    String conditionSQL = condition.toSQL();

    return String.format("SELECT * FROM (%s) WHERE %s",
                        childSQL, conditionSQL);
}
```

**Test Cases**:
- Simple comparisons (age > 18)
- Complex conditions with AND/OR
- NULL checks (IS NULL, IS NOT NULL)
- Function calls in filters (upper(name) = 'JOHN')
- Nested filters

##### Task 2A.5: Implement Sort SQL Generation
**Priority**: HIGH
**Estimated Time**: 2 hours
**File**: `core/src/main/java/com/catalyst2sql/logical/Sort.java`

**Implementation**:
```java
@Override
public String toSQL(SQLGenerator generator) {
    String childSQL = child().toSQL(generator);

    List<String> orderClauses = new ArrayList<>();
    for (int i = 0; i < sortOrders.size(); i++) {
        Expression expr = sortOrders.get(i);
        boolean ascending = i < ascendingOrder.size() ? ascendingOrder.get(i) : true;

        String orderClause = expr.toSQL();
        orderClause += ascending ? " ASC" : " DESC";

        orderClauses.add(orderClause);
    }

    return String.format("SELECT * FROM (%s) ORDER BY %s",
                        childSQL, String.join(", ", orderClauses));
}
```

**Test Cases**:
- Single column sort (ORDER BY age)
- Multi-column sort (ORDER BY last_name, first_name)
- DESC sorting
- Expression-based sorting (ORDER BY amount * price)

##### Task 2A.6: Implement Limit SQL Generation
**Priority**: HIGH
**Estimated Time**: 1 hour
**File**: `core/src/main/java/com/catalyst2sql/logical/Limit.java`

**Implementation**:
```java
@Override
public String toSQL(SQLGenerator generator) {
    String childSQL = child().toSQL(generator);
    return String.format("SELECT * FROM (%s) LIMIT %d", childSQL, limit);
}
```

**Test Cases**:
- Simple LIMIT 10
- LIMIT 0 (empty result)
- LIMIT with large numbers
- LIMIT combined with ORDER BY

---

### **PHASE 2B: DUCKDB RUNTIME & CONNECTION MANAGEMENT** (Days 3-4)
**Goal**: Implement DuckDB connection manager with hardware detection and query execution

#### Tasks

##### Task 2B.1: Create DuckDBConnectionManager
**Priority**: CRITICAL
**Estimated Time**: 4 hours
**File**: `core/src/main/java/com/catalyst2sql/runtime/DuckDBConnectionManager.java`

**Implementation Details**:
```java
package com.catalyst2sql.runtime;

import org.duckdb.DuckDBConnection;
import java.sql.*;
import java.util.concurrent.*;

/**
 * Connection manager for DuckDB with connection pooling and
 * hardware-aware optimization.
 */
public class DuckDBConnectionManager {

    private final String jdbcUrl;
    private final HardwareProfile hardware;
    private final BlockingQueue<DuckDBConnection> connectionPool;
    private final int poolSize;

    public DuckDBConnectionManager() {
        this(detectOptimalConfiguration());
    }

    public DuckDBConnectionManager(Configuration config) {
        this.jdbcUrl = buildJdbcUrl(config);
        this.hardware = HardwareProfile.detect();
        this.poolSize = config.poolSize > 0 ? config.poolSize :
                        Math.min(Runtime.getRuntime().availableProcessors(), 8);
        this.connectionPool = new ArrayBlockingQueue<>(poolSize);

        // Initialize pool
        for (int i = 0; i < poolSize; i++) {
            connectionPool.offer(createConnection());
        }
    }

    /**
     * Acquires a connection from the pool.
     */
    public DuckDBConnection getConnection() throws SQLException {
        try {
            DuckDBConnection conn = connectionPool.poll(30, TimeUnit.SECONDS);
            if (conn == null) {
                throw new SQLException("Connection pool exhausted");
            }
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection", e);
        }
    }

    /**
     * Returns a connection to the pool.
     */
    public void releaseConnection(DuckDBConnection conn) {
        if (conn != null) {
            connectionPool.offer(conn);
        }
    }

    private DuckDBConnection createConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);

        // Configure connection for optimal performance
        try (Statement stmt = duckConn.createStatement()) {
            // Set memory limit based on hardware
            stmt.execute(String.format("SET memory_limit='%s'",
                                      hardware.recommendedMemoryLimit()));

            // Set thread count
            stmt.execute(String.format("SET threads=%d",
                                      hardware.recommendedThreadCount()));

            // Enable progress bar for long queries
            stmt.execute("SET enable_progress_bar=true");

            // Enable parallel CSV/Parquet reading
            stmt.execute("SET preserve_insertion_order=false");
        }

        return duckConn;
    }

    private String buildJdbcUrl(Configuration config) {
        if (config.inMemory) {
            return "jdbc:duckdb:";
        } else {
            return "jdbc:duckdb:" + config.databasePath;
        }
    }

    public void close() throws SQLException {
        for (DuckDBConnection conn : connectionPool) {
            conn.close();
        }
        connectionPool.clear();
    }

    public static class Configuration {
        public boolean inMemory = true;
        public String databasePath = null;
        public int poolSize = 0; // 0 = auto-detect

        public static Configuration inMemory() {
            Configuration config = new Configuration();
            config.inMemory = true;
            return config;
        }

        public static Configuration persistent(String path) {
            Configuration config = new Configuration();
            config.inMemory = false;
            config.databasePath = path;
            return config;
        }
    }

    private static Configuration detectOptimalConfiguration() {
        return Configuration.inMemory();
    }
}
```

##### Task 2B.2: Implement HardwareProfile for Hardware Detection
**Priority**: HIGH
**Estimated Time**: 3 hours
**File**: `core/src/main/java/com/catalyst2sql/runtime/HardwareProfile.java`

**Implementation**:
```java
package com.catalyst2sql.runtime;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

/**
 * Detects hardware capabilities for optimization.
 */
public class HardwareProfile {

    private final int cpuCores;
    private final long totalMemoryBytes;
    private final boolean avx512Supported;
    private final boolean neonSupported;
    private final String architecture;

    private HardwareProfile(int cpuCores, long totalMemory,
                           boolean avx512, boolean neon, String arch) {
        this.cpuCores = cpuCores;
        this.totalMemoryBytes = totalMemory;
        this.avx512Supported = avx512;
        this.neonSupported = neon;
        this.architecture = arch;
    }

    public static HardwareProfile detect() {
        int cores = Runtime.getRuntime().availableProcessors();

        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
            OperatingSystemMXBean.class);
        long memory = osBean.getTotalPhysicalMemorySize();

        String arch = System.getProperty("os.arch").toLowerCase();
        boolean avx512 = arch.contains("x86") || arch.contains("amd64");
        boolean neon = arch.contains("aarch64") || arch.contains("arm");

        return new HardwareProfile(cores, memory, avx512, neon, arch);
    }

    public int recommendedThreadCount() {
        // Use 75% of cores, minimum 1, maximum 16
        return Math.max(1, Math.min(16, (cpuCores * 3) / 4));
    }

    public String recommendedMemoryLimit() {
        // Use 75% of total memory
        long limitBytes = (totalMemoryBytes * 3) / 4;
        return formatBytes(limitBytes);
    }

    private String formatBytes(long bytes) {
        if (bytes >= 1024L * 1024 * 1024) {
            return (bytes / (1024L * 1024 * 1024)) + "GB";
        } else if (bytes >= 1024L * 1024) {
            return (bytes / (1024L * 1024)) + "MB";
        } else {
            return bytes + "B";
        }
    }

    public int cpuCores() { return cpuCores; }
    public long totalMemoryBytes() { return totalMemoryBytes; }
    public boolean supportsAVX512() { return avx512Supported; }
    public boolean supportsNEON() { return neonSupported; }
    public String architecture() { return architecture; }
}
```

##### Task 2B.3: Create Query Executor
**Priority**: HIGH
**Estimated Time**: 3 hours
**File**: `core/src/main/java/com/catalyst2sql/runtime/QueryExecutor.java`

**Implementation**:
```java
package com.catalyst2sql.runtime;

import org.apache.arrow.vector.VectorSchemaRoot;
import java.sql.*;

/**
 * Executes SQL queries against DuckDB and returns results.
 */
public class QueryExecutor {

    private final DuckDBConnectionManager connectionManager;

    public QueryExecutor(DuckDBConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /**
     * Executes a query and returns results as Arrow VectorSchemaRoot.
     */
    public VectorSchemaRoot executeQuery(String sql) throws SQLException {
        DuckDBConnection conn = connectionManager.getConnection();
        try {
            // Execute query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            // Convert to Arrow (implemented in Phase 2C)
            return ArrowInterchange.fromResultSet(rs);

        } finally {
            connectionManager.releaseConnection(conn);
        }
    }

    /**
     * Executes an update/DDL statement.
     */
    public int executeUpdate(String sql) throws SQLException {
        DuckDBConnection conn = connectionManager.getConnection();
        try {
            Statement stmt = conn.createStatement();
            return stmt.executeUpdate(sql);
        } finally {
            connectionManager.releaseConnection(conn);
        }
    }
}
```

---

### **PHASE 2C: PARQUET I/O & ARROW INTERCHANGE** (Days 5-6)
**Goal**: Implement Arrow data interchange and Parquet reader/writer

#### Tasks

##### Task 2C.1: Implement ArrowInterchange
**Priority**: CRITICAL
**Estimated Time**: 4 hours
**File**: `core/src/main/java/com/catalyst2sql/runtime/ArrowInterchange.java`

**Implementation**:
```java
package com.catalyst2sql.runtime;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import java.sql.*;
import java.util.*;

/**
 * Zero-copy Arrow data interchange between DuckDB and Spark.
 */
public class ArrowInterchange {

    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    /**
     * Converts JDBC ResultSet to Arrow VectorSchemaRoot.
     */
    public static VectorSchemaRoot fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build Arrow schema
        List<Field> fields = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String name = meta.getColumnName(i);
            int sqlType = meta.getColumnType(i);
            FieldType fieldType = sqlTypeToArrowType(sqlType);
            fields.add(new Field(name, fieldType, null));
        }
        Schema schema = new Schema(fields);

        // Create vector schema root
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        // Load data
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }

        // Populate vectors
        root.setRowCount(rows.size());
        for (int col = 0; col < columnCount; col++) {
            FieldVector vector = root.getVector(col);
            for (int row = 0; row < rows.size(); row++) {
                Object value = rows.get(row).get(col);
                setVectorValue(vector, row, value);
            }
        }

        return root;
    }

    /**
     * Converts Arrow VectorSchemaRoot to DuckDB table.
     */
    public static void toTable(VectorSchemaRoot root, String tableName,
                               DuckDBConnection conn) throws SQLException {
        // Create table from schema
        String createSQL = generateCreateTable(tableName, root.getSchema());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
        }

        // Insert data
        String insertSQL = generateInsertStatement(tableName, root.getSchema());
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            for (int row = 0; row < root.getRowCount(); row++) {
                for (int col = 0; col < root.getFieldVectors().size(); col++) {
                    FieldVector vector = root.getVector(col);
                    Object value = getVectorValue(vector, row);
                    pstmt.setObject(col + 1, value);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static FieldType sqlTypeToArrowType(int sqlType) {
        // Implementation details...
        return null; // Placeholder
    }

    private static void setVectorValue(FieldVector vector, int index, Object value) {
        // Implementation details...
    }

    private static Object getVectorValue(FieldVector vector, int index) {
        // Implementation details...
        return null;
    }

    private static String generateCreateTable(String tableName, Schema schema) {
        // Implementation details...
        return "";
    }

    private static String generateInsertStatement(String tableName, Schema schema) {
        // Implementation details...
        return "";
    }
}
```

##### Task 2C.2: Implement ParquetReader
**Priority**: CRITICAL
**Estimated Time**: 4 hours
**File**: `core/src/main/java/com/catalyst2sql/io/ParquetReader.java`

**Implementation**:
```java
package com.catalyst2sql.io;

import com.catalyst2sql.logical.LogicalPlan;
import com.catalyst2sql.logical.InMemoryRelation;
import org.apache.arrow.vector.VectorSchemaRoot;
import java.nio.file.Path;
import java.util.List;

/**
 * Reads Parquet files and creates logical plans.
 */
public class ParquetReader {

    /**
     * Reads a single Parquet file.
     */
    public static LogicalPlan readFile(String path) {
        // Use DuckDB's native Parquet reader
        String sql = String.format("SELECT * FROM '%s'", path);
        return new com.catalyst2sql.logical.TableScan(path);
    }

    /**
     * Reads multiple Parquet files matching a glob pattern.
     */
    public static LogicalPlan readGlob(String globPattern) {
        // DuckDB supports glob patterns natively
        String sql = String.format("SELECT * FROM '%s'", globPattern);
        return new com.catalyst2sql.logical.TableScan(globPattern);
    }

    /**
     * Reads partitioned Parquet dataset.
     */
    public static LogicalPlan readPartitioned(String basePath,
                                              List<String> partitionColumns) {
        // DuckDB automatically detects Hive-style partitioning
        String pattern = basePath + "/**/*.parquet";
        return new com.catalyst2sql.logical.TableScan(pattern);
    }

    /**
     * Reads Parquet with filter pushdown.
     */
    public static LogicalPlan readWithFilter(String path, String filterSQL) {
        LogicalPlan scan = readFile(path);
        // Filter will be pushed down to DuckDB
        return scan;
    }
}
```

##### Task 2C.3: Implement ParquetWriter
**Priority**: HIGH
**Estimated Time**: 3 hours
**File**: `core/src/main/java/com/catalyst2sql/io/ParquetWriter.java`

**Implementation**:
```java
package com.catalyst2sql.io;

import com.catalyst2sql.runtime.QueryExecutor;
import java.sql.SQLException;

/**
 * Writes query results to Parquet files.
 */
public class ParquetWriter {

    public enum Compression {
        SNAPPY, GZIP, ZSTD, UNCOMPRESSED
    }

    private final QueryExecutor executor;

    public ParquetWriter(QueryExecutor executor) {
        this.executor = executor;
    }

    /**
     * Writes query results to Parquet file.
     */
    public void write(String sql, String outputPath, Compression compression)
            throws SQLException {
        String copySQL = String.format(
            "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION %s)",
            sql, outputPath, compression.name());

        executor.executeUpdate(copySQL);
    }

    /**
     * Writes with default SNAPPY compression.
     */
    public void write(String sql, String outputPath) throws SQLException {
        write(sql, outputPath, Compression.SNAPPY);
    }

    /**
     * Writes partitioned output.
     */
    public void writePartitioned(String sql, String outputPath,
                                List<String> partitionColumns,
                                Compression compression) throws SQLException {
        String partitionSQL = String.join(", ", partitionColumns);
        String copySQL = String.format(
            "COPY (%s) TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), COMPRESSION %s)",
            sql, outputPath, partitionSQL, compression.name());

        executor.executeUpdate(copySQL);
    }
}
```

---

### **PHASE 2D: EXPRESSION TRANSLATION TESTS & VALIDATION** (Day 7)
**Goal**: Write 100+ expression translation tests and validate all Week 2 deliverables

#### Tasks

##### Task 2D.1: Create ExpressionTranslationTest Suite
**Priority**: CRITICAL
**Estimated Time**: 6 hours
**File**: `tests/src/test/java/com/catalyst2sql/translation/ExpressionTranslationTest.java`

**Test Categories** (100+ tests total):

1. **Arithmetic Expression Tests** (20 tests)
   - Addition, subtraction, multiplication, division
   - Modulo, power, negation
   - Mixed type arithmetic
   - Overflow handling
   - Division by zero

2. **Comparison Expression Tests** (15 tests)
   - Equality (=, !=)
   - Relational (<, <=, >, >=)
   - NULL-safe comparisons (<=>)
   - String comparisons
   - Date comparisons

3. **Logical Expression Tests** (10 tests)
   - AND, OR, NOT
   - Complex nested conditions
   - Short-circuit evaluation
   - Three-valued logic (NULL handling)

4. **String Function Tests** (15 tests)
   - UPPER, LOWER, TRIM
   - CONCAT, SUBSTRING
   - LENGTH, REPLACE
   - LIKE, REGEXP
   - String NULL handling

5. **Math Function Tests** (10 tests)
   - ABS, CEIL, FLOOR
   - ROUND, SQRT, POWER
   - SIN, COS, TAN
   - LOG, EXP

6. **Date Function Tests** (10 tests)
   - YEAR, MONTH, DAY
   - DATE_ADD, DATE_SUB
   - DATEDIFF, DATE_FORMAT
   - CURRENT_DATE, CURRENT_TIMESTAMP

7. **Aggregate Function Tests** (10 tests)
   - SUM, AVG, COUNT
   - MIN, MAX
   - COUNT(DISTINCT)
   - STDDEV, VARIANCE

8. **NULL Handling Tests** (10 tests)
   - IS NULL, IS NOT NULL
   - COALESCE, IFNULL
   - NULLIF
   - NULL in expressions

**Test Template**:
```java
@Test
public void testArithmeticAddition() {
    // Create expressions
    Expression left = Literal.of(10, IntegerType.get());
    Expression right = Literal.of(20, IntegerType.get());
    Expression expr = BinaryExpression.add(left, right);

    // Generate SQL
    String sql = expr.toSQL();

    // Validate SQL
    assertThat(sql).isEqualTo("(10 + 20)");

    // Execute in DuckDB and validate result
    Object result = executeScalar(sql);
    assertThat(result).isEqualTo(30);
}

@Test
public void testStringFunction_Upper() {
    Expression arg = Literal.of("hello", StringType.get());
    Expression func = FunctionCall.of("upper", arg, StringType.get());

    String sql = func.toSQL();
    assertThat(sql).isEqualTo("UPPER('hello')");

    Object result = executeScalar(sql);
    assertThat(result).isEqualTo("HELLO");
}
```

##### Task 2D.2: Create Integration Tests
**Priority**: HIGH
**Estimated Time**: 3 hours
**File**: `tests/src/test/java/com/catalyst2sql/integration/EndToEndTest.java`

**Test Scenarios**:

1. **Simple Query Test**
   ```java
   @Test
   public void testSimpleQuery() throws Exception {
       // Create logical plan
       LogicalPlan scan = TableScan.of("customers.parquet");
       LogicalPlan filter = Filter.of(scan,
           BinaryExpression.greaterThan(
               ColumnReference.of("age"),
               Literal.of(18)));
       LogicalPlan project = Project.of(filter,
           ColumnReference.of("name"),
           ColumnReference.of("email"));

       // Generate SQL
       SQLGenerator generator = new SQLGenerator();
       String sql = generator.generate(project);

       // Execute
       QueryExecutor executor = new QueryExecutor(connectionManager);
       VectorSchemaRoot result = executor.executeQuery(sql);

       // Validate
       assertThat(result.getRowCount()).isGreaterThan(0);
   }
   ```

2. **Parquet Read/Write Test**
   ```java
   @Test
   public void testParquetReadWrite() throws Exception {
       // Read Parquet
       LogicalPlan plan = ParquetReader.readFile("input.parquet");

       // Transform
       plan = Project.of(plan,
           ColumnReference.of("id"),
           FunctionCall.of("upper", ColumnReference.of("name"), StringType.get()));

       // Generate SQL
       String sql = new SQLGenerator().generate(plan);

       // Write to Parquet
       ParquetWriter writer = new ParquetWriter(executor);
       writer.write(sql, "output.parquet", Compression.SNAPPY);

       // Read back and validate
       LogicalPlan readBack = ParquetReader.readFile("output.parquet");
       VectorSchemaRoot result = executor.executeQuery(
           new SQLGenerator().generate(readBack));

       assertThat(result.getRowCount()).isEqualTo(expectedCount);
   }
   ```

##### Task 2D.3: Create Performance Benchmarks
**Priority**: MEDIUM
**Estimated Time**: 2 hours
**File**: `tests/src/test/java/com/catalyst2sql/benchmark/SQLGenerationBenchmark.java`

**Benchmark Tests**:

1. **SQL Generation Performance**
   - Measure time to generate SQL for complex plans
   - Target: < 1ms for simple queries, < 10ms for complex queries

2. **Query Execution Performance**
   - Compare DuckDB vs Spark execution times
   - Target: 5-10x faster than Spark

3. **Parquet I/O Performance**
   - Measure read/write throughput
   - Target: > 1 GB/s on modern hardware

---

## 📊 SUCCESS CRITERIA & VALIDATION

### Functional Requirements

1. **SQL Generation** ✅
   - [ ] Project operator generates valid SELECT clauses
   - [ ] Filter operator generates valid WHERE clauses
   - [ ] TableScan generates valid FROM clauses
   - [ ] Sort generates valid ORDER BY clauses
   - [ ] Limit generates valid LIMIT clauses
   - [ ] Complex nested queries work correctly

2. **DuckDB Execution** ✅
   - [ ] Connection manager creates valid connections
   - [ ] Hardware detection works on x86-64 and ARM
   - [ ] Query executor returns correct results
   - [ ] Connection pooling prevents resource exhaustion

3. **Arrow Interchange** ✅
   - [ ] ResultSet → Arrow conversion works
   - [ ] Arrow → DuckDB table import works
   - [ ] Schema mapping is correct
   - [ ] Data types are preserved

4. **Parquet I/O** ✅
   - [ ] Can read single Parquet files
   - [ ] Can read glob patterns (*.parquet)
   - [ ] Can read partitioned datasets
   - [ ] Can write with SNAPPY compression
   - [ ] Can write with GZIP/ZSTD compression
   - [ ] Partitioned writes work correctly

### Test Coverage Requirements

- **Unit Tests**: 100+ new expression translation tests
- **Integration Tests**: 10+ end-to-end scenarios
- **Total Tests Passing**: 150+ (179 existing + 100 new minimum)
- **Code Coverage**: > 80% for all new code

### Performance Requirements

- **SQL Generation**: < 10ms for complex queries
- **Query Execution**: 5-10x faster than Spark (for analytical workloads)
- **Parquet Read**: > 500 MB/s throughput
- **Parquet Write**: > 300 MB/s throughput

---

## 🚧 RISK MITIGATION

### Risk 1: DuckDB JDBC Driver Compatibility
**Probability**: MEDIUM
**Impact**: HIGH
**Mitigation**:
- Test with multiple DuckDB versions
- Add version detection and compatibility layer
- Provide clear error messages for unsupported versions

### Risk 2: Arrow Conversion Performance
**Probability**: MEDIUM
**Impact**: MEDIUM
**Mitigation**:
- Use zero-copy paths where possible
- Batch data transfers
- Profile and optimize hot paths

### Risk 3: Complex SQL Generation Edge Cases
**Probability**: HIGH
**Impact**: MEDIUM
**Mitigation**:
- Comprehensive test suite with edge cases
- Fuzzing tests for random expression trees
- Validation against DuckDB parser

### Risk 4: Memory Management in Connection Pool
**Probability**: LOW
**Impact**: HIGH
**Mitigation**:
- Implement connection leak detection
- Add connection timeout and cleanup
- Monitor memory usage in tests

---

## 📈 PROGRESS TRACKING

### Daily Checkpoints

**Day 1**: SQL Generation Core
- [ ] SQLGenerator infrastructure complete
- [ ] TableScan, Project, Filter implemented
- [ ] Unit tests passing

**Day 2**: SQL Generation Completion
- [ ] Sort, Limit implemented
- [ ] Complex query tests passing
- [ ] Integration with existing Expression tests

**Day 3**: DuckDB Connection Manager
- [ ] Connection manager implemented
- [ ] Hardware detection working
- [ ] Connection pool tested

**Day 4**: Query Executor
- [ ] Query executor implemented
- [ ] Integration tests passing
- [ ] Performance benchmarks baseline

**Day 5**: Arrow Interchange
- [ ] ResultSet → Arrow working
- [ ] Arrow → DuckDB working
- [ ] Schema mapping validated

**Day 6**: Parquet I/O
- [ ] ParquetReader complete
- [ ] ParquetWriter complete
- [ ] Read/write tests passing

**Day 7**: Testing & Validation
- [ ] 100+ expression translation tests complete
- [ ] All integration tests passing
- [ ] Performance benchmarks meet targets
- [ ] Documentation updated

---

## 📝 DELIVERABLES CHECKLIST

### Code Deliverables
- [ ] `SQLGenerator.java` - Core SQL generation
- [ ] `DuckDBConnectionManager.java` - Connection management
- [ ] `HardwareProfile.java` - Hardware detection
- [ ] `QueryExecutor.java` - Query execution
- [ ] `ArrowInterchange.java` - Arrow conversion
- [ ] `ParquetReader.java` - Parquet reading
- [ ] `ParquetWriter.java` - Parquet writing

### Test Deliverables
- [ ] 100+ expression translation tests
- [ ] 10+ integration tests
- [ ] Performance benchmarks
- [ ] Edge case tests

### Documentation Deliverables
- [ ] API documentation for all new classes
- [ ] Usage examples
- [ ] Performance tuning guide
- [ ] WEEK2_COMPLETION_REPORT.md

### Git Deliverables
- [ ] All code committed with descriptive messages
- [ ] Tests passing in CI/CD
- [ ] No compilation warnings
- [ ] Code reviewed and approved

---

## 🎯 WEEK 2 COMPLETION CRITERIA

Week 2 is considered **COMPLETE** when:

1. ✅ All 5 SQL generation operators implemented (Project, Filter, TableScan, Sort, Limit)
2. ✅ DuckDB connection manager working with hardware detection
3. ✅ Arrow interchange layer functional
4. ✅ Parquet reader supporting files, globs, partitions
5. ✅ Parquet writer with compression options
6. ✅ 150+ tests passing (100+ new expression translation tests)
7. ✅ Performance benchmarks show 5-10x improvement over Spark
8. ✅ All integration tests passing
9. ✅ WEEK2_COMPLETION_REPORT.md created
10. ✅ All changes committed to git

**Target Completion Date**: End of Day 7

---

## 🔄 NEXT STEPS (Week 3 Preview)

After Week 2 completion, Week 3 will focus on:

- Advanced SQL generation (JOIN, UNION, window functions)
- Query optimization layer
- Cost-based optimizer integration
- Additional 100+ integration tests
- Production readiness features

---

*This plan will be executed by the Hive Mind Collective Intelligence System with 4 specialized agents working in parallel.*
