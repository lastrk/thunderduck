# SQL Parser Research for Thunderduck Query AST

## Executive Summary

Based on my research of SQL parser options for translating SparkSQL queries into Thunderduck's query AST, I've analyzed 5 main approaches. The original recommendation was to reuse Apache Spark's existing SQL parser (Option 1).

> **Implementation Outcome (2026-02):** **Option 5 (Custom ANTLR4 Parser)** was chosen instead, to avoid the Scala runtime dependency and gain full control over AST building. The grammar files (`SqlBaseParser.g4`, `SqlBaseLexer.g4`) were imported from Spark v4.0.0 and a custom `SparkSQLAstBuilder` visitor converts ANTLR parse trees directly to Thunderduck's LogicalPlan/Expression AST. See [SPARKSQL_PARSER_DESIGN.md](architect/SPARKSQL_PARSER_DESIGN.md) for the full design.

---

## Current State Analysis

Thunderduck currently uses **SQLRelation** nodes that pass raw SQL strings through without parsing:

```java
public class SQLRelation extends LogicalPlan {
    private final String sql;

    @Override
    public String toSQL(SQLGenerator generator) {
        return sql;  // Pass-through, no transformation
    }
}
```

This approach is fragile because:
- No validation or analysis of the SQL
- Cannot participate in query optimization
- Doesn't benefit from schema inference or nullability checks
- Creates separate execution path from DataFrame API

---

## Option 1: Apache Spark's SQL Parser (RECOMMENDED)

### Overview
Spark's Catalyst parser is an ANTLR4-based SQL parser that transforms SQL into logical plan ASTs. It's the **same parser Spark uses internally** for SQL queries.

### Architecture
- **Lexer**: `SqlBaseLexer` - tokenizes SQL into comprehensible tokens
- **Parser**: `SqlBaseParser` - builds parse tree from tokens using ANTLR grammar rules
- **AstBuilder**: `AstBuilder` - converts ANTLR parse tree into Catalyst logical plans
- **Grammar**: `SqlBase.g4` - ANTLR4 grammar defining Spark SQL syntax

### Pros
- ✅ **Perfect Spark SQL compatibility** - uses the exact same grammar as Spark
- ✅ **Already available** - `spark-catalyst` is already in your `provided` scope dependencies
- ✅ **Produces Catalyst LogicalPlan** - can be mapped to Thunderduck logical plans
- ✅ **No additional dependencies** - zero added dependency weight
- ✅ **Well-maintained** - actively developed by Apache Spark team
- ✅ **Comprehensive** - handles all Spark SQL features (window functions, CTEs, complex types, etc.)

### Cons
- ❌ **Scala-based** - parser is written in Scala (but callable from Java)
- ❌ **Large dependency tree** - spark-catalyst has many transitive dependencies
- ❌ **Requires Scala runtime** - adds ~10MB+ to runtime classpath
- ❌ **API complexity** - Spark's internal APIs can be complex and change between versions

### Dependencies
```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-catalyst_2.13</artifactId>
    <version>4.0.1</version>
    <scope>provided</scope>  <!-- Already in your project -->
</dependency>
```

**Note**: Already in your `pom.xml` with `provided` scope! The parser is available at runtime when running inside a Spark environment.

### Integration Approach

1. **Use CatalystSqlParser** to parse SQL strings into Catalyst `LogicalPlan`
2. **Create a LogicalPlan converter** to map Catalyst plans to Thunderduck plans
3. **Leverage existing converters** - Your `RelationConverter` already handles Spark Connect protocol → Thunderduck plans

```java
// Example integration
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

String sql = "SELECT * FROM users WHERE age > 18";
LogicalPlan sparkPlan = CatalystSqlParser$.MODULE$.parsePlan(sql);
// Then convert sparkPlan to Thunderduck LogicalPlan using RelationConverter pattern
```

### Performance
- ANTLR4-based with **adaptive LL(*) parsing**
- Spark uses **caching** for DFA states and prediction contexts (configurable via `spark.sql.manage.parser.caches`)
- Performance is acceptable for Spark's production workloads (handles millions of queries/day)

---

## Option 2: Apache Calcite

### Overview
Apache Calcite is a dynamic data management framework with a powerful SQL parser and query optimizer.

### Pros
- ✅ **Dialect support** - includes `SparkSqlDialect` for Spark SQL compatibility
- ✅ **Pure Java** - no Scala dependencies
- ✅ **Powerful optimizer** - hundreds of built-in optimization rules
- ✅ **Well-maintained** - latest release Nov 2025 (version 1.41.0)
- ✅ **Industry adoption** - used by Hive, Drill, Storm, Flink

### Cons
- ❌ **Incomplete Spark SQL support** - `SparkSqlDialect` is for SQL *generation*, not parsing
- ❌ **Large dependency** - Calcite is a full query processing framework (heavyweight)
- ❌ **Over-engineered** - you only need parsing, not full query optimization
- ❌ **Learning curve** - complex API and architecture

### Dependencies
```xml
<dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-core</artifactId>
    <version>1.41.0</version>
</dependency>
```

### Verdict
**Not recommended** - Calcite's SparkSqlDialect is for SQL generation (Calcite → Spark SQL), not for parsing Spark SQL. You'd be pulling in a massive framework for minimal benefit.

---

## Option 3: JSqlParser

### Overview
JSqlParser is a popular Java SQL parser library that uses JavaCC as the parser generator.

### Pros
- ✅ **Pure Java** - no Scala dependencies
- ✅ **Easy to use** - simple API with visitor pattern
- ✅ **Multi-dialect** - supports Oracle, MySQL, PostgreSQL, SQL Server, DuckDB, etc.
- ✅ **Active development** - version 5.3+ includes performance improvements and JMH benchmarks
- ✅ **Lightweight** - smaller than Spark Catalyst or Calcite

### Cons
- ❌ **Limited Spark SQL support** - not designed specifically for Spark SQL syntax
- ❌ **Performance concerns** - 30x slower than manually-written parsers, slower than ANTLR4
- ❌ **Syntax gaps** - may not handle all Spark SQL features (window frames, advanced aggregates, etc.)
- ❌ **Requires JDK11+** - JSqlParser 5.0+ requires Java 11

### Dependencies
```xml
<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>5.3</version>
</dependency>
```

### Performance Benchmarks
- JSqlParser (JavaCC): **~85ms** for parsing TPC-DS queries
- ANTLR4-based: **~65ms** (23% faster)
- Manually-written: **~3ms** (30x faster)

### Verdict
**Not recommended** - While easy to use, JSqlParser lacks comprehensive Spark SQL support and has performance concerns.

---

## Option 4: DuckDB's SQL Parser

### Overview
DuckDB has a high-performance C++ SQL parser optimized for analytical queries.

### Pros
- ✅ **Extremely fast** - native C++ implementation
- ✅ **Already using DuckDB** - you already depend on DuckDB JDBC driver
- ✅ **Analytical SQL focus** - designed for OLAP workloads like Spark

### Cons
- ❌ **No Java bindings** - parser is C++ only, not exposed via JDBC
- ❌ **DuckDB SQL != Spark SQL** - different syntax for some features
- ❌ **Would require JNI** - need to create Java bindings to access parser
- ❌ **Maintenance burden** - would need to maintain custom JNI wrapper

### Verdict
**Not feasible** - DuckDB's parser is not exposed to Java, and creating JNI bindings would be significant effort.

---

## Option 5: Custom ANTLR4 Parser

### Overview
Generate a custom parser from Spark's `SqlBase.g4` ANTLR grammar using ANTLR4.

### Pros
- ✅ **Pure Java** - ANTLR4 can generate Java parsers
- ✅ **Spark SQL grammar** - uses Spark's exact grammar file
- ✅ **Full control** - customize AST building to match Thunderduck directly
- ✅ **No Scala** - eliminates Scala runtime dependency

### Cons
- ❌ **Maintenance burden** - need to sync grammar with Spark versions
- ❌ **Requires AST building** - need to write visitor to convert ANTLR parse tree to Thunderduck plans
- ❌ **ANTLR runtime dependency** - adds `antlr4-runtime` (~700KB)
- ❌ **Performance concerns** - ANTLR4 has 50ms+ overhead vs hand-written parsers
- ❌ **Grammar extraction** - need to extract and maintain `SqlBase.g4` from Spark

### Dependencies
```xml
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>4.13.1</version>
</dependency>
```

### Integration Approach
1. Extract `SqlBase.g4` grammar from Spark
2. Use ANTLR4 Maven plugin to generate Java parser
3. Implement custom `AstBuilder` to convert parse tree to Thunderduck plans

### Verdict
**Viable but high effort** - This gives you full control and eliminates Scala, but requires significant implementation and maintenance effort.

---

## Comparison Matrix

| Feature | Spark Parser | Calcite | JSqlParser | DuckDB Parser | Custom ANTLR4 |
|---------|--------------|---------|------------|---------------|---------------|
| **Spark SQL Compatibility** | ⭐⭐⭐⭐⭐ Perfect | ⭐⭐ Limited | ⭐⭐⭐ Good | ⭐⭐ Poor | ⭐⭐⭐⭐⭐ Perfect |
| **Dependencies** | Already present | Large | Medium | Not available | Small |
| **Performance** | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Good | ⭐⭐ Fair | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Good |
| **Integration Effort** | ⭐⭐⭐⭐ Low | ⭐⭐ High | ⭐⭐⭐⭐ Low | ⭐ Not feasible | ⭐⭐ High |
| **Maintenance** | ⭐⭐⭐⭐⭐ Low | ⭐⭐⭐ Medium | ⭐⭐⭐⭐ Low | ⭐ Not feasible | ⭐⭐ High |
| **Pure Java** | ❌ No (Scala) | ✅ Yes | ✅ Yes | ❌ C++ | ✅ Yes |

---

## Recommendation: Use Spark's Catalyst Parser (Option 1)

### Why This Is The Best Choice

1. **Already available** - `spark-catalyst` is already in your dependencies with `provided` scope
2. **Perfect compatibility** - Uses Spark's exact SQL grammar, ensuring 100% Spark SQL support
3. **Proven at scale** - Handles billions of queries daily in Spark deployments
4. **Low maintenance** - Maintained by Apache Spark team, automatically stays in sync
5. **Reuse existing patterns** - Your `RelationConverter` already converts Spark Connect plans to Thunderduck plans

### Implementation Plan

1. **Create SQL Parser Facade**
   ```java
   public class SparkSqlParser {
       public LogicalPlan parse(String sql) {
           // Use CatalystSqlParser to parse SQL
           org.apache.spark.sql.catalyst.plans.logical.LogicalPlan sparkPlan =
               CatalystSqlParser$.MODULE$.parsePlan(sql);

           // Convert to Thunderduck LogicalPlan
           return convertSparkPlan(sparkPlan);
       }
   }
   ```

2. **Create Catalyst → Thunderduck Converter**
   - Mirror the approach in `RelationConverter`
   - Map Catalyst's `LogicalPlan` classes to Thunderduck's `LogicalPlan` classes
   - Reuse existing expression converters from `ExpressionConverter`

3. **Update SQLRelation**
   ```java
   public class SQLRelation extends LogicalPlan {
       private final String sql;
       private final LogicalPlan parsedPlan;  // Cache parsed plan

       public SQLRelation(String sql) {
           this.sql = sql;
           this.parsedPlan = SparkSqlParser.parse(sql);
       }

       @Override
       public String toSQL(SQLGenerator generator) {
           // Use parsed plan instead of raw SQL
           return generator.generate(parsedPlan);
       }
   }
   ```

### Handling the Scala Dependency

**Concern**: Spark Catalyst is written in Scala.

**Solution**: This is actually not a problem because:
- Scala compiles to JVM bytecode and is fully interoperable with Java
- You already use `spark-catalyst` as a `provided` dependency
- When running in a Spark environment (which Thunderduck does), the Scala runtime is already present
- For standalone usage, users would need Scala runtime anyway (same as using Spark)

---

## Alternative: Custom ANTLR4 Parser (Option 5)

If the Scala dependency is a **hard blocker**, the custom ANTLR4 parser is the best alternative:

### Pros
- Pure Java implementation
- Full control over AST building
- Direct mapping to Thunderduck plans

### Implementation Effort
- **High** - Estimated 2-3 weeks for initial implementation
- Need to extract and maintain `SqlBase.g4` grammar
- Build custom AST visitor to convert parse tree to Thunderduck plans
- Ongoing maintenance to sync with Spark SQL changes

### When to Choose This
- If eliminating Scala is a hard requirement
- If you want to optimize parser performance specifically for Thunderduck
- If you need to customize the SQL dialect (e.g., add Thunderduck-specific extensions)

---

## Sources

### Apache Spark SQL Parser
- [SparkSqlParser on GitHub](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala)
- [SparkSqlParser - The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/sql/SparkSqlParser/)
- [SQL Parsing and AST Building | apache/spark | DeepWiki](https://deepwiki.com/apache/spark/3.1-sql-parsing-and-query-planning)
- [Writing custom optimization in Apache Spark SQL - parser](https://www.waitingforcode.com/apache-spark-sql/writing-custom-optimization-apache-spark-sql-parser/read)
- [Spark SQL Parser Framework](https://mallikarjuna_g.gitbooks.io/spark/content/spark-sql-sql-parsers.html)
- [How to parse Spark SQL faster | by Wubaoqi | Medium](https://wubaoqi.medium.com/how-to-parse-spark-sql-faster-6e7c1f6a5e95)

### Apache Calcite
- [SparkSqlDialect (Apache Calcite API)](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/dialect/SparkSqlDialect.html)
- [Apache Calcite Architecture and Streaming SQL](https://www.xenonstack.com/insights/apache-calcite)
- [Apache Calcite: A Foundational Framework for Optimized](https://arxiv.org/pdf/1802.10233)

### JSqlParser
- [JSqlParser GitHub Repository](https://github.com/JSQLParser/JSqlParser)
- [JSQLParser 5.3 documentation](https://jsqlparser.github.io/JSqlParser/)
- [Top Open-Source SQL Parsers in 2025](https://www.bytebase.com/blog/top-open-source-sql-parsers/)
- [How We Improved Our SQL Parser Speed by 70x](https://www.bytebase.com/blog/how-we-improved-sql-parser-speed-70x/)

### DuckDB
- [Overview of DuckDB Internals](https://duckdb.org/docs/stable/internals/overview)
- [Java JDBC Client – DuckDB](https://duckdb.org/docs/stable/clients/java)

### ANTLR4 and Parser Generators
- [ANTLR](https://www.antlr.org/)
- [About The ANTLR Parser Generator](https://www.antlr.org/about.html)
- [SQL Parser Using ANTLR4 in Java - A Basic Guide](https://javatecharc.com/sql-parser-using-antlr4-in-java/)
- [ANTLR and JavaCC Parser Generators - OSTERING](https://ostering.com/blog/2015/12/29/antlr-and-javacc-parser-generators/)
- [What would be the benefit of switching to ANTLR 4 as a parser generator?](https://github.com/JSQLParser/JSqlParser/wiki/What-would-be-the-benefit-of-switching-to-ANTLR-4-as-a-parser-generator%3F)

### Maven Dependencies
- [Maven Repository: org.apache.spark » spark-catalyst](https://mvnrepository.com/artifact/org.apache.spark/spark-catalyst)
- [spark/sql/catalyst/pom.xml at master · apache/spark](https://github.com/apache/spark/blob/master/sql/catalyst/pom.xml)
- [Building Spark - Spark 4.0.1 Documentation](https://spark.apache.org/docs/latest/building-spark.html)

---

*Research completed: 2025-12-21*
