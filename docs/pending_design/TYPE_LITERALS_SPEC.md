# Type Literals Specification for Spark Connect

**Version:** 1.0
**Date:** 2025-12-16
**Status:** Draft
**Purpose:** Specification for implementing missing type literal support in Thunderduck

---

## Executive Summary

This document specifies the behavior, syntax, and DuckDB mapping for missing type literal support in Spark Connect. Currently, Thunderduck supports primitive type literals (null, boolean, numeric, string, date, timestamp, decimal, binary) but lacks support for:

1. **TimestampNTZ** - Timestamp without timezone
2. **CalendarInterval** - Calendar-based intervals (months, days, microseconds)
3. **YearMonthInterval** - Year-month intervals
4. **DayTimeInterval** - Day-time intervals
5. **Array literals** - Literal arrays with elements
6. **Map literals** - Literal maps with key-value pairs
7. **Struct literals** - Literal structs with named fields

**Implementation Priority:** Medium (Phase 3 in gap analysis)

**Estimated Effort:** 2-3 weeks

---

## Table of Contents

1. [Current State](#1-current-state)
2. [TimestampNTZ](#2-timestampntz)
3. [CalendarInterval](#3-calendarinterval)
4. [YearMonthInterval](#4-yearmonthinterval)
5. [DayTimeInterval](#5-daytimeinterval)
6. [Array Literals](#6-array-literals)
7. [Map Literals](#7-map-literals)
8. [Struct Literals](#8-struct-literals)
9. [Implementation Roadmap](#9-implementation-roadmap)
10. [Test Cases](#10-test-cases)

---

## 1. Current State

### 1.1 Implemented Literal Types

From `ExpressionConverter.java` (lines 68-105), the following literal types are currently supported:

| Literal Type | Proto Field | Java Type | DuckDB Type | Status |
|--------------|-------------|-----------|-------------|--------|
| Null | `null` | `null` | N/A | ✅ |
| Binary | `binary` | `byte[]` | BLOB | ✅ |
| Boolean | `boolean` | `boolean` | BOOLEAN | ✅ |
| Byte | `byte` | `byte` | TINYINT | ✅ |
| Short | `short` | `short` | SMALLINT | ✅ |
| Integer | `integer` | `int` | INTEGER | ✅ |
| Long | `long` | `long` | BIGINT | ✅ |
| Float | `float` | `float` | FLOAT | ✅ |
| Double | `double` | `double` | DOUBLE | ✅ |
| String | `string` | `String` | VARCHAR | ✅ |
| Date | `date` | `int` (days since epoch) | DATE | ✅ |
| Timestamp | `timestamp` | `long` (microseconds since epoch) | TIMESTAMP | ✅ |
| Decimal | `decimal` | `String` (value) + precision/scale | DECIMAL(p,s) | ✅ |

### 1.2 Missing Literal Types

From `expressions.proto` (lines 167-198):

```protobuf
message Literal {
  oneof literal_type {
    // ... implemented types ...

    // NOT IMPLEMENTED:
    int64 timestamp_ntz = 18;                    // ❌
    CalendarInterval calendar_interval = 19;    // ❌
    int32 year_month_interval = 20;             // ❌
    int64 day_time_interval = 21;               // ❌
    Array array = 22;                           // ❌
    Map map = 23;                               // ❌
    Struct struct = 24;                         // ❌
    SpecializedArray specialized_array = 25;   // ❌
  }
}
```

---

## 2. TimestampNTZ

### 2.1 Spark Definition

**Type:** `TimestampNTZ` (Timestamp without Time Zone)

**Description:** Represents timestamps without any timezone information. All operations are performed without timezone conversions.

**Introduced:** Spark 3.4.0

**Value Range:** Year, month, day, hour, minute, second fields (microsecond precision)

### 2.2 Spark SQL Syntax

```sql
-- Cast to TimestampNTZ
SELECT CAST('2025-01-15 10:30:00' AS TIMESTAMP_NTZ);
SELECT CAST('2025-01-15 10:30:00' AS TIMESTAMP WITHOUT TIME ZONE);

-- Using configuration (changes default TIMESTAMP type)
SET spark.sql.timestampType=TIMESTAMP_NTZ;
```

### 2.3 Spark DataFrame API

```python
from pyspark.sql.functions import lit
from datetime import datetime

# Create literal TimestampNTZ
df = spark.createDataFrame([(1,)], ["id"])
df.select(
    lit(datetime(2025, 1, 15, 10, 30, 0)).cast("timestamp_ntz")
)
```

### 2.4 Protobuf Representation

From `expressions.proto` line 188:
```protobuf
// Timestamp in units of microseconds since the UNIX epoch (without timezone information).
int64 timestamp_ntz = 18;
```

**Storage Format:** 64-bit integer, microseconds since Unix epoch (1970-01-01 00:00:00)

**Example:**
- `2025-01-15 10:30:00.123456` → `1736938200123456` microseconds

### 2.5 DuckDB Mapping

**DuckDB Type:** `TIMESTAMP` (without timezone)

**Key Point:** DuckDB's default `TIMESTAMP` type is already without timezone, making it semantically equivalent to Spark's `TIMESTAMP_NTZ`.

**Conversion:**
```sql
-- DuckDB interprets microseconds as timestamp
SELECT to_timestamp(1736938200123456 / 1000000.0);  -- Dividing by 1M converts µs to seconds
-- Returns: 2025-01-15 10:30:00.123456

-- Alternative: Use epoch_us function
SELECT epoch_us(1736938200123456);
-- Returns: 2025-01-15 10:30:00.123456
```

### 2.6 Implementation Notes

**Similarity to Timestamp:** TimestampNTZ has identical storage format to Timestamp (microseconds since epoch). The difference is semantic (timezone awareness), not representational.

**Current Implementation:** Both `TIMESTAMP` and `TIMESTAMP_NTZ` proto types already map to `"TIMESTAMP"` in `SparkDataTypeConverter.java` (lines 63-65):

```java
case TIMESTAMP:
case TIMESTAMP_NTZ:
    return "TIMESTAMP";
```

**Required Changes:**
1. Add `case TIMESTAMP_NTZ:` to `convertLiteral()` in `ExpressionConverter.java`
2. Handle identical to `TIMESTAMP` case (lines 94-96)

**Code:**
```java
case TIMESTAMP_NTZ:
    // TimestampNTZ is stored as microseconds since epoch, same as TIMESTAMP
    return new com.thunderduck.expression.Literal(
        literal.getTimestampNtz(),
        TimestampType.get()  // DuckDB TIMESTAMP is without timezone
    );
```

### 2.7 Test Cases

```python
# Test 1: Basic literal
df = spark.sql("SELECT TIMESTAMP_NTZ '2025-01-15 10:30:00' as ts")
assert df.collect()[0].ts == datetime(2025, 1, 15, 10, 30, 0)

# Test 2: Arithmetic
df = spark.sql("""
    SELECT
        TIMESTAMP_NTZ '2025-01-15 10:30:00' + INTERVAL '1' DAY as next_day
""")

# Test 3: Comparison
df = spark.sql("""
    SELECT TIMESTAMP_NTZ '2025-01-15 10:30:00' > TIMESTAMP_NTZ '2025-01-14 10:30:00' as is_greater
""")
assert df.collect()[0].is_greater == True
```

---

## 3. CalendarInterval

### 3.1 Spark Definition

**Type:** `CalendarInterval`

**Description:** Represents calendar-based intervals with three components: months, days, and microseconds. Used for date arithmetic where month/day lengths vary.

**Components:**
- **months** (int32): Number of months (can be negative)
- **days** (int32): Number of days (can be negative)
- **microseconds** (int64): Number of microseconds (can be negative)

### 3.2 Spark SQL Syntax

```sql
-- Various interval syntaxes
SELECT INTERVAL '1' YEAR;              -- 12 months
SELECT INTERVAL '3' MONTH;             -- 3 months
SELECT INTERVAL '7' DAY;               -- 7 days
SELECT INTERVAL '2' HOUR;              -- 7200000000 microseconds
SELECT INTERVAL '1 year 2 months';     -- 14 months
SELECT INTERVAL '3 days 4 hours';      -- 3 days + 14400000000 microseconds

-- Complex intervals
SELECT INTERVAL '1-2' YEAR TO MONTH;   -- 1 year 2 months = 14 months
SELECT INTERVAL '100 10:30:40.999' DAY TO SECOND;  -- 100 days + time
```

### 3.3 Protobuf Representation

From `expressions.proto` (lines 210-214):
```protobuf
message CalendarInterval {
  int32 months = 1;
  int32 days = 2;
  int64 microseconds = 3;
}
```

**Example:**
- `INTERVAL '1 year 2 months 3 days 4 hours'`
  - months = 14 (1*12 + 2)
  - days = 3
  - microseconds = 14400000000 (4 hours * 3600 * 1000000)

### 3.4 DuckDB Mapping

**DuckDB Type:** `INTERVAL`

DuckDB's INTERVAL type has the same three-component structure:
- Months component (for years/months)
- Days component
- Microseconds component (for time-of-day)

**Conversion Examples:**
```sql
-- Creating intervals in DuckDB
SELECT INTERVAL '1' YEAR;              -- Works
SELECT INTERVAL '3' MONTH;             -- Works
SELECT INTERVAL '7' DAY;               -- Works
SELECT INTERVAL '2' HOUR;              -- Works

-- Compound intervals
SELECT INTERVAL '1 year 2 months';     -- Works
SELECT INTERVAL '3 days 4 hours';      -- Works

-- Using MAKE_INTERVAL function
SELECT MAKE_INTERVAL(
    years := 1,
    months := 2,
    days := 3,
    hours := 4,
    mins := 0,
    secs := 0
);
```

### 3.5 Implementation Strategy

**Challenge:** DuckDB intervals must be expressed in SQL syntax, not as raw component values.

**Option 1: SQL String Construction (Recommended)**
```java
case CALENDAR_INTERVAL:
    CalendarInterval interval = literal.getCalendarInterval();
    String intervalSQL = buildIntervalSQL(
        interval.getMonths(),
        interval.getDays(),
        interval.getMicroseconds()
    );
    return new RawSQLExpression(intervalSQL);

private static String buildIntervalSQL(int months, int days, long microseconds) {
    List<String> parts = new ArrayList<>();

    if (months != 0) {
        parts.add(String.format("INTERVAL '%d' MONTH", months));
    }
    if (days != 0) {
        parts.add(String.format("INTERVAL '%d' DAY", days));
    }
    if (microseconds != 0) {
        // Convert microseconds to seconds with fractional part
        double seconds = microseconds / 1_000_000.0;
        parts.add(String.format("INTERVAL '%.6f' SECOND", seconds));
    }

    if (parts.isEmpty()) {
        return "INTERVAL '0' SECOND";
    }

    // Join with + operator
    return String.join(" + ", parts);
}
```

**Option 2: MAKE_INTERVAL Function**
```java
case CALENDAR_INTERVAL:
    CalendarInterval interval = literal.getCalendarInterval();
    int years = interval.getMonths() / 12;
    int months = interval.getMonths() % 12;
    int days = interval.getDays();
    long hours = interval.getMicroseconds() / 3_600_000_000L;
    long remainingMicros = interval.getMicroseconds() % 3_600_000_000L;
    long minutes = remainingMicros / 60_000_000L;
    remainingMicros = remainingMicros % 60_000_000L;
    double seconds = remainingMicros / 1_000_000.0;

    String sql = String.format(
        "MAKE_INTERVAL(years := %d, months := %d, days := %d, " +
        "hours := %d, mins := %d, secs := %.6f)",
        years, months, days, hours, minutes, seconds
    );
    return new RawSQLExpression(sql);
```

### 3.6 Edge Cases

1. **Negative intervals:** All components can be negative
   ```sql
   INTERVAL '-1' YEAR  -- -12 months
   ```

2. **Zero interval:** All components zero
   ```sql
   INTERVAL '0' SECOND
   ```

3. **Mixed signs:** Components can have different signs (unusual but valid)
   ```sql
   -- months=12, days=-1, microseconds=0
   -- Represents "1 year minus 1 day"
   ```

### 3.7 Test Cases

```python
# Test 1: Simple interval
df = spark.sql("SELECT INTERVAL '1' YEAR as iv")
# Verify: months=12, days=0, microseconds=0

# Test 2: Compound interval
df = spark.sql("SELECT INTERVAL '1 year 2 months 3 days' as iv")
# Verify: months=14, days=3, microseconds=0

# Test 3: Date arithmetic
df = spark.sql("""
    SELECT DATE '2025-01-15' + INTERVAL '1' MONTH as next_month
""")
# Result: 2025-02-15

# Test 4: Negative interval
df = spark.sql("SELECT INTERVAL '-1' YEAR as iv")
# Verify: months=-12

# Test 5: Microsecond precision
df = spark.sql("SELECT INTERVAL '1.123456' SECOND as iv")
# Verify: microseconds=1123456
```

---

## 4. YearMonthInterval

### 4.1 Spark Definition

**Type:** `YearMonthIntervalType(startField, endField)`

**Description:** Represents year-month intervals as a contiguous subset of YEAR and MONTH fields.

**Storage:** Single int32 value representing total months

**Range:** ±178,956,970 years and 11 months (±2,147,483,647 months)

**Field Values:**
- startField: 0 (MONTH) or 1 (YEAR)
- endField: 0 (MONTH) or 1 (YEAR)

### 4.2 Spark SQL Syntax

```sql
-- Year to month
SELECT INTERVAL '100-11' YEAR TO MONTH;  -- 100 years, 11 months = 1211 months

-- Year only
SELECT INTERVAL '2021' YEAR;             -- 24252 months

-- Month only
SELECT INTERVAL '10' MONTH;              -- 10 months
```

### 4.3 Protobuf Representation

From `expressions.proto` line 191:
```protobuf
int32 year_month_interval = 20;
```

From `types.proto` (lines 131-135):
```protobuf
message YearMonthInterval {
  optional int32 start_field = 1;  // 0=MONTH, 1=YEAR
  optional int32 end_field = 2;    // 0=MONTH, 1=YEAR
  uint32 type_variation_reference = 3;
}
```

**Storage:** Total number of months as int32

**Examples:**
- `INTERVAL '1' YEAR` → 12 months
- `INTERVAL '2-3' YEAR TO MONTH` → 27 months (2*12 + 3)
- `INTERVAL '5' MONTH` → 5 months

### 4.4 DuckDB Mapping

**DuckDB Type:** `INTERVAL` (month component only)

DuckDB represents year-month intervals using the months component of its INTERVAL type.

**Conversion:**
```sql
-- From months to interval
SELECT INTERVAL '12' MONTH;           -- 1 year
SELECT INTERVAL '27' MONTH;           -- 2 years 3 months

-- Alternative syntax
SELECT INTERVAL '2-3' YEAR TO MONTH;  -- DuckDB supports this syntax
```

### 4.5 Implementation Strategy

```java
case YEAR_MONTH_INTERVAL:
    int totalMonths = literal.getYearMonthInterval();

    // Option 1: Simple month interval
    String sql = String.format("INTERVAL '%d' MONTH", totalMonths);
    return new RawSQLExpression(sql);

    // Option 2: Year-to-month format (more readable for large values)
    // int years = totalMonths / 12;
    // int months = totalMonths % 12;
    // String sql = String.format("INTERVAL '%d-%d' YEAR TO MONTH", years, months);
    // return new RawSQLExpression(sql);
```

### 4.6 Test Cases

```python
# Test 1: Years
df = spark.sql("SELECT INTERVAL '2' YEAR as iv")
# totalMonths = 24

# Test 2: Months
df = spark.sql("SELECT INTERVAL '5' MONTH as iv")
# totalMonths = 5

# Test 3: Year to month
df = spark.sql("SELECT INTERVAL '2-3' YEAR TO MONTH as iv")
# totalMonths = 27

# Test 4: Negative
df = spark.sql("SELECT INTERVAL '-1' YEAR as iv")
# totalMonths = -12

# Test 5: Arithmetic
df = spark.sql("""
    SELECT DATE '2025-01-15' + INTERVAL '2' YEAR as future
""")
# Result: 2027-01-15
```

---

## 5. DayTimeInterval

### 5.1 Spark Definition

**Type:** `DayTimeIntervalType(startField, endField)`

**Description:** Represents day-time intervals as a contiguous subset of DAY, HOUR, MINUTE, SECOND fields.

**Storage:** Single int64 value representing total microseconds

**Range:** ±106,751,991 days, 23 hours, 59 minutes, 59.999999 seconds

**Field Values:**
- 0 = SECOND
- 1 = MINUTE
- 2 = HOUR
- 3 = DAY

### 5.2 Spark SQL Syntax

```sql
-- Day to second
SELECT INTERVAL '100 10:30:40.999999' DAY TO SECOND;

-- Day only
SELECT INTERVAL '100' DAY;

-- Hour to minute
SELECT INTERVAL '123:10' HOUR TO MINUTE;

-- Minute to second
SELECT INTERVAL '1000:01.001' MINUTE TO SECOND;

-- Second only
SELECT INTERVAL '3661.5' SECOND;  -- 1 hour, 1 minute, 1.5 seconds
```

### 5.3 Protobuf Representation

From `expressions.proto` line 192:
```protobuf
int64 day_time_interval = 21;
```

From `types.proto` (lines 137-141):
```protobuf
message DayTimeInterval {
  optional int32 start_field = 1;  // 0=SECOND, 1=MINUTE, 2=HOUR, 3=DAY
  optional int32 end_field = 2;
  uint32 type_variation_reference = 3;
}
```

**Storage:** Total microseconds as int64

**Conversion Factors:**
- 1 day = 86,400,000,000 microseconds
- 1 hour = 3,600,000,000 microseconds
- 1 minute = 60,000,000 microseconds
- 1 second = 1,000,000 microseconds

**Examples:**
- `INTERVAL '1' DAY` → 86,400,000,000 microseconds
- `INTERVAL '1' HOUR` → 3,600,000,000 microseconds
- `INTERVAL '1.5' SECOND` → 1,500,000 microseconds

### 5.4 DuckDB Mapping

**DuckDB Type:** `INTERVAL` (day + microsecond components)

DuckDB represents day-time intervals using days and microseconds components.

**Conversion:**
```sql
-- DuckDB interval syntax
SELECT INTERVAL '100' DAY;
SELECT INTERVAL '10:30:40.999' HOUR TO SECOND;
SELECT INTERVAL '100 10:30:40' DAY TO SECOND;
```

### 5.5 Implementation Strategy

```java
case DAY_TIME_INTERVAL:
    long totalMicroseconds = literal.getDayTimeInterval();

    // Extract components
    long days = totalMicroseconds / 86_400_000_000L;
    long remainingMicros = Math.abs(totalMicroseconds % 86_400_000_000L);

    long hours = remainingMicros / 3_600_000_000L;
    remainingMicros = remainingMicros % 3_600_000_000L;

    long minutes = remainingMicros / 60_000_000L;
    remainingMicros = remainingMicros % 60_000_000L;

    double seconds = remainingMicros / 1_000_000.0;

    // Build interval SQL
    List<String> parts = new ArrayList<>();

    if (days != 0) {
        parts.add(String.format("INTERVAL '%d' DAY", days));
    }
    if (hours != 0) {
        parts.add(String.format("INTERVAL '%d' HOUR", hours));
    }
    if (minutes != 0) {
        parts.add(String.format("INTERVAL '%d' MINUTE", minutes));
    }
    if (seconds != 0 || parts.isEmpty()) {
        parts.add(String.format("INTERVAL '%.6f' SECOND", seconds));
    }

    String sql = String.join(" + ", parts);
    return new RawSQLExpression(sql);
```

### 5.6 Test Cases

```python
# Test 1: Days
df = spark.sql("SELECT INTERVAL '5' DAY as iv")
# microseconds = 432,000,000,000

# Test 2: Hours
df = spark.sql("SELECT INTERVAL '2' HOUR as iv")
# microseconds = 7,200,000,000

# Test 3: Complex
df = spark.sql("SELECT INTERVAL '1 02:30:45.123' DAY TO SECOND as iv")
# microseconds = 95445123000

# Test 4: Negative
df = spark.sql("SELECT INTERVAL '-1' DAY as iv")
# microseconds = -86,400,000,000

# Test 5: Arithmetic
df = spark.sql("""
    SELECT TIMESTAMP '2025-01-15 10:00:00' + INTERVAL '1' HOUR as result
""")
# Result: 2025-01-15 11:00:00
```

---

## 6. Array Literals

### 6.1 Spark Definition

**Type:** `ARRAY<element_type>`

**Description:** Ordered collection of elements of the same type. Arrays can contain NULL values and can be nested.

### 6.2 Spark SQL Syntax

```sql
-- Using array() function
SELECT array(1, 2, 3) as arr;

-- Nested arrays
SELECT array(array(1, 2), array(3, 4)) as nested;

-- Mixed with NULL
SELECT array(1, NULL, 3) as arr_with_null;

-- Different types (with casting)
SELECT array(1, 2.5, 3) as mixed;  -- All cast to DOUBLE
```

### 6.3 Spark DataFrame API

```python
from pyspark.sql.functions import array, lit

# Create array literal
df.select(array(lit(1), lit(2), lit(3)).alias("arr"))

# Nested
df.select(array(array(lit(1), lit(2)), array(lit(3), lit(4))).alias("nested"))
```

### 6.4 Protobuf Representation

From `expressions.proto` (lines 216-219):
```protobuf
message Array {
  DataType element_type = 1;
  repeated Literal elements = 2;
}
```

**Structure:**
- `element_type`: Type of all elements in the array
- `elements`: List of Literal values (recursively defined)

**Example:**
```
Array {
  element_type: INTEGER
  elements: [
    Literal { integer: 1 },
    Literal { integer: 2 },
    Literal { integer: 3 }
  ]
}
```

### 6.5 DuckDB Mapping

**DuckDB Type:** `LIST`

DuckDB uses `LIST` for variable-length arrays (ARRAY is for fixed-length).

**Syntax:**
```sql
-- Bracket notation (recommended)
SELECT [1, 2, 3] as arr;

-- list_value function
SELECT list_value(1, 2, 3) as arr;

-- Nested lists
SELECT [[1, 2], [3, 4]] as nested;

-- With NULL
SELECT [1, NULL, 3] as arr_with_null;

-- Empty list (requires type casting)
SELECT []::INTEGER[] as empty_arr;
```

### 6.6 Implementation Strategy

```java
case ARRAY:
    Array arrayLiteral = literal.getArray();
    DataType elementType = convertDataType(arrayLiteral.getElementType());

    // Convert each element
    List<String> elementSQLs = new ArrayList<>();
    for (Literal elem : arrayLiteral.getElementsList()) {
        com.thunderduck.expression.Expression elemExpr = convertLiteral(elem);
        elementSQLs.add(elemExpr.toSQL());
    }

    // Build DuckDB LIST syntax
    String listSQL = "[" + String.join(", ", elementSQLs) + "]";

    // If empty, need type cast
    if (elementSQLs.isEmpty()) {
        String duckdbType = TypeMapper.toDuckDBType(elementType);
        listSQL = "[]::(" + duckdbType + "[])"
    }

    return new RawSQLExpression(listSQL);
```

**Alternative using list_value:**
```java
String listSQL = "list_value(" + String.join(", ", elementSQLs) + ")";
```

### 6.7 Test Cases

```python
# Test 1: Simple array
df = spark.sql("SELECT array(1, 2, 3) as arr")
assert df.collect()[0].arr == [1, 2, 3]

# Test 2: Nested arrays
df = spark.sql("SELECT array(array(1, 2), array(3, 4)) as nested")
assert df.collect()[0].nested == [[1, 2], [3, 4]]

# Test 3: Array with NULL
df = spark.sql("SELECT array(1, NULL, 3) as arr")
assert df.collect()[0].arr == [1, None, 3]

# Test 4: Empty array
df = spark.sql("SELECT array() as empty")
assert df.collect()[0].empty == []

# Test 5: String array
df = spark.sql("SELECT array('a', 'b', 'c') as arr")
assert df.collect()[0].arr == ['a', 'b', 'c']

# Test 6: Array operations
df = spark.sql("SELECT array_contains(array(1, 2, 3), 2) as contains")
assert df.collect()[0].contains == True
```

---

## 7. Map Literals

### 7.1 Spark Definition

**Type:** `MAP<key_type, value_type>`

**Description:** Collection of key-value pairs. All keys must be the same type, all values must be the same type. Keys must be unique and non-null.

### 7.2 Spark SQL Syntax

```sql
-- Using map() function (key1, val1, key2, val2, ...)
SELECT map('a', 1, 'b', 2, 'c', 3) as m;

-- Nested maps
SELECT map('outer', map('inner', 42)) as nested;

-- Different key/value types
SELECT map(1, 'one', 2, 'two') as int_to_str;
```

### 7.3 Spark DataFrame API

```python
from pyspark.sql.functions import create_map, lit

# Create map literal
df.select(create_map(lit('a'), lit(1), lit('b'), lit(2)).alias("m"))
```

### 7.4 Protobuf Representation

From `expressions.proto` (lines 221-226):
```protobuf
message Map {
  DataType key_type = 1;
  DataType value_type = 2;
  repeated Literal keys = 3;
  repeated Literal values = 4;
}
```

**Structure:**
- `key_type`: Type of all keys
- `value_type`: Type of all values
- `keys`: List of key literals (length N)
- `values`: List of value literals (length N)

**Example:**
```
Map {
  key_type: STRING
  value_type: INTEGER
  keys: [
    Literal { string: "a" },
    Literal { string: "b" }
  ]
  values: [
    Literal { integer: 1 },
    Literal { integer: 2 }
  ]
}
```

### 7.5 DuckDB Mapping

**DuckDB Type:** `MAP`

DuckDB has native MAP support with similar semantics to Spark.

**Syntax:**
```sql
-- Curly brace notation with MAP keyword
SELECT MAP {'a': 1, 'b': 2, 'c': 3} as m;

-- Two-list constructor
SELECT MAP(['a', 'b', 'c'], [1, 2, 3]) as m;

-- map_from_entries (list of structs)
SELECT map_from_entries([('a', 1), ('b', 2)]) as m;

-- Nested maps
SELECT MAP {'outer': MAP {'inner': 42}} as nested;

-- Different key types
SELECT MAP {1: 'one', 2: 'two'} as int_keys;
```

### 7.6 Implementation Strategy

```java
case MAP:
    Map mapLiteral = literal.getMap();
    DataType keyType = convertDataType(mapLiteral.getKeyType());
    DataType valueType = convertDataType(mapLiteral.getValueType());

    if (mapLiteral.getKeysCount() != mapLiteral.getValuesCount()) {
        throw new PlanConversionException(
            "Map literal keys and values must have same length");
    }

    // Convert keys and values
    List<String> keySQLs = new ArrayList<>();
    List<String> valueSQLs = new ArrayList<>();

    for (int i = 0; i < mapLiteral.getKeysCount(); i++) {
        Literal keyLit = mapLiteral.getKeys(i);
        Literal valLit = mapLiteral.getValues(i);

        keySQLs.add(convertLiteral(keyLit).toSQL());
        valueSQLs.add(convertLiteral(valLit).toSQL());
    }

    // Build DuckDB MAP syntax using two-list constructor
    String keysList = "[" + String.join(", ", keySQLs) + "]";
    String valuesList = "[" + String.join(", ", valueSQLs) + "]";
    String mapSQL = "MAP(" + keysList + ", " + valuesList + ")";

    return new RawSQLExpression(mapSQL);
```

**Alternative using curly brace notation:**
```java
// Build key:value pairs
List<String> pairs = new ArrayList<>();
for (int i = 0; i < mapLiteral.getKeysCount(); i++) {
    String keySQL = convertLiteral(mapLiteral.getKeys(i)).toSQL();
    String valueSQL = convertLiteral(mapLiteral.getValues(i)).toSQL();
    pairs.add(keySQL + ": " + valueSQL);
}

String mapSQL = "MAP {" + String.join(", ", pairs) + "}";
return new RawSQLExpression(mapSQL);
```

### 7.7 Test Cases

```python
# Test 1: Simple map
df = spark.sql("SELECT map('a', 1, 'b', 2) as m")
assert df.collect()[0].m == {'a': 1, 'b': 2}

# Test 2: Empty map
df = spark.sql("SELECT map() as empty")
assert df.collect()[0].empty == {}

# Test 3: Integer keys
df = spark.sql("SELECT map(1, 'one', 2, 'two') as m")
assert df.collect()[0].m == {1: 'one', 2: 'two'}

# Test 4: Nested maps
df = spark.sql("SELECT map('outer', map('inner', 42)) as nested")
assert df.collect()[0].nested == {'outer': {'inner': 42}}

# Test 5: Map operations
df = spark.sql("SELECT map_keys(map('a', 1, 'b', 2)) as keys")
assert sorted(df.collect()[0].keys) == ['a', 'b']

# Test 6: NULL values (keys cannot be NULL)
df = spark.sql("SELECT map('a', 1, 'b', NULL) as m")
assert df.collect()[0].m == {'a': 1, 'b': None}
```

---

## 8. Struct Literals

### 8.1 Spark Definition

**Type:** `STRUCT<field1_name: field1_type, field2_name: field2_type, ...>`

**Description:** Collection of named fields, each with its own type. Similar to a row or record.

### 8.2 Spark SQL Syntax

```sql
-- Using struct() function
SELECT struct(1 as id, 'Alice' as name) as person;

-- Using named_struct()
SELECT named_struct('id', 1, 'name', 'Alice') as person;

-- Nested structs
SELECT struct(
    1 as id,
    struct('Alice' as first, 'Smith' as last) as name
) as person;

-- Accessing fields
SELECT person.name FROM (
    SELECT struct(1 as id, 'Alice' as name) as person
);
```

### 8.3 Spark DataFrame API

```python
from pyspark.sql.functions import struct, lit

# Create struct literal
df.select(struct(lit(1).alias("id"), lit("Alice").alias("name")).alias("person"))
```

### 8.4 Protobuf Representation

From `expressions.proto` (lines 228-231):
```protobuf
message Struct {
  DataType struct_type = 1;
  repeated Literal elements = 2;
}
```

The `struct_type` is a `DataType.Struct` from `types.proto` (lines 167-170):
```protobuf
message Struct {
  repeated StructField fields = 1;
  uint32 type_variation_reference = 2;
}

message StructField {
  string name = 1;
  DataType data_type = 2;
  bool nullable = 3;
  optional string metadata = 4;
}
```

**Structure:**
- `struct_type`: Defines field names and types
- `elements`: List of values (positionally matched to fields)

**Example:**
```
Struct {
  struct_type: {
    fields: [
      { name: "id", data_type: INTEGER },
      { name: "name", data_type: STRING }
    ]
  }
  elements: [
    Literal { integer: 1 },
    Literal { string: "Alice" }
  ]
}
```

### 8.5 DuckDB Mapping

**DuckDB Type:** `STRUCT`

DuckDB has native STRUCT support with named fields.

**Syntax:**
```sql
-- Curly brace notation (recommended)
SELECT {'id': 1, 'name': 'Alice'} as person;

-- struct_pack function with := operator
SELECT struct_pack(id := 1, name := 'Alice') as person;

-- row() function (unnamed, requires casting)
SELECT row(1, 'Alice')::STRUCT(id INTEGER, name VARCHAR) as person;

-- Nested structs
SELECT {
    'id': 1,
    'name': {'first': 'Alice', 'last': 'Smith'}
} as person;

-- Accessing fields
SELECT person.name FROM (
    SELECT {'id': 1, 'name': 'Alice'} as person
);
```

### 8.6 Implementation Strategy

```java
case STRUCT:
    Struct structLiteral = literal.getStruct();
    DataType structType = structLiteral.getStructType();

    if (!structType.hasStruct()) {
        throw new PlanConversionException("Struct literal must have struct type");
    }

    DataType.Struct typeInfo = structType.getStruct();

    if (typeInfo.getFieldsCount() != structLiteral.getElementsCount()) {
        throw new PlanConversionException(
            "Struct literal field count mismatch");
    }

    // Build field: value pairs
    List<String> fieldPairs = new ArrayList<>();
    for (int i = 0; i < typeInfo.getFieldsCount(); i++) {
        String fieldName = typeInfo.getFields(i).getName();
        Literal fieldValue = structLiteral.getElements(i);

        String valueSQL = convertLiteral(fieldValue).toSQL();

        // Quote field names to handle special characters
        String quotedName = "'" + fieldName.replace("'", "''") + "'";
        fieldPairs.add(quotedName + ": " + valueSQL);
    }

    // Build DuckDB STRUCT syntax
    String structSQL = "{" + String.join(", ", fieldPairs) + "}";
    return new RawSQLExpression(structSQL);
```

**Alternative using struct_pack:**
```java
// Build struct_pack arguments
List<String> packArgs = new ArrayList<>();
for (int i = 0; i < typeInfo.getFieldsCount(); i++) {
    String fieldName = typeInfo.getFields(i).getName();
    String valueSQL = convertLiteral(structLiteral.getElements(i)).toSQL();

    // struct_pack uses unquoted field names with := operator
    packArgs.add(fieldName + " := " + valueSQL);
}

String structSQL = "struct_pack(" + String.join(", ", packArgs) + ")";
return new RawSQLExpression(structSQL);
```

### 8.7 Test Cases

```python
# Test 1: Simple struct
df = spark.sql("SELECT struct(1 as id, 'Alice' as name) as person")
row = df.collect()[0]
assert row.person.id == 1
assert row.person.name == 'Alice'

# Test 2: named_struct
df = spark.sql("SELECT named_struct('id', 1, 'name', 'Alice') as person")
row = df.collect()[0]
assert row.person.id == 1
assert row.person.name == 'Alice'

# Test 3: Nested struct
df = spark.sql("""
    SELECT struct(
        1 as id,
        struct('Alice' as first, 'Smith' as last) as name
    ) as person
""")
row = df.collect()[0]
assert row.person.name.first == 'Alice'
assert row.person.name.last == 'Smith'

# Test 4: NULL fields
df = spark.sql("SELECT struct(1 as id, NULL as name) as person")
row = df.collect()[0]
assert row.person.id == 1
assert row.person.name is None

# Test 5: Field access
df = spark.sql("""
    SELECT person.name FROM (
        SELECT struct(1 as id, 'Alice' as name) as person
    )
""")
assert df.collect()[0].name == 'Alice'
```

---

## 9. Implementation Roadmap

### 9.1 Phase 1: Foundation (1 week)

**Goal:** Add basic infrastructure for complex literals

**Tasks:**
1. Add `TimestampNTZ` literal support (easiest, identical to Timestamp)
2. Create utility methods for interval SQL generation
3. Add unit tests for interval utilities

**Deliverables:**
- TimestampNTZ working with basic tests
- Helper methods: `buildIntervalSQL()`, `buildCalendarIntervalSQL()`, etc.

### 9.2 Phase 2: Intervals (1 week)

**Goal:** Implement all interval types

**Tasks:**
1. Implement `CalendarInterval` literals
2. Implement `YearMonthInterval` literals
3. Implement `DayTimeInterval` literals
4. Add comprehensive interval tests
5. Test interval arithmetic with dates/timestamps

**Deliverables:**
- All 3 interval types working
- 15+ differential tests for intervals

### 9.3 Phase 3: Complex Types (1 week)

**Goal:** Implement array, map, struct literals

**Tasks:**
1. Implement `Array` literals (recursive)
2. Implement `Map` literals
3. Implement `Struct` literals
4. Add nested structure tests
5. Integration with existing array/map/struct functions

**Deliverables:**
- All 3 complex types working
- 20+ differential tests for complex literals
- Support for nested structures (array of structs, map of arrays, etc.)

### 9.4 Phase 4: Edge Cases & Polish (2-3 days)

**Goal:** Handle edge cases and optimize

**Tasks:**
1. Empty collections ([], {}, map())
2. NULL handling in complex types
3. Type inference for empty literals
4. Performance optimization for large literals
5. Error message improvements

**Deliverables:**
- Robust edge case handling
- Clear error messages
- Documentation updates

---

## 10. Test Cases

### 10.1 Differential Test Suite

Create `/workspace/tests/integration/test_type_literals.py`:

```python
import pytest
from pyspark.sql import SparkSession
from datetime import datetime, date

class TestTypeLiterals:
    """Differential tests for type literals"""

    # TimestampNTZ
    def test_timestamp_ntz_literal(self, spark, thunderduck):
        query = "SELECT TIMESTAMP_NTZ '2025-01-15 10:30:00' as ts"
        # Compare results...

    def test_timestamp_ntz_arithmetic(self, spark, thunderduck):
        query = """
            SELECT
                TIMESTAMP_NTZ '2025-01-15 10:30:00' + INTERVAL '1' DAY as next_day
        """
        # Compare results...

    # CalendarInterval
    def test_calendar_interval_year(self, spark, thunderduck):
        query = "SELECT INTERVAL '1' YEAR as iv"
        # Compare results...

    def test_calendar_interval_compound(self, spark, thunderduck):
        query = "SELECT INTERVAL '1 year 2 months 3 days' as iv"
        # Compare results...

    # YearMonthInterval
    def test_year_month_interval(self, spark, thunderduck):
        query = "SELECT INTERVAL '2-3' YEAR TO MONTH as iv"
        # Compare results...

    # DayTimeInterval
    def test_day_time_interval(self, spark, thunderduck):
        query = "SELECT INTERVAL '1 02:30:45' DAY TO SECOND as iv"
        # Compare results...

    # Array literals
    def test_array_literal_int(self, spark, thunderduck):
        query = "SELECT array(1, 2, 3) as arr"
        # Compare results...

    def test_array_literal_nested(self, spark, thunderduck):
        query = "SELECT array(array(1, 2), array(3, 4)) as nested"
        # Compare results...

    def test_array_literal_with_null(self, spark, thunderduck):
        query = "SELECT array(1, NULL, 3) as arr"
        # Compare results...

    # Map literals
    def test_map_literal_string_int(self, spark, thunderduck):
        query = "SELECT map('a', 1, 'b', 2) as m"
        # Compare results...

    def test_map_literal_nested(self, spark, thunderduck):
        query = "SELECT map('outer', map('inner', 42)) as nested"
        # Compare results...

    # Struct literals
    def test_struct_literal_simple(self, spark, thunderduck):
        query = "SELECT struct(1 as id, 'Alice' as name) as person"
        # Compare results...

    def test_struct_literal_nested(self, spark, thunderduck):
        query = """
            SELECT struct(
                1 as id,
                struct('Alice' as first, 'Smith' as last) as name
            ) as person
        """
        # Compare results...
```

### 10.2 Unit Tests

Add to `ExpressionConverterTest.java`:

```java
@Test
public void testTimestampNtzLiteral() {
    Expression.Literal literal = Expression.Literal.newBuilder()
        .setTimestampNtz(1736938200123456L)
        .build();

    Expression expr = Expression.newBuilder()
        .setLiteral(literal)
        .build();

    com.thunderduck.expression.Expression result = converter.convert(expr);

    assertTrue(result instanceof Literal);
    assertEquals(1736938200123456L, ((Literal) result).value());
    assertEquals(TimestampType.get(), result.dataType());
}

@Test
public void testArrayLiteralInt() {
    Expression.Literal.Array array = Expression.Literal.Array.newBuilder()
        .setElementType(DataType.newBuilder().setInteger(DataType.Integer.newBuilder()).build())
        .addElements(Expression.Literal.newBuilder().setInteger(1).build())
        .addElements(Expression.Literal.newBuilder().setInteger(2).build())
        .addElements(Expression.Literal.newBuilder().setInteger(3).build())
        .build();

    Expression.Literal literal = Expression.Literal.newBuilder()
        .setArray(array)
        .build();

    Expression expr = Expression.newBuilder()
        .setLiteral(literal)
        .build();

    com.thunderduck.expression.Expression result = converter.convert(expr);

    assertTrue(result instanceof RawSQLExpression);
    assertEquals("[1, 2, 3]", result.toSQL());
}

// Similar tests for Map, Struct, Intervals...
```

---

## 11. References

### 11.1 Documentation

**Spark:**
- [Spark SQL Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)
- [Databricks INTERVAL Type](https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html)
- Spark Connect Protocol: `/workspace/connect-server/src/main/proto/spark/connect/`

**DuckDB:**
- [DuckDB Data Types](https://duckdb.org/docs/stable/sql/data_types/overview)
- [DuckDB LIST Type](https://duckdb.org/docs/stable/sql/data_types/list)
- [DuckDB MAP Type](https://duckdb.org/docs/stable/sql/data_types/map)
- [DuckDB STRUCT Type](https://duckdb.org/docs/stable/sql/data_types/struct)
- [DuckDB Literal Types](https://duckdb.org/docs/stable/sql/data_types/literal_types)

### 11.2 Source Files

**Protocol Definitions:**
- `/workspace/connect-server/src/main/proto/spark/connect/expressions.proto` (lines 167-243)
- `/workspace/connect-server/src/main/proto/spark/connect/types.proto` (lines 28-205)

**Implementation:**
- `/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/ExpressionConverter.java`
- `/workspace/connect-server/src/main/java/com/thunderduck/connect/converter/SparkDataTypeConverter.java`
- `/workspace/core/src/main/java/com/thunderduck/types/TypeMapper.java`

### 11.3 Related Documents

- `/workspace/CURRENT_FOCUS_SPARK_CONNECT_GAP_ANALYSIS.md` - Section 2.3 (Literal Type Support)
- `/workspace/docs/architect/DIFFERENTIAL_TESTING_ARCHITECTURE.md` - Testing strategy

---

## Appendix A: Quick Reference

### Literal Type Support Matrix

| Type | Spark SQL | DuckDB | Status | Complexity |
|------|-----------|--------|--------|------------|
| TimestampNTZ | `TIMESTAMP_NTZ '...'` | `TIMESTAMP` | ❌ | Low (identical to TIMESTAMP) |
| CalendarInterval | `INTERVAL '...'` | `INTERVAL` | ❌ | Medium (component extraction) |
| YearMonthInterval | `INTERVAL '...' YEAR TO MONTH` | `INTERVAL` | ❌ | Low (total months) |
| DayTimeInterval | `INTERVAL '...' DAY TO SECOND` | `INTERVAL` | ❌ | Medium (component extraction) |
| Array | `array(1, 2, 3)` | `[1, 2, 3]` | ❌ | Medium (recursive) |
| Map | `map('a', 1)` | `MAP {'a': 1}` | ❌ | Medium (key-value pairs) |
| Struct | `struct(1 as id)` | `{'id': 1}` | ❌ | Medium (named fields) |

### Conversion Factors

**Time Units:**
- 1 day = 86,400 seconds = 86,400,000,000 microseconds
- 1 hour = 3,600 seconds = 3,600,000,000 microseconds
- 1 minute = 60 seconds = 60,000,000 microseconds
- 1 second = 1,000,000 microseconds

**Calendar Units:**
- 1 year = 12 months
- Month lengths vary (28-31 days)
- Day lengths vary with DST (23-25 hours)

---

**Document Version:** 1.0
**Last Updated:** 2025-12-16
**Author:** Generated from Spark Connect 4.0.x protocol and DuckDB documentation
