# M24: WriteOperation Implementation

**Date:** 2025-12-12
**Status:** Complete

## Summary

Implemented `df.write.parquet()`, `df.write.csv()`, and `df.write.json()` support using DuckDB's COPY statement to write data to files in various formats.

## Implementation Details

### Files Modified
- `connect-server/.../SparkConnectServiceImpl.java` - Added `executeWriteOperation()` method (~220 lines)

### Files Created
- `tests/src/test/java/com/thunderduck/command/WriteOperationTest.java` - 15 unit tests

## Spark Connect Protocol

From `commands.proto:88-147`:
```protobuf
message WriteOperation {
  Relation input = 1;
  string source = 2;  // Format: parquet, csv, json

  oneof save_type {
    string path = 3;       // File path
    SaveTable table = 4;   // Table write (not implemented)
  }

  SaveMode mode = 5;
  repeated string partitioning_columns = 6;
  map<string, string> options = 9;

  enum SaveMode {
    SAVE_MODE_UNSPECIFIED = 0;
    SAVE_MODE_APPEND = 1;
    SAVE_MODE_OVERWRITE = 2;
    SAVE_MODE_ERROR_IF_EXISTS = 3;
    SAVE_MODE_IGNORE = 4;
  }
}
```

## DuckDB SQL Generation

### Parquet Write
```sql
COPY (SELECT ...) TO '/path/output.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
```

### CSV Write
```sql
COPY (SELECT ...) TO '/path/output.csv' (FORMAT CSV, HEADER true, DELIMITER ',')
```

### JSON Write
```sql
COPY (SELECT ...) TO '/path/output.json' (FORMAT JSON)
```

### Partitioned Write
```sql
COPY (SELECT ...) TO '/path/output' (FORMAT PARQUET, PARTITION_BY ("category"))
```

## Key Design Decisions

### 1. SaveMode Implementation
- **OVERWRITE** (default): Direct COPY overwrites file
- **ERROR_IF_EXISTS**: Check file existence before COPY, throw exception if exists
- **IGNORE**: Check file existence, skip if exists
- **APPEND**: Read existing file, UNION ALL with new data, write back (read-union-write pattern)

### 2. Server-Side Execution
- All writes are server-side (files created on server's filesystem)
- Client sends proto via gRPC, server executes COPY
- This is standard Spark Connect behavior

### 3. Format Detection
- Default format: `parquet` (if source not specified)
- Format options: `parquet`, `csv`, `json`
- Options map used for format-specific settings (header, delimiter, compression)

### 4. Option Handling
- **Compression**: Maps to DuckDB COMPRESSION option (SNAPPY, GZIP, ZSTD)
- **CSV Header**: Maps to HEADER true/false
- **CSV Delimiter**: Maps to DELIMITER option
- **Partitioning**: Maps to PARTITION_BY clause

## Test Coverage

15 tests covering:
- **ParquetWriteTests** (5 tests): Simple write, GZIP compression, ZSTD compression, partitioned output, round-trip validation
- **CSVWriteTests** (3 tests): Simple write, custom delimiter, no header
- **JSONWriteTests** (1 test): Simple write
- **SaveModeTests** (3 tests): Overwrite, error-if-exists check, append mode
- **ErrorHandlingTests** (2 tests): Non-existent directory, invalid SQL
- **LargeDataTests** (1 test): 100K rows efficiency

## Coverage Update

Gap analysis updated to v1.8:
- Commands: 2 implemented + 1 = **3 implemented (30%)**
- Phase 1 is now complete (all 10 critical gaps addressed)

## Limitations

1. **Table writes not supported**: Only path-based writes (`df.write.parquet("/path")`)
2. **S3/Cloud storage**: Requires httpfs extension and credential chain (future enhancement)
3. **Bucket by**: Not implemented (complex partitioning strategy)

## Future Enhancements

1. **S3 Support**: Add httpfs extension with `CREATE SECRET (TYPE s3, PROVIDER credential_chain)` for IAM role and environment variable credential support
2. **WriteOperationV2**: Table API writes for data lake scenarios
3. **Streaming writes**: For streaming DataFrames

## Lessons Learned

1. **COPY statement is powerful**: DuckDB's COPY handles all format-specific serialization
2. **APPEND requires read-union-write**: Unlike append-only file systems, DuckDB requires rewriting the entire file
3. **Partitioned writes create directories**: PARTITION_BY creates directory structure, not a single file
