# M33: Tail Operation Streaming Fix

**Date:** 2025-12-16
**Status:** Complete

## Summary

Fixed the `df.tail(n)` operation which was failing with Arrow memory leak errors. The fix replaces the buffering approach with a streaming ring buffer pattern that properly manages Arrow memory lifecycle.

## Problem

The Tail operation was causing Arrow allocator memory leak errors:
```
Memory leak error from Arrow allocator
```

The root cause was `TailBatchCollector` which:
1. Collected ALL batches into memory before processing
2. Created copied VectorSchemaRoots that weren't properly closed
3. Leaked Arrow buffers when iterating through results

## Solution

Replaced `TailBatchCollector` with `TailBatchIterator` using a streaming ring buffer approach:

### Key Changes

1. **New `TailBatchIterator`** (`core/src/main/java/com/thunderduck/runtime/TailBatchIterator.java`)
   - Uses a ring buffer to keep only the last N rows
   - Properly copies row data (not Arrow buffers) into the buffer
   - Implements `ArrowBatchIterator` interface for streaming compatibility
   - Memory-efficient: only stores N rows regardless of input size

2. **Deleted `TailBatchCollector`** - old buffering approach that caused memory leaks

3. **Updated `StreamingResultHandler`**
   - Added `streamTailResults()` method for tail-specific handling
   - Uses `TailBatchIterator` to process results in streaming fashion

4. **Updated `SparkConnectServiceImpl`**
   - Routes tail operations through the new streaming handler

## Test Results

**Before:**
- `test_tail`: XFAIL (Memory leak error)
- `test_tail_more_than_rows`: XFAIL (Memory leak error)

**After:**
- `test_tail`: PASS
- `test_tail_more_than_rows`: PASS

Full test suite: **20 passed, 8 xfailed** (up from 18 passed, 10 xfailed)

## Files Modified

- `core/src/main/java/com/thunderduck/runtime/TailBatchIterator.java` (new)
- `core/src/main/java/com/thunderduck/runtime/TailBatchCollector.java` (deleted)
- `connect-server/src/main/java/com/thunderduck/connect/service/StreamingResultHandler.java`
- `connect-server/src/main/java/com/thunderduck/connect/service/SparkConnectServiceImpl.java`
- `core/src/main/java/com/thunderduck/logical/Tail.java`
- `tests/integration/test_dataframe_operations.py` (removed xfail markers)
- `CURRENT_FOCUS_E2E_TEST_GAPS.md` (updated status)

## Architecture

```
df.tail(5)
    ↓
Execute full query with streaming
    ↓
TailBatchIterator (ring buffer, keeps last N rows)
    ↓
StreamingResultHandler.streamTailResults()
    ↓
Arrow IPC serialization → gRPC response
```

The ring buffer approach ensures:
- Only N rows in memory, regardless of total result size
- Proper Arrow buffer lifecycle management
- No memory leaks from unclosed VectorSchemaRoots

## Lessons Learned

1. Arrow VectorSchemaRoot objects must be carefully managed - copying the batch object doesn't copy the underlying buffers properly
2. Ring buffer pattern is ideal for tail operations - O(1) space for the last N items
3. Streaming approach is more robust than collecting all results first
