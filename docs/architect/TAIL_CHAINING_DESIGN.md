# Tail Operation Design

**Date**: 2025-12-12
**Status**: Implemented

## Summary

In Spark Connect, `tail()` is an **action**, not a transformation. It returns `List[Row]` and must be in "tail position" (root of the plan tree).

## Spark Semantics

```python
# PySpark implementation (spark/python/pyspark/sql/connect/dataframe.py)
def tail(self, num: int) -> List[Row]:
    return DataFrame(plan.Tail(child=self._plan, limit=num), session=self._session).collect()
```

Key constraints:
- Returns `List[Row]`, not `DataFrame`
- Immediately calls `.collect()` - eager evaluation
- "Currently, this plan can only be a root node" (from Spark source)

## Valid vs Invalid Patterns

| Pattern | Valid? | Reason |
|---------|--------|--------|
| `df.tail(100)` | ✅ | Returns list of last 100 rows |
| `df.filter(...).tail(100)` | ✅ | Filter first, then tail |
| `df.tail(100).filter(...)` | ❌ | `List` has no `filter` method |

## ThunderDuck Implementation

- `Tail` detected at plan root (`SparkConnectServiceImpl.java:183`)
- Child plan executed as SQL, streamed through `TailBatchCollector`
- O(N) memory via circular buffer of Arrow batches
- Results streamed back to client

## References

- Spark: `basicLogicalOperators.scala` - "Can only be a root node"
- Spark: `limit.scala` - `CollectTailExec` physical operator
- ThunderDuck: `TailBatchCollector.java`, `Tail.java`
