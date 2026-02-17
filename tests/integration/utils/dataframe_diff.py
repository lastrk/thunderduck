"""
DataFrame Diff Utility for Differential Testing

Provides detailed row-by-row comparison of DataFrames from Spark Reference
and Thunderduck, with clear diff output for mismatches.
"""

import os
import threading
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def collect_with_timeout(df: DataFrame, timeout: int, name: str) -> list:
    """
    Collect DataFrame with timeout.

    Args:
        df: DataFrame to collect
        timeout: Timeout in seconds
        name: Name for error messages (e.g., "Spark Reference", "Thunderduck")

    Returns:
        List of rows

    Raises:
        TimeoutError: If collection exceeds timeout
    """
    result = []
    exception = []

    def collect_func():
        try:
            result.extend(df.collect())
        except Exception as e:
            exception.append(e)

    thread = threading.Thread(target=collect_func)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        raise TimeoutError(f"{name} query execution timed out after {timeout}s")

    if exception:
        raise exception[0]

    return result


class DataFrameDiff:
    """
    Compare two DataFrames and produce detailed diff output
    """

    def __init__(self, epsilon: float = 1e-6, relaxed: bool = False):
        """
        Initialize DataFrame diff utility

        Args:
            epsilon: Tolerance for floating point comparisons
            relaxed: If True, skip type/nullable checks and use lenient numeric matching
        """
        self.epsilon = epsilon
        self.relaxed = relaxed

    def compare(
        self,
        reference_df: DataFrame,
        test_df: DataFrame,
        query_name: str = "Query",
        max_diff_rows: int = 10,
        timeout: int = 10,
        ignore_nullable: bool = False
    ) -> tuple[bool, str, dict]:
        """
        Compare two DataFrames and produce detailed diff

        Args:
            reference_df: Reference DataFrame (Spark)
            test_df: Test DataFrame (Thunderduck)
            query_name: Name of the query for logging
            max_diff_rows: Maximum number of diff rows to show
            timeout: Timeout in seconds for each collect() operation (default: 30)
            ignore_nullable: If True, ignore nullable mismatches between schemas

        Returns:
            Tuple of (passed: bool, message: str, stats: dict)
        """
        print(f"\n{'=' * 80}")
        print(f"Comparing: {query_name}")
        print(f"{'=' * 80}")

        stats = {
            'query_name': query_name,
            'schemas_match': False,
            'row_counts_match': False,
            'data_match': False,
            'reference_rows': 0,
            'test_rows': 0
        }

        # 1. Compare schemas
        schema_match, schema_msg = self._compare_schemas(
            reference_df.schema, test_df.schema, ignore_nullable
        )
        stats['schemas_match'] = schema_match

        if not schema_match:
            print("✗ Schema mismatch")
            print(schema_msg)
            return False, f"Schema mismatch:\n{schema_msg}", stats

        print("✓ Schemas match")

        # 2. Collect and compare row counts (with timeout)
        print(f"  Collecting Spark Reference results (timeout: {timeout}s)...")
        reference_rows = collect_with_timeout(reference_df, timeout, "Spark Reference")

        print(f"  Collecting Thunderduck results (timeout: {timeout}s)...")
        test_rows = collect_with_timeout(test_df, timeout, "Thunderduck")

        stats['reference_rows'] = len(reference_rows)
        stats['test_rows'] = len(test_rows)

        print(f"  Reference rows: {len(reference_rows):,}")
        print(f"  Test rows:      {len(test_rows):,}")

        if len(reference_rows) != len(test_rows):
            stats['row_counts_match'] = False
            msg = f"Row count mismatch: Reference={len(reference_rows)}, Test={len(test_rows)}"
            print(f"✗ {msg}")
            return False, msg, stats

        stats['row_counts_match'] = True
        print("✓ Row counts match")

        # 3. Compare data row-by-row
        mismatches = []
        for i, (ref_row, test_row) in enumerate(zip(reference_rows, test_rows)):
            match, diff_msg = self._compare_rows(
                ref_row, test_row, i, reference_df.columns
            )
            if not match:
                mismatches.append((i, diff_msg))
                if len(mismatches) >= max_diff_rows:
                    break

        if mismatches:
            stats['data_match'] = False
            diff_output = self._format_diff_output(
                mismatches, reference_rows, test_rows, reference_df.columns
            )
            print(f"✗ Found {len(mismatches)} mismatched rows (showing first {max_diff_rows})")
            print(diff_output)
            return False, f"Data mismatch:\n{diff_output}", stats

        stats['data_match'] = True
        print(f"✓ All {len(reference_rows):,} rows match")
        print(f"\n{'=' * 80}")
        print(f"✓ {query_name} PASSED")
        print(f"{'=' * 80}")

        return True, "Results match perfectly", stats

    def _compare_schemas(
        self, reference_schema: StructType, test_schema: StructType,
        ignore_nullable: bool = False
    ) -> tuple[bool, str]:
        """
        Compare two schemas

        Args:
            reference_schema: The reference schema (Spark)
            test_schema: The test schema (Thunderduck)
            ignore_nullable: If True, ignore nullable mismatches (DuckDB vs Spark differences)

        Returns:
            Tuple of (match: bool, message: str)
        """
        ref_fields = [(f.name, str(f.dataType), f.nullable) for f in reference_schema.fields]
        test_fields = [(f.name, str(f.dataType), f.nullable) for f in test_schema.fields]

        if len(ref_fields) != len(test_fields):
            msg = f"Column count mismatch: Reference={len(ref_fields)}, Test={len(test_fields)}"
            return False, msg

        mismatches = []
        for i, (ref_field, test_field) in enumerate(zip(ref_fields, test_fields)):
            ref_name, ref_type, ref_nullable = ref_field
            test_name, test_type, test_nullable = test_field

            if ref_name != test_name:
                mismatches.append(
                    f"  Column {i}: name mismatch - Reference='{ref_name}', Test='{test_name}'"
                )

            # In relaxed mode, skip type checking entirely
            if not self.relaxed and ref_type != test_type:
                mismatches.append(
                    f"  Column '{ref_name}': type mismatch - Reference={ref_type}, Test={test_type}"
                )

            # In relaxed mode, skip nullable checking entirely
            if not self.relaxed and not ignore_nullable and ref_nullable != test_nullable:
                mismatches.append(
                    f"  Column '{ref_name}': nullable mismatch - Reference={ref_nullable}, Test={test_nullable}"
                )

        if mismatches:
            return False, "\n".join(mismatches)

        return True, ""

    def _compare_rows(
        self, ref_row, test_row, row_index: int, columns: list[str]
    ) -> tuple[bool, str]:
        """
        Compare two rows

        Returns:
            Tuple of (match: bool, diff_message: str)
        """
        ref_dict = ref_row.asDict()
        test_dict = test_row.asDict()

        diffs = []
        for col_name in columns:
            ref_val = ref_dict[col_name]
            test_val = test_dict[col_name]

            equal = self._values_equal_relaxed(ref_val, test_val) if self.relaxed else self._values_equal(ref_val, test_val)
            if not equal:
                diffs.append({
                    'column': col_name,
                    'reference': ref_val,
                    'test': test_val
                })

        if diffs:
            return False, diffs

        return True, ""

    def _values_equal(self, ref_val: Any, test_val: Any) -> bool:
        """
        Compare two values with appropriate tolerance

        Args:
            ref_val: Reference value
            test_val: Test value

        Returns:
            True if values are equal (within tolerance)
        """
        # Handle nulls
        if ref_val is None and test_val is None:
            return True
        if ref_val is None or test_val is None:
            return False

        # Handle floats with epsilon
        if isinstance(ref_val, float) and isinstance(test_val, float):
            return abs(ref_val - test_val) < self.epsilon

        # Handle Decimal
        if isinstance(ref_val, Decimal) and isinstance(test_val, Decimal):
            return abs(ref_val - test_val) < Decimal(str(self.epsilon))

        # Handle mixed numeric types (int vs float)
        if isinstance(ref_val, (int, float)) and isinstance(test_val, (int, float)):
            return abs(float(ref_val) - float(test_val)) < self.epsilon

        # Exact equality for everything else
        return ref_val == test_val

    def _values_equal_relaxed(self, ref_val: Any, test_val: Any) -> bool:
        """
        Relaxed value comparison: type-agnostic numeric matching.

        In relaxed mode, we don't care about type differences between
        numeric types (int vs float vs Decimal) — only that the values
        are numerically equivalent.
        """
        # NULL handling — same as strict
        if ref_val is None and test_val is None:
            return True
        if ref_val is None or test_val is None:
            return False

        # Both numeric (int, float, Decimal) — convert to float, compare
        if isinstance(ref_val, (int, float, Decimal)) and isinstance(test_val, (int, float, Decimal)):
            ref_f = float(ref_val)
            test_f = float(test_val)
            # For integral values: exact integer comparison first
            if ref_f == int(ref_f) and test_f == int(test_f):
                return int(ref_f) == int(test_f)
            # For fractional values: epsilon comparison
            return abs(ref_f - test_f) < self.epsilon

        # Everything else: direct equality, fallback to string comparison
        try:
            return ref_val == test_val
        except Exception:
            return str(ref_val) == str(test_val)

    def _format_diff_output(
        self,
        mismatches: list[tuple[int, list[dict]]],
        reference_rows: list,
        test_rows: list,
        columns: list[str]
    ) -> str:
        """
        Format diff output in a readable way

        Args:
            mismatches: List of (row_index, diff_list) tuples
            reference_rows: Reference rows
            test_rows: Test rows
            columns: Column names

        Returns:
            Formatted diff string
        """
        output = []
        output.append(f"\n{'=' * 80}")
        output.append(f"DIFF: {len(mismatches)} mismatched rows")
        output.append(f"{'=' * 80}")

        for row_idx, diffs in mismatches:
            output.append(f"\n--- Row {row_idx} ---")

            ref_row = reference_rows[row_idx].asDict()
            test_row = test_rows[row_idx].asDict()

            # Show full row context
            output.append("Reference row:")
            for col in columns:
                val = ref_row[col]
                output.append(f"  {col}: {val}")

            output.append("\nTest row:")
            for col in columns:
                val = test_row[col]
                # Highlight mismatched columns
                marker = "  "
                for diff in diffs:
                    if diff['column'] == col:
                        marker = "❌"
                        break
                output.append(f"{marker} {col}: {val}")

            # Show detailed diff for mismatched columns
            output.append("\nDifferences:")
            for diff in diffs:
                col = diff['column']
                ref_val = diff['reference']
                test_val = diff['test']
                output.append(f"  {col}:")
                output.append(f"    Reference: {ref_val} ({type(ref_val).__name__})")
                output.append(f"    Test:      {test_val} ({type(test_val).__name__})")

                # For numeric types, show the difference
                if isinstance(ref_val, (int, float)) and isinstance(test_val, (int, float)):
                    diff_val = abs(float(ref_val) - float(test_val))
                    output.append(f"    Diff:      {diff_val:.10f}")

        output.append(f"\n{'=' * 80}")
        return "\n".join(output)


def _is_relaxed_mode():
    """Check if running in relaxed comparison mode.

    Returns True for 'relaxed' and 'auto' modes (without extension, types won't
    match exactly). Only 'strict' mode uses strict comparison.
    """
    mode = os.environ.get('THUNDERDUCK_COMPAT_MODE', 'auto')
    return mode.lower() in ('relaxed', 'auto')


# Convenience function for pytest
def assert_dataframes_equal(
    reference_df: DataFrame,
    test_df: DataFrame,
    query_name: str = "Query",
    epsilon: float = 1e-6,
    max_diff_rows: int = 10,
    timeout: int = 10,
    orchestrator=None,
    ignore_nullable: bool = False
):
    """
    Assert that two DataFrames are equal, with detailed diff on failure.

    If orchestrator is provided, uses the V2 infrastructure with:
    - Proper error classification (hard errors vs soft errors)
    - Timing measurements
    - Server health monitoring

    If orchestrator is not provided, uses basic timeout behavior.

    Args:
        reference_df: Reference DataFrame (Spark)
        test_df: Test DataFrame (Thunderduck)
        query_name: Name of the query for error messages
        epsilon: Tolerance for floating point comparisons
        max_diff_rows: Maximum number of diff rows to show
        timeout: Timeout in seconds for each collect() operation (default: 30)
        orchestrator: Optional TestOrchestrator instance for timing and health checks
        ignore_nullable: If True, ignore nullable mismatches (DuckDB vs Spark differences)

    Raises:
        AssertionError: If DataFrames don't match
        TimeoutError: If query execution times out (basic mode)
        HardError subclass: On timeout, crash, or connection failure (orchestrator mode)
    """
    relaxed = _is_relaxed_mode()
    # In relaxed mode, use a wider epsilon for numeric comparison
    effective_epsilon = max(epsilon, 0.001) if relaxed else epsilon

    if orchestrator is not None:
        # Use V2 path with timing and health checks
        from .exceptions import ResultMismatchError
        try:
            compare_differential(
                orchestrator=orchestrator,
                spark_df=reference_df,
                thunderduck_df=test_df,
                query_name=query_name,
                timeout=timeout,
                epsilon=effective_epsilon,
                max_diff_rows=max_diff_rows,
                ignore_nullable=ignore_nullable,
                relaxed=relaxed
            )
        except ResultMismatchError as e:
            raise AssertionError(str(e)) from e
    else:
        # Use basic path (existing behavior)
        diff = DataFrameDiff(epsilon=effective_epsilon, relaxed=relaxed)
        passed, message, stats = diff.compare(
            reference_df, test_df, query_name, max_diff_rows, timeout, ignore_nullable
        )

        if not passed:
            raise AssertionError(f"{query_name} failed:\n{message}")


# ============================================================================
# Orchestrator-Based Comparison (internal implementation)
# ============================================================================

def compare_differential(
    orchestrator,
    spark_df: DataFrame,
    thunderduck_df: DataFrame,
    query_name: str,
    timeout: int = None,
    epsilon: float = 1e-6,
    max_diff_rows: int = 10,
    ignore_nullable: bool = False,
    relaxed: bool = False
) -> None:
    """
    Compare DataFrames with proper error handling using the orchestrator.

    This function:
    1. Checks server health before collection
    2. Uses orchestrator's timed collection with proper error classification
    3. Records timing metrics
    4. Raises appropriate exceptions for hard vs soft errors

    Args:
        orchestrator: TestOrchestrator instance for timing and error handling
        spark_df: DataFrame from Spark Reference
        thunderduck_df: DataFrame from Thunderduck
        query_name: Name of the query for logging
        timeout: Collection timeout (uses orchestrator default if None)
        epsilon: Tolerance for floating point comparisons
        max_diff_rows: Maximum number of diff rows to show
        ignore_nullable: If True, ignore nullable mismatches
        relaxed: If True, skip type/nullable checks and use lenient numeric matching

    Raises:
        HardError subclass: On timeout, crash, or connection failure
        ResultMismatchError: On value differences (soft error)
    """
    from .exceptions import (
        HealthCheckError,
        QueryTimeoutError,
        ResultMismatchError,
        ServerCrashError,
    )

    timeout = timeout or orchestrator.collect_timeout

    print(f"\n{'=' * 80}")
    print(f"Comparing: {query_name}")
    print(f"{'=' * 80}")

    # Check servers are healthy before attempting collection
    try:
        orchestrator.check_servers_healthy()
    except HealthCheckError as e:
        orchestrator.on_hard_error(e, test_name=query_name)
        raise

    # 1. Compare schemas first (doesn't require collection)
    diff = DataFrameDiff(epsilon=epsilon, relaxed=relaxed)
    schema_match, schema_msg = diff._compare_schemas(
        spark_df.schema, thunderduck_df.schema, ignore_nullable
    )

    if not schema_match:
        print("✗ Schema mismatch")
        print(schema_msg)
        raise ResultMismatchError(
            f"Schema mismatch in {query_name}:\n{schema_msg}",
            query_name=query_name
        )

    print("✓ Schemas match")

    # 2. Collect Spark Reference results with timeout
    print(f"  Collecting Spark Reference results (timeout: {timeout}s)...")
    try:
        spark_rows = orchestrator.collect_result(spark_df, "spark", timeout)
    except QueryTimeoutError as e:
        orchestrator.on_hard_error(e, test_name=query_name)
        raise
    except Exception as e:
        error = ServerCrashError(f"Spark Reference failed during collection: {e}")
        orchestrator.on_hard_error(error, test_name=query_name)
        raise error from e

    print(f"  Spark Reference: {len(spark_rows):,} rows")

    # 3. Check Spark server still healthy after collection
    try:
        orchestrator.check_servers_healthy()
    except HealthCheckError as e:
        orchestrator.on_hard_error(e, test_name=query_name)
        raise

    # 4. Collect Thunderduck results with timeout
    print(f"  Collecting Thunderduck results (timeout: {timeout}s)...")
    try:
        thunderduck_rows = orchestrator.collect_result(thunderduck_df, "thunderduck", timeout)
    except QueryTimeoutError as e:
        orchestrator.on_hard_error(e, test_name=query_name)
        raise
    except Exception as e:
        error = ServerCrashError(f"Thunderduck failed during collection: {e}")
        orchestrator.on_hard_error(error, test_name=query_name)
        raise error from e

    print(f"  Thunderduck: {len(thunderduck_rows):,} rows")

    # 5. Compare row counts
    if len(spark_rows) != len(thunderduck_rows):
        msg = f"Row count mismatch: Spark={len(spark_rows)}, Thunderduck={len(thunderduck_rows)}"
        print(f"✗ {msg}")
        raise ResultMismatchError(
            msg,
            spark_result=spark_rows,
            thunderduck_result=thunderduck_rows,
            query_name=query_name
        )

    print("✓ Row counts match")

    # 6. Compare data row-by-row
    mismatches = []
    columns = spark_df.columns

    for i, (spark_row, td_row) in enumerate(zip(spark_rows, thunderduck_rows)):
        match, diff_msg = diff._compare_rows(
            spark_row, td_row, i, columns
        )
        if not match:
            mismatches.append((i, diff_msg))
            if len(mismatches) >= max_diff_rows:
                break

    if mismatches:
        diff_output = diff._format_diff_output(
            mismatches, spark_rows, thunderduck_rows, columns
        )
        print(f"✗ Found {len(mismatches)} mismatched rows (showing first {max_diff_rows})")
        print(diff_output)
        raise ResultMismatchError(
            f"Data mismatch in {query_name}:\n{diff_output}",
            spark_result=spark_rows,
            thunderduck_result=thunderduck_rows,
            query_name=query_name
        )

    print(f"✓ All {len(spark_rows):,} rows match")
    print(f"\n{'=' * 80}")
    print(f"✓ {query_name} PASSED")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    # Example usage
    print("DataFrameDiff utility loaded")
    print("Use assert_dataframes_equal() for pytest integration")
    print("  - Without orchestrator: basic timeout behavior")
    print("  - With orchestrator: timing measurements and health checks")
