"""
Result Validator for TPC-H Integration Tests

Validates query results against expected outputs.
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
import math


class ResultValidator:
    """Validates query results against expected outputs"""

    def __init__(self, epsilon: float = 1e-6):
        """
        Initialize validator

        Args:
            epsilon: Tolerance for floating point comparisons
        """
        self.epsilon = epsilon

    def validate_row_count(self, df: DataFrame, expected_count: int) -> bool:
        """
        Validate that DataFrame has expected number of rows

        Args:
            df: DataFrame to validate
            expected_count: Expected number of rows

        Returns:
            True if row count matches

        Raises:
            AssertionError if validation fails
        """
        actual_count = df.count()
        if actual_count != expected_count:
            raise AssertionError(
                f"Row count mismatch: expected {expected_count}, got {actual_count}"
            )
        return True

    def validate_schema(self, df: DataFrame, expected_columns: List[str]) -> bool:
        """
        Validate that DataFrame has expected columns

        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names

        Returns:
            True if schema matches

        Raises:
            AssertionError if validation fails
        """
        actual_columns = df.columns
        if set(actual_columns) != set(expected_columns):
            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)
            msg = []
            if missing:
                msg.append(f"Missing columns: {missing}")
            if extra:
                msg.append(f"Extra columns: {extra}")
            raise AssertionError(f"Schema mismatch: {', '.join(msg)}")
        return True

    def validate_column_values(
        self,
        df: DataFrame,
        column: str,
        expected_values: List[Any]
    ) -> bool:
        """
        Validate that a column contains expected values

        Args:
            df: DataFrame to validate
            column: Column name to check
            expected_values: List of expected values

        Returns:
            True if values match

        Raises:
            AssertionError if validation fails
        """
        actual_values = [row[column] for row in df.collect()]
        actual_set = set(actual_values)
        expected_set = set(expected_values)

        if actual_set != expected_set:
            missing = expected_set - actual_set
            extra = actual_set - expected_set
            msg = []
            if missing:
                msg.append(f"Missing values: {missing}")
            if extra:
                msg.append(f"Extra values: {extra}")
            raise AssertionError(
                f"Column '{column}' value mismatch: {', '.join(msg)}"
            )
        return True

    def compare_numeric_value(self, actual: float, expected: float) -> bool:
        """
        Compare two numeric values with epsilon tolerance

        Args:
            actual: Actual value
            expected: Expected value

        Returns:
            True if values are within epsilon
        """
        if actual is None and expected is None:
            return True
        if actual is None or expected is None:
            return False

        # Handle infinity
        if math.isinf(expected):
            return math.isinf(actual) and (actual > 0) == (expected > 0)

        # Handle very small numbers
        if abs(expected) < self.epsilon:
            return abs(actual) < self.epsilon

        # Relative error comparison
        relative_error = abs((actual - expected) / expected)
        return relative_error < self.epsilon

    def validate_dataframe_equals(
        self,
        actual_df: DataFrame,
        expected_df: DataFrame,
        check_order: bool = True
    ) -> bool:
        """
        Validate that two DataFrames are equal

        Args:
            actual_df: Actual DataFrame
            expected_df: Expected DataFrame
            check_order: Whether to check row order

        Returns:
            True if DataFrames are equal

        Raises:
            AssertionError if validation fails
        """
        # Convert to pandas for easier comparison
        actual_pd = actual_df.toPandas()
        expected_pd = expected_df.toPandas()

        # Check row counts
        if len(actual_pd) != len(expected_pd):
            raise AssertionError(
                f"Row count mismatch: expected {len(expected_pd)}, got {len(actual_pd)}"
            )

        # Check columns
        if set(actual_pd.columns) != set(expected_pd.columns):
            raise AssertionError(
                f"Column mismatch: expected {expected_pd.columns.tolist()}, "
                f"got {actual_pd.columns.tolist()}"
            )

        # Sort both if order doesn't matter
        if not check_order:
            actual_pd = actual_pd.sort_values(by=list(actual_pd.columns)).reset_index(drop=True)
            expected_pd = expected_pd.sort_values(by=list(expected_pd.columns)).reset_index(drop=True)

        # Compare values
        try:
            pd.testing.assert_frame_equal(
                actual_pd,
                expected_pd,
                check_dtype=False,  # Allow type differences
                atol=self.epsilon
            )
        except AssertionError as e:
            raise AssertionError(f"DataFrame values don't match: {e}")

        return True

    def validate_aggregate_result(
        self,
        df: DataFrame,
        expected_aggregates: Dict[str, Any]
    ) -> bool:
        """
        Validate aggregate results

        Args:
            df: DataFrame with aggregate results (should have 1 row)
            expected_aggregates: Dict mapping column name to expected value

        Returns:
            True if aggregates match

        Raises:
            AssertionError if validation fails
        """
        rows = df.collect()
        if len(rows) != 1:
            raise AssertionError(
                f"Expected 1 row for aggregate result, got {len(rows)}"
            )

        row = rows[0]
        mismatches = []

        for col_name, expected_value in expected_aggregates.items():
            if col_name not in row:
                raise AssertionError(f"Column '{col_name}' not found in result")

            actual_value = row[col_name]

            # Compare based on type
            if isinstance(expected_value, float):
                if not self.compare_numeric_value(actual_value, expected_value):
                    mismatches.append(
                        f"{col_name}: expected {expected_value}, got {actual_value}"
                    )
            else:
                if actual_value != expected_value:
                    mismatches.append(
                        f"{col_name}: expected {expected_value}, got {actual_value}"
                    )

        if mismatches:
            raise AssertionError(f"Aggregate mismatches:\n" + "\n".join(mismatches))

        return True

    def print_comparison(self, actual_df: DataFrame, expected_df: DataFrame):
        """
        Print a comparison of two DataFrames for debugging

        Args:
            actual_df: Actual DataFrame
            expected_df: Expected DataFrame
        """
        print("\n=== Actual Results ===")
        actual_df.show(truncate=False)

        print("\n=== Expected Results ===")
        expected_df.show(truncate=False)

        print("\n=== Pandas Comparison ===")
        actual_pd = actual_df.toPandas()
        expected_pd = expected_df.toPandas()

        print("\nActual:")
        print(actual_pd)

        print("\nExpected:")
        print(expected_pd)

        if len(actual_pd) == len(expected_pd):
            print("\n=== Differences ===")
            for col in actual_pd.columns:
                if col in expected_pd.columns:
                    diff = actual_pd[col] != expected_pd[col]
                    if diff.any():
                        print(f"\nColumn '{col}' differences:")
                        print(actual_pd[diff][[col]])
                        print("vs")
                        print(expected_pd[diff][[col]])
