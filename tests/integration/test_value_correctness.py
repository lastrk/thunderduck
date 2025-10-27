"""
Value Correctness Tests: Compare Thunderduck vs Spark Reference Results

These tests validate that Thunderduck produces IDENTICAL values to Spark,
not just matching row counts/schemas.
"""

import pytest
import json
from pathlib import Path


@pytest.mark.correctness
class TestValueCorrectness:
    """Validate query results match Spark reference exactly"""

    def load_reference(self, qnum):
        """Load Spark reference results from JSON"""
        ref_file = Path(f"/workspace/tests/integration/expected_results/q{qnum}_spark_reference.json")
        with open(ref_file) as f:
            return json.load(f)

    def compare_values(self, spark_val, td_val, epsilon=0.01):
        """Compare two values with tolerance for floats"""
        if spark_val is None and td_val is None:
            return True
        if spark_val is None or td_val is None:
            return False

        # Numeric comparison with epsilon (handles int vs float)
        try:
            spark_num = float(spark_val)
            td_num = float(td_val)
            return abs(spark_num - td_num) <= epsilon
        except (ValueError, TypeError):
            pass

        # String/date comparison
        return str(spark_val) == str(td_val)

    def test_q1_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q1: Validate all values match Spark reference"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q1 - Value-by-Value Comparison")
        print("=" * 80)

        # Load reference
        reference = self.load_reference(1)
        ref_rows = reference['rows']

        # Execute on Thunderduck
        query = load_tpch_query(1)
        result = spark.sql(query)
        td_rows = result.collect()

        # Compare row count
        assert len(td_rows) == len(ref_rows), \
            f"Row count mismatch: Spark={len(ref_rows)}, Thunderduck={len(td_rows)}"

        print(f"\n✓ Row counts match: {len(td_rows)}")

        # Compare values row-by-row
        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': Spark={ref_row[col]}, TD={td_dict.get(col)}"
                    )

        if mismatches:
            print(f"\n❌ Found {len(mismatches)} value mismatches:")
            for mm in mismatches[:10]:
                print(f"   {mm}")
            assert False, f"{len(mismatches)} value mismatches found"

        print(f"✓ All {len(td_rows)} rows match exactly!")
        print(f"\n✅ Q1 CORRECTNESS VALIDATED")

    def test_q6_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q6: Validate revenue calculation matches Spark"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q6 - Revenue Calculation")
        print("=" * 80)

        reference = self.load_reference(6)
        ref_rows = reference['rows']

        query = load_tpch_query(6)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == 1, "Q6 should return 1 row"
        assert len(td_rows) == len(ref_rows)

        # Compare revenue value
        ref_revenue = ref_rows[0]['revenue']
        td_revenue = float(td_rows[0]['revenue'])

        print(f"\nSpark revenue:       {ref_revenue:,.2f}")
        print(f"Thunderduck revenue: {td_revenue:,.2f}")
        print(f"Difference:          {abs(ref_revenue - td_revenue):,.6f}")

        assert abs(ref_revenue - td_revenue) < 0.01, \
            f"Revenue mismatch: {ref_revenue} vs {td_revenue}"

        print(f"\n✅ Q6 CORRECTNESS VALIDATED")

    def test_q13_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q13: Validate customer distribution"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q13 - Customer Distribution")
        print("=" * 80)

        reference = self.load_reference(13)
        ref_rows = reference['rows']

        query = load_tpch_query(13)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows), \
            f"Row count: Spark={len(ref_rows)}, TD={len(td_rows)}"

        print(f"\n✓ Row counts match: {len(td_rows)}")

        # Compare all values
        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = td_row.asDict()
            for col in ref_row.keys():
                ref_val = ref_row[col]
                td_val = int(td_dict[col]) if isinstance(td_dict[col], int) else td_dict[col]
                if ref_val != td_val:
                    mismatches.append(f"Row {i}, '{col}': {ref_val} vs {td_val}")

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All {len(td_rows)} rows match!")
        print(f"\n✅ Q13 CORRECTNESS VALIDATED")

    def test_q5_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q5: Validate multi-join query"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q5 - Multi-Way Join")
        print("=" * 80)

        reference = self.load_reference(5)
        ref_rows = reference['rows']

        query = load_tpch_query(5)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"✓ Row counts match: {len(td_rows)}")

        # Compare values
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    assert False, f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"

        print(f"✓ All values match!")
        print(f"\n✅ Q5 CORRECTNESS VALIDATED")

    def test_q3_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q3: Validate join and aggregation"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q3 - 3-Way Join")
        print("=" * 80)

        reference = self.load_reference(3)
        ref_rows = reference['rows']

        query = load_tpch_query(3)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"✓ Row counts match: {len(td_rows)}")

        # Compare values
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else (v.isoformat() if hasattr(v, 'isoformat') else v)
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    assert False, f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"

        print(f"✓ All values match!")
        print(f"\n✅ Q3 CORRECTNESS VALIDATED")

    def test_q10_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q10: Validate joins with top-N"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q10 - Join + Top-N")
        print("=" * 80)

        reference = self.load_reference(10)
        ref_rows = reference['rows']

        query = load_tpch_query(10)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"✓ Row counts match: {len(td_rows)}")

        # Compare values
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    assert False, f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"

        print(f"✓ All values match!")
        print(f"\n✅ Q10 CORRECTNESS VALIDATED")

    def test_q12_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q12: Validate join with case when"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q12 - Join + Case When")
        print("=" * 80)

        reference = self.load_reference(12)
        ref_rows = reference['rows']

        query = load_tpch_query(12)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"✓ Row counts match: {len(td_rows)}")

        # Compare values using compare_values (handles int/float)
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    assert False, f"Row {i}, '{col}': Spark={ref_row[col]} vs TD={td_dict.get(col)}"

        print(f"✓ All values match!")
        print(f"\n✅ Q12 CORRECTNESS VALIDATED")

    def test_q18_correctness(self, spark, tpch_tables, load_tpch_query):
        """Q18: Validate subquery handling"""
        print("\n" + "=" * 80)
        print("CORRECTNESS TEST: Q18 - Subquery")
        print("=" * 80)

        reference = self.load_reference(18)
        ref_rows = reference['rows']

        query = load_tpch_query(18)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"✓ Row counts match: {len(td_rows)}")

        # Compare values
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else (v.isoformat() if hasattr(v, 'isoformat') else v)
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    assert False, f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"

        print(f"✓ All values match!")
        print(f"\n✅ Q18 CORRECTNESS VALIDATED")
