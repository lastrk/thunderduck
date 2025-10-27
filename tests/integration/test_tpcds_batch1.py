"""
TPC-DS Batch 1 Correctness Tests: Q1-Q5
Compare Thunderduck vs Spark reference results
"""

import pytest
import json
from pathlib import Path


@pytest.mark.tpcds
class TestTPCDSBatch1:
    """Validate TPC-DS Q1-Q5 against Spark references"""

    def load_reference(self, qnum):
        """Load Spark reference results from JSON"""
        ref_file = Path(f"/workspace/tests/integration/expected_results/tpcds_q{qnum}_spark_reference.json")
        with open(ref_file) as f:
            return json.load(f)

    def load_query(self, qnum):
        """Load TPC-DS query SQL"""
        query_file = Path(f"/workspace/benchmarks/tpcds_queries/q{qnum}.sql")
        return query_file.read_text()

    def compare_values(self, spark_val, td_val, epsilon=0.01):
        """Compare two values with tolerance for floats"""
        if spark_val is None and td_val is None:
            return True
        if spark_val is None or td_val is None:
            return False

        # Numeric comparison with epsilon
        try:
            spark_num = float(spark_val)
            td_num = float(td_val)
            return abs(spark_num - td_num) <= epsilon
        except (ValueError, TypeError):
            pass

        # String/date comparison
        return str(spark_val) == str(td_val)

    def test_tpcds_q1_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q1: Customer returns validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q1 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(1)
        ref_rows = reference['rows']

        query = load_tpcds_query(1)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows), \
            f"Row count mismatch: Spark={len(ref_rows)}, Thunderduck={len(td_rows)}"

        print(f"\n✓ Row counts match: {len(td_rows)}")

        # Compare values
        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = td_row.asDict()
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
        print(f"\n✅ TPC-DS Q1 CORRECTNESS VALIDATED")

    def test_tpcds_q2_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q2: Weekly sales validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q2 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(2)
        ref_rows = reference['rows']

        query = load_tpcds_query(2)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"\n✓ Row counts match: {len(td_rows)}")

        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"
                    )

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All {len(td_rows)} rows match!")
        print(f"\n✅ TPC-DS Q2 CORRECTNESS VALIDATED")

    def test_tpcds_q3_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q3: Brand analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q3 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(3)
        ref_rows = reference['rows']

        query = load_tpcds_query(3)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"\n✓ Row counts match: {len(td_rows)}")

        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"
                    )

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All values match!")
        print(f"\n✅ TPC-DS Q3 CORRECTNESS VALIDATED")

    def test_tpcds_q4_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q4: Customer profitability validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q4 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(4)
        ref_rows = reference['rows']

        query = load_tpcds_query(4)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"\n✓ Row counts match: {len(td_rows)}")

        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"
                    )

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All values match!")
        print(f"\n✅ TPC-DS Q4 CORRECTNESS VALIDATED")

    def test_tpcds_q5_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q5: Sales/returns analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q5 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(5)
        ref_rows = reference['rows']

        query = load_tpcds_query(5)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"\n✓ Row counts match: {len(td_rows)}")

        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(ref_rows, td_rows)):
            td_dict = {k: float(v) if hasattr(v, '__float__') else v
                      for k, v in td_row.asDict().items()}

            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_dict.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': {ref_row[col]} vs {td_dict.get(col)}"
                    )

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All values match!")
        print(f"\n✅ TPC-DS Q5 CORRECTNESS VALIDATED")
