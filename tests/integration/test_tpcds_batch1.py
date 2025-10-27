"""
TPC-DS Batch 1 Correctness Tests: Q1-Q20
Compare Thunderduck vs Spark reference results
"""

import pytest
import json
from pathlib import Path


@pytest.mark.tpcds
class TestTPCDSBatch1:
    """Validate TPC-DS Q1-Q20 against Spark references"""

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

    def compare_results_order_independent(self, ref_rows, td_rows):
        """
        Compare results with order-independent sorting

        Handles queries where ORDER BY has ties - sorts both result sets
        by all columns to enable comparison regardless of tie-breaking order.

        Returns: (sorted_ref, sorted_td) both as lists of dicts
        """
        # Convert TD rows to dicts
        td_dicts = []
        for row in td_rows:
            td_dict = {}
            for k, v in row.asDict().items():
                # Convert Decimal to float
                if hasattr(v, '__float__') and not isinstance(v, (int, float)):
                    td_dict[k] = float(v)
                else:
                    td_dict[k] = v
            td_dicts.append(td_dict)

        # Sort function that works for dicts
        def sort_key(row_dict):
            # Create stable sort key from all values
            return tuple(str(row_dict.get(k, '')) for k in sorted(row_dict.keys()))

        sorted_ref = sorted(ref_rows, key=sort_key)
        sorted_td = sorted(td_dicts, key=sort_key)

        return sorted_ref, sorted_td

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

    def test_tpcds_q6_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q6: State-level sales validation (ORDER BY with ties)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q6 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(6)
        ref_rows = reference['rows']

        query = load_tpcds_query(6)
        result = spark.sql(query)
        td_rows = result.collect()

        assert len(td_rows) == len(ref_rows)
        print(f"\n✓ Row counts match: {len(td_rows)}")

        # Q6 uses ORDER BY cnt - when counts are equal, tie-breaking is non-deterministic
        # Use order-independent comparison
        print("  Note: Using order-independent comparison (ORDER BY has ties)")
        sorted_ref, sorted_td = self.compare_results_order_independent(ref_rows, td_rows)

        mismatches = []
        for i, (ref_row, td_row) in enumerate(zip(sorted_ref, sorted_td)):
            for col in ref_row.keys():
                if not self.compare_values(ref_row[col], td_row.get(col)):
                    mismatches.append(
                        f"Row {i}, '{col}': {ref_row[col]} vs {td_row.get(col)}"
                    )

        assert len(mismatches) == 0, f"Mismatches: {mismatches[:5]}"
        print(f"✓ All values match (after stable sort)!")
        print(f"\n✅ TPC-DS Q6 CORRECTNESS VALIDATED")

    def test_tpcds_q7_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q7: Promotional sales validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q7 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(7)
        ref_rows = reference['rows']

        query = load_tpcds_query(7)
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
        print(f"\n✅ TPC-DS Q7 CORRECTNESS VALIDATED")

    def test_tpcds_q8_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q8: Store sales by zip code validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q8 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(8)
        ref_rows = reference['rows']

        query = load_tpcds_query(8)
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
        print(f"\n✅ TPC-DS Q8 CORRECTNESS VALIDATED")

    def test_tpcds_q9_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q9: Reason for returns validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q9 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(9)
        ref_rows = reference['rows']

        query = load_tpcds_query(9)
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
        print(f"\n✅ TPC-DS Q9 CORRECTNESS VALIDATED")

    def test_tpcds_q11_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q11: Customer demographics validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q11 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(11)
        ref_rows = reference['rows']

        query = load_tpcds_query(11)
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
        print(f"\n✅ TPC-DS Q11 CORRECTNESS VALIDATED")

    def test_tpcds_q12_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q12: Web sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q12 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(12)
        ref_rows = reference['rows']

        query = load_tpcds_query(12)
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
        print(f"\n✅ TPC-DS Q12 CORRECTNESS VALIDATED")

    def test_tpcds_q13_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q13: Store sales by demographics validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q13 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(13)
        ref_rows = reference['rows']

        query = load_tpcds_query(13)
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
        print(f"\n✅ TPC-DS Q13 CORRECTNESS VALIDATED")

    def test_tpcds_q14a_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q14a: Item cross-selling validation (part 1)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q14a CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('14a')
        ref_rows = reference['rows']

        query = load_tpcds_query('14a')
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
        print(f"\n✅ TPC-DS Q14a CORRECTNESS VALIDATED")

    def test_tpcds_q14b_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q14b: Item cross-selling validation (part 2)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q14b CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('14b')
        ref_rows = reference['rows']

        query = load_tpcds_query('14b')
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
        print(f"\n✅ TPC-DS Q14b CORRECTNESS VALIDATED")

    def test_tpcds_q15_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q15: Customer zip code sales validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q15 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(15)
        ref_rows = reference['rows']

        query = load_tpcds_query(15)
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
        print(f"\n✅ TPC-DS Q15 CORRECTNESS VALIDATED")

    def test_tpcds_q16_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q16: Order counts by ship mode validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q16 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(16)
        ref_rows = reference['rows']

        query = load_tpcds_query(16)
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
        print(f"\n✅ TPC-DS Q16 CORRECTNESS VALIDATED")

    def test_tpcds_q17_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q17: Quarterly store sales validation (empty result set)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q17 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(17)
        ref_rows = reference['rows']

        query = load_tpcds_query(17)
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
        print(f"\n✅ TPC-DS Q17 CORRECTNESS VALIDATED")

    def test_tpcds_q18_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q18: Catalog sales summary validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q18 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(18)
        ref_rows = reference['rows']

        query = load_tpcds_query(18)
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
        print(f"\n✅ TPC-DS Q18 CORRECTNESS VALIDATED")

    def test_tpcds_q19_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q19: Brand sales by manager validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q19 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(19)
        ref_rows = reference['rows']

        query = load_tpcds_query(19)
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
        print(f"\n✅ TPC-DS Q19 CORRECTNESS VALIDATED")

    def test_tpcds_q20_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q20: Item sales by category validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q20 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(20)
        ref_rows = reference['rows']

        query = load_tpcds_query(20)
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
        print(f"\n✅ TPC-DS Q20 CORRECTNESS VALIDATED")

    def test_tpcds_q21_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q21: Warehouse inventory validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q21 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(21)
        ref_rows = reference['rows']

        query = load_tpcds_query(21)
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
        print(f"\n✅ TPC-DS Q21 CORRECTNESS VALIDATED")

    def test_tpcds_q22_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q22: Inventory before/after validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q22 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(22)
        ref_rows = reference['rows']

        query = load_tpcds_query(22)
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
        print(f"\n✅ TPC-DS Q22 CORRECTNESS VALIDATED")

    def test_tpcds_q23a_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q23a: Frequent customer analysis validation (part 1)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q23a CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('23a')
        ref_rows = reference['rows']

        query = load_tpcds_query('23a')
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
        print(f"\n✅ TPC-DS Q23a CORRECTNESS VALIDATED")

    def test_tpcds_q23b_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q23b: Frequent customer analysis validation (part 2)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q23b CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('23b')
        ref_rows = reference['rows']

        query = load_tpcds_query('23b')
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
        print(f"\n✅ TPC-DS Q23b CORRECTNESS VALIDATED")

    def test_tpcds_q24a_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q24a: Multi-channel customer analysis validation (part 1)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q24a CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('24a')
        ref_rows = reference['rows']

        query = load_tpcds_query('24a')
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
        print(f"\n✅ TPC-DS Q24a CORRECTNESS VALIDATED")

    def test_tpcds_q24b_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q24b: Multi-channel customer analysis validation (part 2)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q24b CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('24b')
        ref_rows = reference['rows']

        query = load_tpcds_query('24b')
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
        print(f"\n✅ TPC-DS Q24b CORRECTNESS VALIDATED")

    def test_tpcds_q25_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q25: Store sales by category validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q25 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(25)
        ref_rows = reference['rows']

        query = load_tpcds_query(25)
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
        print(f"\n✅ TPC-DS Q25 CORRECTNESS VALIDATED")

    def test_tpcds_q26_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q26: Item promotion sales validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q26 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(26)
        ref_rows = reference['rows']

        query = load_tpcds_query(26)
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
        print(f"\n✅ TPC-DS Q26 CORRECTNESS VALIDATED")

    def test_tpcds_q27_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q27: Store sales by state validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q27 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(27)
        ref_rows = reference['rows']

        query = load_tpcds_query(27)
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
        print(f"\n✅ TPC-DS Q27 CORRECTNESS VALIDATED")

    def test_tpcds_q28_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q28: Store sales by category validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q28 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(28)
        ref_rows = reference['rows']

        query = load_tpcds_query(28)
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
        print(f"\n✅ TPC-DS Q28 CORRECTNESS VALIDATED")

    def test_tpcds_q29_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q29: Store sales by item class validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q29 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(29)
        ref_rows = reference['rows']

        query = load_tpcds_query(29)
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
        print(f"\n✅ TPC-DS Q29 CORRECTNESS VALIDATED")

    def test_tpcds_q31_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q31: Customer cross-channel analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q31 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(31)
        ref_rows = reference['rows']

        query = load_tpcds_query(31)
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
        print(f"\n✅ TPC-DS Q31 CORRECTNESS VALIDATED")

    def test_tpcds_q32_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q32: Catalog sales discount validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q32 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(32)
        ref_rows = reference['rows']

        query = load_tpcds_query(32)
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
        print(f"\n✅ TPC-DS Q32 CORRECTNESS VALIDATED")

    def test_tpcds_q33_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q33: Manufacturer sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q33 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(33)
        ref_rows = reference['rows']

        query = load_tpcds_query(33)
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
        print(f"\n✅ TPC-DS Q33 CORRECTNESS VALIDATED")

    def test_tpcds_q34_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q34: Customer preferences validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q34 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(34)
        ref_rows = reference['rows']

        query = load_tpcds_query(34)
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
        print(f"\n✅ TPC-DS Q34 CORRECTNESS VALIDATED")

    def test_tpcds_q36_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q36: Store sales by item and state validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q36 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(36)
        ref_rows = reference['rows']

        query = load_tpcds_query(36)
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
        print(f"\n✅ TPC-DS Q36 CORRECTNESS VALIDATED")

    def test_tpcds_q37_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q37: Inventory item promotion validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q37 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(37)
        ref_rows = reference['rows']

        query = load_tpcds_query(37)
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
        print(f"\n✅ TPC-DS Q37 CORRECTNESS VALIDATED")

    def test_tpcds_q38_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q38: Customer count by demographics validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q38 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(38)
        ref_rows = reference['rows']

        query = load_tpcds_query(38)
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
        print(f"\n✅ TPC-DS Q38 CORRECTNESS VALIDATED")

    def test_tpcds_q39a_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q39a: Warehouse inventory validation (part 1)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q39a CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('39a')
        ref_rows = reference['rows']

        query = load_tpcds_query('39a')
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
        print(f"\n✅ TPC-DS Q39a CORRECTNESS VALIDATED")

    def test_tpcds_q39b_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q39b: Warehouse inventory validation (part 2)"""
        print("\n" + "=" * 80)
        print("TPC-DS Q39b CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference('39b')
        ref_rows = reference['rows']

        query = load_tpcds_query('39b')
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
        print(f"\n✅ TPC-DS Q39b CORRECTNESS VALIDATED")

    def test_tpcds_q40_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q40: Warehouse sales by state validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q40 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(40)
        ref_rows = reference['rows']

        query = load_tpcds_query(40)
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
        print(f"\n✅ TPC-DS Q40 CORRECTNESS VALIDATED")

    def test_tpcds_q41_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q41: Item attributes analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q41 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(41)
        ref_rows = reference['rows']

        query = load_tpcds_query(41)
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
        print(f"\n✅ TPC-DS Q41 CORRECTNESS VALIDATED")

    def test_tpcds_q42_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q42: Store sales by date validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q42 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(42)
        ref_rows = reference['rows']

        query = load_tpcds_query(42)
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
        print(f"\n✅ TPC-DS Q42 CORRECTNESS VALIDATED")

    def test_tpcds_q43_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q43: Store sales by day of week validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q43 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(43)
        ref_rows = reference['rows']

        query = load_tpcds_query(43)
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
        print(f"\n✅ TPC-DS Q43 CORRECTNESS VALIDATED")

    def test_tpcds_q44_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q44: Store sales ranking validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q44 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(44)
        ref_rows = reference['rows']

        query = load_tpcds_query(44)
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
        print(f"\n✅ TPC-DS Q44 CORRECTNESS VALIDATED")

    def test_tpcds_q45_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q45: Web sales by item and zip validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q45 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(45)
        ref_rows = reference['rows']

        query = load_tpcds_query(45)
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
        print(f"\n✅ TPC-DS Q45 CORRECTNESS VALIDATED")

    def test_tpcds_q46_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q46: Customer demographics by location validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q46 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(46)
        ref_rows = reference['rows']

        query = load_tpcds_query(46)
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
        print(f"\n✅ TPC-DS Q46 CORRECTNESS VALIDATED")

    def test_tpcds_q47_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q47: Item brand analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q47 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(47)
        ref_rows = reference['rows']

        query = load_tpcds_query(47)
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
        print(f"\n✅ TPC-DS Q47 CORRECTNESS VALIDATED")

    def test_tpcds_q48_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q48: Store sales quantity and cost validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q48 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(48)
        ref_rows = reference['rows']

        query = load_tpcds_query(48)
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
        print(f"\n✅ TPC-DS Q48 CORRECTNESS VALIDATED")

    def test_tpcds_q49_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q49: Return reasons by channel validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q49 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(49)
        ref_rows = reference['rows']

        query = load_tpcds_query(49)
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
        print(f"\n✅ TPC-DS Q49 CORRECTNESS VALIDATED")

    def test_tpcds_q50_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q50: Store sales by date and channel validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q50 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(50)
        ref_rows = reference['rows']

        query = load_tpcds_query(50)
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
        print(f"\n✅ TPC-DS Q50 CORRECTNESS VALIDATED")
