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

    def test_tpcds_q10_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q10: Customer segment analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q10 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(10)
        ref_rows = reference['rows']

        query = load_tpcds_query(10)
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
        print(f"\n✅ TPC-DS Q10 CORRECTNESS VALIDATED")

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
        """TPC-DS Q16: Order counts by ship mode validation (ORDER BY with ties)"""
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

        # Use order-independent comparison (ORDER BY has ties)
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
        """TPC-DS Q24a: Multi-channel customer analysis validation (part 1) (ORDER BY with ties)"""
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

        # Use order-independent comparison (ORDER BY has ties)
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
        """TPC-DS Q34: Customer preferences validation (ORDER BY with ties)"""
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

        # Use order-independent comparison (ORDER BY has ties)
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
        """TPC-DS Q38: Customer count by demographics validation (ORDER BY with ties)"""
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

        # Use order-independent comparison (ORDER BY has ties)
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

    def test_tpcds_q51_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q51: Web sales by date validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q51 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(51)
        ref_rows = reference['rows']

        query = load_tpcds_query(51)
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
        print(f"\n✅ TPC-DS Q51 CORRECTNESS VALIDATED")

    def test_tpcds_q52_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q52: Item brand analysis by month validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q52 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(52)
        ref_rows = reference['rows']

        query = load_tpcds_query(52)
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
        print(f"\n✅ TPC-DS Q52 CORRECTNESS VALIDATED")

    def test_tpcds_q53_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q53: Manufacturer sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q53 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(53)
        ref_rows = reference['rows']

        query = load_tpcds_query(53)
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
        print(f"\n✅ TPC-DS Q53 CORRECTNESS VALIDATED")

    def test_tpcds_q54_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q54: Customer segmentation by category validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q54 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(54)
        ref_rows = reference['rows']

        query = load_tpcds_query(54)
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
        print(f"\n✅ TPC-DS Q54 CORRECTNESS VALIDATED")

    def test_tpcds_q55_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q55: Item brand sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q55 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(55)
        ref_rows = reference['rows']

        query = load_tpcds_query(55)
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
        print(f"\n✅ TPC-DS Q55 CORRECTNESS VALIDATED")

    def test_tpcds_q56_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q56: Multi-channel sales by item validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q56 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(56)
        ref_rows = reference['rows']

        query = load_tpcds_query(56)
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
        print(f"\n✅ TPC-DS Q56 CORRECTNESS VALIDATED")

    def test_tpcds_q57_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q57: Catalog sales by call center validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q57 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(57)
        ref_rows = reference['rows']

        query = load_tpcds_query(57)
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
        print(f"\n✅ TPC-DS Q57 CORRECTNESS VALIDATED")

    def test_tpcds_q58_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q58: Sales by item and date validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q58 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(58)
        ref_rows = reference['rows']

        query = load_tpcds_query(58)
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
        print(f"\n✅ TPC-DS Q58 CORRECTNESS VALIDATED")

    def test_tpcds_q59_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q59: Week-over-week sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q59 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(59)
        ref_rows = reference['rows']

        query = load_tpcds_query(59)
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
        print(f"\n✅ TPC-DS Q59 CORRECTNESS VALIDATED")

    def test_tpcds_q60_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q60: Multi-channel item analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q60 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(60)
        ref_rows = reference['rows']

        query = load_tpcds_query(60)
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
        print(f"\n✅ TPC-DS Q60 CORRECTNESS VALIDATED")

    def test_tpcds_q61_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q61: Promotional sales analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q61 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(61)
        ref_rows = reference['rows']

        query = load_tpcds_query(61)
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
        print(f"\n✅ TPC-DS Q61 CORRECTNESS VALIDATED")

    def test_tpcds_q62_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q62: Web sales ship delay analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q62 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(62)
        ref_rows = reference['rows']

        query = load_tpcds_query(62)
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
        print(f"\n✅ TPC-DS Q62 CORRECTNESS VALIDATED")

    def test_tpcds_q63_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q63: Item brand cross-channel analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q63 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(63)
        ref_rows = reference['rows']

        query = load_tpcds_query(63)
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
        print(f"\n✅ TPC-DS Q63 CORRECTNESS VALIDATED")

    def test_tpcds_q64_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q64: Cross-item catalog revenue analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q64 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(64)
        ref_rows = reference['rows']

        query = load_tpcds_query(64)
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
        print(f"\n✅ TPC-DS Q64 CORRECTNESS VALIDATED")

    def test_tpcds_q65_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q65: Store sales by quarter and channel validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q65 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(65)
        ref_rows = reference['rows']

        query = load_tpcds_query(65)
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
        print(f"\n✅ TPC-DS Q65 CORRECTNESS VALIDATED")

    def test_tpcds_q66_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q66: Multi-channel time-based analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q66 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(66)
        ref_rows = reference['rows']

        query = load_tpcds_query(66)
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
        print(f"\n✅ TPC-DS Q66 CORRECTNESS VALIDATED")

    def test_tpcds_q67_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q67: Store product profitability analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q67 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(67)
        ref_rows = reference['rows']

        query = load_tpcds_query(67)
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
        print(f"\n✅ TPC-DS Q67 CORRECTNESS VALIDATED")

    def test_tpcds_q68_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q68: Store customer demographics analysis validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q68 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(68)
        ref_rows = reference['rows']

        query = load_tpcds_query(68)
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
        print(f"\n✅ TPC-DS Q68 CORRECTNESS VALIDATED")

    def test_tpcds_q70_correctness(self, spark, tpcds_tables, load_tpcds_query):
        """TPC-DS Q70: Store monthly sales ranking validation"""
        print("\n" + "=" * 80)
        print("TPC-DS Q70 CORRECTNESS TEST")
        print("=" * 80)

        reference = self.load_reference(70)
        ref_rows = reference['rows']

        query = load_tpcds_query(70)
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
        print(f"\n✅ TPC-DS Q70 CORRECTNESS VALIDATED")
