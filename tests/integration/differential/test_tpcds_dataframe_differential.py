"""
TPC-DS DataFrame API Differential Tests

Tests 33 TPC-DS queries implemented using pure PySpark DataFrame API,
comparing Thunderduck against Apache Spark 4.0.1.

These queries use only DataFrame operations (no SQL-specific features like
CTEs, ROLLUP/CUBE, EXISTS, INTERSECT/EXCEPT, or correlated subqueries).

Compatible queries: Q3, Q7, Q9, Q12-Q13, Q15, Q17, Q19-Q20, Q25-Q26, Q29,
Q32, Q37, Q40-Q43, Q45, Q48, Q50, Q52, Q55, Q62, Q71, Q82, Q84-Q85,
Q91-Q92, Q96, Q98-Q99

Note: Q72 excluded due to Spark OOM in differential testing environment.
"""

import pytest
import sys
from pathlib import Path

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent / "utils"))
from dataframe_diff import assert_dataframes_equal

# Import query implementations
sys.path.insert(0, str(Path(__file__).parent.parent / "tpcds_dataframe"))
from tpcds_dataframe_queries import COMPATIBLE_QUERIES, get_query_implementation


@pytest.mark.differential
@pytest.mark.timeout(300)  # 5 minutes - some TPC-DS queries are slow
@pytest.mark.parametrize("query_num", COMPATIBLE_QUERIES)
def test_tpcds_dataframe_query(
    spark_reference,
    spark_thunderduck,
    tpcds_tables_reference,
    tpcds_tables_thunderduck,
    query_num
):
    """
    Test TPC-DS DataFrame API query against Spark reference.

    Each query is implemented using pure PySpark DataFrame API and
    executed on both Apache Spark (reference) and Thunderduck (test).
    Results are compared row-by-row with floating-point tolerance.

    The tpcds_tables_* fixtures ensure tables are registered in each session.
    """
    query_fn = get_query_implementation(query_num)
    assert query_fn is not None, f"No implementation for Q{query_num}"

    # Execute on Spark reference
    ref_result = query_fn(spark_reference)

    # Execute on Thunderduck
    test_result = query_fn(spark_thunderduck)

    # Compare results
    assert_dataframes_equal(
        ref_result,
        test_result,
        f"tpcds_q{query_num}_dataframe",
        epsilon=1e-6
    )
