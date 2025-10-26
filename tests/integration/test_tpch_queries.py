"""
TPC-H Integration Tests for Thunderduck Spark Connect

Tests TPC-H queries using both SQL and DataFrame API approaches.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count


class TestTPCHQuery1:
    """TPC-H Q1: Pricing Summary Report

    Tests: scan, filter, aggregate, sort operations
    Complexity: Simple
    Expected Results: 4 rows (one per l_returnflag/l_linestatus combination)
    """

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_q1_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q1 via SQL"""
        # Load query from file
        sql = load_tpch_query(1)

        # Execute query
        result = spark.sql(sql)

        # Validate results
        rows = result.collect()

        # Should return 4 rows for SF=0.01
        validator.validate_row_count(result, 4)

        # Validate schema
        expected_columns = [
            'l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price',
            'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price',
            'avg_disc', 'count_order'
        ]
        validator.validate_schema(result, expected_columns)

        # Validate we have the expected flag/status combinations
        flag_status_pairs = [(row['l_returnflag'], row['l_linestatus']) for row in rows]
        assert len(flag_status_pairs) == 4, f"Expected 4 flag/status pairs, got {len(flag_status_pairs)}"

        # Check that aggregates are positive
        for row in rows:
            assert row['sum_qty'] > 0, "sum_qty should be positive"
            assert row['sum_base_price'] > 0, "sum_base_price should be positive"
            assert row['count_order'] > 0, "count_order should be positive"

        print(f"\n✓ TPC-H Q1 (SQL) passed: {len(rows)} rows returned")

    @pytest.mark.tpch
    @pytest.mark.dataframe
    @pytest.mark.timeout(60)
    def test_q1_dataframe_api(self, spark, tpch_tables, tpch_data_dir, validator):
        """Test TPC-H Q1 via DataFrame API"""
        # Load lineitem table
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        # Build query using DataFrame API
        result = (lineitem
            .filter(col("l_shipdate") <= "1998-12-01")
            .groupBy("l_returnflag", "l_linestatus")
            .agg(
                _sum("l_quantity").alias("sum_qty"),
                _sum("l_extendedprice").alias("sum_base_price"),
                _sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("sum_disc_price"),
                _sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).alias("sum_charge"),
                _avg("l_quantity").alias("avg_qty"),
                _avg("l_extendedprice").alias("avg_price"),
                _avg("l_discount").alias("avg_disc"),
                _count("*").alias("count_order")
            )
            .orderBy("l_returnflag", "l_linestatus")
        )

        # Collect results
        rows = result.collect()

        # Validate results
        validator.validate_row_count(result, 4)

        # Check that aggregates are positive
        for row in rows:
            assert row['sum_qty'] > 0, "sum_qty should be positive"
            assert row['sum_base_price'] > 0, "sum_base_price should be positive"
            assert row['count_order'] > 0, "count_order should be positive"

        print(f"\n✓ TPC-H Q1 (DataFrame API) passed: {len(rows)} rows returned")

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_q1_sql_vs_dataframe(self, spark, tpch_tables, load_tpch_query, tpch_data_dir):
        """Verify SQL and DataFrame API produce identical results for Q1"""
        # SQL result
        sql = load_tpch_query(1)
        sql_result = spark.sql(sql)

        # DataFrame API result
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        df_result = (lineitem
            .filter(col("l_shipdate") <= "1998-12-01")
            .groupBy("l_returnflag", "l_linestatus")
            .agg(
                _sum("l_quantity").alias("sum_qty"),
                _sum("l_extendedprice").alias("sum_base_price"),
                _sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("sum_disc_price"),
                _sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).alias("sum_charge"),
                _avg("l_quantity").alias("avg_qty"),
                _avg("l_extendedprice").alias("avg_price"),
                _avg("l_discount").alias("avg_disc"),
                _count("*").alias("count_order")
            )
            .orderBy("l_returnflag", "l_linestatus")
        )

        # Compare results
        sql_rows = sql_result.collect()
        df_rows = df_result.collect()

        assert len(sql_rows) == len(df_rows), \
            f"Row count mismatch: SQL={len(sql_rows)}, DataFrame={len(df_rows)}"

        # Compare each row
        for i, (sql_row, df_row) in enumerate(zip(sql_rows, df_rows)):
            assert sql_row['l_returnflag'] == df_row['l_returnflag'], \
                f"Row {i}: l_returnflag mismatch"
            assert sql_row['l_linestatus'] == df_row['l_linestatus'], \
                f"Row {i}: l_linestatus mismatch"

            # Compare aggregates with small epsilon for floating point
            assert abs(sql_row['sum_qty'] - df_row['sum_qty']) < 0.01, \
                f"Row {i}: sum_qty mismatch"
            assert abs(sql_row['sum_base_price'] - df_row['sum_base_price']) < 0.01, \
                f"Row {i}: sum_base_price mismatch"

        print("\n✓ TPC-H Q1: SQL and DataFrame API produce identical results")


class TestTPCHQuery3:
    """TPC-H Q3: Shipping Priority

    Tests: 3-way joins, filtering, aggregation, top-N
    Complexity: Moderate
    """

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_q3_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q3 via SQL"""
        # Load query from file
        sql = load_tpch_query(3)

        # Execute query
        result = spark.sql(sql)

        # Collect results
        rows = result.collect()

        # Should return up to 10 rows (LIMIT 10)
        assert len(rows) <= 10, f"Expected at most 10 rows, got {len(rows)}"

        # Validate schema
        expected_columns = ['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']
        validator.validate_schema(result, expected_columns)

        # Validate results are ordered by revenue DESC
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                assert rows[i]['revenue'] >= rows[i + 1]['revenue'], \
                    f"Results not properly ordered by revenue DESC"

        # Check that revenue is positive
        for row in rows:
            assert row['revenue'] > 0, "revenue should be positive"

        print(f"\n✓ TPC-H Q3 (SQL) passed: {len(rows)} rows returned")

    @pytest.mark.tpch
    @pytest.mark.dataframe
    @pytest.mark.timeout(60)
    def test_q3_dataframe_api(self, spark, tpch_tables, tpch_data_dir, validator):
        """Test TPC-H Q3 via DataFrame API"""
        # Load tables
        customer = spark.read.parquet(str(tpch_data_dir / "customer.parquet"))
        orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        # Build query using DataFrame API
        result = (customer
            .filter(col("c_mktsegment") == "BUILDING")
            .join(orders, col("c_custkey") == col("o_custkey"))
            .filter(col("o_orderdate") < "1995-03-15")
            .join(lineitem, col("l_orderkey") == col("o_orderkey"))
            .filter(col("l_shipdate") > "1995-03-15")
            .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
            .agg(
                _sum(col("l_extendedprice") * (1 - col("l_discount"))).alias("revenue")
            )
            .orderBy(col("revenue").desc(), col("o_orderdate"))
            .limit(10)
        )

        # Collect results
        rows = result.collect()

        # Should return up to 10 rows
        assert len(rows) <= 10, f"Expected at most 10 rows, got {len(rows)}"

        # Validate results are ordered by revenue DESC
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                assert rows[i]['revenue'] >= rows[i + 1]['revenue'], \
                    f"Results not properly ordered by revenue DESC"

        print(f"\n✓ TPC-H Q3 (DataFrame API) passed: {len(rows)} rows returned")


class TestTPCHQuery6:
    """TPC-H Q6: Forecasting Revenue Change

    Tests: scan, complex filters, aggregate
    Complexity: Simple
    Expected Results: 1 row (single aggregate)
    """

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_q6_sql(self, spark, tpch_tables, load_tpch_query, validator):
        """Test TPC-H Q6 via SQL"""
        # Load query from file
        sql = load_tpch_query(6)

        # Execute query
        result = spark.sql(sql)

        # Collect results
        rows = result.collect()

        # Should return exactly 1 row (single aggregate)
        validator.validate_row_count(result, 1)

        # Validate schema
        expected_columns = ['revenue']
        validator.validate_schema(result, expected_columns)

        # Validate revenue is positive
        revenue = rows[0]['revenue']
        assert revenue > 0, f"Expected positive revenue, got {revenue}"

        print(f"\n✓ TPC-H Q6 (SQL) passed: revenue={revenue:.2f}")

    @pytest.mark.tpch
    @pytest.mark.dataframe
    @pytest.mark.timeout(60)
    def test_q6_dataframe_api(self, spark, tpch_tables, tpch_data_dir, validator):
        """Test TPC-H Q6 via DataFrame API"""
        # Load lineitem table
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        # Build query using DataFrame API
        result = (lineitem
            .filter(
                (col("l_shipdate") >= "1994-01-01") &
                (col("l_shipdate") < "1995-01-01") &
                (col("l_discount") >= 0.05) &
                (col("l_discount") <= 0.07) &
                (col("l_quantity") < 24)
            )
            .agg(
                _sum(col("l_extendedprice") * col("l_discount")).alias("revenue")
            )
        )

        # Collect results
        rows = result.collect()

        # Should return exactly 1 row
        validator.validate_row_count(result, 1)

        # Validate revenue is positive
        revenue = rows[0]['revenue']
        assert revenue > 0, f"Expected positive revenue, got {revenue}"

        print(f"\n✓ TPC-H Q6 (DataFrame API) passed: revenue={revenue:.2f}")

    @pytest.mark.tpch
    @pytest.mark.timeout(60)
    def test_q6_sql_vs_dataframe(self, spark, tpch_tables, load_tpch_query, tpch_data_dir):
        """Verify SQL and DataFrame API produce identical results for Q6"""
        # SQL result
        sql = load_tpch_query(6)
        sql_result = spark.sql(sql)

        # DataFrame API result
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        df_result = (lineitem
            .filter(
                (col("l_shipdate") >= "1994-01-01") &
                (col("l_shipdate") < "1995-01-01") &
                (col("l_discount") >= 0.05) &
                (col("l_discount") <= 0.07) &
                (col("l_quantity") < 24)
            )
            .agg(
                _sum(col("l_extendedprice") * col("l_discount")).alias("revenue")
            )
        )

        # Compare results
        sql_revenue = sql_result.collect()[0]['revenue']
        df_revenue = df_result.collect()[0]['revenue']

        # Compare with small epsilon for floating point
        assert abs(sql_revenue - df_revenue) < 0.01, \
            f"Revenue mismatch: SQL={sql_revenue}, DataFrame={df_revenue}"

        print(f"\n✓ TPC-H Q6: SQL and DataFrame API produce identical results (revenue={sql_revenue:.2f})")


class TestBasicDataFrameOperations:
    """Basic DataFrame API operations to ensure fundamentals work"""

    @pytest.mark.timeout(30)
    def test_read_parquet(self, spark, tpch_data_dir):
        """Test reading Parquet files"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        count = lineitem.count()
        assert count > 0, "lineitem table should not be empty"
        print(f"\n✓ Read Parquet: lineitem has {count} rows")

    @pytest.mark.timeout(30)
    def test_simple_filter(self, spark, tpch_data_dir):
        """Test simple filter operation"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        filtered = lineitem.filter(col("l_quantity") > 40)
        count = filtered.count()
        assert count > 0, "Filtered result should not be empty"
        print(f"\n✓ Simple Filter: {count} rows with l_quantity > 40")

    @pytest.mark.timeout(30)
    def test_simple_select(self, spark, tpch_data_dir):
        """Test simple select/project operation"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        selected = lineitem.select("l_orderkey", "l_quantity", "l_extendedprice")
        rows = selected.limit(5).collect()
        assert len(rows) > 0, "Selected result should not be empty"
        print(f"\n✓ Simple Select: retrieved {len(rows)} rows")

    @pytest.mark.timeout(30)
    def test_simple_aggregate(self, spark, tpch_data_dir):
        """Test simple aggregation"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        result = lineitem.agg(_sum("l_quantity").alias("total_quantity"))
        total = result.collect()[0]['total_quantity']
        assert total > 0, "Total quantity should be positive"
        print(f"\n✓ Simple Aggregate: total_quantity={total:.2f}")

    @pytest.mark.timeout(30)
    def test_simple_groupby(self, spark, tpch_data_dir):
        """Test simple group by operation"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        result = lineitem.groupBy("l_returnflag").agg(_count("*").alias("count"))
        rows = result.collect()
        assert len(rows) > 0, "Group by result should not be empty"
        print(f"\n✓ Simple GroupBy: {len(rows)} distinct l_returnflag values")

    @pytest.mark.timeout(30)
    def test_simple_orderby(self, spark, tpch_data_dir):
        """Test simple order by operation"""
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))
        result = lineitem.orderBy(col("l_quantity").desc()).limit(10)
        rows = result.collect()

        # Verify descending order
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                assert rows[i]['l_quantity'] >= rows[i + 1]['l_quantity'], \
                    "Results not properly ordered DESC"

        print(f"\n✓ Simple OrderBy: retrieved {len(rows)} rows in DESC order")

    @pytest.mark.timeout(30)
    def test_simple_join(self, spark, tpch_data_dir):
        """Test simple join operation"""
        orders = spark.read.parquet(str(tpch_data_dir / "orders.parquet"))
        lineitem = spark.read.parquet(str(tpch_data_dir / "lineitem.parquet"))

        result = orders.join(lineitem, col("o_orderkey") == col("l_orderkey"))
        count = result.count()
        assert count > 0, "Join result should not be empty"
        print(f"\n✓ Simple Join: orders ⋈ lineitem = {count} rows")
