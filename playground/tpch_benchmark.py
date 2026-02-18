import marimo

__generated_with = "0.19.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # TPC-H Benchmark: DuckDB vs Thunderduck vs Apache Spark

    Reproduction of the [Coiled TPC-H benchmark](https://docs.coiled.io/blog/tpch.html)
    comparing three engines on all 22 TPC-H queries:

    - **DuckDB** — vanilla DuckDB (in-process, native SQL)
    - **Thunderduck** — DuckDB-backed Spark Connect server (PySpark API)
    - **Apache Spark** — reference Spark Connect server (PySpark API)

    Thunderduck and Spark run queries as SparkSQL via `spark.sql()`.
    DuckDB runs equivalent queries in its native SQL dialect.
    An OS page-cache warmup pass runs first so no engine pays a cold-read penalty.

    Each query runs **N iterations**; the reported time is the **median**.
    The first iteration warms up JIT and caches, so increasing N gives more
    stable results where compute dominates over per-call Spark Connect overhead.

    ---
    """)
    return


@app.cell
def _(mo):
    iterations_slider = mo.ui.slider(
        start=1, stop=20, value=5, step=1,
        label="Iterations per query",
    )
    mo.md(f"""
    ### Configuration

    {iterations_slider}

    **Tip:** At SF=20 with 1-2s queries, use 5-10 iterations so compute dominates
    over Spark Connect per-call overhead (~300-500ms constant cost).
    """)
    return (iterations_slider,)


@app.cell
def _():
    import os
    import time
    from pathlib import Path

    import duckdb
    from pyspark.sql import SparkSession

    from util import (
        TPCH_TABLES,
        find_best_data_dir,
        load_tables,
        load_tables_duckdb,
        warmup_tables,
        warmup_tables_duckdb,
    )

    THUNDERDUCK_URL = os.environ.get("THUNDERDUCK_URL", "sc://localhost:15002")
    SPARK_URL = os.environ.get("SPARK_URL", "sc://localhost:15003")

    PLAYGROUND_DIR = Path(__file__).parent if "__file__" in dir() else Path("playground")
    GENERATED_DATA_DIR = PLAYGROUND_DIR / "data"
    FALLBACK_DATA_DIR = Path(os.environ.get("TPCH_DATA_DIR", "tests/integration/tpch_sf001"))
    return (
        FALLBACK_DATA_DIR,
        GENERATED_DATA_DIR,
        SPARK_URL,
        SparkSession,
        THUNDERDUCK_URL,
        duckdb,
        find_best_data_dir,
        load_tables,
        load_tables_duckdb,
        time,
        warmup_tables,
        warmup_tables_duckdb,
    )


@app.cell
def _(SPARK_URL, SparkSession, THUNDERDUCK_URL):
    spark_td = (
        SparkSession.builder
        .appName("Thunderduck-Benchmark")
        .remote(THUNDERDUCK_URL)
        .getOrCreate()
    )
    spark_ref = (
        SparkSession.builder
        .appName("Spark-Benchmark")
        .remote(SPARK_URL)
        .getOrCreate()
    )
    print(f"Thunderduck: {THUNDERDUCK_URL}")
    print(f"Spark:       {SPARK_URL}")
    return spark_ref, spark_td


@app.cell
def _(
    FALLBACK_DATA_DIR,
    GENERATED_DATA_DIR,
    duckdb,
    find_best_data_dir,
    load_tables,
    load_tables_duckdb,
    spark_ref,
    spark_td,
    warmup_tables,
    warmup_tables_duckdb,
):
    data_dir, scale_factor = find_best_data_dir(GENERATED_DATA_DIR, FALLBACK_DATA_DIR)
    if data_dir is None:
        raise FileNotFoundError("TPC-H data not found")

    # DuckDB in-process connection with fixed resource limits for benchmarks
    duck_conn = duckdb.connect()
    duck_conn.execute("SET threads=2")
    duck_conn.execute("SET memory_limit='4GB'")

    load_tables(spark_td, data_dir)
    load_tables(spark_ref, data_dir)
    load_tables_duckdb(duck_conn, data_dir)
    print(f"Loaded TPC-H tables (SF={scale_factor}) from {data_dir}")

    print("Warming up DuckDB cache...")
    warmup_tables_duckdb(duck_conn)
    print("Warming up Thunderduck cache...")
    warmup_tables(spark_td)
    print("Warming up Spark cache...")
    warmup_tables(spark_ref)
    print("Cache warmup complete.")
    return duck_conn, scale_factor


@app.cell
def _(scale_factor):
    # ---------------------------------------------------------------------------
    # SparkSQL queries Q1-Q22 (used by Thunderduck and Spark via spark.sql())
    # ---------------------------------------------------------------------------

    SPARK_QUERIES = {
        1: """
select
    l_returnflag, l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from lineitem
where l_shipdate <= date('1998-09-02')
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
""",
        2: """
select
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
from part, supplier, partsupp, nation, region
where
    p_partkey = ps_partkey and s_suppkey = ps_suppkey
    and p_size = 15 and p_type like '%BRASS'
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select min(ps_supplycost)
        from partsupp, supplier, nation, region
        where p_partkey = ps_partkey and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by s_acctbal desc, n_name, s_name, p_partkey
limit 100
""",
        3: """
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate, o_shippriority
from customer, orders, lineitem
where
    c_mktsegment = 'BUILDING' and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-15'
    and l_shipdate > date '1995-03-15'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
limit 10
""",
        4: """
select o_orderpriority, count(*) as order_count
from orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select * from lineitem
        where l_orderkey = o_orderkey and l_commitdate < l_receiptdate
    )
group by o_orderpriority
order by o_orderpriority
""",
        5: """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, orders, lineitem, supplier, nation, region
where
    c_custkey = o_custkey and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc
""",
        6: """
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.05 and 0.07
    and l_quantity < 24
""",
        7: """
select supp_nation, cust_nation, l_year, sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation, n2.n_name as cust_nation,
        year(l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    from supplier, lineitem, orders, customer, nation n1, nation n2
    where
        s_suppkey = l_suppkey and o_orderkey = l_orderkey
        and c_custkey = o_custkey and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
        and l_shipdate between date '1995-01-01' and date '1996-12-31'
) as shipping
group by supp_nation, cust_nation, l_year
order by supp_nation, cust_nation, l_year
""",
        8: """
select o_year,
    round(sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume), 2) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by o_year
order by o_year
""",
        9: """
select nation, o_year, round(sum(amount), 2) as sum_profit
from (
    select
        n_name as nation, year(o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from part, supplier, lineitem, partsupp, orders, nation
    where
        s_suppkey = l_suppkey and ps_suppkey = l_suppkey
        and ps_partkey = l_partkey and p_partkey = l_partkey
        and o_orderkey = l_orderkey and s_nationkey = n_nationkey
        and p_name like '%green%'
) as profit
group by nation, o_year
order by nation, o_year desc
""",
        10: """
select
    c_custkey, c_name,
    round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
from customer, orders, lineitem, nation
where
    c_custkey = o_custkey and l_orderkey = o_orderkey
    and o_orderdate >= date '1993-10-01'
    and o_orderdate < date '1993-10-01' + interval '3' month
    and l_returnflag = 'R' and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
limit 20
""",
        11: f"""
select ps_partkey, round(sum(ps_supplycost * ps_availqty), 2) as value
from partsupp, supplier, nation
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey
having sum(ps_supplycost * ps_availqty) > (
    select sum(ps_supplycost * ps_availqty) * {0.0001 / scale_factor}
    from partsupp, supplier, nation
    where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
)
order by value desc
""",
        12: """
select
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
    sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by l_shipmode
order by l_shipmode
""",
        13: """
select c_count, count(*) as custdist
from (
    select c_custkey, count(o_orderkey) as c_count
    from customer left outer join orders on
        c_custkey = o_custkey and o_comment not like '%special%requests%'
    group by c_custkey
) as c_orders
group by c_count
order by custdist desc, c_count desc
""",
        14: """
select
    round(100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end)
        / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
from lineitem, part
where l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month
""",
        # Q15 handled specially — see benchmark runner
        15: None,
        16: """
select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
from partsupp, part
where
    p_partkey = ps_partkey and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select s_suppkey from supplier where s_comment like '%Customer%Complaints%'
    )
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size
""",
        17: """
select round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
from lineitem, part
where
    p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
    and l_quantity < (
        select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey
    )
""",
        18: """
select
    c_name, c_custkey, o_orderkey, o_orderdate,
    o_totalprice, sum(l_quantity) as total_quantity
from customer, orders, lineitem
where
    o_orderkey in (
        select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300
    )
    and c_custkey = o_custkey and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100
""",
        19: """
select round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue
from lineitem, part
where
    (p_partkey = l_partkey and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 11
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
    or (p_partkey = l_partkey and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 20
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
    or (p_partkey = l_partkey and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 30
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
""",
        20: """
select s_name, s_address
from supplier, nation
where
    s_suppkey in (
        select ps_suppkey from partsupp
        where ps_partkey in (select p_partkey from part where p_name like 'forest%')
            and ps_availqty > (
                select 0.5 * sum(l_quantity) from lineitem
                where l_partkey = ps_partkey and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1994-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey and n_name = 'CANADA'
order by s_name
""",
        21: """
select s_name, count(*) as numwait
from supplier, lineitem l1, orders, nation
where
    s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select * from lineitem l2
        where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select * from lineitem l3
        where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'
group by s_name
order by numwait desc, s_name
limit 100
""",
        22: """
select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
from (
    select substring(c_phone, 1, 2) as cntrycode, c_acctbal
    from customer
    where
        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        and c_acctbal > (
            select avg(c_acctbal) from customer
            where c_acctbal > 0.00
                and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        )
        and not exists (select * from orders where o_custkey = c_custkey)
) as custsale
group by cntrycode
order by cntrycode
""",
    }

    # Q15 SparkSQL: temp view DDL + query + cleanup
    SPARK_Q15_DDL = """
create temp view revenue as
    select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from lineitem
    where l_shipdate >= date '1996-01-01'
        and l_shipdate < date '1996-01-01' + interval '3' month
    group by l_suppkey
"""
    SPARK_Q15_QUERY = """
select s_suppkey, s_name, s_address, s_phone, total_revenue
from supplier, revenue
where s_suppkey = supplier_no
    and total_revenue = (select max(total_revenue) from revenue)
order by s_suppkey
"""
    SPARK_Q15_CLEANUP = "drop view if exists revenue"

    return SPARK_Q15_CLEANUP, SPARK_Q15_DDL, SPARK_Q15_QUERY, SPARK_QUERIES


@app.cell
def _(scale_factor):
    # ---------------------------------------------------------------------------
    # DuckDB-native queries Q1-Q22
    # ---------------------------------------------------------------------------
    # Differences from SparkSQL:
    #   - DATE '...' literal instead of date('...')
    #   - INTERVAL 3 MONTH instead of interval '3' month
    #   - year() and extract() work identically in DuckDB

    DUCKDB_QUERIES = {
        1: """
select
    l_returnflag, l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from lineitem
where l_shipdate <= DATE '1998-09-02'
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
""",
        2: """
select
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
from part, supplier, partsupp, nation, region
where
    p_partkey = ps_partkey and s_suppkey = ps_suppkey
    and p_size = 15 and p_type like '%BRASS'
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select min(ps_supplycost)
        from partsupp, supplier, nation, region
        where p_partkey = ps_partkey and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by s_acctbal desc, n_name, s_name, p_partkey
limit 100
""",
        3: """
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate, o_shippriority
from customer, orders, lineitem
where
    c_mktsegment = 'BUILDING' and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < DATE '1995-03-15'
    and l_shipdate > DATE '1995-03-15'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
limit 10
""",
        4: """
select o_orderpriority, count(*) as order_count
from orders
where
    o_orderdate >= DATE '1993-07-01'
    and o_orderdate < DATE '1993-07-01' + INTERVAL 3 MONTH
    and exists (
        select * from lineitem
        where l_orderkey = o_orderkey and l_commitdate < l_receiptdate
    )
group by o_orderpriority
order by o_orderpriority
""",
        5: """
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from customer, orders, lineitem, supplier, nation, region
where
    c_custkey = o_custkey and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey
    and r_name = 'ASIA'
    and o_orderdate >= DATE '1994-01-01'
    and o_orderdate < DATE '1994-01-01' + INTERVAL 1 YEAR
group by n_name
order by revenue desc
""",
        6: """
select sum(l_extendedprice * l_discount) as revenue
from lineitem
where
    l_shipdate >= DATE '1994-01-01'
    and l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
    and l_discount between 0.05 and 0.07
    and l_quantity < 24
""",
        7: """
select supp_nation, cust_nation, l_year, sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation, n2.n_name as cust_nation,
        year(l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    from supplier, lineitem, orders, customer, nation n1, nation n2
    where
        s_suppkey = l_suppkey and o_orderkey = l_orderkey
        and c_custkey = o_custkey and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
            or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
        and l_shipdate between DATE '1995-01-01' and DATE '1996-12-31'
) as shipping
group by supp_nation, cust_nation, l_year
order by supp_nation, cust_nation, l_year
""",
        8: """
select o_year,
    round(sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume), 2) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) as volume,
        n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey
        and o_orderdate between DATE '1995-01-01' and DATE '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
group by o_year
order by o_year
""",
        9: """
select nation, o_year, round(sum(amount), 2) as sum_profit
from (
    select
        n_name as nation, year(o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from part, supplier, lineitem, partsupp, orders, nation
    where
        s_suppkey = l_suppkey and ps_suppkey = l_suppkey
        and ps_partkey = l_partkey and p_partkey = l_partkey
        and o_orderkey = l_orderkey and s_nationkey = n_nationkey
        and p_name like '%green%'
) as profit
group by nation, o_year
order by nation, o_year desc
""",
        10: """
select
    c_custkey, c_name,
    round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
from customer, orders, lineitem, nation
where
    c_custkey = o_custkey and l_orderkey = o_orderkey
    and o_orderdate >= DATE '1993-10-01'
    and o_orderdate < DATE '1993-10-01' + INTERVAL 3 MONTH
    and l_returnflag = 'R' and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
limit 20
""",
        11: f"""
select ps_partkey, round(sum(ps_supplycost * ps_availqty), 2) as value
from partsupp, supplier, nation
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey
having sum(ps_supplycost * ps_availqty) > (
    select sum(ps_supplycost * ps_availqty) * {0.0001 / scale_factor}
    from partsupp, supplier, nation
    where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
)
order by value desc
""",
        12: """
select
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
    sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= DATE '1994-01-01'
    and l_receiptdate < DATE '1994-01-01' + INTERVAL 1 YEAR
group by l_shipmode
order by l_shipmode
""",
        13: """
select c_count, count(*) as custdist
from (
    select c_custkey, count(o_orderkey) as c_count
    from customer left outer join orders on
        c_custkey = o_custkey and o_comment not like '%special%requests%'
    group by c_custkey
) as c_orders
group by c_count
order by custdist desc, c_count desc
""",
        14: """
select
    round(100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end)
        / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
from lineitem, part
where l_partkey = p_partkey
    and l_shipdate >= DATE '1995-09-01'
    and l_shipdate < DATE '1995-09-01' + INTERVAL 1 MONTH
""",
        # Q15 handled specially — see benchmark runner
        15: None,
        16: """
select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
from partsupp, part
where
    p_partkey = ps_partkey and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select s_suppkey from supplier where s_comment like '%Customer%Complaints%'
    )
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size
""",
        17: """
select round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
from lineitem, part
where
    p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
    and l_quantity < (
        select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey
    )
""",
        18: """
select
    c_name, c_custkey, o_orderkey, o_orderdate,
    o_totalprice, sum(l_quantity) as total_quantity
from customer, orders, lineitem
where
    o_orderkey in (
        select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300
    )
    and c_custkey = o_custkey and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate
limit 100
""",
        19: """
select round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue
from lineitem, part
where
    (p_partkey = l_partkey and p_brand = 'Brand#12'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 11
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
    or (p_partkey = l_partkey and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 20
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
    or (p_partkey = l_partkey and p_brand = 'Brand#34'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 30
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON')
""",
        20: """
select s_name, s_address
from supplier, nation
where
    s_suppkey in (
        select ps_suppkey from partsupp
        where ps_partkey in (select p_partkey from part where p_name like 'forest%')
            and ps_availqty > (
                select 0.5 * sum(l_quantity) from lineitem
                where l_partkey = ps_partkey and l_suppkey = ps_suppkey
                    and l_shipdate >= DATE '1994-01-01'
                    and l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
            )
    )
    and s_nationkey = n_nationkey and n_name = 'CANADA'
order by s_name
""",
        21: """
select s_name, count(*) as numwait
from supplier, lineitem l1, orders, nation
where
    s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select * from lineitem l2
        where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select * from lineitem l3
        where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey and n_name = 'SAUDI ARABIA'
group by s_name
order by numwait desc, s_name
limit 100
""",
        22: """
select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
from (
    select substring(c_phone, 1, 2) as cntrycode, c_acctbal
    from customer
    where
        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        and c_acctbal > (
            select avg(c_acctbal) from customer
            where c_acctbal > 0.00
                and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        )
        and not exists (select * from orders where o_custkey = c_custkey)
) as custsale
group by cntrycode
order by cntrycode
""",
    }

    # Q15 DuckDB: temp view DDL + query + cleanup
    DUCKDB_Q15_DDL = """
create or replace temp view revenue as
    select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from lineitem
    where l_shipdate >= DATE '1996-01-01'
        and l_shipdate < DATE '1996-01-01' + INTERVAL 3 MONTH
    group by l_suppkey
"""
    DUCKDB_Q15_QUERY = """
select s_suppkey, s_name, s_address, s_phone, total_revenue
from supplier, revenue
where s_suppkey = supplier_no
    and total_revenue = (select max(total_revenue) from revenue)
order by s_suppkey
"""
    DUCKDB_Q15_CLEANUP = "drop view if exists revenue"

    return DUCKDB_Q15_CLEANUP, DUCKDB_Q15_DDL, DUCKDB_Q15_QUERY, DUCKDB_QUERIES


@app.cell
def _(
    DUCKDB_Q15_CLEANUP,
    DUCKDB_Q15_DDL,
    DUCKDB_Q15_QUERY,
    DUCKDB_QUERIES,
    SPARK_Q15_CLEANUP,
    SPARK_Q15_DDL,
    SPARK_Q15_QUERY,
    SPARK_QUERIES,
    duck_conn,
    iterations_slider,
    mo,
    spark_ref,
    spark_td,
    time,
):
    import statistics

    N = iterations_slider.value

    # ---------------------------------------------------------------------------
    # Benchmark runner: execute each query N times on all three engines,
    # report median elapsed time.
    # ---------------------------------------------------------------------------

    def _run_spark_q15(spark):
        start = time.time()
        spark.sql(SPARK_Q15_DDL).collect()
        result = spark.sql(SPARK_Q15_QUERY).toPandas()
        elapsed = time.time() - start
        try:
            spark.sql(SPARK_Q15_CLEANUP).collect()
        except Exception:
            pass
        return result, elapsed

    def _run_spark(spark, sql):
        start = time.time()
        result = spark.sql(sql).toPandas()
        elapsed = time.time() - start
        return result, elapsed

    def _run_duckdb_q15(conn):
        start = time.time()
        conn.execute(DUCKDB_Q15_DDL)
        result = conn.execute(DUCKDB_Q15_QUERY).fetchdf()
        elapsed = time.time() - start
        try:
            conn.execute(DUCKDB_Q15_CLEANUP)
        except Exception:
            pass
        return result, elapsed

    def _run_duckdb(conn, sql):
        start = time.time()
        result = conn.execute(sql).fetchdf()
        elapsed = time.time() - start
        return result, elapsed

    def _bench(fn, *args):
        """Run fn N times, return (row_count, median_seconds, status) or failure."""
        timings = []
        row_count = None
        for _ in range(N):
            try:
                result, elapsed = fn(*args)
                timings.append(elapsed)
                row_count = len(result)
            except Exception as e:
                return None, None, str(e)[:80]
        return row_count, statistics.median(timings), "OK"

    results = []
    for qnum in sorted(SPARK_QUERIES.keys()):
        label = f"Q{qnum}"
        print(f"Running {label} ({N}x)...", end=" ", flush=True)

        # --- DuckDB ---
        if qnum == 15:
            db_rows, db_t, db_st = _bench(_run_duckdb_q15, duck_conn)
        else:
            db_rows, db_t, db_st = _bench(_run_duckdb, duck_conn, DUCKDB_QUERIES[qnum])

        # --- Thunderduck ---
        if qnum == 15:
            td_rows, td_t, td_st = _bench(_run_spark_q15, spark_td)
        else:
            td_rows, td_t, td_st = _bench(_run_spark, spark_td, SPARK_QUERIES[qnum])

        # --- Spark ---
        if qnum == 15:
            sp_rows, sp_t, sp_st = _bench(_run_spark_q15, spark_ref)
        else:
            sp_rows, sp_t, sp_st = _bench(_run_spark, spark_ref, SPARK_QUERIES[qnum])

        results.append({
            "query": label,
            "db_ms": round(db_t * 1000, 1) if db_t is not None else None,
            "td_ms": round(td_t * 1000, 1) if td_t is not None else None,
            "spark_ms": round(sp_t * 1000, 1) if sp_t is not None else None,
            "db_rows": db_rows,
            "td_rows": td_rows,
            "spark_rows": sp_rows,
            "db_status": db_st,
            "td_status": td_st,
            "spark_status": sp_st,
        })

        parts = []
        parts.append(f"DB={'FAIL' if db_t is None else f'{db_t*1000:.0f}ms'}")
        parts.append(f"TD={'FAIL' if td_t is None else f'{td_t*1000:.0f}ms'}")
        parts.append(f"Spark={'FAIL' if sp_t is None else f'{sp_t*1000:.0f}ms'}")
        if td_t and sp_t and td_t > 0:
            parts.append(f"TD/Spark={sp_t/td_t:.1f}x")
        print("  ".join(parts))

    # ---- Build markdown results table ----
    header = (
        "| Query | DuckDB | Thunderduck | Spark | TD vs Spark | TD Overhead |\n"
        "|-------|--------|-------------|-------|-------------|-------------|\n"
    )
    rows = []
    td_fail = []
    for r in results:
        db = f"{r['db_ms']}ms" if r['db_ms'] is not None else "FAIL"
        td = f"{r['td_ms']}ms" if r['td_ms'] is not None else "FAIL"
        sp = f"{r['spark_ms']}ms" if r['spark_ms'] is not None else "FAIL"

        # TD vs Spark speedup
        if r['td_ms'] is not None and r['spark_ms'] is not None and r['td_ms'] > 0:
            td_sp = f"**{r['spark_ms'] / r['td_ms']:.1f}x**"
        else:
            td_sp = "N/A"

        # TD overhead vs raw DuckDB
        if r['td_ms'] is not None and r['db_ms'] is not None and r['db_ms'] > 0:
            overhead = f"{r['td_ms'] / r['db_ms']:.1f}x"
        else:
            overhead = "N/A"

        if r['td_status'] != "OK":
            td_fail.append(r['query'])

        rows.append(f"| {r['query']} | {db} | {td} | {sp} | {td_sp} | {overhead} |")

    # ---- Summary stats (queries where all three succeeded) ----
    ok = [r for r in results if r['db_ms'] is not None and r['td_ms'] is not None and r['spark_ms'] is not None]
    nq = len(ok)
    if nq > 0:
        tot_db = sum(r['db_ms'] for r in ok)
        tot_td = sum(r['td_ms'] for r in ok)
        tot_sp = sum(r['spark_ms'] for r in ok)
        geo_td_sp = 1.0
        geo_overhead = 1.0
        for r in ok:
            geo_td_sp *= r['spark_ms'] / r['td_ms'] if r['td_ms'] > 0 else 1.0
            geo_overhead *= r['td_ms'] / r['db_ms'] if r['db_ms'] > 0 else 1.0
        geo_td_sp = geo_td_sp ** (1.0 / nq)
        geo_overhead = geo_overhead ** (1.0 / nq)
        total_td_sp = f"{tot_sp / tot_td:.2f}" if tot_td > 0 else "N/A"
        total_overhead = f"{tot_td / tot_db:.2f}" if tot_db > 0 else "N/A"
    else:
        tot_db = tot_td = tot_sp = 0
        geo_td_sp = geo_overhead = 0
        total_td_sp = total_overhead = "N/A"

    passed = sum(1 for r in results if r['td_status'] == "OK" and r['spark_status'] == "OK" and r['db_status'] == "OK")
    rows_text = "\n".join(rows)

    summary_md = f"""
## Benchmark Results ({N} iterations, median time)

{passed}/{len(results)} queries completed on all three engines.

**TD vs Spark** = how much faster Thunderduck is than Spark (higher is better).
**TD Overhead** = Thunderduck time / DuckDB time — the cost of the Spark Connect layer (lower is better).

{header}{rows_text}
| **Total** | **{tot_db:.0f}ms** | **{tot_td:.0f}ms** | **{tot_sp:.0f}ms** | **{total_td_sp}x** | **{total_overhead}x** |
| **Geo Mean** | | | | **{geo_td_sp:.2f}x** | **{geo_overhead:.2f}x** |
"""

    if td_fail:
        fail_list = ", ".join(td_fail)
        summary_md += f"\n**Failed on Thunderduck:** {fail_list}\n"

    mo.md(summary_md)
    return


if __name__ == "__main__":
    app.run()
