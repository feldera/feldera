from util import run_pipeline

# https://github.com/feldera/feldera/issues/4448
tables = {
    "lineitem": """
  CREATE TABLE LINEITEM (
          L_ORDERKEY    INTEGER NOT NULL,
          L_PARTKEY     INTEGER NOT NULL,
          L_SUPPKEY     INTEGER NOT NULL,
          L_LINENUMBER  INTEGER NOT NULL,
          L_QUANTITY    DECIMAL(15,2) NOT NULL,
          L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
          L_DISCOUNT    DECIMAL(15,2) NOT NULL,
          L_TAX         DECIMAL(15,2) NOT NULL,
          L_RETURNFLAG  CHAR(1) NOT NULL,
          L_LINESTATUS  CHAR(1) NOT NULL,
          L_SHIPDATE    DATE NOT NULL,
          L_COMMITDATE  DATE NOT NULL,
          L_RECEIPTDATE DATE NOT NULL,
          L_SHIPINSTRUCT CHAR(25) NOT NULL,
          L_SHIPMODE     /*CHAR(10)*/STRING NOT NULL,
          L_COMMENT      VARCHAR(44) NOT NULL
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/lineitem",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
  """,
    "orders": """
  CREATE TABLE ORDERS  (
          O_ORDERKEY       INTEGER NOT NULL,
          O_CUSTKEY        INTEGER NOT NULL,
          O_ORDERSTATUS    CHAR(1) NOT NULL,
          O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
          O_ORDERDATE      DATE NOT NULL,
          O_ORDERPRIORITY  CHAR(15) NOT NULL,
          O_CLERK          CHAR(15) NOT NULL,
          O_SHIPPRIORITY   INTEGER NOT NULL,
          O_COMMENT        VARCHAR(79) NOT NULL
  ) WITH (
    'materialized' = 'true',
     'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/orders",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
  """,
    # https://github.com/feldera/feldera/issues/4448
    "part": """
  CREATE TABLE PART (
          P_PARTKEY     INTEGER NOT NULL,
          P_NAME        VARCHAR(55) NOT NULL,
          P_MFGR        CHAR(25) NOT NULL,
          P_BRAND       CHAR(10) NOT NULL,
          P_TYPE        VARCHAR(25) NOT NULL,
          P_SIZE        INTEGER NOT NULL,
          P_CONTAINER   /*CHAR(10)*/STRING NOT NULL,
          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
          P_COMMENT     VARCHAR(23) NOT NULL
  ) WITH (
    'materialized' = 'true',
     'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/part",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
  );
  """,
    "customer": """
  CREATE TABLE CUSTOMER (
          C_CUSTKEY     INTEGER NOT NULL,
          C_NAME        VARCHAR(25) NOT NULL,
          C_ADDRESS     VARCHAR(40) NOT NULL,
          C_NATIONKEY   INTEGER NOT NULL,
          C_PHONE       CHAR(15) NOT NULL,
          C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
          C_MKTSEGMENT  CHAR(10) NOT NULL,
          C_COMMENT     VARCHAR(117) NOT NULL
  ) WITH (
    'materialized' = 'true',
     'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/customer",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
 );
  """,
    "supplier": """
  CREATE TABLE SUPPLIER (
          S_SUPPKEY     INTEGER NOT NULL,
          S_NAME        CHAR(25) NOT NULL,
          S_ADDRESS     VARCHAR(40) NOT NULL,
          S_NATIONKEY   INTEGER NOT NULL,
          S_PHONE       CHAR(15) NOT NULL,
          S_ACCTBAL     DECIMAL(15,2) NOT NULL,
          S_COMMENT     VARCHAR(101) NOT NULL
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/supplier",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
  """,
    "partsupp": """
  CREATE TABLE PARTSUPP (
          PS_PARTKEY     INTEGER NOT NULL,
          PS_SUPPKEY     INTEGER NOT NULL,
          PS_AVAILQTY    INTEGER NOT NULL,
          PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
          PS_COMMENT     VARCHAR(199) NOT NULL
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/partsupp",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
  """,
    "nation": """
  CREATE TABLE NATION  (
          N_NATIONKEY  INTEGER NOT NULL,
          N_NAME       CHAR(25) NOT NULL,
          N_REGIONKEY  INTEGER NOT NULL,
          N_COMMENT    VARCHAR(152)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/nation",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
  """,
    "region": """
  CREATE TABLE REGION  (
          R_REGIONKEY  INTEGER NOT NULL,
          R_NAME       CHAR(25) NOT NULL,
          R_COMMENT    VARCHAR(152)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
        "name": "delta_table_input",
        "config": {
            "uri": "s3://batchtofeldera/region",
            "aws_skip_signature": "true",
            "aws_region": "ap-southeast-2",
            "mode": "snapshot"
        }
        }
    }]'
);
""",
}

views = {
    "q1": """
    select
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      CAST(ROUND(avg(l_quantity), 1) as  DECIMAL(15, 1)) as avg_qty,
      CAST(ROUND(avg(l_extendedprice), 1) as DECIMAL(15, 1)) as avg_price,
      CAST(ROUND(avg(l_discount), 1) as DECIMAL(15, 1)) as avg_disc,
      count(*) as count_order
  from
      lineitem
  where
      l_shipdate <= date '1998-12-01' - interval '71' DAY
  group by
      l_returnflag,
      l_linestatus
  order by
      l_returnflag,
      l_linestatus
  """,
    "q2": """
  select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 38
    and p_type like '%TIN'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'MIDDLE EAST'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'MIDDLE EAST'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
LIMIT 100""",
    "q3": """
  select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
    c_mktsegment = 'FURNITURE'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-29'
    and l_shipdate > date '1995-03-29'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
LIMIT 10""",
    "q4": """
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1997-07-01'
    and o_orderdate < date '1997-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority""",
    "q5": """
  select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'MIDDLE EAST'
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc""",
    "q6": """
  select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.08 - 0.01 and 0.08 + 0.01
    and l_quantity < 24""",
    "q7": """
  select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = 'ROMANIA' and n2.n_name = 'INDIA')
                or (n1.n_name = 'INDIA' and n2.n_name = 'ROMANIA')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year""",
    "q8": """
  select
    o_year,
    CAST(ROUND(sum(case
        when nation = 'INDIA' then volume
        else 0
    end) / sum(volume)) as DECIMAL(15, 2)) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'ASIA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'PROMO BRUSHED COPPER'
    ) as all_nations
group by
    o_year
order by
    o_year""",
    "q9": """
  select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
            s_suppkey = l_suppkey
            and ps_suppkey = l_suppkey
            and ps_partkey = l_partkey
            and p_partkey = l_partkey
            and o_orderkey = l_orderkey
            and s_nationkey = n_nationkey
            and p_name like '%yellow%'
    ) as profit
group by
    nation,
    o_year
order by
nation,
    o_year desc""",
    "q10": """
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
LIMIT 20""",
    "q11": """
  select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'ARGENTINA'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'ARGENTINA'
        )
order by
    value desc""",
    "q12": """
  select
    l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('FOB', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode""",
    "q13": """
  select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%express%packages%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc""",
    "q14": """
  select
    CAST(ROUND(100.00 * sum(case
        when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
        else 0
    end) / sum(l_extendedprice * (1 - l_discount))) as DECIMAL(15,2)) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1994-03-01'
    and l_shipdate < date '1994-03-01' + interval '1' month""",
    "revenue0": """
  select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1993-01-01'
        and l_shipdate < date '1993-01-01' + interval '3' month
    group by
        l_suppkey
  """,
    "q15": """
  select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey""",
    "q16": """
  select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'SMALL PLATED%'
    and p_size in (19, 17, 16, 23, 10, 4, 38, 11)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size""",
    "q17": """
  select
    CAST(ROUND(sum(l_extendedprice) / 7.0) as DECIMAL(15,2)) as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#52'
    and p_container = 'LG CAN'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    )""",
    "q18": """
  select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity) as sum_quantity
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 313
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
LIMIT 100""",
    "q19": """
  select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#22'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 8 and l_quantity <= 8 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 24 and l_quantity <= 24 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )""",
    "q20": """
  select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'frosted%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1994-01-01'
                    and l_shipdate < date '1994-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'IRAN'
order by
    s_name""",
    "q21": """
  select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    s_name
order by
    numwait desc,
    s_name
LIMIT 100
""",
    "q22": """
  select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2) in
                ('30', '24', '31', '38', '25', '34', '37')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone from 1 for 2) in
                        ('30', '24', '31', '38', '25', '34', '37')
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode""",
}


run_pipeline("tpc-h-test", tables, views)
