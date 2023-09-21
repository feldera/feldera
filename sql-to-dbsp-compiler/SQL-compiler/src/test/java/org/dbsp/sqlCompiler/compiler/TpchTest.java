package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.IOException;

public class TpchTest extends BaseSQLTests {
    static final String tpch = "CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,\n" +
            "                            N_NAME       CHAR(25) NOT NULL,\n" +
            "                            N_REGIONKEY  INTEGER NOT NULL,\n" +
            "                            N_COMMENT    VARCHAR(152));\n" +
            "\n" +
            "CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,\n" +
            "                            R_NAME       CHAR(25) NOT NULL,\n" +
            "                            R_COMMENT    VARCHAR(152));\n" +
            "\n" +
            "CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,\n" +
            "                          P_NAME        VARCHAR(55) NOT NULL,\n" +
            "                          P_MFGR        CHAR(25) NOT NULL,\n" +
            "                          P_BRAND       CHAR(10) NOT NULL,\n" +
            "                          P_TYPE        VARCHAR(25) NOT NULL,\n" +
            "                          P_SIZE        INTEGER NOT NULL,\n" +
            "                          P_CONTAINER   CHAR(10) NOT NULL,\n" +
            "                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,\n" +
            "                          P_COMMENT     VARCHAR(23) NOT NULL );\n" +
            "\n" +
            "CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             S_NAME        CHAR(25) NOT NULL,\n" +
            "                             S_ADDRESS     VARCHAR(40) NOT NULL,\n" +
            "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
            "                             S_PHONE       CHAR(15) NOT NULL,\n" +
            "                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,\n" +
            "                             S_COMMENT     VARCHAR(101) NOT NULL);\n" +
            "\n" +
            "CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,\n" +
            "                             PS_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             PS_AVAILQTY    INTEGER NOT NULL,\n" +
            "                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,\n" +
            "                             PS_COMMENT     VARCHAR(199) NOT NULL );\n" +
            "\n" +
            "CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,\n" +
            "                             C_NAME        VARCHAR(25) NOT NULL,\n" +
            "                             C_ADDRESS     VARCHAR(40) NOT NULL,\n" +
            "                             C_NATIONKEY   INTEGER NOT NULL,\n" +
            "                             C_PHONE       CHAR(15) NOT NULL,\n" +
            "                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,\n" +
            "                             C_MKTSEGMENT  CHAR(10) NOT NULL,\n" +
            "                             C_COMMENT     VARCHAR(117) NOT NULL);\n" +
            "\n" +
            "CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,\n" +
            "                           O_CUSTKEY        INTEGER NOT NULL,\n" +
            "                           O_ORDERSTATUS    CHAR(1) NOT NULL,\n" +
            "                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,\n" +
            "                           O_ORDERDATE      DATE NOT NULL,\n" +
            "                           O_ORDERPRIORITY  CHAR(15) NOT NULL,\n" +
            "                           O_CLERK          CHAR(15) NOT NULL,\n" +
            "                           O_SHIPPRIORITY   INTEGER NOT NULL,\n" +
            "                           O_COMMENT        VARCHAR(79) NOT NULL);\n" +
            "\n" +
            "CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,\n" +
            "                             L_PARTKEY     INTEGER NOT NULL,\n" +
            "                             L_SUPPKEY     INTEGER NOT NULL,\n" +
            "                             L_LINENUMBER  INTEGER NOT NULL,\n" +
            "                             L_QUANTITY    DECIMAL(15,2) NOT NULL,\n" +
            "                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,\n" +
            "                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,\n" +
            "                             L_TAX         DECIMAL(15,2) NOT NULL,\n" +
            "                             L_RETURNFLAG  CHAR(1) NOT NULL,\n" +
            "                             L_LINESTATUS  CHAR(1) NOT NULL,\n" +
            "                             L_SHIPDATE    DATE NOT NULL,\n" +
            "                             L_COMMITDATE  DATE NOT NULL,\n" +
            "                             L_RECEIPTDATE DATE NOT NULL,\n" +
            "                             L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
            "                             L_SHIPMODE     CHAR(10) NOT NULL,\n" +
            "                             L_COMMENT      VARCHAR(44) NOT NULL);\n" +
            "\n" +
            "// query source:\n" +
            "// https://github.com/2ndQuadrant/pg-tpch/blob/master/queries\n" +
            "\n" +
            "// Q1\n" +
            "create view q1 (\n" +
            "    l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    sum_qty,\n" +
            "    sum_base_price,\n" +
            "    sum_disc_price,\n" +
            "    sum_charge,\n" +
            "    avg_qty,\n" +
            "    avg_price,\n" +
            "    avg_disc,\n" +
            "    count_order\n" +
            ") as\n" +
            "select\n" +
            "    l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    sum(l_quantity) as sum_qty,\n" +
            "    sum(l_extendedprice) as sum_base_price,\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
            "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
            "    avg(l_quantity) as avg_qty,\n" +
            "    avg(l_extendedprice) as avg_price,\n" +
            "    avg(l_discount) as avg_disc,\n" +
            "    count(*) as count_order\n" +
            "from\n" +
            "    lineitem\n" +
            "where\n" +
            "    l_shipdate <= date '1998-12-01' - interval '71' DAY\n" +
            "group by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus\n" +
            "order by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus;\n" +
            "\n" +
            "\n" +
            "// Q2\n" +
            "create view q2 (\n" +
            "    s_acctbal,\n" +
            "    s_name,\n" +
            "    n_name,\n" +
            "    p_partkey,\n" +
            "    p_mfgr,\n" +
            "    s_address,\n" +
            "    s_phone,\n" +
            "    s_comment\n" +
            ") as\n" +
            "select\n" +
            "    s_acctbal,\n" +
            "    s_name,\n" +
            "    n_name,\n" +
            "    p_partkey,\n" +
            "    p_mfgr,\n" +
            "    s_address,\n" +
            "    s_phone,\n" +
            "    s_comment\n" +
            "from\n" +
            "    part,\n" +
            "    supplier,\n" +
            "    partsupp,\n" +
            "    nation,\n" +
            "    region\n" +
            "where\n" +
            "    p_partkey = ps_partkey\n" +
            "    and s_suppkey = ps_suppkey\n" +
            "    and p_size = 38\n" +
            "    and p_type like '%TIN'\n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_regionkey = r_regionkey\n" +
            "    and r_name = 'MIDDLE EAST'\n" +
            "    and ps_supplycost = (\n" +
            "        select\n" +
            "            min(ps_supplycost)\n" +
            "        from\n" +
            "            partsupp,\n" +
            "            supplier,\n" +
            "            nation,\n" +
            "            region\n" +
            "        where\n" +
            "            p_partkey = ps_partkey\n" +
            "            and s_suppkey = ps_suppkey\n" +
            "            and s_nationkey = n_nationkey\n" +
            "            and n_regionkey = r_regionkey\n" +
            "            and r_name = 'MIDDLE EAST'\n" +
            "    )\n" +
            "order by\n" +
            "    s_acctbal desc,\n" +
            "    n_name,\n" +
            "    s_name,\n" +
            "    p_partkey\n" +
            "LIMIT 100;\n" +
            "\n" +
            "// Q3\n" +
            "create view q3 (\n" +
            "    l_orderkey,\n" +
            "    revenue,\n" +
            "    o_orderdate,\n" +
            "    o_shippriority\n" +
            ") as\n" +
            "select\n" +
            "    l_orderkey,\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
            "    o_orderdate,\n" +
            "    o_shippriority\n" +
            "from\n" +
            "    customer,\n" +
            "    orders,\n" +
            "    lineitem\n" +
            "where\n" +
            "    c_mktsegment = 'FURNITURE'\n" +
            "    and c_custkey = o_custkey\n" +
            "    and l_orderkey = o_orderkey\n" +
            "    and o_orderdate < date '1995-03-29'\n" +
            "    and l_shipdate > date '1995-03-29'\n" +
            "group by\n" +
            "    l_orderkey,\n" +
            "    o_orderdate,\n" +
            "    o_shippriority\n" +
            "order by\n" +
            "    revenue desc,\n" +
            "    o_orderdate\n" +
            "LIMIT 10;\n" +
            "\n" +
            "// Q4\n" +
            "create view q4 (\n" +
            "    o_orderpriority,\n" +
            "    order_count\n" +
            ") as\n" +
            "select\n" +
            "    o_orderpriority,\n" +
            "    count(*) as order_count\n" +
            "from\n" +
            "    orders\n" +
            "where\n" +
            "    o_orderdate >= date '1997-07-01'\n" +
            "    and o_orderdate < date '1997-07-01' + interval '3' month\n" +
            "    and exists (\n" +
            "        select\n" +
            "            *\n" +
            "        from\n" +
            "            lineitem\n" +
            "        where\n" +
            "            l_orderkey = o_orderkey\n" +
            "            and l_commitdate < l_receiptdate\n" +
            "    )\n" +
            "group by\n" +
            "    o_orderpriority\n" +
            "order by\n" +
            "    o_orderpriority;\n" +
            "\n" +
            "\n" +
            "// q5\n" +
            "create view q5 (\n" +
            "    n_name,\n" +
            "    revenue\n" +
            ") as\n" +
            "select\n" +
            "    n_name,\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
            "from\n" +
            "    customer,\n" +
            "    orders,\n" +
            "    lineitem,\n" +
            "    supplier,\n" +
            "    nation,\n" +
            "    region\n" +
            "where\n" +
            "    c_custkey = o_custkey\n" +
            "    and l_orderkey = o_orderkey\n" +
            "    and l_suppkey = s_suppkey\n" +
            "    and c_nationkey = s_nationkey\n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_regionkey = r_regionkey\n" +
            "    and r_name = 'MIDDLE EAST'\n" +
            "    and o_orderdate >= date '1994-01-01'\n" +
            "    and o_orderdate < date '1994-01-01' + interval '1' year\n" +
            "group by\n" +
            "    n_name\n" +
            "order by\n" +
            "    revenue desc;\n" +
            "\n" +
            "// q6\n" +
            "create view q6 (\n" +
            "    revenue\n" +
            ") as\n" +
            "select\n" +
            "    sum(l_extendedprice * l_discount) as revenue\n" +
            "from\n" +
            "    lineitem\n" +
            "where\n" +
            "    l_shipdate >= date '1994-01-01'\n" +
            "    and l_shipdate < date '1994-01-01' + interval '1' year\n" +
            "    and l_discount between 0.08 - 0.01 and 0.08 + 0.01\n" +
            "    and l_quantity < 24;\n" +
            "\n" +
            "// q7\n" +
            "create view q7 (\n" +
            "    supp_nation,\n" +
            "    cust_nation,\n" +
            "    l_year,\n" +
            "    revenue\n" +
            ") as\n" +
            "select\n" +
            "    supp_nation,\n" +
            "    cust_nation,\n" +
            "    l_year,\n" +
            "    sum(volume) as revenue\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            n1.n_name as supp_nation,\n" +
            "            n2.n_name as cust_nation,\n" +
            "            extract(year from l_shipdate) as l_year,\n" +
            "            l_extendedprice * (1 - l_discount) as volume\n" +
            "        from\n" +
            "            supplier,\n" +
            "            lineitem,\n" +
            "            orders,\n" +
            "            customer,\n" +
            "            nation n1,\n" +
            "            nation n2\n" +
            "        where\n" +
            "            s_suppkey = l_suppkey\n" +
            "            and o_orderkey = l_orderkey\n" +
            "            and c_custkey = o_custkey\n" +
            "            and s_nationkey = n1.n_nationkey\n" +
            "            and c_nationkey = n2.n_nationkey\n" +
            "            and (\n" +
            "                (n1.n_name = 'ROMANIA' and n2.n_name = 'INDIA')\n" +
            "                or (n1.n_name = 'INDIA' and n2.n_name = 'ROMANIA')\n" +
            "            )\n" +
            "            and l_shipdate between date '1995-01-01' and date '1996-12-31'\n" +
            "    ) as shipping\n" +
            "group by\n" +
            "    supp_nation,\n" +
            "    cust_nation,\n" +
            "    l_year\n" +
            "order by\n" +
            "    supp_nation,\n" +
            "    cust_nation,\n" +
            "    l_year;\n" +
            "\n" +
            "// q8\n" +
            "create view q8 (\n" +
            "    o_year,\n" +
            "    mkt_share\n" +
            ") as\n" +
            "select\n" +
            "    o_year,\n" +
            "    sum(case\n" +
            "        when nation = 'INDIA' then volume\n" +
            "        else 0\n" +
            "    end) / sum(volume) as mkt_share\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            extract(year from o_orderdate) as o_year,\n" +
            "            l_extendedprice * (1 - l_discount) as volume,\n" +
            "            n2.n_name as nation\n" +
            "        from\n" +
            "            part,\n" +
            "            supplier,\n" +
            "            lineitem,\n" +
            "            orders,\n" +
            "            customer,\n" +
            "            nation n1,\n" +
            "            nation n2,\n" +
            "            region\n" +
            "        where\n" +
            "            p_partkey = l_partkey\n" +
            "            and s_suppkey = l_suppkey\n" +
            "            and l_orderkey = o_orderkey\n" +
            "            and o_custkey = c_custkey\n" +
            "            and c_nationkey = n1.n_nationkey\n" +
            "            and n1.n_regionkey = r_regionkey\n" +
            "            and r_name = 'ASIA'\n" +
            "            and s_nationkey = n2.n_nationkey\n" +
            "            and o_orderdate between date '1995-01-01' and date '1996-12-31'\n" +
            "            and p_type = 'PROMO BRUSHED COPPER'\n" +
            "    ) as all_nations\n" +
            "group by\n" +
            "    o_year\n" +
            "order by\n" +
            "    o_year;\n" +
            "\n" +
            "// q9\n" +
            "create view q9 (\n" +
            "    nation,\n" +
            "    o_year,\n" +
            "    sum_profit\n" +
            ") as\n" +
            "select\n" +
            "    nation,\n" +
            "    o_year,\n" +
            "    sum(amount) as sum_profit\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            n_name as nation,\n" +
            "            extract(year from o_orderdate) as o_year,\n" +
            "            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" +
            "        from\n" +
            "            part,\n" +
            "            supplier,\n" +
            "            lineitem,\n" +
            "            partsupp,\n" +
            "            orders,\n" +
            "            nation\n" +
            "        where\n" +
            "            s_suppkey = l_suppkey\n" +
            "            and ps_suppkey = l_suppkey\n" +
            "            and ps_partkey = l_partkey\n" +
            "            and p_partkey = l_partkey\n" +
            "            and o_orderkey = l_orderkey\n" +
            "            and s_nationkey = n_nationkey\n" +
            "            and p_name like '%yellow%'\n" +
            "    ) as profit\n" +
            "group by\n" +
            "    nation,\n" +
            "    o_year\n" +
            "order by\n" +
            "nation,\n" +
            "    o_year desc;\n" +
            "\n" +
            "// q10\n" +
            "create view q10 (\n" +
            "    c_custkey,\n" +
            "    c_name,\n" +
            "    revenue,\n" +
            "    c_acctbal,\n" +
            "    n_name,\n" +
            "    c_address,\n" +
            "    c_phone,\n" +
            "    c_comment\n" +
            ") as\n" +
            "select\n" +
            "    c_custkey,\n" +
            "    c_name,\n" +
            "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
            "    c_acctbal,\n" +
            "    n_name,\n" +
            "    c_address,\n" +
            "    c_phone,\n" +
            "    c_comment\n" +
            "from\n" +
            "    customer,\n" +
            "    orders,\n" +
            "    lineitem,\n" +
            "    nation\n" +
            "where\n" +
            "    c_custkey = o_custkey\n" +
            "    and l_orderkey = o_orderkey\n" +
            "    and o_orderdate >= date '1994-01-01'\n" +
            "    and o_orderdate < date '1994-01-01' + interval '3' month\n" +
            "    and l_returnflag = 'R'\n" +
            "    and c_nationkey = n_nationkey\n" +
            "group by\n" +
            "    c_custkey,\n" +
            "    c_name,\n" +
            "    c_acctbal,\n" +
            "    c_phone,\n" +
            "    n_name,\n" +
            "    c_address,\n" +
            "    c_comment\n" +
            "order by\n" +
            "    revenue desc\n" +
            "LIMIT 20;\n" +
            "\n" +
            "// q11\n" +
            "create view q11 (\n" +
            "    ps_partkey,\n" +
            "    value\n" +
            ") as\n" +
            "select\n" +
            "    ps_partkey,\n" +
            "    sum(ps_supplycost * ps_availqty) as value\n" +
            "from\n" +
            "    partsupp,\n" +
            "    supplier,\n" +
            "    nation\n" +
            "where\n" +
            "    ps_suppkey = s_suppkey\n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_name = 'ARGENTINA'\n" +
            "group by\n" +
            "    ps_partkey having\n" +
            "        sum(ps_supplycost * ps_availqty) > (\n" +
            "            select\n" +
            "                sum(ps_supplycost * ps_availqty) * 0.0001000000\n" +
            "            from\n" +
            "                partsupp,\n" +
            "                supplier,\n" +
            "                nation\n" +
            "            where\n" +
            "                ps_suppkey = s_suppkey\n" +
            "                and s_nationkey = n_nationkey\n" +
            "                and n_name = 'ARGENTINA'\n" +
            "        )\n" +
            "order by\n" +
            "    value desc;\n" +
            "\n" +
            "// q12\n" +
            "create view q12 (\n" +
            "    l_shipmode,\n" +
            "    high_line_count,\n" +
            "    low_line_count\n" +
            ") as\n" +
            "select\n" +
            "    l_shipmode,\n" +
            "    sum(case\n" +
            "        when o_orderpriority = '1-URGENT'\n" +
            "            or o_orderpriority = '2-HIGH'\n" +
            "            then 1\n" +
            "        else 0\n" +
            "    end) as high_line_count,\n" +
            "    sum(case\n" +
            "        when o_orderpriority <> '1-URGENT'\n" +
            "            and o_orderpriority <> '2-HIGH'\n" +
            "            then 1\n" +
            "        else 0\n" +
            "    end) as low_line_count\n" +
            "from\n" +
            "    orders,\n" +
            "    lineitem\n" +
            "where\n" +
            "    o_orderkey = l_orderkey\n" +
            "    and l_shipmode in ('FOB', 'SHIP')\n" +
            "    and l_commitdate < l_receiptdate\n" +
            "    and l_shipdate < l_commitdate\n" +
            "    and l_receiptdate >= date '1994-01-01'\n" +
            "    and l_receiptdate < date '1994-01-01' + interval '1' year\n" +
            "group by\n" +
            "    l_shipmode\n" +
            "order by\n" +
            "    l_shipmode;\n" +
            "\n" +
            "// q13\n" +
            "create view q13 (\n" +
            "    c_count,\n" +
            "    custdist\n" +
            ") as\n" +
            "select\n" +
            "    c_count,\n" +
            "    count(*) as custdist\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            c_custkey,\n" +
            "            count(o_orderkey)\n" +
            "        from\n" +
            "            customer left outer join orders on\n" +
            "                c_custkey = o_custkey\n" +
            "                and o_comment not like '%express%packages%'\n" +
            "        group by\n" +
            "            c_custkey\n" +
            "    ) as c_orders (c_custkey, c_count)\n" +
            "group by\n" +
            "    c_count\n" +
            "order by\n" +
            "    custdist desc,\n" +
            "    c_count desc;\n" +
            "\n" +
            "// q14\n" +
            "create view q14 (\n" +
            "    promo_revenue\n" +
            ") as\n" +
            "select\n" +
            "    100.00 * sum(case\n" +
            "        when p_type like 'PROMO%'\n" +
            "            then l_extendedprice * (1 - l_discount)\n" +
            "        else 0\n" +
            "    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\n" +
            "from\n" +
            "    lineitem,\n" +
            "    part\n" +
            "where\n" +
            "    l_partkey = p_partkey\n" +
            "    and l_shipdate >= date '1994-03-01'\n" +
            "    and l_shipdate < date '1994-03-01' + interval '1' month;\n" +
            "\n" +
            "// q15 ???\n" +
            "\n" +
            "\n" +
            "// q16\n" +
            "create view q16 (\n" +
            "    p_brand,\n" +
            "    p_type,\n" +
            "    p_size,\n" +
            "    supplier_cnt\n" +
            ") as\n" +
            "select\n" +
            "    p_brand,\n" +
            "    p_type,\n" +
            "    p_size,\n" +
            "    count(distinct ps_suppkey) as supplier_cnt\n" +
            "from\n" +
            "    partsupp,\n" +
            "    part\n" +
            "where\n" +
            "    p_partkey = ps_partkey\n" +
            "    and p_brand <> 'Brand#45'\n" +
            "    and p_type not like 'SMALL PLATED%'\n" +
            "    and p_size in (19, 17, 16, 23, 10, 4, 38, 11)\n" +
            "    and ps_suppkey not in (\n" +
            "        select\n" +
            "            s_suppkey\n" +
            "        from\n" +
            "            supplier\n" +
            "        where\n" +
            "            s_comment like '%Customer%Complaints%'\n" +
            "    )\n" +
            "group by\n" +
            "    p_brand,\n" +
            "    p_type,\n" +
            "    p_size\n" +
            "order by\n" +
            "    supplier_cnt desc,\n" +
            "    p_brand,\n" +
            "    p_type,\n" +
            "    p_size;\n" +
            "\n" +
            "create view q17 (\n" +
            "    avg_yearly\n" +
            ") as\n" +
            "select\n" +
            "    sum(l_extendedprice) / 7.0 as avg_yearly\n" +
            "from\n" +
            "    lineitem,\n" +
            "    part\n" +
            "where\n" +
            "    p_partkey = l_partkey\n" +
            "    and p_brand = 'Brand#52'\n" +
            "    and p_container = 'LG CAN'\n" +
            "    and l_quantity < (\n" +
            "        select\n" +
            "            0.2 * avg(l_quantity)\n" +
            "        from\n" +
            "            lineitem\n" +
            "        where\n" +
            "            l_partkey = p_partkey\n" +
            "    );\n" +
            "\n" +
            "// q18\n" +
            "create view q18 (\n" +
            "    c_name,\n" +
            "    c_custkey,\n" +
            "    o_orderkey,\n" +
            "    o_orderdate,\n" +
            "    o_totalprice,\n" +
            "    sum_quantity\n" +
            ") as\n" +
            "select\n" +
            "    c_name,\n" +
            "    c_custkey,\n" +
            "    o_orderkey,\n" +
            "    o_orderdate,\n" +
            "    o_totalprice,\n" +
            "    sum(l_quantity) as sum_quantity\n" +
            "from\n" +
            "    customer,\n" +
            "    orders,\n" +
            "    lineitem\n" +
            "where\n" +
            "    o_orderkey in (\n" +
            "        select\n" +
            "            l_orderkey\n" +
            "        from\n" +
            "            lineitem\n" +
            "        group by\n" +
            "            l_orderkey having\n" +
            "                sum(l_quantity) > 313\n" +
            "    )\n" +
            "    and c_custkey = o_custkey\n" +
            "    and o_orderkey = l_orderkey\n" +
            "group by\n" +
            "    c_name,\n" +
            "    c_custkey,\n" +
            "    o_orderkey,\n" +
            "    o_orderdate,\n" +
            "    o_totalprice\n" +
            "order by\n" +
            "    o_totalprice desc,\n" +
            "    o_orderdate\n" +
            "LIMIT 100;\n" +
            "\n" +
            "// q19\n" +
            "create view q19 (\n" +
            "    revenue\n" +
            ") as\n" +
            "select\n" +
            "    sum(l_extendedprice* (1 - l_discount)) as revenue\n" +
            "from\n" +
            "    lineitem,\n" +
            "    part\n" +
            "where\n" +
            "    (\n" +
            "        p_partkey = l_partkey\n" +
            "        and p_brand = 'Brand#22'\n" +
            "        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n" +
            "        and l_quantity >= 8 and l_quantity <= 8 + 10\n" +
            "        and p_size between 1 and 5\n" +
            "        and l_shipmode in ('AIR', 'AIR REG')\n" +
            "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "    )\n" +
            "    or\n" +
            "    (\n" +
            "        p_partkey = l_partkey\n" +
            "        and p_brand = 'Brand#23'\n" +
            "        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
            "        and l_quantity >= 10 and l_quantity <= 10 + 10\n" +
            "        and p_size between 1 and 10\n" +
            "        and l_shipmode in ('AIR', 'AIR REG')\n" +
            "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "    )\n" +
            "    or\n" +
            "    (\n" +
            "        p_partkey = l_partkey\n" +
            "        and p_brand = 'Brand#12'\n" +
            "        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
            "        and l_quantity >= 24 and l_quantity <= 24 + 10\n" +
            "        and p_size between 1 and 15\n" +
            "        and l_shipmode in ('AIR', 'AIR REG')\n" +
            "        and l_shipinstruct = 'DELIVER IN PERSON'\n" +
            "    );\n" +
            "\n" +
            "create view q20 (\n" +
            "    s_name,\n" +
            "    s_address\n" +
            ") as\n" +
            "select\n" +
            "    s_name,\n" +
            "    s_address\n" +
            "from\n" +
            "    supplier,\n" +
            "    nation\n" +
            "where\n" +
            "    s_suppkey in (\n" +
            "        select\n" +
            "            ps_suppkey\n" +
            "        from\n" +
            "            partsupp\n" +
            "        where\n" +
            "            ps_partkey in (\n" +
            "                select\n" +
            "                    p_partkey\n" +
            "                from\n" +
            "                    part\n" +
            "                where\n" +
            "                    p_name like 'frosted%'\n" +
            "            )\n" +
            "            and ps_availqty > (\n" +
            "                select\n" +
            "                    0.5 * sum(l_quantity)\n" +
            "                from\n" +
            "                    lineitem\n" +
            "                where\n" +
            "                    l_partkey = ps_partkey\n" +
            "                    and l_suppkey = ps_suppkey\n" +
            "                    and l_shipdate >= date '1994-01-01'\n" +
            "                    and l_shipdate < date '1994-01-01' + interval '1' year\n" +
            "            )\n" +
            "    )\n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_name = 'IRAN'\n" +
            "order by\n" +
            "    s_name;\n" +
            "\n" +
            "// q21\n" +
            "create view q21 (\n" +
            "    s_name,\n" +
            "    numwait\n" +
            ") as\n" +
            "select\n" +
            "    s_name,\n" +
            "    count(*) as numwait\n" +
            "from\n" +
            "    supplier,\n" +
            "    lineitem l1,\n" +
            "    orders,\n" +
            "    nation\n" +
            "where\n" +
            "    s_suppkey = l1.l_suppkey\n" +
            "    and o_orderkey = l1.l_orderkey\n" +
            "    and o_orderstatus = 'F'\n" +
            "    and l1.l_receiptdate > l1.l_commitdate\n" +
            "    and exists (\n" +
            "        select\n" +
            "            *\n" +
            "        from\n" +
            "            lineitem l2\n" +
            "        where\n" +
            "            l2.l_orderkey = l1.l_orderkey\n" +
            "            and l2.l_suppkey <> l1.l_suppkey\n" +
            "    )\n" +
            "    and not exists (\n" +
            "        select\n" +
            "            *\n" +
            "        from\n" +
            "            lineitem l3\n" +
            "        where\n" +
            "            l3.l_orderkey = l1.l_orderkey\n" +
            "            and l3.l_suppkey <> l1.l_suppkey\n" +
            "            and l3.l_receiptdate > l3.l_commitdate\n" +
            "    )\n" +
            "    and s_nationkey = n_nationkey\n" +
            "    and n_name = 'GERMANY'\n" +
            "group by\n" +
            "    s_name\n" +
            "order by\n" +
            "    numwait desc,\n" +
            "    s_name\n" +
            "LIMIT 100;\n" +
            "\n" +
            "\n" +
            "// q22\n" +
            "create view q22 (\n" +
            "    cntrycode,\n" +
            "    numcust,\n" +
            "    totacctbal\n" +
            ") as\n" +
            "select\n" +
            "    cntrycode,\n" +
            "    count(*) as numcust,\n" +
            "    sum(c_acctbal) as totacctbal\n" +
            "from\n" +
            "    (\n" +
            "        select\n" +
            "            substring(c_phone from 1 for 2) as cntrycode,\n" +
            "            c_acctbal\n" +
            "        from\n" +
            "            customer\n" +
            "        where\n" +
            "            substring(c_phone from 1 for 2) in\n" +
            "                ('30', '24', '31', '38', '25', '34', '37')\n" +
            "            and c_acctbal > (\n" +
            "                select\n" +
            "                    avg(c_acctbal)\n" +
            "                from\n" +
            "                    customer\n" +
            "                where\n" +
            "                    c_acctbal > 0.00\n" +
            "                    and substring(c_phone from 1 for 2) in\n" +
            "                        ('30', '24', '31', '38', '25', '34', '37')\n" +
            "            )\n" +
            "            and not exists (\n" +
            "                select\n" +
            "                    *\n" +
            "                from\n" +
            "                    orders\n" +
            "                where\n" +
            "                    o_custkey = c_custkey\n" +
            "            )\n" +
            "    ) as custsale\n" +
            "group by\n" +
            "    cntrycode\n" +
            "order by\n" +
            "    cntrycode;";

    @Test
    public void compileTpch() throws IOException, InterruptedException {
        DBSPCompiler compiler = this.testCompiler();
        compiler.compileStatements(tpch);
        System.err.println(compiler.messages);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = getCircuit(compiler);
        RustFileWriter writer = new RustFileWriter(compiler, testFilePath);
        writer.add(circuit);
        writer.writeAndClose();
        Utilities.compileAndTestRust(rustDirectory, true);
    }
}
