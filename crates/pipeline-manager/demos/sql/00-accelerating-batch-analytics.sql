-- Use Case: Accelerating Batch Analytics (accelerating-batch-analytics)
--
-- Speed up batch analytics with an always-on incremental Feldera pipeline.
--
-- ## Detailed Description
-- This demo converts the Spark batch job to a Feldera pipeline without
-- requiring changes to the queries.
-- Detailed description here: https://docs.feldera.com/use_cases/batch/intro
--
-- Instead of a Spark job that needs to be rerun every time the input changes,
-- with Feldera, these changes are reflected immediately in the output.
--
-- The queries in this demo are taken from the TPC-H benchmark.
--
-- ## How to Run
--
-- * Run the pipeline. It should take a few seconds to populate input tables
--   from the Delta Lake and evaluate all views.
-- * Run the ad-hoc query to display the contents of the q1 view: `SELECT * FROM q1`
-- * To demonstrate incremental computation, insert a new record using the
--   following ad-hoc query:
-- ```
-- INSERT INTO LINEITEM VALUES (1, 5, 4, 1, 50, 0.80, 0.65, 0.10, 'B', 'C', '1998-09-01', '1998-09-01', '1998-09-01', 'DELIVER IN PERSON', 'TRUCK', 'new record insertion')
-- ```
-- * Re-run the `SELECT` query to see the updated results.

SET FELDERA_IGNORE_WARNING_UNUSED_COLUMN = 1;
SET FELDERA_IGNORE_WARNING_ORDER_BY_IS_IGNORED = 1;

-- Lineitem
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
        L_SHIPMODE     CHAR(10) NOT NULL,
        L_COMMENT      VARCHAR(44) NOT NULL
) WITH (
 'connectors' = '[{
    "name": "lineitem",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/lineitem",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Orders
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
 'connectors' = '[{
    "name": "orders",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/orders",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Part
CREATE TABLE PART (
        P_PARTKEY     INTEGER NOT NULL,
        P_NAME        VARCHAR(55) NOT NULL,
        P_MFGR        CHAR(25) NOT NULL,
        P_BRAND       CHAR(10) NOT NULL,
        P_TYPE        VARCHAR(25) NOT NULL,
        P_SIZE        INTEGER NOT NULL,
        P_CONTAINER   CHAR(10) NOT NULL,
        P_RETAILPRICE DECIMAL(15,2) NOT NULL,
        P_COMMENT     VARCHAR(23) NOT NULL
) WITH (
 'connectors' = '[{
    "name": "part",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/part",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Customer
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
 'connectors' = '[{
    "name": "customer",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/customer",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Supplier
CREATE TABLE SUPPLIER (
        S_SUPPKEY     INTEGER NOT NULL,
        S_NAME        CHAR(25) NOT NULL,
        S_ADDRESS     VARCHAR(40) NOT NULL,
        S_NATIONKEY   INTEGER NOT NULL,
        S_PHONE       CHAR(15) NOT NULL,
        S_ACCTBAL     DECIMAL(15,2) NOT NULL,
        S_COMMENT     VARCHAR(101) NOT NULL
) WITH (
 'connectors' = '[{
    "name": "supplier",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/supplier",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Part supplies
CREATE TABLE PARTSUPP (
        PS_PARTKEY     INTEGER NOT NULL,
        PS_SUPPKEY     INTEGER NOT NULL,
        PS_AVAILQTY    INTEGER NOT NULL,
        PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
        PS_COMMENT     VARCHAR(199) NOT NULL
) WITH (
 'connectors' = '[{
    "name": "partsupp",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/partsupp",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Nation
CREATE TABLE NATION  (
        N_NATIONKEY  INTEGER NOT NULL,
        N_NAME       CHAR(25) NOT NULL,
        N_REGIONKEY  INTEGER NOT NULL,
        N_COMMENT    VARCHAR(152)
) WITH (
 'connectors' = '[{
    "name": "nation",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/nation",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Region
CREATE TABLE REGION  (
        R_REGIONKEY  INTEGER NOT NULL,
        R_NAME       CHAR(25) NOT NULL,
        R_COMMENT    VARCHAR(152)
) WITH (
 'connectors' = '[{
    "name": "region",
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://batchtofeldera/region",
        "aws_skip_signature": "true",
        "aws_region": "ap-southeast-2",
        "mode": "snapshot_and_follow"
      }
    }
 }]'
);

-- Pricing Summary Report
CREATE MATERIALIZED VIEW q1
AS SELECT
        l_returnflag,
        l_linestatus,
        SUM(l_quantity) AS sum_qty,
        SUM(l_extendedprice) AS sum_base_price,
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        AVG(l_quantity) AS avg_qty,
        AVG(l_extendedprice) AS avg_price,
        AVG(l_discount) AS avg_disc,
        COUNT(*) AS count_order
FROM
        lineitem
WHERE
        l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
        l_returnflag,
        l_linestatus
ORDER BY
        l_returnflag,
        l_linestatus;

-- Minimum Cost Supplier
CREATE MATERIALIZED VIEW q2
AS SELECT
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
FROM
        part,
        supplier,
        partsupp,
        nation,
        region
WHERE
        p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND p_size = 15
        AND p_type LIKE '%BRASS'
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
        AND ps_supplycost = (
                SELECT
                        MIN(ps_supplycost)
                FROM
                        partsupp,
                        supplier,
                        nation,
                        region
                WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
        )
ORDER BY
        s_acctbal DESC,
        n_name,
        s_name,
        p_partkey
LIMIT 100;

-- Shipping Priority
CREATE MATERIALIZED VIEW q3
AS SELECT
        l_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
FROM
        customer,
        orders,
        lineitem
WHERE
        c_mktsegment = 'BUILDING'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < DATE '1995-03-15'
        AND l_shipdate > DATE '1995-03-15'
GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
ORDER BY
        revenue DESC,
        o_orderdate
LIMIT 10;

-- Order Priority Checking
CREATE MATERIALIZED VIEW q4
AS SELECT
        o_orderpriority,
        COUNT(*) AS order_count
FROM
        orders
WHERE
        o_orderdate >= DATE '1993-07-01'
        AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
        AND EXISTS (
                SELECT
                        *
                FROM
                        lineitem
                WHERE
                        l_orderkey = o_orderkey
                        AND l_commitdate < l_receiptdate
        )
GROUP BY
        o_orderpriority
ORDER BY
        o_orderpriority;

-- Local Supplier Volume
CREATE MATERIALIZED VIEW q5
AS SELECT
        n_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'ASIA'
        AND o_orderdate >= DATE '1994-01-01'
        AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
GROUP BY
        n_name
ORDER BY
        revenue DESC;

-- Forecasting Revenue Change
CREATE MATERIALIZED VIEW q6
AS SELECT
        SUM(l_extendedprice * l_discount) AS revenue
FROM
        lineitem
WHERE
        l_shipdate >= DATE '1994-01-01'
        AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        AND l_discount BETWEEN .06 - 0.01 AND .06 + 0.01
        AND l_quantity < 24;

-- Volume Shipping
CREATE MATERIALIZED VIEW q7
AS SELECT
        supp_nation,
        cust_nation,
        l_year,
        SUM(volume) AS revenue
FROM
        (
                SELECT
                        n1.n_name AS supp_nation,
                        n2.n_name AS cust_nation,
                        YEAR(l_shipdate) AS l_year,
                        l_extendedprice * (1 - l_discount) AS volume
                FROM
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                WHERE
                        s_suppkey = l_suppkey
                        AND o_orderkey = l_orderkey
                        AND c_custkey = o_custkey
                        AND s_nationkey = n1.n_nationkey
                        AND c_nationkey = n2.n_nationkey
                        AND (
                                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                        )
                        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        ) AS shipping
GROUP BY
        supp_nation,
        cust_nation,
        l_year
ORDER BY
        supp_nation,
        cust_nation,
        l_year;

-- National Market Share
CREATE MATERIALIZED VIEW q8
AS SELECT
        o_year,
        SUM(CASE
                WHEN nation = 'BRAZIL' THEN volume
                ELSE 0
        END) / SUM(volume) AS mkt_share
FROM
        (
                SELECT
                        YEAR(o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) AS volume,
                        n2.n_name AS nation
                FROM
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                WHERE
                        p_partkey = l_partkey
                        AND s_suppkey = l_suppkey
                        AND l_orderkey = o_orderkey
                        AND o_custkey = c_custkey
                        AND c_nationkey = n1.n_nationkey
                        AND n1.n_regionkey = r_regionkey
                        AND r_name = 'AMERICA'
                        AND s_nationkey = n2.n_nationkey
                        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                        AND p_type = 'ECONOMY ANODIZED STEEL'
        ) AS all_nations
GROUP BY
        o_year
ORDER BY
        o_year;

-- Product Type Profit Measure
CREATE MATERIALIZED VIEW q9
AS SELECT
        nation,
        o_year,
        SUM(amount) AS sum_profit
FROM
        (
                SELECT
                        n_name AS nation,
                        YEAR(o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                FROM
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                WHERE
                        s_suppkey = l_suppkey
                        AND ps_suppkey = l_suppkey
                        AND ps_partkey = l_partkey
                        AND p_partkey = l_partkey
                        AND o_orderkey = l_orderkey
                        AND s_nationkey = n_nationkey
                        AND p_name LIKE '%green%'
        ) AS profit
GROUP BY
        nation,
        o_year
ORDER BY
        nation,
        o_year DESC;

-- Returned Item Reporting
CREATE MATERIALIZED VIEW q10
AS SELECT
        c_custkey,
        c_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
FROM
        customer,
        orders,
        lineitem,
        nation
WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate >= DATE '1993-10-01'
        AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
        AND l_returnflag = 'R'
        AND c_nationkey = n_nationkey
GROUP BY
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
ORDER BY
        revenue DESC
LIMIT 20;
