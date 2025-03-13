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
create materialized view q1
as select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;

-- Minimum Cost Supplier
create materialized view q2
as select
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
	and p_size = 15
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
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
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

-- Shipping Priority
create materialized view q3
as select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;

-- Order Priority Checking
create materialized view q4
as select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1993-07-01'
	and o_orderdate < date '1993-07-01' + interval '3' month
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
	o_orderpriority;

-- Local Supplier Volume
create materialized view q5
as select
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
	and r_name = 'ASIA'
	and o_orderdate >= date '1994-01-01'
	and o_orderdate < date '1994-01-01' + interval '1' year
group by
	n_name
order by
	revenue desc;

-- Forecasting Revenue Change
create materialized view q6
as select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24;

-- Volume Shipping
create materialized view q7
as select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			year(l_shipdate) as l_year,
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
				(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
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
	l_year;

-- National Market Share
create materialized view q8
as select
	o_year,
	sum(case
		when nation = 'BRAZIL' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			year(o_orderdate) as o_year,
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
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'ECONOMY ANODIZED STEEL'
	) as all_nations
group by
	o_year
order by
	o_year;

-- Product Type Profit Measure
create materialized view q9
as select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			year(o_orderdate) as o_year,
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
			and p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;

-- Returned Item Reporting
create materialized view q10
as select
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
	and o_orderdate >= date '1993-10-01'
	and o_orderdate < date '1993-10-01' + interval '3' month
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
limit 20;
