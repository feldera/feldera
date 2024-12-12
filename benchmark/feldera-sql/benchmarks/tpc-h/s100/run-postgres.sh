#! /bin/sh

psql() {
    time command psql "$@"
}

# How to do indexing.
index_mode=no_indexes		# Do not use indexes at all
#index_mode=keep_index		# Keep indexes around and update them
#index_mode=reindex		# Compute entirely new indexes for each chunk

# How to add the data
data_mode=batched		# Add and query one batch (1/10th) at a time.
#data_mode=full			# Add all the data together.

echo '* Drop old tables (if any).'
psql <<EOF
DROP TABLE PART;
DROP TABLE SUPPLIER;
DROP TABLE PARTSUPP;
DROP TABLE CUSTOMER;
DROP TABLE ORDERS;
DROP TABLE LINEITEM;
DROP TABLE NATION;
DROP TABLE REGION;
EOF

datadir=$PWD

echo 'Create and load new tables (except orders and lineitems).'
psql <<EOF
BEGIN;

	CREATE TABLE PART (

		P_PARTKEY		SERIAL,
		P_NAME			VARCHAR(55),
		P_MFGR			CHAR(25),
		P_BRAND			CHAR(10),
		P_TYPE			VARCHAR(25),
		P_SIZE			INTEGER,
		P_CONTAINER		CHAR(10),
		P_RETAILPRICE	DECIMAL,
		P_COMMENT		VARCHAR(23)
	);

	COPY part FROM '$datadir/part.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE REGION (
		R_REGIONKEY	SERIAL,
		R_NAME		CHAR(25),
		R_COMMENT	VARCHAR(152)
	);

	COPY region FROM '$datadir/region.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE NATION (
		N_NATIONKEY		SERIAL,
		N_NAME			CHAR(25),
		N_REGIONKEY		BIGINT NOT NULL,  -- references R_REGIONKEY
		N_COMMENT		VARCHAR(152)
	);

	COPY nation FROM '$datadir/nation.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE SUPPLIER (
		S_SUPPKEY		SERIAL,
		S_NAME			CHAR(25),
		S_ADDRESS		VARCHAR(40),
		S_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
		S_PHONE			CHAR(15),
		S_ACCTBAL		DECIMAL,
		S_COMMENT		VARCHAR(101)
	);

	COPY supplier FROM '$datadir/supplier.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE CUSTOMER (
		C_CUSTKEY		SERIAL,
		C_NAME			VARCHAR(25),
		C_ADDRESS		VARCHAR(40),
		C_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
		C_PHONE			CHAR(15),
		C_ACCTBAL		DECIMAL,
		C_MKTSEGMENT	CHAR(10),
		C_COMMENT		VARCHAR(117)
	);

	COPY customer FROM '$datadir/customer.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE PARTSUPP (
		PS_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY
		PS_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY
		PS_AVAILQTY		INTEGER,
		PS_SUPPLYCOST	DECIMAL,
		PS_COMMENT		VARCHAR(199)
	);

	COPY partsupp FROM '$datadir/partsupp.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE ORDERS (
		O_ORDERKEY		SERIAL,
		O_CUSTKEY		BIGINT NOT NULL, -- references C_CUSTKEY
		O_ORDERSTATUS	CHAR(1),
		O_TOTALPRICE	DECIMAL,
		O_ORDERDATE		DATE,
		O_ORDERPRIORITY	CHAR(15),
		O_CLERK			CHAR(15),
		O_SHIPPRIORITY	INTEGER,
		O_COMMENT		VARCHAR(79)
	);

	--COPY orders FROM '$datadir/orders.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;

BEGIN;

	CREATE TABLE LINEITEM (
		L_ORDERKEY		BIGINT NOT NULL, -- references O_ORDERKEY
		L_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
		L_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
		L_LINENUMBER	INTEGER,
		L_QUANTITY		DECIMAL,
		L_EXTENDEDPRICE	DECIMAL,
		L_DISCOUNT		DECIMAL,
		L_TAX			DECIMAL,
		L_RETURNFLAG	CHAR(1),
		L_LINESTATUS	CHAR(1),
		L_SHIPDATE		DATE,
		L_COMMITDATE	DATE,
		L_RECEIPTDATE	DATE,
		L_SHIPINSTRUCT	CHAR(25),
		L_SHIPMODE		CHAR(10),
		L_COMMENT		VARCHAR(44)
	);

	--copy lineitem FROM '$datadir/lineitem.csv' WITH (FORMAT csv, DELIMITER '|');

COMMIT;
EOF

add_indexes() {
    echo '* Add indexes.'
    psql <<EOF
-- indexes on the foreign keys

CREATE INDEX IDX_SUPPLIER_NATION_KEY ON SUPPLIER (S_NATIONKEY);

CREATE INDEX IDX_PARTSUPP_PARTKEY ON PARTSUPP (PS_PARTKEY);
CREATE INDEX IDX_PARTSUPP_SUPPKEY ON PARTSUPP (PS_SUPPKEY);

CREATE INDEX IDX_CUSTOMER_NATIONKEY ON CUSTOMER (C_NATIONKEY);

CREATE INDEX IDX_ORDERS_CUSTKEY ON ORDERS (O_CUSTKEY);

CREATE INDEX IDX_LINEITEM_ORDERKEY ON LINEITEM (L_ORDERKEY);
CREATE INDEX IDX_LINEITEM_PART_SUPP ON LINEITEM (L_PARTKEY,L_SUPPKEY);

CREATE INDEX IDX_NATION_REGIONKEY ON NATION (N_REGIONKEY);


-- aditional indexes

CREATE INDEX IDX_LINEITEM_SHIPDATE ON LINEITEM (L_SHIPDATE, L_DISCOUNT, L_QUANTITY);

CREATE INDEX IDX_ORDERS_ORDERDATE ON ORDERS (O_ORDERDATE);
EOF
}

drop_indexes() {
    echo '* Drop indexes.'
    psql <<EOF
DROP INDEX IDX_SUPPLIER_NATION_KEY;
DROP INDEX IDX_PARTSUPP_PARTKEY;
DROP INDEX IDX_PARTSUPP_SUPPKEY;
DROP INDEX IDX_CUSTOMER_NATIONKEY;
DROP INDEX IDX_ORDERS_CUSTKEY;
DROP INDEX IDX_LINEITEM_ORDERKEY;
DROP INDEX IDX_LINEITEM_PART_SUPP;
DROP INDEX IDX_NATION_REGIONKEY;
DROP INDEX IDX_LINEITEM_SHIPDATE;
DROP INDEX IDX_ORDERS_ORDERDATE;
EOF
}

if test $index_mode = keep_index; then
    add_indexes
fi

run_batch() {
    local chunk=$1
    echo
    echo "* Add chunk $chunk."
    psql <<EOF
begin;
copy lineitem FROM '$datadir/lineitem-$chunk.csv' WITH (FORMAT csv, DELIMITER '|');
copy orders FROM '$datadir/orders-$chunk.csv' WITH (FORMAT csv, DELIMITER '|');
commit;
EOF
    if test $index_mode = reindex; then
	add_indexes
    fi
    echo "* Run q5 for chunk $chunk."
    for i in 1; do
	psql <<EOF
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
    revenue desc;
EOF
    done
    if test $index_mode = reindex; then
	drop_indexes
    fi
}

case $data_mode in
    batched) for chunk in $(seq 10); do run_batch $chunk; done ;;
    full) run_batch full ;;
esac
