-- The queries are taken from https://github.com/2ndQuadrant/pg-tpch/tree/master/queries

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
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/lineitem.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

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
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/orders.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

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
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/part.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

CREATE TABLE CUSTOMER (
        C_CUSTKEY     INTEGER NOT NULL,
        C_NAME        VARCHAR(25) NOT NULL,
        C_ADDRESS     VARCHAR(40) NOT NULL,
        C_NATIONKEY   INTEGER NOT NULL,
        C_PHONE       CHAR(15) NOT NULL,
        C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
        C_MKTSEGMENT  CHAR(10) NOT NULL,
        C_COMMENT     VARCHAR(117) NOT NULL
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/customer.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

CREATE TABLE SUPPLIER (
        S_SUPPKEY     INTEGER NOT NULL,
        S_NAME        CHAR(25) NOT NULL,
        S_ADDRESS     VARCHAR(40) NOT NULL,
        S_NATIONKEY   INTEGER NOT NULL,
        S_PHONE       CHAR(15) NOT NULL,
        S_ACCTBAL     DECIMAL(15,2) NOT NULL,
        S_COMMENT     VARCHAR(101) NOT NULL
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/supplier.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

CREATE TABLE PARTSUPP (
        PS_PARTKEY     INTEGER NOT NULL,
        PS_SUPPKEY     INTEGER NOT NULL,
        PS_AVAILQTY    INTEGER NOT NULL,
        PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
        PS_COMMENT     VARCHAR(199) NOT NULL
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/partsupp.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

CREATE TABLE NATION  (
        N_NATIONKEY  INTEGER NOT NULL,
        N_NAME       CHAR(25) NOT NULL,
        N_REGIONKEY  INTEGER NOT NULL,
        N_COMMENT    VARCHAR(152)
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/nation.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

CREATE TABLE REGION  (
        R_REGIONKEY  INTEGER NOT NULL,
        R_NAME       CHAR(25) NOT NULL,
        R_COMMENT    VARCHAR(152)
) WITH ('connectors' = '[{{
    "transport": {{
        "name": "file_input",
        "config": {{ "path": "{folder}/data-large/region.csv" }}
    }},
    "format": {{
        "name": "csv",
        "config": {{ "delimiter": "|" }}
    }}
}}]');

