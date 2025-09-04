import sys
import time
import unittest
from feldera.pipeline import Pipeline
from feldera.testutils import (
    IndexSpec,
    ViewSpec,
    build_pipeline,
    check_for_endpoint_errors,
    checkpoint_pipeline,
    generate_program,
    log,
    number_of_processed_records,
    run_workload,
    transaction,
    unique_pipeline_name,
    validate_outputs,
)
import tempfile
import os
import argparse
from typing import List, Optional


class TPCHTestConfig:
    def __init__(
        self,
        mode: str,
        input_mode: str,
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        s3_path: Optional[str] = None,
        s3_region: Optional[str] = None,
        input_dir: Optional[str] = None,
    ):
        self.mode = mode

        if mode not in ["transaction", "stream", "checkpoint"]:
            raise ValueError(f"Unknown mode: {mode}")

        self.input_mode = input_mode

        if self.input_mode == "file":
            self.input_dir = input_dir
            if not self.input_dir:
                raise ValueError("input_dir must be specified if input_mode is 'file'")
        elif self.input_mode == "s3":
            self.s3_bucket = s3_bucket

            if not self.s3_bucket:
                raise ValueError("s3_bucket must be specified if input_mode is 's3'")

            self.s3_prefix = s3_prefix
            if not self.s3_prefix:
                raise ValueError("s3_prefix must be specified if input_mode is 's3'")

            self.s3_region = s3_region
            if not self.s3_region:
                raise ValueError("s3_region must be specified if input_mode is 's3'")

        elif self.input_mode == "delta":
            self.s3_path = s3_path
            if not self.s3_path:
                raise ValueError("s3_path must be specified if input_mode is 'delta'")

            self.s3_region = s3_region
            if not self.s3_region:
                raise ValueError("s3_region must be specified if input_mode is 'delta'")
        else:
            raise ValueError(f"Unknown input mode: {self.input_mode}")

    def __repr__(self):
        return f"IndexSpec(name={self.name!r},columns={self.columns!r})"

def run_cli():
    """Run the TPC-H test with configuration specified via CLI arguments."""

    parser = argparse.ArgumentParser(description="TPC-H test")

    parser.add_argument(
        "--mode",
        default="transaction",
        choices=["transaction", "stream", "checkpoint"],
        help="Test mode: 'transaction' (default), 'stream' or 'checkpoint'. In 'transaction' mode, data is ingested in a single transactions. This mode is used for testing and benchmarking transactions."
        " In 'stream' mode, data is ingested without transactions. This mode is used for testing and benchmarking incremental processing."
        " In 'checkpoint' mode, the dataset is split into segments; the pipeline ingests a segment in a series of transactions, making a checkpoint mid-segment. The pipeline is then shutdown and restarted from the last checkpoint. This mode is used for testing checkpointing and bootstrapping.",
    )

    parser.add_argument(
        "--input-dir",
        nargs="?",
        help="Path to the input data directory. Use this setting to ingest input data from local files. The directory should contain the TPC-H CSV files named as follows: lineitem.csv, orders.csv, part.csv, customer.csv, supplier.csv, partsupp.csv, nation.csv, region.csv.",
    )

    parser.add_argument(
        "--s3-bucket",
        nargs="?",
        help="S3 bucket containing the input dataset. Use this setting to ingest data using the S3 input connector.",
    )

    parser.add_argument(
        "--s3-prefix",
        nargs="?",
        help="S3 prefix within the bucket specified via --s3-bucket containing the input dataset. The S3 location should contain the TPC-H CSV files named as follows: lineitem.csv, orders.csv, part.csv, customer.csv, supplier.csv, partsupp.csv, nation.csv, region.csv.",
    )

    parser.add_argument(
        "--s3-path",
        nargs="?",
        help="S3 bucket and prefix containing the input dataset in the DeltaLake format. Use this setting to ingest data using the DeltaLake input connector.",
    )

    parser.add_argument(
        "--s3-region",
        nargs="?",
        help="S3 bucket region. Required if --s3-bucket or --s3-path is specified.",
    )

    args = parser.parse_args()

    if sum(x is not None for x in (args.s3_bucket, args.s3_path, args.input_dir)) > 1:
        raise ValueError(
            "At most one of --input_dir, --s3_path, --s3_bucket can be specified"
        )

    if args.input_dir:
        input_mode = "file"
        s3_path = None
        s3_region = None
    elif args.s3_bucket:
        input_mode = "s3"
        s3_region = args.s3_region
    elif args.s3_path:
        input_mode = "delta"
        s3_path = args.s3_path
        s3_region = args.s3_region
    else:
        input_mode = "delta"
        s3_path = "s3://batchtofeldera"
        s3_region = "ap-southeast-2"

    mode = args.mode

    log(f"Test mode: {mode}")
    log(f"Input mode: {input_mode}")

    config = TPCHTestConfig(
        mode,
        input_mode,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        s3_region=s3_region,
        s3_path=s3_path,
        input_dir=args.input_dir,
    )

    tpch_test(config)


def delta_input_connector(s3_path: str, region: str, table: str) -> str:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        aws_access = f"""
            "aws_access_key_id": "{os.environ.get("AWS_ACCESS_KEY_ID")}",
            "aws_secret_access_key": "{os.environ.get("AWS_SECRET_ACCESS_KEY")}",
        """
    else:
        aws_access = '"aws_skip_signature": "true",'

    return f"""{{
        "transport": {{
            "name": "delta_table_input",
            "config": {{
                "uri": "{s3_path}/{table}",
                {aws_access}
                "aws_region": "{region}",
                "mode": "snapshot"
            }}
        }}
    }}"""


def file_input_connector(path: str, table: str) -> str:
    return f"""{{
        "transport": {{
            "name": "file_input",
            "config": {{
                "path": "{path}/{table}.csv"
            }}
        }},
        "format": {{
            "name": "csv",
            "config": {{
                "headers": true
            }}
        }}
    }}"""


def s3_input_connector(bucket: str, prefix: str, region: str, table: str) -> str:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        aws_access = f"""
            "aws_access_key_id": "{os.environ.get("AWS_ACCESS_KEY_ID")}",
            "aws_secret_access_key": "{os.environ.get("AWS_SECRET_ACCESS_KEY")}",
        """
    else:
        aws_access = '"aws_skip_signature": "true",'

    return f"""{{
        "transport": {{
            "name": "s3_input",
            "config": {{
                "bucket_name": "{bucket}",
                "key": "{prefix}/{table}.csv",
                {aws_access}
                "region": "{region}"
            }}
        }},
        "format": {{
            "name": "csv",
            "config": {{
                "headers": true
            }}
        }}
    }}"""


def input_connector(config: TPCHTestConfig, table: str) -> str:
    if config.input_mode == "delta":
        return delta_input_connector(config.s3_path, config.s3_region, table)
    elif config.input_mode == "s3":
        return s3_input_connector(
            config.s3_bucket, config.s3_prefix, config.s3_region, table
        )
    elif config.input_mode == "file":
        return file_input_connector(config.input_dir, table)
    else:
        raise RuntimeError(f"Unknown input mode: {config.input_mode}")


# https://github.com/feldera/feldera/issues/4448
def tpch_tables(config: TPCHTestConfig):
    return {
        "lineitem": f"""
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
          L_COMMENT      VARCHAR(44) NOT NULL,
          PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "lineitem")}]'
);
  """,
        "orders": f"""
  CREATE TABLE ORDERS  (
          O_ORDERKEY       INTEGER NOT NULL PRIMARY KEY,
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
    'connectors' = '[{input_connector(config, "orders")}]'
);
  """,
        # https://github.com/feldera/feldera/issues/4448
        "part": f"""
  CREATE TABLE PART (
          P_PARTKEY     INTEGER NOT NULL PRIMARY KEY,
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
    'connectors' = '[{input_connector(config, "part")}]'
  );
  """,
        "customer": f"""
  CREATE TABLE CUSTOMER (
          C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY,
          C_NAME        VARCHAR(25) NOT NULL,
          C_ADDRESS     VARCHAR(40) NOT NULL,
          C_NATIONKEY   INTEGER NOT NULL,
          C_PHONE       CHAR(15) NOT NULL,
          C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
          C_MKTSEGMENT  CHAR(10) NOT NULL,
          C_COMMENT     VARCHAR(117) NOT NULL
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "customer")}]'
 );
  """,
        "supplier": f"""
  CREATE TABLE SUPPLIER (
          S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,
          S_NAME        CHAR(25) NOT NULL,
          S_ADDRESS     VARCHAR(40) NOT NULL,
          S_NATIONKEY   INTEGER NOT NULL,
          S_PHONE       CHAR(15) NOT NULL,
          S_ACCTBAL     DECIMAL(15,2) NOT NULL,
          S_COMMENT     VARCHAR(101) NOT NULL
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "supplier")}]'
);
  """,
        "partsupp": f"""
  CREATE TABLE PARTSUPP (
          PS_PARTKEY     INTEGER NOT NULL,
          PS_SUPPKEY     INTEGER NOT NULL,
          PS_AVAILQTY    INTEGER NOT NULL,
          PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
          PS_COMMENT     VARCHAR(199) NOT NULL,
          PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "partsupp")}]'

);
  """,
        "nation": f"""
  CREATE TABLE NATION  (
          N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY,
          N_NAME       CHAR(25) NOT NULL,
          N_REGIONKEY  INTEGER NOT NULL,
          N_COMMENT    VARCHAR(152)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "nation")}]'

);
  """,
        "region": f"""
  CREATE TABLE REGION  (
          R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,
          R_NAME       CHAR(25) NOT NULL,
          R_COMMENT    VARCHAR(152)
  ) WITH (
    'materialized' = 'true',
    'connectors' = '[{input_connector(config, "region")}]'
);
""",
    }


def tpch_views(q_dirs: List[str]) -> List[ViewSpec]:
    return [
        ViewSpec(
            "q1",
            """
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
            connectors=f"""[{{
        "index": "q1_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[1]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q1_idx", ["l_returnflag", "l_linestatus"])],
        ),
        ViewSpec(
            "q2",
            """
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
        ),
        ViewSpec(
            "q3",
            """
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
            connectors=f"""[{{
        "index": "q3_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[3]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[
                IndexSpec("q3_idx", ["l_orderkey", "o_orderdate", "o_shippriority"])
            ],
        ),
        ViewSpec(
            "q4",
            """
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
            connectors=f"""[{{
        "index": "q4_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[4]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q4_idx", ["o_orderpriority"])],
        ),
        ViewSpec(
            "q5",
            """
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
            connectors=f"""[{{
        "index": "q5_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[5]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q5_idx", ["n_name"])],
        ),
        ViewSpec(
            "q6",
            """
  select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.08 - 0.01 and 0.08 + 0.01
    and l_quantity < 24""",
            connectors=f"""[{{
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[6]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
        ),
        #     ViewSpec("q7", """
        #   select
        #     supp_nation,
        #     cust_nation,
        #     l_year,
        #     sum(volume) as revenue
        # from
        #     (
        #         select
        #             n1.n_name as supp_nation,
        #             n2.n_name as cust_nation,
        #             extract(year from l_shipdate) as l_year,
        #             l_extendedprice * (1 - l_discount) as volume
        #         from
        #             supplier,
        #             lineitem,
        #             orders,
        #             customer,
        #             nation n1,
        #             nation n2
        #         where
        #             s_suppkey = l_suppkey
        #             and o_orderkey = l_orderkey
        #             and c_custkey = o_custkey
        #             and s_nationkey = n1.n_nationkey
        #             and c_nationkey = n2.n_nationkey
        #             and (
        #                 (n1.n_name = 'ROMANIA' and n2.n_name = 'INDIA')
        #                 or (n1.n_name = 'INDIA' and n2.n_name = 'ROMANIA')
        #             )
        #             and l_shipdate between date '1995-01-01' and date '1996-12-31'
        #     ) as shipping
        # group by
        #     supp_nation,
        #     cust_nation,
        #     l_year
        # order by
        #     supp_nation,
        #     cust_nation,
        #     l_year""",
        #     connectors=f"""[{{
        #         "index": "q7_idx",
        #         "transport": {{
        #             "name": "delta_table_output",
        #             "config": {{
        #                 "uri": "file://{q_dirs[7]}",
        #                 "mode": "truncate"
        #             }}
        #         }},
        #         "enable_output_buffer": true,
        #         "max_output_buffer_time_millis": 2000
        #     }}]""",
        #     indexes=[IndexSpec("q7_idx", ["supp_nation", "cust_nation", "l_year"])]
        # ),
        ViewSpec(
            "q8",
            """
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
            connectors=f"""[{{
        "index": "q8_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[8]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q8_idx", ["o_year"])],
        ),
        ViewSpec(
            "q9",
            """
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
            connectors=f"""[{{
        "index": "q9_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[9]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q9_idx", ["nation", "o_year"])],
        ),
        ViewSpec(
            "q10",
            """
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
            connectors=f"""[{{
        "index": "q10_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[10]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q10_idx", ["c_custkey"])],
        ),
        ViewSpec(
            "q11",
            """
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
    ps_partkey
having
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
            connectors=f"""[{{
        "index": "q11_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[11]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q11_idx", ["ps_partkey"])],
        ),
        ViewSpec(
            "q12",
            """
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
            connectors=f"""[{{
        "index": "q12_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[12]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q12_idx", ["l_shipmode"])],
        ),
        ViewSpec(
            "q13",
            """
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
            connectors=f"""[{{
        "index": "q13_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[13]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q13_idx", ["c_count"])],
        ),
        ViewSpec(
            "q14",
            """
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
        ),
        ViewSpec(
            "q15",
            """
    with revenue0 as (select
            l_suppkey as supplier_no,
            sum(l_extendedprice * (1 - l_discount)) as total_revenue
        from
            lineitem
        where
            l_shipdate >= date '1993-01-01'
            and l_shipdate < date '1993-01-01' + interval '3' month
        group by
            l_suppkey)
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
        ),
        ViewSpec(
            "q16",
            """
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
            connectors=f"""[{{
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[16]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
        ),
        ViewSpec(
            "q17",
            """
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
        ),
        ViewSpec(
            "q18",
            """
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
            connectors=f"""[{{
        "index": "q18_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[18]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q18_idx", ["c_custkey", "o_orderkey"])],
        ),
        ViewSpec(
            "q19",
            """
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
        ),
        ViewSpec(
            "q20",
            """
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
        ),
        ViewSpec(
            "q21",
            """
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
            connectors=f"""[{{
        "index": "q21_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[21]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q21_idx", ["s_name"])],
        ),
        ViewSpec(
            "q22",
            """
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
            connectors=f"""[{{
        "index": "q22_idx",
        "transport": {{
            "name": "delta_table_output",
            "config": {{
                "uri": "file://{q_dirs[22]}",
                "mode": "truncate"
            }}
        }},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 2000
    }}]""",
            indexes=[IndexSpec("q22_idx", ["cntrycode"])],
        ),
    ]


def tpch_test_segment(
    pipeline: Pipeline,
    tables: dict,
    views: List[ViewSpec],
    expected_processed_records,
    segment_size: int,
) -> tuple[bool, int]:
    """Run a test segment.

    Start the pipeline (from a checkpoint if one exists), run a series of transactions followed by streaming ingest periods,
    until the pipeline processed segment_size records. A checkpoint is created halfway through the segment, or at the end. The
    pipeline is then paused,outputs validated, and the pipeline stopped.
    """

    # Start pipeline.
    start_time = time.monotonic()
    log(
        f"Starting pipeline to process {segment_size} records, starting from (approximately) {expected_processed_records} processed records"
    )
    pipeline.start()
    log(f"Pipeline started in {time.monotonic() - start_time} seconds")

    # Current number of processed records.
    initial_processed_records = number_of_processed_records(pipeline)

    log(
        f"Initial processed records at the start of a segment: {initial_processed_records}"
    )

    if initial_processed_records < expected_processed_records:
        raise RuntimeError(
            f"Expected at least {expected_processed_records} processed records on startup, got {initial_processed_records}"
        )

    # Expected number of processed records after this segment.
    expected_processed_records = initial_processed_records + segment_size

    # Make a checkpoint halfway through the segment after processing this many records.
    halfway_processed_records = (
        initial_processed_records + expected_processed_records
    ) >> 1

    checkpoint = False

    while not pipeline.is_complete():
        current_processed_records = number_of_processed_records(pipeline)
        log(
            f"Processed {current_processed_records} total records so far (processed {current_processed_records - initial_processed_records} records in this segment)"
        )

        if current_processed_records >= expected_processed_records:
            log("Сompleting test segment")
            break

        # Transaction
        transaction(pipeline, 100)
        check_for_endpoint_errors(pipeline)

        # Streaming ingest (no transaction)
        log("Running streaming ingest for 10 seconds")
        time.sleep(10)
        check_for_endpoint_errors(pipeline)

        if not checkpoint:
            processed_before_checkpoint = number_of_processed_records(pipeline)

            if processed_before_checkpoint >= halfway_processed_records:
                log(
                    f"Creating checkpoint after processing {processed_before_checkpoint} records"
                )
                checkpoint_pipeline(pipeline)
                checkpoint = True

    if not checkpoint:
        processed_before_checkpoint = number_of_processed_records(pipeline)
        log(
            f"Creating checkpoint at the end of the segment after processing {processed_before_checkpoint} records"
        )
        checkpoint(pipeline)

    pipeline.pause()

    # validate outputs
    validate_outputs(pipeline, tables, views)

    complete = pipeline.is_complete()

    log("Stopping pipeline")
    pipeline.stop(force=True)

    return (complete, processed_before_checkpoint)


def tpch_test(config: TPCHTestConfig):
    # Directory to store output delta tables.
    output_dir = tempfile.mkdtemp()

    q_dirs = {}

    for q in range(1, 23):
        q_dirs[q] = os.path.join(output_dir, f"q{q}")
        os.makedirs(q_dirs[q], exist_ok=False)
        log(f"Created directory {q_dirs[q]} for query q{q} output")

    tables = tpch_tables(config)
    views = tpch_views(q_dirs)

    if config.mode == "checkpoint":
        pipeline = build_pipeline(
            unique_pipeline_name("tpc-h-checkpoint"), tables, views
        )

        last_processed = 0
        iteration = 1
        modified_views = views
        while True:
            (complete, last_processed) = tpch_test_segment(
                pipeline, tables, modified_views, last_processed, 100000000
            )
            if complete:
                break

            iteration += 1
            modified_views = list(
                map(
                    lambda view: view.clone_with_name(f"{view.name}_{iteration}"), views
                )
            )

            sql = generate_program(tables, modified_views)
            pipeline.modify(sql=sql)

    elif config.mode == "transaction":
        run_workload(
            unique_pipeline_name("tpc-h-transaction"), tables, views, transaction=True
        )
    elif config.mode == "stream":
        run_workload(
            unique_pipeline_name("tpc-h-stream"), tables, views, transaction=False
        )
    else:
        raise RuntimeError(f"Unknown mode: {config.mode}")


class TestTPCH(unittest.TestCase):
    def test_tpch_transaction(self):
        config = TPCHTestConfig(
            "transaction",
            "delta",
            s3_path="s3://batchtofeldera",
            s3_region="ap-southeast-2",
        )
        tpch_test(config)

    def test_tpch_stream(self):
        config = TPCHTestConfig(
            "stream", "delta", s3_path="s3://batchtofeldera", s3_region="ap-southeast-2"
        )
        tpch_test(config)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_cli()
    else:
        unittest.main()
