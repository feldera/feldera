#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "deltalake>=0.20",
#     "pyarrow>=15",
# ]
# ///
"""Publish TPC-H Delta tables to the public demo-data bucket.

The accelerating-batch-analytics demo and python/tests/workloads/test_tpch.py read
these tables anonymously. Regenerate them with:

    ./scripts/tpch_demo_data.py --scale-factor 0.01 --scale-factor 0.1 --scale-factor 1

Data comes from the official TPC-H dbgen (via the tpch-kit packaging, which adds a
macOS build and drops dbgen's trailing row delimiter but leaves the generator
untouched). Uploading needs write credentials for the bucket; readers need none.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
from deltalake import write_deltalake

TPCH_KIT_REPO = "https://github.com/gregrahn/tpch-kit.git"

DECIMAL = pa.decimal128(15, 2)


def column(name: str, arrow_type: pa.DataType, nullable: bool = False) -> pa.Field:
    return pa.field(name, arrow_type, nullable=nullable)


# Column order must match dbgen's output, which follows the TPC-H spec. Only
# n_comment and r_comment are nullable, mirroring the demo's CREATE TABLE.
SCHEMAS: dict[str, pa.Schema] = {
    "lineitem": pa.schema(
        [
            column("l_orderkey", pa.int32()),
            column("l_partkey", pa.int32()),
            column("l_suppkey", pa.int32()),
            column("l_linenumber", pa.int32()),
            column("l_quantity", DECIMAL),
            column("l_extendedprice", DECIMAL),
            column("l_discount", DECIMAL),
            column("l_tax", DECIMAL),
            column("l_returnflag", pa.string()),
            column("l_linestatus", pa.string()),
            column("l_shipdate", pa.date32()),
            column("l_commitdate", pa.date32()),
            column("l_receiptdate", pa.date32()),
            column("l_shipinstruct", pa.string()),
            column("l_shipmode", pa.string()),
            column("l_comment", pa.string()),
        ]
    ),
    "orders": pa.schema(
        [
            column("o_orderkey", pa.int32()),
            column("o_custkey", pa.int32()),
            column("o_orderstatus", pa.string()),
            column("o_totalprice", DECIMAL),
            column("o_orderdate", pa.date32()),
            column("o_orderpriority", pa.string()),
            column("o_clerk", pa.string()),
            column("o_shippriority", pa.int32()),
            column("o_comment", pa.string()),
        ]
    ),
    "part": pa.schema(
        [
            column("p_partkey", pa.int32()),
            column("p_name", pa.string()),
            column("p_mfgr", pa.string()),
            column("p_brand", pa.string()),
            column("p_type", pa.string()),
            column("p_size", pa.int32()),
            column("p_container", pa.string()),
            column("p_retailprice", DECIMAL),
            column("p_comment", pa.string()),
        ]
    ),
    "customer": pa.schema(
        [
            column("c_custkey", pa.int32()),
            column("c_name", pa.string()),
            column("c_address", pa.string()),
            column("c_nationkey", pa.int32()),
            column("c_phone", pa.string()),
            column("c_acctbal", DECIMAL),
            column("c_mktsegment", pa.string()),
            column("c_comment", pa.string()),
        ]
    ),
    "supplier": pa.schema(
        [
            column("s_suppkey", pa.int32()),
            column("s_name", pa.string()),
            column("s_address", pa.string()),
            column("s_nationkey", pa.int32()),
            column("s_phone", pa.string()),
            column("s_acctbal", DECIMAL),
            column("s_comment", pa.string()),
        ]
    ),
    "partsupp": pa.schema(
        [
            column("ps_partkey", pa.int32()),
            column("ps_suppkey", pa.int32()),
            column("ps_availqty", pa.int32()),
            column("ps_supplycost", DECIMAL),
            column("ps_comment", pa.string()),
        ]
    ),
    "nation": pa.schema(
        [
            column("n_nationkey", pa.int32()),
            column("n_name", pa.string()),
            column("n_regionkey", pa.int32()),
            column("n_comment", pa.string(), nullable=True),
        ]
    ),
    "region": pa.schema(
        [
            column("r_regionkey", pa.int32()),
            column("r_name", pa.string()),
            column("r_comment", pa.string(), nullable=True),
        ]
    ),
}

# Canonical row counts at scale factor 1, straight from the TPC-H spec. Every
# other scale factor scales linearly except nation and region, which are fixed.
SF1_ROW_COUNTS = {
    "lineitem": 6001215,
    "orders": 1500000,
    "partsupp": 800000,
    "part": 200000,
    "customer": 150000,
    "supplier": 10000,
    "nation": 25,
    "region": 5,
}


def build_dbgen(work_dir: Path) -> Path:
    """Clone and build the official dbgen, returning the directory holding it."""
    dbgen_dir = work_dir / "tpch-kit" / "dbgen"
    if (dbgen_dir / "dbgen").exists():
        return dbgen_dir

    print(f"Cloning {TPCH_KIT_REPO}")
    subprocess.run(
        ["git", "clone", "--depth", "1", TPCH_KIT_REPO, str(work_dir / "tpch-kit")],
        check=True,
    )

    machine = "MACOS" if sys.platform == "darwin" else "LINUX"
    print(f"Building dbgen for {machine}")
    subprocess.run(
        ["make", f"MACHINE={machine}", "DATABASE=POSTGRESQL"],
        cwd=dbgen_dir,
        check=True,
    )
    return dbgen_dir


def generate_tables(dbgen_dir: Path, scale_factor: float, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Generating scale factor {scale_factor} into {out_dir}")
    subprocess.run(
        [str(dbgen_dir / "dbgen"), "-s", str(scale_factor), "-f", "-q"],
        cwd=dbgen_dir,
        env={**os.environ, "DSS_PATH": str(out_dir)},
        check=True,
    )


def read_table(tbl_path: Path, schema: pa.Schema) -> pa.Table:
    """Parse a dbgen .tbl file into an Arrow table with the TPC-H schema."""
    return pacsv.read_csv(
        tbl_path,
        read_options=pacsv.ReadOptions(column_names=schema.names),
        parse_options=pacsv.ParseOptions(delimiter="|", quote_char=False),
        convert_options=pacsv.ConvertOptions(
            column_types={f.name: f.type for f in schema},
            # dbgen never emits an empty field, so an empty string is data, not a
            # null. Passing no null strings keeps it that way.
            null_values=[],
            strings_can_be_null=False,
        ),
    ).cast(schema)


def check_row_count(table: str, scale_factor: float, actual: int) -> None:
    """Reject a truncated generate or a parse that split rows."""
    if table in ("nation", "region"):
        expected = SF1_ROW_COUNTS[table]
    else:
        expected = round(SF1_ROW_COUNTS[table] * scale_factor)

    # lineitem carries 1-7 rows per order, so its total drifts a little with the
    # generator's random draw. Every other table scales exactly.
    tolerance = max(10, round(expected * 0.05)) if table == "lineitem" else 0

    if abs(actual - expected) > tolerance:
        raise SystemExit(
            f"{table} at sf{scale_factor:g}: parsed {actual} rows, expected "
            f"{expected}" + (f" +/- {tolerance}" if tolerance else "")
        )


def publish(
    tbl_dir: Path, target_root: str, scale_factor: float, storage_options: dict
) -> None:
    for table, schema in SCHEMAS.items():
        arrow_table = read_table(tbl_dir / f"{table}.tbl", schema)
        check_row_count(table, scale_factor, arrow_table.num_rows)

        uri = f"{target_root}/{table}"
        print(f"  {table}: {arrow_table.num_rows} rows -> {uri}")
        write_deltalake(
            uri,
            arrow_table,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=storage_options,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale-factor",
        type=float,
        action="append",
        dest="scale_factors",
        help="TPC-H scale factor; repeat to publish several (default: 0.01, 0.1, 1)",
    )
    parser.add_argument(
        "--bucket",
        default="feldera-demo-datasets",
        help="Destination S3 bucket (default: %(default)s)",
    )
    parser.add_argument(
        "--prefix",
        default="tpch",
        help="Key prefix under the bucket (default: %(default)s)",
    )
    parser.add_argument(
        "--region",
        default="us-west-1",
        help="Bucket region (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Write Delta tables here instead of S3, for a local dry run",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Keep dbgen and the .tbl files here instead of a temporary directory",
    )
    args = parser.parse_args()

    scale_factors = args.scale_factors or [0.01, 0.1, 1]

    work_dir = args.work_dir or Path(tempfile.mkdtemp(prefix="tpch-"))
    work_dir.mkdir(parents=True, exist_ok=True)
    keep_work_dir = args.work_dir is not None

    try:
        dbgen_dir = build_dbgen(work_dir)

        for scale_factor in scale_factors:
            label = f"sf{scale_factor:g}"
            tbl_dir = work_dir / f"tbl-{label}"
            generate_tables(dbgen_dir, scale_factor, tbl_dir)

            if args.output_dir:
                target_root = str((args.output_dir / label).resolve())
                storage_options = {}
            else:
                target_root = f"s3://{args.bucket}/{args.prefix}/{label}"
                storage_options = {"AWS_REGION": args.region}

            print(f"Publishing {label} to {target_root}")
            publish(tbl_dir, target_root, scale_factor, storage_options)
    finally:
        if not keep_work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)

    print("Done.")


if __name__ == "__main__":
    main()
