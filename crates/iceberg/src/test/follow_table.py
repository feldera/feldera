# Create and incrementally append to an Iceberg table through a REST catalog,
# for the connector's follow-mode tests. Each `append` produces a new snapshot
# the connector must pick up.
#
# The table schema matches `IcebergTestStruct` in the Rust tests, so the same
# `data()` generator and `file_to_zset` assertions apply.
#
# Connection settings come from the environment (defaults target the local
# docker setup in crates/iceberg/src/test/README.md):
#   FELDERA_ICEBERG_REST_URI      (default http://localhost:8181)
#   FELDERA_ICEBERG_S3_ENDPOINT   (default http://localhost:9000)
#   FELDERA_ICEBERG_S3_KEY        (default minio)
#   FELDERA_ICEBERG_S3_SECRET     (default miniopasswd)
#   FELDERA_ICEBERG_S3_REGION     (default us-east-1)

import argparse
import os
from decimal import Decimal

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    BinaryType,
    DateType,
    DoubleType,
    DecimalType,
    FloatType,
    FixedType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
)

# Iceberg schema (matches `IcebergTestStruct`).
SCHEMA = Schema(
    NestedField(1, "b", BooleanType(), required=True),
    NestedField(2, "i", IntegerType(), required=True),
    NestedField(3, "l", LongType(), required=True),
    NestedField(4, "r", FloatType(), required=True),
    NestedField(5, "d", DoubleType(), required=True),
    NestedField(6, "dec", DecimalType(10, 3), required=True),
    NestedField(7, "dt", DateType(), required=True),
    NestedField(8, "tm", TimeType(), required=True),
    NestedField(9, "ts", TimestampType(), required=True),
    NestedField(10, "s", StringType(), required=True),
    NestedField(11, "fixed", FixedType(5), required=True),
    NestedField(12, "varbin", BinaryType(), required=True),
    NestedField(13, "tstz", TimestamptzType(), required=True),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("b", pa.bool_(), nullable=False),
        pa.field("i", pa.int32(), nullable=False),
        pa.field("l", pa.int64(), nullable=False),
        pa.field("r", pa.float32(), nullable=False),
        pa.field("d", pa.float64(), nullable=False),
        pa.field("dec", pa.decimal128(10, 3), nullable=False),
        pa.field("dt", pa.date32(), nullable=False),
        pa.field("tm", pa.time64("us"), nullable=False),
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("s", pa.string(), nullable=False),
        pa.field("fixed", pa.binary(5), nullable=False),
        pa.field("varbin", pa.binary(), nullable=False),
        pa.field("tstz", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="date")
)


def catalog():
    return RestCatalog(
        "follow",
        **{
            "uri": os.getenv("FELDERA_ICEBERG_REST_URI", "http://localhost:8181"),
            "s3.endpoint": os.getenv(
                "FELDERA_ICEBERG_S3_ENDPOINT", "http://localhost:9000"
            ),
            "s3.access-key-id": os.getenv("FELDERA_ICEBERG_S3_KEY", "minio"),
            "s3.secret-access-key": os.getenv(
                "FELDERA_ICEBERG_S3_SECRET", "miniopasswd"
            ),
            "s3.region": os.getenv("FELDERA_ICEBERG_S3_REGION", "us-east-1"),
        },
    )


def arrow_chunk(json_file):
    """Load an ndjson chunk (the format `data_to_ndjson` writes) into an Arrow
    table matching the Iceberg schema."""
    df = pd.read_json(json_file, lines=True)
    df["tm"] = pd.to_datetime(df["tm"]).dt.time
    df["ts"] = pd.to_datetime(df["ts"]).astype("datetime64[us]")
    df["tstz"] = pd.to_datetime(df["tstz"], utc=True).astype("datetime64[us, UTC]")
    df["dt"] = pd.to_datetime(df["dt"]).dt.date
    df["dec"] = df["dec"].apply(lambda x: Decimal(f"{x:.3f}"))
    df["fixed"] = df["fixed"].apply(lambda x: bytes(x))
    df["varbin"] = df["varbin"].apply(lambda x: bytes(x))
    return pa.Table.from_pandas(df, schema=ARROW_SCHEMA)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--op", choices=["create", "append", "overwrite"], required=True
    )
    parser.add_argument("--table", required=True, help="table as 'namespace.name'")
    parser.add_argument("--json-file", required=True, help="ndjson chunk to append")
    args = parser.parse_args()

    cat = catalog()
    namespace = args.table.split(".")[0]

    if args.op == "create":
        try:
            cat.create_namespace(namespace)
        except Exception:
            pass
        try:
            cat.drop_table(args.table)
        except Exception:
            pass
        table = cat.create_table(args.table, SCHEMA, partition_spec=PARTITION_SPEC)
    else:
        table = cat.load_table(args.table)

    chunk = arrow_chunk(args.json_file)
    if args.op == "overwrite":
        # Copy-on-write rewrite: removes the old data files, adds `chunk`.
        table.overwrite(chunk)
    else:
        table.append(chunk)
    # Print the current snapshot id so the caller can log progress.
    print(table.metadata.current_snapshot_id)


if __name__ == "__main__":
    main()
