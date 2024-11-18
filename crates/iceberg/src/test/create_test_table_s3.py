# Script used to create a table that matches the defintion of TestStruct2

from decimal import Decimal
import random
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    LongType,
    MapType,
    StringType,
    StructType,
    NestedField,
    TimeType,
    TimestampType,
    IntegerType,
    FloatType,
    DoubleType,
    DecimalType,
    BinaryType,
    FixedType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from datetime import time, timedelta

import datetime
import os
import sys
import pyarrow as pa
import pandas as pd
import numpy as np
import argparse

parser = argparse.ArgumentParser(
    description="Create an Iceberg table populated with random data"
)

parser.add_argument(
    "--catalog",
    choices=["glue", "rest", "sql"],
    default="sql",
    help="Catalog type (default: sql)",
)
parser.add_argument(
    "--warehouse-path",
    default="/tmp/warehouse",
    help="Location to create the warehouse; only used in conjunction with '--catalog=sql' in (default: /tmp/warehouse)",
)
parser.add_argument(
    "--rows",
    type=int,
    default=1000000,
    help="Number of rows to generate (default: 1000000)",
)
parser.add_argument("--json-file", help="JSON file to load data from")


args = parser.parse_args()


if args.catalog == "glue":
    from pyiceberg.catalog.glue import GlueCatalog

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key_id:
        print("Error: AWS_ACCESS_KEY_ID is not set")
        sys.exit(1)

    if not aws_secret_access_key:
        print("Error: AWS_SECRET_ACCESS_KEY is not set")
        sys.exit(1)

    print("Connecting to Glue catalog")
    catalog = GlueCatalog(
        "glue",
        **{
            "glue.access-key-id": aws_access_key_id,
            "glue.secret-access-key": aws_secret_access_key,
            "glue.region": "us-east-1",
            "s3.access-key-id": aws_access_key_id,
            "s3.secret-access-key": aws_secret_access_key,
            "s3.region": "us-east-1",
        },
    )
    location = "s3://feldera-iceberg-test/test_table"
elif args.catalog == "rest":
    print("REST catalog not yet supported")
    exit(1)
else:
    warehouse_path = args.warehouse_path
    location = f"{warehouse_path}/test_table"

    print(f"Creating SQL catalog at {warehouse_path}")

    catalog = SqlCatalog(
        "sql",
        **{
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    try:
        catalog.create_namespace("iceberg_test")
    except:
        pass

# Iceberg schema (matches `IcebergTestStruct`)
schema = Schema(
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
    # NestedField(11, "uuid", UUIDType(), required=True),
    NestedField(11, "fixed", FixedType(5), required=True),
    NestedField(12, "varbin", BinaryType(), required=True),
)

# Equivalent arrow schema
arrow_schema = pa.schema(
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
        # pa.field("uuid", pa.binary(16), nullable=False),
        pa.field("fixed", pa.binary(5), nullable=False),
        pa.field("varbin", pa.binary(), nullable=False),
    ]
)

partition_spec = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="date")
)

try:
    print("Deleting existing table, if any")
    catalog.drop_table("iceberg_test.test_table")
except:
    pass

print("Creating Iceberg table")

table = catalog.create_table(
    "iceberg_test.test_table", schema, location=location, partition_spec=partition_spec
)

# Number of records
num_records = args.rows

print(f"Generating {num_records} rows")

# Generate random data
if args.json_file:
    with open(args.json_file, "r") as file:
        for i, line in enumerate(file):
            print(line)
            if i == 4:
                break

    pandas_df = pd.read_json(args.json_file, lines=True)
    pandas_df["tm"] = pd.to_datetime(pandas_df["tm"]).dt.time
    pandas_df["ts"] = pd.to_datetime(pandas_df["ts"])
    pandas_df["dt"] = pd.to_datetime(pandas_df["dt"]).dt.date
    pandas_df['dec'] = pandas_df["dec"].apply(lambda x: Decimal(f"{x:.3f}"))
    # pandas_df['uuid'] = pandas_df['uuid'].apply(lambda x: bytes(x))
    pandas_df['fixed'] = pandas_df['fixed'].apply(lambda x: bytes(x))
    pandas_df['varbin'] = pandas_df['varbin'].apply(lambda x: bytes(x))

else:
    # Generate a range of dates between 2024-01-01 and 2024-12-31
    date_range = pd.date_range(start='2024-01-01', end='2024-12-31')

    data = {
        "b": np.random.choice([True, False], size=num_records),  # Boolean
        "i": np.arange(1, num_records + 1, dtype=np.int32),
        "l": np.random.randint(np.iinfo(np.int64).min, np.iinfo(np.int64).max, size=num_records, dtype=np.int64),  # int64
        "r": np.random.uniform(-1e6, 1e6, size=num_records).astype(np.float32),  # float32
        "d": np.random.uniform(-1e12, 1e12, size=num_records).astype(np.float64),  # float64
        "dec": [Decimal(random.uniform(-1e5, 1e5)).quantize(Decimal("0.001")) for _ in range(num_records)],
        #"dt": pd.date_range(start="2000-01-01", periods=num_records, freq="D").date,  # date32
        "dt": [date_range[i % len(date_range)] for i in range(num_records)],
        "tm": [time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59), random.randint(0, 999999)) for _ in range(num_records)],
        "ts": [
            datetime.datetime(2023, 1, 1) + datetime.timedelta(seconds=i)
            for i in range(num_records)
        ],
        "s": [f"string_{i}" for i in range(num_records)],  # string
        # "uuid": [uuid.uuid4().bytes for _ in range(num_records)],  # binary(16) - UUID
        "fixed": [np.random.bytes(5) for _ in range(num_records)],  # fixed binary(5)
        "varbin": [np.random.bytes(np.random.randint(1, 20)) for _ in range(num_records)]  # variable-length binary
    }

    # Create the DataFrame
    pandas_df = pd.DataFrame(data)

    #print(pandas_df.head())

print("Generating Pandas dataframe")

# pyiceberg does not support nanosecond timestamps
pandas_df["ts"] = pandas_df["ts"].astype("datetime64[us]")

print("Converting Pandas dataframe to Arrow")

arrow_table = pa.Table.from_pandas(pandas_df, schema=arrow_schema)

print("Writing data to Iceberg table")

table.append(arrow_table)

# Extract and print metadata location.
print(table.inspect.metadata_log_entries().column("file")[-1].as_py())
