"""Schema of the Iceberg test table, shared by the scripts that write it.

The columns match `IcebergTestStruct` in the Rust tests; a column added here
must be added there too.
"""

from decimal import Decimal

import pandas as pd
import pyarrow as pa
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


def _test_struct_fields(base):
    """`TestStruct`'s fields, numbered from `base`.

    Three columns nest this struct, and field ids must be unique across the
    whole schema, so each occurrence gets its own range.
    """
    return [
        NestedField(base, "id", LongType(), required=True),
        NestedField(base + 1, "b", BooleanType(), required=True),
        NestedField(base + 2, "i", LongType(), required=False),
        NestedField(base + 3, "s", StringType(), required=True),
    ]


ARROW_TEST_STRUCT = pa.struct(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("b", pa.bool_(), nullable=False),
        pa.field("i", pa.int64(), nullable=True),
        pa.field("s", pa.string(), nullable=False),
    ]
)

# Columns whose values are Python dicts in ndjson but (key, value) pairs in
# Arrow.
MAP_COLUMNS = ["string_string_map", "string_struct_map"]

SCHEMA_FIELDS = [
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
    NestedField(
        14,
        "string_array",
        ListType(101, StringType(), element_required=True),
        required=True,
    ),
    NestedField(15, "struct1", StructType(*_test_struct_fields(110)), required=True),
    NestedField(
        16,
        "struct_array",
        ListType(120, StructType(*_test_struct_fields(121)), element_required=True),
        required=True,
    ),
    NestedField(
        17,
        "string_string_map",
        MapType(130, StringType(), 131, StringType(), value_required=True),
        required=True,
    ),
    NestedField(
        18,
        "string_struct_map",
        MapType(
            140,
            StringType(),
            141,
            StructType(*_test_struct_fields(142)),
            value_required=True,
        ),
        required=True,
    ),
]

ARROW_FIELDS = [
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
    pa.field(
        "string_array",
        pa.list_(pa.field("element", pa.string(), nullable=False)),
        nullable=False,
    ),
    pa.field("struct1", ARROW_TEST_STRUCT, nullable=False),
    pa.field(
        "struct_array",
        pa.list_(pa.field("element", ARROW_TEST_STRUCT, nullable=False)),
        nullable=False,
    ),
    pa.field(
        "string_string_map",
        pa.map_(pa.string(), pa.field("value", pa.string(), nullable=False)),
        nullable=False,
    ),
    pa.field(
        "string_struct_map",
        pa.map_(pa.string(), pa.field("value", ARROW_TEST_STRUCT, nullable=False)),
        nullable=False,
    ),
]


def test_struct(i):
    """One `TestStruct` value, matching what the Rust `data()` generator writes."""
    return {"id": i, "b": i % 2 != 0, "i": None, "s": f"s{i}"}


def ndjson_to_pandas(json_file):
    """Load the ndjson `data_to_ndjson` writes into a frame Arrow can convert."""
    df = pd.read_json(json_file, lines=True)
    df["tm"] = pd.to_datetime(df["tm"]).dt.time
    df["ts"] = pd.to_datetime(df["ts"]).astype("datetime64[us]")
    df["tstz"] = pd.to_datetime(df["tstz"], utc=True).astype("datetime64[us, UTC]")
    df["dt"] = pd.to_datetime(df["dt"]).dt.date
    df["dec"] = df["dec"].apply(lambda x: Decimal(f"{x:.3f}"))
    df["fixed"] = df["fixed"].apply(lambda x: bytes(x))
    df["varbin"] = df["varbin"].apply(lambda x: bytes(x))
    for column in MAP_COLUMNS:
        df[column] = df[column].apply(lambda x: list(x.items()))
    return df
