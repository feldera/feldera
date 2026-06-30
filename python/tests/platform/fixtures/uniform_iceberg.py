"""Build a UC-Uniform-over-Iceberg-shaped Delta table fixture with pyarrow.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess
(``uv run --with pyarrow python uniform_iceberg.py <output_dir>``). Unlike the
column-mapping fixture, this one cannot be written by Delta Spark: it reproduces
what a *native Iceberg writer* (Flink / pyiceberg, exposed as Delta by Unity
Catalog Uniform) puts on disk -- Parquet columns named by their **logical** names
carrying a Parquet ``field_id`` -- while the Delta log uses ``columnMapping.mode =
'id'``. We therefore emit the Parquet with pyarrow and hand-write the
``_delta_log``.

The table mirrors the customer's ``cdc_raw`` shape:

* ``id``     -- physical name ``col-100`` *diverges* from the on-disk ``id``
               (a nullable scalar resolved by field id).
* ``after``  -- a six-field nested struct whose children *also* diverge: each is
               mapped to ``col-<id>`` while the file names them logically
               (``transaction__id`` ...). Under by-name resolution the struct cast
               target (``col-104`` ...) shares no overlap with the file's logical
               child names, the customer's "Cannot cast struct with 6 fields to 6
               fields because there is no field name overlap".
* ``op``     -- physical name ``col-102`` *diverges* and is **non-nullable**.
               This is the column that fails an unpatched read with
               ``Non-nullable column 'col-102' is missing from the physical schema``.

A correct snapshot read resolves every column -- top level and the struct's
children -- by Parquet ``field_id``, so the diverging columns come back with their
real values rather than NULL (or a hard error for ``op`` / the struct cast).

Bump ``FIXTURE_VERSION`` in ``test_delta_input_uniform_iceberg.py`` on any change
here, since a cached fixture is reused based on its path alone.
"""

from __future__ import annotations

import json
import os
import sys


# (logical child name, field id) for the six-field ``after`` struct. Children are
# mapped to diverging physical names ``col-<id>`` in the Delta log below.
AFTER_CHILDREN = [
    ("transaction__id", 104),
    ("transaction__merchant_id", 105),
    ("transaction__merchant_name", 106),
    ("transaction__time", 107),
    ("transaction__status", 108),
    ("transaction__amount", 109),
]


# Final logical rows expected from a snapshot read. Shared with the test via import
# so the two never drift. ``after`` is a nested row.
EXPECTED_ROWS = [
    {
        "id": "txn-001",
        "after": {
            "transaction__id": "txn-001",
            "transaction__merchant_id": "m-100",
            "transaction__merchant_name": "Coffee Shop",
            "transaction__time": "2026-01-01T00:00:00Z",
            "transaction__status": "settled",
            "transaction__amount": "12.50",
        },
        "op": "c",
    },
    {
        "id": "txn-002",
        "after": {
            "transaction__id": "txn-002",
            "transaction__merchant_id": "m-200",
            "transaction__merchant_name": "Gas Station",
            "transaction__time": "2026-01-02T00:00:00Z",
            "transaction__status": "settled",
            "transaction__amount": "40.00",
        },
        "op": "c",
    },
    {
        "id": "txn-003",
        "after": {
            "transaction__id": "txn-003",
            "transaction__merchant_id": "m-300",
            "transaction__merchant_name": "Restaurant",
            "transaction__time": "2026-01-03T00:00:00Z",
            "transaction__status": "reversed",
            "transaction__amount": "75.25",
        },
        "op": "u",
    },
]


def _field_id(value: int) -> dict:
    return {b"PARQUET:field_id": str(value).encode()}


def build(table_path: str) -> None:
    """Create the UC-Uniform-over-Iceberg table at ``table_path``."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    after_type = pa.struct(
        [
            pa.field(name, pa.string(), metadata=_field_id(field_id))
            for name, field_id in AFTER_CHILDREN
        ]
    )
    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=True, metadata=_field_id(100)),
            pa.field("after", after_type, nullable=True, metadata=_field_id(103)),
            pa.field("op", pa.string(), nullable=False, metadata=_field_id(102)),
        ]
    )
    table = pa.table(
        {
            "id": [r["id"] for r in EXPECTED_ROWS],
            "after": [r["after"] for r in EXPECTED_ROWS],
            "op": [r["op"] for r in EXPECTED_ROWS],
        },
        schema=schema,
    )

    parquet_rel = "part-00000-uniform-iceberg.parquet"
    log_dir = os.path.join(table_path, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)
    # store_schema=False keeps the file Iceberg-shaped (no embedded Arrow schema);
    # the field_ids are written into the Parquet schema regardless.
    pq.write_table(table, os.path.join(table_path, parquet_rel), store_schema=False)
    size = os.path.getsize(os.path.join(table_path, parquet_rel))

    _write_log(log_dir, parquet_rel, size)


def _column_metadata(field_id: int, physical_name: str) -> dict:
    return {
        "delta.columnMapping.id": field_id,
        "delta.columnMapping.physicalName": physical_name,
    }


def _schema_string() -> str:
    # Every field -- top level and the struct's children -- uses a diverging
    # `col-<id>` physical name, exactly as a native Iceberg writer's Uniform log
    # does, so a correct read must resolve by field id at every level.
    after_type = {
        "type": "struct",
        "fields": [
            {
                "name": name,
                "type": "string",
                "nullable": True,
                "metadata": _column_metadata(field_id, f"col-{field_id}"),
            }
            for name, field_id in AFTER_CHILDREN
        ],
    }
    fields = [
        {
            "name": "id",
            "type": "string",
            "nullable": True,
            "metadata": _column_metadata(100, "col-100"),
        },
        {
            "name": "after",
            "type": after_type,
            "nullable": True,
            "metadata": _column_metadata(103, "col-103"),
        },
        {
            "name": "op",
            "type": "string",
            "nullable": False,
            "metadata": _column_metadata(102, "col-102"),
        },
    ]
    return json.dumps({"type": "struct", "fields": fields})


def _write_log(log_dir: str, parquet_rel: str, size: int) -> None:
    # Legacy column-mapping protocol (reader 2 / writer 5): no feature lists,
    # which the kernel rejects below reader version 3.
    protocol = {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}
    metadata = {
        "metaData": {
            "id": "uniform-iceberg-fixture-0000-0000-000000000000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": _schema_string(),
            "partitionColumns": [],
            "configuration": {
                "delta.columnMapping.mode": "id",
                "delta.columnMapping.maxColumnId": "109",
            },
            "createdTime": 1700000000000,
        }
    }
    add = {
        "add": {
            "path": parquet_rel,
            "partitionValues": {},
            "size": size,
            "modificationTime": 1700000000000,
            "dataChange": True,
        }
    }
    with open(os.path.join(log_dir, "00000000000000000000.json"), "w") as f:
        for entry in (protocol, metadata, add):
            f.write(json.dumps(entry) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: uniform_iceberg.py <output_dir>")
    build(sys.argv[1])
