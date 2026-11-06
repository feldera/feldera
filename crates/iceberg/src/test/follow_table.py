# Create and incrementally append to an Iceberg table through a REST catalog,
# for the connector's follow-mode tests. Each `append` produces a new snapshot
# the connector must pick up.
#
# The table schema matches the columns of the `iceberg` connector suite in the
# Rust tests, so the same `data()` generator and `output_zset` assertions apply.
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

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

from test_table_schema import ARROW_FIELDS, SCHEMA_FIELDS, ndjson_to_pandas

SCHEMA = Schema(*SCHEMA_FIELDS)
ARROW_SCHEMA = pa.schema(ARROW_FIELDS)

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
    return pa.Table.from_pandas(ndjson_to_pandas(json_file), schema=ARROW_SCHEMA)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--op",
        choices=[
            "create",
            "create-empty",
            "append",
            "append-files",
            "overwrite",
            "break-file",
            "restore-file",
        ],
        required=True,
    )
    parser.add_argument("--table", required=True, help="table as 'namespace.name'")
    parser.add_argument("--json-file", help="ndjson chunk to append")
    parser.add_argument("--path", help="data file to break or restore")
    args = parser.parse_args()

    cat = catalog()
    namespace = args.table.split(".")[0]

    if args.op in ("create", "create-empty"):
        try:
            cat.create_namespace(namespace)
        except Exception:
            pass
        try:
            cat.drop_table(args.table)
        except Exception:
            pass
        table = cat.create_table(args.table, SCHEMA, partition_spec=PARTITION_SPEC)
        if args.op == "create-empty":
            # A table that has no snapshot yet.
            return
    else:
        table = cat.load_table(args.table)

    if args.op in ("break-file", "restore-file"):
        # Overwrite a data file with zeros of the same size, keeping a copy, so
        # that a read fails on every store until the file is restored.
        saved = args.path + ".orig"
        src, dst = (args.path, saved) if args.op == "break-file" else (saved, args.path)
        data = table.io.new_input(src).open().read()
        table.io.new_output(dst).create(overwrite=True).write(data)
        if args.op == "break-file":
            table.io.new_output(args.path).create(overwrite=True).write(
                bytes(len(data))
            )
        return

    chunk = arrow_chunk(args.json_file)
    if args.op == "overwrite":
        # Copy-on-write rewrite: removes the old data files, adds `chunk`.
        table.overwrite(chunk)
    elif args.op == "append-files":
        # One data file per row, in one snapshot; print the files in manifest order.
        with table.transaction() as tx:
            tx.set_properties({"write.target-file-size-bytes": "1"})
            tx.append(chunk)
        snapshot = table.current_snapshot()
        for manifest in snapshot.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                if entry.snapshot_id == snapshot.snapshot_id:
                    print(entry.data_file.file_path)
        return
    else:
        table.append(chunk)
    # Print the current snapshot id so the caller can log progress.
    print(table.metadata.current_snapshot_id)


if __name__ == "__main__":
    main()
