"""Create one table (`--table`) for the follow-mode deletion-vector tests with Spark.

* `v3_dv`: a V3 table with merge-on-read DELETE, UPDATE and MERGE, a
  copy-on-write DELETE, and a compaction.
* `v2_mor`: a V2 table with a position delete file, then an append and a
  copy-on-write DELETE.
* `v3_types`: a V3 table with a column of each type that Spark writes, and
  merge-on-read DELETE and UPDATE.
* `leftover_old`, then `leftover_new`: one V3 table. Iceberg 1.9 leaves a
  deletion vector in the table when a copy-on-write DELETE removes its data
  file. Iceberg 1.10 then drops that vector in a later commit.

Prints the table's snapshot ids, final rows and expected read count as JSON
on the last line. Settings use the same environment variables as `follow_table.py`.
"""

import argparse
import fcntl
import json
import os
import tempfile

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.0"
# The last release before writers had to remove the vector of a removed file.
OLD_ICEBERG_VERSION = "1.9.2"
CATALOG = "rest"
CREATED_AT = "TIMESTAMP_NTZ '2024-01-01 00:00:00'"


def spark_session(warehouse: str, old_writer: bool) -> SparkSession:
    # The warehouse scheme picks the file IO: GCS in CI, S3 (MinIO) locally.
    gcs = warehouse.startswith("gs://")
    bundle = "iceberg-gcp-bundle" if gcs else "iceberg-aws-bundle"
    # Iceberg 1.9 has no Spark 4 runtime.
    if old_writer:
        version, runtime = OLD_ICEBERG_VERSION, "iceberg-spark-runtime-3.5_2.12"
    else:
        version, runtime = ICEBERG_VERSION, "iceberg-spark-runtime-4.0_2.13"
    catalog = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("feldera-iceberg-v3-fixture")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:{runtime}:{version},"
            f"org.apache.iceberg:{bundle}:{version}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(
            f"{catalog}.uri",
            os.getenv("FELDERA_ICEBERG_REST_URI", "http://localhost:8181"),
        )
        .config(f"{catalog}.warehouse", warehouse)
    )
    if gcs:
        builder = builder.config(
            f"{catalog}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"
        )
    else:
        builder = (
            builder.config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(
                f"{catalog}.s3.endpoint",
                os.getenv("FELDERA_ICEBERG_S3_ENDPOINT", "http://localhost:9000"),
            )
            .config(f"{catalog}.s3.path-style-access", "true")
            .config(
                f"{catalog}.s3.access-key-id",
                os.getenv("FELDERA_ICEBERG_S3_KEY", "minio"),
            )
            .config(
                f"{catalog}.s3.secret-access-key",
                os.getenv("FELDERA_ICEBERG_S3_SECRET", "miniopasswd"),
            )
            .config(
                f"{catalog}.client.region",
                os.getenv("FELDERA_ICEBERG_S3_REGION", "us-east-1"),
            )
        )
    # Test cases run in parallel, and Spark's jar cache does not lock itself.
    lock_path = os.path.join(tempfile.gettempdir(), "feldera-spark-jars.lock")
    with open(lock_path, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_table(spark, table: str, format_version: int):
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    # The identity partition gives each file and deletion vector a partition
    # value. Spark also writes `created_at` into every file.
    spark.sql(f"""
        CREATE TABLE {table} (id BIGINT NOT NULL, name STRING, created_at TIMESTAMP_NTZ)
        USING iceberg
        PARTITIONED BY (created_at)
        TBLPROPERTIES (
            'format-version' = '{format_version}',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read',
            -- Keep the `files` of `insert`, not one file per partition.
            'write.distribution-mode' = 'none',
            -- Many row groups per file, so `_pos` must count across them.
            'write.parquet.row-group-size-bytes' = '1',
            'write.parquet.row-group-check-min-record-count' = '1',
            'write.parquet.row-group-check-max-record-count' = '1'
        )
    """)


def insert(spark, table: str, start: int, end: int, files: int):
    spark.sql(f"""
        INSERT INTO {table}
        SELECT /*+ REPARTITION_BY_RANGE({files}, id) */ id, concat('s', id), {CREATED_AT}
        FROM range({start}, {end})
    """)


def set_delete_mode(spark, table: str, mode: str):
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('write.delete.mode' = '{mode}')")


def snapshots(spark, table: str):
    """The table's snapshots, oldest first, found through their parents."""
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, operation, summary FROM {table}.snapshots"
    ).collect()
    by_parent = {row["parent_id"]: row for row in rows}
    chain = [by_parent[None]]
    while chain[-1]["snapshot_id"] in by_parent:
        chain.append(by_parent[chain[-1]["snapshot_id"]])
    return chain


def current_snapshot_id(spark, table: str) -> int:
    return snapshots(spark, table)[-1]["snapshot_id"]


def follow_records(spark, table: str, after: int) -> int:
    """Rows follow mode reads after snapshot `after`: every inserted row, every
    live row of a removed file, and every newly deleted row. Compactions
    (`replace`) are skipped."""
    chain = snapshots(spark, table)
    ids = [snapshot["snapshot_id"] for snapshot in chain]
    total = 0
    for snapshot in chain[ids.index(after) + 1 :]:
        if snapshot["operation"] == "replace":
            continue
        summary = {
            key: int(value)
            for key, value in snapshot["summary"].items()
            if value.isdigit()
        }
        total += (
            summary.get("added-records", 0)
            + summary.get("deleted-records", 0)
            + summary.get("added-position-deletes", 0)
            - summary.get("removed-position-deletes", 0)
        )
    return total


def build_v3(spark, table: str) -> dict:
    create_table(spark, table, 3)
    insert(spark, table, 0, 100, files=4)
    start = current_snapshot_id(spark, table)
    # The first files keep the old spec, so a deletion vector must name it.
    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(2, id)")

    # First deletion vectors, then replacements of them.
    spark.sql(f"DELETE FROM {table} WHERE id % 10 = 0")
    spark.sql(f"DELETE FROM {table} WHERE id % 10 = 1")
    spark.sql(f"UPDATE {table} SET name = concat('u', id) WHERE id % 10 = 2")
    spark.sql(f"""
        MERGE INTO {table} t
        USING (SELECT id, concat('m', id) AS name, {CREATED_AT} AS created_at
               FROM range(95, 105)) s
        ON t.id = s.id
        WHEN MATCHED AND t.id % 2 = 0 THEN DELETE
        WHEN MATCHED THEN UPDATE SET name = s.name
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Rewrites the file holding id 55, which has a deletion vector.
    set_delete_mode(spark, table, "copy-on-write")
    spark.sql(f"DELETE FROM {table} WHERE id = 55")
    set_delete_mode(spark, table, "merge-on-read")

    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"table => '{table.split('.', 1)[1]}', options => map('rewrite-all', 'true'))"
    )
    spark.sql(f"DELETE FROM {table} WHERE id % 10 = 4")

    operations = [s["operation"] for s in snapshots(spark, table)]
    assert "replace" in operations, f"no compaction snapshot in {operations}"
    dvs = spark.sql(
        f"SELECT count(*) AS n FROM {table}.all_delete_files WHERE file_format = 'PUFFIN'"
    ).collect()[0]["n"]
    assert dvs > 0, "the writer wrote no deletion vectors"
    added = snapshots(spark, table)[0]["summary"]["added-data-files"]
    assert added == "4", f"the first insert wrote {added} data files"

    return {
        "table": table,
        "start_snapshot_id": start,
        "end_snapshot_id": current_snapshot_id(spark, table),
        "follow_records": follow_records(spark, table, start),
        "rows": [
            [row["id"], row["name"]]
            for row in spark.sql(f"SELECT id, name FROM {table} ORDER BY id").collect()
        ],
    }


def build_v2(spark, table: str) -> dict:
    create_table(spark, table, 2)
    insert(spark, table, 0, 10, files=1)
    insert(spark, table, 10, 20, files=1)
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    start = current_snapshot_id(spark, table)
    insert(spark, table, 20, 30, files=1)

    # Rewrites only the second file; the first keeps its position delete file.
    set_delete_mode(spark, table, "copy-on-write")
    spark.sql(f"DELETE FROM {table} WHERE id = 13")

    return {"table": table, "start_snapshot_id": start}


def live_leftover_vectors(spark, table: str) -> int:
    """Live deletion vectors whose data file is no longer in the table."""
    return spark.sql(f"""
        SELECT count(*) AS n FROM {table}.delete_files d
        WHERE d.file_format = 'PUFFIN'
          AND d.referenced_data_file NOT IN (SELECT file_path FROM {table}.files)
    """).collect()[0]["n"]


def build_leftover_old(spark, table: str) -> dict:
    create_table(spark, table, 3)
    insert(spark, table, 0, 10, files=1)
    start = current_snapshot_id(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    # Rewrites the file; Iceberg 1.9 keeps the vector that deleted id 1.
    set_delete_mode(spark, table, "copy-on-write")
    spark.sql(f"DELETE FROM {table} WHERE id = 2")
    leftover = live_leftover_vectors(spark, table)
    assert leftover == 1, f"expected one leftover deletion vector, found {leftover}"
    return {"table": table, "start_snapshot_id": start}


def build_leftover_new(spark, table: str) -> dict:
    # A retry after a partial run would no longer test the dropped vector.
    leftover = live_leftover_vectors(spark, table)
    assert leftover == 1, f"expected 1 leftover deletion vector, found {leftover}"
    insert(spark, table, 10, 11, files=1)
    # Rewrites the only old file, so no data file is as old as the leftover
    # vector, and Iceberg 1.10 drops it.
    spark.sql(f"DELETE FROM {table} WHERE id = 3")
    leftover = live_leftover_vectors(spark, table)
    assert leftover == 0, f"Iceberg kept {leftover} leftover deletion vectors"
    snapshot_ids = [s["snapshot_id"] for s in snapshots(spark, table)]
    return {
        "table": table,
        "start_snapshot_id": snapshot_ids[0],
        "end_snapshot_id": snapshot_ids[-1],
        "rows": [
            [row["id"], row["name"]]
            for row in spark.sql(f"SELECT id, name FROM {table} ORDER BY id").collect()
        ],
    }


BASIC_STRUCT = (
    "STRUCT<id: BIGINT NOT NULL, b: BOOLEAN NOT NULL, i: BIGINT, s: STRING NOT NULL>"
)


def basic_struct(n: str) -> str:
    return (
        f"named_struct('id', {n}, 'b', {n} % 3 = 0, "
        f"'i', IF({n} % 4 = 0, NULL, {n} * 2), 's', concat('t', {n}))"
    )


def build_types(spark, table: str) -> dict:
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    # Spark cannot write `time` or `fixed`.
    spark.sql(f"""
        CREATE TABLE {table} (
            b BOOLEAN NOT NULL, i INT NOT NULL, l BIGINT NOT NULL, r FLOAT NOT NULL,
            d DOUBLE NOT NULL, `dec` DECIMAL(10, 3) NOT NULL, dt DATE NOT NULL,
            ts TIMESTAMP_NTZ NOT NULL, s STRING NOT NULL, varbin BINARY NOT NULL,
            tstz TIMESTAMP NOT NULL, string_array ARRAY<STRING> NOT NULL,
            struct1 {BASIC_STRUCT} NOT NULL,
            struct_array ARRAY<{BASIC_STRUCT}> NOT NULL,
            string_string_map MAP<STRING, STRING> NOT NULL,
            string_struct_map MAP<STRING, {BASIC_STRUCT}> NOT NULL)
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read')
    """)
    spark.sql(f"""
        INSERT INTO {table}
        SELECT id % 2 = 0, CAST(id AS INT), id, CAST(id AS FLOAT) / 4,
            CAST(id AS DOUBLE) / 8, CAST(id / 1000 AS DECIMAL(10, 3)),
            date_add(DATE '2024-01-01', CAST(id AS INT)),
            CAST(timestamp_seconds(1704067200 + id) AS TIMESTAMP_NTZ),
            concat('s', id), CAST(concat('b', id) AS BINARY),
            timestamp_seconds(1704067200 + id),
            array(concat('a', id), concat('b', id)), {basic_struct("id")},
            array({basic_struct("id")}, {basic_struct("id + 1")}),
            map(concat('k', id), concat('v', id)),
            map(concat('k', id), {basic_struct("id")})
        FROM (SELECT /*+ REPARTITION_BY_RANGE(4, id) */ id FROM range(0, 40))
    """)
    start = current_snapshot_id(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE l % 5 = 0")
    spark.sql(f"""
        UPDATE {table} SET s = concat('u', l), struct1 = {basic_struct("l + 100")}
        WHERE l % 7 = 1
    """)
    # Replaces the deletion vectors of the first DELETE.
    spark.sql(f"DELETE FROM {table} WHERE l % 5 = 1")
    return {
        "table": table,
        "start_snapshot_id": start,
        "end_snapshot_id": current_snapshot_id(spark, table),
        "follow_records": follow_records(spark, table, start),
        "row_count": spark.table(table).count(),
    }


BUILDERS = {
    "v3": ("v3_dv", build_v3),
    "types": ("v3_types", build_types),
    "v2": ("v2_mor", build_v2),
    "leftover_old": ("leftover", build_leftover_old),
    "leftover_new": ("leftover", build_leftover_new),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True, choices=list(BUILDERS))
    args = parser.parse_args()

    warehouse = os.getenv("FELDERA_ICEBERG_WAREHOUSE", "s3://test/iceberg-follow")
    spark = spark_session(warehouse, old_writer=args.table == "leftover_old")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{args.namespace}")
    name, build = BUILDERS[args.table]
    result = build(spark, f"{CATALOG}.{args.namespace}.{name}")
    # Feldera names tables without the Spark catalog prefix.
    result["table"] = result["table"].split(".", 1)[1]
    print(json.dumps(result))


if __name__ == "__main__":
    main()
