"""Create the tables for the follow-mode deletion-vector tests with Spark.

* `v3_dv`: a V3 table with merge-on-read DELETE, UPDATE and MERGE, a
  copy-on-write DELETE, and a compaction.
* `v2_mor`: a V2 table with a position delete file.

Prints the snapshot ids, final rows and expected read count as JSON on the
last line. Settings use the same environment variables as `follow_table.py`.
"""

import argparse
import json
import os

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.0"
CATALOG = "rest"
CREATED_AT = "TIMESTAMP_NTZ '2024-01-01 00:00:00'"


def spark_session(warehouse: str) -> SparkSession:
    # The warehouse scheme picks the file IO: GCS in CI, S3 (MinIO) locally.
    gcs = warehouse.startswith("gs://")
    bundle = "iceberg-gcp-bundle" if gcs else "iceberg-aws-bundle"
    catalog = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("feldera-iceberg-v3-fixture")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{ICEBERG_VERSION},"
            f"org.apache.iceberg:{bundle}:{ICEBERG_VERSION}",
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
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_table(spark, table: str, format_version: int):
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    # The identity partition makes the reader fill `created_at` from the
    # partition of the file or deletion vector.
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
    return spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at"
    ).collect()


def current_snapshot_id(spark, table: str) -> int:
    return snapshots(spark, table)[-1]["snapshot_id"]


def follow_records(spark, table: str, after: int) -> int:
    """Rows follow mode reads after snapshot `after`: every inserted row, every
    live row of a removed file, and every newly deleted row. Compactions
    (`replace`) are skipped."""
    total = 0
    found = False
    for snapshot in snapshots(spark, table):
        if found and snapshot["operation"] != "replace":
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
        found = found or snapshot["snapshot_id"] == after
    return total


def build_v3(spark, table: str) -> dict:
    create_table(spark, table, 3)
    insert(spark, table, 0, 100, files=4)
    start = current_snapshot_id(spark, table)

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

    # Rewrites only the second file; the first keeps its position delete file.
    set_delete_mode(spark, table, "copy-on-write")
    spark.sql(f"DELETE FROM {table} WHERE id = 13")

    return {"table": table, "start_snapshot_id": start}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()

    warehouse = os.getenv("FELDERA_ICEBERG_WAREHOUSE", "s3://test/iceberg-follow")
    spark = spark_session(warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{args.namespace}")
    result = {
        "v3": build_v3(spark, f"{CATALOG}.{args.namespace}.v3_dv"),
        "v2": build_v2(spark, f"{CATALOG}.{args.namespace}.v2_mor"),
    }
    # Feldera names tables without the Spark catalog prefix.
    for table in result.values():
        table["table"] = table["table"].split(".", 1)[1]
    print(json.dumps(result))


if __name__ == "__main__":
    main()
