"""Build Delta tables that record a Change Data Feed, for the `change_feed` input tests.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess::

    uv run --with "delta-spark>=4.2,<5" python change_data_feed.py \\
        <dest> <total_rows> <expected_active> \\
        [--deletion-vectors] [--partitioned] [--column-mapping] [--add-column]

Delta Spark is the writer that matters here. delta-rs can record a change feed,
and the Rust tests use it for that, but it cannot write a deletion vector
alongside one, nor enable column mapping at all. Spark can, and a table with
those features is the shape a Databricks source actually has -- one where, as
the ``--deletion-vectors`` history below shows, a `DELETE` records no change
data and the connector has to fall back on the commit's file actions.

The table is ``(id, name, value)``, plus ``grp`` when ``--partitioned``. Every
variant has the same five-commit history, so a test's ``version`` and
``end_version`` mean the same thing whatever the flags:

* ``v0`` -- ``CREATE TABLE``. Table properties can only be set here: enabling
  column mapping on an existing change-feed table is rejected outright.
* ``v1`` -- ``INSERT`` of ``total_rows`` rows, one file per partition. A single
  file is what makes the read amplification measurable: a copy-on-write
  ``UPDATE`` of one row rewrites all of it.
* ``v2`` -- ``UPDATE`` of the single row ``id = 1``. Records one
  ``update_preimage`` and one ``update_postimage``.
* ``v3`` -- ``DELETE`` of the even ``id`` rows. Copy-on-write records that many
  ``delete`` rows. With ``--deletion-vectors`` it records *nothing*: the commit
  is a same-path ``add``/``remove`` pair whose deletion vectors already say
  which rows left, and Spark's own change feed reader derives the deletes from
  them.
* ``v4`` -- one appended row, ``id = total_rows + 1``. Records *no* change data
  either: the protocol lets a writer skip it when a commit only adds rows.
* ``v5`` -- a ``MERGE`` that updates one row, deletes another, and inserts a
  third. This is the only commit that records all four change types at once,
  and the shape a Databricks source is usually maintained by. It leaves the row
  count where ``v4`` left it: one row leaves, one arrives, and the arriving id
  is odd so v3's invariant still holds.

``--add-column`` appends two more commits, so the five above keep their numbers:

* ``v6`` -- ``ALTER TABLE ADD COLUMN extra STRING``.
* ``v7`` -- an ``UPDATE`` that sets ``extra`` on one surviving row, recording
  change data under the wider schema.

Adding a column is the only schema change a change-feed table can undergo: Delta
rejects ``DROP COLUMN`` and ``RENAME COLUMN`` outright without column mapping,
and rejects them again *with* column mapping once a change feed is enabled
(``DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION``). A reader therefore never has
to interpret change data written under a column that has since been renamed
away -- only under one that did not exist yet.

So the flags pick which paths a test exercises. Without them every modifying
commit carries change data; ``--deletion-vectors`` sends the ``DELETE`` through
the ``add``/``remove`` path instead; ``--column-mapping`` renames every
column on disk to ``col-<uuid>`` and replaces the ``grp=<value>`` directory with
an opaque prefix, leaving the log's ``partitionValues`` -- keyed by physical
name -- as the only source of a partition value.

The builder asserts that ``expected_active`` rows remain readable and that the
``UPDATE`` recorded change data, then exits.
"""

from __future__ import annotations

import sys


UPDATED_NAME = "updated"
MERGED_NAME = "merged"

# Rows the v5 MERGE acts on. All three survive v3's delete of the even ids, so
# the merge's three clauses each match something.
MERGE_UPDATED_ID = 3
MERGE_DELETED_ID = 5

# The row v7 sets `extra` on. Odd, so it survives v3's delete, and clear of the
# ids the merge touches.
EXTRA_UPDATED_ID = 7
EXTRA_VALUE = "set"

# Column mapping needs reader 2 / writer 5; deletion vectors need 3 / 7. The two
# are never requested together, so each variant states only its own minimum.
COLUMN_MAPPING_PROPERTIES = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}


def _create_table(spark, dest: str, partitioned: bool, dvs: bool, mapped: bool) -> None:
    """v0: the table and its properties, which can only be set at creation."""
    properties = {"delta.enableChangeDataFeed": "true"}
    if dvs:
        properties["delta.enableDeletionVectors"] = "true"
    if mapped:
        properties.update(COLUMN_MAPPING_PROPERTIES)

    columns = "id BIGINT, name STRING, value DOUBLE"
    if partitioned:
        columns += ", grp STRING"
    tblproperties = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    spark.sql(
        f"CREATE TABLE delta.`{dest}` ({columns}) USING delta"
        + (" PARTITIONED BY (grp)" if partitioned else "")
        + f" TBLPROPERTIES ({tblproperties})"
    )


def _rows(spark, partitioned: bool, start: int, end: int):
    """Rows with ids in ``[start, end)``."""
    columns = [
        "id",
        "concat('user_', id) as name",
        "cast(id * 1.5 as double) as value",
    ]
    if partitioned:
        columns.append("cast(id % 3 as string) as grp")
    return spark.range(start, end).selectExpr(*columns)


def _merge(spark, dest: str, total_rows: int, partitioned: bool) -> None:
    """v5: one commit that updates, deletes, and inserts.

    The source names an operation per row so all three `MERGE` clauses fire. The
    inserted id is odd and clear of the appended row, so the invariants the
    other commits establish -- no even id survives, one row is named
    ``updated`` -- still hold after this one.
    """
    inserted_id = total_rows + 3
    columns = "id, name, value" + (", grp" if partitioned else "")
    source_columns = [
        "id",
        "name",
        "cast(id * 1.5 as double) as value",
        "op",
    ]
    if partitioned:
        source_columns.insert(3, "cast(id % 3 as string) as grp")
    spark.createDataFrame(
        [
            (MERGE_UPDATED_ID, MERGED_NAME, "update"),
            (MERGE_DELETED_ID, "gone", "delete"),
            (inserted_id, f"user_{inserted_id}", "insert"),
        ],
        "id BIGINT, name STRING, op STRING",
    ).selectExpr(*source_columns).createOrReplaceTempView("merge_source")

    spark.sql(
        f"MERGE INTO delta.`{dest}` t USING merge_source s ON t.id = s.id"
        " WHEN MATCHED AND s.op = 'update' THEN UPDATE SET t.name = s.name"
        " WHEN MATCHED AND s.op = 'delete' THEN DELETE"
        f" WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({columns.replace('  ', ' ')})"
    )


def _add_column(spark, dest: str) -> None:
    """v6 and v7: widen the schema, then record change data under it."""
    t = f"delta.`{dest}`"
    spark.sql(f"ALTER TABLE {t} ADD COLUMN extra STRING")
    spark.sql(f"UPDATE {t} SET extra = '{EXTRA_VALUE}' WHERE id = {EXTRA_UPDATED_ID}")


def _write(
    spark,
    dest: str,
    total_rows: int,
    partitioned: bool,
    dvs: bool,
    mapped: bool,
    add_column: bool,
) -> None:
    _create_table(spark, dest, partitioned, dvs, mapped)

    t = f"delta.`{dest}`"
    (
        _rows(spark, partitioned, 1, total_rows + 1)
        .repartition(1)
        .write.format("delta")
        .mode("append")
        .save(dest)
    )
    spark.sql(f"UPDATE {t} SET name = '{UPDATED_NAME}' WHERE id = 1")
    spark.sql(f"DELETE FROM {t} WHERE id % 2 = 0")
    (
        _rows(spark, partitioned, total_rows + 1, total_rows + 2)
        .write.format("delta")
        .mode("append")
        .save(dest)
    )
    _merge(spark, dest, total_rows, partitioned)
    if add_column:
        _add_column(spark, dest)


def main() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    dest = sys.argv[1]
    total_rows = int(sys.argv[2])
    expected_active = int(sys.argv[3])
    flags = sys.argv[4:]
    dvs = "--deletion-vectors" in flags
    partitioned = "--partitioned" in flags
    mapped = "--column-mapping" in flags
    add_column = "--add-column" in flags

    builder = (
        SparkSession.builder.appName("feldera_change_feed_fixture")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        _write(spark, dest, total_rows, partitioned, dvs, mapped, add_column)

        active = spark.read.format("delta").load(dest).count()
        assert active == expected_active, (
            f"builder expected {expected_active} active rows after the fixture "
            f"commits, got {active}"
        )
        # A fixture whose UPDATE recorded no change data would send every commit
        # down the add/remove fallback and prove nothing about the change feed.
        # Only v2 is checked: with deletion vectors the v3 DELETE records none,
        # which is the point of that variant.
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 2)
            .option("endingVersion", 2)
            .load(dest)
        )
        kinds = {row["_change_type"] for row in changes.collect()}
        assert kinds == {"update_preimage", "update_postimage"}, (
            f"the UPDATE at v2 recorded change types {sorted(kinds)}"
        )
        # The MERGE is the only commit that records all four at once, which is
        # what makes it worth having.
        merge_kinds = {
            row["_change_type"]
            for row in spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 5)
            .option("endingVersion", 5)
            .load(dest)
            .collect()
        }
        assert merge_kinds == {
            "insert",
            "delete",
            "update_preimage",
            "update_postimage",
        }, f"the MERGE at v5 recorded change types {sorted(merge_kinds)}"
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
