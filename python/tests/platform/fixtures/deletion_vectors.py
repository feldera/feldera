"""Seed Delta tables with deletion vectors for the DV input tests.

Run this under ``delta-spark``, not bare ``pyspark``. The Delta write path and
deletion-vector support live in the Delta Lake Spark *JARs* that Spark loads at
runtime; ``delta-spark`` is the standard way to obtain them. It depends on a
compatible ``pyspark`` and ships ``configure_spark_with_delta_pip``, which
auto-resolves the matching Delta JAR. Bare ``pyspark`` could write Delta too,
but only by hardcoding the Delta Maven coordinate and Scala suffix and keeping
them in lockstep with the pyspark version, which is fragile and no cheaper (the
JARs download at runtime either way). ``tests.utils.ensure_delta_spark_fixture``
invokes this file with::

    uv run --with "delta-spark>=4.2,<5" python deletion_vectors.py \\
        <dest> <total_rows> <expected_active> [--cdc]

Without ``--cdc``, it writes a two-version table:

* v0: ``total_rows`` rows (``id``, ``name``, ``value``) with DVs enabled.
* v1: ``DELETE`` of the even ``id`` rows, which produces deletion vectors.

With ``--cdc``, it writes a four-version table shaped like a CDC event log
(``id``, ``__feldera_op``, ``__feldera_ts``, ``lsn``):

* v0: ``total_rows`` insert events (``__feldera_op = 'i'``).
* v1: ``DELETE`` of the even ``id`` events, producing deletion vectors.
* v2: ``RESTORE`` to v0, shrinking the deletion vectors away again.
* v3: one more insert event with ``id = total_rows + 1``.

With ``--overwrite``, it writes a three-version CDC-shaped table that exercises
the connector's ``removes_masked`` path:

* v0: ``total_rows`` insert events.
* v1: ``DELETE`` of the even ``id`` events, producing deletion vectors.
* v2: ``INSERT OVERWRITE`` re-inserting exactly those even events, which removes
  the DV-bearing file (a `remove` carrying the deletion vector, on a new path)
  and adds the even events back.

In every case the builder asserts that exactly ``expected_active`` rows remain
readable, then exits.
"""

import sys


def _write_plain_fixture(spark, dest: str, total_rows: int) -> None:
    """v0: `total_rows` rows; v1: DV `DELETE` of the even ids."""
    (
        spark.range(1, total_rows + 1)
        .selectExpr(
            "cast(id as int) as id",
            "concat('user_', id) as name",
            "cast(id * 1.5 as double) as value",
        )
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .save(dest)
    )
    spark.sql("DELETE FROM delta.`" + dest + "` WHERE id % 2 = 0")


def _cdc_events(spark, start_id: int, end_id: int):
    """Build a DataFrame of CDC insert events with ids in [start_id, end_id]."""
    return spark.range(start_id, end_id + 1).selectExpr(
        "id",
        "'i' as __feldera_op",
        # Monotone event time derived from the id; `lsn` breaks ts ties.
        "timestamp_micros(id * 1000000) as __feldera_ts",
        "id as lsn",
    )


def _write_cdc_fixture(spark, dest: str, total_rows: int) -> None:
    """v0: events; v1: DV `DELETE`; v2: `RESTORE` (DV shrink); v3: one event."""
    (
        _cdc_events(spark, 1, total_rows)
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .save(dest)
    )
    spark.sql("DELETE FROM delta.`" + dest + "` WHERE id % 2 = 0")
    # Un-delete the soft-deleted events: the restore commit rewrites each
    # file's deletion vector back to empty, a same-path `remove`/`add` pair
    # the connector must skip, just like the delete in v1.
    spark.sql("RESTORE TABLE delta.`" + dest + "` TO VERSION AS OF 0")
    (
        _cdc_events(spark, total_rows + 1, total_rows + 1)
        .write.format("delta")
        .mode("append")
        .save(dest)
    )


def _write_cdc_overwrite_fixture(spark, dest: str, total_rows: int) -> None:
    """v0: events; v1: DV `DELETE` of evens; v2: `INSERT OVERWRITE` re-inserting
    exactly the deleted even events.

    The overwrite removes the DV-bearing v1 file and adds a new file, so in CDC
    mode the `remove` is *unmatched* (a different path) yet carries the delete's
    deletion vector. This is the commit shape that drives the connector's
    `removes_masked` path. The new file's rows equal the file's DV-*dead* rows,
    so a correctly DV-masked `remove` cancels nothing (the even events are
    emitted), whereas an unmasked `remove` would read the whole file and wrongly
    cancel them.
    """
    (
        _cdc_events(spark, 1, total_rows)
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .save(dest)
    )
    spark.sql("DELETE FROM delta.`" + dest + "` WHERE id % 2 = 0")
    # Re-insert exactly the soft-deleted even events, with identical values. The
    # removed file still physically holds these rows (the DV only masks them),
    # so an unmasked `remove` would cancel the new copies by value.
    (
        _cdc_events(spark, 1, total_rows)
        .where("id % 2 = 0")
        .write.format("delta")
        .mode("overwrite")
        .save(dest)
    )


def main() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    dest = sys.argv[1]
    total_rows = int(sys.argv[2])
    expected_active = int(sys.argv[3])
    cdc = "--cdc" in sys.argv[4:]
    overwrite = "--overwrite" in sys.argv[4:]

    builder = (
        SparkSession.builder.appName("feldera_dv_fixture")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.properties.defaults.enableDeletionVectors",
            "true",
        )
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        spark.sparkContext.setLogLevel("ERROR")
        if overwrite:
            _write_cdc_overwrite_fixture(spark, dest, total_rows)
        elif cdc:
            _write_cdc_fixture(spark, dest, total_rows)
        else:
            _write_plain_fixture(spark, dest, total_rows)
        active = spark.read.format("delta").load(dest).count()
        assert active == expected_active, (
            f"builder expected {expected_active} active rows after the "
            f"fixture commits, got {active}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
