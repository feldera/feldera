"""Seed a Delta table with deletion vectors for the snapshot DV test.

Run this under ``delta-spark``, not bare ``pyspark``. The Delta write path and
deletion-vector support live in the Delta Lake Spark *JARs* that Spark loads at
runtime; ``delta-spark`` is the standard way to obtain them — it depends on a
compatible ``pyspark`` and ships ``configure_spark_with_delta_pip``, which
auto-resolves the matching Delta JAR. Bare ``pyspark`` could write Delta too,
but only by hardcoding the Delta Maven coordinate and Scala suffix and keeping
them in lockstep with the pyspark version — fragile, and no cheaper (the JARs
download at runtime either way). ``tests.utils.ensure_delta_spark_fixture``
invokes this file with::

    uv run --with "delta-spark>=4.2,<5" python deletion_vectors.py \\
        <dest> <total_rows> <expected_active>

It writes ``total_rows`` rows to ``dest`` with DVs enabled, runs a ``DELETE``
on the even ``id`` rows that produces deletion vectors, then asserts that
exactly ``expected_active`` rows remain readable.
"""

import sys


def main() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    dest = sys.argv[1]
    total_rows = int(sys.argv[2])
    expected_active = int(sys.argv[3])

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
        active = spark.read.format("delta").load(dest).count()
        assert active == expected_active, (
            f"builder expected {expected_active} active rows after DV "
            f"DELETE, got {active}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
