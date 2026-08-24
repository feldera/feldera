"""Build a Delta CDC table whose DROP COLUMN shifts the delete-marker column.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess
(``uv run --with "delta-spark>=4.2,<5" python cdc_delete_filter_drop.py
<output_dir>``). DROP COLUMN needs column mapping, which only Delta Spark can
write; neither ``delta-rs`` nor the ``deltalake`` wheel can drop a column.

Bump ``FIXTURE_VERSION`` in ``test_delta_input_cdc_schema_drop.py`` on any
change here: a cached fixture is reused based on its path alone.

The table's history (one commit per step):

* ``v0`` CREATE TABLE ``(id, b, extra, __feldera_op, s, __feldera_ts)``
* ``v1`` INSERT ids 1 and 2, both with ``__feldera_op = 'i'``
* ``v2`` DROP COLUMN ``extra``
* ``v3`` INSERT a delete marker for id 1 (``__feldera_op = 'd'``)

``__feldera_op`` sits at index 3 until the drop and at index 2 after it, which
leaves ``s`` -- also a string -- where a ``cdc_delete_filter`` compiled against
the pre-drop schema still reads. A connector that follows the shift would apply
the v3 marker and leave only id 2; one that reads the stale index sees
``s = 'd'``, never true, and inserts id 1 a second time.
"""

from __future__ import annotations

import sys


def build(table_path: str) -> None:
    """Create the column-mapped table at ``table_path`` with the drop history."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("feldera-cdc-drop-fixture")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        t = f"delta.`{table_path}`"
        props = (
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name',"
            "               'delta.minReaderVersion' = '2',"
            "               'delta.minWriterVersion' = '5')"
        )
        spark.sql(
            f"CREATE TABLE {t} (id BIGINT, b BOOLEAN, extra STRING,"
            " __feldera_op STRING, s STRING, __feldera_ts TIMESTAMP_NTZ)"
            f" USING delta {props}"
        )
        spark.sql(
            f"INSERT INTO {t} VALUES"
            " (1,false,'x','i','keep',TIMESTAMP_NTZ'2020-01-01 00:00:01'),"
            " (2,false,'x','i','keep',TIMESTAMP_NTZ'2020-01-01 00:00:01')"
        )
        spark.sql(f"ALTER TABLE {t} DROP COLUMN extra")
        spark.sql(
            f"INSERT INTO {t} VALUES"
            " (1,false,'d','keep',TIMESTAMP_NTZ'2020-01-01 00:00:02')"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: cdc_delete_filter_drop.py <output_dir>")
    build(sys.argv[1])
