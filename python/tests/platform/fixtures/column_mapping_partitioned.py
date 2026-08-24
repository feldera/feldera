"""Build a column-mapped Delta table partitioned by a string column.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess
(``uv run --with "delta-spark>=4.2,<5" python column_mapping_partitioned.py
<output_dir>``). PySpark is the only writer that can enable column mapping;
neither ``delta-rs`` nor the ``deltalake`` wheel can.

Bump ``PARTITIONED_FIXTURE_VERSION`` in ``test_delta_input_column_mapping.py``
on any change here: a cached fixture is reused based on its path alone.

The table's history (one commit per step):

* ``v0`` CREATE TABLE ``(id, full_name, region)`` PARTITIONED BY ``region``,
  with ``delta.columnMapping.mode = 'name'``
* ``v1`` INSERT three rows across two regions

Delta keeps a partition column's value in the log, never in the data file, and
under column mapping it keys ``partitionValues`` by the column's *physical*
name (``col-<uuid>``) rather than its logical one. A follow read that looks the
value up by logical name finds no entry and fails the commit. Column mapping
also drops the Hive ``region=<value>`` directory in favour of an opaque prefix,
so the path carries no partition value to fall back on.
"""

from __future__ import annotations

import sys


# Logical rows the connector must produce, whatever the physical layout.
EXPECTED_ROWS = [
    {"id": 1, "full_name": "alice", "region": "us east"},
    {"id": 2, "full_name": "bob", "region": "us/west"},
    {"id": 3, "full_name": "carol", "region": "us east"},
]


def build(table_path: str) -> None:
    """Create the column-mapped, partitioned table at ``table_path``."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("feldera-column-mapping-partitioned-fixture")
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
        spark.sql(
            f"CREATE TABLE {t} (id BIGINT, full_name STRING, region STRING)"
            " USING delta PARTITIONED BY (region)"
            " TBLPROPERTIES ('delta.columnMapping.mode' = 'name',"
            "                'delta.minReaderVersion' = '2',"
            "                'delta.minWriterVersion' = '5')"
        )
        spark.sql(
            f"INSERT INTO {t} VALUES"
            " (1,'alice','us east'),(2,'bob','us/west'),(3,'carol','us east')"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: column_mapping_partitioned.py <output_dir>")
    build(sys.argv[1])
