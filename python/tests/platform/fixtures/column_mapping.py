"""Build a column-mapped, schema-evolved Delta table fixture with PySpark.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess
(``uv run --with "delta-spark>=4.2,<5" python column_mapping.py <output_dir>
<mode>``), where ``<mode>`` is a ``delta.columnMapping.mode`` value: ``name``
(physical Parquet columns are renamed to ``col-<uuid>``) or ``id`` (columns are
matched by Parquet field ID). PySpark is currently the only writer that can
produce a Delta table with column mapping enabled *and* perform the rename /
drop schema-evolution operations that make logical column names diverge from
the physical ones; neither ``delta-rs`` nor the ``deltalake`` Python wheel can.
DROP COLUMN under column mapping requires delta-spark 4.x (Delta writer v5,
reader v2). PySpark imports stay function-local so importing this module (for
``EXPECTED_ROWS``) never pulls in the JVM/Spark stack.

Changing this builder changes the fixture it produces, but a cached fixture is
reused based on its path alone — bump ``FIXTURE_VERSION`` in
``test_delta_input_column_mapping.py`` on any builder change.

The resulting table's history (one commit per step):

* ``v0`` CREATE TABLE with ``delta.columnMapping.mode = '<mode>'``
* ``v1`` INSERT two rows under the original ``(id, name, amount)`` schema
* ``v2`` RENAME COLUMN ``name`` -> ``full_name``
* ``v3`` ADD COLUMN ``country``
* ``v4`` INSERT two rows under the evolved ``(id, full_name, amount, country)``
* ``v5`` DROP COLUMN ``amount``
* ``v6`` INSERT one row under the final ``(id, full_name, country)`` schema

A snapshot read at the latest version must therefore return the final logical
schema with ``amount`` gone, ``country`` NULL for the rows written before it
existed, and every value resolved through column mapping.
"""

from __future__ import annotations

import sys


# Final logical rows expected from a snapshot read of the latest version.
# Shared with the test via import so the two never drift.
EXPECTED_ROWS = [
    {"id": 1, "full_name": "alice", "country": None},
    {"id": 2, "full_name": "bob", "country": None},
    {"id": 3, "full_name": "carol", "country": "US"},
    {"id": 4, "full_name": "dave", "country": "UK"},
    {"id": 5, "full_name": "erin", "country": "FR"},
]


def build(table_path: str, mode: str = "name") -> None:
    """Create the column-mapped, schema-evolved table at ``table_path``.

    :param mode: ``delta.columnMapping.mode`` for the table: ``name`` or ``id``.
    """
    if mode not in ("name", "id"):
        raise ValueError(f"unsupported column-mapping mode: {mode!r}")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("feldera-column-mapping-fixture")
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
            f"""
            CREATE TABLE {t} (id BIGINT, name STRING, amount DOUBLE)
            USING delta
            TBLPROPERTIES ('delta.columnMapping.mode' = '{mode}',
                           'delta.minReaderVersion' = '2',
                           'delta.minWriterVersion' = '5')
            """
        )
        spark.sql(f"INSERT INTO {t} VALUES (1,'alice',10.0),(2,'bob',20.0)")
        spark.sql(f"ALTER TABLE {t} RENAME COLUMN name TO full_name")
        spark.sql(f"ALTER TABLE {t} ADD COLUMN (country STRING)")
        spark.sql(f"INSERT INTO {t} VALUES (3,'carol',30.0,'US'),(4,'dave',40.0,'UK')")
        spark.sql(f"ALTER TABLE {t} DROP COLUMN amount")
        spark.sql(f"INSERT INTO {t} VALUES (5,'erin','FR')")
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        raise SystemExit("usage: column_mapping.py <output_dir> [name|id]")
    build(*sys.argv[1:])
