"""Build a column-mapped Delta table with a nested struct column via PySpark.

``tests.utils.ensure_delta_spark_fixture`` runs this as a subprocess
(``uv run --with "delta-spark>=4.2,<5" python column_mapping_nested.py <dir>``).
With ``delta.columnMapping.mode = 'name'`` every field is stored on disk under a
physical name (``col-<uuid>``), including the nested ``after`` struct's children.
PySpark is currently the only writer that can produce column mapping.

Models a ``uc://`` CDC table with a nested ``after`` struct. History (one commit
per step): v0 CREATE, v1 INSERT two rows, v2 INSERT one row. No schema evolution,
so a follow replay from v0 converges to a snapshot.
"""

from __future__ import annotations

import sys


# Rows expected from a read of the whole table. Imported by the test so the two
# never drift.
EXPECTED_ROWS = [
    {"after": {"transaction__id": "t1", "transaction__amount": "10"}, "op": "c"},
    {"after": {"transaction__id": "t2", "transaction__amount": "20"}, "op": "c"},
    {"after": {"transaction__id": "t3", "transaction__amount": "30"}, "op": "u"},
]


def build(table_path: str) -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("feldera-column-mapping-nested-fixture")
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
            f"CREATE TABLE {t} ("
            "  after STRUCT<transaction__id: STRING, transaction__amount: STRING>,"
            "  op STRING"
            f") USING delta {props}"
        )
        spark.sql(
            f"INSERT INTO {t} VALUES "
            "(named_struct('transaction__id','t1','transaction__amount','10'),'c'),"
            "(named_struct('transaction__id','t2','transaction__amount','20'),'c')"
        )
        spark.sql(
            f"INSERT INTO {t} VALUES "
            "(named_struct('transaction__id','t3','transaction__amount','30'),'u')"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: column_mapping_nested.py <output_dir>")
    build(sys.argv[1])
