"""Create the Unity Catalog fixture for the Uniform/Iceberg read test.

Run by hand, not by pytest, from the ``python`` directory:

    source ~/.feldera-dbx-test.env
    .venv/bin/python -m tests.platform.fixtures.unity_uniform
    .venv/bin/python -m tests.platform.fixtures.unity_uniform --drop

The table is a managed Iceberg table, which Unity Catalog also exposes as Delta.
That is the shape #7076 was reported on: a native Iceberg writer names the
Parquet columns by their *logical* names and identifies them by Parquet
``field_id``, while the Delta log Unity Catalog synthesizes uses
``columnMapping.mode = 'id'`` with physical names ``col-<id>``. A reader that
pairs columns by physical name finds none of them, so a nullable column reads as
NULL and a ``NOT NULL`` one fails the read outright.

`python/tests/platform/fixtures/uniform_iceberg.py` builds the same shape
locally with pyarrow and a hand-written log. It covers the reader; only this one
covers Databricks' own log synthesis and the ``uc://`` credential path, which is
why both exist.

The schema is chosen so that no part of the read can succeed by accident:

* a nested ``STRUCT`` whose children are mapped too, so pairing must resolve by
  field id *inside* a struct and not only at the top level;
* an ``ARRAY<STRUCT>`` and a ``MAP<STRING, STRUCT>``, the shapes whose element
  fields are paired separately from the columns around them;
* a ``NOT NULL`` column, which turns a failed pairing into a hard error rather
  than into silent NULLs.

Each row carries a ``batch`` tag naming the commit that wrote it, so a test
replaying the last commit alone can ask Databricks for the same subset.
"""

from __future__ import annotations

import os
import sys

from tests.platform.fixtures import unity_api

#: The service principal cannot create schemas, so this one has to exist.
SCHEMA = os.environ.get("DELTA_TABLE_TEST_UNITY_SCHEMA", "default")
TABLE = "uniform_iceberg"

#: Rows per commit, and the tag their ``batch`` column carries.
BATCHES = ["b0", "b1"]
ROWS_PER_BATCH = 4

#: The Delta versions the statements below produce. Asserted after the run, so a
#: workspace that numbers them differently is reported rather than assumed.
CREATE_VERSION = 0
LAST_VERSION = len(BATCHES)

#: The tag of the commit a follow or CDC test replays on its own.
REPLAY_BATCH = BATCHES[-1]

#: The table's columns, in Feldera SQL. A test declares its own table with these
#: so the two schemas cannot drift.
FELDERA_COLUMNS = """
    id VARCHAR NOT NULL,
    batch VARCHAR,
    amount DECIMAL(10, 2),
    after ROW(
        transaction__id VARCHAR,
        transaction__merchant_id VARCHAR,
        transaction__merchant_name VARCHAR,
        transaction__status VARCHAR
    ),
    history VARCHAR ARRAY,
    tags MAP<VARCHAR, VARCHAR>,
    op VARCHAR NOT NULL
"""


def statements(catalog: str) -> list[str]:
    """The SQL that builds the table, one commit per element after the create.

    :param catalog: The Unity catalog to build in.
    """
    t = f"{catalog}.{SCHEMA}.{TABLE}"
    creates = [
        f"DROP TABLE IF EXISTS {t}",
        # `USING ICEBERG` is what makes the log come out `columnMapping.mode =
        # 'id'`; a plain Delta table cannot be put into that mode.
        f"CREATE TABLE {t} ("
        " id STRING NOT NULL,"
        " batch STRING,"
        " amount DECIMAL(10, 2),"
        " after STRUCT<"
        "transaction__id: STRING,"
        "transaction__merchant_id: STRING,"
        "transaction__merchant_name: STRING,"
        "transaction__status: STRING>,"
        " history ARRAY<STRING>,"
        " tags MAP<STRING, STRING>,"
        " op STRING NOT NULL"
        ") USING ICEBERG",
    ]
    inserts = []
    for commit, batch in enumerate(BATCHES):
        values = ", ".join(
            _row_values(batch, commit * ROWS_PER_BATCH + row)
            for row in range(ROWS_PER_BATCH)
        )
        inserts.append(f"INSERT INTO {t} VALUES {values}")
    return creates + inserts


def _row_values(batch: str, n: int) -> str:
    """One row's ``VALUES`` tuple, every column populated.

    Nothing is NULL: a column that reads as NULL is the #7076 symptom, so the
    fixture must have no NULL of its own for it to hide behind.

    :param batch: The tag of the commit writing the row.
    :param n: The row's index across the whole table.
    """
    return (
        f"('txn-{n:03d}', '{batch}', {n}.25,"
        f" named_struct("
        f"'transaction__id', 'txn-{n:03d}',"
        f"'transaction__merchant_id', 'm-{n:03d}',"
        f"'transaction__merchant_name', 'Merchant {n}',"
        f"'transaction__status', 'settled'),"
        f" array('created', 'settled'),"
        f" map('channel', 'web', 'region', 'us'),"
        f" 'c')"
    )


def _check(host: str, token: str, warehouse: str, table: str) -> None:
    """Fail unless the table really has the properties the test depends on.

    A table that loses any of them yields a test that passes whether or not the
    reader resolves columns by field id, so check them rather than trust the
    recipe.

    :param table: The table's full ``catalog.schema.name``.
    """
    properties = {
        row[0]: row[1]
        for row in unity_api.sql(host, token, warehouse, f"SHOW TBLPROPERTIES {table}")
    }
    mode = properties.get("delta.columnMapping.mode")
    assert mode == "id", (
        f"{table} has columnMapping.mode = {mode!r}, not 'id'. Only an id-mapped "
        "table reaches the field-id path the test exists to cover; this workspace "
        "did not produce one from `USING ICEBERG`."
    )

    versions = [
        int(row["version"])
        for row in unity_api.sql_dicts(
            host, token, warehouse, f"DESCRIBE HISTORY {table}"
        )
    ]
    assert max(versions) == LAST_VERSION and min(versions) == CREATE_VERSION, (
        f"{table} spans versions {min(versions)}-{max(versions)}, not "
        f"{CREATE_VERSION}-{LAST_VERSION}; the replay window the test pins would "
        "read the wrong commits."
    )

    total, tagged = unity_api.sql(
        host,
        token,
        warehouse,
        f"SELECT count(*), count_if(batch = '{REPLAY_BATCH}') FROM {table}",
    )[0]
    expected = len(BATCHES) * ROWS_PER_BATCH
    assert int(total) == expected, f"expected {expected} rows, got {total}"
    assert int(tagged) == ROWS_PER_BATCH, (
        f"the last commit wrote {tagged} rows, not {ROWS_PER_BATCH}"
    )


def main() -> None:
    host = unity_api.require("DELTA_TABLE_TEST_UNITY_HOST").rstrip("/")
    catalog = unity_api.require("DELTA_TABLE_TEST_UNITY_CATALOG")
    warehouse = unity_api.require("DELTA_TABLE_TEST_UNITY_WAREHOUSE_ID")
    token = unity_api.token_from_env(host)
    t = f"{catalog}.{SCHEMA}.{TABLE}"

    if "--drop" in sys.argv:
        unity_api.sql(host, token, warehouse, f"DROP TABLE IF EXISTS {t}")
        print("  dropped", t)
        return

    for statement in statements(catalog):
        unity_api.sql(host, token, warehouse, statement)
        print("  ok:", statement[:80].replace("\n", " "))

    _check(host, token, warehouse, t)

    print(f"\n{t} ready: {len(BATCHES)} commits of {ROWS_PER_BATCH} rows, id-mapped")
    print("  DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE=uc://" + t)


if __name__ == "__main__":
    # `unity_api` raises rather than exits, so that importing it from a test
    # does not bypass pytest's failure reporting; as a script, report the
    # message and an exit code instead of a traceback.
    try:
        main()
    except unity_api.UnityApiError as error:
        sys.exit(str(error))
