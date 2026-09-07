"""Create the Unity Catalog fixture for the deletion-vector read test.

Run by hand, not by pytest, from the ``python`` directory:

    source ~/.feldera-dbx-test.env
    .venv/bin/python -m tests.platform.fixtures.unity_deletion_vectors
    .venv/bin/python -m tests.platform.fixtures.unity_deletion_vectors --drop

Two properties make this table resurrect deleted rows against an unfixed
reader. Drop either and the read comes back correct whether or not the bug is
present.

Partitioned
    This is the one that matters, and it is not about size. A partitioned
    deletion-vector table is read through several scan streams that share one
    map of keep masks, and a stream can drain a mask that is not its own. An
    unpartitioned table of the same rows -- even 6x larger, over DataFusion's
    10 MB split threshold, scanned across 32 partitions -- was correct in every
    attempt against the same unfixed pin.

Written by Databricks, read over S3
    The writer and object store the connector meets in production, rather than
    a local file written by Spark.

The delete is lopsided (4978 rows from the first partition, 43 from the second)
and lands in a single commit, so Delta packs both vectors into one `.bin` at
different offsets and the second vector's offset depends on the first one's
length. That mirrors the customer table this was built to explain.
"""

from __future__ import annotations

import os
import sys

from tests.platform.fixtures import unity_api

#: The service principal cannot create schemas, so this one has to exist.
SCHEMA = os.environ.get("DELTA_TABLE_TEST_UNITY_SCHEMA", "default")
TABLE = "deletion_vectors"
ROWS = 400_000
HALF = ROWS // 2
DELETED_HEAD = 4978
DELETED_TAIL = 43
EXPECTED_ROWS = ROWS - DELETED_HEAD - DELETED_TAIL

#: Rows the deletion vectors mark deleted; none may be read back.
DELETED_PREDICATE = (
    f"id < {DELETED_HEAD} OR (id >= {HALF} AND id < {HALF} + {DELETED_TAIL})"
)


def statements(catalog: str) -> list[str]:
    t = f"{catalog}.{SCHEMA}.{TABLE}"
    return [
        f"DROP TABLE IF EXISTS {t}",
        f"CREATE TABLE {t} (id BIGINT, half INT, name STRING) USING delta"
        f" PARTITIONED BY (half)"
        f" TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
        f"INSERT INTO {t} SELECT id, cast(id / {HALF} as int) AS half,"
        f" concat('n', id) AS name FROM range(0, {ROWS})",
        # One commit, both files: Delta packs the two vectors into one file.
        f"DELETE FROM {t} WHERE {DELETED_PREDICATE}",
    ]


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

    total, deleted = unity_api.sql(
        host,
        token,
        warehouse,
        f"SELECT count(*), count_if({DELETED_PREDICATE}) FROM {t}",
    )[0]
    assert int(total) == EXPECTED_ROWS, f"expected {EXPECTED_ROWS} rows, got {total}"
    assert int(deleted) == 0, f"Databricks itself returns {deleted} deleted rows"

    # A fixture that loses either property yields a test that passes with the
    # bug present, so check them rather than trust the recipe.
    detail = unity_api.sql_dicts(host, token, warehouse, f"DESCRIBE DETAIL {t}")[0]
    assert "deletionVectors" in str(detail["tableFeatures"]), (
        f"no deletion vectors on {t}: the delete was materialised instead"
    )
    assert detail["partitionColumns"], (
        f"{t} is not partitioned; an unpartitioned table reads correctly even "
        "against the unfixed reader and the test would be vacuous"
    )

    print(
        f"\n{t} ready: {total} rows, {DELETED_HEAD + DELETED_TAIL} deleted by vectors"
    )
    print(f"  {detail['numFiles']} files, {detail['sizeInBytes']} bytes")
    print("  DELTA_TABLE_TEST_UNITY_DV_TABLE=" + t)


if __name__ == "__main__":
    # `unity_api` raises rather than exits, so that importing it from a test
    # does not bypass pytest's failure reporting; as a script, report the
    # message and an exit code instead of a traceback.
    try:
        main()
    except unity_api.UnityApiError as error:
        sys.exit(str(error))
