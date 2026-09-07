"""Create the Unity Catalog fixtures for the live change-feed tests.

Run by hand, not by pytest: it provisions tables in a real Databricks workspace
that the gated tests then read over ``uc://``. Statements go through the SQL
Statement Execution API, so the change data is written by Databricks itself
rather than by delta-rs or local Spark -- which is the whole point, since the
workspace defaults (deletion vectors, row tracking, reader v3) are not
reproducible locally.

    source ~/.feldera-dbx-test.env
    python unity_change_feed.py             # the small keyed-merge table
    python unity_change_feed.py --stress    # the kitchen-sink table
    python unity_change_feed.py --drop      # remove the schema and its tables

Needs ``DELTA_TABLE_TEST_UNITY_HOST``, ``..._CLIENT_ID``, ``..._CLIENT_SECRET``,
``..._WAREHOUSE_ID`` and ``..._CATALOG`` in the environment. The tables are
MANAGED, so ``--drop`` reclaims their storage; nothing is left in S3.

`keyed_merge` is the table the reader has most to get wrong. Every row starts in
its own partition and the merge moves it into a sibling that sorts *earlier*, so
each row's pre-image and post-image land in different change data files and the
post-image group is read first. On a relation with a primary key a retraction is
a delete *by key*, so a reader that does not apply retractions first loses the
row -- the defect this fixture exists to catch, in the shape Databricks actually
produces it. Its history is exact, so a test can name a version range:

* ``v0`` -- ``CREATE TABLE``
* ``v1`` -- ``INSERT`` of ``ROWS`` rows
* ``v2`` -- a ``MERGE``

`stress` is the same hazard at scale, with everything else piled on top: every
column type the connector reads, name-mode column mapping, partitioning, and a
history mixing inserts, merges, updates, deletes and `OPTIMIZE`. See
``STRESS_HISTORY`` for what each version does.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

SCHEMA = "feldera_cdf_test"
TABLE = "keyed_merge"
ROWS = 40

STRESS_TABLE = "stress"
# Rows the first insert writes. Later commits add a quarter and a twentieth more
# and remove a fifth, so the table ends near this number.
STRESS_ROWS = int(os.environ.get("DELTA_TABLE_TEST_UNITY_STRESS_ROWS", "100000"))

# The partition a row lands in, and the one v6 moves a tenth of the rows into.
# '0' sorts before every other value, so a moved row's post-image group is read
# before the group holding its pre-image -- the ordering the reader must not
# depend on.
STRESS_PARTITIONS = 8
MOVED_PARTITION = "0"

# Every column type the connector reads, in both dialects. Names avoid the type
# keywords (`int`, `date`, `binary`, ...) that are reserved on one side or the
# other, so neither DDL needs quoting. `c_variant` is a JSON *string*: that is
# what a Feldera VARIANT column round-trips through Delta as today, Delta's
# native variant type not being readable yet.
#
# (name, Databricks type, Feldera type)
_STRUCT_DBX = "STRUCT<f_id: BIGINT, f_b: BOOLEAN, f_i: BIGINT, f_s: STRING>"
_STRUCT_SQL = "ROW(f_id BIGINT, f_b BOOLEAN, f_i BIGINT, f_s VARCHAR)"
STRESS_COLUMNS = [
    ("id", "BIGINT", "BIGINT NOT NULL"),
    ("c_binary", "BINARY", "VARBINARY"),
    ("c_boolean", "BOOLEAN", "BOOLEAN"),
    ("c_date", "DATE", "DATE"),
    ("c_decimal", "DECIMAL(10,3)", "DECIMAL(10,3)"),
    ("c_double", "DOUBLE", "DOUBLE"),
    ("c_float", "FLOAT", "REAL"),
    ("c_int", "INT", "INT"),
    ("c_smallint", "SMALLINT", "SMALLINT"),
    ("c_tinyint", "TINYINT", "TINYINT"),
    ("c_string", "STRING", "VARCHAR"),
    ("c_nullable", "STRING", "VARCHAR"),
    ("c_timestamp", "TIMESTAMP", "TIMESTAMP"),
    ("c_array", "ARRAY<STRING>", "VARCHAR ARRAY"),
    ("c_struct", _STRUCT_DBX, _STRUCT_SQL),
    ("c_struct_array", f"ARRAY<{_STRUCT_DBX}>", f"{_STRUCT_SQL} ARRAY"),
    ("c_map", "MAP<STRING, STRING>", "MAP<VARCHAR, VARCHAR>"),
    ("c_struct_map", f"MAP<STRING, {_STRUCT_DBX}>", f"MAP<VARCHAR, {_STRUCT_SQL}>"),
    ("c_variant", "STRING", "VARCHAR"),
    ("c_uuid", "STRING", "UUID"),
    ("grp", "STRING", "VARCHAR"),
]

STRESS_COLUMN_NAMES = [name for name, _, _ in STRESS_COLUMNS]


def stress_databricks_columns() -> str:
    return ", ".join(f"{name} {dbx}" for name, dbx, _ in STRESS_COLUMNS)


def stress_feldera_columns() -> str:
    """The same table as Feldera SQL, keyed on `id`.

    The primary key is what makes the ordering hazard reachable: it turns each
    retraction into a delete *by key*, so a post-image applied before its
    pre-image loses the row instead of cancelling out by value.
    """
    columns = ", ".join(f"{name} {sql}" for name, _, sql in STRESS_COLUMNS)
    return f"{columns}, PRIMARY KEY (id)"


def stress_row_exprs(tag: str) -> str:
    """Every column as a deterministic function of `id`.

    `tag` prefixes the string-shaped columns, so a commit that rewrites a row
    changes values a later aggregate can see. `grp` is derived here too, but v6
    overwrites it for some rows, which is why the oracle reads the table back
    rather than recomputing it.
    """
    inner = (
        f"named_struct('f_id', id, 'f_b', id % 2 = 0, 'f_i', id,"
        f" 'f_s', concat('{tag}', id))"
    )
    exprs = {
        "id": "id",
        "c_binary": f"cast(concat('{tag}', id) as binary)",
        "c_boolean": "id % 2 = 0",
        "c_date": "date_add(DATE'2020-01-01', cast(id % 1000 as int))",
        "c_decimal": "cast((id % 1000000) / 1000 as decimal(10,3))",
        "c_double": "cast(id as double) * 1.5",
        "c_float": "cast(id % 100000 as float) * 0.5",
        "c_int": "cast(id % 2000000 as int)",
        "c_smallint": "cast(id % 32767 as smallint)",
        "c_tinyint": "cast(id % 127 as tinyint)",
        "c_string": f"concat('{tag}', id)",
        # A seventh of the rows are NULL, so a reader that fills a missing
        # column with NULL cannot pass the non-null count by accident.
        "c_nullable": "case when id % 7 = 0 then null else concat('u', id) end",
        "c_timestamp": (
            "timestampadd(SECOND, cast(id % 100000 as int),"
            " TIMESTAMP'2020-01-01 00:00:00')"
        ),
        "c_array": f"array(concat('{tag}', id), 'x')",
        "c_struct": inner,
        "c_struct_array": f"array({inner})",
        "c_map": f"map('k', concat('{tag}', id))",
        "c_struct_map": f"map('k', {inner})",
        "c_variant": "concat('{\"n\":', id, '}')",
        "c_uuid": "concat(lpad(hex(id), 8, '0'), '-0000-0000-0000-000000000000')",
        "grp": f"cast(id % {STRESS_PARTITIONS} as string)",
    }
    assert list(exprs) == STRESS_COLUMN_NAMES, "generator and schema disagree"
    return ", ".join(f"{expr} AS {name}" for name, expr in exprs.items())


# What each `stress` version does, for the tests that replay a range of it.
STRESS_HISTORY = {
    "create": 0,
    "insert": 1,
    "append": 2,
    "merge": 3,
    "delete": 4,
    "optimize": 5,
    "move_partition": 6,
    "update": 7,
    "feed_off": 8,
    "blind_insert": 9,
    "feed_on": 10,
    "final_optimize": 11,
}
STRESS_LAST_VERSION = STRESS_HISTORY["final_optimize"]
# The one commit guaranteed to change data without recording any, so the reader
# has to fall back to the commit's file actions to see it.
BLIND_INSERT_VERSION = STRESS_HISTORY["blind_insert"]


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is not set; source ~/.feldera-dbx-test.env")
    return value


def _token(host: str, client_id: str, client_secret: str) -> str:
    """An OAuth M2M token. Unity vends the storage credentials separately."""
    import base64

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    req = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=b"grant_type=client_credentials&scope=all-apis",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    return json.load(urllib.request.urlopen(req))["access_token"]


def _get(host: str, token: str, path: str) -> dict:
    req = urllib.request.Request(
        f"{host}{path}", headers={"Authorization": f"Bearer {token}"}
    )
    return json.load(urllib.request.urlopen(req))


def _sql(host: str, token: str, warehouse: str, statement: str) -> list:
    """Run one statement to completion.

    The API caps `wait_timeout` at 50s and then hands back a statement id, so a
    long `MERGE` over the stress table has to be polled rather than waited on.
    """
    req = urllib.request.Request(
        f"{host}/api/2.0/sql/statements",
        data=json.dumps(
            {
                "warehouse_id": warehouse,
                "statement": statement,
                "wait_timeout": "50s",
            }
        ).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        result = json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        raise SystemExit(f"HTTP {e.code}: {e.read().decode()[:400]}") from e

    deadline = time.monotonic() + 1800
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            raise SystemExit(f"timed out after 30 minutes:\n{statement[:200]}")
        time.sleep(5)
        result = _get(host, token, f"/api/2.0/sql/statements/{result['statement_id']}")

    state = result["status"]["state"]
    if state != "SUCCEEDED":
        raise SystemExit(
            f"{state}: {json.dumps(result['status'])[:400]}\n{statement[:200]}"
        )
    return result.get("result", {}).get("data_array", [])


def keyed_merge_statements(catalog: str) -> list[str]:
    table = f"{catalog}.{SCHEMA}.{TABLE}"
    # The source rows the merge applies: every row moves from partition
    # `b<nn>` to `a<nn>`, which sorts earlier.
    moves = " UNION ALL ".join(f"SELECT {i} AS id, 'a{i:02}' AS s" for i in range(ROWS))
    inserts = ", ".join(f"({i}, false, NULL, 'b{i:02}')" for i in range(ROWS))

    return [
        f"DROP TABLE IF EXISTS {table}",
        # v0. The workspace adds deletion vectors and row tracking on its own;
        # only the change feed has to be asked for.
        f"CREATE TABLE {table} (id INT, b BOOLEAN, i BIGINT, s STRING)"
        f" USING delta PARTITIONED BY (s)"
        f" TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
        # v1.
        f"INSERT INTO {table} VALUES {inserts}",
        # v2: one commit, every row into an earlier-sorting partition.
        f"MERGE INTO {table} t USING ({moves}) s ON t.id = s.id"
        f" WHEN MATCHED THEN UPDATE SET t.s = s.s",
    ]


def stress_statements(catalog: str) -> list[str]:
    """The kitchen-sink history, one statement per version after the drop.

    Every commit shape the reader handles differently is here: an insert that
    only adds files, a merge recording all four change types at once, a delete
    that under deletion vectors records *no* change data and so must fall back
    to the file actions, an `OPTIMIZE` that rewrites files with
    ``dataChange = false`` and must be a no-op, and an update that moves rows
    across partitions.
    """
    table = f"{catalog}.{SCHEMA}.{STRESS_TABLE}"
    n = STRESS_ROWS

    def source(lo: int, hi: int, tag: str) -> str:
        return f"SELECT {stress_row_exprs(tag)} FROM range({lo}, {hi})"

    def union_source(ranges: list[tuple[int, int]], tag: str) -> str:
        ids = " UNION ALL ".join(
            f"SELECT id FROM range({lo}, {hi})" for lo, hi in ranges
        )
        return f"SELECT {stress_row_exprs(tag)} FROM ({ids})"

    return [
        f"DROP TABLE IF EXISTS {table}",
        # v0. Column mapping puts every column on disk under an opaque
        # `col-<uuid>` name and the partition directory under an opaque prefix,
        # so the log is the only place a column or partition value is named.
        f"CREATE TABLE {table} ({stress_databricks_columns()})"
        f" USING delta PARTITIONED BY (grp)"
        f" TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',"
        f" 'delta.columnMapping.mode' = 'name')",
        # v1, v2: two inserts, so the snapshot spans more than one commit's files.
        f"INSERT INTO {table} {source(0, n, 'b')}",
        f"INSERT INTO {table} {source(n, n + n // 4, 'b')}",
        # v3: one merge recording all four change types -- updates over the
        # first half, deletes on a tenth of it, and a block of new rows.
        f"MERGE INTO {table} t"
        f" USING ({union_source([(0, n // 2), (2 * n, 2 * n + n // 20)], 'c')}) s"
        f" ON t.id = s.id"
        f" WHEN MATCHED AND s.id % 10 = 3 THEN DELETE"
        f" WHEN MATCHED THEN UPDATE SET *"
        f" WHEN NOT MATCHED THEN INSERT *",
        # v4: with deletion vectors this records no change data at all, so the
        # reader has to fall back to the commit's add/remove pair and mask it.
        f"DELETE FROM {table} WHERE id % 10 = 7",
        # v5: rewrites files with `dataChange = false`; must change nothing.
        f"OPTIMIZE {table}",
        # v6: a tenth of the rows move into partition '{MOVED_PARTITION}', which
        # sorts before the ones they came from. Each row's pre-image and
        # post-image land in different change data files, and the post-image
        # file is the one read first.
        f"UPDATE {table} SET grp = '{MOVED_PARTITION}' WHERE id % 10 = 1",
        # v7: a plain update, touching rows the merge already rewrote.
        f"UPDATE {table} SET c_string = concat('d', id) WHERE id % 10 = 5",
        # v8-v10: a block of rows written while the feed is off. Turning the
        # property off and back on is how a table acquires a gap in its change
        # data, and a customer who enables the feed on a table that already has
        # history has exactly this shape. It is also the only commit here
        # guaranteed to record nothing: Databricks reports a `delete` for v4
        # whether it wrote change data or reconstructed it from the deletion
        # vector, so v4 cannot stand in for it.
        f"ALTER TABLE {table} SET TBLPROPERTIES"
        f" ('delta.enableChangeDataFeed' = 'false')",
        f"INSERT INTO {table} {source(3 * n, 3 * n + n // 20, 'e')}",
        f"ALTER TABLE {table} SET TBLPROPERTIES"
        f" ('delta.enableChangeDataFeed' = 'true')",
        # v11.
        f"OPTIMIZE {table}",
    ]


def _change_types(host, token, warehouse, table, version) -> set[str] | None:
    """The change types Databricks reports for one version, or None if it cannot.

    `table_changes` refuses a range whose commits were written with the feed
    disabled, which is the answer for those versions rather than a failure.

    What it reports is not the same question as what the *log* holds: for a
    delete under deletion vectors Databricks reports `delete` rows either way,
    reconstructing them from the vector when no change data file was written.
    Only the connector's own two counters separate the paths, so the tests
    assert on those and this stays a description of the fixture.
    """
    try:
        return {
            row[0]
            for row in _sql(
                host,
                token,
                warehouse,
                f"SELECT DISTINCT _change_type"
                f" FROM table_changes('{table}', {version}, {version})",
            )
        }
    except SystemExit:
        return None


def _verify_keyed_merge(host, token, warehouse, catalog) -> None:
    table = f"{catalog}.{SCHEMA}.{TABLE}"
    rows = _sql(
        host, token, warehouse, f"SELECT count(*), count(DISTINCT s) FROM {table}"
    )
    assert rows and rows[0] == [str(ROWS), str(ROWS)], f"unexpected contents: {rows}"

    # A merge that recorded no change data would leave the test reading the
    # file-action path and proving nothing, so check before handing it over.
    kinds = _change_types(host, token, warehouse, table, 2)
    assert kinds == {"update_preimage", "update_postimage"}, f"v2 recorded {kinds}"

    print(f"\n{table} ready: {ROWS} rows, v0 create, v1 insert, v2 merge")
    print("  DELTA_TABLE_TEST_UNITY_CDF_TABLE=uc://" + table)


def _verify_stress(host, token, warehouse, catalog) -> None:
    """Report the shape of the history, and check it exercises both read paths.

    A stress fixture whose every commit happened to record change data would
    never reach the file-action fallback, and one that recorded none would never
    reach the change feed; either way the test would be weaker than it looks.
    """
    table = f"{catalog}.{SCHEMA}.{STRESS_TABLE}"
    with_change_data = []
    for name, version in STRESS_HISTORY.items():
        if version == 0:
            continue
        kinds = _change_types(host, token, warehouse, table, version)
        if kinds:
            with_change_data.append(name)
        label = (
            "-- feed disabled" if kinds is None else sorted(kinds) or "-- no changes"
        )
        print(f"  v{version} {name:16} {label}")

    assert with_change_data, "no commit recorded change data; the feed is off"
    assert _change_types(host, token, warehouse, table, BLIND_INSERT_VERSION) is None, (
        f"v{BLIND_INSERT_VERSION} was supposed to be written with the feed off, "
        "so that the file-action fallback carries real rows; it was not"
    )

    total, partitions, moved = _sql(
        host,
        token,
        warehouse,
        f"SELECT count(*), count(DISTINCT grp),"
        f" count_if(grp = '{MOVED_PARTITION}') FROM {table}",
    )[0]
    print(f"\n{table} ready: {total} rows in {partitions} partitions, {moved} moved")
    print("  DELTA_TABLE_TEST_UNITY_STRESS_TABLE=uc://" + table)


def main() -> None:
    host = _require("DELTA_TABLE_TEST_UNITY_HOST").rstrip("/")
    catalog = _require("DELTA_TABLE_TEST_UNITY_CATALOG")
    warehouse = _require("DELTA_TABLE_TEST_UNITY_WAREHOUSE_ID")
    token = _token(
        host,
        _require("DELTA_TABLE_TEST_UNITY_CLIENT_ID"),
        _require("DELTA_TABLE_TEST_UNITY_CLIENT_SECRET"),
    )

    if "--drop" in sys.argv:
        _sql(
            host, token, warehouse, f"DROP SCHEMA IF EXISTS {catalog}.{SCHEMA} CASCADE"
        )
        print("  dropped", f"{catalog}.{SCHEMA}")
        return

    stress = "--stress" in sys.argv
    statements = [f"CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}"] + (
        stress_statements(catalog) if stress else keyed_merge_statements(catalog)
    )
    for statement in statements:
        _sql(host, token, warehouse, statement)
        print("  ok:", statement[:88].replace("\n", " "))

    if stress:
        _verify_stress(host, token, warehouse, catalog)
    else:
        _verify_keyed_merge(host, token, warehouse, catalog)


if __name__ == "__main__":
    main()
