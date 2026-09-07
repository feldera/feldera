"""Read a kitchen-sink Databricks table over its change feed, at scale.

Gated on ``DELTA_TABLE_TEST_UNITY_STRESS_TABLE``, because it needs a live Unity
Catalog table that ``fixtures/unity_change_feed.py --stress`` builds:

    source ~/.feldera-dbx-test.env
    python python/tests/platform/fixtures/unity_change_feed.py --stress

Everything the other change-feed tests cover one at a time is present at once
here, written by Databricks rather than by delta-rs or local Spark: every column
type the connector reads, name-mode column mapping, partitioning, deletion
vectors, row tracking, and a history mixing inserts, a four-change-type merge, a
delete, partition moves, `OPTIMIZE`, and a block of rows written while the feed
was switched off. The table is read over ``uc://``, so the catalog vends the
storage credentials and every file is fetched through the object store.

Two things are checked, and they fail for different reasons:

* every run agrees with Databricks' own aggregates over the same table, which is
  the oracle for *what* was read;
* the runs agree with each other across ``change_feed`` and ``transaction_mode``,
  which is the oracle for *how*. `off` reads the same table from the file
  actions alone, so a change-feed defect that a fixed expectation might have been
  written around still shows up as a disagreement.

The table is keyed on `id`, which is what makes an ordering defect visible: a
retraction on a keyed relation is a delete *by key*, so a post-image applied
before its pre-image drops the row instead of cancelling out by value.

Blocked on apache/arrow-rs#11018, with no released `parquet` that works.
`deletion_vector.rs` applies a deletion vector as a Parquet row selection, so
the reader *skips* the deleted rows, and `DeltaBitPackDecoder::skip` mishandles
the 256-value miniblocks Photon writes: 58.3 and later reject them outright
(`cannot skip miniblock of size 256`), while 58.2 and earlier panic once a skip
run exceeds the buffer they size from the physical type -- 64 for INT64 --
which a deletion vector's runs do. Downgrading trades the error for a panic; it
is not a workaround.

Only columns Photon delta-encodes are affected, which it does when the table
carries `delta.parquet.format.version = 2.12.0` (a Unity Catalog schema default
in our workspace). A column whose deltas are constant, such as a dense ascending
key, takes a fast path that sidesteps both failures, so a fixture can pass while
the defect is present -- see https://github.com/ryzhyk/parquet-photon-miniblock-repro.

The symptom here is this test timing out on the first run rather than failing:
the connector retries the unreadable file, so the error repeats in the pipeline
log instead of surfacing.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from typing import Any

import pytest

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT


def _load_fixture_module():
    path = Path(__file__).parent / "fixtures" / "unity_change_feed.py"
    spec = importlib.util.spec_from_file_location("unity_change_feed", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixture = _load_fixture_module()

TABLE = "stress"
CONNECTOR = "stress_in"
SUMMARY_VIEW = "stress_summary"

# The environment the fixture script writes; without them there is no table.
REQUIRED_ENV = [
    "DELTA_TABLE_TEST_UNITY_STRESS_TABLE",
    "DELTA_TABLE_TEST_UNITY_HOST",
    "DELTA_TABLE_TEST_UNITY_CLIENT_ID",
    "DELTA_TABLE_TEST_UNITY_CLIENT_SECRET",
    "DELTA_TABLE_TEST_UNITY_WAREHOUSE_ID",
]
MISSING_ENV = [name for name in REQUIRED_ENV if not os.environ.get(name)]

pytestmark = pytest.mark.skipif(
    bool(MISSING_ENV),
    reason=(
        f"unset: {', '.join(MISSING_ENV)}. Build the table with "
        "`python python/tests/platform/fixtures/unity_change_feed.py --stress`."
    ),
)

# One aggregate per row: (alias, Feldera SQL, Databricks SQL). They differ only
# where the dialects do -- array subscripts are 1-based in Feldera and 0-based in
# Databricks, and the length of an array is `CARDINALITY` against `size`.
#
# Every column is covered, but not every one by a sum: floats are counted rather
# than added, because a sum over 10^5 of them depends on the order they arrive
# in and would fail for a reason that is not a defect. The rest are exact --
# integers, a decimal scaled to an integer, string extremes, distinct counts,
# and a field reached through a struct, an array and a map.
AGGREGATES: list[tuple[str, str, str]] = [
    ("total", "COUNT(*)", "count(*)"),
    ("sum_id", "SUM(id)", "sum(id)"),
    ("sum_int", "SUM(CAST(c_int AS BIGINT))", "sum(cast(c_int as bigint))"),
    (
        "sum_smallint",
        "SUM(CAST(c_smallint AS BIGINT))",
        "sum(cast(c_smallint as bigint))",
    ),
    ("sum_tinyint", "SUM(CAST(c_tinyint AS BIGINT))", "sum(cast(c_tinyint as bigint))"),
    (
        "sum_decimal",
        "SUM(CAST(c_decimal * 1000 AS BIGINT))",
        "sum(cast(c_decimal * 1000 as bigint))",
    ),
    # A seventh of the rows are NULL here, so a reader that filled the column in
    # cannot match this by accident.
    ("nullable_rows", "COUNT(c_nullable)", "count(c_nullable)"),
    ("partitions", "COUNT(DISTINCT grp)", "count(distinct grp)"),
    (
        "moved_rows",
        f"SUM(CASE WHEN grp = '{fixture.MOVED_PARTITION}' THEN 1 ELSE 0 END)",
        f"sum(case when grp = '{fixture.MOVED_PARTITION}' then 1 else 0 end)",
    ),
    (
        "true_rows",
        "SUM(CASE WHEN c_boolean THEN 1 ELSE 0 END)",
        "sum(case when c_boolean then 1 else 0 end)",
    ),
    ("binary_rows", "COUNT(c_binary)", "count(c_binary)"),
    ("double_rows", "COUNT(c_double)", "count(c_double)"),
    ("float_rows", "COUNT(c_float)", "count(c_float)"),
    ("distinct_dates", "COUNT(DISTINCT c_date)", "count(distinct c_date)"),
    (
        "distinct_timestamps",
        "COUNT(DISTINCT c_timestamp)",
        "count(distinct c_timestamp)",
    ),
    ("min_string", "MIN(c_string)", "min(c_string)"),
    ("max_string", "MAX(c_string)", "max(c_string)"),
    ("array_len", "SUM(CARDINALITY(c_array))", "sum(size(c_array))"),
    # `stress.` is needed for a plain ROW; a field reached through a subscript
    # is unambiguous without it.
    ("struct_sum", f"SUM({TABLE}.c_struct.f_id)", "sum(c_struct.f_id)"),
    ("struct_array_sum", "SUM(c_struct_array[1].f_id)", "sum(c_struct_array[0].f_id)"),
    ("map_rows", "COUNT(c_map['k'])", "count(c_map['k'])"),
    ("struct_map_sum", "SUM(c_struct_map['k'].f_id)", "sum(c_struct_map['k'].f_id)"),
    ("variant_rows", "COUNT(c_variant)", "count(c_variant)"),
    ("uuid_rows", "COUNT(c_uuid)", "count(c_uuid)"),
]

# Which commit last wrote each row, by the prefix its string columns carry: `b`
# from the two inserts, `c` from the merge, `d` from the update, `e` from the
# block written with the feed off. Counting them separates a lost update from a
# lost row, which a total alone cannot.
for _tag in "bcde":
    AGGREGATES.append(
        (
            f"{_tag}_rows",
            f"SUM(CASE WHEN c_string LIKE '{_tag}%' THEN 1 ELSE 0 END)",
            f"sum(case when c_string like '{_tag}%' then 1 else 0 end)",
        )
    )


def _connector_config(change_feed: str, transaction_mode: str) -> dict[str, Any]:
    return {
        "uri": os.environ["DELTA_TABLE_TEST_UNITY_STRESS_TABLE"],
        "mode": "snapshot_and_follow",
        "change_feed": change_feed,
        "transaction_mode": transaction_mode,
        "version": fixture.STRESS_HISTORY["create"],
        "end_version": fixture.STRESS_LAST_VERSION,
        "unity_client_id": os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_ID"],
        "unity_client_secret": os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_SECRET"],
        "databricks_host": os.environ["DELTA_TABLE_TEST_UNITY_HOST"],
        # Must be set explicitly (delta-rs #1095), and long enough for a cold
        # read of files the catalog vends credentials for.
        "aws_region": os.environ.get("DELTA_TABLE_TEST_UNITY_REGION", "us-west-1"),
        "timeout": "1000 secs",
    }


def _build_sql(change_feed: str, transaction_mode: str) -> str:
    """The table, plus a one-row view holding every aggregate.

    The aggregates are computed by a view rather than by an ad hoc query so that
    the nested-column expressions are parsed by the pipeline's own SQL dialect,
    which is the one the column types are declared in.
    """
    import json

    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": _connector_config(change_feed, transaction_mode),
                },
            }
        ]
    ).replace("'", "''")
    selects = ",\n    ".join(f"{expr} AS {alias}" for alias, expr, _ in AGGREGATES)
    return (
        f"CREATE TABLE {TABLE} ({fixture.stress_feldera_columns()})"
        f" WITH ('connectors' = '{connectors}');\n"
        f"CREATE MATERIALIZED VIEW {SUMMARY_VIEW} AS SELECT\n"
        f"    {selects}\n"
        f"FROM {TABLE};"
    )


# Every commit increments exactly one of the first two, so together they say
# which read path each commit took -- the only place that distinction is
# observable, since Databricks reports the same change rows for a delete whether
# it wrote change data or reconstructed them from the deletion vector. The third
# says whether the follow commits were ingested inside Feldera transactions,
# which is what separates a `transaction_mode` run from a repeat of the first.
FROM_CHANGE_DATA = "input_connector_delta_commits_from_change_data"
FROM_FILE_ACTIONS = "input_connector_delta_commits_from_file_actions"
FOLLOW_TRANSACTIONS = "input_connector_delta_follow_transaction_starts"


def _delta_counters(pipeline) -> dict[str, int]:
    """Read the connector's counters off the Prometheus endpoint.

    Summed over the scrape's lines rather than matched to one, so a multi-host
    pipeline reports the whole connector rather than one host's share.
    """
    counters = dict.fromkeys(
        [FROM_CHANGE_DATA, FROM_FILE_ACTIONS, FOLLOW_TRANSACTIONS], 0
    )
    for line in pipeline.metrics(format="prometheus").splitlines():
        for name in counters:
            if line.startswith(name):
                counters[name] += int(float(line.rsplit(" ", 1)[-1]))
    return counters


def _ingest(
    pipeline_name: str, *, change_feed: str, transaction_mode: str
) -> tuple[dict[str, str], dict[str, int]]:
    """Replay the whole history once and return the summary and the counters."""
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=_build_sql(change_feed, transaction_mode),
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
        ),
    ).create_or_replace()
    pipeline.start()
    try:
        # A connector that cannot read a file retries it forever rather than
        # failing, so a stall arrives here as the timeout rather than as an
        # error on the endpoint.
        pipeline.wait_for_completion(force_stop=False, timeout_s=1800)
        rows = list(pipeline.query(f"SELECT * FROM {SUMMARY_VIEW}"))
        # Read before stopping: the metrics are only live while the pipeline runs.
        counters = _delta_counters(pipeline)
    finally:
        pipeline.stop(force=True)

    assert len(rows) == 1, f"the summary view must hold one row, got {len(rows)}"
    summary = {alias: _normalize(rows[0][alias]) for alias, _, _ in AGGREGATES}
    return summary, counters


def _normalize(value) -> str:
    """One spelling per value, so the two sources can be compared as text.

    Databricks returns every column as a string and Feldera returns numbers as
    numbers, so neither side's native types survive the comparison anyway.
    """
    return "NULL" if value is None else str(value)


def _databricks_summary() -> dict[str, str]:
    """The same aggregates, computed by Databricks over the same table."""
    host = os.environ["DELTA_TABLE_TEST_UNITY_HOST"].rstrip("/")
    token = fixture._token(
        host,
        os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_ID"],
        os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_SECRET"],
    )
    table = os.environ["DELTA_TABLE_TEST_UNITY_STRESS_TABLE"].removeprefix("uc://")
    selects = ", ".join(f"{expr} AS {alias}" for alias, _, expr in AGGREGATES)
    row = fixture._sql(
        host,
        token,
        os.environ["DELTA_TABLE_TEST_UNITY_WAREHOUSE_ID"],
        f"SELECT {selects} FROM {table}",
    )[0]
    return {alias: _normalize(value) for (alias, _, _), value in zip(AGGREGATES, row)}


def _assert_matches(label: str, got: dict[str, str], want: dict[str, str]) -> None:
    differences = [
        f"  {alias}: {label} {got[alias]!r} != {want[alias]!r}"
        for alias in got
        if got[alias] != want[alias]
    ]
    assert not differences, f"{label} disagrees:\n" + "\n".join(differences)


def test_delta_input_unity_stress(pipeline_name):
    """One Unity Catalog table read four ways -- `change_feed=auto`,
    `change_feed=off`, and `auto` again under each of `transaction_mode`
    `always` and `catchup` -- arrives with the contents Databricks reports for
    it.

    The four runs are not four assertions of the same thing. `off` reaches the
    table through file actions, an independent path to the same contents, so
    the `auto` runs have something to agree with; `always` and `catchup` cut
    the same history into transactions at different points, so a boundary drawn
    in the middle of a commit's changes shows up here as a lost or doubled
    row.
    """
    expected = _databricks_summary()
    assert int(expected["total"]) > 0, "the fixture table is empty"

    baseline, counters = _ingest(
        f"{pipeline_name}_auto",
        change_feed="auto",
        transaction_mode="none",
    )
    _assert_matches("change_feed=auto", baseline, expected)
    assert counters[FROM_CHANGE_DATA] > 0, (
        "no commit was read from its change data, so this ran as an expensive "
        "copy of the file-action test"
    )
    assert counters[FROM_FILE_ACTIONS] > 0, (
        "no commit fell back to its file actions, so the rows the fixture wrote "
        f"with the feed off (v{fixture.BLIND_INSERT_VERSION}) cannot have arrived"
    )

    off, off_counters = _ingest(
        f"{pipeline_name}_off",
        change_feed="off",
        transaction_mode="none",
    )
    assert off_counters[FROM_CHANGE_DATA] == 0, (
        "`change_feed = off` must not read change data; it read "
        f"{off_counters[FROM_CHANGE_DATA]} commits from it"
    )
    _assert_matches("change_feed=off", off, expected)

    for transaction_mode in ("always", "catchup"):
        result, txn_counters = _ingest(
            f"{pipeline_name}_{transaction_mode}",
            change_feed="auto",
            transaction_mode=transaction_mode,
        )
        assert txn_counters[FROM_CHANGE_DATA] > 0, (
            f"transaction_mode={transaction_mode} stopped reading change data"
        )
        _assert_matches(f"transaction_mode={transaction_mode}", result, expected)
        if transaction_mode == "always":
            # `catchup` only batches a burst into a transaction when the
            # connector is orchestrated with pause/resume, which this test does
            # not do, so only `always` is guaranteed to open one here.
            assert txn_counters[FOLLOW_TRANSACTIONS] > 0, (
                "transaction_mode=always ingested every commit outside a "
                "transaction, so this run repeated the first one"
            )
