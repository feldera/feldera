"""Read a real Unity Catalog Uniform/Iceberg table, in every ingest mode.

Gated on ``DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE``, because it needs a live Unity
Catalog table that ``fixtures/unity_uniform.py`` builds:

    source ~/.feldera-dbx-test.env
    cd python && .venv/bin/python -m tests.platform.fixtures.unity_uniform

Unity Catalog exposes a managed Iceberg table as Delta: the data files carry the
*logical* column names with a Parquet ``field_id``, while the synthesized Delta
log is ``columnMapping.mode = 'id'`` with physical names ``col-<id>``. Pairing
the two by name finds nothing, so a nullable column reads as NULL and a
``NOT NULL`` one fails the read (#7076). ``fixtures/uniform_iceberg.py`` builds
the same shape locally; only this test covers Databricks' own log synthesis and
the ``uc://`` path, where the catalog vends the storage credentials and every
file is fetched through the object store rather than listed.

Databricks is the oracle: the aggregates are computed twice over the same table,
once by the pipeline and once by the workspace, so nothing here is a hardcoded
expectation that could be written around a defect. Every column is counted,
which is what makes the #7076 symptom visible -- a column resolved by name
against a logically-named file reads as entirely NULL, and its count drops to
zero while the row total stays right.

Each mode plans its reads differently and gets its own test: ``snapshot`` reads
the table at its latest version, while ``follow`` and ``cdc`` replay the log
commit by commit. The fixture's commits let the replays take the last one alone,
which distinguishes a working replay from one that quietly re-read the table.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures import unity_api, unity_uniform as fixture

TABLE = "t"
CONNECTOR = "uniform_in"
SUMMARY_VIEW = "uniform_summary"

# The environment the fixture script writes; without them there is no table.
REQUIRED_ENV = [
    "DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE",
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
        "`python -m tests.platform.fixtures.unity_uniform`."
    ),
)


@pytest.fixture(scope="module", autouse=True)
def require_id_mapped_table():
    """Fail the module unless the table under test is really id-mapped.

    Databricks is the oracle, so it reads whatever table this points at and
    agrees with the pipeline about it. A table that lost the mapping, or an
    environment left pointing at an older fixture, would pass every case below
    while covering none of what they exist to cover.
    """
    host = os.environ["DELTA_TABLE_TEST_UNITY_HOST"].rstrip("/")
    table = os.environ["DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE"].removeprefix("uc://")
    mode = fixture.column_mapping_mode(host, unity_api.token_from_env(host), table)
    assert mode == "id", (
        f"{table} has columnMapping.mode = {mode!r}, not 'id'. Rebuild the "
        "fixture with `python -m tests.platform.fixtures.unity_uniform` and set "
        "DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE to the name it prints."
    )


# One aggregate per row: (alias, Feldera SQL, Databricks SQL). They differ only
# where the dialects do -- the length of an array is `CARDINALITY` against
# `size`.
#
# Each column is counted as well as summed, because the two fail differently: a
# column paired by name against a logically-named file comes back entirely NULL,
# which a count catches and a sum over the surviving columns would not.
AGGREGATES: list[tuple[str, str, str]] = [
    ("total", "COUNT(*)", "count(*)"),
    ("n_id", "COUNT(id)", "count(id)"),
    ("n_batch", "COUNT(batch)", "count(batch)"),
    ("n_amount", "COUNT(amount)", "count(amount)"),
    ("n_op", "COUNT(op)", "count(op)"),
    # Reached through the struct: its children are mapped too, so this is zero
    # unless the pairing resolves by field id inside a struct as well.
    (
        "n_merchant",
        "COUNT(after.transaction__merchant_name)",
        "count(after.transaction__merchant_name)",
    ),
    ("n_history", "COUNT(CARDINALITY(history))", "count(size(history))"),
    ("n_tags", "COUNT(tags['channel'])", "count(tags['channel'])"),
    # Scaled to an integer so the two sides spell a decimal the same way.
    (
        "sum_amount",
        "SUM(CAST(amount * 100 AS BIGINT))",
        "sum(cast(amount * 100 as bigint))",
    ),
    ("min_id", "MIN(id)", "min(id)"),
    ("max_id", "MAX(id)", "max(id)"),
    ("min_after_id", "MIN(after.transaction__id)", "min(after.transaction__id)"),
    ("len_history", "SUM(CARDINALITY(history))", "sum(size(history))"),
    ("min_tag", "MIN(tags['region'])", "min(tags['region'])"),
]

# Replay the last commit alone: the ones before it are the already-consumed
# baseline. A replay that ignored the baseline would return the whole table, so
# the narrower expectation is the point.
_REPLAY_CONFIG = {
    "version": fixture.LAST_VERSION - 1,
    "end_version": fixture.LAST_VERSION,
}

# CDC mode needs a delete filter and an order-by. The fixture marks no deletes,
# so use a predicate no row satisfies; resolving it against the logical column
# name also confirms a filter survives the physical-name remap.
_CDC_CONFIG = {
    **_REPLAY_CONFIG,
    "cdc_delete_filter": "op = 'x'",
    "cdc_order_by": "id asc",
}


def _connector_config(mode: str, extra: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "uri": os.environ["DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE"],
        "mode": mode,
        "unity_client_id": os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_ID"],
        "unity_client_secret": os.environ["DELTA_TABLE_TEST_UNITY_CLIENT_SECRET"],
        "databricks_host": os.environ["DELTA_TABLE_TEST_UNITY_HOST"],
        # Must be set explicitly (delta-rs #1095), and long enough for a cold
        # read of files the catalog vends credentials for.
        "aws_region": os.environ.get("DELTA_TABLE_TEST_UNITY_REGION", "us-west-1"),
        "timeout": "1000 secs",
        **(extra or {}),
    }


def _build_sql(mode: str, extra: dict[str, Any] | None) -> str:
    """The table, plus a one-row view holding every aggregate.

    The aggregates are computed by a view rather than by an ad hoc query so that
    the nested-column expressions are parsed by the pipeline's own SQL dialect,
    which is the one the column types are declared in.

    :param mode: The connector's ``mode``.
    :param extra: Connector fields the mode needs, such as a replay window.
    """
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": _connector_config(mode, extra),
                },
            }
        ]
    ).replace("'", "''")
    selects = ",\n    ".join(f"{expr} AS {alias}" for alias, expr, _ in AGGREGATES)
    return (
        f"CREATE TABLE {TABLE} ({fixture.FELDERA_COLUMNS})"
        f" WITH ('connectors' = '{connectors}');\n"
        f"CREATE MATERIALIZED VIEW {SUMMARY_VIEW} AS SELECT\n"
        f"    {selects}\n"
        f"FROM {TABLE};"
    )


def _normalize(value) -> str:
    """One spelling per value, so the two sources can be compared as text.

    Databricks returns every column as a string and Feldera returns numbers as
    numbers, so neither side's native types survive the comparison anyway.
    """
    return "NULL" if value is None else str(value)


def _ingest(pipeline_name: str, mode: str, extra: dict[str, Any] | None) -> dict:
    """Read the table once in ``mode`` and return the pipeline's aggregates."""
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=_build_sql(mode, extra),
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
        pipeline.wait_for_completion(force_stop=False, timeout_s=900)
        rows = list(pipeline.query(f"SELECT * FROM {SUMMARY_VIEW}"))
    finally:
        pipeline.stop(force=True)

    assert len(rows) == 1, f"the summary view must hold one row, got {len(rows)}"
    return {alias: _normalize(rows[0][alias]) for alias, _, _ in AGGREGATES}


def _databricks_summary(where: str) -> dict[str, str]:
    """The same aggregates, computed by Databricks over the same table.

    :param where: The rows the mode under test should have delivered, as a SQL
        predicate.
    """
    host = os.environ["DELTA_TABLE_TEST_UNITY_HOST"].rstrip("/")
    token = unity_api.token_from_env(host)
    table = os.environ["DELTA_TABLE_TEST_UNITY_UNIFORM_TABLE"].removeprefix("uc://")
    selects = ", ".join(f"{expr} AS {alias}" for alias, _, expr in AGGREGATES)
    row = unity_api.sql(
        host,
        token,
        os.environ["DELTA_TABLE_TEST_UNITY_WAREHOUSE_ID"],
        f"SELECT {selects} FROM {table} WHERE {where}",
    )[0]
    return {alias: _normalize(value) for (alias, _, _), value in zip(AGGREGATES, row)}


def _run_uniform_test(
    pipeline_name: str,
    *,
    mode: str,
    where: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """Read the table in ``mode`` and check it against Databricks' own answer.

    :param mode: The connector's ``mode``.
    :param where: The rows this mode should deliver, as a Databricks predicate.
    :param extra: Connector fields the mode needs, such as a replay window.
    """
    expected = _databricks_summary(where)
    assert int(expected["total"]) > 0, (
        f"Databricks reports no rows for `{where}`; the fixture table is empty "
        "or was built with different batches"
    )

    got = _ingest(pipeline_name, mode, extra)
    differences = [
        f"  {alias}: read {got[alias]!r}, Databricks {expected[alias]!r}"
        for alias in expected
        if got[alias] != expected[alias]
    ]
    assert not differences, (
        f"the {mode} read disagrees with Databricks over the same table. A count "
        f"of zero against a non-zero total is a column paired by name instead of "
        f"by field id:\n" + "\n".join(differences)
    )


def test_delta_input_uniform_unity_snapshot(pipeline_name):
    """Snapshot read of the whole table at its latest version."""
    _run_uniform_test(pipeline_name, mode="snapshot", where="true")


def test_delta_input_uniform_unity_follow(pipeline_name):
    """Follow the log across the last commit, which is the only one applied.

    Follow plans its reads per commit rather than over the table, so it resolves
    columns through a different path than ``snapshot`` and needs its own case.
    """
    _run_uniform_test(
        pipeline_name,
        mode="follow",
        where=f"batch = '{fixture.REPLAY_BATCH}'",
        extra=_REPLAY_CONFIG,
    )


def test_delta_input_uniform_unity_cdc(pipeline_name):
    """Read the last commit in CDC mode, where no row is a delete.

    A ``uc://`` location has no directory to list, so CDC reads every file
    straight from the object store (#7112). That branch has no other coverage.
    """
    _run_uniform_test(
        pipeline_name,
        mode="cdc",
        where=f"batch = '{fixture.REPLAY_BATCH}'",
        extra=_CDC_CONFIG,
    )
