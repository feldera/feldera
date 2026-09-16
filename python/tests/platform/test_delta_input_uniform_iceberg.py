"""Reads of a UC-Uniform-over-Iceberg table (``columnMapping.mode='id'``).

A Unity Catalog Uniform table exposes Iceberg data files as Delta. A native
Iceberg writer (Flink / pyiceberg) names the Parquet columns by their *logical*
names and identifies them by Parquet ``field_id``; the synthesized Delta log uses
``columnMapping.mode='id'`` with physical names ``col-<id>``. A correct read must
resolve each column by field id, not by physical name -- otherwise a column whose
physical name (``col-<id>``) is absent from the logically-named file reads as NULL
(nullable) or fails outright (non-nullable).

Each ingest mode plans its reads differently, so each is covered: ``snapshot``
reads the table at its latest version, while ``follow`` and ``cdc`` replay the
log commit by commit. The fixture's two commits let a replay starting at v0 see
only the second one, which distinguishes a working follow path from one that
quietly re-read the whole table.

This reproduces the customer's ``cdc_raw`` failure on two fronts: the non-nullable
top-level ``op`` (physical name ``col-102``) is missing from the physical schema
under by-name resolution, and the nested ``after`` struct's six children are
*themselves* mapped to ``col-<id>`` -- so a by-name struct cast finds no overlap
with the file's logical child names ("Cannot cast struct with 6 fields to 6 fields
because there is no field name overlap"). A correct read resolves by field id at
every level. The fixture is written with pyarrow (Delta Spark cannot produce
logical-name-on-disk Parquet); the builder lives in ``fixtures/uniform_iceberg.py``
and runs via ``uv run --with pyarrow``.
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures.uniform_iceberg import EXPECTED_ROWS, V1_ROWS
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture

TABLE = "t"
CONNECTOR = "delta_in"
# Bump to invalidate cached MinIO/local copies when the fixture definition changes.
FIXTURE_VERSION = "v2"

# pyarrow builder that writes the logical-name Parquet + hand-written _delta_log.
# It runs in a subprocess (see ensure_delta_spark_fixture) with only pyarrow.
_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "uniform_iceberg.py"


# Replay the second commit only: v0 is the already-consumed baseline. A follow or
# CDC read that ignored the baseline and re-read the table would return v0's rows
# too, so the narrower expectation is the point.
_REPLAY_CONFIG = {"version": 0, "end_version": 1}

# CDC mode needs a delete filter and an order-by. The fixture marks no deletes, so
# use a predicate no row satisfies; resolving it against the logical column name
# also confirms a filter survives the physical-name remap.
_CDC_CONFIG = {
    **_REPLAY_CONFIG,
    "cdc_delete_filter": "op = 'x'",
    "cdc_order_by": "id asc",
}


def _build_sql(loc: DeltaTestLocation, extra_config: dict | None = None) -> str:
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": {**loc.connector_config, **(extra_config or {})},
                },
            }
        ]
    ).replace("'", "''")
    # `op` is declared NOT NULL to match the customer; `after` is the customer's
    # six-field nested ROW whose children are column-mapped to diverging `col-<id>`
    # names. Pre-fix, the snapshot fails resolving `op`/`col-102` and casting the
    # `after` struct ("no field name overlap").
    return (
        f"CREATE TABLE {TABLE} ("
        "id VARCHAR,"
        "after ROW("
        "transaction__id VARCHAR,"
        "transaction__merchant_id VARCHAR,"
        "transaction__merchant_name VARCHAR,"
        "transaction__time VARCHAR,"
        "transaction__status VARCHAR,"
        "transaction__amount VARCHAR"
        "),"
        "op VARCHAR NOT NULL"
        f") WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def _snapshot_rows(pipeline) -> list[dict]:
    # Project the two diverging top-level scalars plus a nested child of `after`.
    # The nested projection exercises field-id resolution *inside* the struct --
    # the case that fails the struct cast pre-fix, not just the top-level columns.
    rows = pipeline.query(
        f"SELECT id, op, after.transaction__merchant_name AS merchant FROM {TABLE}"
    )
    return sorted(
        ({"id": r["id"], "op": r["op"], "merchant": r["merchant"]} for r in rows),
        key=lambda r: r["id"],
    )


def _run_uniform_test(
    pipeline_name: str,
    *,
    mode: str,
    expected_rows: list[dict],
    extra_config: dict | None = None,
) -> None:
    """Ingest the Uniform table in ``mode`` and check that every column resolves
    by Parquet field id: the diverging non-nullable ``op`` (``col-102``) comes
    back with real values instead of failing, and the nested ``after`` struct --
    whose children are themselves mapped to ``col-<id>`` -- is reconstructed by
    field id instead of failing the struct cast.

    One wrapper test per mode rather than ``pytest.mark.parametrize`` so each gets
    a distinct pipeline name: the ``pipeline_name`` fixture derives the name from
    the test function, and parametrized cases sharing one name could collide
    under ``pytest -n``.
    """
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode=mode,
        stable_subpath=f"uniform_iceberg_id_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(
            loc, _FIXTURE_BUILDER, delta_spark_spec="pyarrow>=15"
        )

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc, extra_config),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        expected = sorted(
            (
                {
                    "id": r["id"],
                    "op": r["op"],
                    "merchant": r["after"]["transaction__merchant_name"],
                }
                for r in expected_rows
            ),
            key=lambda r: r["id"],
        )
        rows = _snapshot_rows(pipeline)
        assert rows == expected, (
            f"{mode} read must resolve column-mapped Uniform/Iceberg data by "
            "field id at every level (top-level op/col-102 and the nested after "
            f"struct's children); got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


def test_delta_input_uniform_iceberg_id_snapshot(pipeline_name):
    """Snapshot read of the whole table at its latest version."""
    _run_uniform_test(pipeline_name, mode="snapshot", expected_rows=EXPECTED_ROWS)


def test_delta_input_uniform_iceberg_id_follow(pipeline_name):
    """Follow the log from v0, which applies the second commit alone.

    Follow plans its reads per commit rather than over the table, so it resolves
    columns through a different path than ``snapshot`` and needs its own case.
    """
    _run_uniform_test(
        pipeline_name,
        mode="follow",
        expected_rows=V1_ROWS,
        extra_config=_REPLAY_CONFIG,
    )


def test_delta_input_uniform_iceberg_id_cdc(pipeline_name):
    """Read the second commit in CDC mode, where no row is a delete."""
    _run_uniform_test(
        pipeline_name,
        mode="cdc",
        expected_rows=V1_ROWS,
        extra_config=_CDC_CONFIG,
    )
