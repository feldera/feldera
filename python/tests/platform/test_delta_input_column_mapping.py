"""Read a column-mapped, schema-evolved Delta table in every ingest mode.

A table with ``delta.columnMapping.mode = 'name'`` stores opaque physical
Parquet column names (``col-<uuid>``) that differ from the logical names; with
``mode = 'id'`` columns are matched by Parquet field ID instead. Either way, a
correct read must resolve every value through the column-mapping metadata in
the Delta log. snapshot reads let delta-rs resolve this; the follow and CDC
paths read each commit's data file as raw Parquet and must resolve the physical
names themselves, including across the fixture's rename/add/drop history (a
renamed column keeps its physical name, so the latest schema still matches it).

PySpark seeds the fixture (it is currently the only writer that supports
column-mapping rename/drop); the builder lives in ``fixtures/column_mapping.py``
and runs via ``uv run --with delta-spark`` so the Spark/JVM wheels are only
fetched on cache miss.
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures.column_mapping import EXPECTED_ROWS
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture

TABLE = "t"
CONNECTOR = "delta_in"
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "v2"

# Spark builder that writes the column-mapped, schema-evolved table. It runs in
# a subprocess (see ensure_delta_spark_fixture) rather than being imported here.
_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "column_mapping.py"


# The final, post-evolution logical columns. Used unless a test reads only part
# of the history (see the end_version test, which sees the pre-drop schema).
_FINAL_COLUMNS = "id BIGINT NOT NULL, full_name VARCHAR, country VARCHAR"


def _build_sql(
    loc: DeltaTestLocation,
    extra_config: dict | None = None,
    columns: str = _FINAL_COLUMNS,
) -> str:
    config = dict(loc.connector_config)
    config.update(extra_config or {})
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {
                    "name": "delta_table_input",
                    "config": config,
                },
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ({columns})"
        f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def _snapshot_rows(pipeline) -> list[dict]:
    rows = pipeline.query(f"SELECT id, full_name, country FROM {TABLE}")
    return sorted(
        (
            {"id": r["id"], "full_name": r["full_name"], "country": r["country"]}
            for r in rows
        ),
        key=lambda r: r["id"],
    )


# Replay the whole fixture history (v0 CREATE through v6) in follow/CDC mode.
# ``version`` is the already-consumed baseline (the empty v0 table), so the
# connector applies every later commit: the three inserts and the interleaved
# rename/add/drop, which together produce the final logical rows.
_REPLAY_CONFIG = {"version": 0, "end_version": 6}


def _run_column_mapping_test(
    pipeline_name: str,
    mapping_mode: str,
    *,
    mode: str,
    extra_config: dict | None = None,
) -> None:
    """Ingest the column-mapped table in ``mode`` and check that every value
    resolves to the expected final logical schema (``EXPECTED_ROWS``).

    snapshot mode reads the latest version directly; follow/CDC replay the log
    from v0 and so must resolve the physical names across the rename/add/drop
    history themselves.

    One wrapper test per (ingest mode, ``delta.columnMapping.mode``) pair rather
    than ``pytest.mark.parametrize`` so each gets a distinct pipeline name — the
    ``pipeline_name`` fixture derives the name from the test function, and
    parametrized cases sharing one name could collide under ``pytest -n``.
    """
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode=mode,
        stable_subpath=f"column_mapping_{mapping_mode}_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER, builder_args=(mapping_mode,))

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

        rows = _snapshot_rows(pipeline)
        assert rows == EXPECTED_ROWS, (
            f"{mode} read must resolve column-mapped, schema-evolved data: "
            "dropped 'amount' gone, 'country' NULL for pre-add rows; "
            f"got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


def test_delta_input_column_mapping_name_snapshot(pipeline_name):
    """Snapshot read with ``delta.columnMapping.mode = 'name'``."""
    _run_column_mapping_test(pipeline_name, "name", mode="snapshot")


def test_delta_input_column_mapping_id_snapshot(pipeline_name):
    """Snapshot read with ``delta.columnMapping.mode = 'id'``."""
    _run_column_mapping_test(pipeline_name, "id", mode="snapshot")


def test_delta_input_column_mapping_name_follow(pipeline_name):
    """Follow a column-mapped (``mode = 'name'``) table across schema evolution.

    The follow path reads each commit's data file as raw Parquet, so it must
    resolve the physical (``col-<uuid>``) names to the latest logical schema
    itself -- including the v1 rows written before the ``name`` -> ``full_name``
    rename, which still match because the physical name is unchanged.
    """
    _run_column_mapping_test(
        pipeline_name,
        "name",
        mode="snapshot_and_follow",
        extra_config=_REPLAY_CONFIG,
    )


def test_delta_input_column_mapping_id_follow(pipeline_name):
    """Follow a column-mapped (``mode = 'id'``) table across schema evolution."""
    _run_column_mapping_test(
        pipeline_name,
        "id",
        mode="snapshot_and_follow",
        extra_config=_REPLAY_CONFIG,
    )


# CDC mode requires a delete filter and an order-by; the fixture has no
# delete-marker column, so use an always-false predicate on a real logical
# column. Every row stays an insert, and resolving the filter and order-by
# against logical names confirms they survive the physical-name remap.
_CDC_CONFIG = {
    **_REPLAY_CONFIG,
    "cdc_delete_filter": "id < 0",
    "cdc_order_by": "id asc",
}


def test_delta_input_column_mapping_name_cdc(pipeline_name):
    """CDC-replay a column-mapped (``mode = 'name'``) table across evolution."""
    _run_column_mapping_test(
        pipeline_name, "name", mode="cdc", extra_config=_CDC_CONFIG
    )


def test_delta_input_column_mapping_id_cdc(pipeline_name):
    """CDC-replay a column-mapped (``mode = 'id'``) table across evolution."""
    _run_column_mapping_test(pipeline_name, "id", mode="cdc", extra_config=_CDC_CONFIG)


# Replaying a window that ends before the table's latest version must resolve
# against that window's final schema, not the latest. Stopping at v4 (after the
# rename and ADD country, before the v5 DROP amount) pins the schema to v4:
# ``amount`` is still present and ``country`` exists but is NULL for the v1 rows
# written before it. A connector pinned to the latest schema instead would drop
# ``amount``, so this guards the ``end_version`` cap on the pinned schema.
_V4_COLUMNS = "id BIGINT NOT NULL, full_name VARCHAR, amount DOUBLE, country VARCHAR"
_V4_EXPECTED_ROWS = [
    {"id": 1, "full_name": "alice", "amount": 10.0, "country": None},
    {"id": 2, "full_name": "bob", "amount": 20.0, "country": None},
    {"id": 3, "full_name": "carol", "amount": 30.0, "country": "US"},
    {"id": 4, "full_name": "dave", "amount": 40.0, "country": "UK"},
]


def test_delta_input_column_mapping_follow_end_version(pipeline_name):
    """Follow a window ending before latest: schema pins to the window's end."""
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="snapshot_and_follow",
        stable_subpath=f"column_mapping_name_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER, builder_args=("name",))

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(
                loc,
                extra_config={"version": 0, "end_version": 4},
                columns=_V4_COLUMNS,
            ),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        rows = sorted(
            (
                {
                    "id": r["id"],
                    "full_name": r["full_name"],
                    "amount": r["amount"],
                    "country": r["country"],
                }
                for r in pipeline.query(
                    f"SELECT id, full_name, amount, country FROM {TABLE}"
                )
            ),
            key=lambda r: r["id"],
        )
        assert rows == _V4_EXPECTED_ROWS, (
            "replay up to v4 must keep the pre-drop schema (amount present); "
            f"got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


# Resuming a pipeline mid-history: ``version`` is a non-zero already-consumed
# baseline, so the connector replays only the commits after it. Starting at v2
# (after the rename, before ADD country) skips the v1 rows (1 and 2) and applies
# v3..v6. The schema still pins to the latest version (v6), so the replayed
# files -- including the v4 rows written under the pre-drop schema -- resolve
# against the final logical schema. This is the restart-a-pipeline path: a
# stored resume version other than v0.
_MID_HISTORY_EXPECTED_ROWS = [
    {"id": 3, "full_name": "carol", "country": "US"},
    {"id": 4, "full_name": "dave", "country": "UK"},
    {"id": 5, "full_name": "erin", "country": "FR"},
]


def test_delta_input_column_mapping_follow_resume_mid_history(pipeline_name):
    """Resume following from a non-zero version: replay only the later commits."""
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="snapshot_and_follow",
        stable_subpath=f"column_mapping_name_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER, builder_args=("name",))

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc, extra_config={"version": 2, "end_version": 6}),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        rows = _snapshot_rows(pipeline)
        assert rows == _MID_HISTORY_EXPECTED_ROWS, (
            "resuming at v2 must replay only v3..v6 (rows 1 and 2 already "
            f"consumed) and resolve them against the final schema; got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()
