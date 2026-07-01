"""Read a column-mapped, schema-evolved Delta table in every ingest mode.

A table with ``delta.columnMapping.mode = 'name'`` stores opaque physical
Parquet column names (``col-<uuid>``) that differ from the logical names; with
``mode = 'id'`` columns are matched by Parquet field ID instead. Either way, a
correct read must resolve every value through the column-mapping metadata in
the Delta log. snapshot reads let delta-rs resolve this against the latest
schema; the follow and CDC paths read each commit's data file as raw Parquet and
must resolve the physical names themselves, against the schema active when that
commit was written. Because of that, replaying the rename/add/drop history from
v0 does not converge to a snapshot of the latest table: rows written before a
column was renamed resolve under the old logical name (NULL in the SQL relation).

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
# rename/add/drop.
_REPLAY_CONFIG = {"version": 0, "end_version": 6}


# Follow/CDC read each commit's data against the schema active when that commit
# was written, so a replay from v0 does NOT converge to a snapshot of the latest
# table on renamed columns. The v1 rows (1, 2) were written under the original
# logical name ``name``; at v1 the connector resolves them to ``name``, which the
# SQL relation (``full_name``) does not have, so ``full_name`` is NULL for them.
# Rows written at v4/v6 (after the v2 rename) carry ``full_name`` and resolve
# normally. Contrast ``EXPECTED_ROWS``, which a snapshot read of v6 produces by
# resolving the whole table against its final schema.
_REPLAY_EXPECTED_ROWS = [
    {"id": 1, "full_name": None, "country": None},
    {"id": 2, "full_name": None, "country": None},
    {"id": 3, "full_name": "carol", "country": "US"},
    {"id": 4, "full_name": "dave", "country": "UK"},
    {"id": 5, "full_name": "erin", "country": "FR"},
]


def _run_column_mapping_test(
    pipeline_name: str,
    mapping_mode: str,
    *,
    mode: str,
    extra_config: dict | None = None,
) -> None:
    """Ingest the column-mapped table in ``mode`` and check that every value
    resolves through the column-mapping metadata.

    snapshot mode reads the latest version directly, resolving the whole table
    against its final schema (``EXPECTED_ROWS``). follow/CDC replay the log from
    v0 and read each commit against the schema active when it was written, so
    the pre-rename rows surface their old logical name and ``full_name`` is NULL
    for them (``_REPLAY_EXPECTED_ROWS``).

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
        expected = EXPECTED_ROWS if mode == "snapshot" else _REPLAY_EXPECTED_ROWS
        assert rows == expected, (
            f"{mode} read must resolve column-mapped, schema-evolved data: "
            "dropped 'amount' gone, 'country' NULL for pre-add rows, and "
            "(follow/CDC only) 'full_name' NULL for rows written before the "
            f"rename; got {rows}"
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

    The follow path reads each commit's data file as raw Parquet, resolving the
    physical (``col-<uuid>``) names against the schema active when that commit
    was written. The v1 rows predate the ``name`` -> ``full_name`` rename, so
    they resolve to ``name`` and read NULL for the SQL ``full_name`` column;
    rows written after the rename resolve normally (``_REPLAY_EXPECTED_ROWS``).
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


# Replaying a window that ends at v4 (after the rename and ADD country, before
# the v5 DROP amount) reads each commit against its own schema. The v1 rows
# (1, 2) were written under ``(id, name, amount)``: ``amount`` is present (10/20),
# but the column is logically ``name`` there, so the SQL ``full_name`` is NULL
# for them; ``country`` did not yet exist, so it is NULL too. The v4 rows (3, 4)
# were written after the rename and ADD, so every column resolves. ``amount``
# survives for all rows because it is dropped only at v5, past this window.
_V4_COLUMNS = "id BIGINT NOT NULL, full_name VARCHAR, amount DOUBLE, country VARCHAR"
_V4_EXPECTED_ROWS = [
    {"id": 1, "full_name": None, "amount": 10.0, "country": None},
    {"id": 2, "full_name": None, "amount": 20.0, "country": None},
    {"id": 3, "full_name": "carol", "amount": 30.0, "country": "US"},
    {"id": 4, "full_name": "dave", "amount": 40.0, "country": "UK"},
]


def test_delta_input_column_mapping_follow_end_version(pipeline_name):
    """Follow a window ending at v4: each commit resolves against its own schema."""
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
            "replay up to v4 must read each commit against its own schema: "
            "'amount' present (dropped only at v5), 'full_name' NULL for the "
            f"pre-rename v1 rows; got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


# Resuming a pipeline mid-history: ``version`` is a non-zero already-consumed
# baseline, so the connector replays only the commits after it. Starting at v2
# (after the rename, before ADD country) skips the v1 rows (1 and 2) and applies
# v3..v6. Every replayed commit here postdates the rename, so reading each
# against its own active schema resolves ``full_name`` for all of them -- no
# divergence from a snapshot in this window. This is the restart-a-pipeline
# path: a stored resume version other than v0.
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
            "consumed), each resolved against its own (post-rename) schema; "
            f"got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()
