"""
Delta Lake input connector catchup tests.

* ``snapshot_and_follow`` + ``transaction_mode=catchup`` — initial snapshot in
  one transaction, then pause/resume follow rounds.
* ``cdc`` + ``transaction_mode=catchup`` — no initial snapshot; pre-existing
  commits are skipped, then the same pause/resume catchup rounds.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from http import HTTPStatus

import pyarrow as pa
import pytest

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT, enterprise_only
from tests.platform.helper import api_url, get
from tests.utils import DeltaTestLocation, wait_for_condition

TABLE = "t"
CONNECTOR = "delta_in"
ENDPOINT = f"{TABLE}.{CONNECTOR}"

INITIAL_VERSIONS = 10
FOLLOW_ROUNDS = (3, 2, 4)


def _writer_storage_options(loc: DeltaTestLocation) -> dict | None:
    opts = loc.delta_storage_options()
    if not opts:
        return None
    if loc.uri.startswith("s3://"):
        opts.setdefault("aws_s3_allow_unsafe_rename", "true")
    return opts


def _delta_version(loc: DeltaTestLocation) -> int:
    from deltalake import DeltaTable

    dt = DeltaTable(loc.uri, storage_options=_writer_storage_options(loc))
    return dt.version()


def _append_version(loc: DeltaTestLocation, row_id: int) -> None:
    """Append one row, producing exactly one new Delta table version."""
    from deltalake import write_deltalake

    write_deltalake(
        loc.uri,
        pa.Table.from_pylist([{"id": row_id}]),
        mode="append",
        schema_mode="merge",
        storage_options=_writer_storage_options(loc),
    )


def _seed_delta_table(loc: DeltaTestLocation, num_versions: int) -> None:
    """Create a Delta table and append ``num_versions`` commits (one row each)."""
    from deltalake import write_deltalake

    write_deltalake(
        loc.uri,
        pa.Table.from_pylist([{"id": 0}]),
        mode="overwrite",
        storage_options=_writer_storage_options(loc),
    )
    for version in range(1, num_versions):
        _append_version(loc, version * 2)


def _connector_config(
    loc: DeltaTestLocation, *, mode: str, paused: bool = False
) -> str:
    config = dict(loc.connector_config)
    config.update(
        {
            "mode": mode,
            "transaction_mode": "catchup",
            "filter": "id % 2 = 0",
        }
    )
    if mode == "cdc":
        config.update(
            {
                "cdc_delete_filter": "__feldera_op = 'd'",
                "cdc_order_by": "__feldera_ts asc, lsn asc",
            }
        )
    connector = {
        "name": CONNECTOR,
        "transport": {
            "name": "delta_table_input",
            "config": config,
        },
    }
    if paused:
        connector["paused"] = True
    return json.dumps([connector])


def _build_sql(loc: DeltaTestLocation, *, mode: str, paused: bool = False) -> str:
    connectors = _connector_config(loc, mode=mode, paused=paused).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ("
        "id BIGINT NOT NULL"
        f") WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def _delta_metric(pipeline_name: str, metric_name: str) -> float:
    response = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=prometheus"))
    assert response.status_code == HTTPStatus.OK, response.text
    pattern = rf'^{re.escape(metric_name)}\{{[^}}]*endpoint="{re.escape(ENDPOINT)}"[^}}]*\}}\s+(\S+)'
    for line in response.text.splitlines():
        match = re.match(pattern, line)
        if match:
            return float(match.group(1))
    return -1.0


def _delta_counter(pipeline_name: str, metric_name: str) -> int:
    value = _delta_metric(pipeline_name, metric_name)
    return 0 if value < 0 else int(value)


def _completed_version(pipeline) -> int | None:
    status = pipeline.input_connector_stats(TABLE, CONNECTOR)
    if status.completed_frontier is None:
        return None
    metadata = status.completed_frontier.metadata
    if isinstance(metadata, dict):
        version = metadata.get("version")
        if isinstance(version, int):
            return version
    return None


def _wait_for_connector_paused(
    pipeline, *, paused: bool, timeout_s: float = 60.0
) -> None:
    wait_for_condition(
        f"connector {ENDPOINT} paused={paused}",
        lambda: pipeline.input_connector_stats(TABLE, CONNECTOR).paused is paused,
        timeout_s=timeout_s,
        poll_interval_s=0.2,
    )


def _wait_for_completed_version(
    pipeline, target: int, timeout_s: float = 120.0
) -> None:
    wait_for_condition(
        f"delta waterline version {target}",
        lambda: _completed_version(pipeline) == target,
        timeout_s=timeout_s,
        poll_interval_s=0.2,
    )


def _materialized_row_count(pipeline) -> int:
    rows = list(pipeline.query(f"SELECT COUNT(*) AS c FROM {TABLE}"))
    return int(rows[0]["c"])


_CDC_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("__feldera_op", pa.string()),
        pa.field("__feldera_ts", pa.timestamp("us")),
        pa.field("lsn", pa.int64()),
    ]
)


def _cdc_ts(ts_us: int) -> datetime:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc)


def _append_cdc_insert(
    loc: DeltaTestLocation,
    row_id: int,
    ts_us: int,
    *,
    mode: str = "append",
) -> None:
    from deltalake import write_deltalake

    # `lsn` mirrors `__feldera_ts` here; the catchup test never ties on ts.
    write_deltalake(
        loc.uri,
        pa.Table.from_pylist(
            [
                {
                    "id": row_id,
                    "__feldera_op": "i",
                    "__feldera_ts": _cdc_ts(ts_us),
                    "lsn": ts_us,
                }
            ],
            schema=_CDC_SCHEMA,
        ),
        mode=mode,
        schema_mode="merge",
        storage_options=_writer_storage_options(loc),
    )


def _seed_cdc_table(loc: DeltaTestLocation, num_versions: int) -> None:
    """Create a CDC Delta table and append ``num_versions`` insert commits."""
    for version in range(num_versions):
        _append_cdc_insert(
            loc,
            version * 2,
            (version + 1) * 1_000,
            mode="overwrite" if version == 0 else "append",
        )


def _run_catchup_rounds(
    pipeline,
    pipeline_name: str,
    loc: DeltaTestLocation,
    *,
    table_version: int,
    next_row_id: int,
    next_ts_us: int,
    cdc: bool,
) -> tuple[int, int, int]:
    """Pause, append, resume for each round; return final version, row id, and ts."""
    for round_idx, num_versions in enumerate(FOLLOW_ROUNDS):
        pipeline.pause_connector(TABLE, CONNECTOR)
        _wait_for_connector_paused(pipeline, paused=True)

        follow_at_round_start = _delta_counter(
            pipeline_name, "input_connector_delta_follow_transaction_starts"
        )
        version_before_burst = table_version

        for _ in range(num_versions):
            if cdc:
                _append_cdc_insert(loc, next_row_id, next_ts_us)
                next_ts_us += 1_000
            else:
                _append_version(loc, next_row_id)
            next_row_id += 2
            table_version = _delta_version(loc)

        assert table_version == version_before_burst + num_versions
        assert (
            _completed_version(pipeline) is None
            or _completed_version(pipeline) < table_version
        ), f"round {round_idx}: connector must not ingest commits written while paused"

        pipeline.resume_connector(TABLE, CONNECTOR)
        _wait_for_connector_paused(pipeline, paused=False)
        _wait_for_completed_version(pipeline, table_version)

        follow_at_round_end = _delta_counter(
            pipeline_name, "input_connector_delta_follow_transaction_starts"
        )
        assert follow_at_round_end - follow_at_round_start == 1, (
            f"round {round_idx}: catchup must batch {num_versions} Delta commits "
            "into one Feldera transaction"
        )
        assert (
            _delta_metric(pipeline_name, "input_connector_delta_catchup_target_version")
            < 0
        ), (
            f"round {round_idx}: catchup target metric must clear after the window closes"
        )

    return table_version, next_row_id, next_ts_us


@enterprise_only
def test_delta_input_catchup_snapshot_and_follow(pipeline_name):
    """
    Catchup mode batches snapshot ingest and each follow burst into one Feldera
    transaction when the connector is orchestrated with pause/resume.
    """
    loc = DeltaTestLocation.create(pipeline_name)
    try:
        _seed_delta_table(loc, INITIAL_VERSIONS)
        assert _delta_version(loc) == INITIAL_VERSIONS - 1

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc, mode="snapshot_and_follow"),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()

        pipeline.start()

        _wait_for_completed_version(pipeline, INITIAL_VERSIONS - 1)
        assert _materialized_row_count(pipeline) == INITIAL_VERSIONS
        assert (
            _delta_counter(
                pipeline_name, "input_connector_delta_snapshot_transaction_starts"
            )
            == 1
        ), "initial snapshot must run in a single Feldera transaction"
        assert (
            _delta_counter(
                pipeline_name, "input_connector_delta_follow_transaction_starts"
            )
            == 0
        ), "follow transactions must not start until new commits are ingested"

        table_version, next_row_id, _ = _run_catchup_rounds(
            pipeline,
            pipeline_name,
            loc,
            table_version=_delta_version(loc),
            next_row_id=INITIAL_VERSIONS * 2,
            next_ts_us=0,
            cdc=False,
        )

        expected_rows = INITIAL_VERSIONS + sum(FOLLOW_ROUNDS)
        assert _materialized_row_count(pipeline) == expected_rows
        assert _completed_version(pipeline) == table_version

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


@enterprise_only
def test_delta_input_catchup_cdc(pipeline_name):
    """
    CDC catchup skips pre-existing commits (no snapshot ingest), then batches
    each pause/resume follow burst into one Feldera transaction.
    """
    loc = DeltaTestLocation.create(pipeline_name)
    try:
        _seed_cdc_table(loc, INITIAL_VERSIONS)
        assert _delta_version(loc) == INITIAL_VERSIONS - 1

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc, mode="cdc"),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()

        pipeline.start()

        _wait_for_completed_version(pipeline, INITIAL_VERSIONS - 1)
        assert _materialized_row_count(pipeline) == 0, (
            "CDC mode must not ingest pre-existing commits as a snapshot"
        )
        assert (
            _delta_counter(
                pipeline_name, "input_connector_delta_snapshot_transaction_starts"
            )
            == 0
        ), "CDC mode must not start a snapshot transaction"
        assert (
            _delta_counter(
                pipeline_name, "input_connector_delta_follow_transaction_starts"
            )
            == 0
        ), "follow transactions must not start until new commits are ingested"

        table_version, next_row_id, _ = _run_catchup_rounds(
            pipeline,
            pipeline_name,
            loc,
            table_version=_delta_version(loc),
            next_row_id=INITIAL_VERSIONS * 2,
            next_ts_us=(INITIAL_VERSIONS + 1) * 1_000,
            cdc=True,
        )

        expected_rows = sum(FOLLOW_ROUNDS)
        assert _materialized_row_count(pipeline) == expected_rows
        assert _completed_version(pipeline) == table_version

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


_CDC_VAL_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("val", pa.int64()),
        pa.field("__feldera_op", pa.string()),
        pa.field("__feldera_ts", pa.timestamp("us")),
        pa.field("lsn", pa.int64()),
    ]
)


@enterprise_only
def test_delta_input_cdc_multi_key_order_by(pipeline_name):
    """A multi-key `"cdc_order_by": "__feldera_ts asc, lsn asc"` parses and
    honors the second key as a tiebreaker.

    The table has a primary key, so two inserts of the same key in one
    transaction collapse to a last-writer-wins upsert. Both carry the same
    `__feldera_ts`; only the `lsn asc` tiebreak makes the larger `lsn`
    the last writer. The losing row is written to the file first, so a
    single-key sort would let it win instead.
    """
    from deltalake import write_deltalake

    loc = DeltaTestLocation.create(pipeline_name)

    def append(rows: list[dict], *, mode: str = "append") -> None:
        write_deltalake(
            loc.uri,
            pa.Table.from_pylist(
                [
                    {
                        "id": r["id"],
                        "val": r["val"],
                        "__feldera_op": "i",
                        "__feldera_ts": _cdc_ts(r["ts_us"]),
                        "lsn": r["lsn"],
                    }
                    for r in rows
                ],
                schema=_CDC_VAL_SCHEMA,
            ),
            mode=mode,
            schema_mode="merge",
            storage_options=_writer_storage_options(loc),
        )

    try:
        # CDC mode skips pre-existing commits; seed (not ingested) and follow.
        append([{"id": 0, "val": 0, "ts_us": 1_000, "lsn": 0}], mode="overwrite")

        connectors = _connector_config(loc, mode="cdc").replace("'", "''")
        sql = (
            f"CREATE TABLE {TABLE} (id BIGINT NOT NULL PRIMARY KEY, val BIGINT) "
            f"WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
        )
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=1,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        _wait_for_completed_version(pipeline, _delta_version(loc))
        assert _materialized_row_count(pipeline) == 0

        # One transaction, two upserts of id 0 at the same `__feldera_ts`. The
        # winner (lsn 2, val 200) is written to the file first, so a sort that
        # ignored `lsn` would leave the loser (lsn 1) last and let val 100 win.
        append(
            [
                {"id": 0, "val": 200, "ts_us": 2_000, "lsn": 2},
                {"id": 0, "val": 100, "ts_us": 2_000, "lsn": 1},
            ]
        )
        _wait_for_completed_version(pipeline, _delta_version(loc))
        rows = list(pipeline.query(f"SELECT val FROM {TABLE} WHERE id = 0"))
        assert rows and rows[0]["val"] == 200, (
            f"lsn asc must make the lsn=2 upsert the last writer; got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
