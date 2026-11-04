"""Snapshots of a materialized view in a multihost pipeline.

Each host holds part of the view.  The host that runs a connector builds the
connector's snapshot from the parts of every host, which it fetches from the
other hosts over HTTP(S).  The snapshot must hold each row of the view exactly
once.

(These tests pass with single-host also, where the one host holds the view.)
"""

from __future__ import annotations

import json
from collections import Counter

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.runtime.test_output_snapshot import (
    collect_output_chunks,
    normalize_egress_rows,
)
from tests.utils import DeltaTestLocation, wait_for_condition

# Enough rows that every worker on every host has some.
N_ROWS = 200


def _build(pipeline_name: str, connectors: list[dict] | None = None):
    """Builds a pipeline with materialized view `v` over table `t`, with
    `connectors` on `v`."""
    with_connectors = (
        " WITH ('connectors' = '{}')".format(json.dumps(connectors).replace("'", "''"))
        if connectors
        else ""
    )
    sql = (
        "CREATE TABLE t (id BIGINT NOT NULL) WITH ('materialized' = 'true');\n"
        f"CREATE MATERIALIZED VIEW v{with_connectors} AS SELECT id FROM t;"
    )
    return PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
        ),
    ).create_or_replace()


def test_multihost_egress_snapshot(pipeline_name):
    """An `/egress` stream with `send_snapshot` starts with every row of the
    view, from every host, then carries the view's changes."""
    pipeline = _build(pipeline_name)
    pipeline.start()
    try:
        pipeline.input_json("t", [{"id": i} for i in range(N_ROWS)], wait=True)

        response = TEST_CLIENT.http.post(
            path=f"/pipelines/{pipeline_name}/egress/v",
            params={
                "format": "json",
                "array": "false",
                "backpressure": "true",
                "send_snapshot": "true",
            },
            stream=True,
        )
        try:
            stream = (json.loads(line) for line in response.iter_lines() if line)
            chunks, rows = collect_output_chunks(stream, N_ROWS, timeout_s=120.0)
            assert all(chunk.get("snapshot") is True for chunk in chunks)
            ids = Counter(row["id"] for row in normalize_egress_rows(rows))
            assert ids == Counter(range(N_ROWS))

            pipeline.input_json("t", [{"id": N_ROWS}], wait=True)
            chunks, rows = collect_output_chunks(stream, 1)
            assert all(chunk.get("snapshot") is False for chunk in chunks)
            assert normalize_egress_rows(rows) == [{"id": N_ROWS}]
        finally:
            response.close()
    finally:
        pipeline.stop(force=True)


def test_multihost_delta_output_snapshot(pipeline_name):
    """A Delta output that starts after the view has rows writes every row of
    the view, from every host, exactly once."""
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        pipeline = _build(
            pipeline_name,
            [
                {
                    "name": "delta_out",
                    "send_snapshot": True,
                    "paused": True,
                    "transport": {
                        "name": "delta_table_output",
                        "config": loc.connector_config,
                    },
                }
            ],
        )
        pipeline.start()
        try:
            pipeline.input_json("t", [{"id": i} for i in range(N_ROWS)], wait=True)
            pipeline.start_output_connector("v", "delta_out")
            wait_for_condition(
                f"{N_ROWS} rows in the Delta table",
                lambda: len(loc.read_rows()) >= N_ROWS,
                timeout_s=300.0,
                poll_interval_s=1.0,
            )
            ids = Counter(row["id"] for row in loc.read_rows())
            assert ids == Counter(range(N_ROWS))
        finally:
            pipeline.stop(force=True)
    finally:
        loc.cleanup()
