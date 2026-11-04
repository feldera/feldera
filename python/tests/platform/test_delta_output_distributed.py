"""Delta output: a distributed connector writes from every host.

Each host writes the part of the view that its workers hold, and the part on
the connector's assigned host commits every part's files in one Delta commit
per transaction.  The table must hold each row of the view exactly once.

(These tests pass with single-host also, where the one host writes everything.)
"""

from __future__ import annotations

import json
from collections import Counter

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT, enterprise_only
from tests.utils import DeltaTestLocation, wait_for_condition

VIEW = "v"
CONNECTOR = "delta_out"

# Enough rows that every worker on every host has some.
ROWS_PER_BATCH = 200


def _build(
    pipeline_name: str, loc: DeltaTestLocation, endpoint_options: dict | None = None
):
    """Builds a pipeline whose view has a distributed Delta output connector,
    with `endpoint_options` added to the connector's options."""
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "distributed": True,
                "transport": {
                    "name": "delta_table_output",
                    "config": loc.connector_config,
                },
                **(endpoint_options or {}),
            }
        ]
    ).replace("'", "''")
    sql = (
        "CREATE TABLE t (id BIGINT NOT NULL) WITH ('materialized' = 'true');\n"
        f"CREATE MATERIALIZED VIEW {VIEW} WITH ('connectors' = '{connectors}')"
        " AS SELECT id, id * 10 AS tens FROM t;"
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


def _insert(pipeline, batch: int) -> None:
    first = batch * ROWS_PER_BATCH
    pipeline.input_json(
        "t", [{"id": i} for i in range(first, first + ROWS_PER_BATCH)], wait=True
    )


def _wait_for_ids(loc: DeltaTestLocation, n: int) -> Counter:
    """Waits until the table holds `n` rows, then returns how many times it
    holds each id."""
    wait_for_condition(
        f"{n} rows in the Delta table",
        lambda: len(loc.read_rows()) >= n,
        timeout_s=300.0,
        poll_interval_s=1.0,
    )
    rows = loc.read_rows()
    assert all(row["tens"] == row["id"] * 10 for row in rows)
    return Counter(row["id"] for row in rows)


def _files_per_commit(loc: DeltaTestLocation) -> list[int]:
    """The number of data files that each commit of the log adds."""
    counts = []
    for log_path in loc.log_json_paths():
        actions = [json.loads(line) for line in loc._read_text(log_path).splitlines()]
        counts.append(sum(1 for a in actions if a.get("add") is not None))
    return counts


def test_delta_output_distributed(pipeline_name):
    """Every host writes its part of each transaction, and the table holds
    each row exactly once."""
    n_batches = 3
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        pipeline = _build(pipeline_name, loc)
        pipeline.start()
        try:
            for batch in range(n_batches):
                _insert(pipeline, batch)
            n = n_batches * ROWS_PER_BATCH
            assert _wait_for_ids(loc, n) == Counter(range(n))

            # One commit holds the files of all the hosts that had output.
            assert max(_files_per_commit(loc)) >= FELDERA_TEST_NUM_HOSTS

            # The status of the connector combines the parts on every host.
            metrics = pipeline.output_connector_stats(VIEW, CONNECTOR).metrics
            assert metrics.transmitted_records == n
        finally:
            pipeline.stop(force=True)
    finally:
        loc.cleanup()


def test_delta_output_distributed_snapshot(pipeline_name):
    """A connector that starts after the view has rows writes the snapshot
    from every host, then the changes after it, each row exactly once."""
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        pipeline = _build(
            pipeline_name,
            loc,
            {"send_snapshot": True, "paused": True},
        )
        pipeline.start()
        try:
            _insert(pipeline, 0)
            pipeline.start_output_connector(VIEW, CONNECTOR)
            assert _wait_for_ids(loc, ROWS_PER_BATCH) == Counter(range(ROWS_PER_BATCH))

            _insert(pipeline, 1)
            n = 2 * ROWS_PER_BATCH
            assert _wait_for_ids(loc, n) == Counter(range(n))
        finally:
            pipeline.stop(force=True)
    finally:
        loc.cleanup()


# Resuming needs a checkpoint, and so fault tolerance.
@enterprise_only
def test_delta_output_distributed_resume(pipeline_name):
    """After a resume from a checkpoint, the connector writes only the changes
    since the checkpoint: no host writes its part of the view again."""
    loc = DeltaTestLocation.create(pipeline_name, mode="append")
    try:
        pipeline = _build(pipeline_name, loc)
        pipeline.start()
        try:
            _insert(pipeline, 0)
            assert _wait_for_ids(loc, ROWS_PER_BATCH) == Counter(range(ROWS_PER_BATCH))
            pipeline.checkpoint(wait=True)
            pipeline.stop(force=False)

            pipeline.start()
            _insert(pipeline, 1)
            n = 2 * ROWS_PER_BATCH
            assert _wait_for_ids(loc, n) == Counter(range(n))
        finally:
            pipeline.stop(force=True)
    finally:
        loc.cleanup()
