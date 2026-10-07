"""Delta input: a distributed connector divides the snapshot among the hosts.

Each host reads a different subset of the snapshot's data files, and only the
connector's home host follows the log after the snapshot.  Together, the hosts
must read each record exactly once.  The table has no primary key, so a record
that two hosts read would count twice.

(These tests pass with single-host also, where the one host reads everything.)
"""

from __future__ import annotations

import json

import pyarrow as pa

from feldera import PipelineBuilder
from feldera.enums import FaultToleranceModel
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, wait_for_condition

TABLE = "t"
CONNECTOR = "delta_in"

# More commits than hosts, each with a file per partition, so that every host
# has files to read.
N_COMMITS = 2 * FELDERA_TEST_NUM_HOSTS + 2
ROWS_PER_COMMIT = 50
N_ROWS = N_COMMITS * ROWS_PER_COMMIT

_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("part", pa.string()),
    ]
)


def _seed(loc: DeltaTestLocation, commits: range = range(N_COMMITS)) -> None:
    """Writes `commits` to a table partitioned by `part`, `ROWS_PER_COMMIT`
    rows each.  By default, writes versions 0 through `N_COMMITS - 1`."""
    from deltalake import write_deltalake

    storage_options = loc.writer_storage_options()
    for commit in commits:
        first = commit * ROWS_PER_COMMIT
        rows = pa.Table.from_pylist(
            [
                {"id": i, "part": f"p{i % 3}"}
                for i in range(first, first + ROWS_PER_COMMIT)
            ],
            schema=_SCHEMA,
        )
        write_deltalake(
            loc.uri,
            rows,
            mode="overwrite" if commit == 0 else "append",
            partition_by=["part"],
            storage_options=storage_options,
        )


def _run(pipeline_name: str, mode: str, extra_config: dict[str, object]) -> dict:
    loc = DeltaTestLocation.create(pipeline_name, mode=mode)
    try:
        _seed(loc)
        config = dict(loc.connector_config)
        config.update(extra_config)
        connectors = json.dumps(
            [
                {
                    "name": CONNECTOR,
                    "distributed": True,
                    "transport": {"name": "delta_table_input", "config": config},
                }
            ]
        ).replace("'", "''")
        sql = (
            f"CREATE TABLE {TABLE} (id BIGINT NOT NULL, part VARCHAR)"
            f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
        )
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()
        pipeline.start()
        # Query before stopping: an ad-hoc query needs a live pipeline.
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)
        [counts] = list(
            pipeline.query(
                f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS n_distinct FROM {TABLE}"
            )
        )
        pipeline.stop(force=True)
    finally:
        loc.cleanup()
    return counts


def test_delta_input_distributed_snapshot(pipeline_name):
    """The hosts together read every record of the snapshot exactly once."""
    counts = _run(pipeline_name, "snapshot", {})
    assert counts == {"n": N_ROWS, "n_distinct": N_ROWS}


def test_delta_input_distributed_snapshot_and_follow(pipeline_name):
    """The hosts read the snapshot at one version, in one transaction, and
    one host follows the commits after it."""
    counts = _run(
        pipeline_name,
        "snapshot_and_follow",
        {
            "version": N_COMMITS - 2,
            "end_version": N_COMMITS - 1,
            "transaction_mode": "snapshot",
        },
    )
    assert counts == {"n": N_ROWS, "n_distinct": N_ROWS}


def test_delta_input_distributed_snapshot_transaction(pipeline_name):
    """The hosts read every record exactly once when they read the snapshot
    in one transaction."""
    counts = _run(pipeline_name, "snapshot", {"transaction_mode": "snapshot"})
    assert counts == {"n": N_ROWS, "n_distinct": N_ROWS}


def test_delta_input_distributed_resume(pipeline_name):
    """After a resume from a checkpoint, the host that follows the table reads
    the commits made while the pipeline was suspended, once, and no host reads
    its part of the snapshot again."""
    n_more_commits = 3
    loc = DeltaTestLocation.create(pipeline_name, mode="snapshot_and_follow")
    try:
        _seed(loc)
        config = dict(loc.connector_config)
        config["transaction_mode"] = "snapshot"
        connectors = json.dumps(
            [
                {
                    "name": CONNECTOR,
                    "distributed": True,
                    "transport": {"name": "delta_table_input", "config": config},
                }
            ]
        ).replace("'", "''")
        sql = (
            f"CREATE TABLE {TABLE} (id BIGINT NOT NULL, part VARCHAR)"
            f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
        )
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
            ),
        ).create_or_replace()

        def counts():
            [counts] = list(
                pipeline.query(
                    f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS n_distinct"
                    f" FROM {TABLE}"
                )
            )
            return counts

        def wait_for_rows(n):
            wait_for_condition(
                f"{n} rows ingested",
                lambda: counts() == {"n": n, "n_distinct": n},
                timeout_s=300.0,
                poll_interval_s=1.0,
            )

        pipeline.start()
        try:
            wait_for_rows(N_ROWS)
            pipeline.checkpoint(wait=True)
            pipeline.stop(force=False)

            _seed(loc, range(N_COMMITS, N_COMMITS + n_more_commits))
            pipeline.start()
            n_total = (N_COMMITS + n_more_commits) * ROWS_PER_COMMIT
            wait_for_rows(n_total)

            # Nothing arrives twice, even after time to do so.
            assert counts() == {"n": n_total, "n_distinct": n_total}
        finally:
            pipeline.stop(force=True)
    finally:
        loc.cleanup()
