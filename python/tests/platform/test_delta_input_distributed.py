"""Delta input: a distributed connector divides the snapshot among the hosts.

Each host reads a different subset of the snapshot's data files.  Together, the
hosts must read each record exactly once.  The table has no primary key, so a record
that two hosts read would count twice.

(These tests pass with single-host also, where the one host reads everything.)
"""

from __future__ import annotations

import json

import pyarrow as pa

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation

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


def _seed(loc: DeltaTestLocation) -> None:
    """Writes `N_COMMITS` commits to a table partitioned by `part`: versions
    0 through `N_COMMITS - 1`."""
    from deltalake import write_deltalake

    storage_options = loc.writer_storage_options()
    for commit in range(N_COMMITS):
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
