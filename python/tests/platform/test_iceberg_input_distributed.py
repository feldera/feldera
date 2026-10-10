"""Iceberg input: a distributed connector divides the snapshot among the hosts.

Each host reads a different subset of the snapshot's data files.  Together, the
hosts must read each record exactly once.  The table has no primary key, so a
record that two hosts read would count twice.

(These tests pass with single-host also, where the one host reads everything.)
"""

from __future__ import annotations

import json

import pyarrow as pa

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT, enterprise_only
from tests.utils import IcebergTestLocation

TABLE = "t"
CONNECTOR = "iceberg_in"

# More appends than hosts, each with a file per partition, so that every host
# has files to read.
N_APPENDS = 2 * FELDERA_TEST_NUM_HOSTS + 2
ROWS_PER_APPEND = 50
N_ROWS = N_APPENDS * ROWS_PER_APPEND

_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("part", pa.string(), nullable=True),
    ]
)


def _seed(loc: IcebergTestLocation) -> None:
    """Creates a table partitioned by `part`, with `N_APPENDS` snapshots."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "part", StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=2, field_id=1000, transform=IdentityTransform(), name="part"
        )
    )
    loc.create_table(schema, partition_spec=spec)
    for append in range(N_APPENDS):
        first = append * ROWS_PER_APPEND
        loc.append(
            pa.Table.from_pylist(
                [
                    {"id": i, "part": f"p{i % 3}"}
                    for i in range(first, first + ROWS_PER_APPEND)
                ],
                schema=_ARROW_SCHEMA,
            )
        )


def _run(pipeline_name: str, extra_config: dict[str, object]) -> dict:
    loc = IcebergTestLocation.create(pipeline_name)
    try:
        _seed(loc)
        connectors = json.dumps(
            [
                {
                    "name": CONNECTOR,
                    "distributed": True,
                    "transport": {
                        "name": "iceberg_input",
                        "config": loc.connector_config(mode="snapshot", **extra_config),
                    },
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
                f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS n_distinct,"
                f" COUNT(DISTINCT part) AS n_parts, COUNT(part) AS n_with_part"
                f" FROM {TABLE}"
            )
        )
        pipeline.stop(force=True)
    finally:
        loc.remove_if_local()
    return counts


# Every record exactly once, with its partition value.
_EXPECTED = {"n": N_ROWS, "n_distinct": N_ROWS, "n_parts": 3, "n_with_part": N_ROWS}


@enterprise_only
def test_iceberg_input_distributed_snapshot(pipeline_name):
    """The hosts together read every record of the snapshot exactly once."""
    assert _run(pipeline_name, {}) == _EXPECTED


@enterprise_only
def test_iceberg_input_distributed_snapshot_transaction(pipeline_name):
    """The hosts read every record exactly once when they read the snapshot
    in one transaction."""
    assert _run(pipeline_name, {"transaction_mode": "snapshot"}) == _EXPECTED
