"""Delta input: a ``filter`` may name a column the SQL table does not declare.

Follow mode used to project the frame down to the SQL columns before parsing the
filter, failing with "No field named ..." while the snapshot path accepted it
(https://github.com/feldera/feldera/issues/6908). The run is
``snapshot_and_follow`` over a two-version table, so it covers both paths.
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

ROWS_PER_VERSION = 10
# Even ids are in region 'us' and pass the filter; odd ids are in 'eu'.
KEPT_IDS = [i for i in range(2 * ROWS_PER_VERSION) if i % 2 == 0]

# `region` exists only in the Delta table; the filter below is its only reader.
_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("region", pa.string()),
    ]
)


def _rows(start: int, count: int) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"id": i, "region": "us" if i % 2 == 0 else "eu"}
            for i in range(start, start + count)
        ],
        schema=_SCHEMA,
    )


def _seed_two_versions(loc: DeltaTestLocation) -> None:
    """v0: the first batch of rows; v1: the second, as a follow-mode commit."""
    from deltalake import write_deltalake

    storage_options = loc.writer_storage_options()
    write_deltalake(
        loc.uri,
        _rows(0, ROWS_PER_VERSION),
        mode="overwrite",
        storage_options=storage_options,
    )
    write_deltalake(
        loc.uri,
        _rows(ROWS_PER_VERSION, ROWS_PER_VERSION),
        mode="append",
        storage_options=storage_options,
    )


def _build_sql(loc: DeltaTestLocation, *, filter_expr: str) -> str:
    """Declare only `id`: `region` is visible to the filter alone."""
    config = dict(loc.connector_config)
    config.update({"filter": filter_expr, "version": 0, "end_version": 1})
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {"name": "delta_table_input", "config": config},
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} (id BIGINT NOT NULL)"
        f" WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def test_delta_input_filter_over_undeclared_column(pipeline_name):
    filter_expr = "region = 'us'"
    loc = DeltaTestLocation.create(pipeline_name, mode="snapshot_and_follow")
    try:
        _seed_two_versions(loc)
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc, filter_expr=filter_expr),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()
        pipeline.start()
        # Query before stopping: an ad-hoc query needs a live pipeline.
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        ids = [
            int(row["id"])
            for row in pipeline.query(f"SELECT id FROM {TABLE} ORDER BY id")
        ]
        pipeline.stop(force=True)
    finally:
        loc.cleanup()

    # An error in either phase fails the pipeline before it completes.
    assert ids == KEPT_IDS, (
        f"filter '{filter_expr}' must keep exactly the 'us' rows across the "
        "snapshot and the follow commit"
    )
