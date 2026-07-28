"""Follow a column-mapped table with a nested struct column.

With ``delta.columnMapping.mode = 'name'`` the nested ``after`` struct's children
are stored on disk under physical names (``col-<uuid>``). The follow path reads
each commit as raw Parquet and must resolve those names at every level. This
guards the regression where only top-level columns were renamed, so nested
children stayed physical and the read dropped the rows (see
``fixtures/column_mapping_nested.py``).
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.platform.fixtures.column_mapping_nested import EXPECTED_ROWS
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture

TABLE = "t"
CONNECTOR = "delta_in"
# Bump to invalidate cached copies when the fixture definition changes.
FIXTURE_VERSION = "v1"

_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "column_mapping_nested.py"


def _build_sql(loc: DeltaTestLocation) -> str:
    config = dict(loc.connector_config)
    # Replay v1 + v2 (v0 is the empty CREATE) through the follow path.
    config.update({"version": 0, "end_version": 2})
    connectors = json.dumps(
        [
            {
                "name": CONNECTOR,
                "transport": {"name": "delta_table_input", "config": config},
            }
        ]
    ).replace("'", "''")
    return (
        f"CREATE TABLE {TABLE} ("
        "after ROW(transaction__id VARCHAR, transaction__amount VARCHAR),"
        "op VARCHAR"
        f") WITH ('materialized' = 'true', 'connectors' = '{connectors}');"
    )


def test_delta_input_column_mapping_nested_follow(pipeline_name):
    """Follow a nested column-mapped (``mode = 'name'``) table: the nested struct
    children resolve to their logical names instead of being dropped."""
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="snapshot_and_follow",
        stable_subpath=f"column_mapping_nested_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER)

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=_build_sql(loc),
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        pipeline.start()
        pipeline.wait_for_completion(force_stop=False, timeout_s=600)

        # Project a nested child to exercise resolution inside the struct.
        rows = sorted(
            (
                {"id": r["id"], "amount": r["amount"], "op": r["op"]}
                for r in pipeline.query(
                    "SELECT after.transaction__id AS id,"
                    f" after.transaction__amount AS amount, op FROM {TABLE}"
                )
            ),
            key=lambda r: r["id"],
        )
        expected = sorted(
            (
                {
                    "id": r["after"]["transaction__id"],
                    "amount": r["after"]["transaction__amount"],
                    "op": r["op"],
                }
                for r in EXPECTED_ROWS
            ),
            key=lambda r: r["id"],
        )
        assert rows == expected, (
            "follow must resolve the nested column-mapped struct children to "
            f"their logical names; got {rows}"
        )

        pipeline.stop(force=True)
    finally:
        loc.cleanup()
