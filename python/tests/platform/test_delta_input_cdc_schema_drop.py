"""A CDC `DROP COLUMN` must not silently repoint the compiled delete filter.

`cdc_delete_filter` becomes a DataFusion `PhysicalExpr`, which binds each column
by index and ignores its name. Dropping a column ahead of the delete marker
shifts the indices under it: with the marker's old slot occupied by another
string column, a stale expression would evaluate that column instead and land
every delete as an insert, with no error anywhere. The connector compiles the
expression against each transaction's own frame, so the delete still applies.

DROP COLUMN needs column mapping, so PySpark seeds the fixture; the builder
lives in `fixtures/cdc_delete_filter_drop.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS

from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, ensure_delta_spark_fixture

from .helper import api_url, gen_pipeline_name, get

TABLE = "t"
CONNECTOR = "delta_in"
# Bump to invalidate cached MinIO copies when the fixture definition changes.
FIXTURE_VERSION = "v1"

_FIXTURE_BUILDER = Path(__file__).parent / "fixtures" / "cdc_delete_filter_drop.py"

# v0 is the already-consumed empty baseline, so the replay covers v1..v3: the
# two inserts, the DROP COLUMN, and the delete marker.
_CONFIG = {
    "version": 0,
    "end_version": 3,
    "cdc_delete_filter": "__feldera_op = 'd'",
    "cdc_order_by": "__feldera_ts asc",
}


@gen_pipeline_name
def test_delta_input_cdc_delete_filter_after_drop(pipeline_name):
    loc = DeltaTestLocation.create(
        pipeline_name,
        mode="cdc",
        stable_subpath=f"cdc_delete_filter_drop_{FIXTURE_VERSION}",
    )
    try:
        ensure_delta_spark_fixture(loc, _FIXTURE_BUILDER)

        config = dict(loc.connector_config)
        config.update(_CONFIG)
        connectors = json.dumps(
            [
                {
                    "name": CONNECTOR,
                    "transport": {"name": "delta_table_input", "config": config},
                }
            ]
        ).replace("'", "''")
        sql = (
            f"CREATE TABLE {TABLE} (id BIGINT NOT NULL, b BOOLEAN, s VARCHAR)"
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

        # The replay is bounded by end_version, so it always reaches
        # end-of-input.
        pipeline.wait_for_completion(force_stop=False, timeout_s=300)

        # The v3 marker deletes id 1. A stale expression would read `s`
        # ("keep", never 'd'), turn the delete into an insert, and leave id 1
        # in the table twice.
        rows = sorted(
            (
                {"id": r["id"], "s": r["s"]}
                for r in pipeline.query(f"SELECT * FROM {TABLE}")
            ),
            key=lambda r: r["id"],
        )
        assert rows == [{"id": 2, "s": "keep"}], (
            f"expected the delete of id 1 to apply after the drop; got {rows}"
        )

        # `Pipeline.stats` omits the error payloads; only this selector returns
        # them, so read the messages from the REST endpoint directly.
        stats = get(
            api_url(f"/pipelines/{pipeline_name}/stats?include_connector_errors=true")
        ).json()
        messages = [
            err.get("message", "")
            for entry in stats.get("inputs", [])
            for err in (entry.get("parse_errors") or [])
        ]
        assert not messages, messages

        pipeline.stop(force=True)
    finally:
        loc.cleanup()
