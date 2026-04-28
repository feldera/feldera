"""
Delta Lake output connector: restart semantics.

Exercises the interaction between `delta_table_output` `truncate` mode
and pipeline suspend/resume.  Three scenarios:

* `test_clean_resume_preserves_table` — restarting an unchanged pipeline
  keeps the data written before the checkpoint.
* `test_modified_connector_re_truncates_on_resume` — changing the
  connector config on resume re-truncates the table (the modified
  connector is treated as a new incarnation).
* `test_modified_view_re_truncates_on_resume` — changing the view's
  schema on resume forces a rebuild from scratch.

The delta table backend toggles automatically via `DeltaTestLocation`:

* Local runs use a `file://` URI under `/tmp`.
* CI runs use the in-cluster MinIO endpoint, so the pipeline pod and the
  test runner reach the table over S3.
"""

import json

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    unique_pipeline_name,
)
from tests import TEST_CLIENT, enterprise_only, skip_on_arm64
from tests.utils import DeltaTestLocation


# ─── helpers ───────────────────────────────────────────────────────────


def _connector_config(loc: DeltaTestLocation, extra: dict | None = None) -> str:
    config = dict(loc.connector_config)
    if extra:
        config.update(extra)
    return json.dumps([{"transport": {"name": "delta_table_output", "config": config}}])


def _sql(
    loc: DeltaTestLocation,
    *,
    extra_connector_options: dict | None = None,
    extra_view_column: bool = False,
) -> str:
    v_body = (
        "SELECT id, 'v2' AS tag, id * 10 AS extra FROM t"
        if extra_view_column
        else "SELECT id, 'v1' AS tag FROM t"
    )
    return (
        "CREATE TABLE t (id INT) WITH ('materialized' = 'true');\n"
        "CREATE MATERIALIZED VIEW v WITH ("
        "'connectors' = '"
        + _connector_config(loc, extra_connector_options)
        + "') AS "
        + v_body
        + ";"
    )


def _build_pipeline(name: str, sql: str):
    return PipelineBuilder(
        TEST_CLIENT,
        name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            logging="debug",
        ),
    ).create_or_replace()


def _seed_50_rows_and_suspend(name: str, loc: DeltaTestLocation):
    """Start a fresh pipeline, write 50 rows, checkpoint, and stop."""
    pipeline = _build_pipeline(name, _sql(loc))
    pipeline.start()
    pipeline.input_json("t", [{"id": i} for i in range(50)], wait=True)
    pipeline.checkpoint(wait=True)
    pipeline.stop(force=False)
    return pipeline


# ─── tests ─────────────────────────────────────────────────────────────


@enterprise_only
@skip_on_arm64
def test_clean_resume_preserves_table():
    """Restarting an unchanged pipeline keeps the delta table contents."""
    name = unique_pipeline_name("delta_restart_clean")
    loc = DeltaTestLocation.create(name)
    try:
        pipeline = _seed_50_rows_and_suspend(name, loc)

        # Resume the SAME pipeline object — connector identity is preserved.
        pipeline.start()
        assert loc.row_count() == 50

        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert loc.row_count() == 80

        pipeline.stop(force=True)
        pipeline.clear_storage()
    finally:
        loc.cleanup()


@enterprise_only
@skip_on_arm64
def test_modified_connector_re_truncates_on_resume():
    """Changing the connector config on resume makes it a new incarnation that re-truncates."""
    name = unique_pipeline_name("delta_restart_conn_modified")
    loc = DeltaTestLocation.create(name)
    try:
        _seed_50_rows_and_suspend(name, loc)

        # Resume with a MODIFIED connector config (extra `checkpoint_interval` field).
        pipeline = _build_pipeline(
            name,
            _sql(loc, extra_connector_options={"checkpoint_interval": 60}),
        )
        pipeline.start()
        assert loc.row_count() == 0  # re-truncated

        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert loc.row_count() == 30

        pipeline.stop(force=True)
        pipeline.clear_storage()
    finally:
        loc.cleanup()


@enterprise_only
@skip_on_arm64
def test_modified_view_re_truncates_on_resume():
    """Changing the view's schema on resume forces a rebuild from scratch."""
    name = unique_pipeline_name("delta_restart_view_modified")
    loc = DeltaTestLocation.create(name)
    try:
        _seed_50_rows_and_suspend(name, loc)

        # Resume with a MODIFIED view (adds an `extra` column).
        pipeline = _build_pipeline(name, _sql(loc, extra_view_column=True))
        pipeline.start()
        assert loc.row_count() == 0  # rebuilt empty

        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert loc.row_count() == 30

        pipeline.stop(force=True)
        pipeline.clear_storage()
    finally:
        loc.cleanup()
