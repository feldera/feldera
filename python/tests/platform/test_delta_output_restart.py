"""
Delta Lake output connector: restart semantics.

Exercises the interaction between `delta_table_output` `truncate` mode and
pipeline suspend/resume.  Covers three scenarios:

1. Clean suspend/resume preserves data written before the checkpoint.
2. Modifying the connector config between suspend and resume re-truncates
   the table (the modified connector is treated as new).
3. Modifying the upstream view's schema between suspend and resume also
   re-truncates the table.

The tests require enterprise features (checkpoint / suspend) and a local
filesystem shared with the Feldera server.  Delta table state is inferred
by replaying `_delta_log/*.json` to find the currently-active `add` actions
and summing the `numRecords` field from each action's `stats`.  We do not
scan parquet files directly because `SaveMode::Overwrite` leaves orphaned
parquet files on disk that a naive recursive scan would double-count, and
we want the test to rely only on what the delta log reports.
"""

import json
import shutil
import tempfile
from pathlib import Path
from typing import List

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    unique_pipeline_name,
)
from tests import TEST_CLIENT, enterprise_only


# ─── delta log reader ──────────────────────────────────────────────────


def _active_add_actions(table_dir: Path) -> List[dict]:
    """Replay the delta log and return the `add` actions currently in effect."""
    log_dir = table_dir / "_delta_log"
    if not log_dir.exists():
        return []
    active: dict[str, dict] = {}
    for entry in sorted(log_dir.glob("*.json")):
        for line in entry.read_text().splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if "add" in obj:
                active[obj["add"]["path"]] = obj["add"]
            elif "remove" in obj:
                active.pop(obj["remove"]["path"], None)
    return list(active.values())


def delta_row_count(table_dir: Path) -> int:
    """Sum of `numRecords` across all active files, per the delta log stats."""
    total = 0
    for add in _active_add_actions(table_dir):
        stats = add.get("stats")
        if not stats:
            raise RuntimeError(f"delta add action missing 'stats' field: {add}")
        total += json.loads(stats)["numRecords"]
    return total


# ─── pipeline helpers ──────────────────────────────────────────────────


def _connector_config(uri: str, extra: dict | None = None) -> str:
    config = {"uri": uri, "mode": "truncate"}
    if extra:
        config.update(extra)
    return json.dumps(
        [
            {
                "transport": {
                    "name": "delta_table_output",
                    "config": config,
                }
            }
        ]
    )


def _sql(
    *,
    uri: str,
    extra_connector_options: dict | None = None,
    extra_view_column: bool = False,
) -> str:
    v_body = "SELECT id, 'v1' AS tag FROM t"
    if extra_view_column:
        v_body = "SELECT id, 'v2' AS tag, id * 10 AS extra FROM t"
    return (
        "CREATE TABLE t (id INT) WITH ('materialized' = 'true');\n"
        "CREATE MATERIALIZED VIEW v WITH ("
        "'connectors' = '"
        + _connector_config(uri, extra_connector_options)
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


# ─── tests ─────────────────────────────────────────────────────────────


@enterprise_only
def test_truncate_preserved_across_clean_suspend_resume():
    """Unchanged pipeline: suspend+resume keeps data in the delta table."""
    name = unique_pipeline_name("delta_restart_clean")
    table_dir = Path(tempfile.mkdtemp(prefix="feldera_delta_test_"))
    uri = f"file://{table_dir}"

    try:
        pipeline = _build_pipeline(name, _sql(uri=uri))
        pipeline.start()
        pipeline.input_json("t", [{"id": i} for i in range(50)], wait=True)
        assert delta_row_count(table_dir) == 50

        pipeline.checkpoint(wait=True)
        pipeline.stop(force=False)

        # Resume with the SAME config — no truncation
        pipeline.start()
        assert delta_row_count(table_dir) == 50, (
            "clean restart should preserve delta table contents"
        )

        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert delta_row_count(table_dir) == 80
    finally:
        try:
            pipeline.stop(force=True)
            pipeline.clear_storage()
        except Exception:
            pass
        shutil.rmtree(table_dir, ignore_errors=True)


@enterprise_only
def test_truncate_reapplied_when_connector_modified():
    """Changing the connector config between suspend and resume must re-truncate."""
    name = unique_pipeline_name("delta_restart_conn_changed")
    table_dir = Path(tempfile.mkdtemp(prefix="feldera_delta_test_"))
    uri = f"file://{table_dir}"

    try:
        pipeline = _build_pipeline(name, _sql(uri=uri))
        pipeline.start()
        pipeline.input_json("t", [{"id": i} for i in range(50)], wait=True)
        assert delta_row_count(table_dir) == 50

        pipeline.checkpoint(wait=True)
        pipeline.stop(force=False)

        # Resume with a MODIFIED connector config
        # (extra `checkpoint_interval` field).
        pipeline = _build_pipeline(
            name,
            _sql(uri=uri, extra_connector_options={"checkpoint_interval": 60}),
        )
        pipeline.start()

        # The new connector incarnation must truncate the table and must not
        # inherit the stale 50-row snapshot that would survive without the
        # output_statistics filter.  No new data has been fed since resume, so
        # the post-restart count is exactly zero.
        post_resume = delta_row_count(table_dir)
        assert post_resume == 0, (
            f"modified connector should truncate on resume with no re-emission; "
            f"expected 0 rows, got {post_resume}"
        )
        assert (table_dir / "_delta_log").exists()

        # Feed new data and confirm the new incarnation accepts it with an
        # exact row-count delta — proving the connector is live, not stuck.
        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert delta_row_count(table_dir) == 30
    finally:
        try:
            pipeline.stop(force=True)
            pipeline.clear_storage()
        except Exception:
            pass
        shutil.rmtree(table_dir, ignore_errors=True)


@enterprise_only
def test_truncate_reapplied_when_view_modified():
    """Changing the view's schema between suspend and resume must re-truncate."""
    name = unique_pipeline_name("delta_restart_view_changed")
    table_dir = Path(tempfile.mkdtemp(prefix="feldera_delta_test_"))
    uri = f"file://{table_dir}"

    try:
        pipeline = _build_pipeline(name, _sql(uri=uri))
        pipeline.start()
        pipeline.input_json("t", [{"id": i} for i in range(50)], wait=True)
        assert delta_row_count(table_dir) == 50

        pipeline.checkpoint(wait=True)
        pipeline.stop(force=False)

        # Resume with a MODIFIED view (adds a column), connector config unchanged.
        pipeline = _build_pipeline(name, _sql(uri=uri, extra_view_column=True))
        pipeline.start()
        # A schema change forces the connector to rebuild the table from
        # scratch; the old 50-row snapshot must not survive.  With no new
        # upstream input, the post-resume row count is exactly zero.
        post_resume = delta_row_count(table_dir)
        assert post_resume == 0, (
            f"schema change should reset row count; expected 0, got {post_resume}"
        )

        # Confirm the `extra` column is present in the most recent `metaData`
        # action of the delta log.
        log_dir = table_dir / "_delta_log"
        latest_meta = None
        for entry in sorted(log_dir.glob("*.json")):
            for line in entry.read_text().splitlines():
                if not line.strip():
                    continue
                obj = json.loads(line)
                if "metaData" in obj:
                    latest_meta = obj["metaData"]
        assert latest_meta is not None, "no metaData action found in delta log"
        schema = json.loads(latest_meta["schemaString"])
        columns = [f["name"] for f in schema.get("fields", [])]
        assert "extra" in columns, (
            f"delta log schema after view change missing 'extra' column: {columns}"
        )

        # Feed new data and verify it lands in the rebuilt table with the new
        # schema — exact row count, no leftover or duplicated rows.
        pipeline.input_json("t", [{"id": i} for i in range(50, 80)], wait=True)
        assert delta_row_count(table_dir) == 30
    finally:
        try:
            pipeline.stop(force=True)
            pipeline.clear_storage()
        except Exception:
            pass
        shutil.rmtree(table_dir, ignore_errors=True)
