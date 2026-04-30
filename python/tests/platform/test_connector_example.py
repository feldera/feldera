"""
End-to-end integration test for the `hello-lines` reference connector
(`crates/connector-example`).

The test exercises the full external-connector path:
  connectors.toml → describer build → manifest → SQL compile → pipeline run → rows

Two scenarios are covered:

1. **Happy path**: five-line input file, `interval_ms=0`, assert all five rows
   appear in the output view.

2. **Checkpoint / restore**: same file; checkpoint after three rows, stop and
   restart the pipeline, assert only the remaining two rows are emitted (not a
   full re-read from the beginning).

Prerequisites
-------------
* A running pipeline-manager instance accessible at `BASE_URL`.
* Cargo and the Feldera source tree available to the server so the describer
  build can resolve the path dependency.
* The `FELDERA_CONNECTOR_EXAMPLE_PATH` environment variable may optionally
  override the connector's crate directory; it defaults to the path relative
  to this file: ``../../../../crates/connector-example``.
"""

from __future__ import annotations

import json
import os
import time
import tempfile
from http import HTTPStatus
from pathlib import Path

import pytest

from tests import TEST_CLIENT, enterprise_only
from .helper import (
    api_url,
    create_pipeline,
    start_pipeline,
    stop_pipeline,
    adhoc_query_json,
    wait_for_condition,
    gen_pipeline_name,
    get,
    post_no_body,
    http_request,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_CONNECTOR_PATH = (
    Path(__file__).parent / ".." / ".." / ".." / "crates" / "connector-example"
).resolve()

CONNECTOR_EXAMPLE_PATH = Path(
    os.environ.get("FELDERA_CONNECTOR_EXAMPLE_PATH", str(_DEFAULT_CONNECTOR_PATH))
).resolve()

# Five input lines used across both tests.
INPUT_LINES = ["alpha", "beta", "gamma", "delta", "epsilon"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _put_connectors_toml(content: str) -> None:
    """Upload a new connectors.toml blob and assert 200/202 acceptance."""
    resp = http_request(
        "PUT",
        api_url("/connectors/connectors.toml"),
        data=content.encode(),
        headers={"Content-Type": "text/plain"},
    )
    assert resp.status_code in (HTTPStatus.OK, HTTPStatus.ACCEPTED), (
        f"PUT connectors.toml failed: {resp.status_code} {resp.text}"
    )


def _wait_for_describer_ready(timeout_s: float = 300.0) -> None:
    """Poll GET /v0/connectors/status until state == 'ready'."""

    def _is_ready() -> bool:
        r = get(api_url("/connectors/status"))
        if r.status_code != HTTPStatus.OK:
            return False
        return r.json().get("state") == "ready"

    wait_for_condition(
        "connector describer reaches 'ready' state",
        _is_ready,
        timeout_s=timeout_s,
        poll_interval_s=2.0,
    )


def _row_count(pipeline_name: str, view: str) -> int:
    rows = adhoc_query_json(
        pipeline_name,
        f"SELECT COUNT(*) AS c FROM {view}",
    )
    if not rows:
        return 0
    return rows[0].get("c", 0)


# ---------------------------------------------------------------------------
# Test fixture: upload connectors.toml once per module and wait for describer
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def connectors_toml_ready(request):
    """
    Upload a connectors.toml that adds the hello-lines example connector as a
    path dependency, then block until the describer build succeeds.

    Uses module scope so the (potentially slow) Cargo build runs only once per
    test session.
    """
    content = (
        f'connector-example = {{ path = "{CONNECTOR_EXAMPLE_PATH}" }}\n'
    )
    _put_connectors_toml(content)
    _wait_for_describer_ready(timeout_s=600.0)
    yield
    # Restore an empty connectors.toml after the module finishes so other tests
    # are not affected.
    _put_connectors_toml("")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@gen_pipeline_name
def test_hello_lines_five_rows(pipeline_name):
    """
    Full plugin path:
    connectors.toml → describer → manifest → SQL compile → pipeline run → 5 rows.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        for line in INPUT_LINES:
            f.write(line + "\n")
        input_path = f.name

    try:
        sql = f"""
        CREATE TABLE input_lines (line TEXT)
            WITH ('connectors' = '[{{
                "transport": {{
                    "name": "hello_lines",
                    "config": {{"path": "{input_path}", "interval_ms": 0}}
                }}
            }}]');
        CREATE VIEW output_lines AS SELECT line FROM input_lines;
        """
        create_pipeline(pipeline_name, sql)
        start_pipeline(pipeline_name)

        wait_for_condition(
            f"pipeline {pipeline_name!r} emits 5 rows",
            lambda: _row_count(pipeline_name, "output_lines") == 5,
            timeout_s=60.0,
            poll_interval_s=0.5,
        )

        rows = adhoc_query_json(
            pipeline_name,
            "SELECT line FROM output_lines ORDER BY line",
        )
        assert [r["line"] for r in rows] == sorted(INPUT_LINES), (
            f"Unexpected rows: {rows}"
        )
    finally:
        stop_pipeline(pipeline_name)
        os.unlink(input_path)


@enterprise_only
@gen_pipeline_name
def test_hello_lines_checkpoint_no_duplicate_rows(pipeline_name):
    """
    ExactlyOnce seek: after all rows are processed and a checkpoint is taken,
    restarting the pipeline must NOT re-emit rows already seen.

    Strategy: ingest all 5 lines, checkpoint, stop, restart, wait a moment,
    then assert the row count is still 5 (not 10).  If the connector ignores
    the seek payload and re-reads from position 0, the count would double.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        for line in INPUT_LINES:
            f.write(line + "\n")
        input_path = f.name

    try:
        sql = f"""
        CREATE TABLE input_lines (line TEXT)
            WITH ('connectors' = '[{{
                "transport": {{
                    "name": "hello_lines",
                    "config": {{"path": "{input_path}", "interval_ms": 0}}
                }}
            }}]');
        CREATE MATERIALIZED VIEW output_lines AS SELECT line FROM input_lines;
        """
        create_pipeline(pipeline_name, sql)
        start_pipeline(pipeline_name)

        # Wait for all 5 rows.
        wait_for_condition(
            f"pipeline {pipeline_name!r} emits all 5 rows",
            lambda: _row_count(pipeline_name, "output_lines") == 5,
            timeout_s=60.0,
            poll_interval_s=0.5,
        )

        # Checkpoint.
        resp = post_no_body(api_url(f"/pipelines/{pipeline_name}/checkpoint"))
        assert resp.status_code == HTTPStatus.OK, (
            f"Checkpoint POST failed: {resp.status_code} {resp.text}"
        )
        seq = resp.json().get("checkpoint_sequence_number")

        wait_for_condition(
            "checkpoint completes",
            lambda: get(api_url(f"/pipelines/{pipeline_name}/checkpoint_status"))
            .json()
            .get("success")
            == seq,
            timeout_s=30.0,
            poll_interval_s=0.2,
        )

        # Force-stop and restart.  The pipeline-manager passes the checkpoint's
        # resume_info (byte offset = EOF) to HelloLinesEndpoint::open(), so the
        # connector begins at EOF and emits nothing more.
        stop_pipeline(pipeline_name, force=True)
        start_pipeline(pipeline_name)

        # Wait for the pipeline to reach a stable running state, then verify
        # that the row count has not increased beyond 5.
        time.sleep(2.0)
        count = _row_count(pipeline_name, "output_lines")
        assert count == 5, (
            f"Expected 5 rows after checkpoint+restart, got {count}. "
            "The hello-lines connector may not have honoured the ExactlyOnce "
            "seek payload (it re-read from position 0 instead of EOF)."
        )
    finally:
        stop_pipeline(pipeline_name)
        os.unlink(input_path)
