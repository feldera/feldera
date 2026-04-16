import json
import time
from http import HTTPStatus
from urllib.parse import quote_plus

from .helper import (
    create_pipeline,
    get,
    post_no_body,
    http_request,
    api_url,
    start_pipeline_as_paused,
    resume_pipeline,
    stop_pipeline,
    clear_pipeline,
    gen_pipeline_name,
    wait_for_condition,
)

from feldera.testutils import FELDERA_TEST_NUM_HOSTS
from feldera.stats import PipelineStatistics
from feldera.enums import PipelineStatus


def _ingest_lines(name: str, table: str, body: str):
    r = http_request(
        "POST",
        api_url(f"/pipelines/{name}/ingress/{table}"),
        headers={"Content-Type": "text/plain"},
        data=body.encode("utf-8"),
    )
    assert r.status_code in (HTTPStatus.OK, HTTPStatus.ACCEPTED), (
        r.status_code,
        r.text,
    )
    return r


def _adhoc_count(name: str, table: str) -> int:
    path = api_url(
        f"/pipelines/{name}/query?sql={quote_plus(f'SELECT COUNT(*) AS c FROM {table}')}&format=json"
    )
    r = get(path)
    if r.status_code != HTTPStatus.OK:
        return -1
    txt = r.text.strip()
    if not txt:
        return 0
    line = json.loads(txt.split("\n")[0])
    return line.get("c") or 0


@gen_pipeline_name
def test_pipeline_metrics(pipeline_name):
    """
    Tests that circuit metrics can be retrieved from the pipeline.
    """
    create_pipeline(pipeline_name, "")
    start_pipeline_as_paused(pipeline_name)

    # Default
    r_default = get(api_url(f"/pipelines/{pipeline_name}/metrics"))
    assert r_default.status_code == HTTPStatus.OK
    assert "# TYPE records_processed_total counter" in r_default.text

    # Prometheus
    r_prom = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=prometheus"))
    assert r_prom.status_code == HTTPStatus.OK
    assert "# TYPE records_processed_total counter" in r_prom.text

    # JSON
    if FELDERA_TEST_NUM_HOSTS == 1:
        r_json = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=json"))
        assert r_json.status_code == HTTPStatus.OK
        parsed_json = json.loads(r_json.text)
        assert isinstance(parsed_json, list), "Expected JSON metrics array"

        assert any(m.get("key") == "records_processed_total" for m in parsed_json), (
            "records_processed_total missing in JSON metrics"
        )

    # Invalid
    r_bad = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=does-not-exist"))
    assert r_bad.status_code == HTTPStatus.BAD_REQUEST


@gen_pipeline_name
def test_pipeline_stats(pipeline_name):
    """
    Tests retrieving pipeline statistics via `/stats`.
    """
    sql = """
    CREATE TABLE t1(c1 INT) WITH (
        'materialized'='true',
        'connectors'='[{
            "transport":{
                "name":"datagen",
                "config":{"plan":[{"limit":5,"rate":1000}]}
            }
        }]'
    );
    CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;
    """.strip()

    create_pipeline(pipeline_name, sql)
    start_pipeline_as_paused(pipeline_name)

    # Create output connector on v1 (egress)
    r_out = post_no_body(api_url(f"/pipelines/{pipeline_name}/egress/v1"), stream=True)
    assert r_out.status_code == HTTPStatus.OK, (r_out.status_code, r_out.text)

    resume_pipeline(pipeline_name)

    # Wait for datagen completion
    wait_for_condition(
        "datagen ingests 5 rows into t1",
        lambda: _adhoc_count(pipeline_name, "t1") == 5,
        timeout_s=10.0,
        poll_interval_s=1.0,
    )
    assert _adhoc_count(pipeline_name, "t1") == 5, "Did not ingest expected 5 rows"

    # Wait for all the steps to be completed.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        r_stats = get(api_url(f"/pipelines/{pipeline_name}/stats"))
        assert r_stats.status_code == HTTPStatus.OK, (r_stats.status_code, r_stats.text)
        stats = PipelineStatistics.from_dict(r_stats.json())
        gm = stats.global_metrics
        steps = gm.total_initiated_steps
        if steps is not None and steps == gm.total_completed_steps:
            break

    r_stats = get(api_url(f"/pipelines/{pipeline_name}/stats"))
    assert r_stats.status_code == HTTPStatus.OK, (r_stats.status_code, r_stats.text)
    r_stats_json = r_stats.json()
    keys = sorted(r_stats_json.keys())
    assert keys == [
        "checkpoint_activity",
        "global_metrics",
        "inputs",
        "outputs",
        "permanent_checkpoint_errors",
        "suspend_error",
    ]
    stats = PipelineStatistics.from_dict(r_stats_json)

    gm = stats.global_metrics
    assert gm.state == PipelineStatus.RUNNING
    assert gm.total_input_records == 5
    assert gm.total_processed_records == 5
    assert gm.pipeline_complete
    assert gm.buffered_input_records == 0
    assert gm.buffered_input_bytes == 0

    inputs = stats.inputs
    assert len(inputs) == 1
    inp = inputs[0]
    assert inp.config["stream"] == "t1"
    assert inp.metrics.buffered_bytes == 0
    assert inp.metrics.buffered_records == 0
    assert inp.metrics.end_of_input
    assert inp.metrics.num_parse_errors == 0
    assert inp.metrics.num_transport_errors == 0
    assert inp.metrics.total_bytes == 40
    assert inp.metrics.total_records == 5

    outputs = stats.outputs
    assert len(outputs) == 1
    out = outputs[0]
    assert out.config["stream"] == "v1"
    assert out.metrics.total_processed_steps == steps

    # /time_series
    def time_series_ready():
        resp = get(api_url(f"/pipelines/{pipeline_name}/time_series"))
        if resp.status_code != HTTPStatus.OK:
            return False
        samples = resp.json().get("samples") or []
        return len(samples) > 1 and samples[-1].get("r") == 5

    wait_for_condition(
        "time_series has >=2 samples and reflects 5 processed records",
        time_series_ready,
        timeout_s=10.0,
        poll_interval_s=1.0,
    )

    r_ts = get(api_url(f"/pipelines/{pipeline_name}/time_series"))
    assert r_ts.status_code == HTTPStatus.OK, r_ts.text
    ts = r_ts.json()
    samples = ts.get("samples") or []
    assert len(samples) > 1, f"Expected >=2 samples, got {len(samples)}"
    last = samples[-1]
    assert last.get("r") == 5


@gen_pipeline_name
def test_pipeline_logs(pipeline_name):
    """
    - Logs 404 before pipeline creation.
    - Create pipeline; poll until logs return 200.
    - Pause / start / stop / clear transitions keep logs accessible (200).
    - After delete, logs eventually return 404 again.
    """
    # 404 before creation
    r = get(api_url(f"/pipelines/{pipeline_name}/logs"))
    assert r.status_code == HTTPStatus.NOT_FOUND

    # Create pipeline
    create_pipeline(
        pipeline_name, "CREATE TABLE t1(c1 INTEGER) WITH ('materialized'='true');"
    )

    # Poll for logs availability
    wait_for_condition(
        "logs endpoint becomes available",
        lambda: get(
            api_url(f"/pipelines/{pipeline_name}/logs"), stream=True
        ).status_code
        == HTTPStatus.OK,
        timeout_s=30.0,
        poll_interval_s=0.5,
    )

    # Pause pipeline
    start_pipeline_as_paused(pipeline_name)
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Start pipeline
    resume_pipeline(pipeline_name)
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Stop force
    stop_pipeline(pipeline_name, force=True)
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Clear storage
    clear_pipeline(pipeline_name)
    # Logs should remain accessible
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Delete pipeline
    dr = http_request("DELETE", api_url(f"/pipelines/{pipeline_name}"))
    assert dr.status_code in (HTTPStatus.OK, HTTPStatus.ACCEPTED), (
        dr.status_code,
        dr.text,
    )

    # Poll until logs become unavailable (404)
    wait_for_condition(
        "logs endpoint becomes unavailable after deletion",
        lambda: get(
            api_url(f"/pipelines/{pipeline_name}/logs"), stream=True
        ).status_code
        == HTTPStatus.NOT_FOUND,
        timeout_s=30.0,
        poll_interval_s=0.5,
    )
