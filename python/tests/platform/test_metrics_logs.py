import json
import time
from http import HTTPStatus
from urllib.parse import quote_plus

from .helper import (
    get,
    post_json,
    post_no_body,
    http_request,
    wait_for_program_success,
    api_url,
    start_pipeline,
    stop_pipeline,
    clear_pipeline,
    pause_pipeline,
    gen_pipeline_name,
)


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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": ""})
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)
    pause_pipeline(pipeline_name)

    # Default
    r_default = get(api_url(f"/pipelines/{pipeline_name}/metrics"))
    assert r_default.status_code == HTTPStatus.OK
    text_default = r_default.text

    # Prometheus
    r_prom = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=prometheus"))
    assert r_prom.status_code == HTTPStatus.OK
    text_prom = r_prom.text

    # JSON
    r_json = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=json"))
    assert r_json.status_code == HTTPStatus.OK
    parsed_json = json.loads(r_json.text)
    assert isinstance(parsed_json, list), "Expected JSON metrics array"

    # Invalid
    r_bad = get(api_url(f"/pipelines/{pipeline_name}/metrics?format=does-not-exist"))
    assert r_bad.status_code == HTTPStatus.BAD_REQUEST

    # Minimal checks
    assert "# TYPE records_processed_total counter" in text_default
    assert "# TYPE records_processed_total counter" in text_prom
    assert any(m.get("key") == "records_processed_total" for m in parsed_json), (
        "records_processed_total missing in JSON metrics"
    )


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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)
    start_pipeline(pipeline_name)

    # Create output connector on v1 (egress)
    r_out = post_no_body(api_url(f"/pipelines/{pipeline_name}/egress/v1"), stream=True)
    assert r_out.status_code == HTTPStatus.OK, (r_out.status_code, r_out.text)

    # Wait for datagen completion
    time.sleep(3)
    deadline = time.time() + 10
    while time.time() < deadline:
        cnt = _adhoc_count(pipeline_name, "t1")
        if cnt == 5:
            break
        time.sleep(1)
    assert _adhoc_count(pipeline_name, "t1") == 5, "Did not ingest expected 5 rows"

    r_stats = get(api_url(f"/pipelines/{pipeline_name}/stats"))
    assert r_stats.status_code == HTTPStatus.OK, (r_stats.status_code, r_stats.text)
    stats = r_stats.json()
    keys = sorted(stats.keys())
    assert keys == ["global_metrics", "inputs", "outputs", "suspend_error"]

    gm = stats["global_metrics"]
    assert gm.get("state") == "Running"
    assert gm.get("total_input_records") == 5
    assert gm.get("total_processed_records") == 5
    assert gm.get("pipeline_complete")
    assert gm.get("buffered_input_records") == 0
    assert gm.get("buffered_input_bytes") == 0

    inputs = stats["inputs"]
    assert isinstance(inputs, list) and len(inputs) == 1
    inp = inputs[0]
    assert inp.get("config", {}).get("stream") == "t1"
    assert inp.get("metrics", {}).get("buffered_bytes") == 0
    assert inp.get("metrics", {}).get("buffered_records") == 0
    assert inp.get("metrics", {}).get("end_of_input")
    assert inp.get("metrics", {}).get("num_parse_errors") == 0
    assert inp.get("metrics", {}).get("num_transport_errors") == 0
    assert inp.get("metrics", {}).get("total_bytes") == 40
    assert inp.get("metrics", {}).get("total_records") == 5

    outputs = stats["outputs"]
    assert isinstance(outputs, list) and len(outputs) == 1
    out = outputs[0]
    assert out.get("config", {}).get("stream") == "v1"
    assert out.get("metrics", {}).get("total_processed_input_records") == 5

    # /time_series
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
    r = post_json(
        api_url("/pipelines"),
        {
            "name": pipeline_name,
            "program_code": "CREATE TABLE t1(c1 INTEGER) WITH ('materialized'='true');",
        },
    )
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

    # Poll for logs availability
    deadline = time.time() + 30
    while time.time() < deadline:
        resp = get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True)
        if resp.status_code == HTTPStatus.OK:
            break
        elif resp.status_code == HTTPStatus.NOT_FOUND:
            time.sleep(0.5)
            continue
        else:
            raise AssertionError(
                f"Unexpected status while waiting for logs: {resp.status_code} {resp.text}"
            )
    else:
        raise TimeoutError("Logs did not become available in time")

    # Pause pipeline
    pause_pipeline(pipeline_name)
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Start pipeline
    start_pipeline(pipeline_name)
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
    deadline = time.time() + 30
    while time.time() < deadline:
        resp = get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True)
        if resp.status_code == HTTPStatus.NOT_FOUND:
            break
        elif resp.status_code == HTTPStatus.OK:
            time.sleep(0.5)
            continue
        else:
            raise AssertionError(
                f"Unexpected status while waiting for logs to disappear: {resp.status_code} {resp.text}"
            )
    else:
        raise TimeoutError("Logs did not become unavailable after deletion")
