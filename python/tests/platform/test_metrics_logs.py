import json
import time
import uuid
from http import HTTPStatus
from urllib.parse import quote_plus

import requests

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
    wait_for_pipeline_reachable,
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
    wait_for_pipeline_reachable(pipeline_name)

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
    wait_for_pipeline_reachable(pipeline_name)

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
        lambda: (
            get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
            == HTTPStatus.OK
        ),
        timeout_s=30.0,
        poll_interval_s=0.5,
    )

    # Pause pipeline
    start_pipeline_as_paused(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)
    assert (
        get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
        == HTTPStatus.OK
    )

    # Start pipeline
    resume_pipeline(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)
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
        lambda: (
            get(api_url(f"/pipelines/{pipeline_name}/logs"), stream=True).status_code
            == HTTPStatus.NOT_FOUND
        ),
        timeout_s=30.0,
        poll_interval_s=0.5,
    )


_POSITION_HEADERS = ("feldera-logs-epoch", "feldera-logs-seq", "feldera-logs-gap")


def _read_logs(
    pipeline_name: str,
    cursor: str | None,
    count: int,
    timeout_s: float = 30.0,
) -> tuple[dict | None, list[str]]:
    """
    Opens the logs stream and reads the first `count` log lines from it.

    `cursor` selects the resume protocol: `None` omits the parameter entirely and asks
    for the legacy stream, an empty string asks to start from the beginning of the
    retained buffer. A caller that supplies either form of a cursor is told its position
    in the response headers, which are returned separately from the log lines.

    A `count` of zero reads the position alone, which is all a caller already at the end
    of the stream can expect to receive. The position arrives with the response head, so
    such a read completes without waiting for a line that may never come.
    """
    path = api_url(f"/pipelines/{pipeline_name}/logs")
    if cursor is not None:
        path += f"?cursor={quote_plus(cursor)}"

    lines: list[str] = []
    with get(path, stream=True, timeout=timeout_s) as resp:
        assert resp.status_code == HTTPStatus.OK, (resp.status_code, resp.text)
        position = _position_of(resp)
        if count > 0:
            for raw in resp.iter_lines():
                lines.append(raw.decode("utf-8"))
                if len(lines) >= count:
                    break
    return position, lines


def _position_of(resp: requests.Response) -> dict | None:
    """
    The position a logs response reports, or `None` if it reports none.

    All three headers are required. A response carrying only some of them could not be
    turned into a cursor, so it is a failure rather than something to interpret.
    """
    present = [h in resp.headers for h in _POSITION_HEADERS]
    assert all(present) or not any(present), dict(resp.headers)
    if not all(present):
        return None
    epoch, seq, gap = (resp.headers[h] for h in _POSITION_HEADERS)
    return {"epoch": epoch, "seq": int(seq), "gap": int(gap)}


def _has_log_lines(pipeline_name: str, count: int) -> bool:
    """
    Whether the stream already holds `count` lines. A stream with fewer simply stays open
    until more arrive, so a read timeout is the expected answer for "not yet". Every other
    failure is reported, rather than being retried until the enclosing wait gives up with
    nothing to show for it.
    """
    try:
        _, lines = _read_logs(pipeline_name, "", count, timeout_s=5.0)
        return len(lines) >= count
    except requests.exceptions.Timeout:
        return False


@gen_pipeline_name
def test_pipeline_logs_cursor(pipeline_name):
    """
    A caller that presents a cursor receives the lines it is missing, rather than the
    whole retained buffer, with neither duplication nor loss across the reconnect.
    """
    create_pipeline(pipeline_name, "CREATE TABLE t1(c1 INTEGER);")
    start_pipeline_as_paused(pipeline_name)
    wait_for_pipeline_reachable(pipeline_name)

    prefix, suffix = 3, 2
    wait_for_condition(
        "pipeline has produced enough log lines",
        lambda: _has_log_lines(pipeline_name, prefix + suffix),
        timeout_s=60.0,
        poll_interval_s=1.0,
    )

    # A full catch-up, read first, is the authority on what the stream holds. Every read
    # below asserts a zero gap, so an eviction crossing the test fails on the position
    # that reports it rather than on a line comparison that cannot explain itself.
    whole_position, whole = _read_logs(pipeline_name, "", prefix + suffix)
    assert (whole_position["seq"], whole_position["gap"]) == (0, 0), whole_position
    epoch = whole_position["epoch"]

    # Read the head of the stream, then reconnect where that read left off. The two
    # partial reads must reconstruct the prefix of the full read exactly: a cursor that
    # replayed would duplicate lines here, one that skipped would drop them.
    first, head = _read_logs(pipeline_name, "", prefix)
    assert (first["epoch"], first["seq"], first["gap"]) == (epoch, 0, 0), first

    resumed, tail = _read_logs(pipeline_name, f"{epoch}:{prefix}", suffix)
    assert resumed["epoch"] == epoch, resumed
    assert (resumed["seq"], resumed["gap"]) == (prefix, 0), resumed
    assert head + tail == whole

    # A cursor from another instance of the buffer refers to lines this instance never
    # held, so it is answered with a full catch-up instead of being trusted.
    stale, replayed = _read_logs(pipeline_name, f"{uuid.uuid4()}:{prefix}", prefix)
    assert stale["epoch"] == epoch, stale
    assert (stale["seq"], stale["gap"]) == (0, 0), stale
    assert replayed == head

    # A cursor past the end of the stream names a position the buffer can never reach.
    # Answering with the end of the stream is what lets the next connection resume, where
    # echoing the position back would deliver nothing for the life of the epoch.
    beyond_end, _ = _read_logs(pipeline_name, f"{epoch}:{2**64 - 1}", 0)
    assert beyond_end["gap"] == 0, beyond_end
    assert prefix + suffix <= beyond_end["seq"] < 2**64 - 1, beyond_end

    # Omitting the cursor keeps the legacy stream, which reports no position at all.
    legacy_position, legacy = _read_logs(pipeline_name, None, 1)
    assert legacy_position is None
    assert legacy[0] == whole[0], legacy

    # A malformed cursor can only come from a broken client, so it is rejected rather
    # than quietly reinterpreted as some other position.
    r = get(api_url(f"/pipelines/{pipeline_name}/logs?cursor=nonsense"), stream=True)
    assert r.status_code == HTTPStatus.BAD_REQUEST, (r.status_code, r.text)
