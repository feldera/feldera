import time
import json
from http import HTTPStatus
from urllib.parse import quote_plus

import pytest

from tests import TEST_CLIENT, enterprise_only
from .helper import (
    post_json,
    get,
    wait_for_program_success,
    api_url,
    post_no_body,
    start_pipeline,
    pause_pipeline,
    stop_pipeline,
    connector_paused,
    connector_action,
    gen_pipeline_name,
)


def _adhoc_count(name: str) -> int:
    path = api_url(
        f"/pipelines/{name}/query?sql={quote_plus('SELECT COUNT(*) AS c FROM t1')}&format=json"
    )
    r = get(path)
    if r.status_code != HTTPStatus.OK:
        return -1
    txt = r.text.strip()
    if not txt:
        return 0
    line = json.loads(txt.split("\n")[0])
    return line.get("c") or 0


def _wait_for_condition(desc: str, predicate, timeout_s: float = 30.0, sleep_s=0.5):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(sleep_s)
    raise TimeoutError(f"Timeout waiting for condition: {desc}")


@gen_pipeline_name
def test_checkpoint_oss(pipeline_name):
    """
    On OSS builds (non-enterprise), checkpoint endpoint should return NOT_IMPLEMENTED
    with error_code EnterpriseFeature.
    Skips itself if running against enterprise edition.
    """
    if TEST_CLIENT.get_config().edition.is_enterprise():
        pytest.skip("Enterprise edition: use enterprise checkpoint test instead")

    sql = "CREATE TABLE t1(x int) WITH ('materialized'='true');"
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

    resp = post_no_body(api_url(f"/pipelines/{pipeline_name}/checkpoint"))
    assert resp.status_code == HTTPStatus.NOT_IMPLEMENTED, resp.text
    body = resp.json()
    assert body.get("error_code") == "EnterpriseFeature", body


@enterprise_only
@gen_pipeline_name
def test_checkpoint_enterprise(pipeline_name):
    """
    Enterprise: invoke /checkpoint multiple times, poll /checkpoint_status for completion.
    """
    sql = "CREATE TABLE t1(x int) WITH ('materialized'='true');"
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)
    pause_pipeline(pipeline_name)

    for _ in range(5):
        resp = post_no_body(api_url(f"/pipelines/{pipeline_name}/checkpoint"))
        assert resp.status_code == HTTPStatus.OK, (
            f"Checkpoint POST failed: {resp.status_code} {resp.text}"
        )
        seq = resp.json().get("checkpoint_sequence_number")
        assert isinstance(seq, int), (
            f"Missing checkpoint_sequence_number in {resp.text}"
        )

        # Poll /checkpoint_status until success == seq
        deadline = time.time() + 10
        while True:
            status_resp = get(api_url(f"/pipelines/{pipeline_name}/checkpoint_status"))
            assert status_resp.status_code == HTTPStatus.OK, status_resp.text
            status_obj = status_resp.json()
            if status_obj.get("success") == seq:
                break
            if time.time() > deadline:
                raise TimeoutError(
                    f"Timeout waiting for checkpoint seq={seq} (status={status_obj})"
                )
            time.sleep(0.2)


@gen_pipeline_name
def test_suspend_oss(pipeline_name):
    """
    On OSS builds, attempting a non-force stop (suspend) should return NOT_IMPLEMENTED with EnterpriseFeature.
    Skips itself if enterprise.
    """
    if TEST_CLIENT.get_config().edition.is_enterprise():
        pytest.skip("Enterprise edition: use enterprise suspend test instead")

    sql = "CREATE TABLE t1(x int) WITH ('materialized'='true');"
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)

    resp = post_no_body(api_url(f"/pipelines/{pipeline_name}/stop?force=false"))
    assert resp.status_code == HTTPStatus.NOT_IMPLEMENTED, (
        resp.status_code,
        resp.text,
    )
    body = resp.json()
    assert body.get("error_code") == "EnterpriseFeature", body


@enterprise_only
@gen_pipeline_name
def test_suspend_enterprise(pipeline_name):
    """
    Enterprise suspend/resume sequence with connector dependencies:
      1. All three connectors (c1, c2[label1], c3[start_after label1]) start paused.
      2. Start pipeline -> connectors remain paused.
      3. Start c1 -> expect 1 record after completion.
      4. Suspend pipeline (stop without force) and resume -> c1 should remain running (EOI),
         c2,c3 paused.
      5. Start c2 -> c2 runs, triggers start_after dependency for c3 -> data from both.
      6. Suspend/resume again -> all connectors in EOI, verify no new data arrives.
    """
    sql = r"""
    CREATE TABLE t1 (
        x int
    ) WITH (
        'materialized' = 'true',
        'connectors' = '[{
            "name": "c1",
            "paused": true,
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [{
                        "limit": 1,
                        "fields": { "x": { "values": [1] } }
                    }]
                }
            }
        },
        {
            "name": "c2",
            "paused": true,
            "labels": ["label1"],
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [{
                        "limit": 3,
                        "fields": { "x": { "values": [2,3,4] } }
                    }]
                }
            }
        },
        {
            "name": "c3",
            "paused": true,
            "start_after": ["label1"],
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [{
                        "limit": 5,
                        "fields": { "x": { "values": [5,6,7,8,9] } }
                    }]
                }
            }
        }]'
    );
    """.strip()

    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

    # Start pipeline (all connectors remain paused)
    start_pipeline(pipeline_name)
    assert connector_paused(pipeline_name, "t1", "c1")
    assert connector_paused(pipeline_name, "t1", "c2")
    assert connector_paused(pipeline_name, "t1", "c3")

    # Start connector c1
    connector_action(pipeline_name, "t1", "c1", "start")

    _wait_for_condition(
        "1 record from c1",
        lambda: _adhoc_count(pipeline_name) == 1,
        timeout_s=10,
    )

    # Suspend (non-force) and resume pipeline
    stop_pipeline(pipeline_name, force=False)
    start_pipeline(pipeline_name)
    # After resume: c1 running (EOI), c2,c3 still paused
    assert not connector_paused(pipeline_name, "t1", "c1")
    assert connector_paused(pipeline_name, "t1", "c2")
    assert connector_paused(pipeline_name, "t1", "c3")

    # Start c2 (should also allow c3 to run automatically after c2 finishes, due to start_after label)
    connector_action(pipeline_name, "t1", "c2", "start")

    _wait_for_condition(
        "9 total records after c2/c3",
        lambda: _adhoc_count(pipeline_name) == 9,
        timeout_s=15,
    )

    # Suspend/resume again
    stop_pipeline(pipeline_name, force=False)
    start_pipeline(pipeline_name)

    # All connectors should now be running (EOI)
    assert not connector_paused(pipeline_name, "t1", "c1")
    assert not connector_paused(pipeline_name, "t1", "c2")
    assert not connector_paused(pipeline_name, "t1", "c3")

    # Wait a bit and ensure no new records (limit sets done)
    final_count = _adhoc_count(pipeline_name)
    time.sleep(5)
    assert _adhoc_count(pipeline_name) == final_count, (
        "Received new records after all connectors reached EOI"
    )

@gen_pipeline_name
def test_stop_start(pipeline_name):
    sql = r"""
    CREATE TABLE t1 (
        x int
    ) WITH (
        'materialized' = 'true',
        'connectors' = '[{
            "name": "c1",
            "paused": true,
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [{
                        "limit": 1,
                        "fields": { "x": { "values": [1] } }
                    }]
                }
            }
        }]'
    );
    """.strip()

    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

    for _i in range(0, 10):
        stop_pipeline(pipeline_name, force=False)
        start_pipeline(pipeline_name)
        final_count = _adhoc_count(pipeline_name)
        assert _adhoc_count(pipeline_name) == final_count, (
            "Received new records after all connectors reached EOI"
        )
