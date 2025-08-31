# TODO: these tests should be part of runtime tests

import os
import time
import json
import tempfile
from http import HTTPStatus

from .helper import (
    get,
    post_json,
    http_request,
    wait_for_program_success,
    api_url,
    start_pipeline,
    gen_pipeline_name,
    adhoc_query_json,
)


def _ingress_with_token(
    pipeline: str,
    table: str,
    record_json: str,
    *,
    format: str = "json",
    update_format: str = "raw",
):
    path = api_url(
        f"/pipelines/{pipeline}/ingress/{table}?format={format}&update_format={update_format}"
    )
    r = http_request(
        "POST",
        path,
        headers={"Content-Type": "application/json"},
        data=record_json.encode("utf-8"),
    )
    assert r.status_code == HTTPStatus.OK, (r.status_code, r.text)
    body = r.json()
    token = body.get("token")
    assert token, f"Expected completion token in response: {body}"
    return token


def _wait_token(pipeline: str, token: str, timeout_s: float = 30.0):
    path = api_url(f"/pipelines/{pipeline}/completion_status?token={token}")
    deadline = time.time() + timeout_s
    while True:
        r = get(path)
        assert r.status_code in (HTTPStatus.OK, HTTPStatus.ACCEPTED), (
            r.status_code,
            r.text,
        )
        status = r.json().get("status")
        if status == "complete":
            return
        assert status == "inprogress"
        if time.time() > deadline:
            raise TimeoutError(
                f"Timed out waiting for completion token={token} (last status={status})"
            )
        time.sleep(0.1)


def _count_for_value(pipeline: str, value: int):
    rows = adhoc_query_json(
        pipeline, f"select count(*) as c from t1 where c1 = {value}"
    )
    if not rows:
        return 0
    return rows[0].get("c") or 0


@gen_pipeline_name
def test_completion_tokens(pipeline_name):
    """
    - Pipeline without output connectors
    - Ingest many single-row JSON events, each returning a completion token
    - Poll completion_status for each token
    - Validate the row becomes visible exactly once
    """
    sql = (
        "CREATE TABLE t1(c1 integer, c2 bool, c3 varchar) "
        "WITH ('materialized' = 'true'); "
        "CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;"
    )
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)
    start_pipeline(pipeline_name)

    for i in range(0, 200):
        token = _ingress_with_token(
            pipeline_name,
            "T1",
            json.dumps({"c1": i, "c2": True}),
            format="json",
            update_format="raw",
        )
        _wait_token(pipeline_name, token)
        assert _count_for_value(pipeline_name, i) == 1, f"Value {i} expected count 1"


@gen_pipeline_name
def test_completion_tokens_with_outputs(pipeline_name):
    """
    - Pipeline with multiple file_output connectors on materialized views.
    - Ingest multiple records, verify completion tokens, and validate counts.
    - Start a paused datagen input connector, obtain a completion token through its endpoint,
      wait for completion, and validate resulting counts.
    """

    # Prepare temporary file paths
    def _temp_path() -> str:
        fd, path = tempfile.mkstemp(prefix="feldera_ct_", suffix=".out")
        os.close(fd)
        # Return path; do not delete; backend will write to it (if local FS accessible)
        return path

    output_path1 = _temp_path()
    output_path2 = _temp_path()
    output_path3 = _temp_path()
    output_path4 = _temp_path()

    # Embed paths into connectors JSON (JSON within single-quoted SQL string)
    def connector_json(paths):
        arr = []
        for p in paths:
            arr.append(
                {
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": p,
                        },
                    },
                    "format": {"name": "json"},
                }
            )
        return json.dumps(arr)

    connectors_v1 = connector_json([output_path1, output_path2])
    connectors_v2 = connector_json([output_path3, output_path4])

    sql = f"""
CREATE TABLE t1(c1 integer, c2 bool, c3 varchar)
WITH (
    'materialized' = 'true',
    'connectors' = '[{{
        "name": "datagen_connector",
        "paused": true,
        "transport": {{
            "name": "datagen",
            "config": {{"plan": [{{"limit": 1}}]}}
        }}
    }}]'
);

CREATE MATERIALIZED VIEW v1
WITH (
    'connectors' = '{connectors_v1}'
) AS SELECT * FROM t1;

CREATE MATERIALIZED VIEW v2
WITH (
    'connectors' = '{connectors_v2}'
) AS SELECT * FROM t1;
""".strip()

    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)
    start_pipeline(pipeline_name)

    # Ingest a number of rows; track expected counts
    for i in range(0, 50):
        token = _ingress_with_token(
            pipeline_name,
            "T1",
            json.dumps({"c1": i, "c2": True}),
            format="json",
            update_format="raw",
        )
        _wait_token(pipeline_name, token)
        assert _count_for_value(pipeline_name, i) == 1

    # Start the datagen connector
    r = http_request(
        "POST",
        api_url(
            f"/pipelines/{pipeline_name}/tables/t1/connectors/datagen_connector/start"
        ),
    )
    assert r.status_code == HTTPStatus.OK, (r.status_code, r.text)
    time.sleep(1.0)  # Allow connector to emit its record(s)

    # Obtain a completion token for the datagen connector
    r = get(
        api_url(
            f"/pipelines/{pipeline_name}/tables/t1/connectors/datagen_connector/completion_token"
        )
    )
    assert r.status_code == HTTPStatus.OK, (r.status_code, r.text)
    token = r.json().get("token")
    assert token, f"Missing token in connector completion_token response: {r.text}"

    _wait_token(pipeline_name, token)

    # Datagen (limit 1) produces c1=0 row; we inserted c1=0 already -> expected count becomes 2
    rows = adhoc_query_json(pipeline_name, "select count(*) as c from t1 where c1 = 0")
    assert rows and rows[0].get("c") == 2, f"Expected count 2 for c1=0, got {rows}"
