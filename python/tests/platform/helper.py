"""
Helper utilities for platform tests.

Provides:

- Lightweight REST wrappers (no SDK abstraction where raw status codes matter)
- Polling helpers (compilation, generic condition)
- Simple selector/object helpers

No automatic cleanup; pipelines are left in place for inspection after failures.
"""

from __future__ import annotations

import json
import pytest
import requests
from typing import Any, Dict, Iterable
from http import HTTPStatus
from urllib.parse import quote, quote_plus

from feldera.testutils_oidc import get_oidc_test_helper
from tests import (
    FELDERA_REQUESTS_VERIFY,
    API_KEY,
    BASE_URL,
    TEST_CLIENT,
    unique_pipeline_name,
)
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS

API_PREFIX = "/v0"


def _base_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
    }

    # Try OIDC authentication first, then fall back to API_KEY
    oidc_helper = get_oidc_test_helper()
    if oidc_helper is not None:
        token = oidc_helper.obtain_access_token()
        headers["Authorization"] = f"Bearer {token}"
    elif API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    return headers


def api_url(fragment: str) -> str:
    if not fragment.startswith("/"):
        fragment = "/" + fragment
    return f"{API_PREFIX}{fragment}"


def http_request(method: str, path: str, **kwargs) -> requests.Response:
    """
    Low-level request wrapper (no retries). Raises only on network errors.

    Args:
        method: HTTP method (GET, POST, etc.)
        path: URL path
        base_headers: Optional override for base headers. If None (default), uses _base_headers().
                      If empty dict {}, no base headers are applied (for testing unauthenticated requests).
        **kwargs: Additional arguments passed to requests.request()
    """
    if not path.startswith("/"):
        path = "/" + path
    url = BASE_URL.rstrip("/") + path

    # Allow override of base headers for testing unauthenticated requests
    base_headers_arg = kwargs.pop("base_headers", None)
    if base_headers_arg is None:
        base_headers = _base_headers()  # Default: include auth headers
    else:
        base_headers = base_headers_arg  # Override: could be {} for no auth

    custom_headers = kwargs.pop("headers", None) or {}
    headers = {
        **base_headers,
        **custom_headers,
    }  # Merge, with custom headers taking precedence

    # Provide a default timeout to avoid hanging tests.
    timeout = kwargs.pop("timeout", 30)
    kwargs["verify"] = FELDERA_REQUESTS_VERIFY
    resp = requests.request(
        method.upper(), url, headers=headers, timeout=timeout, **kwargs
    )
    return resp


def get(path: str, **kw) -> requests.Response:
    return http_request("GET", path, **kw)


def get_pipeline(name: str, selector: str) -> requests.Response:
    return get(f"{API_PREFIX}/pipelines/{name}?selector={selector}")


def post_no_body(path: str, **kw):
    return http_request("POST", path, **kw)


def post_json(path: str, body: Dict[str, Any], **kw) -> requests.Response:
    return http_request("POST", path, json=body, **kw)


def put_json(path: str, body: Dict[str, Any], **kw) -> requests.Response:
    return http_request("PUT", path, json=body, **kw)


def patch_json(path: str, body: Dict[str, Any], **kw) -> requests.Response:
    return http_request("PATCH", path, json=body, **kw)


def delete(path: str, **kw) -> requests.Response:
    return http_request("DELETE", path, **kw)


def create_pipeline(name: str, sql: str):
    r = post_json(
        api_url("/pipelines"),
        {
            "name": name,
            "program_code": sql,
            "runtime_config": {
                "workers": FELDERA_TEST_NUM_WORKERS,
                "hosts": FELDERA_TEST_NUM_HOSTS,
                "logging": "debug",
            },
        },
    )
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(name, 1)


def start_pipeline(name: str, wait: bool = True, observe_start: bool = False):
    TEST_CLIENT.start_pipeline(name, wait=wait, observe_start=observe_start)


def resume_pipeline(name: str, wait: bool = True):
    TEST_CLIENT.resume_pipeline(name, wait=wait)


def start_pipeline_as_paused(name: str, wait: bool = True):
    TEST_CLIENT.start_pipeline_as_paused(name, wait=wait)


def pause_pipeline(name: str, wait: bool = True):
    TEST_CLIENT.pause_pipeline(name, wait=wait)


def stop_pipeline(name: str, force: bool = True, wait: bool = True):
    TEST_CLIENT.stop_pipeline(name, force=force, wait=wait)


def clear_pipeline(name: str, wait: bool = True):
    r = post_no_body(f"{API_PREFIX}/pipelines/{name}/clear")
    if wait and r.status_code == HTTPStatus.ACCEPTED:
        TEST_CLIENT.clear_storage(name, timeout_s=60.0)
    return r


def reset_pipeline(name: str):
    if get_pipeline(name, "status").status_code != HTTPStatus.OK:
        return
    stop_pipeline(name, force=True)
    clear_pipeline(name)


def delete_pipeline(name: str):
    return delete(api_url(f"pipelines/{name}"))


def cleanup_pipeline(name: str):
    reset_pipeline(name)
    delete_pipeline(name)


def connector_action(pipeline: str, table: str, connector: str, action: str):
    table_enc = quote(table, safe="")
    r = post_no_body(
        api_url(
            f"/pipelines/{pipeline}/tables/{table_enc}/connectors/{connector}/{action}"
        )
    )
    return r


def adhoc_query_json(pipeline: str, sql: str):
    "Runs a SQL query, returns results as list of dicts (JSON lines format)."
    path = api_url(f"/pipelines/{pipeline}/query?sql={quote_plus(sql)}&format=json")
    resp = get(path)
    assert resp.status_code == HTTPStatus.OK, (
        f"Adhoc query failed: status={resp.status_code}, body={resp.text}"
    )
    raw = resp.text.strip()
    if not raw:
        return []
    lines = raw.split("\n")
    return [json.loads(line) for line in lines if line]


def pipeline_stats(pipeline: str):
    r = get(api_url(f"/pipelines/{pipeline}/stats"))
    assert r.status_code == HTTPStatus.OK, (r.status_code, r.text)
    return r.json()


def connector_stats(pipeline: str, table: str, connector: str):
    table_enc = quote(table, safe="")
    res = get(
        api_url(
            f"/pipelines/{pipeline}/tables/{table_enc}/connectors/{connector}/stats"
        )
    )
    assert res.status_code == HTTPStatus.OK, (res.status_code, res.text)
    return res.json()


def connector_paused(pipeline, table: str, connector: str) -> bool:
    return connector_stats(pipeline, table, connector)["paused"]


def wait_for_program_success(
    pipeline_name: str,
    expected_program_version: int,
    timeout_s: float = 1800.0,
    sleep_s: float = 0.5,
) -> None:
    """
    Wait until the pipeline's program has compiled successfully and reached
    `expected_program_version` or newer.
    """
    TEST_CLIENT.wait_for_program_success(
        pipeline_name,
        expected_program_version=expected_program_version,
        timeout_s=timeout_s,
        poll_interval_s=sleep_s,
    )


def wait_for_condition(
    description: str,
    predicate,
    timeout_s: float = 30.0,
    sleep_s: float = 0.2,
) -> None:
    """
    Proxy to Feldera client generic condition waiter.

    Usage guidance:
    - Use this helper in tests that only have names/REST helpers and no
      `Pipeline` object in scope.
    - If a `Pipeline` object exists, prefer calling
      `pipeline.client.wait_for_condition(...)` directly.
    """
    TEST_CLIENT.wait_for_condition(
        description,
        predicate,
        timeout_s=timeout_s,
        poll_interval_s=sleep_s,
    )


def extract_object_by_name(
    collection: Iterable[Dict[str, Any]], name: str
) -> Dict[str, Any]:
    for obj in collection:
        if obj.get("name") == name:
            return obj
    raise KeyError(f"Object with name '{name}' not found in collection")


def gen_pipeline_name(func):
    """
    Decorator for pytest functions that automatically generates a unique pipeline name.
    The decorated function will receive a 'pipeline_name' parameter.

    After the test completes, attempts to delete the pipeline but ignores any errors.
    """
    return pytest.mark.usefixtures("pipeline_name")(func)


__all__ = [
    "API_PREFIX",
    "HTTPStatus",
    "http_request",
    "get",
    "post_json",
    "put_json",
    "patch_json",
    "delete",
    "wait_for_program_success",
    "wait_for_condition",
    "unique_pipeline_name",
    "extract_object_by_name",
    "gen_pipeline_name",
    "connector_action",
]
