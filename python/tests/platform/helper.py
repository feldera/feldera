"""
Helper utilities for platform tests.

Provides:

- Lightweight REST wrappers (no SDK abstraction where raw status codes matter)
- Polling helpers (compilation, generic condition)
- Simple selector/object helpers

No automatic cleanup; pipelines are left in place for inspection after failures.
"""

from __future__ import annotations

import time
import json
import logging
import pytest
import requests
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from http import HTTPStatus
from urllib.parse import quote, quote_plus

from tests import FELDERA_TLS_INSECURE, API_KEY, BASE_URL, unique_pipeline_name
from .oidc_test_helper import get_oidc_test_helper

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
    """
    if not path.startswith("/"):
        path = "/" + path
    url = BASE_URL.rstrip("/") + path
    headers = kwargs.pop("headers", None) or _base_headers()
    # Provide a default timeout to avoid hanging tests.
    timeout = kwargs.pop("timeout", 30)
    kwargs["verify"] = not FELDERA_TLS_INSECURE
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


def wait_for_deployment_status(name: str, desired: str, timeout_s: float = 60.0):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        r = get_pipeline(name, "status")
        if r.status_code != HTTPStatus.OK:
            time.sleep(0.25)
            continue
        obj = r.json()
        last = obj.get("deployment_status")
        if last == desired:
            return obj
        time.sleep(0.25)
    raise TimeoutError(
        f"Timed out waiting for pipeline '{name}' deployment_status={desired} (last={last})"
    )


def start_pipeline(name: str, wait: bool = True):
    r = post_no_body(api_url(f"/pipelines/{name}/start"))
    assert r.status_code == HTTPStatus.ACCEPTED, (
        f"Unexpected start response: {r.status_code} {r.text}"
    )
    if wait:
        wait_for_deployment_status(name, "Running", 30)
    return r


def resume_pipeline(name: str, wait: bool = True):
    r = post_no_body(api_url(f"/pipelines/{name}/resume"))
    assert r.status_code == HTTPStatus.ACCEPTED, (
        f"Unexpected resume response: {r.status_code} {r.text}"
    )
    if wait:
        wait_for_deployment_status(name, "Running", 30)
    return r


def start_pipeline_as_paused(name: str, wait: bool = True):
    r = post_no_body(api_url(f"/pipelines/{name}/start"), params={"initial": "paused"})
    assert r.status_code == HTTPStatus.ACCEPTED, (
        f"Unexpected pause response: {r.status_code} {r.text}"
    )
    if wait:
        wait_for_deployment_status(name, "Paused", 30)
    return r


def pause_pipeline(name: str, wait: bool = True):
    r = post_no_body(api_url(f"/pipelines/{name}/pause"))
    assert r.status_code == HTTPStatus.ACCEPTED, (
        f"Unexpected pause response: {r.status_code} {r.text}"
    )
    if wait:
        wait_for_deployment_status(name, "Paused", 30)
    return r


def stop_pipeline(name: str, force: bool = True, wait: bool = True):
    r = post_no_body(
        api_url(f"/pipelines/{name}/stop?force={'true' if force else 'false'}")
    )
    assert r.status_code == HTTPStatus.ACCEPTED, (
        f"Unexpected stop response: {r.status_code} {r.text}"
    )
    wait_for_deployment_status(name, "Stopped", 30)
    return r


def wait_for_cleared_storage(name: str, timeout_s: float = 60.0):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        r = get_pipeline(name, "status")
        if r.status_code != HTTPStatus.OK:
            time.sleep(0.25)
            continue
        obj = r.json()
        last = obj.get("storage_status")
        if last == "Cleared":
            return obj
        time.sleep(0.25)
    raise TimeoutError(
        f"Timed out waiting for pipeline '{name}' to clear storage (last={last})"
    )


def clear_pipeline(name: str, wait: bool = True):
    r = post_no_body(f"{API_PREFIX}/pipelines/{name}/clear")
    if wait and r.status_code == HTTPStatus.ACCEPTED:
        wait_for_cleared_storage(name)
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


@dataclass
class WaitResult:
    ok: bool
    last_status: Optional[str]
    last_object: Optional[Dict[str, Any]]
    elapsed_s: float


def wait_for_program_success(
    pipeline_name: str,
    expected_program_version: int,
    timeout_s: float = 1800.0,
    sleep_s: float = 0.5,
) -> WaitResult:
    """
    Poll until the pipeline's program_status is Success and program_version
    >= expected_program_version.

    Mirrors semantics of the Rust `wait_for_compiled_program` helper.

    Returns a WaitResult. Raises AssertionError on compile error, TimeoutError on timeout.
    """
    deadline = time.time() + timeout_s
    last_status = None
    last_obj: Optional[Dict[str, Any]] = None

    while True:
        if time.time() > deadline:
            raise TimeoutError(
                f"Timed out waiting for pipeline '{pipeline_name}' to compile "
                f"(expected program_version >= {expected_program_version}, "
                f"last_status={last_status}, last_obj={last_obj})"
            )
        resp = get(f"{API_PREFIX}/pipelines/{pipeline_name}")
        if resp.status_code == HTTPStatus.NOT_FOUND:
            raise RuntimeError(
                f"Pipeline '{pipeline_name}' disappeared during compilation wait"
            )
        try:
            obj = resp.json()
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse pipeline JSON: {e}; body={resp.text!r}"
            )
        last_obj = obj
        last_status = obj.get("program_status")
        version = obj.get("program_version") or 0

        if last_status == "Success" and version >= expected_program_version:
            return WaitResult(
                True, last_status, last_obj, timeout_s - (deadline - time.time())
            )

        if last_status in ("SqlError", "RustError"):
            raise AssertionError(
                "Compilation failed: " + json.dumps(obj.get("program_error"), indent=2)
            )

        time.sleep(sleep_s)


def wait_for_condition(
    description: str,
    predicate,
    timeout_s: float = 30.0,
    sleep_s: float = 0.2,
) -> None:
    """
    Generic polling helper.
    predicate: callable returning truthy when condition met (can be sync or async).
    """
    start = time.time()
    deadline = start + timeout_s
    attempt = 0
    while True:
        now = time.time()
        if now > deadline:
            raise TimeoutError(f"Timeout waiting for condition: {description}")
        attempt += 1
        try:
            result = predicate()
        except Exception as e:  # noqa: BLE001
            logging.debug("Predicate raised %s (attempt %d), continuing", e, attempt)
            result = False

        if result:
            return
        time.sleep(sleep_s)


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
