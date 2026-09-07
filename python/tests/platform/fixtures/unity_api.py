"""Minimal Databricks REST client for the Unity Catalog fixtures.

Only what the fixtures need: an OAuth token, a statement runner that polls, and
the two Unity calls that turn a table name into an ``s3://`` location plus a
temporary credential.

Credentials come from the environment; source ``~/.feldera-dbx-test.env``.
"""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.request

#: The API caps `wait_timeout` at 50s, so anything longer has to be polled.
STATEMENT_TIMEOUT_S = 1800

#: Per-request ceiling. Generous: a statement call blocks server-side for up to
#: `wait_timeout`, and these run against a warehouse that may be cold. It exists
#: to break a stalled connection, not to bound a slow query -- without it a
#: hung socket never returns and `STATEMENT_TIMEOUT_S`, which is only checked
#: between calls, is never reached.
REQUEST_TIMEOUT_S = 300


class UnityApiError(Exception):
    """A Unity Catalog call failed, or its configuration is missing.

    Deliberately not `SystemExit`: this module is imported by tests as well as
    run from `__main__`, and `SystemExit` is a `BaseException` that sidesteps
    pytest's normal failure reporting. `main()` turns it back into an exit code.
    """


def require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise UnityApiError(f"{name} is not set; source ~/.feldera-dbx-test.env")
    return value


def token(host: str, client_id: str, client_secret: str) -> str:
    """An OAuth M2M token. Unity vends the storage credentials separately."""
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    request = urllib.request.Request(
        f"{host}/oidc/v1/token",
        data=b"grant_type=client_credentials&scope=all-apis",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    return json.load(urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_S))[
        "access_token"
    ]


def token_from_env(host: str) -> str:
    return token(
        host,
        require("DELTA_TABLE_TEST_UNITY_CLIENT_ID"),
        require("DELTA_TABLE_TEST_UNITY_CLIENT_SECRET"),
    )


def get(host: str, tok: str, path: str) -> dict:
    request = urllib.request.Request(
        f"{host}{path}", headers={"Authorization": f"Bearer {tok}"}
    )
    return json.load(urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_S))


def post(host: str, tok: str, path: str, body: dict) -> dict:
    request = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    )
    return json.load(urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_S))


def _run(host: str, tok: str, warehouse: str, statement: str) -> dict:
    try:
        result = post(
            host,
            tok,
            "/api/2.0/sql/statements",
            {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"},
        )
    except urllib.error.HTTPError as e:
        raise UnityApiError(f"HTTP {e.code}: {e.read().decode()[:400]}") from e

    deadline = time.monotonic() + STATEMENT_TIMEOUT_S
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            raise UnityApiError(
                f"timed out after {STATEMENT_TIMEOUT_S}s:\n{statement[:200]}"
            )
        time.sleep(5)
        result = get(host, tok, f"/api/2.0/sql/statements/{result['statement_id']}")

    state = result["status"]["state"]
    if state != "SUCCEEDED":
        raise UnityApiError(
            f"{state}: {json.dumps(result['status'])[:400]}\n{statement[:200]}"
        )
    return result


def sql(host: str, tok: str, warehouse: str, statement: str) -> list:
    """Run one statement to completion and return its rows as lists of cells."""
    return _run(host, tok, warehouse, statement).get("result", {}).get("data_array", [])


def sql_dicts(host: str, tok: str, warehouse: str, statement: str) -> list[dict]:
    """As `sql`, but keyed by column name.

    `DESCRIBE DETAIL` has no stable column *order* worth relying on, and
    Databricks rejects `SELECT ... FROM (DESCRIBE DETAIL t)`, so read the names
    off the result manifest instead of counting positions.
    """
    result = _run(host, tok, warehouse, statement)
    names = [c["name"] for c in result["manifest"]["schema"]["columns"]]
    return [
        dict(zip(names, row)) for row in result.get("result", {}).get("data_array", [])
    ]


def table_location_and_credentials(host: str, tok: str, full_name: str) -> dict:
    """Resolve a table to its ``s3://`` location and a temporary read credential.

    Reading over the raw location rather than ``uc://`` is deliberate where a
    test needs the object-store reader itself in the picture.
    """
    table = get(host, tok, f"/api/2.1/unity-catalog/tables/{full_name}")
    creds = post(
        host,
        tok,
        "/api/2.1/unity-catalog/temporary-table-credentials",
        {"table_id": table["table_id"], "operation": "READ"},
    )["aws_temp_credentials"]
    return {
        "uri": table["storage_location"],
        "aws_access_key_id": creds["access_key_id"],
        "aws_secret_access_key": creds["secret_access_key"],
        "aws_session_token": creds["session_token"],
    }
