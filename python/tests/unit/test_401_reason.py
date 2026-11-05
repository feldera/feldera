"""A 401 error names the reason the server gave in `WWW-Authenticate`."""

from typing import Optional

import requests

from feldera.rest.errors import FelderaAPIError


def _error_401(www_authenticate: Optional[str]) -> FelderaAPIError:
    req = requests.Request(
        "GET",
        "https://ci.example.com/v0/pipelines",
        headers={"Authorization": "Bearer t"},
    ).prepare()
    resp = requests.Response()
    resp.status_code = 401
    resp.request = req
    resp._content = b""
    if www_authenticate is not None:
        resp.headers["WWW-Authenticate"] = www_authenticate
    return FelderaAPIError("unauthorized", resp)


def test_reason_from_bearer_challenge():
    # The shape actix-web-httpauth writes, with an escaped quote.
    err = _error_401(
        'Bearer error_description="Could not verify the token\'s \\"kid\\""'
    )
    assert "credential rejected" in str(err)
    assert 'Reason: Could not verify the token\'s "kid"' in str(err)


def test_no_reason_without_description():
    assert "Reason:" not in str(_error_401(None))
    assert "Reason:" not in str(_error_401('Bearer realm="feldera"'))
