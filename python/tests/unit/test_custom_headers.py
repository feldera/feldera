"""Tests for the `headers` argument: extra HTTP headers on every request, and
what they take precedence over.

A deployment behind an authenticating proxy admits a request by the proxy's own
session cookie, which no Feldera credential can replace.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable
from unittest import mock

import pytest
import requests

from feldera.rest._httprequests import HttpRequests
from feldera.rest.config import Config
from feldera.rest.retry import RetryConfig


def _make_response(status_code: int = 200, body: bytes = b"{}") -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = body
    resp.headers["content-type"] = "application/json"
    prepared = requests.PreparedRequest()
    prepared.prepare(method="GET", url="http://example.test/v0/x")
    resp.request = prepared
    return resp


@contextmanager
def patch_method(name: str, responses: Iterable[requests.Response]):
    items = list(responses)

    def _call(*args, **kwargs):
        if not items:
            raise AssertionError("exhausted mock responses")
        return items.pop(0)

    with mock.patch(f"requests.{name}") as m:
        m.__name__ = name
        m.side_effect = _call
        yield m


def patch_get(responses: Iterable[requests.Response]):
    return patch_method("get", responses)


def _on_the_wire(call) -> requests.structures.CaseInsensitiveDict:
    """The headers as the request actually carries them.

    `requests` folds the mapping into a `CaseInsensitiveDict`, where two
    spellings of one name collapse and the last one inserted wins, so a
    plain dict comparison can pass while the wire says otherwise.
    """
    prepared = requests.PreparedRequest()
    prepared.prepare(
        method="GET", url="http://example.test/v0/x", headers=_headers_of(call)
    )
    return prepared.headers


def _client(**kwargs) -> HttpRequests:
    cfg = Config(
        url="http://example.test",
        retry_config=RetryConfig(
            max_retries=0,
            initial_backoff=0.0,
            max_backoff=0.0,
            multiplier=1.0,
            unhealthy_backoff=0.0,
        ),
        **kwargs,
    )
    return HttpRequests(cfg)


def _headers_of(call) -> dict:
    return call.kwargs["headers"]


class TestCustomHeaders:
    def test_headers_are_sent_with_every_request(self):
        http = _client(headers={"Cookie": "AWSELBAuthSessionCookie-0=abc"})
        with patch_get([_make_response(), _make_response()]) as m:
            http.get("/a")
            http.get("/b")
        for call in m.call_args_list:
            assert _headers_of(call)["Cookie"] == "AWSELBAuthSessionCookie-0=abc"

    def test_header_replaces_the_clients_own(self):
        # Both the bearer and the tenant are set by the client itself; a header
        # supplied by the caller wins over each.
        http = _client(
            api_key="apikey:from-config",
            tenant="from-config",
            headers={
                "Authorization": "Bearer from-header",
                "Feldera-Tenant": "from-header",
            },
        )
        with patch_get([_make_response()]) as m:
            http.get("/x")
        headers = _headers_of(m.call_args_list[0])
        assert headers["Authorization"] == "Bearer from-header"
        assert headers["Feldera-Tenant"] == "from-header"

    def test_header_replaces_the_clients_own_whatever_the_case(self):
        # `requests` compares header names case-insensitively, so a caller who
        # writes `authorization` must override the client's `Authorization`
        # rather than race it for the last insertion.
        http = _client(
            api_key="apikey:from-config",
            tenant="from-config",
            headers={
                "authorization": "Bearer from-header",
                "feldera-tenant": "from-header",
            },
        )
        with patch_get([_make_response()]) as m:
            http.get("/x")
        sent = _headers_of(m.call_args_list[0])
        assert len({name.lower() for name in sent}) == len(sent), (
            f"one spelling per header name should reach requests: {sent}"
        )
        headers = _on_the_wire(m.call_args_list[0])
        assert headers["Authorization"] == "Bearer from-header"
        assert headers["Feldera-Tenant"] == "from-header"

    def test_content_type_is_the_documented_exception(self):
        # Each request sets its own `Content-Type`, because it describes the
        # body the client serialized. The docstrings promise exactly this.
        http = _client(headers={"Content-Type": "text/plain"})
        with patch_method("post", [_make_response()]) as m:
            http.post("/x", body={"a": 1})
        assert _on_the_wire(m.call_args_list[0])["Content-Type"] == "application/json"

    def test_headers_reach_the_health_probe(self):
        # The 502 health probe is a separate request and needs the same
        # credentials to get past the proxy.
        http = _client(headers={"Cookie": "session=abc"})
        with patch_get([_make_response(body=b'{"all_healthy": true}')]) as m:
            assert http._check_cluster_health() is True
        assert _headers_of(m.call_args_list[0])["Cookie"] == "session=abc"

    def test_client_identity_survives_custom_headers(self):
        http = _client(headers={"X-Trace-Id": "42"})
        with patch_get([_make_response()]) as m:
            http.get("/x")
        headers = _headers_of(m.call_args_list[0])
        assert headers["User-Agent"] == "feldera-python-sdk/v1"
        assert headers["X-Trace-Id"] == "42"

    def test_no_headers_is_the_default(self):
        # Without the argument, a request carries only what the client itself
        # sets. The names are compared, never the values: one of them is a
        # credential.
        http = _client()
        with patch_get([_make_response()]) as m:
            http.get("/x")
        assert set(_headers_of(m.call_args_list[0])) <= {
            "User-Agent",
            "Authorization",
            "Feldera-Tenant",
            "Content-Type",
        }


class TestHeaderValidation:
    def test_non_string_header_is_rejected(self):
        # `requests` would otherwise fail deep inside the request, naming
        # neither the header nor the caller that supplied it.
        with pytest.raises(TypeError):
            _client(headers={"X-Count": 42})
        with pytest.raises(TypeError):
            _client(headers={42: "value"})

    def test_empty_header_name_is_rejected(self):
        with pytest.raises(ValueError):
            _client(headers={"  ": "value"})

    def test_headers_are_copied_from_the_caller(self):
        # A later change to the caller's mapping must not alter what is sent.
        supplied = {"Cookie": "session=abc"}
        http = _client(headers=supplied)
        supplied["Cookie"] = "session=changed"
        with patch_get([_make_response()]) as m:
            http.get("/x")
        assert _headers_of(m.call_args_list[0])["Cookie"] == "session=abc"
