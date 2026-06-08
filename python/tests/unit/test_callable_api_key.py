"""Tests for the callable `api_key` path: per-request resolution and the
single re-resolve retry on a 401 (OIDC workload-identity token rotation)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, List, Optional
from unittest import mock

import pytest
import requests

from feldera.rest._httprequests import HttpRequests
from feldera.rest.config import Config
from feldera.rest.errors import FelderaAPIError
from feldera.rest.retry import RetryConfig


def _make_response(status_code: int, body: bytes = b"{}") -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = body
    resp.headers["content-type"] = "application/json"
    prepared = requests.PreparedRequest()
    prepared.prepare(method="GET", url="http://example.test/v0/x")
    resp.request = prepared
    return resp


def _sequence(responses: Iterable[object]):
    items = list(responses)

    def _call(*args, **kwargs):
        if not items:
            raise AssertionError("exhausted mock responses")
        nxt = items.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    return _call


@contextmanager
def patch_get(responses: Iterable[object]):
    with mock.patch("requests.get") as m:
        m.__name__ = "get"
        m.side_effect = _sequence(responses)
        yield m


def _client(api_key) -> HttpRequests:
    cfg = Config(
        url="http://example.test",
        api_key=api_key,
        retry_config=RetryConfig(
            max_retries=0,
            initial_backoff=0.0,
            max_backoff=0.0,
            multiplier=1.0,
            unhealthy_backoff=0.0,
        ),
    )
    return HttpRequests(cfg)


def _auth_of(call) -> Optional[str]:
    return call.kwargs["headers"].get("Authorization")


class TestResolveBearer:
    def test_static_key_used_verbatim(self):
        assert _client("apikey:abc")._resolve_bearer() == "apikey:abc"

    def test_callable_invoked_and_stripped(self):
        assert _client(lambda: "  tok  ")._resolve_bearer() == "tok"

    def test_non_str_callable_raises(self):
        with pytest.raises(TypeError):
            _client(lambda: 123)._resolve_bearer()

    def test_callable_invoked_per_request(self):
        tokens = iter(["t1", "t2"])
        http = _client(lambda: next(tokens))
        with patch_get([_make_response(200), _make_response(200)]) as m:
            http.get("/a")
            http.get("/b")
        assert _auth_of(m.call_args_list[0]) == "Bearer t1"
        assert _auth_of(m.call_args_list[1]) == "Bearer t2"


class TestUnauthorizedRetry:
    def test_401_reresolves_callable_once_then_succeeds(self):
        # First token is stale (401), the callable then yields a fresh token
        # that succeeds. The retry must use the freshly resolved token.
        tokens = iter(["stale", "fresh"])
        http = _client(lambda: next(tokens))
        with patch_get([_make_response(401), _make_response(200)]) as m:
            http.get("/x")
        assert len(m.call_args_list) == 2
        assert _auth_of(m.call_args_list[0]) == "Bearer stale"
        assert _auth_of(m.call_args_list[1]) == "Bearer fresh"

    def test_second_401_propagates(self):
        http = _client(lambda: "always-stale")
        with patch_get([_make_response(401), _make_response(401)]):
            with pytest.raises(FelderaAPIError):
                http.get("/x")

    def test_static_key_401_not_retried(self):
        # A static key cannot be re-resolved, so a 401 propagates without a
        # second attempt.
        http = _client("apikey:static")
        with patch_get([_make_response(401)]) as m:
            with pytest.raises(FelderaAPIError):
                http.get("/x")
        assert len(m.call_args_list) == 1


class TestHealthProbeAuth:
    def test_cluster_health_probe_sends_bearer(self):
        http = _client(lambda: "probe-tok")
        with patch_get([_make_response(200, b'{"all_healthy": true}')]) as m:
            assert http._check_cluster_health() is True
        assert _auth_of(m.call_args_list[0]) == "Bearer probe-tok"
