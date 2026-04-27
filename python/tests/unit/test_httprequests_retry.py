"""Tests for the tenacity-based retry behavior in HttpRequests."""

from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from typing import Iterable, List, Optional
from unittest import mock

import pytest
import requests

from feldera.rest._httprequests import HttpRequests
from feldera.rest.config import Config
from feldera.rest.errors import (
    FelderaAPIError,
    FelderaCommunicationError,
    FelderaTimeoutError,
)
from feldera.rest.retry import RetryConfig


def _make_response(
    status_code: int,
    body: bytes = b"{}",
    content_type: str = "application/json",
    headers: Optional[dict] = None,
) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = body
    resp.headers["content-type"] = content_type
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    # __validate uses request.request.url when formatting 401 messages — supply
    # a harmless PreparedRequest so attribute access does not fail elsewhere.
    prepared = requests.PreparedRequest()
    prepared.prepare(method="GET", url="http://example.test/v0/x")
    resp.request = prepared
    return resp


def _fast_retry(max_retries: int = 3, **overrides) -> RetryConfig:
    # Near-zero backoff so tests finish quickly.
    base = dict(
        max_retries=max_retries,
        initial_backoff=0.0,
        max_backoff=0.0,
        multiplier=1.0,
        unhealthy_backoff=0.0,
    )
    base.update(overrides)
    return RetryConfig(**base)


def _make_client(retry_config: Optional[RetryConfig] = None) -> HttpRequests:
    cfg = Config(
        url="http://example.test",
        retry_config=retry_config or _fast_retry(),
    )
    return HttpRequests(cfg)


def _sequence(responses: Iterable[object]):
    """Return a side_effect callable producing the given responses/exceptions."""
    items = list(responses)
    calls: List[int] = []

    def _call(*args, **kwargs):
        calls.append(1)
        if not items:
            raise AssertionError("exhausted mock responses")
        nxt = items.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    _call.remaining = items
    _call.call_count_list = calls
    return _call


@contextmanager
def patch_requests(method: str, responses: Iterable[object]):
    """Patch `requests.<method>` with a mock that carries `__name__`.

    `HttpRequests.send_request` dispatches on `http_method.__name__`, so the
    MagicMock replacement must expose that attribute.
    """
    with mock.patch(f"requests.{method}") as m:
        m.__name__ = method
        m.side_effect = _sequence(responses)
        yield m


class TestRetryConfig:
    def test_defaults(self):
        cfg = RetryConfig()
        assert cfg.max_retries == 3
        assert cfg.initial_backoff == 2.0
        assert cfg.max_backoff == 64.0
        assert cfg.multiplier == 2.0
        assert cfg.jitter == 0.0
        assert cfg.unhealthy_backoff == 90.0
        assert cfg.retryable_status_codes == frozenset({408, 429, 502, 503, 504})

    def test_validation(self):
        with pytest.raises(ValueError):
            RetryConfig(max_retries=-1)
        with pytest.raises(ValueError):
            RetryConfig(initial_backoff=-0.5)
        with pytest.raises(ValueError):
            RetryConfig(max_backoff=-1.0)
        with pytest.raises(ValueError):
            RetryConfig(multiplier=0)
        with pytest.raises(ValueError):
            RetryConfig(jitter=-0.1)
        with pytest.raises(ValueError):
            RetryConfig(unhealthy_backoff=-1.0)

    def test_is_frozen(self):
        cfg = RetryConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.max_retries = 99  # type: ignore[misc]

    def test_status_codes_are_coerced_to_frozenset(self):
        cfg = RetryConfig(retryable_status_codes={500, 502})  # plain set
        assert isinstance(cfg.retryable_status_codes, frozenset)
        assert cfg.retryable_status_codes == frozenset({500, 502})

    def test_config_default_retry_config(self):
        assert Config().retry_config == RetryConfig()

    def test_config_uses_custom_retry_config(self):
        rc = RetryConfig(max_retries=7)
        assert Config(retry_config=rc).retry_config is rc


class TestRetryBehavior:
    def test_success_no_retry(self):
        client = _make_client()
        with patch_requests("get", [_make_response(200, b'{"ok": true}')]) as m:
            result = client.get("/foo")
        assert result == {"ok": True}
        assert m.call_count == 1

    def test_503_then_success(self):
        client = _make_client()
        with patch_requests(
            "get",
            [_make_response(503), _make_response(503), _make_response(200, b"{}")],
        ) as m:
            assert client.get("/foo") == {}
        assert m.call_count == 3

    def test_503_exhausts_raises(self):
        client = _make_client(_fast_retry(max_retries=2))
        with patch_requests("get", [_make_response(503)] * 3) as m:
            with pytest.raises(FelderaAPIError) as exc_info:
                client.get("/foo")
        assert exc_info.value.status_code == 503
        # max_retries=2 → 1 initial + 2 retries == 3 total calls.
        assert m.call_count == 3

    @pytest.mark.parametrize("status", [408, 429, 504])
    def test_other_retryable_statuses(self, status):
        client = _make_client()
        with patch_requests(
            "get", [_make_response(status), _make_response(200, b"{}")]
        ) as m:
            client.get("/foo")
        assert m.call_count == 2

    def test_404_is_not_retried(self):
        client = _make_client()
        with patch_requests(
            "get", [_make_response(404, b'{"error":"not found"}')]
        ) as m:
            with pytest.raises(FelderaAPIError) as exc_info:
                client.get("/foo")
        assert exc_info.value.status_code == 404
        assert m.call_count == 1

    def test_500_is_not_retried_by_default(self):
        client = _make_client()
        with patch_requests("get", [_make_response(500, b'{"error":"boom"}')]) as m:
            with pytest.raises(FelderaAPIError) as exc_info:
                client.get("/foo")
        assert exc_info.value.status_code == 500
        assert m.call_count == 1

    def test_custom_retryable_status_codes_includes_500(self):
        client = _make_client(_fast_retry(retryable_status_codes=frozenset({500})))
        with patch_requests(
            "get", [_make_response(500), _make_response(200, b"{}")]
        ) as m:
            client.get("/foo")
        assert m.call_count == 2

    def test_timeout_then_success(self):
        client = _make_client()
        with patch_requests(
            "get",
            [requests.exceptions.ReadTimeout("boom"), _make_response(200, b"{}")],
        ) as m:
            client.get("/foo")
        assert m.call_count == 2

    def test_connect_timeout_is_retried(self):
        # ConnectTimeout inherits from BOTH ConnectionError and Timeout — make
        # sure we treat it as retryable (Timeout branch wins) and surface the
        # final failure as FelderaTimeoutError.
        client = _make_client(_fast_retry(max_retries=1))
        with patch_requests(
            "get", [requests.exceptions.ConnectTimeout("conn")] * 2
        ) as m:
            with pytest.raises(FelderaTimeoutError):
                client.get("/foo")
        assert m.call_count == 2

    def test_timeout_exhausts_raises_timeout_error(self):
        client = _make_client(_fast_retry(max_retries=1))
        with patch_requests("get", [requests.exceptions.ReadTimeout("boom")] * 2) as m:
            with pytest.raises(FelderaTimeoutError):
                client.get("/foo")
        assert m.call_count == 2

    def test_connection_error_wrapped(self):
        client = _make_client()
        # ConnectionError isn't retryable — the first raise should propagate
        # and be wrapped as FelderaCommunicationError.
        with patch_requests("get", [requests.exceptions.ConnectionError("down")]):
            with pytest.raises(FelderaCommunicationError):
                client.get("/foo")

    def test_no_retries_when_max_retries_zero(self):
        client = _make_client(_fast_retry(max_retries=0))
        with patch_requests("get", [_make_response(503)]) as m:
            with pytest.raises(FelderaAPIError):
                client.get("/foo")
        assert m.call_count == 1

    def test_custom_retry_config_uses_its_limit(self):
        client = _make_client(_fast_retry(max_retries=5))
        with patch_requests("get", [_make_response(503)] * 6) as m:
            with pytest.raises(FelderaAPIError):
                client.get("/foo")
        assert m.call_count == 6  # 1 initial + 5 retries


class TestRetryAfter:
    def test_retry_after_header_is_honored(self):
        client = _make_client(_fast_retry(max_backoff=0.0))
        with patch_requests(
            "get",
            [
                _make_response(503, headers={"Retry-After": "0"}),
                _make_response(200, b"{}"),
            ],
        ) as m:
            with mock.patch("tenacity.nap.time.sleep") as sleeper:
                client.get("/foo")
        assert m.call_count == 2
        # Server asked for 0s; jitter and exp backoff are also 0 in _fast_retry.
        sleeper.assert_called_once_with(0.0)

    def test_retry_after_capped_at_max_backoff(self):
        # Server asks for 9999s; we should cap at our local max_backoff.
        client = _make_client(
            _fast_retry(initial_backoff=0.0, max_backoff=2.5, multiplier=1.0)
        )
        with patch_requests(
            "get",
            [
                _make_response(503, headers={"Retry-After": "9999"}),
                _make_response(200, b"{}"),
            ],
        ):
            with mock.patch("tenacity.nap.time.sleep") as sleeper:
                client.get("/foo")
        sleeper.assert_called_once_with(2.5)

    def test_retry_after_http_date_is_parsed(self):
        from datetime import datetime, timezone, timedelta

        future = datetime.now(timezone.utc) + timedelta(seconds=1)
        header = {"Retry-After": future.strftime("%a, %d %b %Y %H:%M:%S GMT")}
        client = _make_client(_fast_retry(max_backoff=10.0))
        with patch_requests(
            "get", [_make_response(503, headers=header), _make_response(200, b"{}")]
        ):
            with mock.patch("tenacity.nap.time.sleep") as sleeper:
                client.get("/foo")
        # Should sleep ~1s (minus a few ms of test overhead). Just assert the
        # call happened with a reasonable positive value capped at max_backoff.
        sleeper.assert_called_once()
        (slept,), _ = sleeper.call_args
        assert 0.0 <= slept <= 10.0


class TestExponentialBackoff:
    def test_backoff_grows_as_documented(self):
        """The first retry waits initial_backoff, then * multiplier, capped."""
        cfg = RetryConfig(
            max_retries=4,
            initial_backoff=1.0,
            max_backoff=8.0,
            multiplier=2.0,
            jitter=0.0,
        )
        client = _make_client(cfg)
        with patch_requests("get", [_make_response(503)] * 5):
            with mock.patch("tenacity.nap.time.sleep") as sleeper:
                with pytest.raises(FelderaAPIError):
                    client.get("/foo")
        # 4 retries → 4 sleeps. Schedule: 1, 2, 4, 8 (capped).
        slept = [call.args[0] for call in sleeper.call_args_list]
        assert slept == [1.0, 2.0, 4.0, 8.0]

    def test_jitter_adds_uniform_extra(self):
        cfg = RetryConfig(
            max_retries=2,
            initial_backoff=1.0,
            max_backoff=10.0,
            multiplier=1.0,
            jitter=0.5,
        )
        client = _make_client(cfg)
        with patch_requests("get", [_make_response(503)] * 3):
            with mock.patch("tenacity.nap.time.sleep") as sleeper:
                with pytest.raises(FelderaAPIError):
                    client.get("/foo")
        slept = [call.args[0] for call in sleeper.call_args_list]
        assert all(1.0 <= s < 1.5 for s in slept), slept


class Test502HealthHandling:
    def test_spurious_502_retries_immediately(self):
        """Healthy cluster + 502 → wait function returns 0 (no backoff)."""
        client = _make_client(_fast_retry(unhealthy_backoff=99.0))
        with patch_requests(
            "get", [_make_response(502), _make_response(200, b"{}")]
        ) as m:
            with mock.patch.object(
                client, "_check_cluster_health", return_value=True
            ) as health:
                with mock.patch("tenacity.nap.time.sleep") as sleeper:
                    client.get("/foo")
        assert m.call_count == 2
        assert health.call_count == 1
        sleeper.assert_called_once_with(0.0)

    def test_unhealthy_502_uses_unhealthy_backoff(self):
        """Unhealthy cluster + 502 → wait function returns unhealthy_backoff."""
        cfg = RetryConfig(
            max_retries=1,
            initial_backoff=0.0,
            max_backoff=0.0,
            multiplier=1.0,
            unhealthy_backoff=5.0,
        )
        client = _make_client(cfg)
        with patch_requests(
            "get", [_make_response(502), _make_response(200, b"{}")]
        ) as m:
            with mock.patch.object(
                client, "_check_cluster_health", return_value=False
            ) as health:
                with mock.patch("tenacity.nap.time.sleep") as sleeper:
                    client.get("/foo")
        assert m.call_count == 2
        assert health.call_count == 1
        sleeper.assert_called_once_with(5.0)

    def test_unhealthy_502_exhausts_retries(self):
        """If the cluster stays unhealthy, retries proceed until max_retries."""
        client = _make_client(_fast_retry(max_retries=2))
        with patch_requests("get", [_make_response(502)] * 3) as m:
            with mock.patch.object(client, "_check_cluster_health", return_value=False):
                with pytest.raises(FelderaAPIError) as exc_info:
                    client.get("/foo")
        assert exc_info.value.status_code == 502
        assert m.call_count == 3


class TestFelderaClientAcceptsRetryConfig:
    def test_passes_retry_config_through(self):
        from feldera.rest.feldera_client import FelderaClient

        rc = RetryConfig(max_retries=9, initial_backoff=0.1)
        # Skip the server-version handshake performed in __init__.
        with mock.patch.object(
            FelderaClient, "get_config", return_value=mock.Mock(version="x")
        ):
            client = FelderaClient(url="http://example.test", retry_config=rc)
        assert client.config.retry_config is rc
        assert client.http.config.retry_config is rc
