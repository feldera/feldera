"""Tests for the tenacity-based retry behavior in HttpRequests."""

from __future__ import annotations

import dataclasses
import time
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
        assert cfg.deadline_seconds is None
        assert cfg.initial_backoff == 2.0
        assert cfg.max_backoff == 64.0
        assert cfg.multiplier == 2.0
        assert cfg.jitter == 0.0
        assert cfg.unhealthy_backoff == 90.0
        assert cfg.expired_token_wait_seconds == 0.0
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
        with pytest.raises(ValueError):
            RetryConfig(deadline_seconds=0.0)
        with pytest.raises(ValueError):
            RetryConfig(deadline_seconds=-1.0)
        with pytest.raises(ValueError):
            RetryConfig(expired_token_wait_seconds=-1.0)

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

    def test_get_connection_error_then_success(self):
        # GET is idempotent — a connection reset mid-poll is safe to retry.
        client = _make_client()
        with patch_requests(
            "get",
            [requests.exceptions.ConnectionError("reset"), _make_response(200, b"{}")],
        ) as m:
            client.get("/foo")
        assert m.call_count == 2

    def test_get_connection_error_exhausts_raises_wrapped(self):
        client = _make_client(_fast_retry(max_retries=1))
        with patch_requests(
            "get", [requests.exceptions.ConnectionError("down")] * 2
        ) as m:
            with pytest.raises(FelderaCommunicationError):
                client.get("/foo")
        assert m.call_count == 2

    def test_post_connection_error_is_not_retried(self):
        # POST isn't idempotent — a lost response may hide an applied write,
        # so retrying could resubmit it. The first raise must propagate.
        client = _make_client()
        with patch_requests("post", [requests.exceptions.ConnectionError("down")]) as m:
            with pytest.raises(FelderaCommunicationError):
                client.post("/foo")
        assert m.call_count == 1

    def test_post_marked_idempotent_retries_connection_error(self):
        client = _make_client()
        with patch_requests(
            "post",
            [requests.exceptions.ConnectionError("reset"), _make_response(200, b"{}")],
        ) as m:
            client.post("/foo", idempotent=True)
        assert m.call_count == 2

    def test_get_marked_not_idempotent_skips_connection_error_retry(self):
        client = _make_client()
        with patch_requests("get", [requests.exceptions.ConnectionError("down")]) as m:
            with pytest.raises(FelderaCommunicationError):
                client.send_request(requests.get, "/foo", idempotent=False)
        assert m.call_count == 1

    def test_idempotent_delete_treats_404_after_retry_as_success(self):
        # First attempt drops the connection after the server applied the
        # DELETE; the retry finds the resource gone. The postcondition holds.
        client = _make_client()
        with patch_requests(
            "delete",
            [
                requests.exceptions.ConnectionError("reset"),
                _make_response(404, b'{"error":"not found"}'),
            ],
        ) as m:
            assert client.delete("/foo", idempotent=True) is None
        assert m.call_count == 2

    def test_idempotent_delete_first_attempt_404_still_raises(self):
        # No retry happened, so the resource genuinely did not exist.
        client = _make_client()
        with patch_requests(
            "delete", [_make_response(404, b'{"error":"not found"}')]
        ) as m:
            with pytest.raises(FelderaAPIError) as exc_info:
                client.delete("/foo", idempotent=True)
        assert exc_info.value.status_code == 404
        assert m.call_count == 1

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


class TestRetryDeadline:
    def test_deadline_lifts_attempt_cap(self):
        # max_retries=1 alone would stop after 2 calls; the deadline keeps
        # the client retrying until it succeeds.
        client = _make_client(_fast_retry(max_retries=1, deadline_seconds=60.0))
        responses = [_make_response(503)] * 5 + [_make_response(200, b"{}")]
        with patch_requests("get", responses) as m:
            assert client.get("/foo") == {}
        assert m.call_count == 6

    def test_deadline_expiry_stops_retrying(self):
        cfg = RetryConfig(
            max_retries=0,
            initial_backoff=0.05,
            max_backoff=0.05,
            multiplier=1.0,
            deadline_seconds=0.12,
        )
        client = _make_client(cfg)
        with patch_requests("get", [_make_response(503)] * 100) as m:
            with pytest.raises(FelderaAPIError) as exc_info:
                client.get("/foo")
        assert exc_info.value.status_code == 503
        # Attempts run at ~0, 0.05, 0.10, 0.15 seconds; the budget expires
        # after at most four. Bounds are loose to tolerate scheduling delay.
        assert 2 <= m.call_count <= 5


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


class TestClientMarksIdempotentEndpoints:
    @staticmethod
    def _client_with_mock_http():
        from feldera.rest.feldera_client import FelderaClient

        # Skip the server-version handshake performed in __init__.
        with mock.patch.object(
            FelderaClient, "get_config", return_value=mock.Mock(version="x")
        ):
            client = FelderaClient(url="http://example.test")
        client.http = mock.Mock()
        return client

    def test_stop_pipeline_posts_idempotent(self):
        client = self._client_with_mock_http()
        client.stop_pipeline("p", force=True, wait=False)
        assert client.http.post.call_args.kwargs["idempotent"] is True

    def test_pause_pipeline_posts_idempotent(self):
        client = self._client_with_mock_http()
        client.pause_pipeline("p", wait=False)
        assert client.http.post.call_args.kwargs["idempotent"] is True

    def test_delete_pipeline_deletes_idempotent(self):
        client = self._client_with_mock_http()
        client.delete_pipeline("p")
        assert client.http.delete.call_args.kwargs["idempotent"] is True


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


def _jwt(exp: float) -> str:
    import base64
    import json

    def segment(value: object) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
        )

    return f"{segment({'alg': 'RS256'})}.{segment({'exp': exp})}.c2ln"


class _TokenFile:
    """A credential a background refresher replaces, as `fda` and CI see it.

    `refresh_at` models the refresher landing that many seconds into the
    client's wait; without it the token never changes, as when the refresher
    has died.
    """

    def __init__(self, token: str, clock=None, refresh_at=None, replacement=None):
        self.token = token
        self.clock = clock
        self.refresh_at = refresh_at
        self.replacement = replacement
        self.reads = 0

    def __call__(self) -> str:
        self.reads += 1
        if self.refresh_at is not None and self.clock() >= self.refresh_at:
            self.token = self.replacement
        return self.token


@contextmanager
def _instant_sleep():
    """Run the expired-token wait on a clock that costs no wall time.

    `_monotonic` and `_sleep` are the module's own names for the clock, so the
    fake stays inside the code under test; patching `time.sleep` would replace
    it for tenacity and every other caller in the process too.
    """
    now = [0.0]
    with mock.patch("feldera.rest._httprequests._monotonic", lambda: now[0]):
        with mock.patch(
            "feldera.rest._httprequests._sleep",
            lambda seconds: now.__setitem__(0, now[0] + seconds),
        ):
            yield now


class TestExpiredBearerWaitsForItsRefresher:
    """401 handling for a callable credential.

    https://github.com/feldera/feldera/issues/7048: a refresher that missed a
    cycle left the token file holding an expired token, and re-resolving it
    immediately just presented the same dead token again.
    """

    @staticmethod
    def _client(**overrides) -> tuple[HttpRequests, _TokenFile]:
        credential = _TokenFile(_jwt(exp=-1_000))  # long expired
        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(max_retries=0, **overrides),
        )
        return HttpRequests(cfg), credential

    def test_waits_for_the_refresher_and_retries_with_the_new_token(self):
        expired, fresh = _jwt(exp=-1_000), _jwt(exp=2**31)
        with _instant_sleep() as now:
            credential = _TokenFile(
                expired, clock=lambda: now[0], refresh_at=20.0, replacement=fresh
            )
            cfg = Config(
                url="http://example.test",
                api_key=credential,
                retry_config=_fast_retry(
                    max_retries=0, expired_token_wait_seconds=150.0
                ),
            )
            sent = []

            def record(*args, **kwargs):
                sent.append(kwargs["headers"]["Authorization"])
                return _make_response(401 if len(sent) == 1 else 200)

            with patch_requests("get", []) as m:
                m.side_effect = record
                HttpRequests(cfg).get("/foo")

        assert sent == [f"Bearer {expired}", f"Bearer {fresh}"]
        # It waited for the refresher rather than replaying the dead token.
        assert 20.0 <= now[0] < 20.0 + 5.0

    def test_gives_up_after_the_budget_and_reports_the_instance_failure(self):
        client, _ = self._client(expired_token_wait_seconds=30.0)
        with patch_requests("get", [_make_response(401)] * 2) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError) as exc_info:
                    client.get("/foo")
        assert exc_info.value.status_code == 401
        assert m.call_count == 2
        # The wait is bounded by the budget, not by the refresher showing up.
        assert 30.0 <= now[0] < 30.0 + 5.0

    def test_a_credential_that_is_merely_refused_does_not_wait(self):
        """A live token the instance rejects is a trust problem, not a race.

        Waiting on it would turn a misconfigured trust or a wrong API key into
        a hang on every request.
        """
        credential = _TokenFile(_jwt(exp=2**31))  # valid for decades
        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(max_retries=0, expired_token_wait_seconds=150.0),
        )
        client = HttpRequests(cfg)
        with patch_requests("get", [_make_response(401)] * 2) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError):
                    client.get("/foo")
        assert m.call_count == 2, "expected exactly one re-resolved retry"
        assert now[0] == 0.0, "waited on a token that had not expired"

    def test_an_opaque_credential_does_not_wait(self):
        credential = _TokenFile("not-a-jwt-at-all")
        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(max_retries=0, expired_token_wait_seconds=150.0),
        )
        client = HttpRequests(cfg)
        with patch_requests("get", [_make_response(401)] * 2):
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError):
                    client.get("/foo")
        assert now[0] == 0.0

    def test_without_a_budget_the_behaviour_is_one_immediate_retry(self):
        client, _ = self._client()  # expired_token_wait_seconds defaults to 0
        with patch_requests("get", [_make_response(401), _make_response(200)]) as m:
            with _instant_sleep() as now:
                client.get("/foo")
        assert m.call_count == 2
        assert now[0] == 0.0

    def test_a_token_replaced_mid_flight_retries_without_waiting(self):
        """The refresher can land between building a request and its rejection."""
        client, credential = self._client(expired_token_wait_seconds=150.0)
        fresh = _jwt(exp=2**31)

        def record(*args, **kwargs):
            if kwargs["headers"]["Authorization"].endswith(fresh):
                return _make_response(200)
            credential.token = fresh
            return _make_response(401)

        with patch_requests("get", []) as m:
            m.side_effect = record
            with _instant_sleep() as now:
                client.get("/foo")
        assert m.call_count == 2
        assert now[0] == 0.0, "waited although the credential had already changed"

    def test_a_second_expired_token_does_not_end_the_wait(self):
        """A refresher can mint from a skewed clock, or half-write its file.

        The single retry is worth more than the first token that merely
        differs from the dead one.
        """
        first, second, fresh = _jwt(exp=-1_000), _jwt(exp=-900), _jwt(exp=2**31)
        with _instant_sleep() as now:

            def credential() -> str:
                if now[0] >= 40.0:
                    return fresh
                return second if now[0] >= 5.0 else first

            cfg = Config(
                url="http://example.test",
                api_key=credential,
                retry_config=_fast_retry(
                    max_retries=0, expired_token_wait_seconds=150.0
                ),
            )
            sent = []

            def record(*args, **kwargs):
                sent.append(kwargs["headers"]["Authorization"])
                return _make_response(401 if len(sent) == 1 else 200)

            with patch_requests("get", []) as m:
                m.side_effect = record
                HttpRequests(cfg).get("/foo")

        assert sent == [f"Bearer {first}", f"Bearer {fresh}"]
        assert now[0] == 40.0, "the replacement dead token ended the wait"

    @pytest.mark.parametrize("status", [400, 403, 404, 500])
    def test_only_401_enters_the_wait_path(self, status: int):
        """Every other refusal is reported as-is, expired bearer or not."""
        client, credential = self._client(expired_token_wait_seconds=150.0)
        with patch_requests("get", [_make_response(status)]) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError) as exc_info:
                    client.get("/foo")
        assert exc_info.value.status_code == status
        assert m.call_count == 1
        assert now[0] == 0.0
        assert credential.reads == 1, "re-resolved the credential for a non-401"

    def test_a_clock_ahead_of_the_idp_costs_delay_but_not_a_hang(self):
        """`exp` is read against the local clock, with no allowance for skew.

        A host running ahead of the issuer reads a still-live token as expired
        and spends the budget waiting for a refresh that was never due. The
        cost is bounded by the budget, so the caller still gets the instance's
        answer rather than a hang.
        """
        credential = _TokenFile(_jwt(exp=time.time() - 1))  # expired by a hair
        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(max_retries=0, expired_token_wait_seconds=30.0),
        )
        with patch_requests("get", [_make_response(401)] * 2) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError) as exc_info:
                    HttpRequests(cfg).get("/foo")
        assert exc_info.value.status_code == 401
        assert m.call_count == 2
        assert now[0] == 30.0

    def test_the_expiry_comes_from_the_token_the_retry_will_send(self):
        """The wait is decided by the headers, never by a second resolve.

        A refresh landing between building the retry headers and reading the
        expiry would otherwise clear the wait while the retry still carried
        the expired token.
        """
        expired, fresh = _jwt(exp=-1_000), _jwt(exp=2**31)
        reads = [0]

        def credential() -> str:
            # The original request and the retry headers both read the expired
            # token; only a third resolve sees the replacement.
            reads[0] += 1
            return expired if reads[0] <= 2 else fresh

        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(max_retries=0, expired_token_wait_seconds=150.0),
        )
        sent = []

        def record(*args, **kwargs):
            auth = kwargs["headers"]["Authorization"]
            sent.append(auth)
            return _make_response(200 if auth.endswith(fresh) else 401)

        with patch_requests("get", []) as m:
            m.side_effect = record
            with _instant_sleep() as now:
                HttpRequests(cfg).get("/foo")

        assert sent == [f"Bearer {expired}", f"Bearer {fresh}"]
        assert reads[0] == 3, "resolved the credential more than the flow needs"
        assert now[0] == 5.0, "the replacement arrived on the first poll"

    def test_the_fake_clock_does_not_reach_tenacity(self):
        """The wait is measured on this module's clock alone.

        Patching `time.sleep` process-wide instead would fold tenacity's own
        backoff into `now`, and every measurement here would quietly drift the
        moment a case was given a real backoff.
        """
        credential = _TokenFile(_jwt(exp=-1_000))
        cfg = Config(
            url="http://example.test",
            api_key=credential,
            retry_config=_fast_retry(
                max_retries=1,
                initial_backoff=0.2,
                max_backoff=0.2,
                expired_token_wait_seconds=30.0,
            ),
        )
        # 503 costs one real 0.2s tenacity backoff before the 401s arrive.
        responses = [_make_response(503)] + [_make_response(401)] * 2
        with patch_requests("get", responses) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError):
                    HttpRequests(cfg).get("/foo")
        assert m.call_count == 3
        assert now[0] == 30.0, "tenacity's backoff leaked into the fake clock"

    def test_a_static_credential_still_fails_at_once(self):
        cfg = Config(
            url="http://example.test",
            api_key=_jwt(exp=-1_000),
            retry_config=_fast_retry(max_retries=0, expired_token_wait_seconds=150.0),
        )
        client = HttpRequests(cfg)
        with patch_requests("get", [_make_response(401)]) as m:
            with _instant_sleep() as now:
                with pytest.raises(FelderaAPIError):
                    client.get("/foo")
        # Nothing refreshes a string, so there is nothing to wait for.
        assert m.call_count == 1
        assert now[0] == 0.0
