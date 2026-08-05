"""Tests for the retry behavior when minting a GitHub Actions OIDC token."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from unittest import mock

import pytest

from feldera.testutils import _OIDC_MINT_MAX_RETRIES, _mint_github_oidc_token


class _FakeResponse:
    def __init__(self, value: str):
        self._body = json.dumps({"value": value}).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, *args, **kwargs):
        return self._body


def _http_error(request: urllib.request.Request, code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(request.full_url, code, "error", {}, None)


def _request() -> urllib.request.Request:
    return urllib.request.Request("https://example.test/token")


class TestOidcMintRetry:
    def test_retries_on_503_then_succeeds(self):
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            if len(calls) == 1:
                raise _http_error(request, 503)
            return _FakeResponse("minted-token")

        with (
            mock.patch("urllib.request.urlopen", side_effect=fake_urlopen),
            mock.patch("time.sleep"),
        ):
            token = _mint_github_oidc_token(_request())

        assert token == "minted-token"
        assert len(calls) == 2

    def test_does_not_retry_non_transient_status(self):
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            raise _http_error(request, 401)

        with (
            mock.patch("urllib.request.urlopen", side_effect=fake_urlopen),
            mock.patch("time.sleep"),
        ):
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                _mint_github_oidc_token(_request())

        assert exc_info.value.code == 401
        assert len(calls) == 1

    def test_exhausts_retries_and_raises(self):
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            raise _http_error(request, 503)

        with (
            mock.patch("urllib.request.urlopen", side_effect=fake_urlopen),
            mock.patch("time.sleep"),
        ):
            with pytest.raises(urllib.error.HTTPError):
                _mint_github_oidc_token(_request())

        assert len(calls) == _OIDC_MINT_MAX_RETRIES + 1

    def test_retries_on_connection_error(self):
        calls = []

        def fake_urlopen(request, timeout):
            calls.append(request)
            if len(calls) == 1:
                raise urllib.error.URLError("connection reset")
            return _FakeResponse("minted-token")

        with (
            mock.patch("urllib.request.urlopen", side_effect=fake_urlopen),
            mock.patch("time.sleep"),
        ):
            token = _mint_github_oidc_token(_request())

        assert token == "minted-token"
        assert len(calls) == 2
