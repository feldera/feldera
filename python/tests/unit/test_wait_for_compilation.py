"""Tests for FelderaClient._wait_for_compilation's program version check."""

from __future__ import annotations

from unittest import mock

import pytest

from feldera.rest.feldera_client import FelderaClient


def _client_returning(program_version: int) -> FelderaClient:
    # Skip the server-version handshake performed in __init__.
    with mock.patch.object(
        FelderaClient, "get_config", return_value=mock.Mock(version="x")
    ):
        client = FelderaClient(url="http://example.test")
    pipeline = mock.Mock(program_status="Success", program_version=program_version)
    client.get_pipeline = mock.Mock(return_value=pipeline)
    return client


def test_exact_version_is_accepted():
    _client_returning(2)._wait_for_compilation("p", expected_program_version=2)


def test_newer_version_is_accepted_with_warning(caplog):
    # One edit can give two bumps: the PATCH, then the compiler adopting the
    # pipeline under its own platform version (feldera/feldera#7748).
    _client_returning(3)._wait_for_compilation("p", expected_program_version=2)
    assert "newer than the expected version 2" in caplog.text


def test_older_version_raises():
    with pytest.raises(RuntimeError, match=r"\(1\) < expected .* \(2\)"):
        _client_returning(1)._wait_for_compilation("p", expected_program_version=2)
