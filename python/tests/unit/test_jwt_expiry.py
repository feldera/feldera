"""Tests for reading a bearer token's expiry without verifying it."""

from __future__ import annotations

import base64
import json

import pytest

from feldera.rest._jwt import seconds_since_expiry, token_expiry


def _jwt(payload: object) -> str:
    def segment(value: object) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
        )

    return f"{segment({'alg': 'RS256'})}.{segment(payload)}.c2ln"


def test_reads_exp():
    assert token_expiry(_jwt({"exp": 1_700_000_000})) == 1_700_000_000.0
    assert (
        token_expiry(_jwt({"exp": 1_700_000_000.5, "sub": "repo:x"})) == 1_700_000_000.5
    )


@pytest.mark.parametrize(
    "token",
    [
        pytest.param("opaque-api-key", id="not-a-jwt"),
        pytest.param("two.parts", id="too-few-segments"),
        pytest.param("a.b.c.d", id="too-many-segments"),
        pytest.param("head.!!!not-base64!!!.sig", id="undecodable-payload"),
        pytest.param("head.bm90IGpzb24.sig", id="payload-is-not-json"),
        pytest.param(_jwt(["exp", 1]), id="payload-is-not-an-object"),
        pytest.param(_jwt({"sub": "repo:x"}), id="no-exp"),
        pytest.param(_jwt({"exp": "soon"}), id="exp-is-not-a-number"),
        pytest.param(_jwt({"exp": True}), id="exp-is-a-bool"),
        pytest.param("", id="empty"),
        pytest.param(None, id="none"),
        pytest.param(b"head.eyJleHAiOjF9.sig", id="bytes"),
    ],
)
def test_states_nothing_rather_than_guessing(token):
    """An unreadable expiry must not read as an expired one.

    Everything here is a credential the client cannot date, and treating any
    of them as expired would make a wrong API key wait for a refresher that
    does not exist.
    """
    assert token_expiry(token) is None
    assert seconds_since_expiry(token) is None


def test_seconds_since_expiry_only_reports_the_past():
    token = _jwt({"exp": 1000})
    assert seconds_since_expiry(token, now=1120) == 120.0
    assert seconds_since_expiry(token, now=1000) is None
    assert seconds_since_expiry(token, now=999) is None


def test_padding_is_restored_for_every_payload_length():
    """Unpadded base64url is what a JWT carries, at every residue mod 4."""
    for filler in range(1, 12):
        token = _jwt({"exp": 1000, "pad": "x" * filler})
        assert token_expiry(token) == 1000.0
