"""Unit tests for the LLM client's transient-error retry."""

from __future__ import annotations

import types

import anthropic
import httpx
import pytest

from felderize import llm


def _make_client():
    """Build an AnthropicClient without constructing a real Anthropic SDK client."""
    client = llm.AnthropicClient.__new__(llm.AnthropicClient)
    client.model = "test-model"
    client.max_tokens = 16
    client.verbose = False
    return client


def _ok_message(text="OK"):
    usage = types.SimpleNamespace(input_tokens=1, output_tokens=1)
    return types.SimpleNamespace(
        usage=usage, content=[types.SimpleNamespace(text=text)]
    )


class _FakeStream:
    def __init__(self, behavior):
        self._behavior = behavior

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get_final_message(self):
        return self._behavior()


def test_retries_on_transient_connection_error(monkeypatch):
    monkeypatch.setattr(llm.time, "sleep", lambda s: None)  # no real backoff
    calls = {"n": 0}

    def behavior():
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.RemoteProtocolError("peer closed connection")
        return _ok_message("recovered")

    client = _make_client()
    client.client = types.SimpleNamespace(
        messages=types.SimpleNamespace(stream=lambda **kw: _FakeStream(behavior))
    )

    out = client.translate("sys", "user")
    assert out == "recovered"
    assert calls["n"] == 2  # failed once, succeeded on retry


def test_gives_up_after_max_retries(monkeypatch):
    monkeypatch.setattr(llm.time, "sleep", lambda s: None)

    def behavior():
        raise httpx.ConnectError("refused")

    client = _make_client()
    client.client = types.SimpleNamespace(
        messages=types.SimpleNamespace(stream=lambda **kw: _FakeStream(behavior))
    )

    with pytest.raises(httpx.ConnectError):
        client.translate("sys", "user")


# ---------------------------------------------------------------------------
# Context-window overflow handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message, expected",
    [
        ("prompt is too long: 250000 tokens > 200000 maximum", True),
        ("input length exceeds the context window", True),
        ("requested 300000 tokens exceeds the maximum allowed", True),
        ("invalid model: claude-bogus", False),
        ("credit balance is too low", False),
    ],
)
def test_is_context_length_error_classification(message, expected):
    err = _bad_request(message)
    assert llm._is_context_length_error(err) is expected


def _bad_request(message):
    response = httpx.Response(400, request=httpx.Request("POST", "http://test"))
    return anthropic.BadRequestError(message, response=response, body=None)


def test_translate_raises_prompt_too_large_on_overflow():
    def behavior():
        raise _bad_request("prompt is too long: 250000 tokens > 200000 maximum")

    client = _make_client()
    client.client = types.SimpleNamespace(
        messages=types.SimpleNamespace(stream=lambda **kw: _FakeStream(behavior))
    )

    with pytest.raises(llm.PromptTooLargeError):
        client.translate("sys", "user")


def test_translate_propagates_other_bad_requests():
    def behavior():
        raise _bad_request("invalid model: claude-bogus")

    client = _make_client()
    client.client = types.SimpleNamespace(
        messages=types.SimpleNamespace(stream=lambda **kw: _FakeStream(behavior))
    )

    with pytest.raises(anthropic.BadRequestError):
        client.translate("sys", "user")


# ---------------------------------------------------------------------------
# Rate-limit retry + client construction
# ---------------------------------------------------------------------------


def _rate_limit_error():
    response = httpx.Response(429, request=httpx.Request("POST", "http://test"))
    return anthropic.RateLimitError("slow down", response=response, body=None)


def test_retries_on_rate_limit_then_succeeds(monkeypatch):
    monkeypatch.setattr(llm.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def behavior():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _rate_limit_error()
        return _ok_message("recovered")

    client = _make_client()
    client.client = types.SimpleNamespace(
        messages=types.SimpleNamespace(stream=lambda **kw: _FakeStream(behavior))
    )

    assert client.translate("sys", "user") == "recovered"
    assert calls["n"] == 2


def test_create_client_builds_anthropic_client():
    from felderize.config import Config

    config = Config(model="m", api_key="k", feldera_compiler=None)
    client = llm.create_client(config)
    assert isinstance(client, llm.AnthropicClient)
    assert client.model == "m"
    assert client.max_tokens == config.max_tokens
