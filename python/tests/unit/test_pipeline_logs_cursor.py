"""Tests for reading the pipeline logs stream from a cursor."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Optional
from unittest import mock

import pytest
import requests

from feldera.rest.errors import FelderaAPIError, FelderaError
from feldera.rest.feldera_client import FelderaClient
from feldera.rest.logs import LogPosition
from feldera.pipeline import Pipeline
from feldera.rest.retry import RetryConfig

_CONFIG_BODY = b'{"version": "0.0.0", "edition": "Open source"}'

_POSITION_HEADERS = {
    "feldera-logs-epoch": "0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08",
    "feldera-logs-seq": "41272",
    "feldera-logs-gap": "0",
}

EPOCH = _POSITION_HEADERS["feldera-logs-epoch"]


def _make_response(
    status_code: int,
    body: bytes = b"{}",
    content_type: str = "application/json",
    headers: Optional[dict] = None,
) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = body
    # Iterating a response reads from `raw`, which a handmade response has none of.
    # Marking the content consumed makes `iter_lines` replay `_content` instead.
    resp._content_consumed = True
    resp.headers["content-type"] = content_type
    for key, value in (headers or {}).items():
        resp.headers[key] = value
    prepared = requests.PreparedRequest()
    prepared.prepare(method="GET", url="http://example.test/v0/x")
    resp.request = prepared
    return resp


def _logs_response(body: bytes, headers: Optional[dict] = _POSITION_HEADERS):
    return _make_response(200, body, content_type="text/plain", headers=headers)


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
def _patch_get(responses: Iterable[object]):
    """Patch `requests.get`, which `HttpRequests` dispatches on by name."""
    with mock.patch("requests.get") as m:
        m.__name__ = "get"
        m.side_effect = _sequence(responses)
        yield m


def _make_client(responses: Iterable[object]):
    """A client whose construction consumes one response, the server config."""
    return _patch_get([_make_response(200, _CONFIG_BODY), *responses])


# Near-zero backoff so a retry test finishes quickly.
_FAST_RETRY = RetryConfig(
    max_retries=3, initial_backoff=0.0, max_backoff=0.0, multiplier=1.0
)


def _client() -> FelderaClient:
    return FelderaClient(url="http://example.test", retry_config=_FAST_RETRY)


class TestLogPosition:
    def test_cursor_counts_the_lines_read(self):
        position = LogPosition(epoch=EPOCH, seq=7, gap=0)
        assert position.cursor() == f"{EPOCH}:7"
        assert position.cursor(3) == f"{EPOCH}:10"

    def test_from_headers_reads_all_three(self):
        assert LogPosition.from_headers(_POSITION_HEADERS) == LogPosition(
            epoch=EPOCH, seq=41272, gap=0
        )

    @pytest.mark.parametrize("dropped", list(_POSITION_HEADERS))
    def test_partial_position_is_an_error(self, dropped):
        headers = {k: v for k, v in _POSITION_HEADERS.items() if k != dropped}
        with pytest.raises(FelderaError) as err:
            LogPosition.from_headers(headers)
        assert dropped in str(err.value)


class TestResumePipelineLogs:
    def test_first_connection_asks_for_the_whole_buffer(self):
        with _make_client([_logs_response(b"a\nb\n")]) as m:
            stream = _client().resume_pipeline_logs("p")
        # Empty, not absent: the parameter is what asks to be told the position.
        assert m.call_args.kwargs["params"] == {"cursor": ""}
        assert stream.position == LogPosition(epoch=EPOCH, seq=41272, gap=0)
        assert list(stream) == ["a", "b"]

    def test_cursor_is_sent_as_given(self):
        cursor = f"{EPOCH}:41272"
        with _make_client([_logs_response(b"c\n")]) as m:
            _client().resume_pipeline_logs("p", cursor)
        assert m.call_args.kwargs["params"] == {"cursor": cursor}

    def test_position_of_a_resume_that_lost_lines(self):
        headers = {
            **_POSITION_HEADERS,
            "feldera-logs-seq": "9",
            "feldera-logs-gap": "4",
        }
        with _make_client([_logs_response(b"c\n", headers)]):
            stream = _client().resume_pipeline_logs("p", f"{EPOCH}:5")
        assert (stream.position.seq, stream.position.gap) == (9, 4)
        # The next cursor follows the lines delivered, not the lines asked for.
        assert stream.position.cursor(1) == f"{EPOCH}:10"

    def test_transient_failure_is_retried(self):
        with _make_client([_make_response(503), _logs_response(b"a\n")]) as m:
            stream = _client().resume_pipeline_logs("p")
        assert list(stream) == ["a"]
        assert m.call_count == 3  # config, 503, retry

    def test_rejected_cursor_is_not_retried(self):
        with _make_client([_make_response(400, b'{"message": "malformed"}')]) as m:
            with pytest.raises(FelderaAPIError) as err:
                _client().resume_pipeline_logs("p", "nonsense")
        assert err.value.status_code == 400
        assert m.call_count == 2  # config, the rejected request

    def test_response_without_a_position_is_an_error(self):
        with _make_client([_logs_response(b"a\n", headers=None)]):
            with pytest.raises(FelderaError):
                _client().resume_pipeline_logs("p")

    def test_closing_releases_the_response(self):
        response = _logs_response(b"a\n")
        with _make_client([response]):
            with mock.patch.object(response, "close") as closed:
                with _client().resume_pipeline_logs("p") as stream:
                    assert stream.position.epoch == EPOCH
                closed.assert_called_once()

    def test_pipeline_resumes_through_the_client(self):
        # `Pipeline` is the API most callers reach for, so the cursor has to survive the
        # trip through it, not only through `FelderaClient`.
        with _make_client([_logs_response(b"a\n")]) as m:
            pipeline = Pipeline(_client())
            pipeline._inner = mock.Mock(name="inner")
            pipeline._inner.name = "p"
            stream = pipeline.resume_logs(f"{EPOCH}:41272")
        assert m.call_args.kwargs["params"] == {"cursor": f"{EPOCH}:41272"}
        assert m.call_args.args[0].endswith("/pipelines/p/logs")
        assert stream.position.epoch == EPOCH

    def test_every_newline_yields_a_line(self):
        # The cursor is derived by counting, so the count has to match the server's line
        # for line. Blank lines carry a sequence number, and a `\r` inside a line does not.
        body = b"a\n\nb\rc\n\n\n"
        with _make_client([_logs_response(body)]):
            stream = _client().resume_pipeline_logs("p")
        lines = list(stream)
        assert lines == ["a", "", "b\rc", "", ""]
        assert len(lines) == body.count(b"\n")

    def test_a_line_cut_short_is_not_counted(self):
        # The connection dropped mid-line. Counting the fragment would advance the cursor
        # past a line that was never delivered whole.
        with _make_client([_logs_response(b"a\nb")]):
            stream = _client().resume_pipeline_logs("p")
        assert list(stream) == ["a"]

    def test_lines_split_across_chunks_are_rejoined(self):
        response = _logs_response(b"")
        with _make_client([response]):
            stream = _client().resume_pipeline_logs("p")
        with mock.patch.object(
            response, "iter_content", return_value=iter([b"ab", b"c\nde", b"f\n"])
        ):
            assert list(stream) == ["abc", "def"]

    def test_legacy_stream_sends_no_cursor(self):
        # A caller that omits the cursor keeps the stream it has always received, which
        # carries no position and is free to open with a notice rather than a log line.
        with _make_client([_logs_response(b"a\nb\n", headers=None)]) as m:
            assert list(_client().get_pipeline_logs("p")) == ["a", "b"]
        assert m.call_args.kwargs["params"] is None
