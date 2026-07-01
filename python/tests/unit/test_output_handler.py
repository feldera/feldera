"""Tests for the change-stream listener (`OutputHandler` / `CallbackRunner`).

These tests exercise the connection phase of `Pipeline.listen()` and
`Pipeline.foreach_chunk()` against a mocked REST client. In particular, they
pin down the regression where a failure to establish the egress connection
left the caller blocked forever on an event that no one would ever set
(observed as a 1-hour `pytest` timeout in CI inside `pipeline.listen()`).
"""

from __future__ import annotations

from threading import Thread
from typing import Any, Callable, Iterable, Mapping, Optional
from unittest import mock

import pytest

from feldera.enums import PipelineStatus
from feldera.output_handler import OutputHandler
from feldera.pipeline import Pipeline
from feldera.rest.errors import FelderaTimeoutError
from feldera.rest.sql_table import SQLTable

# Generous bound on calls that must complete quickly; reached only when the
# code under test hangs, which is precisely the regression being tested.
WATCHDOG_TIMEOUT_S = 60.0

ID_FIELD = {
    "name": "id",
    "case_sensitive": False,
    "columntype": {"type": "INTEGER", "nullable": False},
}

HEARTBEAT_CHUNK: Mapping[str, Any] = {"sequence_number": 0, "snapshot": False}

DATA_CHUNK: Mapping[str, Any] = {
    "sequence_number": 1,
    "snapshot": False,
    "json_data": [{"insert": {"id": 1}}, {"insert": {"id": 2}}],
}


def _make_client(
    listen_side_effect: Optional[BaseException] = None,
    chunks: Optional[Iterable[Mapping[str, Any]]] = None,
) -> mock.Mock:
    """
    Build a mock `FelderaClient` whose `listen_to_pipeline` either raises
    `listen_side_effect` or returns a generator factory yielding `chunks`.
    """

    inner = mock.Mock()
    inner.name = "test_pipeline"
    inner.tables = [SQLTable("t1", fields=[ID_FIELD])]
    inner.views = []

    client = mock.Mock()
    client.get_pipeline.return_value = inner

    if listen_side_effect is not None:
        client.listen_to_pipeline.side_effect = listen_side_effect
    else:

        def factory():
            yield from chunks or []

        client.listen_to_pipeline.return_value = factory

    return client


def _call_with_watchdog(fn: Callable[[], Any]) -> Mapping[str, Any]:
    """
    Run `fn` on a daemon thread and fail the test if it does not finish in
    time, so that a hang in the code under test cannot hang the test suite.

    Returns {"returned": value} or {"raised": exception}.
    """

    outcome: dict[str, Any] = {}

    def target():
        try:
            outcome["returned"] = fn()
        except BaseException as e:
            outcome["raised"] = e

    thread = Thread(target=target, daemon=True)
    thread.start()
    thread.join(WATCHDOG_TIMEOUT_S)
    if thread.is_alive():
        pytest.fail("call did not complete: the listener is hanging")
    return outcome


def test_start_raises_when_connection_fails():
    """
    An egress request that fails before the first chunk arrives must raise
    from `start()`, not leave the caller blocked forever.
    """

    error = FelderaTimeoutError("egress request timed out")
    client = _make_client(listen_side_effect=error)
    handler = OutputHandler(client, "test_pipeline", "t1")

    outcome = _call_with_watchdog(handler.start)

    assert outcome.get("raised") is error


def test_start_raises_when_first_read_fails():
    """A connection that dies during the first body read must also raise."""

    error = ConnectionError("connection reset by peer")

    def factory():
        raise error
        yield  # pragma: no cover -- makes `factory` a generator

    inner_client = _make_client(chunks=[])
    inner_client.listen_to_pipeline.return_value = factory
    handler = OutputHandler(inner_client, "test_pipeline", "t1")

    outcome = _call_with_watchdog(handler.start)

    assert outcome.get("raised") is error


def test_start_returns_when_stream_ends_before_first_chunk():
    """
    A stream that closes cleanly before the first chunk (e.g., the pipeline
    stopped concurrently) is not an error: `start()` returns and the listener
    observes no output.
    """

    client = _make_client(chunks=[])
    handler = OutputHandler(client, "test_pipeline", "t1")

    outcome = _call_with_watchdog(handler.start)

    assert "raised" not in outcome
    assert handler.to_dict() == []


def test_start_unblocks_on_heartbeat_and_buffers_data():
    """
    The first chunk is normally an empty heartbeat; it must unblock `start()`
    without producing output, and subsequent data chunks must be buffered.
    """

    client = _make_client(chunks=[HEARTBEAT_CHUNK, DATA_CHUNK])
    handler = OutputHandler(client, "test_pipeline", "t1")

    outcome = _call_with_watchdog(handler.start)
    assert "raised" not in outcome

    # The data chunk is processed asynchronously; wait for the runner thread
    # to drain the (finite) stream before checking the buffer.
    handler.handler.join(WATCHDOG_TIMEOUT_S)
    assert not handler.handler.is_alive()

    assert handler.to_dict() == [
        {"id": 1, "insert_delete": 1},
        {"id": 2, "insert_delete": 1},
    ]


def test_foreach_chunk_raises_when_connection_fails():
    """`foreach_chunk` must propagate a failed egress connection, not hang."""

    error = FelderaTimeoutError("egress request timed out")
    client = _make_client(listen_side_effect=error)
    pipeline = Pipeline._from_inner(client.get_pipeline.return_value, client)

    with mock.patch.object(Pipeline, "status", return_value=PipelineStatus.RUNNING):
        outcome = _call_with_watchdog(
            lambda: pipeline.foreach_chunk("t1", lambda df, seq: None)
        )

    assert outcome.get("raised") is error
