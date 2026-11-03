"""Tests for `Pipeline.wait_for_completion` with a transient UNAVAILABLE status.

Covers feldera#6986: the manager reports UNAVAILABLE after one failed status
probe, so `wait_for_completion` must wait through it instead of failing.
These mock the `Pipeline` accessors, without a running pipeline.
"""

import itertools
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

from feldera.enums import PipelineStatus
from feldera.pipeline import Pipeline
from feldera.rest.errors import (
    FelderaAPIError,
    FelderaCommunicationError,
    FelderaTimeoutError,
)


def _pipeline() -> Pipeline:
    pipeline = Pipeline(mock.Mock())
    pipeline._inner = SimpleNamespace(name="test_pipeline")
    return pipeline


def _stats(complete: bool):
    return SimpleNamespace(global_metrics=SimpleNamespace(pipeline_complete=complete))


def _api_error(status_code: int = 503) -> FelderaAPIError:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = b'{"error_code": "PipelineUnavailable", "message": "down"}'
    resp.headers["content-type"] = "application/json"
    prepared = requests.PreparedRequest()
    prepared.prepare(method="GET", url="http://example.test/v0/x")
    resp.request = prepared
    return FelderaAPIError("unavailable", resp)


def _run(pipeline, statuses, stats, clock=None, **kwargs):
    """Run `wait_for_completion` with the given `status()` and `stats()` results."""
    with (
        mock.patch.object(Pipeline, "status", side_effect=statuses),
        mock.patch.object(Pipeline, "stats", side_effect=stats) as stats_mock,
        mock.patch.object(
            Pipeline,
            "deployment_runtime_status_details",
            return_value={"reason": "probe timed out"},
        ),
        mock.patch("feldera.pipeline.time.sleep"),
        # The fake clock below is global, so the slow-wait warning could fire.
        mock.patch("feldera.pipeline.LongOperationWarning"),
        mock.patch(
            "feldera.pipeline.time.monotonic",
            side_effect=clock or itertools.count(0.0, 1.0),
        ),
    ):
        pipeline.wait_for_completion(**kwargs)
    return stats_mock


def test_unavailable_at_start_is_accepted():
    stats = _run(
        _pipeline(), [PipelineStatus.UNAVAILABLE], [_stats(False), _stats(True)]
    )
    assert stats.call_count == 2


def test_transient_unavailable_while_waiting_is_accepted():
    stats = _run(
        _pipeline(),
        [
            PipelineStatus.RUNNING,
            PipelineStatus.UNAVAILABLE,
            PipelineStatus.UNAVAILABLE,
        ],
        [_stats(False), _api_error(), _api_error(), _stats(True)],
    )
    assert stats.call_count == 4


def test_unavailable_for_too_long_raises_with_status():
    with pytest.raises(RuntimeError, match=r"status: UNAVAILABLE.*probe timed out"):
        _run(
            _pipeline(),
            itertools.chain(
                [PipelineStatus.RUNNING], itertools.repeat(PipelineStatus.UNAVAILABLE)
            ),
            itertools.repeat(_api_error()),
            clock=itertools.count(0.0, 10.0),
            unavailable_timeout_s=30,
        )


@pytest.mark.parametrize(
    "error",
    [FelderaTimeoutError("read timed out"), FelderaCommunicationError("reset")],
)
def test_client_error_while_unavailable_is_accepted(error):
    stats = _run(
        _pipeline(),
        [PipelineStatus.RUNNING, PipelineStatus.UNAVAILABLE],
        [_stats(False), error, _stats(True)],
    )
    assert stats.call_count == 3


def test_timeout_applies_while_unavailable():
    with pytest.raises(TimeoutError):
        _run(
            _pipeline(),
            itertools.chain(
                [PipelineStatus.RUNNING], itertools.repeat(PipelineStatus.UNAVAILABLE)
            ),
            itertools.chain([_stats(False)], itertools.repeat(_api_error())),
            clock=itertools.count(0.0, 10.0),
            timeout_s=25,
            unavailable_timeout_s=1000,
        )


def test_unavailable_streak_resets_after_success():
    """Two short outages must not add up to one long outage."""
    stats = _run(
        _pipeline(),
        [PipelineStatus.RUNNING] + [PipelineStatus.UNAVAILABLE] * 4,
        [
            _api_error(),
            _api_error(),
            _stats(False),
            _api_error(),
            _api_error(),
            _stats(True),
        ],
        clock=itertools.count(0.0, 10.0),
        unavailable_timeout_s=15,
    )
    assert stats.call_count == 6


def test_stopped_pipeline_raises_with_status():
    with pytest.raises(RuntimeError, match=r"status: STOPPED"):
        _run(_pipeline(), [PipelineStatus.STOPPED], [])


def test_stopped_while_waiting_raises_with_status():
    with pytest.raises(RuntimeError, match=r"status: STOPPING"):
        _run(
            _pipeline(),
            [PipelineStatus.RUNNING, PipelineStatus.STOPPING],
            [_stats(False), _api_error()],
        )


def test_stats_error_on_running_pipeline_propagates():
    with pytest.raises(FelderaAPIError):
        _run(
            _pipeline(),
            [PipelineStatus.RUNNING, PipelineStatus.RUNNING],
            [_api_error(500)],
        )
