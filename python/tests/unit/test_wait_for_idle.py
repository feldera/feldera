"""Tests for Pipeline.wait_for_idle(), which polls `/stats` until the input and
processed record counts stop changing and agree.

A paused pipeline holds the input that its connectors buffered until it
resumes, so for a paused pipeline, that input counts as processed.  The
exception is HTTP input connectors, which a paused pipeline still polls.
"""

from types import SimpleNamespace

import pytest

from feldera.enums import PipelineStatus
from feldera.pipeline import Pipeline


def endpoint(name, buffered):
    return SimpleNamespace(
        endpoint_name=name, metrics=SimpleNamespace(buffered_records=buffered)
    )


def pipeline_with_stats(state, total_input, total_processed, buffered, http=0):
    """`buffered` records wait in a Kafka connector, `http` in an HTTP one."""
    pipeline = Pipeline.__new__(Pipeline)
    metrics = SimpleNamespace(
        state=state,
        total_input_records=total_input,
        total_processed_records=total_processed,
        buffered_input_records=buffered + http,
    )
    inputs = [
        endpoint("t.kafka", buffered),
        endpoint("t.api-ingress-0123456789abcdef", http),
    ]
    pipeline.stats = lambda: SimpleNamespace(global_metrics=metrics, inputs=inputs)
    return pipeline


def wait(pipeline):
    pipeline.wait_for_idle(idle_interval_s=0.05, timeout_s=0.5, poll_interval_s=0.01)


def test_paused_pipeline_with_held_input_is_idle():
    wait(pipeline_with_stats(PipelineStatus.PAUSED, 10, 7, 3))


def test_running_pipeline_with_buffered_input_is_not_idle():
    with pytest.raises(RuntimeError):
        wait(pipeline_with_stats(PipelineStatus.RUNNING, 10, 7, 3))


def test_paused_pipeline_with_input_in_flight_is_not_idle():
    # 10 received = 7 processed + 2 buffered + 1 in the circuit.
    with pytest.raises(RuntimeError):
        wait(pipeline_with_stats(PipelineStatus.PAUSED, 10, 7, 2))


def test_paused_pipeline_with_buffered_http_input_is_not_idle():
    # A paused pipeline still processes forced HTTP input, so wait for it.
    with pytest.raises(RuntimeError):
        wait(pipeline_with_stats(PipelineStatus.PAUSED, 10, 7, 2, http=1))
