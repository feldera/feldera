"""Tests for Pipeline.wait_for_completion's handling of fatal input errors."""

import pytest

from feldera.enums import PipelineStatus
from feldera.pipeline import Pipeline
from feldera.stats import PipelineStatistics

BASE_STATS = {
    "global_metrics": {
        "state": "running",
        "incarnation_uuid": "00000000-0000-0000-0000-000000000000",
        "start_time": 0,
        "transaction_status": "NoTransaction",
        "total_processed_records": 0,
        "total_input_records": 0,
        "pipeline_complete": False,
    },
    "inputs": [],
    "outputs": [],
}

INPUT_STATUS = {
    "endpoint_name": "t.input",
    "config": {},
    "metrics": {},
    "paused": False,
    "barrier": False,
}


def stats(complete: bool, fatal_error: str | None = None) -> PipelineStatistics:
    d = {
        **BASE_STATS,
        "global_metrics": {
            **BASE_STATS["global_metrics"],
            "pipeline_complete": complete,
        },
        "inputs": [{**INPUT_STATUS, "fatal_error": fatal_error}],
    }
    return PipelineStatistics.from_dict(d)


class FakePipeline(Pipeline):
    """A pipeline that replays a fixed sequence of `/stats` replies."""

    def __init__(self, replies):
        self.replies = list(replies)

    @property
    def name(self) -> str:
        return "test"

    def status(self) -> PipelineStatus:
        return PipelineStatus.RUNNING

    def stats(self) -> PipelineStatistics:
        # The last reply repeats, so a wait that should raise instead hangs to
        # its timeout -- the behaviour these tests guard against.
        return self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]


def test_fatal_input_error_raises():
    pipeline = FakePipeline([stats(complete=False, fatal_error="no field named meta")])
    with pytest.raises(RuntimeError, match="no field named meta"):
        pipeline.wait_for_completion(timeout_s=30)


def test_completion_wins_over_a_late_fatal_error():
    pipeline = FakePipeline([stats(complete=True, fatal_error="already reported")])
    pipeline.wait_for_completion(timeout_s=30)


def test_healthy_pipeline_waits_for_completion():
    pipeline = FakePipeline([stats(complete=False), stats(complete=True)])
    pipeline.wait_for_completion(timeout_s=30)
