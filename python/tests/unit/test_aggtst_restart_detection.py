"""The aggtest harness reports a pipeline restart instead of an empty view.

Covers feldera#6986 and feldera#7688: a pipeline pod that restarts after
ingestion answers every view query with an empty result. The harness must
name the restart, not the first view that looked wrong, and must still fail
when every view expected no rows.
"""

import uuid
from types import SimpleNamespace
from unittest import mock

import pytest

from tests.runtime_aggtest import aggtst_base
from tests.runtime_aggtest.aggtst_base import (
    PipelineRestartedError,
    RuntimeExecutionError,
    TstAccumulator,
    View,
    check_incarnation,
    raise_if_restarted,
)

INCARNATION_A = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000000")
INCARNATION_B = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000000")

VIEW_SQL = "CREATE MATERIALIZED VIEW v AS SELECT 1 AS x"


class FakePipeline:
    """A running pipeline whose stats() answers follow a script.

    `incarnations` lists one answer per stats() call; the last entry repeats.
    An Exception entry is raised instead of answered.
    """

    def __init__(self, incarnations, rows):
        self.name = "aggtest_pipeline"
        self._incarnations = list(incarnations)
        self._rows = rows

    def start(self):
        pass

    def stats(self):
        answer = self._incarnations[0]
        if len(self._incarnations) > 1:
            self._incarnations.pop(0)
        if isinstance(answer, Exception):
            raise answer
        metrics = SimpleNamespace(incarnation_uuid=answer, total_input_records=0)
        return SimpleNamespace(global_metrics=metrics)

    def input_json(self, *args, **kwargs):
        pass

    def wait_for_completion(self, **kwargs):
        pass

    def query_arrow_dicts(self, query):
        return iter(self._rows)

    def deployment_error(self):
        return None


def _run(pipeline: FakePipeline, views: list[View]):
    """Drive run_pipeline with a fake pipeline, no manager involved."""
    builder = mock.Mock()
    builder.create_or_replace.return_value = pipeline
    with (
        mock.patch.object(aggtst_base, "PipelineBuilder", return_value=builder),
        mock.patch.object(aggtst_base, "reclaim_pipeline", return_value=[]),
    ):
        TstAccumulator().run_pipeline("unit", VIEW_SQL, views, tables=[])


def test_helper_reports_the_restart_with_the_view_diff():
    cause = AssertionError("failed view: varchar_arg_min_gby")
    pipeline = FakePipeline([INCARNATION_B], rows=[])

    with pytest.raises(PipelineRestartedError) as info:
        raise_if_restarted(pipeline, INCARNATION_A, cause)

    message = str(info.value)
    assert str(INCARNATION_A) in message
    assert str(INCARNATION_B) in message
    assert "0 input records" in message
    assert "varchar_arg_min_gby" in message
    assert info.value.__cause__ is cause


def test_helper_keeps_quiet_without_a_restart():
    cause = AssertionError("failed view: varchar_arg_min_gby")

    raise_if_restarted(FakePipeline([INCARNATION_A], rows=[]), INCARNATION_A, cause)
    raise_if_restarted(FakePipeline([INCARNATION_B], rows=[]), None, cause)
    check_incarnation(FakePipeline([INCARNATION_A], rows=[]), INCARNATION_A)


def test_restart_reaches_the_caller_through_run_pipeline():
    pipeline = FakePipeline([INCARNATION_A, INCARNATION_B], rows=[])

    with pytest.raises(RuntimeExecutionError) as info:
        _run(pipeline, [View(VIEW_SQL, [{"x": 1}])])

    assert isinstance(info.value.__cause__, PipelineRestartedError)
    assert "restarted" in str(info.value)
    assert "failed view: v" in str(info.value)


def test_plain_mismatch_keeps_the_assertion():
    pipeline = FakePipeline([INCARNATION_A], rows=[])

    with pytest.raises(RuntimeExecutionError) as info:
        _run(pipeline, [View(VIEW_SQL, [{"x": 1}])])

    assert isinstance(info.value.__cause__, AssertionError)
    assert "restarted" not in str(info.value)
    assert "Received" in str(info.value)


def test_restart_fails_views_that_expect_no_rows():
    pipeline = FakePipeline([INCARNATION_A, INCARNATION_B], rows=[])

    with pytest.raises(RuntimeExecutionError) as info:
        _run(pipeline, [View(VIEW_SQL, [])])

    assert isinstance(info.value.__cause__, PipelineRestartedError)


def test_failed_probe_keeps_the_view_diff(capsys):
    pipeline = FakePipeline([INCARNATION_A, ConnectionError("pod is down")], rows=[])

    with pytest.raises(RuntimeExecutionError) as info:
        _run(pipeline, [View(VIEW_SQL, [{"x": 1}])])

    assert isinstance(info.value.__cause__, AssertionError)
    assert "Received" in str(info.value)
    assert "pod is down" in capsys.readouterr().out


def test_correct_views_pass():
    pipeline = FakePipeline([INCARNATION_A], rows=[{"x": 1}])

    _run(pipeline, [View(VIEW_SQL, [{"x": 1}])])
