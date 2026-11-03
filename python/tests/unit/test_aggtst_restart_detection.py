"""The aggtest harness reports a pipeline restart instead of an empty view.

Covers feldera#6986 and feldera#7688: a pipeline pod that restarts after
ingestion answers every view query with an empty result. The harness must
name the restart, not the first view that looked wrong.
"""

import uuid
from types import SimpleNamespace

import pytest

from tests.runtime_aggtest.aggtst_base import (
    PipelineRestartedError,
    raise_if_restarted,
)

INCARNATION_A = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000000")
INCARNATION_B = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000000")


def _pipeline(incarnation: uuid.UUID, total_input_records: int) -> SimpleNamespace:
    metrics = SimpleNamespace(
        incarnation_uuid=incarnation, total_input_records=total_input_records
    )
    return SimpleNamespace(
        name="aggtest_pipeline",
        stats=lambda: SimpleNamespace(global_metrics=metrics),
    )


def test_restart_replaces_the_view_mismatch():
    cause = AssertionError("failed view: varchar_arg_min_gby")

    with pytest.raises(PipelineRestartedError) as info:
        raise_if_restarted(_pipeline(INCARNATION_B, 0), INCARNATION_A, cause)

    message = str(info.value)
    assert str(INCARNATION_A) in message
    assert str(INCARNATION_B) in message
    assert "0 input records" in message
    assert "varchar_arg_min_gby" in message
    assert info.value.__cause__ is cause


def test_same_incarnation_keeps_the_view_mismatch():
    cause = AssertionError("failed view: varchar_arg_min_gby")

    raise_if_restarted(_pipeline(INCARNATION_A, 42), INCARNATION_A, cause)


def test_unknown_start_incarnation_keeps_the_view_mismatch():
    cause = AssertionError("failed view: varchar_arg_min_gby")

    raise_if_restarted(_pipeline(INCARNATION_B, 0), None, cause)
