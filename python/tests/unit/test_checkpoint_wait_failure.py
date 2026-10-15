"""Tests that `checkpoint(wait=True)`/`sync_checkpoint(wait=True)` raise when
the operation they wait for fails, instead of returning as if it succeeded.

These mock the `FelderaClient`, so they run without a pipeline.
"""

from types import SimpleNamespace
from unittest import mock

import pytest

from feldera.enums import CheckpointStatus
from feldera.pipeline import Pipeline

UUID = "11111111-0000-0000-0000-000000000000"


def _pipeline(client) -> Pipeline:
    pipeline = Pipeline(client)
    pipeline._inner = SimpleNamespace(name="test_pipeline")
    return pipeline


@pytest.fixture(autouse=True)
def _clear_failure_error():
    """`CheckpointStatus.Failure` is a single shared member that the status
    checks write the error onto; reset it so it can't leak between tests."""
    yield
    CheckpointStatus.Failure.error = None


def test_checkpoint_wait_raises_on_failure():
    client = mock.Mock()
    client.checkpoint_pipeline_response.return_value = {"checkpoint_sequence_number": 3}
    client.checkpoint_pipeline_status.side_effect = [
        {"success": 2, "failure": None},
        {"success": 2, "failure": {"sequence_number": 3, "error": "disk full"}},
    ]

    with mock.patch("feldera.pipeline.time.sleep"):
        with pytest.raises(RuntimeError) as e:
            _pipeline(client).checkpoint(wait=True)

    assert str(e.value) == "failed to make checkpoint '3': disk full"


def test_checkpoint_without_wait_does_not_check_status():
    client = mock.Mock()
    client.checkpoint_pipeline_response.return_value = {"checkpoint_sequence_number": 3}

    assert _pipeline(client).checkpoint() == 3
    client.checkpoint_pipeline_status.assert_not_called()


def test_sync_checkpoint_wait_failure_message_includes_error():
    client = mock.Mock()
    client.sync_checkpoint_response.return_value = {"checkpoint_uuid": UUID}
    client.sync_checkpoint_status.return_value = {
        "running": [],
        "success": None,
        "periodic": None,
        "failure": {"uuid": UUID, "error": "access denied"},
    }

    with pytest.raises(RuntimeError) as e:
        _pipeline(client).sync_checkpoint(wait=True)

    assert str(e.value) == f"failed to sync checkpoint '{UUID}': access denied"
