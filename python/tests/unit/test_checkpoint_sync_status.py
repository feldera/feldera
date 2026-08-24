"""Tests for Pipeline.sync_checkpoint_status(), which classifies the
`/checkpoint/sync_status` response into a CheckpointStatus.

The classification is what `Pipeline.sync_checkpoint(wait=True)` polls, so a
misread of the response either hangs that loop or fails it on a stale result
from an earlier sync of the same checkpoint.
"""

from unittest.mock import MagicMock
from uuid import UUID

import pytest

from feldera.enums import CheckpointStatus
from feldera.pipeline import Pipeline

# `running` is absent, `success` present, `periodic` absent, and so on: the
# heuristic for pipelines that predate `running` orders UUIDs, so these three
# are ordered LOW < SUBJECT < HIGH.
LOW = "00000000-0000-0000-0000-000000000005"
SUBJECT = "00000000-0000-0000-0000-00000000000a"
HIGH = "00000000-0000-0000-0000-00000000000f"


@pytest.fixture()
def pipeline() -> Pipeline:
    """A `Pipeline` whose only live dependency is the sync status response."""
    client = MagicMock()
    p = Pipeline(client)
    p._inner = MagicMock()
    p._inner.name = "test-pipeline"
    return p


@pytest.fixture(autouse=True)
def _clear_failure_error():
    """Reset the error that `sync_checkpoint_status` writes onto the enum.

    `CheckpointStatus.Failure` is a single shared member, so an error recorded
    by one test would otherwise leak into the next.
    """
    yield
    CheckpointStatus.Failure.error = None


def status_for(pipeline: Pipeline, resp: dict) -> CheckpointStatus:
    pipeline.client.sync_checkpoint_status.return_value = resp
    return pipeline.sync_checkpoint_status(SUBJECT)


@pytest.mark.parametrize(
    "description,resp,expected",
    [
        (
            "sync registered and not yet finished",
            {"running": [SUBJECT], "success": None, "periodic": None},
            CheckpointStatus.InProgress,
        ),
        (
            "re-sync in flight outranks the earlier success",
            {"running": [SUBJECT], "success": SUBJECT, "periodic": None},
            CheckpointStatus.InProgress,
        ),
        (
            "re-sync in flight outranks the earlier failure",
            {
                "running": [SUBJECT],
                "success": None,
                "failure": {"uuid": SUBJECT, "error": "stale"},
            },
            CheckpointStatus.InProgress,
        ),
        (
            "manual sync succeeded",
            {"running": [], "success": SUBJECT, "periodic": None},
            CheckpointStatus.Success,
        ),
        (
            "periodic sync succeeded",
            {"running": [], "success": None, "periodic": SUBJECT},
            CheckpointStatus.Success,
        ),
        (
            "sync failed",
            {
                "running": [],
                "success": None,
                "failure": {"uuid": SUBJECT, "error": "SignatureDoesNotMatch"},
            },
            CheckpointStatus.Failure,
        ),
        (
            # A current pipeline never reports this: recording an outcome
            # clears the opposite slot when it names the same checkpoint.  One
            # that predates that fix can, and the two slots carry no ordering,
            # so the SDK has to pick.  It prefers `success` because the
            # reachable history is retry-after-failure, where the success is
            # the newer fact; preferring `failure` would raise from
            # `sync_checkpoint(wait=True)` for a sync that did complete.
            "legacy pipeline reports the same checkpoint as both",
            {
                "running": [],
                "success": SUBJECT,
                "failure": {"uuid": SUBJECT, "error": "stale"},
            },
            CheckpointStatus.Success,
        ),
        (
            "pipeline has no record of this checkpoint",
            {"running": [], "success": HIGH, "periodic": None},
            CheckpointStatus.Unknown,
        ),
        (
            # Same response as the last case below, minus `running`, and the
            # answer differs: `running` is authoritative, so its absence from
            # an empty set is proof the sync request never landed.
            "no record of this checkpoint, only an earlier one has synced",
            {"running": [], "success": LOW, "periodic": None},
            CheckpointStatus.Unknown,
        ),
        (
            "old pipeline, nothing synced yet",
            {"success": None, "periodic": None},
            CheckpointStatus.InProgress,
        ),
        (
            "old pipeline, a later checkpoint has synced",
            {"success": HIGH, "periodic": None},
            CheckpointStatus.Unknown,
        ),
        (
            "old pipeline, only an earlier checkpoint has synced",
            {"success": LOW, "periodic": None},
            CheckpointStatus.InProgress,
        ),
    ],
)
def test_classification(pipeline: Pipeline, description, resp, expected):
    assert status_for(pipeline, resp) == expected, description


def test_failure_carries_the_error(pipeline: Pipeline):
    status = status_for(
        pipeline,
        {
            "running": [],
            "success": None,
            "failure": {"uuid": SUBJECT, "error": "SignatureDoesNotMatch"},
        },
    )
    assert status == CheckpointStatus.Failure
    assert status.get_error() == "SignatureDoesNotMatch"


def test_other_checkpoints_running_are_ignored(pipeline: Pipeline):
    """Only the requested checkpoint's presence in `running` matters."""
    resp = {"running": [LOW, HIGH], "success": SUBJECT, "periodic": None}
    assert status_for(pipeline, resp) == CheckpointStatus.Success


def test_accepts_a_uuid_object(pipeline: Pipeline):
    """`sync_checkpoint` returns a str, but callers hold UUID objects too."""
    pipeline.client.sync_checkpoint_status.return_value = {
        "running": [SUBJECT],
        "success": None,
        "periodic": None,
    }
    assert pipeline.sync_checkpoint_status(UUID(SUBJECT)) == CheckpointStatus.InProgress
