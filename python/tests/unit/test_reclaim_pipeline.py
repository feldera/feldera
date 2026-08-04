"""Unit tests for the CI pipeline sweep: reclaiming and target selection."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from feldera import testutils
from tests import stop_ci_run_pipelines


@pytest.fixture()
def client() -> MagicMock:
    """Stand in for the module-level TEST_CLIENT of both modules under test."""
    mock = MagicMock()
    with (
        patch.object(testutils, "TEST_CLIENT", mock),
        patch.object(stop_ci_run_pipelines, "TEST_CLIENT", mock),
    ):
        yield mock


class TestReclaimPipeline:
    def test_reclaims_a_running_pipeline(self, client: MagicMock):
        assert testutils.reclaim_pipeline("p") == []
        client.stop_pipeline.assert_called_once()
        client.clear_storage.assert_called_once()
        client.delete_pipeline.assert_not_called()

    def test_bounds_the_wait_on_every_step(self, client: MagicMock):
        testutils.reclaim_pipeline("p", delete=True)
        for call in (client.stop_pipeline, client.clear_storage):
            assert call.call_args.kwargs["timeout_s"] is not None

    def test_clears_storage_even_when_stopping_fails(self, client: MagicMock):
        client.stop_pipeline.side_effect = RuntimeError("still Stopping")

        failures = testutils.reclaim_pipeline("p", delete=True)

        client.clear_storage.assert_called_once()
        client.delete_pipeline.assert_called_once()
        assert failures == ["p: stop: still Stopping"]

    def test_reports_every_failed_step(self, client: MagicMock):
        client.stop_pipeline.side_effect = RuntimeError("no")
        client.clear_storage.side_effect = RuntimeError("nope")
        client.delete_pipeline.side_effect = RuntimeError("never")

        assert testutils.reclaim_pipeline("p", delete=True) == [
            "p: stop: no",
            "p: clear storage: nope",
            "p: delete: never",
        ]


def pipeline(name: str, deployment: str, storage: str) -> SimpleNamespace:
    return SimpleNamespace(
        name=name, deployment_status=deployment, storage_status=storage
    )


class TestSweep:
    ALL = [
        pipeline("abcde_test_running", "Running", "InUse"),
        pipeline("abcde_test_stopped_but_not_cleared", "Stopped", "InUse"),
        pipeline("abcde_test_done", "Stopped", "Cleared"),
        pipeline("fffff_other_run_running", "Running", "InUse"),
    ]

    def reclaimed_names(self, client: MagicMock, argv: list[str]) -> list[str]:
        client.pipelines.return_value = self.ALL
        with patch("sys.argv", ["stop_ci_run_pipelines", "--prefix", "abcde_", *argv]):
            assert stop_ci_run_pipelines.main() == 0
        return sorted(call.args[0] for call in client.stop_pipeline.call_args_list)

    def test_leaves_other_runs_alone(self, client: MagicMock):
        assert "fffff_other_run_running" not in self.reclaimed_names(client, [])

    def test_reclaims_whatever_still_holds_compute_or_storage(self, client: MagicMock):
        assert self.reclaimed_names(client, []) == [
            "abcde_test_running",
            "abcde_test_stopped_but_not_cleared",
        ]

    def test_never_deletes(self, client: MagicMock):
        """Records survive the sweep: a failed run is read off its pipelines."""
        self.reclaimed_names(client, [])
        client.delete_pipeline.assert_not_called()

    def test_survives_an_unreachable_instance(self, client: MagicMock):
        client.pipelines.side_effect = RuntimeError("connection refused")
        with patch("sys.argv", ["stop_ci_run_pipelines"]):
            assert stop_ci_run_pipelines.main() == 0
