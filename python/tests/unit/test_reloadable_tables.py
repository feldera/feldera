"""Tests for `reloadable_tables`: with `BootstrapPolicy.ALLOW`, the SDK approves
a bootstrap only if every modified table is in the list; otherwise it raises
`TableReloadNotAllowedError` and the pipeline stays in AWAITINGAPPROVAL for the
user to approve or reject.

These mock the HTTP layer, so they run without a pipeline.
"""

from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from feldera.enums import BootstrapPolicy, PipelineStatus
from feldera.rest.errors import TableReloadNotAllowedError
from feldera.rest.feldera_client import FelderaClient, _validate_reloadable_tables


def _program_diff(**changes: list) -> dict:
    diff = {
        "added_tables": [],
        "removed_tables": [],
        "modified_tables": [],
        "added_views": [],
        "removed_views": [],
        "modified_views": [],
    }
    diff.update(changes)
    return diff


def _approval_diff(program_diff: Optional[dict], error: Optional[str] = None) -> dict:
    connectors = {
        f"{change}_{kind}_connectors": []
        for change in ("added", "modified", "removed")
        for kind in ("input", "output")
    }
    return {
        "program_diff": program_diff,
        "program_diff_error": error,
        **connectors,
    }


def _status(status: str, approval_diff: Optional[dict] = None) -> dict[str, Any]:
    return {
        "deployment_status": status,
        "deployment_desired_status": "Running",
        "deployment_error": None,
        "deployment_runtime_status_details": (
            None if approval_diff is None else {"approval_diff": approval_diff}
        ),
    }


def _client(*statuses: dict) -> FelderaClient:
    """A `FelderaClient` whose status polls return `statuses` in order."""
    client = FelderaClient.__new__(FelderaClient)
    client.http = MagicMock()
    client.http.get.side_effect = list(statuses)
    return client


def _start_params(client: FelderaClient) -> dict:
    (start,) = [
        c
        for c in client.http.post.call_args_list
        if c.kwargs["path"].endswith("/start")
    ]
    return start.kwargs["params"]


def _approve_calls(client: FelderaClient) -> list:
    return [
        c
        for c in client.http.post.call_args_list
        if c.kwargs["path"].endswith("/approve")
    ]


def _awaiting(program_diff: Optional[dict], error: Optional[str] = None):
    """Status polls up to the read of the diff awaiting approval."""
    diff = _approval_diff(program_diff, error)
    return _status("AwaitingApproval", diff), _status("AwaitingApproval", diff)


def test_approves_when_every_modified_table_is_reloadable():
    client = _client(
        *_awaiting(_program_diff(modified_tables=["t2"], modified_views=["v3"])),
        _status("Running"),
    )

    status = client.start_pipeline(
        "p",
        bootstrap_policy=BootstrapPolicy.ALLOW,
        silent_bootstrap=True,
        reloadable_tables=["t2"],
    )

    assert status == PipelineStatus.RUNNING
    # The server must wait, or it bootstraps before the SDK sees the diff.
    assert _start_params(client)["bootstrap_policy"] == "await_approval"
    (approve,) = _approve_calls(client)
    assert approve.kwargs["params"] == {"silent_bootstrap": "true"}


def test_raises_when_a_modified_table_is_not_reloadable():
    program_diff = _program_diff(modified_tables=["t1", "t2"])
    client = _client(*_awaiting(program_diff))

    with pytest.raises(TableReloadNotAllowedError) as e:
        client.start_pipeline(
            "p", bootstrap_policy=BootstrapPolicy.ALLOW, reloadable_tables=["t2"]
        )

    assert e.value.blocked_tables == ["t1"]
    assert e.value.approval_diff["program_diff"] == program_diff
    assert "modified tables not in reloadable_tables: t1" in str(e.value)
    # Callers that already catch a failed start keep working.
    assert isinstance(e.value, RuntimeError)
    assert _approve_calls(client) == []


def test_raises_when_the_program_diff_is_unknown():
    client = _client(*_awaiting(None, error="no program information"))

    with pytest.raises(TableReloadNotAllowedError, match="no program information") as e:
        client.start_pipeline(
            "p", bootstrap_policy=BootstrapPolicy.ALLOW, reloadable_tables=["t1"]
        )

    assert e.value.blocked_tables == []
    assert _approve_calls(client) == []


def test_empty_list_approves_changes_that_modify_no_table():
    client = _client(
        *_awaiting(_program_diff(added_views=["v2"], modified_views=["v1"])),
        _status("Running"),
    )

    status = client.start_pipeline(
        "p", bootstrap_policy=BootstrapPolicy.ALLOW, reloadable_tables=[]
    )

    assert status == PipelineStatus.RUNNING
    assert len(_approve_calls(client)) == 1


def test_unmodified_pipeline_starts_without_approval():
    client = _client(_status("Running"))

    status = client.start_pipeline(
        "p", bootstrap_policy=BootstrapPolicy.ALLOW, reloadable_tables=[]
    )

    assert status == PipelineStatus.RUNNING
    assert _approve_calls(client) == []


def test_without_reloadable_tables_allow_is_sent_unchanged():
    client = _client(_status("Running"))

    client.start_pipeline("p", bootstrap_policy=BootstrapPolicy.ALLOW)

    assert _start_params(client)["bootstrap_policy"] == "allow"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"bootstrap_policy": None},
        {"bootstrap_policy": BootstrapPolicy.AWAIT_APPROVAL},
        {"bootstrap_policy": BootstrapPolicy.REJECT},
        {"bootstrap_policy": BootstrapPolicy.ALLOW, "wait": False},
    ],
)
def test_rejects_invalid_combinations_before_starting(kwargs):
    client = _client()

    with pytest.raises(ValueError, match="reloadable_tables"):
        client.start_pipeline("p", reloadable_tables=["t1"], **kwargs)

    client.http.post.assert_not_called()


def _details(*modified_tables: str) -> dict:
    return {
        "approval_diff": _approval_diff(
            _program_diff(modified_tables=list(modified_tables))
        )
    }


@pytest.mark.parametrize(
    "reloadable, modified, approved",
    [
        # Names match the diff exactly.
        (["t1"], "t1", True),
        (["MixedCase"], "MixedCase", True),
        (['a"b'], 'a"b', True),
        # The SDK does not apply SQL case rules or remove quotes.
        (["T1"], "t1", False),
        (["mixedcase"], "MixedCase", False),
        (['"MixedCase"'], "MixedCase", False),
        (['"a""b"'], 'a"b', False),
    ],
)
def test_table_names_match_the_diff_exactly(reloadable, modified, approved):
    if approved:
        _validate_reloadable_tables("p", _details(modified), reloadable)
    else:
        with pytest.raises(TableReloadNotAllowedError):
            _validate_reloadable_tables("p", _details(modified), reloadable)


def test_blocked_tables_can_be_passed_back():
    details = _details("MixedCase", 'esc"aped', "upper_t")

    with pytest.raises(TableReloadNotAllowedError) as e:
        _validate_reloadable_tables("p", details, ["upper_t"])
    assert e.value.blocked_tables == ["MixedCase", 'esc"aped']

    _validate_reloadable_tables("p", details, ["upper_t", *e.value.blocked_tables])


@pytest.mark.parametrize("details", [None, {}, {"approval_diff": None}])
def test_missing_approval_diff_raises(details):
    with pytest.raises(TableReloadNotAllowedError, match="did not report"):
        _validate_reloadable_tables("p", details, ["t1"])
