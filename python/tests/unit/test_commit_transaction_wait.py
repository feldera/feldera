"""Tests for FelderaClient.commit_transaction(), which finds the API's
transaction and waits for it to commit by polling `/stats`.

`transaction_id` in `/stats` is the transaction that the pipeline runs now.
After `start_transaction`, that can still be an earlier transaction that is
committing, so the client must not take it for the API's transaction.
"""

from unittest.mock import MagicMock, patch

import pytest

from feldera.rest.feldera_client import FelderaClient

EARLIER = 4
API = 5
INCARNATION = "0190f0e0-0000-7000-8000-000000000001"
RESTARTED = "0190f0e0-0000-7000-8000-000000000002"


def stats(
    transaction_id, initiators_id=None, initiated_by_api=None, incarnation=INCARNATION
):
    initiators = {
        "transaction_id": initiators_id,
        "initiated_by_api": initiated_by_api,
        "initiated_by_connectors": {},
    }
    return {
        "global_metrics": {
            "transaction_id": transaction_id,
            "transaction_initiators": initiators,
            "incarnation_uuid": incarnation,
        }
    }


# The pipeline commits an earlier transaction, and the API's transaction waits
# for it.
EARLIER_COMMITTING = stats(EARLIER, API, "Started")

# The commit of the API's transaction, from the request to the end.
COMMIT = [
    stats(EARLIER, API, "Committed"),
    stats(API, API, "Committed"),
    stats(API),
    stats(0),
]


def commit(responses, **kwargs):
    """Run `commit_transaction` against the `/stats` responses in `responses`,
    and return how many it read."""
    client = FelderaClient.__new__(FelderaClient)
    client.http = MagicMock()
    client.get_pipeline_stats = MagicMock(side_effect=responses)
    with patch("feldera.rest.feldera_client.time.sleep"):
        client.commit_transaction("test-pipeline", **kwargs)
    client.http.post.assert_called_once()
    assert client.http.post.call_args.kwargs["params"] == {
        "transaction_id": API,
        "incarnation_uuid": kwargs.get("incarnation_uuid", INCARNATION),
    }
    return client.get_pipeline_stats.call_count


@pytest.mark.parametrize("transaction_id", [API, None])
def test_waits_for_api_transaction_behind_earlier_commit(transaction_id):
    responses = [EARLIER_COMMITTING] + COMMIT
    assert commit(responses, transaction_id=transaction_id) == len(responses)


def test_commit_after_later_transaction_starts():
    responses = [stats(API, API, "Started"), stats(API), stats(API + 1)]
    assert commit(responses) == len(responses)


def test_no_api_transaction():
    with pytest.raises(RuntimeError):
        commit([stats(EARLIER)])


def test_api_transaction_already_committing():
    with pytest.raises(RuntimeError):
        commit([stats(EARLIER, API, "Committed")])


@pytest.mark.parametrize("start", range(len(COMMIT)))
def test_retry_waits_for_committing_transaction(start):
    """A retry, for example after a `TimeoutError`, waits for the commit that
    the first call started, or succeeds if the transaction committed."""
    responses = [COMMIT[start]] + COMMIT[start:]
    assert commit(responses, transaction_id=API) == len(responses)


def test_transaction_id_mismatch():
    with pytest.raises(ValueError):
        commit([EARLIER_COMMITTING], transaction_id=API + 1)


def test_restart_while_waiting():
    with pytest.raises(RuntimeError, match="restarted"):
        commit([stats(API, API, "Started"), stats(0, incarnation=RESTARTED)])


def test_commit_names_start_incarnation():
    """A commit with the incarnation from `start_transaction` sends it even
    after a restart, so that the pipeline rejects the commit, instead of
    committing a new transaction with the same ID."""
    with pytest.raises(RuntimeError, match="restarted"):
        commit(
            [stats(API, API, "Started", incarnation=RESTARTED)] * 2,
            transaction_id=API,
            incarnation_uuid=INCARNATION,
        )


def test_pipeline_without_initiators():
    responses = [
        {"global_metrics": {"transaction_id": API, "incarnation_uuid": INCARNATION}},
        {"global_metrics": {"transaction_id": API, "incarnation_uuid": INCARNATION}},
        {"global_metrics": {"transaction_id": 0, "incarnation_uuid": INCARNATION}},
    ]
    assert commit(responses, transaction_id=API) == len(responses)
