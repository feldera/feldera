"""Unit tests for the `connector_metadata` argument of `push_to_pipeline`."""

import json
from unittest.mock import MagicMock

import pytest

from feldera.rest.feldera_client import FelderaClient


@pytest.fixture()
def client() -> FelderaClient:
    """A ``FelderaClient`` with a mocked HTTP layer (no real network calls)."""
    c = FelderaClient.__new__(FelderaClient)
    c.http = MagicMock()
    c.http.post.return_value = {"token": "t"}
    return c


def _push(client: FelderaClient, **kwargs) -> dict:
    """Pushes one row with `kwargs` and returns the query parameters of the request."""
    client.push_to_pipeline("p", "t", "json", {"id": 1}, wait=False, **kwargs)
    return client.http.post.call_args.kwargs["params"]


def test_connector_metadata_is_sent_as_json(client: FelderaClient):
    metadata = {"kafka_offset": 42, "kafka_topic": "events", "headers": {"h": "v"}}
    params = _push(client, connector_metadata=metadata)
    assert json.loads(params["connector_metadata"]) == metadata


def test_connector_metadata_is_omitted_by_default(client: FelderaClient):
    params = _push(client)
    assert "connector_metadata" not in params


def test_connector_metadata_must_be_a_mapping(client: FelderaClient):
    with pytest.raises(ValueError, match="connector_metadata"):
        _push(client, connector_metadata=[("kafka_offset", 42)])
    client.http.post.assert_not_called()
