"""The connector endpoints put the table or view name in the URL path.

A quoted SQL identifier can contain any character, so the name has to be
percent-encoded; otherwise `#` or `?` cuts the path short and `/` adds a
path segment.
"""

from unittest.mock import MagicMock

import pytest

from feldera.rest.feldera_client import FelderaClient

# A relation name that only survives if it is fully encoded.
RELATION = '"a/b c#d?%"'
ENCODED_RELATION = "%22a%2Fb%20c%23d%3F%25%22"


@pytest.fixture()
def client() -> FelderaClient:
    """A ``FelderaClient`` with a mocked HTTP layer (no real network calls)."""
    c = FelderaClient.__new__(FelderaClient)
    c.http = MagicMock()
    c.http.get.return_value = {"token": "t"}
    return c


@pytest.mark.parametrize(
    "method,verb,relation_kind,action",
    [
        ("input_connector_stats", "get", "tables", "stats"),
        ("output_connector_stats", "get", "views", "stats"),
        ("pause_input_connector", "post", "tables", "pause"),
        ("start_input_connector", "post", "tables", "start"),
        ("pause_output_connector", "post", "views", "pause"),
        ("start_output_connector", "post", "views", "start"),
        ("generate_completion_token", "get", "tables", "completion_token"),
    ],
)
def test_connector_path_encodes_names(
    client: FelderaClient, method, verb, relation_kind, action
):
    getattr(client, method)("p", RELATION, "c1")

    path = getattr(client.http, verb).call_args.kwargs["path"]
    assert path == (
        f"/pipelines/p/{relation_kind}/{ENCODED_RELATION}/connectors/c1/{action}"
    )
