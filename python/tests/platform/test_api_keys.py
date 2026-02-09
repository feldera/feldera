import uuid

import pytest

from feldera.rest.errors import FelderaAPIError
from tests import TEST_CLIENT


def test_create_api_key():
    name = f"test-api-key-{uuid.uuid4().hex[:8]}"

    try:
        resp = TEST_CLIENT.create_api_key(name)
    except FelderaAPIError as err:
        if err.status_code == 501:
            pytest.skip("API key management not supported in this edition")
        raise

    assert resp.get("name") == name
    assert resp.get("id")
    assert resp.get("api_key")

    # Best-effort cleanup
    try:
        TEST_CLIENT.http.delete(path=f"/api_keys/{name}")
    except FelderaAPIError:
        pass
