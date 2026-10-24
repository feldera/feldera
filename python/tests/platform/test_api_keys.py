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

    try:
        described = {"id": resp["id"], "name": name, "role": "write"}
        assert TEST_CLIENT.get_api_key(name) == described
        assert described in TEST_CLIENT.list_api_keys()
    finally:
        TEST_CLIENT.delete_api_key(name)

    assert name not in [key["name"] for key in TEST_CLIENT.list_api_keys()]
    with pytest.raises(FelderaAPIError) as err:
        TEST_CLIENT.get_api_key(name)
    assert err.value.status_code == 404
