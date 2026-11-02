"""The SDK's tenant member methods, end to end against an authenticated manager.

The other modules here use raw REST on purpose; this one checks that the
`FelderaClient` methods reach the same endpoints and read their answers back.
"""

from __future__ import annotations

import pytest

from feldera.rest.feldera_client import FelderaClient

from .conftest import OWNER_EMAIL, Api, multi_tenant_auth
from .idp import Issuer
from .manager import Manager

pytestmark = pytest.mark.rbac

SDK_TENANT = "m-sdk"


@pytest.fixture(scope="module", autouse=True)
def authenticated(manager: Manager, primary_idp: Issuer):
    """Run against an authenticated manager, booting one if a prior module left
    it unauthenticated."""
    if manager.config is None or not manager.config.is_authenticated:
        manager.restart(multi_tenant_auth(primary_idp))
    return manager


def test_member_lifecycle_through_the_sdk(
    manager: Manager, api: Api, primary_idp: Issuer
):
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    assert api.status("POST", "/tenants", token=owner, body={"name": SDK_TENANT}) in (
        200,
        201,
    )
    client = FelderaClient(
        manager.base_url,
        api_key=owner,
        tenant=SDK_TENANT,
        requests_verify=manager.verify,
    )

    def dana() -> list[dict]:
        return [m for m in client.list_tenant_members() if m["subject"] == "dana"]

    user_id = client.add_tenant_member("dana", "read", email="dana@example.com")[
        "user_id"
    ]
    assert [(m["user_id"], m["role"], m["email"]) for m in dana()] == [
        (user_id, "read", "dana@example.com")
    ]

    client.set_tenant_member_role(user_id, "write")
    assert [m["role"] for m in dana()] == ["write"]

    client.remove_tenant_member(user_id)
    assert dana() == []
