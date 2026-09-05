"""Percent-encoded spellings of a gated route must not bypass authorization.

The server decodes the request path before routing, so an encoded and a plain
spelling reach the same handler. Authorization classifies the decoded path, so a
low-privilege caller is denied whichever spelling it sends, an allowed caller is
admitted either way, and a path that decodes to no route is a 404, never a
handler. These probes send the path undecoded (see `Api.raw_request`), so the
server does the decoding, exactly as a hostile client would force it to.
"""

from __future__ import annotations

import pytest

from .conftest import TENANT, Api, multi_tenant_auth
from .idp import Issuer
from .manager import Manager

pytestmark = pytest.mark.rbac


@pytest.fixture(scope="module", autouse=True)
def authenticated(manager: Manager, primary_idp: Issuer):
    """Run against an authenticated manager, booting one if a prior module left
    it unauthenticated."""
    if manager.config is None or not manager.config.is_authenticated:
        manager.restart(multi_tenant_auth(primary_idp))
    return manager


def token_for(idp: Issuer, role: str) -> str:
    if role == "owner":
        return idp.token("owner", email="owner@example.com")
    subject = {"read": "reader", "write": "writer", "admin": "admin"}[role]
    return idp.token(subject, email=f"{subject}@example.com", tenants=[TENANT])


# Every spelling below decodes to `/v0/tenants`, the owner-only tenant list.
# Encoding one byte, the last byte, the whole word, and using upper-case hex all
# resolve to the same route.
TENANTS_SPELLINGS = [
    pytest.param("/v0/tenants", id="plain"),
    pytest.param("/v0/%74enants", id="first-byte"),
    pytest.param("/v0/tenant%73", id="last-byte"),
    pytest.param("/v0/%74%65%6e%61%6e%74%73", id="all-bytes-lower-hex"),
    pytest.param("/v0/%74%65%6E%61%6E%74%73", id="all-bytes-upper-hex"),
]


@pytest.mark.parametrize("path", TENANTS_SPELLINGS)
@pytest.mark.parametrize("role", ["read", "write", "admin"])
def test_encoded_owner_route_denies_a_lower_role(
    api: Api, primary_idp: Issuer, path: str, role: str
):
    """Every role below owner is refused the tenant list, however it is spelled."""
    token = token_for(primary_idp, role)
    assert (
        api.raw_request("GET", path, token=token, tenant=TENANT).status_code == 403
    ), f"{role} reached {path}; encoding bypassed the owner check"


@pytest.mark.parametrize("path", TENANTS_SPELLINGS)
def test_encoded_owner_route_admits_the_owner(api: Api, primary_idp: Issuer, path: str):
    owner = token_for(primary_idp, "owner")
    assert (
        api.raw_request("GET", path, token=owner, tenant=TENANT).status_code == 200
    ), f"owner was refused {path}; decoding rejected a valid request"


@pytest.mark.parametrize(
    "path",
    [
        pytest.param("/v0/pipelines", id="plain"),
        pytest.param("/v0/%70ipelines", id="first-byte"),
        pytest.param("/v0/pipeline%73", id="last-byte"),
    ],
)
def test_encoded_write_route_denies_a_reader(api: Api, primary_idp: Issuer, path: str):
    """A reader cannot create a pipeline through any spelling of the route."""
    reader = token_for(primary_idp, "read")
    assert (
        api.raw_request(
            "POST", path, token=reader, tenant=TENANT, body={"name": "x"}
        ).status_code
        == 403
    ), f"reader reached POST {path}; encoding bypassed the write check"


@pytest.mark.parametrize(
    "path",
    [
        # `%2E%2E` decodes to `..`, resolving to no route.
        pytest.param("/v0/pipelines/%2E%2E/tenants", id="encoded-dot-dot"),
        # `%2F` stays encoded (not a path separator), so this is one unknown
        # segment, not `/v0/pipelines/tenants`.
        pytest.param("/v0/pipelines%2Ftenants", id="encoded-slash"),
        # `%25` is `%`, so this decodes once to the literal `/v0/%74enants`,
        # which is no route. The server must not decode twice into `/v0/tenants`.
        pytest.param("/v0/%2574enants", id="double-encoded"),
    ],
)
def test_encoding_that_resolves_to_no_route_is_not_found(
    api: Api, primary_idp: Issuer, path: str
):
    """A path that decodes to no route is answered by the guard as 404, not
    handed to a handler and not confused for a gated route."""
    owner = token_for(primary_idp, "owner")
    resp = api.raw_request("GET", path, token=owner, tenant=TENANT)
    assert resp.status_code == 404, f"{path} resolved to a route unexpectedly"
    assert resp.json().get("error_code") == "UnknownEndpoint"
