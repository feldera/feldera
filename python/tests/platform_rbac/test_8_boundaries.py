"""Additional authentication, role, and tenant boundaries over real HTTP.

This module provisions its own identities and tenants, so it also runs alone.
Invalid JWTs are signed by the trusted issuer: rejecting a bad signature would
not prove that the manager checks the claims behind it.
"""

from __future__ import annotations

import base64
import http.client
import ssl
import time

import pytest

from .conftest import Api, membership_auth
from .idp import DEFAULT_AUDIENCE, Issuer
from .manager import Manager
from .rbac_matrix import Route, load_routes, probe_body

pytestmark = pytest.mark.rbac

TENANT_X = "rbac-audit-x"
TENANT_Y = "rbac-audit-y"
TENANT_Z = "rbac-audit-z"
EXTRA_ROUTES = [
    Route("POST", "/v0/pipelines/{pipeline_name}/testing", "write"),
    Route(
        "POST",
        "/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command",
        "write",
    ),
]
ROUTES = load_routes() + EXTRA_ROUTES
# test_1_scenarios already exercises these routes with a write trust.
COVERED_TRUST_ROUTES = {
    ("write", "GET", "/v0/pipelines"),
    ("write", "POST", "/v0/pipelines"),
    ("write", "GET", "/v0/tenant/users"),
}
CREDENTIALS = [
    (kind, role)
    for kind, roles in [
        ("login", ["read", "write", "admin", "owner"]),
        ("key", ["read", "write"]),
        ("trust", ["read", "write", "admin"]),
    ]
    for role in roles
]


@pytest.fixture(scope="module")
def boundaries(manager: Manager, api: Api, primary_idp: Issuer, workload_idp: Issuer):
    manager.restart(membership_auth(primary_idp))
    owner = primary_idp.token("owner", email="owner@example.com")
    tenants = {}
    tokens = {("login", "owner"): owner}
    for name in [TENANT_X, TENANT_Y, TENANT_Z]:
        response = api.v0("POST", "/tenants", token=owner, body={"name": name})
        assert response.status_code == 201, response.text
        tenants[name] = response.json()["id"]

    for role in ["read", "write", "admin"]:
        subject = f"audit-{role}"
        response = api.v0(
            "POST",
            "/tenant/users",
            token=owner,
            tenant=TENANT_X,
            body={"subject": subject, "role": role},
        )
        assert response.status_code == 200, response.text
        tokens["login", role] = primary_idp.token(subject)
        response = api.v0(
            "POST",
            "/oidc_trust",
            token=owner,
            tenant=TENANT_X,
            body={
                "name": subject,
                "issuer": workload_idp.url,
                "subject": subject,
                "audience": DEFAULT_AUDIENCE,
                "role": role,
            },
        )
        assert response.status_code == 201, response.text
        tokens["trust", role] = workload_idp.token(subject)

    for role in ["read", "write"]:
        response = api.v0(
            "POST",
            "/api_keys",
            token=owner,
            tenant=TENANT_X,
            body={"name": f"audit-{role}", "role": role},
        )
        assert response.status_code == 201, response.text
        tokens["key", role] = response.json()["api_key"]

    # The same identity has admin in X and read in Y, through either auth path.
    response = api.v0(
        "POST",
        "/tenant/users",
        token=owner,
        tenant=TENANT_Y,
        body={"subject": "audit-admin", "role": "read"},
    )
    assert response.status_code == 200, response.text
    response = api.v0(
        "POST",
        "/oidc_trust",
        token=owner,
        tenant=TENANT_Y,
        body={
            "name": "audit-admin",
            "issuer": workload_idp.url,
            "subject": "audit-admin",
            "audience": DEFAULT_AUDIENCE,
            "role": "read",
        },
    )
    assert response.status_code == 201, response.text

    for tenant in [TENANT_X, TENANT_Y]:
        response = api.v0(
            "POST",
            "/pipelines",
            token=owner,
            tenant=tenant,
            body={
                "name": "audit-shared",
                "description": tenant,
                "program_code": "CREATE TABLE t (id INT);",
            },
        )
        assert response.status_code == 201, response.text
    response = api.v0(
        "POST",
        "/pipelines",
        token=owner,
        tenant=TENANT_Y,
        body={"name": "audit-private-y", "program_code": "CREATE TABLE t (id INT);"},
    )
    assert response.status_code == 201, response.text
    return tokens, tenants


# test_2_route_matrix covers plain login requests to all documented routes.
@pytest.mark.parametrize(
    "kind,role,route,encoded",
    [
        pytest.param(
            kind,
            role,
            route,
            encoded,
            id=f"{kind}-{role}-{route.id}-{'encoded' if encoded else 'plain'}",
        )
        for kind, role in CREDENTIALS
        for route in ROUTES
        for encoded in [False, True]
        if encoded
        or (kind == "login" and route in EXTRA_ROUTES)
        or kind == "key"
        or (
            kind == "trust"
            and (role, route.method, route.path) not in COVERED_TRUST_ROUTES
        )
    ],
)
def test_additional_route_cases_obey_the_role_floor(
    api: Api, boundaries, kind, role, route, encoded
):
    tokens, _ = boundaries
    path = encode_path(route.probe_path) if encoded else route.probe_path
    response = api.raw_request(
        route.method,
        path,
        token=tokens[kind, role],
        tenant=TENANT_X,
        body=probe_body(route),
    )
    if route.allows(role):
        assert response.status_code not in (401, 403), response.text
    else:
        assert response.status_code == 403, response.text
        assert response.json()["error_code"] == "InsufficientPermissions", response.text


@pytest.mark.parametrize("kind", ["login", "trust"])
def test_ungranted_tenant_uuid_is_rejected(api: Api, boundaries, kind):
    tokens, tenants = boundaries
    response = api.v0(
        "GET", "/pipelines", token=tokens[kind, "admin"], tenant=tenants[TENANT_Z]
    )
    assert response.status_code == 403, response.text


@pytest.mark.parametrize("role", ["read", "write"])
def test_api_keys_accept_only_their_own_tenant(api: Api, boundaries, role):
    tokens, tenants = boundaries
    for selector in [None, TENANT_X, tenants[TENANT_X]]:
        response = api.v0(
            "GET",
            "/pipelines/audit-shared",
            token=tokens["key", role],
            tenant=selector,
        )
        assert response.status_code == 200, response.text
        assert response.json()["description"] == TENANT_X
    for selector in [TENANT_Z, tenants[TENANT_Z], "audit-no-such-tenant"]:
        response = api.v0(
            "GET", "/pipelines", token=tokens["key", role], tenant=selector
        )
        assert response.status_code == 403, response.text


@pytest.mark.parametrize("kind,role", CREDENTIALS)
def test_empty_tenant_selector_is_rejected(api: Api, boundaries, kind, role):
    tokens, _ = boundaries
    response = api.v0("GET", "/pipelines", token=tokens[kind, role], tenant="")
    assert response.status_code == 400, response.text


@pytest.mark.parametrize(
    "values", [(TENANT_X, TENANT_Y), (TENANT_Y, TENANT_X), (b"\xff",)]
)
def test_malformed_tenant_headers_do_not_fall_back_to_a_membership(
    manager: Manager,
    boundaries,
    values,
):
    tokens, _ = boundaries
    connection = http.client.HTTPSConnection(
        "localhost",
        manager.port,
        timeout=10,
        context=ssl.create_default_context(cafile=str(manager.ca_cert)),
    )
    try:
        connection.putrequest("GET", "/v0/pipelines")
        connection.putheader("Authorization", f"Bearer {tokens['login', 'read']}")
        for value in values:
            connection.putheader("Feldera-Tenant", value)
        connection.endheaders()
        response = connection.getresponse()
        assert response.status == 400, response.read()
    finally:
        connection.close()


# test_1_scenarios already checks login-token reads across tenant boundaries.
@pytest.mark.parametrize(
    "kind,method,suffix,body",
    [
        (kind, method, suffix, body)
        for kind in ["login", "key", "trust"]
        for method, suffix, body in [
            ("GET", "", None),
            ("PATCH", "", {"description": "cross-tenant mutation"}),
            ("DELETE", "", None),
            ("POST", "/stop?force=true", None),
        ]
        if kind != "login" or method != "GET"
    ],
)
def test_other_tenants_pipeline_is_inaccessible_by_name(
    api: Api,
    boundaries,
    kind,
    method,
    suffix,
    body,
):
    tokens, _ = boundaries
    response = api.v0(
        method,
        f"/pipelines/audit-private-y{suffix}",
        token=tokens[kind, "write"],
        tenant=TENANT_X,
        body=body,
    )
    assert response.status_code == 404, response.text
    response = api.v0(
        "GET",
        "/pipelines/audit-private-y",
        token=tokens["login", "owner"],
        tenant=TENANT_Y,
    )
    assert response.status_code == 200, response.text
    assert response.json()["description"] != "cross-tenant mutation"


@pytest.mark.parametrize("kind", ["login", "key", "trust"])
def test_request_body_and_query_cannot_override_the_tenant(api: Api, boundaries, kind):
    tokens, tenants = boundaries
    name = f"audit-body-{kind}"
    response = api.v0(
        "POST",
        f"/pipelines?tenant_id={tenants[TENANT_Y]}",
        token=tokens[kind, "write"],
        tenant=TENANT_X,
        body={
            "name": name,
            "tenant_id": tenants[TENANT_Y],
            "program_code": "CREATE TABLE t (id INT);",
        },
    )
    assert response.status_code == 201, response.text
    for tenant, status in [(TENANT_X, 200), (TENANT_Y, 404)]:
        response = api.v0(
            "GET",
            f"/pipelines/{name}",
            token=tokens["login", "owner"],
            tenant=tenant,
        )
        assert response.status_code == status, response.text


# The login/name combination is covered by test_08_one_subject_holds_a_role_per_tenant.
@pytest.mark.parametrize(
    "kind,by_id", [("login", True), ("trust", False), ("trust", True)]
)
def test_role_is_resolved_again_for_the_selected_tenant(
    api: Api, boundaries, kind, by_id
):
    tokens, tenants = boundaries
    token = tokens[kind, "admin"]
    for name in [TENANT_X, TENANT_Y, TENANT_X]:
        selector = tenants[name] if by_id else name
        response = api.v0(
            "GET", "/pipelines/audit-shared", token=token, tenant=selector
        )
        assert response.status_code == 200, response.text
        assert response.json()["description"] == name
        response = api.v0("GET", "/tenant/users", token=token, tenant=selector)
        assert response.status_code == (200 if name == TENANT_X else 403), response.text


@pytest.mark.parametrize("claim", ["aud", "client_id", "token_use"])
def test_required_login_claims_cannot_be_omitted(
    api: Api,
    boundaries,
    primary_idp: Issuer,
    claim: str,
):
    # Generic OIDC requires aud; Cognito-specific fields remain optional here.
    token = primary_idp.token("audit-admin", omit_claims=(claim,))
    response = api.v0("GET", "/tenant/users", token=token, tenant=TENANT_X)
    assert response.status_code == (401 if claim == "aud" else 200), response.text


@pytest.mark.parametrize(
    "claims",
    [
        {"aud": None},
        {"aud": 42},
        {"aud": []},
        {"nbf": "invalid"},
        {"token_use": "id"},
        {"client_id": "another-client"},
    ],
)
def test_invalid_login_claims_are_rejected(
    api: Api, boundaries, primary_idp: Issuer, claims
):
    token = primary_idp.token("audit-admin", claims=claims)
    response = api.v0("GET", "/tenant/users", token=token, tenant=TENANT_X)
    assert response.status_code == 401, response.text


def test_login_token_is_rejected_before_its_validity_period(
    api: Api, boundaries, primary_idp
):
    token = primary_idp.token("audit-admin", claims={"nbf": int(time.time()) + 3600})
    response = api.v0("GET", "/tenant/users", token=token, tenant=TENANT_X)
    assert response.status_code == 401, response.text


def protocol(prefix: str, value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode()).rstrip(b"=").decode()
    return f"feldera-{prefix}.{encoded}"


@pytest.mark.parametrize("kind,role", CREDENTIALS)
def test_websocket_credentials_keep_their_role_and_tenant(
    api: Api, boundaries, kind, role
):
    tokens, _ = boundaries
    token = tokens[kind, role]
    for tenant in [TENANT_X, TENANT_Z]:
        headers = {
            "Sec-WebSocket-Protocol": ", ".join(
                [
                    protocol("bearer", token),
                    protocol("tenant", tenant),
                ]
            )
        }
        response = api.v0("GET", "/tenant/users", headers=headers)
        allowed = role == "owner" or (tenant == TENANT_X and role == "admin")
        assert response.status_code == (200 if allowed else 403), response.text


@pytest.mark.parametrize("route", ROUTES, ids=lambda route: route.id)
def test_encoded_routes_require_authentication(api: Api, boundaries, route):
    # Encode the scope prefix as well as every literal and parameter segment.
    path = encode_path(route.probe_path)
    response = api.raw_request(route.method, path, body=probe_body(route))
    assert response.status_code == 401, response.text


def encode_path(path: str) -> str:
    return "/".join("".join(f"%{ord(c):02x}" for c in part) for part in path.split("/"))
