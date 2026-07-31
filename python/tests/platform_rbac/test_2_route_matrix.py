"""Every gated route, against every role.

The scenarios cover behaviour a reader would want explained. This module covers
breadth instead: it walks the whole route table so a new endpoint cannot ship
without an assertion behind it, and a route whose required role changes fails
here rather than quietly widening.

The suite asserts one bit per route and role: denied, or not denied. That is all
authorization decides. Whether the request then succeeds depends on a body and
resources these probes deliberately do not supply, so the surrounding statuses
(400, 404, 409) are all equally "not denied".
"""

from __future__ import annotations

import pytest

from .conftest import TENANT, Api, multi_tenant_auth
from .idp import Issuer
from .manager import Manager
from .rbac_matrix import ROLE_ORDER, Route, load_routes, probe_body

pytestmark = pytest.mark.rbac

ROUTES = load_routes()


def token_for(idp: Issuer, role: str) -> str:
    """A token holding `role` in the tenant the scenarios provisioned."""
    if role == "owner":
        # Owner comes from deploy-time configuration, not from a membership.
        return idp.token("owner", email="owner@example.com")
    subject = {"read": "reader", "write": "writer", "admin": "admin"}[role]
    return idp.token(subject, email=f"{subject}@example.com", tenants=[TENANT])


@pytest.fixture(scope="module", autouse=True)
def authenticated(manager: Manager, primary_idp: Issuer):
    """Make sure the matrix runs against an authenticated manager.

    The module is order-independent, so it cannot assume which scenario ran
    last; booting the configuration it needs keeps it runnable on its own.
    """
    if manager.config is None or not manager.config.is_authenticated:
        manager.restart(multi_tenant_auth(primary_idp))
    return manager


def test_the_matrix_is_not_empty():
    """A parsing mistake would otherwise turn this module into a no-op."""
    assert len(ROUTES) > 50, f"only {len(ROUTES)} routes parsed from the spec"
    covered = {r.required_role for r in ROUTES}
    assert covered == set(ROLE_ORDER), (
        f"roles missing from the spec: {set(ROLE_ORDER) - covered}"
    )


@pytest.mark.parametrize("role", ROLE_ORDER)
@pytest.mark.parametrize("route", ROUTES, ids=lambda r: r.id)
def test_route_admits_exactly_its_minimum_role(
    api: Api, primary_idp: Issuer, route: Route, role: str
):
    token = token_for(primary_idp, role)
    # `owner` is platform-wide rather than a tenant membership, so an owner
    # belongs to no tenant of its own and names the one it acts in. Everyone
    # else is scoped by their own token, but sending the header keeps the two
    # paths identical from the route's point of view.
    # `probe_path` already carries the `/v0` prefix from the spec, so this goes
    # through `request` rather than `v0`.
    status = api.request(
        route.method,
        route.probe_path,
        token=token,
        tenant=TENANT,
        body=probe_body(route),
    ).status_code

    # A denied caller is refused in middleware, ahead of routing and the
    # handler, so 403 is the only status it can see.
    if route.allows(role):
        assert status != 403, (
            f"{role} should reach {route.id} (needs {route.required_role}) "
            f"but was denied"
        )
    else:
        assert status == 403, (
            f"{role} must not reach {route.id} (needs {route.required_role}); "
            f"got {status}"
        )


def test_no_route_is_reachable_without_a_token(api: Api):
    """Authentication precedes authorization on every gated route."""
    unauthenticated = [
        route
        for route in ROUTES
        if api.request(
            route.method, route.probe_path, body=probe_body(route)
        ).status_code
        != 401
    ]
    assert not unauthenticated, "these routes answered without a token: " + ", ".join(
        r.id for r in unauthenticated
    )
