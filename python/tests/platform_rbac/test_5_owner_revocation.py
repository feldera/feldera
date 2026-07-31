"""Withdrawing platform ownership, the only way it can be withdrawn.

`owner` is deploy-time configuration and never grantable through the API, so it
is never revocable through the API either. The only lever is the configuration
itself, which means revocation is a restart, and nothing else in this suite
proves that lever works.

This runs last. It hands the installation to a different owner, so every earlier
module would lose the owner it depends on.
"""

from __future__ import annotations

import pytest

from .conftest import (
    OWNER_EMAIL,
    OWNER_TRUST_SUBJECT,
    SUCCESSOR_EMAIL,
    TENANT,
    Api,
    multi_tenant_auth,
)
from .idp import Issuer
from .manager import Manager

pytestmark = pytest.mark.rbac


def test_owners_hold_the_platform_before_the_change(
    api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """Both kinds of owner work, so the next test measures a change."""
    assert (
        api.status("GET", "/tenants", token=primary_idp.token("owner", email=OWNER_EMAIL))
        == 200
    )
    assert (
        api.status("GET", "/tenants", token=workload_idp.token(OWNER_TRUST_SUBJECT))
        == 200
    )


def test_restarting_without_them_revokes_both(
    manager: Manager, api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """A restart that stops naming an owner takes ownership away.

    Both kinds go at once: the user named in `FELDERA_OWNERS` and the workload
    named in `FELDERA_OWNER_TRUSTS`. Their tokens are unchanged and still valid,
    which is the point -- nothing about the credential changed, only what the
    installation is willing to say about it.
    """
    former_user = primary_idp.token("owner", email=OWNER_EMAIL)
    former_workload = workload_idp.token(OWNER_TRUST_SUBJECT)

    # Hand the installation to a different owner, and configure no owner trust.
    manager.restart(multi_tenant_auth(primary_idp, owners=SUCCESSOR_EMAIL))

    for name, token in (("user", former_user), ("workload", former_workload)):
        status = api.status("GET", "/tenants", token=token)
        assert status in (401, 403), f"the former owner {name} still reaches /tenants"
        assert api.status("GET", "/config/owners", token=token) in (401, 403)

    # The successor holds it instead, so this is a handover rather than an
    # installation that has simply stopped answering.
    successor = primary_idp.token("successor", email=SUCCESSOR_EMAIL)
    assert api.status("GET", "/tenants", token=successor) == 200
    assert api.status("GET", "/config/owners", token=successor) == 200


def test_a_revoked_owner_keeps_its_tenant_membership(
    api: Api, primary_idp: Issuer
):
    """Losing ownership is not losing the account.

    A former owner that also holds a membership still acts in that tenant at the
    role the membership gives it. Ownership is platform-wide and separate, so
    withdrawing it must not disturb anything tenant-scoped.
    """
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    assert api.status("GET", "/tenant/users", token=admin, tenant=TENANT) == 200
    # And still cannot reach the platform routes, which is what it never had.
    assert api.status("GET", "/tenants", token=admin, tenant=TENANT) == 403
