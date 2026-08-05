"""Membership-driven authorization, with and without login provisioning.

The membership table authorizes every login; the tenancy strategy only
provisions rows, and only while provision-on-login is enabled. These scenarios
pin the union semantics under the default flag, deny-by-default with the flag
off, and the selector behavior shared by both. The module runs in order.
"""

from __future__ import annotations

import pytest

from .conftest import OWNER_EMAIL, Api, membership_auth, multi_tenant_auth
from .idp import Issuer
from .manager import Manager

pytestmark = pytest.mark.rbac

M_ALPHA = "m-alpha"
M_BETA = "m-beta"
M_GAMMA = "m-gamma"


def test_01_membership_reaches_beyond_the_claim(
    manager: Manager, api: Api, primary_idp: Issuer
):
    """With provisioning on, a membership grants access the claim never named."""
    manager.restart(multi_tenant_auth(primary_idp))
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    for name in (M_ALPHA, M_BETA, M_GAMMA):
        assert api.status("POST", "/tenants", token=owner, body={"name": name}) in (
            200,
            201,
        )
    carol = primary_idp.token("carol", tenants=[M_ALPHA])
    assert api.status("GET", "/pipelines", token=carol, tenant=M_ALPHA) == 200

    # An admin adds carol to m-beta; her claim still names only m-alpha.
    added = api.v0(
        "POST",
        "/tenant/users",
        token=owner,
        tenant=M_BETA,
        body={"subject": "carol", "role": "read"},
    )
    assert added.status_code == 200, added.text
    assert api.status("GET", "/pipelines", token=carol, tenant=M_BETA) == 200


def test_02_unknown_and_unjoined_tenants_answer_alike(api: Api, primary_idp: Issuer):
    """The selector is no existence oracle: one answer for both."""
    carol = primary_idp.token("carol", tenants=[M_ALPHA])
    not_member = api.v0("GET", "/pipelines", token=carol, tenant=M_GAMMA)
    unknown = api.v0("GET", "/pipelines", token=carol, tenant="m-absent")
    assert not_member.status_code == unknown.status_code == 403
    assert not_member.json()["error_code"] == unknown.json()["error_code"]


def test_03_headerless_ambiguity_and_the_session_picker(api: Api, primary_idp: Issuer):
    """Several memberships without a selector refuse; the session endpoint
    answers with the list to pick from."""
    carol = primary_idp.token("carol", tenants=[M_ALPHA])
    assert api.status("GET", "/pipelines", token=carol) == 400
    session = api.v0("GET", "/config/session", token=carol)
    assert session.status_code == 200, session.text
    body = session.json()
    assert body["tenant_id"] is None
    assert {m["name"] for m in body["memberships"]} == {M_ALPHA, M_BETA}


def test_04_removal_re_enrolls_while_the_claim_still_names(
    api: Api, primary_idp: Issuer
):
    """Revocation is two-lever with provisioning on: deleting the membership
    alone does not keep out a claim that still names the tenant."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    members = api.v0("GET", "/tenant/users", token=owner, tenant=M_ALPHA).json()
    carol_id = next(m["user_id"] for m in members if m["subject"] == "carol")
    assert (
        api.status("DELETE", f"/tenant/users/{carol_id}", token=owner, tenant=M_ALPHA)
        == 200
    )
    carol = primary_idp.token("carol", tenants=[M_ALPHA])
    assert api.status("GET", "/pipelines", token=carol, tenant=M_ALPHA) == 200


def test_05_passive_claim_entries_enroll_but_never_create(
    api: Api, primary_idp: Issuer
):
    """A listed entry beyond the acting one joins an existing tenant at the
    default role and cannot mint a tenant."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    dave = primary_idp.token("dave", tenants=[M_ALPHA, M_GAMMA, "m-typo"])
    assert api.status("GET", "/pipelines", token=dave, tenant=M_ALPHA) == 200
    assert api.status("GET", "/tenants/m-typo", token=owner) == 404
    gamma_members = api.v0("GET", "/tenant/users", token=owner, tenant=M_GAMMA).json()
    dave_row = next(m for m in gamma_members if m["subject"] == "dave")
    assert dave_row["role"] == "read"


def test_06_empty_tenants_claim_is_no_claim(api: Api, primary_idp: Issuer):
    """A claim mapping that evaluates empty derives the personal tenant rather
    than a tenant literally named the empty string."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    erin = primary_idp.token("erin", tenants=[""])
    assert api.status("GET", "/config/session", token=erin) == 200
    names = {t["name"] for t in api.v0("GET", "/tenants", token=owner).json()}
    assert "" not in names
    assert "erin" in names


def test_07_provisioning_off_denies_until_granted(
    manager: Manager, api: Api, primary_idp: Issuer
):
    """With provisioning off, a login creates nothing; access exists exactly
    while a membership row does."""
    manager.restart(membership_auth(primary_idp))
    frank = primary_idp.token("frank")
    assert api.status("GET", "/pipelines", token=frank) == 403
    session = api.v0("GET", "/config/session", token=frank)
    assert session.status_code == 200, session.text
    assert session.json()["tenant_id"] is None
    assert session.json()["memberships"] == []

    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    assert api.status("POST", "/tenants", token=owner, body={"name": "m-delta"}) in (
        200,
        201,
    )
    added = api.v0(
        "POST",
        "/tenant/users",
        token=owner,
        tenant="m-delta",
        body={"subject": "frank", "role": "write"},
    )
    assert added.status_code == 200, added.text

    # A sole membership lands without a header.
    session = api.v0("GET", "/config/session", token=frank).json()
    assert session["tenant_name"] == "m-delta"
    assert session["role"] == "write"
    assert api.status("GET", "/pipelines", token=frank) == 200


def test_08_removal_is_revocation_with_provisioning_off(api: Api, primary_idp: Issuer):
    """Revocation in Feldera suffices once the issuer does not re-enroll."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    members = api.v0("GET", "/tenant/users", token=owner, tenant="m-delta").json()
    frank_id = next(m["user_id"] for m in members if m["subject"] == "frank")
    assert (
        api.status("DELETE", f"/tenant/users/{frank_id}", token=owner, tenant="m-delta")
        == 200
    )
    frank = primary_idp.token("frank")
    assert api.status("GET", "/pipelines", token=frank) == 403


def test_09_claims_are_ignored_with_provisioning_off(api: Api, primary_idp: Issuer):
    """A token's claim neither creates a tenant nor grants access."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    grace = primary_idp.token("grace", tenants=["m-echo", "m-delta"])
    assert api.status("GET", "/pipelines", token=grace, tenant="m-delta") == 403
    assert api.status("GET", "/tenants/m-echo", token=owner) == 404
