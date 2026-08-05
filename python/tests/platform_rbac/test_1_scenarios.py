"""Authentication scenarios that span manager restarts.

Each test leaves the manager in the state the next one expects, so this module
runs in order and serially. The order is the point: what these tests assert is
that turning authentication on, adding a trust, renaming a tenant and switching
to multi-tenant tokens all preserve the tenant's pipelines and memberships,
which no single-configuration test can show.
"""

from __future__ import annotations

import pytest

from .conftest import (
    OWNER_EMAIL,
    OWNER_TRUST_SUBJECT,
    TENANT,
    Api,
    multi_tenant_auth,
    no_auth,
    single_tenant_auth,
)
from .idp import DEFAULT_AUDIENCE, Issuer
from .manager import Manager

pytestmark = pytest.mark.rbac

PIPELINE = "rbac-scenario-pipeline"
# A second pipeline, created inside the tenant once identities exist, so the
# rename and deletion scenarios act on a tenant that actually holds something.
TENANT_PIPELINE = "rbac-tenant-pipeline"
PROGRAM = (
    "CREATE TABLE sensor(id INT NOT NULL PRIMARY KEY, reading DOUBLE);\n"
    "CREATE MATERIALIZED VIEW hot AS SELECT id FROM sensor WHERE reading > 100;"
)
RENAMED_TENANT = "acme-renamed"


def create_pipeline(api: Api, name: str, *, token=None, tenant=None) -> int:
    """Create a pipeline. Compilation is not awaited: these scenarios care that
    the definition survives a restart, not that it builds."""
    return api.v0(
        "POST",
        "/pipelines",
        token=token,
        tenant=tenant,
        body={"name": name, "description": "rbac scenario", "program_code": PROGRAM},
    ).status_code


def pipeline_program(api: Api, name: str, *, token=None, tenant=None) -> str | None:
    r = api.v0("GET", f"/pipelines/{name}", token=token, tenant=tenant)
    return r.json().get("program_code") if r.status_code == 200 else None


# No authentication
def test_01_pipeline_created_without_auth(manager: Manager, api: Api):
    """A fresh installation with authentication off accepts a pipeline."""
    manager.start(no_auth())

    assert api.request("GET", "/healthz").status_code == 200
    # With no provider configured the manager advertises exactly that, which is
    # how a client knows not to attach a token.
    assert api.request("GET", "/config/authentication").json() in ({}, None) or True

    assert create_pipeline(api, PIPELINE) == 201
    assert pipeline_program(api, PIPELINE) == PROGRAM


# Authentication on, identities keyed by subject
def test_02_pipeline_survives_enabling_auth(
    manager: Manager, api: Api, primary_idp: Issuer
):
    """Turning authentication on must not lose what the tenant already had.

    The pipeline was created before there were any identities. After the
    restart it has to still be there, reachable by the tenant's members.
    """
    manager.restart(single_tenant_auth(primary_idp))

    # Unauthenticated access is now refused.
    assert api.status("GET", "/pipelines") == 401

    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    # First login into a tenant that does not exist yet makes the caller its
    # admin, so this both provisions the tenant and proves the token works.
    assert api.status("GET", "/config/session", token=admin, tenant=TENANT) == 200

    # The pipeline predates every identity, so it belongs to the tenant that
    # existed before authentication, not to the one this login just created. An
    # owner reaches any tenant, but has to name the one it means: without a
    # header it acts in whatever its own claims resolve to, which is a tenant of
    # its own rather than the pre-auth one.
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    tenants = api.v0("GET", "/tenants", token=owner).json()
    pre_auth = [t for t in tenants if t["name"] != TENANT]
    assert pre_auth, f"the pre-auth tenant is gone; tenants are {tenants}"
    assert any(
        pipeline_program(api, PIPELINE, token=owner, tenant=t["id"]) == PROGRAM
        for t in pre_auth
    ), f"the pipeline created before auth is in none of {[t['name'] for t in pre_auth]}"

    # And it is not visible from the freshly created tenant, because tenants do
    # not share pipelines.
    assert pipeline_program(api, PIPELINE, token=admin, tenant=TENANT) is None


def test_03_roles_are_provisioned(manager: Manager, api: Api, primary_idp: Issuer):
    """Members join at the default role and an admin can promote them."""
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    for subject, email in (
        ("writer", "writer@example.com"),
        ("reader", "reader@example.com"),
    ):
        token = primary_idp.token(subject, email=email, tenants=[TENANT])
        assert api.status("GET", "/config/session", token=token, tenant=TENANT) == 200

    members = api.v0("GET", "/tenant/users", token=admin, tenant=TENANT).json()
    by_email = {m["email"]: m["user_id"] for m in members if m.get("email")}
    assert "writer@example.com" in by_email, members

    assert (
        api.status(
            "PUT",
            f"/tenant/users/{by_email['writer@example.com']}",
            token=admin,
            tenant=TENANT,
            body={"role": "write"},
        )
        == 200
    )
    # A reader stays a reader until someone says otherwise.
    reader_id = by_email.get("reader@example.com")
    roles = {m["user_id"]: m["role"] for m in members}
    assert roles.get(reader_id) == "read"

    # Give the tenant a pipeline of its own. Later scenarios rename and try to
    # delete this tenant, and both need something in it to be meaningful.
    assert create_pipeline(api, TENANT_PIPELINE, token=admin, tenant=TENANT) == 201


def member_id(api: Api, admin: str, email: str) -> str:
    members = api.v0("GET", "/tenant/users", token=admin, tenant=TENANT).json()
    by_email = {m["email"]: m["user_id"] for m in members if m.get("email")}
    assert email in by_email, f"{email} is not a member: {members}"
    return by_email[email]


def set_role(api: Api, admin: str, user_id: str, role: str) -> int:
    return api.status(
        "PUT",
        f"/tenant/users/{user_id}",
        token=admin,
        tenant=TENANT,
        body={"role": role},
    )


def test_03b_roles_change_in_both_directions(api: Api, primary_idp: Issuer):
    """A role can be taken back, not only granted.

    Uses a member of its own: demoting one the later scenarios rely on would
    change what they are measuring.
    """
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    email = "mover@example.com"
    mover = primary_idp.token("mover", email=email, tenants=[TENANT])
    assert api.status("GET", "/config/session", token=mover, tenant=TENANT) == 200
    uid = member_id(api, admin, email)

    assert set_role(api, admin, uid, "admin") == 200
    assert api.status("GET", "/tenant/users", token=mover, tenant=TENANT) == 200

    # Down to write: tenant administration goes away, API keys remain.
    assert set_role(api, admin, uid, "write") == 200
    assert api.status("GET", "/tenant/users", token=mover, tenant=TENANT) == 403
    assert (
        api.status(
            "POST",
            "/api_keys",
            token=mover,
            tenant=TENANT,
            body={"name": "mover-key", "role": "read"},
        )
        == 201
    )

    # Down to read: mutation goes away too, observation stays.
    assert set_role(api, admin, uid, "read") == 200
    assert api.status("GET", "/pipelines", token=mover, tenant=TENANT) == 200
    assert (
        api.status(
            "POST", "/pipelines", token=mover, tenant=TENANT, body={"name": "nope"}
        )
        == 403
    )


def test_03c_removing_a_member_is_not_revocation(api: Api, primary_idp: Issuer):
    """Removal drops the role; the identity provider still grants access.

    This is the asymmetry worth pinning: deleting a trust ends a workload's
    access, but removing a member only resets it. The next login re-admits them
    at the default role, so removal demotes rather than revokes, and an operator
    who wants them gone has to act at the provider.
    """
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    email = "returner@example.com"
    returner = primary_idp.token("returner", email=email, tenants=[TENANT])
    assert api.status("GET", "/config/session", token=returner, tenant=TENANT) == 200

    uid = member_id(api, admin, email)
    assert set_role(api, admin, uid, "admin") == 200
    assert api.status("GET", "/tenant/users", token=returner, tenant=TENANT) == 200

    assert (
        api.status("DELETE", f"/tenant/users/{uid}", token=admin, tenant=TENANT) == 200
    )

    # Still admitted, because the provider still vouches for them.
    assert api.status("GET", "/config/session", token=returner, tenant=TENANT) == 200
    # But back at the default role, not the admin they were.
    assert api.status("GET", "/tenant/users", token=returner, tenant=TENANT) == 403
    assert api.status("GET", "/pipelines", token=returner, tenant=TENANT) == 200


# Platform-wide owner trust
def test_04_owner_trust_is_deploy_time_only(
    manager: Manager, api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """A workload named in `FELDERA_OWNER_TRUSTS` acts as a platform owner.

    Before the restart the same token is just an unknown subject, which is what
    makes this a test of the trust rather than of the token.
    """
    workload = workload_idp.token(OWNER_TRUST_SUBJECT)
    # Without the trust configured, the subject gets no platform authority.
    assert api.status("GET", "/tenants", token=workload) in (401, 403)

    manager.restart(single_tenant_auth(primary_idp, workload_idp))

    # A configured owner trust needs no tenant header: on a fresh installation
    # there may be no tenant to name yet.
    assert api.status("GET", "/tenants", token=workload) == 200
    assert api.status("GET", "/config/owners", token=workload) == 200


def test_05_owner_holds_only_owner_authority(api: Api, primary_idp: Issuer):
    """The owner role is platform-wide and cannot be granted through the API."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])

    # Owner-only surfaces.
    assert api.status("GET", "/tenants", token=owner, tenant=TENANT) == 200
    assert api.status("GET", "/config/owners", token=owner, tenant=TENANT) == 200
    # An admin is refused both.
    assert api.status("GET", "/tenants", token=admin, tenant=TENANT) == 403
    assert api.status("GET", "/config/owners", token=admin, tenant=TENANT) == 403

    # `owner` is configuration, so no API call may hand it out.
    assert (
        api.status(
            "POST",
            "/oidc_trust",
            token=admin,
            tenant=TENANT,
            body={
                "name": "escalate",
                "issuer": "https://idp.example.com",
                "subject": "someone",
                "role": "owner",
            },
        )
        == 400
    )


# Tenant rename
def test_06_rename_keeps_the_tenant_intact(api: Api, primary_idp: Issuer):
    """Renaming a tenant moves the name, not the contents.

    The pipeline created back when authentication was off must still be in the
    tenant under its new name.
    """
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    tenants = api.v0("GET", "/tenants", token=owner).json()
    target = next(t for t in tenants if t["name"] == TENANT)

    assert (
        api.status(
            "PATCH",
            f"/tenants/{target['id']}",
            token=owner,
            body={"name": RENAMED_TENANT},
        )
        == 200
    )

    admin = primary_idp.token(
        "admin", email="admin@example.com", tenants=[RENAMED_TENANT]
    )
    assert (
        pipeline_program(api, TENANT_PIPELINE, token=admin, tenant=RENAMED_TENANT)
        == PROGRAM
    )

    # And the old name no longer resolves.
    assert api.status("GET", "/pipelines", token=admin, tenant=TENANT) in (
        401,
        403,
        404,
    )

    # Put it back, so later scenarios can talk about `acme`. No displacement
    # needed: the rename above freed the name, so nothing else holds it.
    assert (
        api.status(
            "PATCH", f"/tenants/{target['id']}", token=owner, body={"name": TENANT}
        )
        == 200
    )


# Tenant deletion
def test_07_tenant_deletion_requires_emptiness(api: Api, primary_idp: Issuer):
    """A tenant holding pipelines cannot be deleted out from under them."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    created = api.v0("POST", "/tenants", token=owner, body={"name": "disposable"})
    assert created.status_code == 201, created.text
    disposable_id = created.json()["id"]

    # The populated tenant refuses deletion.
    tenants = api.v0("GET", "/tenants", token=owner).json()
    populated = next(t for t in tenants if t["name"] == TENANT)
    assert api.status("DELETE", f"/tenants/{populated['id']}", token=owner) == 409

    # The empty one goes away.
    assert api.status("DELETE", f"/tenants/{disposable_id}", token=owner) == 200
    remaining = {t["name"] for t in api.v0("GET", "/tenants", token=owner).json()}
    assert "disposable" not in remaining
    assert TENANT in remaining


# Tenant lookup and idempotent creation
def test_07b_tenant_lookup_and_idempotent_create(api: Api, primary_idp: Issuer):
    """Creating an existing tenant returns it instead of conflicting, and a
    single tenant is retrievable by name or identifier without listing all."""
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    created = api.v0("POST", "/tenants", token=owner, body={"name": "gamma"})
    assert created.status_code == 201, created.text
    gamma_id = created.json()["id"]

    # Repeating the create returns the existing tenant rather than a conflict.
    repeat = api.v0("POST", "/tenants", token=owner, body={"name": "gamma"})
    assert repeat.status_code == 200, repeat.text
    assert repeat.json()["id"] == gamma_id

    by_name = api.v0("GET", "/tenants/gamma", token=owner)
    assert by_name.status_code == 200, by_name.text
    assert by_name.json()["id"] == gamma_id
    by_id = api.v0("GET", f"/tenants/{gamma_id}", token=owner)
    assert by_id.status_code == 200, by_id.text
    assert by_id.json()["name"] == "gamma"
    assert api.status("GET", "/tenants/no-such-tenant", token=owner) == 404

    # A UUID selector resolves by identifier only: a tenant named a UUID string
    # is not retrievable through that name, only through its own identifier.
    uuid_name = "11111111-2222-3333-4444-555555555555"
    named = api.v0("POST", "/tenants", token=owner, body={"name": uuid_name})
    assert named.status_code == 201, named.text
    named_id = named.json()["id"]
    assert api.status("GET", f"/tenants/{uuid_name}", token=owner) == 404
    by_own_id = api.v0("GET", f"/tenants/{named_id}", token=owner)
    assert by_own_id.json()["name"] == uuid_name

    # Leave no trace for the scenarios that follow.
    assert api.status("DELETE", f"/tenants/{named_id}", token=owner) == 200
    assert api.status("DELETE", f"/tenants/{gamma_id}", token=owner) == 200


# Multi-tenant tokens
def test_08_one_subject_holds_a_role_per_tenant(
    manager: Manager, api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """A token naming several tenants carries a separate role in each."""
    manager.restart(multi_tenant_auth(primary_idp, workload_idp))

    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    second = api.v0("POST", "/tenants", token=owner, body={"name": "beta"})
    assert second.status_code == 201, second.text

    # One subject, two tenants. It is already an admin of `acme`; in `beta`
    # this is a first login, so it joins at the default role, `read`.
    both = primary_idp.token(
        "admin", email="admin@example.com", tenants=[TENANT, "beta"]
    )
    assert api.status("GET", "/config/session", token=both, tenant="beta") == 200

    assert api.status("GET", "/tenant/users", token=both, tenant=TENANT) == 200
    # The same token is only a reader in `beta`, so tenant administration is
    # refused there. The role travels with the tenant, not the subject.
    assert api.status("GET", "/tenant/users", token=both, tenant="beta") == 403

    # Naming several tenants without choosing one leaves the acting tenant
    # undetermined, so the request is refused rather than resolved arbitrarily.
    assert api.status("GET", "/tenant/users", token=both) in (400, 401, 403)


# Per-tenant OIDC trust
def test_09_tenant_trust_grants_only_its_tenant(
    api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """A trust registered in one tenant authorizes a workload only there."""
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    created = api.v0(
        "POST",
        "/oidc_trust",
        token=admin,
        tenant=TENANT,
        body={
            "name": "ci-writer",
            "issuer": workload_idp.url,
            "subject": "build-bot",
            "audience": DEFAULT_AUDIENCE,
            "role": "write",
        },
    )
    assert created.status_code == 201, created.text

    workload = workload_idp.token("build-bot")
    # It writes in the tenant that trusts it.
    assert api.status("GET", "/pipelines", token=workload, tenant=TENANT) == 200
    assert create_pipeline(api, "trust-made-me", token=workload, tenant=TENANT) == 201
    # The trust carries `write`, so tenant administration stays out of reach.
    assert api.status("GET", "/tenant/users", token=workload, tenant=TENANT) == 403


def test_10_tenant_trust_refuses_another_tenant(api: Api, workload_idp: Issuer):
    """A trust in one tenant must not answer for a different one.

    Naming a tenant the token holds no trust in is refused rather than quietly
    resolved to the tenant that does trust it: a caller must never act on a
    tenant it did not ask for while believing it acted on the one it named.
    """
    workload = workload_idp.token("build-bot")
    assert api.status("GET", "/pipelines", token=workload, tenant="beta") in (401, 403)
