"""The operator upgrade path, run after everything else.

Displacing a tenant name moves it off the tenant that holds the memberships the
other modules provisioned, so this reshapes the installation and has to come
last. It is a module of its own because file order is what pytest guarantees;
being the last function in another file would not survive someone appending to
it.
"""

from __future__ import annotations

import pytest

from .conftest import OWNER_EMAIL, TENANT, Api
from .idp import Issuer
from .test_1_scenarios import PIPELINE, PROGRAM, TENANT_PIPELINE, pipeline_program

pytestmark = pytest.mark.rbac


def test_11_operator_migrates_the_pre_auth_tenant(api: Api, primary_idp: Issuer):
    """The upgrade path an operator actually takes.

    Turning authentication on strands the pre-auth work in the tenant that
    existed before it, while everyone's tokens name a tenant the first login
    created. Renaming the old tenant onto that name reunites the two, and
    `displace_existing` is what lets it take a name already in use. Nothing is
    merged or deleted: the tenant that gives up the name keeps everything it had.

    This runs last. Displacing a name moves it off the tenant that holds the
    memberships every earlier scenario provisioned, so doing it sooner would
    strip the admin of the tenant it administers.
    """
    owner = primary_idp.token("owner", email=OWNER_EMAIL)
    tenants = api.v0("GET", "/tenants", token=owner).json()

    # The pre-auth tenant is the one still holding the pipeline from test_01,
    # whatever it ended up being called.
    pre_auth = next(
        t
        for t in tenants
        if t["name"] != TENANT
        and pipeline_program(api, PIPELINE, token=owner, tenant=t["id"]) == PROGRAM
    )
    displaced_id = next(t["id"] for t in tenants if t["name"] == TENANT)

    # Taking a name already in use needs the displacement flag.
    assert (
        api.status(
            "PATCH", f"/tenants/{pre_auth['id']}", token=owner, body={"name": TENANT}
        )
        == 409
    )

    renamed = api.v0(
        "PATCH",
        f"/tenants/{pre_auth['id']}",
        token=owner,
        body={"name": TENANT, "displace_existing": True},
    )
    assert renamed.status_code == 200, renamed.text
    # The tenant that gave up the name is reported, not silently dropped.
    assert renamed.json()["displaced"]["id"] == displaced_id

    # The operator's goal: `acme` now resolves to the tenant holding the work
    # that predates authentication.
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    assert api.status("GET", "/config/session", token=admin, tenant=TENANT) == 200
    assert pipeline_program(api, PIPELINE, token=admin, tenant=TENANT) == PROGRAM

    # And the displaced tenant kept everything it had.
    assert (
        pipeline_program(api, TENANT_PIPELINE, token=owner, tenant=displaced_id)
        == PROGRAM
    )
