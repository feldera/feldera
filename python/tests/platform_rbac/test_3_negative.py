"""Tokens and trust registrations the manager must refuse.

Every case here is a near miss: a token that is well-formed and signed, but by
the wrong key, for the wrong audience, from an issuer nobody trusts, or naming a
subject one character off a pattern that would have matched. A suite that only
checks valid credentials cannot tell authentication from a rubber stamp.

These tests run after the scenarios, against the multi-tenant configuration they
leave behind, and none of them mutate anything that later assertions read.
"""

from __future__ import annotations

import pytest

from .conftest import TENANT, Api
from .idp import DEFAULT_AUDIENCE, Issuer

pytestmark = pytest.mark.rbac

# Anything the manager will not accept as a credential is 401. 403 would mean it
# authenticated the caller and then declined the action, which is a different
# and much weaker statement.
REJECTED = 401


def test_no_token_is_rejected(api: Api):
    assert api.status("GET", "/pipelines") == REJECTED


@pytest.mark.parametrize(
    "token",
    [
        pytest.param("", id="empty"),
        pytest.param("not-a-jwt", id="not-a-jwt"),
        pytest.param("a.b.c", id="three-empty-segments"),
        pytest.param("Bearer nested", id="bearer-inside-bearer"),
    ],
)
def test_malformed_tokens_are_rejected(api: Api, token: str):
    assert api.status("GET", "/pipelines", token=token, tenant=TENANT) == REJECTED


def test_token_from_an_untrusted_issuer_is_rejected(api: Api, rogue_idp: Issuer):
    """Correct shape, correct audience, issuer nobody configured or registered."""
    token = rogue_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    assert api.status("GET", "/pipelines", token=token, tenant=TENANT) == REJECTED


def test_token_signed_by_the_wrong_key_is_rejected(
    api: Api, primary_idp: Issuer, rogue_idp: Issuer
):
    """The rogue issuer claims the trusted issuer's identity.

    The only thing separating this token from a valid one is the signature.
    """
    forged = rogue_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    _, payload, signature = forged.split(".")
    genuine = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    # Keep the rogue signature, adopt the trusted issuer's header (and so its
    # `kid`), so the manager looks the key up in the right JWKS and still fails.
    spliced = f"{genuine.split('.')[0]}.{payload}.{signature}"
    assert api.status("GET", "/pipelines", token=spliced, tenant=TENANT) == REJECTED


def test_expired_token_is_rejected(api: Api, primary_idp: Issuer):
    # Well past expiry: the verifier allows a minute of clock skew, so a token
    # that expired a moment ago proves nothing either way.
    expired = primary_idp.token(
        "admin", email="admin@example.com", tenants=[TENANT], expires_in=-3600
    )
    assert api.status("GET", "/pipelines", token=expired, tenant=TENANT) == REJECTED


def test_wrong_audience_is_rejected(api: Api, primary_idp: Issuer):
    """The deployment pins an audience, so a token minted for another service
    must not be reusable here."""
    other = primary_idp.token(
        "admin", email="admin@example.com", tenants=[TENANT], audience="some-other-api"
    )
    assert api.status("GET", "/pipelines", token=other, tenant=TENANT) == REJECTED


def test_unknown_subject_gets_no_tenant_authority(api: Api, primary_idp: Issuer):
    """A validly signed token for a subject with no membership and no trust."""
    stranger = primary_idp.token("nobody-in-particular")
    assert api.status("GET", "/tenant/users", token=stranger, tenant=TENANT) in (
        401,
        403,
    )


def test_tenant_the_token_does_not_name_is_refused(api: Api, primary_idp: Issuer):
    """A token authorizes the tenants it names, not whichever one is asked for."""
    token = primary_idp.token("reader", email="reader@example.com", tenants=[TENANT])
    assert api.status("GET", "/pipelines", token=token, tenant="beta") in (401, 403)
    # An unknown tenant must not be distinguishable from one that exists but is
    # not authorized, or the header becomes a tenant-existence oracle.
    unknown = api.status("GET", "/pipelines", token=token, tenant="no-such-tenant")
    unauthorized = api.status("GET", "/pipelines", token=token, tenant="beta")
    assert unknown == unauthorized, (
        f"unknown tenant returned {unknown} and unauthorized returned "
        f"{unauthorized}; differing codes let a caller enumerate tenant names"
    )


# Trust registration validation
@pytest.mark.parametrize(
    "body,reason",
    [
        pytest.param(
            {"name": "", "issuer": "https://idp.example.com", "subject": "s"},
            "empty name",
            id="empty-name",
        ),
        pytest.param(
            {"name": "empty-issuer", "issuer": "", "subject": "s"},
            "empty issuer",
            id="empty-issuer",
        ),
        pytest.param(
            {
                "name": "empty-subject",
                "issuer": "https://idp.example.com",
                "subject": "",
            },
            "empty subject",
            id="empty-subject",
        ),
        pytest.param(
            {
                "name": "bad name with spaces",
                "issuer": "https://idp.example.com",
                "subject": "s",
            },
            "name outside the permitted character set",
            id="name-charset",
        ),
        pytest.param(
            {
                "name": "owner-escalation",
                "issuer": "https://idp.example.com",
                "subject": "s",
                "role": "owner",
            },
            "owner is deploy-time configuration",
            id="owner-role",
        ),
    ],
)
def test_trust_registration_rejects_bad_input(
    api: Api, primary_idp: Issuer, body: dict, reason: str
):
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    status = api.status("POST", "/oidc_trust", token=admin, tenant=TENANT, body=body)
    assert status == 400, f"expected 400 for {reason}, got {status}"


def test_trust_subject_pattern_near_misses(
    api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """`*` matches a run of characters; everything else must match literally.

    The near misses matter more than the match: a pattern that is too eager
    grants a workload nobody intended to trust.
    """
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    created = api.v0(
        "POST",
        "/oidc_trust",
        token=admin,
        tenant=TENANT,
        body={
            "name": "prefix-trust",
            "issuer": workload_idp.url,
            "subject": "repo:acme/*",
            "audience": DEFAULT_AUDIENCE,
            "role": "read",
        },
    )
    assert created.status_code == 201, created.text

    # Matches the pattern.
    assert (
        api.status(
            "GET",
            "/pipelines",
            token=workload_idp.token("repo:acme/api"),
            tenant=TENANT,
        )
        == 200
    )
    # Near misses that must not.
    for subject in ("repo:acmex/api", "repo:acme", "xrepo:acme/api", "repo:ACME/api"):
        status = api.status(
            "GET", "/pipelines", token=workload_idp.token(subject), tenant=TENANT
        )
        assert status in (401, 403), (
            f"subject {subject!r} matched 'repo:acme/*' ({status})"
        )


def test_trust_audience_must_match_when_set(
    api: Api, primary_idp: Issuer, workload_idp: Issuer
):
    """An audience on the trust is a filter, so a token missing it is refused."""
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    created = api.v0(
        "POST",
        "/oidc_trust",
        token=admin,
        tenant=TENANT,
        body={
            "name": "aud-scoped",
            "issuer": workload_idp.url,
            "subject": "aud-bot",
            "audience": "a-specific-audience",
            "role": "read",
        },
    )
    assert created.status_code == 201, created.text

    # The deployment's audience is not the trust's audience, so this is refused
    # even though the subject and issuer both match.
    wrong = workload_idp.token("aud-bot", audience=DEFAULT_AUDIENCE)
    assert api.status("GET", "/pipelines", token=wrong, tenant=TENANT) in (401, 403)


# `*` matches any run of characters, including an empty one; every other
# character is literal. These pin both directions of that rule, because a
# pattern looser than its author intended silently widens who a trust admits.
#
# Each case namespaces its pattern and subject. Earlier tests leave trusts in
# this tenant -- one of them matches `repo:acme/*` -- and a token is admitted if
# any trust matches it, so an un-namespaced subject would be let in by a
# different trust than the one under test. The prefix is literal and identical
# on both sides, so it cannot change what is being measured.
CLAIM_PATTERNS = [
    # A star may match nothing, so the literals around it can sit together.
    ("empty-run", "repo:acme/*", "repo:acme/", True),
    ("empty-middle", "a*b", "ab", True),
    # Adjacent stars demand no extra characters.
    ("adjacent-stars", "repo:**", "repo:x", True),
    # The tail is anchored separately, so a trailing literal cannot reuse
    # characters an earlier one already consumed.
    ("no-reuse", "a*a", "a", False),
    # Nothing but `*` is special: these are not regex.
    ("dot-is-literal", "repo:acme/a.c", "repo:acme/abc", False),
    ("plus-is-literal", "repo:acme/a+", "repo:acme/aa", False),
    ("bracket-is-literal", "repo:acme/[a]", "repo:acme/a", False),
    # Matching is case-sensitive.
    ("case-sensitive", "Repo:Acme/*", "repo:acme/api", False),
    # A trailing star is a prefix match, so it admits anything that merely
    # begins with the pattern. Pinned because it is the shape that surprises.
    ("prefix-is-open-ended", "svc-a*", "svc-april-fools", True),
]


def register_pattern_trust(
    api: Api, admin: str, workload_idp: Issuer, name: str, subject: str, audience: str
) -> None:
    created = api.v0(
        "POST",
        "/oidc_trust",
        token=admin,
        tenant=TENANT,
        body={
            "name": name,
            "issuer": workload_idp.url,
            "subject": subject,
            "audience": audience,
            "role": "read",
        },
    )
    assert created.status_code == 201, created.text


@pytest.mark.parametrize(
    "name,pattern,subject,should_match",
    CLAIM_PATTERNS,
    ids=[c[0] for c in CLAIM_PATTERNS],
)
def test_subject_pattern_semantics(
    api: Api,
    primary_idp: Issuer,
    workload_idp: Issuer,
    name: str,
    pattern: str,
    subject: str,
    should_match: bool,
):
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    ns = f"sub-{name}:"
    register_pattern_trust(
        api, admin, workload_idp, f"sub-{name}", ns + pattern, DEFAULT_AUDIENCE
    )

    status = api.status(
        "GET", "/pipelines", token=workload_idp.token(ns + subject), tenant=TENANT
    )
    if should_match:
        assert status == 200, f"{ns + subject!r} should match {ns + pattern!r}"
    else:
        assert status in (401, 403), f"{ns + subject!r} must not match {ns + pattern!r}"


@pytest.mark.parametrize(
    "name,pattern,audience,should_match",
    CLAIM_PATTERNS,
    ids=[c[0] for c in CLAIM_PATTERNS],
)
def test_audience_pattern_semantics(
    api: Api,
    primary_idp: Issuer,
    workload_idp: Issuer,
    name: str,
    pattern: str,
    audience: str,
    should_match: bool,
):
    """The audience pattern is matched by the same rule as the subject.

    Asserted separately because the audience is what keeps a trust from
    admitting tokens minted for another service, so a loose pattern here has the
    same consequence and the same corners.
    """
    admin = primary_idp.token("admin", email="admin@example.com", tenants=[TENANT])
    ns = f"aud-{name}:"
    subject = f"aud-bot-{name}"
    register_pattern_trust(
        api, admin, workload_idp, f"aud-{name}", subject, ns + pattern
    )

    status = api.status(
        "GET",
        "/pipelines",
        token=workload_idp.token(subject, audience=ns + audience),
        tenant=TENANT,
    )
    if should_match:
        assert status == 200, (
            f"audience {ns + audience!r} should match {ns + pattern!r}"
        )
    else:
        assert status in (401, 403), (
            f"audience {ns + audience!r} must not match {ns + pattern!r}"
        )
