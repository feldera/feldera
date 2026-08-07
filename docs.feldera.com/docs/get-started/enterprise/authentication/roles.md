---
title: Roles
sidebar_position: 2
---

# Roles

Feldera Enterprise governs access with role-based access control (RBAC). Every
authenticated principal (a human via OIDC, or an API key) holds a role in the
tenant it acts in, and each API route requires a minimum role. The minimum role
for a route is shown on its endpoint in the [API reference](/api).

## Roles

Roles are totally ordered: a higher role includes every capability of the ones
below it.

| Role | Scope | Can do |
|---|---|---|
| `read` | tenant | View pipelines, logs, metrics, stats, and configuration |
| `write` | tenant | Everything `read` can, plus create, edit, run, and delete pipelines; push and query data; and manage the tenant's API keys |
| `admin` | tenant | Everything `write` can, plus manage the tenant's members and their roles, and manage the tenant's OIDC trust relationships |
| `owner` | platform | Everything `admin` can, in any tenant; create tenants; manage the installation |

`read`, `write`, and `admin` are per-tenant memberships. `owner` is
platform-wide and comes only from deploy-time configuration, as a user
(`authorization.owners`) or as an OIDC trust relationship
(`authorization.ownerTrusts`). An owner selects the tenant it acts in with the
`Feldera-Tenant` request header, and acts in the `default` tenant without one.

## How a role is assigned

- On a user's first login to a tenant that the tenancy strategy resolves, if
  the user has no membership yet, they are admitted at the configured default role
  (see [below](#default-roles)) and a membership record is created. This
  login-time enrollment only happens while `authorization.provisionOnLogin` is
  `true` (the default); with it off, access comes solely from memberships
  granted through the API and console, and a user without one is denied.
- When that first login also creates the tenant (auto-provisioning, because the
  resolved tenant did not exist yet), the user is granted the configured
  first-user role, `admin` by default (see [below](#default-roles)). A
  tenant an owner creates explicitly starts with no members, and the first user
  to log into it is admitted at the default role.
- An `admin` or `owner` can pre-provision members and change member roles from the
  Admin page in the web console, or through the
  [set-member-role API](/api/assign-member-role).
- `owner` comes from `authorization.owners` or `authorization.ownerTrusts`; it is
  never assigned as a tenant membership and never granted through the API.
- A federated token is matched against every trust registered for its issuer, and
  the most permissive matching role wins. A configured owner trust outranks any
  tenant-scoped trust the same token also matches, so keep its subject and
  audience patterns narrow: a broad pattern promotes every workload it matches.
- An API key carries a role capped at its creator's role, limited to `read` or
  `write`.

## Revoking access

Removing a member ([`DELETE /v0/tenant/users/{user_id}`](/api/remove-tenant-member),
or the Admin page) deletes their membership. Whether that alone revokes access depends on
`authorization.provisionOnLogin`:

- With provisioning on (the default), the user's next login re-enrolls them at
  the default role whenever the tenancy strategy still resolves the tenant:
  their `tenants` claim still names it, or it is their personal or issuer
  tenant. Full revocation then takes both levers: remove the membership in
  Feldera and stop the strategy from re-provisioning it (adjust the claim at
  the identity provider). Deassigning the user from the application at the
  provider always revokes, because no token is issued at all.
- With provisioning off, nothing re-enrolls: removing the membership is
  revocation, effective on the user's next request.

Removal does not touch what the member created: API keys and OIDC trust
relationships are tenant resources and keep working, and a role demotion
demotes neither. Review API keys and trusts separately when members depart
and revoke them if necessary.

## Platform owners

A new installation has no owners until you configure them.

Owners are configured in Helm. Set `authorization.owners`, which sets the
`FELDERA_OWNERS` environment variable on the pipeline-manager pod, to a list of
identities. Each entry matches an access token in one of three forms:

| Form | Matches | Example |
|---|---|---|
| Verified email | the token's `email`, only when `email_verified` is true | `ops@example.com` |
| OIDC subject | the token's `sub` | `a1b2c3d4-...` |
| Provider-qualified subject | `"<issuer> <subject>"` | `https://accounts.google.com 1234567890` |

```yaml
authorization:
  owners:
    - "ops@example.com"
    - "https://accounts.google.com 1234567890"
```

:::note
Prefer the subject or provider-qualified form over email. An email entry matches
only when the identity provider marks the email verified, and an email address is
user-facing and can change, whereas the subject is stable.

Owner matching reads the access token itself, never the provider's UserInfo
endpoint, so that who is an owner is settled by the token in hand. Many
providers put neither `email` nor `email_verified` in an access token, and an
email entry cannot match on such a provider. If an owner entry appears to have
no effect, check the token's own claims and use the subject form instead.
:::

Set `authorization.ownerTrusts` (environment: `FELDERA_OWNER_TRUSTS`), a list of
OIDC trusts whose matching tokens act as owner:

```yaml
authorization:
  ownerTrusts:
    - issuer: "https://token.actions.githubusercontent.com"
      subject: "repo:acme/infra:ref:refs/heads/main"
      audience: "https://github.com/acme"
```

Trust relationships registered through the API belong to one tenant and grant at
most `admin`.

An owner can pre-provision members and grant `admin` to the users who will
manage each tenant, from the Admin page or through the
[set-member-role API](/api/assign-member-role).

## Default roles

These settings govern how much access an authenticated user has before an admin
or owner assigns them a role.

`authorization.defaultRole` (environment: `FELDERA_AUTH_DEFAULT_ROLE`) is the role
given to an authenticated user who has no explicit membership in the tenant they
resolve to. It must be `read` or `write`; it can never grant `admin` or `owner`,
because those are control-plane roles (managing members, trust, and tenants) that
are granted only explicitly, never handed to an unprovisioned user by default.
The value is applied on the user's first login and recorded as their membership,
so a later change to `defaultRole` does not re-grade a user who has already logged
in. The Helm chart sets this to `write`.

`authorization.firstUserRole` (environment: `FELDERA_AUTH_FIRST_USER_ROLE`) is the
role granted to the user whose login first creates a tenant (auto-provisioning).
It must be `read`, `write`, or `admin`, and defaults to `admin` so the creator can
administer the tenant.
