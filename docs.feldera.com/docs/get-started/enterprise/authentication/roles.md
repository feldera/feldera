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
platform-wide: it is not stored as a tenant membership and is granted only
through configuration (`owners`) or an owner OIDC trust relationship. An owner
can select the tenant it acts in with the `Feldera-Tenant` request header.

## How a role is assigned

- On a user's first login to a tenant, if the user has no membership yet, they
  are admitted at the configured default role (see [below](#default-role-and-owners))
  and a membership record is created.
- When that first login also creates the tenant (auto-provisioning, because the
  resolved tenant did not exist yet), the user is granted the configured
  first-user role, `admin` by default (see [below](#default-role-and-owners)). A
  tenant an owner creates explicitly starts with no members, and the first user
  to log into it is admitted at the default role.
- An `admin` or `owner` can pre-provision members and change member roles from the
  Admin page in the web console, or through the
  [set-member-role API](/api/assign-member-role).
- `owner` comes from the `owners` setting or an owner OIDC trust; it is never
  assigned as a tenant membership.
- A federated token is matched against every trust registered for its issuer, and
  the most permissive matching role wins. An owner trust outranks any
  tenant-scoped trust the same token also matches, so a workload that a tenant
  trust admits at `write` acts as `owner` once a matching owner trust exists.
  Keep the subject and audience patterns on an owner trust narrow: only a
  platform owner can create one, but a broad pattern promotes every workload it
  matches. An owner trust always requires the `Feldera-Tenant` header to name the
  tenant to act in.
- An API key carries a role capped at its creator's role, limited to `read` or
  `write`.

## Platform owners

A new installation has no owners until you configure them. Set
`authorization.owners` (Helm), which sets the `FELDERA_OWNERS` environment
variable on the pipeline-manager pod, to a list of identities. Each entry matches
an access token in one of three forms:

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
:::

An owner can pre-provision members and grant `admin` to the people who will
manage each tenant, from the Admin page or through the
[set-member-role API](/api/assign-member-role).

## Default role and owners

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

`authorization.owners` (environment: `FELDERA_OWNERS`) lists the identities
granted the platform-wide `owner` role, in the forms shown above. It is empty by
default. At least one owner is needed to manage tenants across the installation
and to grant `admin` in a tenant that a user did not create.
