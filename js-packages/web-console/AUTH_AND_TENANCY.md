# Auth, RBAC and tenancy in web-console

What the pipeline manager's RBAC / OIDC-trust / tenant-management system exposes,
and how web-console sees and uses it today. Reference for gating UI on permissions.

## Roles

One ordered role scale. Declaration order is the privilege order; higher includes lower.

| Role    | Rank | Held by                                                    |
| ------- | ---- | ---------------------------------------------------------- |
| `read`  | 0    | interactive login, API key, OIDC trust                     |
| `write` | 1    | interactive login, API key, OIDC trust                     |
| `admin` | 2    | interactive login, OIDC trust (not API keys)               |
| `owner` | 3    | interactive login, platform-wide OIDC trust (not API keys) |

Distinct role subsets exist for different principals:

- `Role` (`read | write | admin | owner`): the full scale. A session's role, a member's role, an OIDC trust's role.
- `MemberRole` (`read | write | admin`): assignable to a tenant member. `owner` is platform-wide, not a tenant membership, so it is never assigned as a member role. An owner resolved by the IdP can still appear in the member list and is shown read-only.
- `MintableKeyRole` (`read | write`): an API key carries exactly one. `admin`/`owner` are never issuable as keys.

`owner` is platform-wide (spans all tenants); `read`/`write`/`admin` are per-tenant.

## How web-console sees the current user's permissions

The role comes from the session payload, not the JWT. `GET /v0/config/session` returns `SessionInfo`:

```
SessionInfo { tenant_id, tenant_name, role }   // role: read | write | admin | owner
```

`src/routes/+layout.ts` reads it into `page.data.feldera`:

| `page.data.feldera` field | Source                          | Use                             |
| ------------------------- | ------------------------------- | ------------------------------- |
| `role`                    | `roleOf(sessionConfig.role)`    | normalized role, default `read` |
| `permissions`             | `permissionsOf(role)`           | the list UI gates read          |
| `tenantId`, `tenantName`  | `sessionConfig.tenant_id/_name` | current acting tenant           |
| `authorizedTenants?`      | JWT `tenants` claim (decoded)   | multi-tenant switch list        |

`role` is the whole permission surface the backend sends: one ordered role,
`read < write < admin < owner`, no separate capability list or resource-level
ACLs. web-console does not gate on the role rank directly. At init `+layout.ts`
materializes `permissions` from the role via the client role to permission map
(see `web-console-permissions.md`), shaped as if the server had sent the list, so
each UI gate names the permission it needs and reads `permissions` for it.

`authorizedTenants` is the only value read from the token: the `tenants` claim
(array, or comma-separated string) lists tenants a multi-tenant login may act in.
An owner's token typically carries no `tenants` claim.

## Tenancy and the acting tenant

A request acts within one tenant, chosen by the `Feldera-Tenant` header.

- Global selection: `getSelectedTenant()` / `setSelectedTenant()` (`src/lib/services/auth.ts`), persisted per session. `applyAuthToRequest` adds it as `Feldera-Tenant` on every call.
- Per-call override: a request that already sets `Feldera-Tenant` wins over the global selection. This lets an owner inspect/manage one tenant on the admin page without moving the global selection.
- Tenants resolve by name. A login lands in the tenant whose name the IdP asserts; `initial_provider` on a tenant is provenance only and does not steer routing.

## API surface and minimum roles

All new endpoints and the role they require:

| Method + path                       | Min role | Purpose                                  |
| ----------------------------------- | -------- | ---------------------------------------- |
| `GET  /v0/tenant/users`             | admin    | list members of the acting tenant        |
| `POST /v0/tenant/users`             | admin    | pre-provision a member by identity       |
| `PUT  /v0/tenant/users/{user_id}`   | admin    | assign/change a member's role            |
| `DELETE /v0/tenant/users/{user_id}` | admin    | remove a member from the acting tenant   |
| `GET  /v0/oidc_trust`               | admin    | list OIDC trusts (tenant or `?platform`) |
| `POST /v0/oidc_trust`               | admin    | create an OIDC trust                     |
| `GET  /v0/oidc_trust/{name}`        | admin    | get one OIDC trust                       |
| `DELETE /v0/oidc_trust/{name}`      | admin    | delete an OIDC trust                     |
| `GET  /v0/tenants`                  | owner    | list all tenants in the installation     |
| `POST /v0/tenants`                  | owner    | create a tenant                          |
| `PATCH /v0/tenants/{tenant_id}`     | owner    | rename a tenant                          |
| `DELETE /v0/tenants/{tenant_id}`    | owner    | delete an empty tenant                   |

Existing API-key routes now require at least `write` (a read-only caller cannot
even list keys). `POST /v0/api_keys` takes an optional `role` (`read`/`write`,
default `read`).

Role caps enforced server-side:

- A member/key role is capped at the caller's own role and may never be `owner`.
- Removing a member drops the role now, but if the IdP still grants access the
  member is re-added at the default role on next login. Revoke at the provider for a durable block.

### Service wrappers

`src/lib/services/pipelineManager.ts` wraps the generated client. Each accepts an
optional `tenant` (name or UUID) that sets the per-call `Feldera-Tenant` header:

| Wrapper                                         | Endpoint                                |
| ----------------------------------------------- | --------------------------------------- |
| `getTenantUsers(tenant?)`                       | list members                            |
| `addTenantUser({subject,email?,role}, tenant?)` | pre-provision member                    |
| `setTenantUserRole(userId, role, tenant?)`      | assign role                             |
| `removeTenantUser(userId, tenant?)`             | remove member                           |
| `getOidcTrustList(tenant?, platform?)`          | list trusts (`platform` = owner trusts) |
| `postOidcTrust(body, tenant?)`                  | create trust                            |
| `deleteOidcTrust(name, tenant?, platform?)`     | delete trust                            |
| `getTenants()`                                  | list tenants (owner)                    |
| `createTenant(name)`                            | create tenant (owner)                   |
| `renameTenant(id, name, displaceExisting?)`     | rename tenant (owner)                   |
| `deleteTenant(id)`                              | delete tenant (owner)                   |
| `postApiKey(name, role='read')`                 | create key with a role                  |

The member's provider and a new tenant's provider are fixed server-side to the
platform's single configured OIDC issuer; they are not caller-settable.

## Key data shapes

```
TenantMember { user_id, provider, subject, role, email? }   // provider = OIDC issuer
TenantInfo   { id, name, initial_provider }
OidcTrustDescr { id, name, issuer, subject, role, audience?, description? }
```

OIDC trust matches a JWT by `issuer` (exact) plus `subject`/`audience` patterns
where `*` matches any run of characters. A match grants the trust's `role`.

## Admin dashboard

Route `/admin` (`src/routes/(system)/(authenticated)/admin/+page.ts`). Gated in
`load`: entry requires `role` of `admin` or `owner`; anyone else is redirected home.

`AdminPage.svelte` composes:

| Section                                 | Visible to | Component / action                                                                  |
| --------------------------------------- | ---------- | ----------------------------------------------------------------------------------- |
| Tenant switcher (view members of ...)   | owner      | picks a tenant UUID; scopes the members table only, not the global selection        |
| Users & roles                           | admin+     | `UserRoleTable`: list members, add by subject/email/role, change role, remove       |
| Owner access (platform-wide OIDC trust) | owner      | list/create/delete `?platform` trusts, role fixed to `owner`                        |
| Tenants                                 | owner      | `TenantList`: list, create, rename (with `displace_existing`), delete empty tenants |

Per-tenant OIDC trust (read/write/admin for CI and services) is managed from the
"Manage OIDC trust" menu (`OidcTrustMenu.svelte`), not the admin page. The admin
page's trust section is only the platform-wide owner trusts.

## Where the UI already gates on role

| Location               | Rule                                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------------------- |
| `admin/+page.ts`       | redirect unless `admin` or `owner`                                                                      |
| `ProfileButton.svelte` | "Admin" and "Manage OIDC trust" shown only to `admin`/`owner`; "Manage API keys" shown only to `write`+ |
| `NewApiKeyForm.svelte` | `write` option offered only when caller is `write`+; otherwise `read` only                              |
| `AdminPage.svelte`     | tenant switcher, read-only Platform owners and tenants sections behind `write:tenant`                   |
| `UserRoleTable.svelte` | `owner` rows shown read-only (not assignable as a member role)                                          |
