// Client-side RBAC: the role to permission map that web-console gates UI on.
//
// The backend enforces a single ordered role per route (read < write < admin <
// owner). It exposes only the caller's own role (via the session payload, see
// AUTH_AND_TENANCY.md), never a route to role table, so the client cannot
// re-derive per-feature authorization from the API. Instead it keeps this
// hardcoded map. A gate then reads as "this feature needs `exec:runtime_upgrade`"
// and the role that grants it is one lookup, not a rank comparison at the call
// site. The map mirrors the backend today; `rbac.spec.ts` guards against the
// backend introducing a role the client does not model.

export type Role = 'read' | 'write' | 'admin' | 'owner'

export type Permission =
  | 'read:pipeline'
  | 'read:pipeline_code'
  | 'read:pipeline_config'
  | 'read:support_bundle'
  | 'write:pipeline'
  | 'write:pipeline_code'
  | 'write:pipeline_config'
  | 'write:pipeline_meta'
  | 'exec:pipeline'
  | 'exec:checkpoint'
  | 'exec:runtime_upgrade'
  | 'exec:pipeline_data'
  | 'write:api_key'
  | 'write:tenant_member'
  | 'write:oidc_trust'
  | 'write:tenant'
  | 'write:owner_trust'

// Ordered low to high. A role grants everything the roles before it grant.
export const ROLES: Role[] = ['read', 'write', 'admin', 'owner']

// What each role adds on top of the role below it (cumulative, see below).
const GRANTS: Record<Role, Permission[]> = {
  read: ['read:pipeline', 'read:pipeline_code', 'read:pipeline_config', 'read:support_bundle'],
  write: [
    'write:pipeline',
    'write:pipeline_code',
    'write:pipeline_config',
    'write:pipeline_meta',
    'exec:pipeline',
    'exec:checkpoint',
    'exec:runtime_upgrade',
    'exec:pipeline_data',
    'write:api_key'
  ],
  admin: ['write:tenant_member', 'write:oidc_trust'],
  owner: ['write:tenant', 'write:owner_trust']
}

// Precompute the cumulative permission set per role once.
const PERMISSIONS: Record<Role, ReadonlySet<Permission>> = (() => {
  const acc: Permission[] = []
  const out = {} as Record<Role, Set<Permission>>
  for (const role of ROLES) {
    acc.push(...GRANTS[role])
    out[role] = new Set(acc)
  }
  return out
})()

export const hasPermission = (role: Role, permission: Permission): boolean =>
  PERMISSIONS[role].has(permission)

// The permissions a role grants, as a plain array. `+layout.ts` materializes
// this into `page.data.feldera.permissions` at session-config init, so UI gates
// read a data field (as if the server sent it) instead of applying the map at
// every call site.
export const permissionsOf = (role: Role): Permission[] => [...PERMISSIONS[role]]

// Default to the least-privileged role when the session payload lacks one, so a
// missing role never silently unlocks a feature.
export const roleOf = (role: string | undefined): Role =>
  (ROLES as string[]).includes(role ?? '') ? (role as Role) : 'read'

// Fallback for a session whose config is not present yet (boot, unauthenticated,
// or a config-load error that still renders the app shell). Frozen so a consumer
// cannot mutate the shared default.
export const DEFAULT_PERMISSIONS: readonly Permission[] = Object.freeze(permissionsOf('read'))

// Whether the session grants a permission. Reads the permission list
// materialized into `page.data.feldera` (see +layout.ts) and falls back to the
// read floor when `feldera` is absent, so gates never crash and never leak write
// access before the session loads. This is the session-facing check; the
// role-facing `hasPermission` above is used at init and in tests.
export const hasPermissions = (
  feldera: { permissions: readonly Permission[] } | undefined,
  permission: Permission
): boolean => (feldera?.permissions ?? DEFAULT_PERMISSIONS).includes(permission)
